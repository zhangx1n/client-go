# 04 - Workqueue：去重、限速、重试

> Informer 告诉你"什么东西变了"，Workqueue 帮你"安全可靠地处理变化"。

---

## 你会学到什么

- 为什么不能直接在 Informer 回调里处理业务逻辑
- 三层队列：基础队列 → 延迟队列 → 限速队列
- 每层解决什么问题
- Forget / Done / AddRateLimited 的区别
- 限速算法（指数退避 + 令牌桶）

---

## 1. 为什么需要 Workqueue？

回顾上一篇，Informer 的 EventHandler 中不能做耗时操作。那业务逻辑放在哪？

```go
// 错误做法：直接在回调里处理
informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
    AddFunc: func(obj interface{}) {
        pod := obj.(*v1.Pod)
        doSomethingExpensive(pod)  // 阻塞了整个事件管道！
    },
})

// 正确做法：回调中只入队，Worker 中处理
informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
    AddFunc: func(obj interface{}) {
        key, _ := cache.MetaNamespaceKeyFunc(obj)
        queue.Add(key)  // 只做这一件事
    },
})
```

Workqueue 解决的核心问题：

| 问题 | Workqueue 的解决方案 |
|------|---------------------|
| 回调阻塞 | 解耦：回调只入队，Worker 异步处理 |
| 重复处理 | 去重：同一个 key 在队列中只出现一次 |
| 处理失败 | 重试：失败后重新入队 |
| 错误风暴 | 限速：指数退避，避免无限快速重试 |
| 并发安全 | 加锁：同一个 key 不会被两个 Worker 同时处理 |

---

## 2. 三层队列架构

client-go 的队列是层层递进的设计：

```
┌─────────────────────────────────────────┐
│  RateLimitingQueue（限速队列）            │  ← 控制器中使用这个
│  AddRateLimited() / Forget()            │
│                                         │
│  ┌─────────────────────────────────┐    │
│  │  DelayingQueue（延迟队列）       │    │
│  │  AddAfter(item, duration)       │    │
│  │                                 │    │
│  │  ┌─────────────────────────┐    │    │
│  │  │  Queue（基础队列）        │    │    │
│  │  │  Add() / Get() / Done() │    │    │
│  │  └─────────────────────────┘    │    │
│  └─────────────────────────────────┘    │
└─────────────────────────────────────────┘
```

每一层在前一层的基础上增加新能力。

---

## 3. 第一层：基础队列（Queue）

### 核心能力：FIFO + 去重 + 并发安全

**源码位置：** `util/workqueue/queue.go`

### 接口定义

**源码位置：** `util/workqueue/queue.go:30-38`

```go
// queue.go:30
type TypedInterface[T comparable] interface {
    Add(item T)                    // 入队
    Get() (item T, shutdown bool)  // 出队（阻塞等待）
    Done(item T)                   // 标记处理完成
    ShutDown()                     // 关闭队列
    // ...
}
```

### 去重机制

Queue 内部有三个核心数据结构：

```go
// queue.go:190-222
type Typed[t comparable] struct {
    queue      []t         // 有序队列（FIFO）
    dirty      set[t]      // 待处理集合（去重用）
    processing set[t]      // 正在处理的集合
    // ...
}
```

**工作原理图解：**

```
Add("pod-a")       queue: [pod-a]      dirty: {pod-a}     processing: {}
Add("pod-b")       queue: [pod-a,b]    dirty: {pod-a,b}   processing: {}
Add("pod-a")       queue: [pod-a,b]    dirty: {pod-a,b}   processing: {}
                   ↑ pod-a 已在 dirty 中，不重复入队！

Get() → "pod-a"   queue: [pod-b]       dirty: {pod-b}     processing: {pod-a}
                   ↑ 从 dirty 移到 processing

Add("pod-a")       queue: [pod-b,a]    dirty: {pod-b,a}   processing: {pod-a}
                   ↑ pod-a 正在被处理，但又有新变化，重新加入 dirty + queue

Done("pod-a")      queue: [pod-b,a]    dirty: {pod-b,a}   processing: {}
                   ↑ pod-a 在 dirty 中，说明处理期间有新变化，保留在队列中
```

**关键点：**
- `dirty` 集合实现去重——同一个 key 只会出现一次
- `processing` 集合防止同一个 key 被两个 Worker 同时处理
- `Done()` 时如果 key 还在 `dirty` 中，说明处理期间又有新事件，会保留等待下次处理

### 源码阅读

| 方法 | 行号 | 关键逻辑 |
|------|------|---------|
| `Add()` | `queue.go:227` | 如果 dirty 中已存在则跳过；如果 processing 中存在只加到 dirty |
| `Get()` | `queue.go:265` | 队列为空时阻塞等待；取出后从 dirty 移到 processing |
| `Done()` | `queue.go:289` | 从 processing 移除；如果 dirty 中还有则重新入队 |

---

## 4. 第二层：延迟队列（DelayingQueue）

### 核心能力：延迟入队

在基础队列上增加了 `AddAfter(item, duration)` —— 等指定时间后才真正加入队列。

**源码位置：** `util/workqueue/delaying_queue.go`

### 接口定义

```go
// delaying_queue.go:37-41
type TypedDelayingInterface[T comparable] interface {
    TypedInterface[T]                          // 继承基础队列
    AddAfter(item T, duration time.Duration)   // 延迟 duration 后入队
}
```

### 使用场景

```go
// 3 秒后重新处理这个 key
queue.AddAfter("default/my-pod", 3*time.Second)
```

这在需要"等待一段时间再重试"的场景下很有用，但你通常不需要直接用 DelayingQueue，因为 RateLimitingQueue 在内部使用了它。

---

## 5. 第三层：限速队列（RateLimitingQueue）★

### 核心能力：自动限速重试

这是控制器中实际使用的队列类型。它在延迟队列的基础上增加了限速逻辑——失败次数越多，等待时间越长。

**源码位置：** `util/workqueue/rate_limiting_queue.go`

### 接口定义

```go
// rate_limiting_queue.go:27-40
type TypedRateLimitingInterface[T comparable] interface {
    TypedDelayingInterface[T]        // 继承延迟队列

    AddRateLimited(item T)           // 按限速规则延迟入队
    Forget(item T)                   // 清除重试计数
    NumRequeues(item T) int          // 查询重试次数
}
```

### 三个关键方法

```go
// 1. AddRateLimited — 按限速规则入队（失败次数越多，等待越久）
queue.AddRateLimited("default/my-pod")
// 第 1 次：等 5ms
// 第 2 次：等 10ms
// 第 3 次：等 20ms
// ...逐步递增，最长 1000s

// 2. Forget — 清除重试计数（处理成功时调用）
queue.Forget("default/my-pod")
// 下次 AddRateLimited 又从 5ms 开始

// 3. NumRequeues — 查询已重试几次
n := queue.NumRequeues("default/my-pod")
if n > 5 {
    // 超过 5 次，放弃
    queue.Forget("default/my-pod")
}
```

### 源码实现

```go
// rate_limiting_queue.go:130-134
type rateLimitingType[T comparable] struct {
    TypedDelayingInterface[T]    // 内嵌延迟队列
    rateLimiter TypedRateLimiter[T]   // 限速器
}

// rate_limiting_queue.go:137
func AddRateLimited(q *rateLimitingType[T], item T) {
    // 用限速器计算等待时间，然后调用 AddAfter
    q.AddAfter(item, q.rateLimiter.When(item))
}
```

---

## 6. 限速算法

### 默认限速器

**源码位置：** `util/workqueue/default_rate_limiters.go:50`

```go
func DefaultTypedControllerRateLimiter[T comparable]() TypedRateLimiter[T] {
    return NewTypedMaxOfRateLimiter(
        NewTypedItemExponentialFailureRateLimiter[T](5*time.Millisecond, 1000*time.Second),
        &TypedBucketRateLimiter[T]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
    )
}
```

默认使用两个限速器的**最大值**：

### (1) 指数退避限速器

**源码位置：** `default_rate_limiters.go:84-90`

```go
type TypedItemExponentialFailureRateLimiter[T comparable] struct {
    failures  map[T]int       // 每个 item 的失败次数
    baseDelay time.Duration   // 基础延迟（5ms）
    maxDelay  time.Duration   // 最大延迟（1000s）
}
```

计算公式：`delay = baseDelay * 2^failures`

```
失败次数    等待时间
   0         5ms
   1        10ms
   2        20ms
   3        40ms
   4        80ms
   5       160ms
   ...
   N       min(5ms * 2^N, 1000s)
```

### (2) 令牌桶限速器

**源码位置：** `default_rate_limiters.go:62-64`

```go
type TypedBucketRateLimiter[T comparable] struct {
    *rate.Limiter  // 标准库的令牌桶
}
```

全局限速：每秒最多处理 10 个，突发最多 100 个。这防止所有 key 同时重试导致的风暴。

### MaxOfRateLimiter — 取最大值

**源码位置：** `default_rate_limiters.go:218-220`

两个限速器都计算出一个延迟时间，取较大的那个。这意味着：
- 单个 key 频繁失败 → 指数退避生效（对这个 key 延迟越来越长）
- 全局重试量太大 → 令牌桶生效（限制总体重试速率）

---

## 7. 完整使用模式

典型的控制器中，队列的使用方式如下：

```go
// 创建队列
queue := workqueue.NewTypedRateLimitingQueue(
    workqueue.DefaultTypedControllerRateLimiter[string](),
)

// ---- Informer 回调中 ----
informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
    AddFunc: func(obj interface{}) {
        key, _ := cache.MetaNamespaceKeyFunc(obj)
        queue.Add(key)  // 注意：这里用 Add，不是 AddRateLimited
    },
    // Update、Delete 类似
})

// ---- Worker 中 ----
func processNextItem() bool {
    // 1. 从队列取出（阻塞等待）
    key, quit := queue.Get()
    if quit {
        return false
    }
    // 2. 标记开始处理
    defer queue.Done(key)

    // 3. 执行业务逻辑
    err := syncHandler(key)

    if err == nil {
        // 4a. 成功：清除重试计数
        queue.Forget(key)
        return true
    }

    // 4b. 失败：限速重试
    if queue.NumRequeues(key) < 5 {
        queue.AddRateLimited(key)  // 指数退避重试
        return true
    }

    // 4c. 超过重试上限：放弃
    queue.Forget(key)
    runtime.HandleError(err)
    return true
}
```

### 流程图

```
Informer 回调: queue.Add(key)
                    │
                    ▼
              ┌───────────┐
              │  Queue     │
              │  [key1,    │
              │   key2,    │
              │   key3]    │
              └─────┬─────┘
                    │
              queue.Get()
                    │
                    ▼
              ┌───────────┐
              │  Worker    │
              │            │
              │  syncHandler(key)
              │            │
              └─────┬─────┘
                    │
            ┌───────┴────────┐
            │                │
         成功              失败
            │                │
    queue.Forget(key)   重试 < 5 次？
    queue.Done(key)     ├── 是 → queue.AddRateLimited(key)
            │           │         queue.Done(key)
            ▼           │
          完成          └── 否 → queue.Forget(key)
                                 queue.Done(key)
                                 记录错误，放弃
```

---

## 8. 易混淆的方法辨析

### Done vs Forget

| 方法 | 作用 | 必须调用？ |
|------|------|-----------|
| `Done(key)` | 释放处理锁，允许同一个 key 再次被 Get | **是**，每次 Get 后必须 Done |
| `Forget(key)` | 清除限速器中的重试计数 | 处理成功时调用 |

```go
key, _ := queue.Get()
defer queue.Done(key)   // 必须调用！否则这个 key 永远不会再被处理

err := process(key)
if err == nil {
    queue.Forget(key)   // 成功了，清除失败计数
}
```

**如果只 Done 不 Forget 会怎样？** 限速器记住了失败次数，下次 `AddRateLimited` 会继续增加延迟。

**如果只 Forget 不 Done 会怎样？** key 卡在 processing 集合中，永远不会再被 Get 出来。这是一个 Bug。

### Add vs AddRateLimited

| 方法 | 场景 |
|------|------|
| `Add(key)` | Informer 回调中使用。立即入队，不需要限速 |
| `AddRateLimited(key)` | 处理失败后重试。按失败次数指数退避 |

---

## 9. 源码阅读清单

| 顺序 | 文件 | 行号 | 看什么 |
|------|------|------|--------|
| 1 | `util/workqueue/queue.go` | 30-38 | TypedInterface 接口 |
| 2 | `util/workqueue/queue.go` | 190-222 | Typed 结构（queue/dirty/processing） |
| 3 | `util/workqueue/queue.go` | 227, 265, 289 | Add/Get/Done 实现 |
| 4 | `util/workqueue/delaying_queue.go` | 37-41 | DelayingInterface 接口 |
| 5 | `util/workqueue/delaying_queue.go` | 249 | AddAfter 实现 |
| 6 | `util/workqueue/rate_limiting_queue.go` | 27-40 | RateLimitingInterface 接口 |
| 7 | `util/workqueue/rate_limiting_queue.go` | 130-145 | 实现（AddRateLimited/Forget） |
| 8 | `util/workqueue/default_rate_limiters.go` | 50 | 默认限速器组合 |
| 9 | `util/workqueue/default_rate_limiters.go` | 84-90 | 指数退避限速器 |
| 10 | `util/workqueue/default_rate_limiters.go` | 62-64 | 令牌桶限速器 |

---

## 10. 自测检查

- [ ] 为什么不能在 Informer 回调中直接处理业务逻辑？
- [ ] Queue 的 `dirty` 和 `processing` 集合各有什么用？
- [ ] 如果处理期间同一个 key 又被 Add 了两次，Done 后会发生什么？
- [ ] `Done()` 和 `Forget()` 的区别是什么？不调用 Done 会怎样？
- [ ] 默认的限速策略是什么？第 5 次失败要等多久？
- [ ] `Add()` 和 `AddRateLimited()` 分别在什么场景使用？
- [ ] 令牌桶限速器解决什么问题？指数退避限速器解决什么问题？

全部能答上来？进入下一篇 → [05-controller-pattern.md](05-controller-pattern.md)
