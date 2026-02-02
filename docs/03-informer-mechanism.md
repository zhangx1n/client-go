# 03 - Informer 机制：client-go 的灵魂

> 这是整个 client-go 最核心、最值得深入的部分。Kubernetes 所有控制器的基石。如果你只深读一个模块，就读这个。

---

## 你会学到什么

- 为什么需要 Informer（不能直接轮询 API Server 吗？）
- LIST-WATCH 机制
- 五大核心组件：Reflector、DeltaFIFO、Controller、Indexer、SharedInformer
- 完整数据流：从 API Server 到你的业务代码
- 每个组件的源码位置和关键方法

---

## 1. 为什么需要 Informer？

假设你要写一个程序，监控所有 Pod 的状态变化。最直觉的做法：

```go
// 错误做法：轮询
for {
    pods, _ := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
    // 对比上次的结果，找出变化...
    time.Sleep(5 * time.Second)
}
```

**这有三个严重问题：**

| 问题 | 说明 |
|------|------|
| 延迟高 | 5 秒轮询间隔内发生的变化都感知不到 |
| 负载大 | 每次 LIST 全量获取所有 Pod，集群有 10000 Pod 就传 10000 个 |
| 扩展差 | 100 个控制器 × 每 5 秒 1 次 LIST = API Server 每秒 20 次全量查询 |

**Informer 的解决方案：**

| 策略 | 效果 |
|------|------|
| LIST + WATCH | 首次全量拉取，之后只接收增量变更事件 |
| 本地缓存 | 读操作从内存缓存读取，零 API Server 请求 |
| 共享 Informer | 同一资源类型全局只建立一个 WATCH 连接 |
| 事件驱动 | 变更即时通知，延迟在毫秒级 |

---

## 2. 整体架构

先看全局图，后面逐个拆解：

```
                    Kubernetes API Server
                           │
                    ① LIST + WATCH
                           │
                           ▼
               ┌───────────────────────┐
               │      Reflector        │   tools/cache/reflector.go
               │                       │
               │  "负责跟 API Server   │
               │   保持同步的哨兵"      │
               └───────────┬───────────┘
                           │
                    ② Add/Update/Delete
                           │
                           ▼
               ┌───────────────────────┐
               │      DeltaFIFO        │   tools/cache/delta_fifo.go
               │                       │
               │  "按对象聚合变更事件   │
               │   的有序队列"          │
               └───────────┬───────────┘
                           │
                       ③ Pop()
                           │
                           ▼
               ┌───────────────────────┐
               │  Controller/Processor │   tools/cache/controller.go
               │                       │   tools/cache/shared_informer.go
               │  "协调者，把变更       │
               │   分发到缓存和处理器"  │
               └─────┬─────────┬───────┘
                     │         │
          ④ 更新缓存  │         │ ⑤ 分发事件
                     ▼         ▼
            ┌──────────┐  ┌───────────────────┐
            │ Indexer   │  │ EventHandler      │
            │ (本地缓存) │  │ OnAdd/OnUpdate/   │
            │           │  │ OnDelete          │
            └──────────┘  └─────────┬─────────┘
                  ↑                 │
                  │          ⑥ Add(key)
            Lister.Get()            │
                  │                 ▼
                  │        ┌──────────────┐
                  └────────│  Workqueue   │ → 你的业务逻辑
                   ⑦ 读缓存 └──────────────┘
```

**数据流总结：**
1. Reflector 通过 LIST 获取全量数据 + WATCH 监听增量变更
2. 变更事件写入 DeltaFIFO
3. Controller 从 DeltaFIFO 弹出事件
4. 更新本地 Indexer 缓存
5. 通知注册的 EventHandler
6. EventHandler 把 key 放入 Workqueue
7. Worker 从 Workqueue 取出 key，通过 Lister 从缓存读取对象，执行业务逻辑

---

## 3. 组件一：ListerWatcher — 连接 API Server 的桥梁

### 一句话理解

ListerWatcher 定义了"怎么从 API Server 获取数据"——两个操作：一次性列出所有（List），持续监听变化（Watch）。

### 源码位置

`tools/cache/listwatch.go:109-118`

```go
// listwatch.go:109
type ListerWatcher interface {
    Lister    // List(options) → 返回资源列表
    Watcher   // Watch(options) → 返回事件流
}
```

### 创建方式

```go
// listwatch.go:218
podListWatcher := cache.NewListWatchFromClient(
    clientset.CoreV1().RESTClient(),  // REST 客户端
    "pods",                            // 资源名称
    "default",                         // 命名空间
    fields.Everything(),               // 字段选择器
)
```

这会创建一个 ListerWatcher，它知道：
- LIST 时调用 `GET /api/v1/namespaces/default/pods`
- WATCH 时调用 `GET /api/v1/namespaces/default/pods?watch=true`

---

## 4. 组件二：Reflector — 数据同步的哨兵

### 一句话理解

Reflector 是 Informer 和 API Server 之间的桥梁。它调用 ListerWatcher 获取数据，然后把变更写入 DeltaFIFO。名字叫"反射器"是因为它将 API Server 的状态"反射"到本地。

### 源码位置

`tools/cache/reflector.go:87-148`

```go
// reflector.go:87
type Reflector struct {
    name                 string              // 用于日志的标识名
    typeDescription      string              // 监听的资源类型描述
    expectedType         reflect.Type        // 期望的对象类型
    store                ReflectorStore      // 数据写入的目标（通常是 DeltaFIFO）
    listerWatcher        ListerWatcherWithContext  // LIST + WATCH 的执行者
    lastSyncResourceVersion string           // 上次同步的 ResourceVersion
    // ...
}
```

### 核心方法：ListAndWatch

**这是整个 Informer 机制最关键的方法。**

**源码位置：** `tools/cache/reflector.go:408-447`

```
ListAndWatchWithContext() 的执行流程：

第一步：LIST（全量拉取）
    │
    ├── 调用 listerWatcher.List()
    ├── 获取所有对象 + 最新的 resourceVersion
    └── 调用 store.Replace() 将全量数据写入 DeltaFIFO
    │
第二步：WATCH（增量监听）
    │
    ├── 调用 listerWatcher.Watch(resourceVersion)
    ├── 收到 ADDED 事件   → store.Add(obj)
    ├── 收到 MODIFIED 事件 → store.Update(obj)
    ├── 收到 DELETED 事件  → store.Delete(obj)
    ├── 收到 BOOKMARK 事件 → 更新 resourceVersion（不做其他操作）
    └── Watch 断开？→ 重新执行 LIST + WATCH
```

### 关键概念：ResourceVersion

**Q: ResourceVersion 是什么？**

A: 每个 K8s 资源都有一个 `resourceVersion` 字段，它是 etcd 的全局递增版本号。Reflector 用它来实现"不丢事件"：

```
时间线：
  rv=100        rv=101         rv=102         rv=103
   │              │              │              │
   ▼              ▼              ▼              ▼
 LIST 拿到      Pod A          Pod B          Pod C
 全量数据       被修改          被创建          被删除
 (rv=100)

Watch 从 rv=100 开始监听，就能收到 101、102、103 的变更事件
```

**Q: Watch 断开了怎么办？**

A: Reflector 会记住最后一次的 resourceVersion，然后从该版本重新 Watch。如果版本太旧（API Server 已经清理了历史），就退化为重新 LIST。

### 源码阅读指引

读 `reflector.go` 时重点关注：

| 行号 | 方法 | 要理解什么 |
|------|------|----------|
| 408-447 | `ListAndWatchWithContext()` | 整体流程 |
| 876-893 | `handleWatch()` | 如何消费 Watch 事件 |
| 904-1023 | `handleAnyWatch()` | 事件处理的核心循环 |

---

## 5. 组件三：DeltaFIFO — 变更事件的有序队列

### 一句话理解

DeltaFIFO 是一个特殊的队列——它不只存储对象的最终状态，而是存储每一个变更步骤（Delta）。同一个对象的多个变更会被聚合到一起。

### 为什么叫 Delta？

普通 FIFO 只存最终状态。假设 Pod A 在短时间内被创建、修改、再修改：

```
普通 FIFO：只知道 Pod A 的最终状态
DeltaFIFO：知道 Pod A 经历了 Added → Updated → Updated

DeltaFIFO 中 Pod A 的数据：
[
    { Type: Added,   Object: PodA_v1 },
    { Type: Updated, Object: PodA_v2 },
    { Type: Updated, Object: PodA_v3 },
]
```

### 核心数据结构

**源码位置：** `tools/cache/delta_fifo.go`

```go
// delta_fifo.go:201-204 — 一个变更事件
type Delta struct {
    Type   DeltaType     // 变更类型
    Object interface{}   // 变更后的对象
}

// delta_fifo.go:167-193 — 变更类型
const (
    Added       DeltaType = "Added"
    Updated     DeltaType = "Updated"
    Deleted     DeltaType = "Deleted"
    Replaced    DeltaType = "Replaced"   // LIST 后全量替换
    Sync        DeltaType = "Sync"       // 周期性重同步
)

// delta_fifo.go:104-146 — 队列本体
type DeltaFIFO struct {
    lock  sync.RWMutex
    cond  sync.Cond
    items map[string]Deltas  // key → 该对象的所有变更
    queue []string           // FIFO 顺序的 key 列表
    // ...
}
```

### 工作方式图解

```
Reflector 写入:
  Add(PodA)     → items["default/podA"] = [{Added, PodA}]        queue: [podA]
  Update(PodA)  → items["default/podA"] = [{Added, PodA},         queue: [podA]
                                            {Updated, PodA'}]
  Add(PodB)     → items["default/podB"] = [{Added, PodB}]        queue: [podA, podB]

Controller 弹出:
  Pop() → 弹出 podA 的所有 Deltas: [{Added, PodA}, {Updated, PodA'}]
          items 删除 podA                                         queue: [podB]
  Pop() → 弹出 podB 的所有 Deltas: [{Added, PodB}]
          items 删除 podB                                         queue: []
```

### 关键方法

| 方法 | 源码行号 | 作用 |
|------|---------|------|
| `Add(obj)` | `delta_fifo.go:336-341` | 追加一个 Added Delta |
| `Update(obj)` | `delta_fifo.go:344-349` | 追加一个 Updated Delta |
| `Delete(obj)` | `delta_fifo.go:356-386` | 追加一个 Deleted Delta |
| `Replace(items, rv)` | `delta_fifo.go:565-644` | 全量替换（LIST 后调用） |
| `Pop(process)` | `delta_fifo.go:509-554` | 弹出一个对象的所有 Deltas 并处理 |

### 小白问答

**Q: 为什么需要按对象聚合？**

A: 如果 Pod A 在 1 秒内更新了 10 次，你不需要处理 10 次——只需要处理最终状态即可。DeltaFIFO 让你能看到所有变更历史，但通常只关心最后一个。

**Q: Pop() 会阻塞吗？**

A: 会。如果队列为空，Pop() 会阻塞等待（用 `sync.Cond` 实现），直到有新数据。

**Q: Replace() 什么时候被调用？**

A: 在 Reflector 执行 LIST 之后。它把 LIST 到的全量数据写入 DeltaFIFO，同时会为已经被删除的对象生成 Deleted 事件。

---

## 6. 组件四：Indexer — 本地缓存

### 一句话理解

Indexer 是一个带索引功能的内存缓存。所有从 API Server 拿到的对象都存在这里，读操作直接从这里读，不需要请求 API Server。

### 接口定义

**源码位置：** `tools/cache/store.go:41-73`（Store 接口）、`tools/cache/index.go:35-55`（Indexer 接口）

```go
// store.go:41 — 基础存储接口
type Store interface {
    Add(obj interface{}) error
    Update(obj interface{}) error
    Delete(obj interface{}) error
    List() []interface{}
    ListKeys() []string
    Get(obj interface{}) (item interface{}, exists bool, err error)
    GetByKey(key string) (item interface{}, exists bool, err error)
    Replace([]interface{}, string) error
    Resync() error
}

// index.go:35 — 带索引的存储接口
type Indexer interface {
    Store
    // 按索引查询
    Index(indexName string, obj interface{}) ([]interface{}, error)
    IndexKeys(indexName, indexedValue string) ([]string, error)
    ByIndex(indexName, indexedValue string) ([]interface{}, error)
    // ...
}
```

### 实现：ThreadSafeStore

**源码位置：** `tools/cache/thread_safe_store.go:239-245`

```go
// thread_safe_store.go:239
type threadSafeMap struct {
    lock  sync.RWMutex
    items map[string]interface{}  // key("namespace/name") → 对象
    index *storeIndex             // 多维索引
}
```

### 索引是什么意思？

默认情况下，对象按 `namespace/name` 作为 key 存储。但有时你需要更灵活的查询：

```go
// 默认只能这样查：
pod, exists, err := indexer.GetByKey("default/my-pod")

// 有了索引，你可以这样查：
// "给我 default 命名空间下的所有 Pod"
pods, err := indexer.ByIndex("namespace", "default")

// "给我某个 Node 上的所有 Pod"（需要自定义索引）
pods, err := indexer.ByIndex("nodeName", "node-1")
```

### Lister：Indexer 的类型安全包装

Lister 是代码生成的，把 Indexer 的 `interface{}` 转成具体类型：

```go
// 直接用 Indexer（类型不安全）
obj, exists, _ := indexer.GetByKey("default/my-pod")
pod := obj.(*v1.Pod)  // 需要类型断言

// 用 Lister（类型安全）
pod, err := podLister.Pods("default").Get("my-pod")  // 直接返回 *v1.Pod
```

**Lister 源码在** `listers/` 目录，是自动生成的。

---

## 7. 组件五：SharedInformer — 协调一切

### 一句话理解

SharedInformer 把上面所有组件组装到一起，提供一个统一的使用入口。"Shared" 的意思是：同一种资源类型只创建一个 Informer 实例，多个使用者共享同一个 Watch 连接和缓存。

### 接口定义

**源码位置：** `tools/cache/shared_informer.go:140-238`

```go
// shared_informer.go:140
type SharedInformer interface {
    // 注册事件处理器
    AddEventHandler(handler ResourceEventHandler) (ResourceEventHandlerRegistration, error)

    // 获取本地缓存
    GetStore() Store

    // 启动 Informer
    Run(stopCh <-chan struct{})

    // 检查缓存是否已同步
    HasSynced() bool

    // ...
}

// shared_informer.go:268 — 带索引的版本
type SharedIndexInformer interface {
    SharedInformer
    AddIndexers(indexers Indexers) error
    GetIndexer() Indexer
}
```

### 内部结构

**源码位置：** `tools/cache/shared_informer.go:420-460`

```go
// shared_informer.go:420
type sharedIndexInformer struct {
    indexer    Indexer          // 本地缓存
    controller Controller      // 内部控制器（管理 Reflector + DeltaFIFO）
    processor  *sharedProcessor // 事件分发器（管理所有 EventHandler）
    listerWatcher ListerWatcher // LIST + WATCH 的执行者
    objectType    runtime.Object // 监听的资源类型
    transform     TransformFunc  // 对象转换函数
    // ...
}
```

### 启动流程

**源码位置：** `tools/cache/shared_informer.go:537-588`

```
sharedIndexInformer.RunWithContext():

1. 创建 DeltaFIFO
   fifo := NewDeltaFIFOWithOptions(...)

2. 创建内部 Controller（包含 Reflector）
   controller := newInformer(fifo, ...)

3. 启动事件分发器
   go processor.run(ctx)

4. 启动内部 Controller（触发 Reflector 的 ListAndWatch）
   controller.RunWithContext(ctx)
```

### HandleDeltas — 事件处理的核心

**源码位置：** `tools/cache/shared_informer.go:724-732`

当 Controller 从 DeltaFIFO 弹出一个对象的所有 Deltas 后，调用 `HandleDeltas`：

```
HandleDeltas(deltas Deltas):

对每个 Delta：
  ├── Sync / Added / Updated:
  │     ├── 对象已在缓存中？
  │     │     ├── 是 → indexer.Update(obj) + 通知 OnUpdate
  │     │     └── 否 → indexer.Add(obj) + 通知 OnAdd
  │     └
  └── Deleted:
        ├── indexer.Delete(obj)
        └── 通知 OnDelete
```

### 事件处理器：ResourceEventHandler

```go
type ResourceEventHandler interface {
    OnAdd(obj interface{}, isInInitialList bool)
    OnUpdate(oldObj, newObj interface{})
    OnDelete(obj interface{})
}

// 简便写法：
informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
    AddFunc: func(obj interface{}) {
        // Pod 被创建了
    },
    UpdateFunc: func(oldObj, newObj interface{}) {
        // Pod 被更新了
    },
    DeleteFunc: func(obj interface{}) {
        // Pod 被删除了
    },
})
```

### 小白问答

**Q: "Shared" 具体是怎么共享的？**

A: 通过 `SharedInformerFactory`（`informers/factory.go:57-75`）。当你调用 `factory.Core().V1().Pods()` 时，Factory 内部会检查是否已经有 Pod Informer，有的话直接返回已有的，没有才创建新的。这样 10 个 Handler 共享同一个 Informer，只建立 1 个 Watch 连接。

**Q: AddEventHandler 中能做耗时操作吗？**

A: **绝对不行。** EventHandler 的回调是同步执行的，会阻塞整个事件处理管道。正确做法是：在回调中只做"提取 key + 放入 Workqueue"，耗时逻辑放到 Worker 中。

**Q: HasSynced() 是什么意思？**

A: Informer 启动后，首先执行 LIST 拉取全量数据。`HasSynced()` 返回 `true` 表示 LIST 的全量数据已经处理完毕，缓存已经是最新的。在此之前从缓存读数据可能不完整。

---

## 8. 使用 SharedInformerFactory

实际使用中，你不会直接创建 SharedInformer，而是通过 Factory：

**源码位置：** `informers/factory.go`

```go
// 创建 Factory
factory := informers.NewSharedInformerFactory(clientset, 10*time.Minute)
//                                                       ↑ resync 周期

// 获取特定资源的 Informer
podInformer := factory.Core().V1().Pods()

// 注册事件处理器
podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
    AddFunc:    func(obj interface{}) { /* ... */ },
    UpdateFunc: func(old, new interface{}) { /* ... */ },
    DeleteFunc: func(obj interface{}) { /* ... */ },
})

// 启动所有 Informer
factory.Start(ctx.Done())           // informers/factory.go:144

// 等待所有缓存同步完成
factory.WaitForCacheSync(ctx.Done())  // informers/factory.go:177

// 通过 Lister 从缓存读取（不请求 API Server）
pod, err := podInformer.Lister().Pods("default").Get("my-pod")
```

### resync 周期是什么？

`10*time.Minute` 表示每 10 分钟触发一次"重同步"——将缓存中所有对象重新投递给 EventHandler（类型为 `Sync`）。目的是让控制器有机会修正因 Bug 或异常导致的状态漂移。

设为 0 表示不重同步。

---

## 9. 完整组件关系图

```
informers.NewSharedInformerFactory(clientset, resync)
    │
    │ factory.Core().V1().Pods()
    ▼
SharedIndexInformer (shared_informer.go:420)
    │
    ├── listerWatcher: NewListWatchFromClient(...)    ← listwatch.go:218
    │        用于 LIST + WATCH API Server
    │
    ├── controller: (controller.go:114)
    │        │
    │        ├── reflector: (reflector.go:87)
    │        │        │
    │        │        └── store: DeltaFIFO (delta_fifo.go:104)
    │        │                    │
    │        │                    └── items: map[string]Deltas
    │        │                        queue: []string
    │        │
    │        └── processLoop: 不断 Pop DeltaFIFO → HandleDeltas
    │
    ├── indexer: cache (store.go:195)
    │        │
    │        └── cacheStorage: threadSafeMap (thread_safe_store.go:239)
    │                    │
    │                    ├── items: map[string]interface{}
    │                    └── index: 多维索引
    │
    └── processor: sharedProcessor
             │
             └── listeners: []*processorListener
                      │
                      └── handler: ResourceEventHandler
                               OnAdd / OnUpdate / OnDelete
```

---

## 10. 源码阅读清单

按这个顺序读，每次只聚焦标注的部分：

| 顺序 | 文件 | 行号 | 看什么 |
|------|------|------|--------|
| 1 | `tools/cache/store.go` | 41-73 | Store 接口（理解缓存抽象） |
| 2 | `tools/cache/index.go` | 35-55, 58 | Indexer 接口 + IndexFunc 类型 |
| 3 | `tools/cache/thread_safe_store.go` | 43-61, 239-245 | ThreadSafeStore 接口和实现 |
| 4 | `tools/cache/listwatch.go` | 109-118, 218-223 | ListerWatcher 接口和创建 |
| 5 | `tools/cache/delta_fifo.go` | 104-146, 167-204 | DeltaFIFO 结构、Delta 和 DeltaType |
| 6 | `tools/cache/delta_fifo.go` | 336-386, 509-554 | Add/Update/Delete/Pop |
| 7 | `tools/cache/delta_fifo.go` | 565-644 | Replace（理解 LIST 后全量替换） |
| 8 | `tools/cache/reflector.go` | 87-148 | Reflector 结构体 |
| 9 | `tools/cache/reflector.go` | 408-447 | ListAndWatchWithContext（最核心） |
| 10 | `tools/cache/reflector.go` | 904-1023 | handleAnyWatch（事件处理循环） |
| 11 | `tools/cache/controller.go` | 123-147, 164-201 | Controller 接口和 RunWithContext |
| 12 | `tools/cache/shared_informer.go` | 140-238 | SharedInformer 接口 |
| 13 | `tools/cache/shared_informer.go` | 420-460 | sharedIndexInformer 结构 |
| 14 | `tools/cache/shared_informer.go` | 537-588 | RunWithContext（启动流程） |
| 15 | `tools/cache/shared_informer.go` | 639-641, 664-722 | AddEventHandler |
| 16 | `tools/cache/shared_informer.go` | 724-732 | HandleDeltas（事件分发核心） |
| 17 | `informers/factory.go` | 57-75, 112, 144, 177, 200 | Factory 结构和关键方法 |

---

## 11. 自测检查

- [ ] LIST-WATCH 模式中，LIST 和 WATCH 分别承担什么角色？
- [ ] ResourceVersion 是用来干什么的？Watch 断开后怎么恢复？
- [ ] DeltaFIFO 中一个对象可以有多个 Delta 吗？举例说明。
- [ ] DeltaFIFO 的 `Replace()` 在什么时候被调用？它会为被删除的对象做什么？
- [ ] Indexer 默认用什么作为 key？
- [ ] SharedInformer 的 "Shared" 体现在哪里？
- [ ] `HasSynced()` 返回 true 意味着什么？
- [ ] 为什么不能在 EventHandler 中做耗时操作？
- [ ] resync 周期设为 0 会怎样？
- [ ] 画出从 API Server 到 EventHandler 的完整数据流（不看笔记）。

全部能答上来？进入下一篇 → [04-workqueue.md](04-workqueue.md)
