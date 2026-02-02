# client-go 源码学习指南

> 本指南面向 Kubernetes 初学者，帮助你系统性地理解 client-go 的架构、核心组件和源码阅读路径。
>
> **分专题详细文档请查看 [`docs/`](docs/README.md) 目录：**
>
> | 专题 | 文档 | 内容 |
> |------|------|------|
> | 学习路线 | [docs/README.md](docs/README.md) | 总索引和阅读建议 |
> | 阶段 1 | [docs/01-getting-started.md](docs/01-getting-started.md) | 连接 API Server 的基础 |
> | 阶段 2 | [docs/02-clients.md](docs/02-clients.md) | 三种客户端详解 |
> | 阶段 3 | [docs/03-informer-mechanism.md](docs/03-informer-mechanism.md) | Informer 机制深度解析 |
> | 阶段 4 | [docs/04-workqueue.md](docs/04-workqueue.md) | 工作队列详解 |
> | 阶段 5 | [docs/05-controller-pattern.md](docs/05-controller-pattern.md) | 控制器模式完整实践 |
> | 阶段 6 | [docs/06-advanced-topics.md](docs/06-advanced-topics.md) | Leader Election / 测试 / 进阶 |

---

## 一、client-go 是什么？

client-go 是 Kubernetes 官方提供的 Go 语言客户端库，用于与 Kubernetes API Server 通信。几乎所有的 Kubernetes 控制器（Controller）、Operator、CLI 工具（如 kubectl）都基于 client-go 构建。

掌握 client-go，你就掌握了 Kubernetes 编程的核心。

---

## 二、学习路线图（由浅入深）

```
阶段1: 基础概念        阶段2: 核心客户端        阶段3: Informer 机制       阶段4: 控制器模式
┌──────────────┐    ┌──────────────────┐    ┌────────────────────┐    ┌─────────────────┐
│ REST Config  │    │ Typed Clientset  │    │ Reflector          │    │ Controller 模式  │
│ kubeconfig   │ →  │ Dynamic Client   │ →  │ DeltaFIFO          │ →  │ Workqueue       │
│ 连接方式      │    │ Discovery Client │    │ SharedInformer     │    │ Leader Election │
└──────────────┘    └──────────────────┘    │ Indexer / Lister   │    │ 完整示例        │
                                            └────────────────────┘    └─────────────────┘
```

---

## 三、核心目录结构

```
client-go/
├── rest/                  # [阶段1] REST 客户端基础层 ★★★
├── tools/
│   ├── clientcmd/         # [阶段1] kubeconfig 加载 ★★
│   ├── cache/             # [阶段3] Informer 核心实现 ★★★★★（最重要）
│   └── leaderelection/    # [阶段4] 领导者选举
├── kubernetes/            # [阶段2] 类型化 Clientset ★★★
│   └── typed/             #         各 API Group 的客户端实现
├── dynamic/               # [阶段2] 动态客户端 ★★
├── discovery/             # [阶段2] API 发现客户端 ★
├── informers/             # [阶段3] 生成的 Informer 工厂 ★★★
├── listers/               # [阶段3] 生成的只读缓存访问器 ★★
├── util/workqueue/        # [阶段4] 工作队列 ★★★★
├── transport/             #         HTTP 传输层与认证
├── applyconfigurations/   #         Server-Side Apply 构建器
├── plugin/                #         认证插件 (GCP, Azure, OIDC)
├── examples/              #         示例程序 ★★★
└── pkg/                   #         内部实现细节
```

> ★ 表示重要程度，★ 越多越关键

---

## 四、阶段 1：基础 —— 如何连接 API Server

### 4.1 需要掌握的概念

| 概念 | 说明 |
|------|------|
| `rest.Config` | 连接 API Server 的所有配置（地址、认证、TLS、限速等） |
| kubeconfig | `~/.kube/config` 文件，包含集群地址和认证信息 |
| In-Cluster Config | Pod 内自动获取 ServiceAccount 配置 |
| QPS / Burst | 客户端限速配置（默认 QPS=5, Burst=10） |

### 4.2 两种连接方式

```go
// 方式一：集群外部（本地开发）
config, err := clientcmd.BuildConfigFromFlags("", "/home/user/.kube/config")

// 方式二：集群内部（Pod 中运行）
config, err := rest.InClusterConfig()
```

### 4.3 源码阅读顺序

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 1 | `rest/config.go` | `Config` 结构体定义，理解所有配置项 |
| 2 | `rest/client.go` | `RESTClient` 接口，`Verb/Get/Post/Put/Delete` 方法 |
| 3 | `rest/request.go` | 链式调用 API：`.Namespace().Resource().Name().Do()` |
| 4 | `tools/clientcmd/client_config.go` | kubeconfig 文件加载与合并逻辑 |

### 4.4 对应示例

- `examples/out-of-cluster-client-configuration/main.go` — 集群外配置
- `examples/in-cluster-client-configuration/main.go` — 集群内配置

---

## 五、阶段 2：核心客户端 —— 三种与 API 交互的方式

### 5.1 Typed Clientset（类型化客户端）★★★

**最常用**。为每个内置资源提供强类型的 CRUD 方法。

```go
clientset, _ := kubernetes.NewForConfig(config)

// 操作 Pod
pod, _ := clientset.CoreV1().Pods("default").Get(ctx, "my-pod", metav1.GetOptions{})

// 操作 Deployment
deploy, _ := clientset.AppsV1().Deployments("default").List(ctx, metav1.ListOptions{})
```

**源码阅读：**

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 1 | `kubernetes/clientset.go` | Clientset 结构，包含 30+ API Group 客户端 |
| 2 | `kubernetes/typed/core/v1/core_client.go` | CoreV1 客户端入口 |
| 3 | `kubernetes/typed/core/v1/pod.go` | Pod 资源的具体 CRUD 实现 |
| 4 | `kubernetes/typed/apps/v1/deployment.go` | Deployment 的 CRUD 实现 |

**示例：** `examples/create-update-delete-deployment/main.go`

### 5.2 Dynamic Client（动态客户端）★★

不需要类型定义，可操作**任何资源**（包括 CRD）。

```go
client, _ := dynamic.NewForConfig(config)

// 通过 GVR（Group-Version-Resource）指定资源
gvr := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
result, _ := client.Resource(gvr).Namespace("default").List(ctx, metav1.ListOptions{})
```

**源码阅读：**

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 1 | `dynamic/interface.go` | 接口定义 |
| 2 | `dynamic/simple.go` | 核心实现，理解 `Unstructured` 类型 |

**示例：** `examples/dynamic-create-update-delete-deployment/main.go`

### 5.3 Discovery Client（发现客户端）★

用于查询集群支持哪些 API 资源。

```go
discoveryClient, _ := discovery.NewDiscoveryClientForConfig(config)
groups, _ := discoveryClient.ServerGroups()           // 所有 API 组
resources, _ := discoveryClient.ServerResourcesForGroupVersion("v1")  // v1 下的资源
```

**源码阅读：** `discovery/discovery_client.go`

### 5.4 三种客户端对比

| 特性 | Typed Clientset | Dynamic Client | Discovery Client |
|------|-----------------|----------------|------------------|
| 类型安全 | 是 | 否（Unstructured） | N/A |
| 支持 CRD | 否（需 code-gen） | 是 | N/A |
| 主要用途 | 内置资源 CRUD | 任意资源 CRUD | 查询 API 能力 |
| 性能 | 最优 | 稍慢（反射） | N/A |

---

## 六、阶段 3：Informer 机制 —— client-go 的灵魂 ★★★★★

> 这是 client-go 最核心、最需要深入理解的部分。Kubernetes 所有控制器的基石。

### 6.1 为什么需要 Informer？

直接轮询 API Server 获取资源状态存在两个问题：
1. **延迟高** —— 轮询间隔内无法感知变化
2. **负载大** —— 大量客户端频繁 LIST 会压垮 API Server

Informer 的解决方案：
- **LIST + WATCH** —— 首次全量拉取，之后增量监听变更
- **本地缓存** —— 读操作直接访问内存缓存，不请求 API Server
- **事件驱动** —— 变更即时通知，延迟极低

### 6.2 核心数据流

这是理解 Informer 的关键图示：

```
                    Kubernetes API Server
                           │
                    LIST + WATCH
                           │
                           ▼
               ┌───────────────────────┐
               │      Reflector        │   tools/cache/reflector.go
               │  (监听 API 变更)       │
               └───────────┬───────────┘
                           │
                    Add/Update/Delete
                           │
                           ▼
               ┌───────────────────────┐
               │      DeltaFIFO        │   tools/cache/delta_fifo.go
               │  (变更事件队列)        │
               └───────────┬───────────┘
                           │
                        Pop()
                           │
                           ▼
               ┌───────────────────────┐
               │    Controller /       │   tools/cache/controller.go
               │    Processor          │   tools/cache/shared_informer.go
               └─────┬─────────┬───────┘
                     │         │
            更新缓存  │         │  分发事件
                     ▼         ▼
            ┌──────────┐  ┌──────────────────────┐
            │ Indexer   │  │ ResourceEventHandler  │
            │ (本地缓存) │  │ OnAdd / OnUpdate /    │
            │           │  │ OnDelete              │
            └──────────┘  └──────────┬───────────┘
                  ↑                  │
                  │           Add(key)
            Lister.Get()             │
                  │                  ▼
                  │         ┌──────────────┐
                  └─────────│  Workqueue   │   util/workqueue/
                   读缓存    │  (工作队列)   │
                            └──────┬───────┘
                                   │
                              Get(key)
                                   │
                                   ▼
                          ┌──────────────────┐
                          │  你的业务逻辑     │
                          │  (Reconcile)     │
                          └──────────────────┘
```

### 6.3 各组件详解与源码

#### (1) Reflector — 反射器

**作用：** 监听 API Server，将变更写入 DeltaFIFO

**源码：** `tools/cache/reflector.go`

**关键逻辑：**
```
1. 调用 ListerWatcher.List() 获取全量数据和 resourceVersion
2. 将全量数据写入 DeltaFIFO（Replace 操作）
3. 调用 ListerWatcher.Watch(resourceVersion) 开始增量监听
4. 收到 ADDED/MODIFIED/DELETED 事件 → 写入 DeltaFIFO
5. Watch 断开 → 重新 LIST（使用最新 resourceVersion）
```

**重点关注：**
- `ListAndWatch()` 方法 — 核心循环
- `watchHandler()` — 处理 Watch 事件
- ResourceVersion 的管理 — 保证不丢事件

#### (2) DeltaFIFO — 增量事件队列

**作用：** 按对象聚合变更事件，保证处理顺序

**源码：** `tools/cache/delta_fifo.go`

**核心数据结构：**
```go
type DeltaFIFO struct {
    items map[string]Deltas  // key → 该对象的所有变更事件
    queue []string           // FIFO 顺序的 key 列表
}

type Delta struct {
    Type   DeltaType    // Added, Updated, Deleted, Replaced, Sync
    Object interface{}  // 变更后的对象
}

type Deltas []Delta  // 同一对象可能积累多个变更
```

**为什么叫 "Delta"？** 因为它不只存储最终状态，而是存储每一个变更步骤。
例如一个 Pod 快速 Add → Update → Update，DeltaFIFO 中会存 3 个 Delta。

**重点关注：**
- `Add/Update/Delete()` — 如何写入变更
- `Pop()` — 如何弹出并处理
- `Replace()` — 全量替换（LIST 后调用）

#### (3) SharedIndexInformer — 共享 Informer

**作用：** 协调 Reflector + DeltaFIFO + Indexer + EventHandler

**源码：** `tools/cache/shared_informer.go`

**关键特性：**
- **共享**：同一资源类型只创建一个 Informer，多个 Handler 共享
- **索引**：支持自定义索引（按 namespace、label 等快速查询）
- **重同步**：定期重新处理所有缓存对象（resync）

**重点关注：**
- `Run()` — 启动 Informer
- `HandleDeltas()` — 处理 DeltaFIFO 弹出的事件
- `AddEventHandler()` — 注册事件处理器

#### (4) Indexer / ThreadSafeStore — 本地缓存

**作用：** 线程安全的内存缓存，支持多维度索引

**源码：** `tools/cache/thread_safe_store.go`, `tools/cache/store.go`

**默认索引：** 按 `namespace/name` 存储对象

```go
// 通过 key 获取对象
obj, exists, err := indexer.GetByKey("default/my-pod")

// 通过索引查询
pods, err := indexer.ByIndex("namespace", "default")
```

#### (5) Lister — 只读访问器

**作用：** 对 Indexer 的类型安全包装，提供只读查询

**源码：** `listers/` 目录（代码生成）

```go
podLister := podInformer.Lister()
pod, err := podLister.Pods("default").Get("my-pod")       // 从缓存读取
pods, err := podLister.Pods("default").List(labels.Everything())  // 列表查询
```

> **关键点：** Lister 从本地缓存读取，**不会**请求 API Server，性能极高。

### 6.4 Informer 源码阅读顺序

| 顺序 | 文件 | 行数 | 阅读重点 |
|------|------|------|---------|
| 1 | `tools/cache/store.go` | ~320 | `Store` 接口，理解缓存抽象 |
| 2 | `tools/cache/index.go` | ~110 | 索引机制的接口定义 |
| 3 | `tools/cache/thread_safe_store.go` | ~350 | 缓存实现，理解索引是如何工作的 |
| 4 | `tools/cache/delta_fifo.go` | ~650 | Delta 和 FIFO 的实现 |
| 5 | `tools/cache/listwatch.go` | ~330 | `ListerWatcher` 接口 |
| 6 | `tools/cache/reflector.go` | ~1500 | 重点读 `ListAndWatch()`，最核心 |
| 7 | `tools/cache/controller.go` | ~800 | Controller 如何协调各组件 |
| 8 | `tools/cache/shared_informer.go` | ~1300 | SharedInformer 完整实现 |

### 6.5 使用 Informer 的标准方式

```go
// 创建 SharedInformerFactory
factory := informers.NewSharedInformerFactory(clientset, 10*time.Minute)

// 获取特定资源的 Informer
podInformer := factory.Core().V1().Pods()

// 注册事件处理器
podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
    AddFunc:    func(obj interface{}) { /* Pod 被创建 */ },
    UpdateFunc: func(old, new interface{}) { /* Pod 被更新 */ },
    DeleteFunc: func(obj interface{}) { /* Pod 被删除 */ },
})

// 启动所有 Informer
factory.Start(ctx.Done())

// 等待缓存同步完成
factory.WaitForCacheSync(ctx.Done())

// 通过 Lister 读取缓存
pod, _ := podInformer.Lister().Pods("default").Get("my-pod")
```

**源码：** `informers/factory.go`

---

## 七、阶段 4：控制器模式 —— 把一切串联起来

### 7.1 Workqueue（工作队列）

**为什么需要 Workqueue？**
- Informer 的事件回调中**不应做耗时操作**（会阻塞后续事件处理）
- 需要**去重**（同一对象短时间多次变更，只处理一次）
- 需要**重试**（处理失败后延迟重试）
- 需要**限速**（避免错误风暴）

**三种队列类型（层层递进）：**

```
Queue (基础队列)
  └── DelayingQueue (延迟队列)
        └── RateLimitingQueue (限速队列) ← 控制器中最常用
```

**源码阅读：**

| 顺序 | 文件 | 关注点 |
|------|------|--------|
| 1 | `util/workqueue/queue.go` | 基础 FIFO + 去重 + Done 标记 |
| 2 | `util/workqueue/delaying_queue.go` | `AddAfter()` 延迟入队 |
| 3 | `util/workqueue/rate_limiting_queue.go` | `AddRateLimited()` 限速重试 |
| 4 | `util/workqueue/default_rate_limiters.go` | 限速算法（指数退避 + 令牌桶） |

**使用方式：**
```go
queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

// 入队（事件回调中）
queue.Add("default/my-pod")

// 出队（Worker 中）
key, quit := queue.Get()
defer queue.Done(key)

// 处理成功 → 清除重试计数
queue.Forget(key)

// 处理失败 → 限速重试
queue.AddRateLimited(key)
```

### 7.2 完整控制器模式

一个标准的 Kubernetes 控制器由以下部分组成：

```
┌────────────────────────────────────────────────────────────┐
│                     Controller                              │
│                                                            │
│  ┌──────────┐    ┌───────────┐    ┌──────────────────┐     │
│  │ Informer │───→│ Workqueue │───→│ Worker (Reconcile)│     │
│  │          │    │           │    │                  │     │
│  │ OnAdd    │    │ Add(key)  │    │ 1. Get(key)     │     │
│  │ OnUpdate │    │           │    │ 2. Lister.Get() │     │
│  │ OnDelete │    │ Get(key)  │    │ 3. 业务逻辑     │     │
│  │          │    │ Done(key) │    │ 4. Done/Retry   │     │
│  └──────────┘    └───────────┘    └──────────────────┘     │
│                                                            │
│  ┌──────────┐                                              │
│  │ Lister   │←── 从缓存读取（不请求 API Server）             │
│  └──────────┘                                              │
└────────────────────────────────────────────────────────────┘
```

### 7.3 必读示例

**`examples/workqueue/main.go`** — 这是一个完整的控制器示例。

关键代码结构：

```go
// 1. 创建 ListerWatcher
podListWatcher := cache.NewListWatchFromClient(
    clientset.CoreV1().RESTClient(), "pods", v1.NamespaceDefault, fields.Everything())

// 2. 创建 Workqueue
queue := workqueue.NewTypedRateLimitingQueue(
    workqueue.DefaultTypedControllerRateLimiter[string]())

// 3. 创建 Informer + 注册事件处理器
indexer, informer := cache.NewIndexerInformer(podListWatcher, &v1.Pod{}, 0,
    cache.ResourceEventHandlerFuncs{
        AddFunc:    func(obj interface{}) { queue.Add(key) },
        UpdateFunc: func(old, new interface{}) { queue.Add(key) },
        DeleteFunc: func(obj interface{}) { queue.Add(key) },
    }, cache.Indexers{})

// 4. Worker 循环处理
func processNextItem() bool {
    key, quit := queue.Get()
    if quit { return false }
    defer queue.Done(key)

    err := syncHandler(key)  // 业务逻辑
    handleErr(err, key)      // 错误处理 + 重试
    return true
}
```

### 7.4 Leader Election（领导者选举）

在高可用部署中，多个控制器副本只能有一个活跃实例。

**源码：** `tools/leaderelection/leaderelection.go`

**示例：** `examples/leader-election/main.go`

---

## 八、完整调用链总结

从创建客户端到控制器运行，完整的调用链如下：

```
kubeconfig 文件
    │
    ▼
clientcmd.BuildConfigFromFlags()      ← tools/clientcmd/
    │
    ▼
rest.Config                           ← rest/config.go
    │
    ├──→ kubernetes.NewForConfig()     ← kubernetes/clientset.go
    │        │
    │        ▼
    │    Clientset.CoreV1().Pods()     ← kubernetes/typed/core/v1/pod.go
    │        │
    │        ▼
    │    RESTClient.Get().Do()         ← rest/request.go
    │
    ├──→ informers.NewSharedInformerFactory()  ← informers/factory.go
    │        │
    │        ▼
    │    factory.Core().V1().Pods()
    │        │
    │        ▼
    │    SharedIndexInformer            ← tools/cache/shared_informer.go
    │        │
    │        ├── Reflector              ← tools/cache/reflector.go
    │        │      └── LIST + WATCH
    │        │
    │        ├── DeltaFIFO             ← tools/cache/delta_fifo.go
    │        │      └── Delta 事件队列
    │        │
    │        ├── Indexer                ← tools/cache/thread_safe_store.go
    │        │      └── 本地缓存
    │        │
    │        └── EventHandler → Workqueue  ← util/workqueue/
    │                              │
    │                              ▼
    │                          Worker → Reconcile（你的业务逻辑）
    │
    └──→ dynamic.NewForConfig()        ← dynamic/simple.go
             └── 操作任意资源（包括 CRD）
```

---

## 九、推荐的源码阅读顺序（完整版）

### 第一轮：理解骨架（先看接口和数据结构）

| 序号 | 文件 | 内容 |
|------|------|------|
| 1 | `doc.go` | 包的总体说明 |
| 2 | `rest/config.go` | Config 结构体 |
| 3 | `rest/client.go` | RESTClient 接口 |
| 4 | `rest/request.go` | 链式请求构建（重点看接口，不用看全部实现） |
| 5 | `kubernetes/clientset.go` | Clientset 是什么 |
| 6 | `kubernetes/typed/core/v1/pod.go` | 一个具体资源客户端长什么样 |

### 第二轮：深入 Informer（最核心）

| 序号 | 文件 | 内容 |
|------|------|------|
| 7 | `tools/cache/store.go` | Store 接口 |
| 8 | `tools/cache/index.go` | Indexer 接口 |
| 9 | `tools/cache/thread_safe_store.go` | 缓存实现 |
| 10 | `tools/cache/listwatch.go` | ListerWatcher 接口 |
| 11 | `tools/cache/delta_fifo.go` | DeltaFIFO 实现 |
| 12 | `tools/cache/reflector.go` | Reflector（重点：`ListAndWatch`） |
| 13 | `tools/cache/controller.go` | 内部 Controller |
| 14 | `tools/cache/shared_informer.go` | SharedInformer 完整流程 |
| 15 | `informers/factory.go` | SharedInformerFactory |

### 第三轮：Workqueue + 控制器

| 序号 | 文件 | 内容 |
|------|------|------|
| 16 | `util/workqueue/queue.go` | 基础队列 |
| 17 | `util/workqueue/delaying_queue.go` | 延迟队列 |
| 18 | `util/workqueue/rate_limiting_queue.go` | 限速队列 |
| 19 | `examples/workqueue/main.go` | 完整控制器示例 |

### 第四轮：进阶主题

| 序号 | 文件 | 内容 |
|------|------|------|
| 20 | `dynamic/simple.go` | 动态客户端 |
| 21 | `discovery/discovery_client.go` | API 发现 |
| 22 | `tools/leaderelection/leaderelection.go` | 领导者选举 |
| 23 | `transport/round_trippers.go` | 认证链 |
| 24 | `tools/clientcmd/client_config.go` | kubeconfig 加载 |

---

## 十、高频面试 / 理解检查问题

用这些问题检验你的理解程度：

1. **Informer 的 LIST-WATCH 机制是如何保证不丢事件的？**
   → 提示：ResourceVersion

2. **DeltaFIFO 和普通 FIFO 有什么区别？为什么需要 Delta？**
   → 提示：同一对象的多次变更、Delete 事件重建

3. **为什么事件回调中不应该做耗时操作？**
   → 提示：回调是同步的，会阻塞整个事件处理管道

4. **Workqueue 的 `Done(key)` 和 `Forget(key)` 有什么区别？**
   → 提示：`Done` 释放处理锁，`Forget` 清除重试计数

5. **SharedInformer 的 "Shared" 体现在哪里？**
   → 提示：同一资源只建立一个 Watch 连接，多个 Handler 共享

6. **Lister 和直接调用 API 有什么区别？**
   → 提示：Lister 读缓存，不请求 API Server

7. **控制器中为什么用 key（namespace/name）而不是传递整个对象？**
   → 提示：入队到处理之间对象可能已变化，应该总是读最新缓存

8. **Leader Election 是如何实现的？**
   → 提示：Lease 对象 + 续约机制

---

## 十一、实践建议

1. **先运行示例**：在本地 kind/minikube 集群中运行 `examples/` 目录下的示例
2. **打断点调试**：在 `reflector.go` 的 `ListAndWatch()` 打断点，观察整个数据流
3. **写一个简单控制器**：监听 Pod 变化并打印日志
4. **逐步增加复杂度**：加入 Workqueue → 加入错误重试 → 加入 Leader Election
5. **阅读真实控制器**：看 `sample-controller`（k8s.io/sample-controller）是如何组织代码的

---

## 十二、常见的代码生成文件（可跳过）

以下文件/目录是代码生成的，初学时可以跳过：

- `kubernetes/typed/` — 所有类型化客户端（由 `client-gen` 生成）
- `informers/` — 所有 Informer 工厂方法（由 `informer-gen` 生成）
- `listers/` — 所有 Lister（由 `lister-gen` 生成）
- `applyconfigurations/` — SSA 配置（由 `applyconfiguration-gen` 生成）

这些文件的头部都有注释：`// Code generated by xxx. DO NOT EDIT.`

理解它们的**模式**即可（看一个 Pod 的实现就够了），不需要逐个阅读。

---

> **总结：** client-go 的核心是 **Informer 机制**（`tools/cache/` 包）。掌握了 Reflector → DeltaFIFO → Indexer → EventHandler → Workqueue 这条数据流，你就理解了 Kubernetes 控制器编程的本质。
