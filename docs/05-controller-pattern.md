# 05 - 控制器模式：把一切串起来

> 前面学了 REST Client、Clientset、Informer、Workqueue —— 现在把它们组装成一个完整的控制器。

---

## 你会学到什么

- 一个标准 K8s 控制器的完整结构
- 从创建到运行的每一步
- 官方示例 `examples/workqueue/main.go` 的逐行解读
- key 的作用和 MetaNamespaceKeyFunc
- 多 Worker 并发处理
- 优雅关闭

---

## 1. 控制器的完整架构

```
┌──────────────────────────────────────────────────────────┐
│                    Controller                             │
│                                                          │
│  ┌─────────────┐     ┌────────────┐     ┌────────────┐  │
│  │  Informer   │────→│ Workqueue  │────→│  Worker(s) │  │
│  │             │     │            │     │            │  │
│  │ EventHandler│     │ key 队列    │     │ Reconcile  │  │
│  │ → Add(key)  │     │            │     │ 业务逻辑    │  │
│  └─────────────┘     └────────────┘     └────────────┘  │
│        │                                      │          │
│        │            ┌────────────┐            │          │
│        └───────────→│  Lister    │←───────────┘          │
│           注册       │ (本地缓存)  │  读取对象             │
│                     └────────────┘                       │
└──────────────────────────────────────────────────────────┘
```

**数据流：**
1. Informer 监听到 API Server 变更
2. EventHandler 提取 key（`namespace/name`），放入 Workqueue
3. Worker 从 Workqueue 取出 key
4. 通过 Lister 从本地缓存获取对象最新状态
5. 执行业务逻辑（Reconcile）
6. 成功 → Forget + Done；失败 → AddRateLimited + Done

---

## 2. 官方示例逐行解读

**文件：** `examples/workqueue/main.go`（225 行）

这是 client-go 官方提供的控制器示例，我们逐块分析。

### 2.1 Controller 结构体定义

```go
// examples/workqueue/main.go:40-44
type Controller struct {
    indexer  cache.Indexer                               // 本地缓存
    queue    workqueue.TypedRateLimitingInterface[string] // 限速工作队列
    informer cache.Controller                            // Informer 控制器
}
```

三个组成部分：缓存、队列、Informer。这是最简化的控制器结构。

### 2.2 Worker 处理循环

```go
// examples/workqueue/main.go:55-71
func (c *Controller) processNextItem() bool {
    // 阻塞等待，直到队列中有数据
    key, quit := c.queue.Get()
    if quit {
        return false  // 队列已关闭，退出
    }
    // 无论成功失败，都要调用 Done
    defer c.queue.Done(key)

    // 执行业务逻辑
    err := c.syncToStdout(key)
    // 处理错误（重试或放弃）
    c.handleErr(err, key)
    return true
}
```

### 2.3 业务逻辑（syncToStdout）

```go
// examples/workqueue/main.go:76-92
func (c *Controller) syncToStdout(key string) error {
    // 从本地缓存获取对象（不请求 API Server）
    obj, exists, err := c.indexer.GetByKey(key)
    if err != nil {
        return err
    }

    if !exists {
        // 对象已被删除
        fmt.Printf("Pod %s does not exist anymore\n", key)
    } else {
        // 对象存在，执行业务逻辑
        fmt.Printf("Sync/Add/Update for Pod %s\n", obj.(*v1.Pod).GetName())
    }
    return nil
}
```

**注意：** 这里用 `indexer.GetByKey(key)` 而不是调用 API Server。本地缓存总是最新的（因为 Informer 在持续同步）。

### 2.4 错误处理（handleErr）

```go
// examples/workqueue/main.go:95-118
func (c *Controller) handleErr(err error, key string) {
    if err == nil {
        // 成功：清除重试计数
        c.queue.Forget(key)
        return
    }

    // 重试次数 < 5：限速重试
    if c.queue.NumRequeues(key) < 5 {
        klog.Infof("Error syncing pod %v: %v", key, err)
        c.queue.AddRateLimited(key)
        return
    }

    // 超过 5 次：放弃
    c.queue.Forget(key)
    runtime.HandleError(err)
    klog.Infof("Dropping pod %q out of the queue: %v", key, err)
}
```

**这段代码是控制器错误处理的标准模式，几乎所有 K8s 控制器都是这个套路。**

### 2.5 启动控制器（Run）

```go
// examples/workqueue/main.go:121-142
func (c *Controller) Run(ctx context.Context, workers int) {
    defer runtime.HandleCrashWithContext(ctx)  // 捕获 panic
    defer c.queue.ShutDown()                   // 关闭队列

    klog.Info("Starting Pod controller")
    go c.informer.RunWithContext(ctx)           // 启动 Informer

    // 等待缓存同步完成（LIST 全量数据处理完毕）
    if !cache.WaitForNamedCacheSyncWithContext(ctx, c.informer.HasSynced) {
        runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
        return
    }

    // 启动多个 Worker
    for i := 0; i < workers; i++ {
        go wait.UntilWithContext(ctx, c.runWorker, time.Second)
    }

    <-ctx.Done()  // 等待 context 取消
    klog.Info("Stopping Pod controller")
}

func (c *Controller) runWorker(ctx context.Context) {
    for c.processNextItem() {
        // 不断处理队列中的数据，直到队列关闭
    }
}
```

**关键点：**
- 必须等 `WaitForCacheSync` 返回 true 才开始处理，否则缓存不完整
- `wait.UntilWithContext` 会循环调用 `runWorker`，如果 Worker panic 了会自动重启
- context 取消后，Worker 逐步退出

### 2.6 main 函数：组装一切

```go
// examples/workqueue/main.go:149-224
func main() {
    // 1. 创建 REST Config
    config, err := clientcmd.BuildConfigFromFlags(master, kubeconfig)

    // 2. 创建 Clientset
    clientset, err := kubernetes.NewForConfig(config)

    // 3. 创建 ListerWatcher（监听 default 命名空间的 Pod）
    podListWatcher := cache.NewListWatchFromClient(
        clientset.CoreV1().RESTClient(), "pods", v1.NamespaceDefault, fields.Everything())

    // 4. 创建限速工作队列
    queue := workqueue.NewTypedRateLimitingQueue(
        workqueue.DefaultTypedControllerRateLimiter[string]())

    // 5. 创建 Informer + Indexer，绑定事件处理器
    indexer, informer := cache.NewIndexerInformer(
        podListWatcher, &v1.Pod{}, 0,
        cache.ResourceEventHandlerFuncs{
            AddFunc: func(obj interface{}) {
                key, err := cache.MetaNamespaceKeyFunc(obj)
                if err == nil { queue.Add(key) }
            },
            UpdateFunc: func(old interface{}, new interface{}) {
                key, err := cache.MetaNamespaceKeyFunc(new)
                if err == nil { queue.Add(key) }
            },
            DeleteFunc: func(obj interface{}) {
                key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
                if err == nil { queue.Add(key) }
            },
        },
        cache.Indexers{},
    )

    // 6. 创建 Controller
    controller := NewController(queue, indexer, informer)

    // 7. 启动
    controller.Run(ctx, 1)  // 1 个 Worker
}
```

---

## 3. 核心概念：Key

### 什么是 Key？

Key 是对象的唯一标识，格式为 `namespace/name`（如 `default/my-pod`），对于集群级资源则只有 `name`。

### 为什么传 Key 而不是传对象？

```go
// 为什么不这样？
queue.Add(pod)  // 传整个 Pod 对象

// 而是这样？
key, _ := cache.MetaNamespaceKeyFunc(pod)
queue.Add(key)  // 只传 "default/my-pod"
```

**原因：** 从入队到被处理之间可能经过几秒甚至几分钟，这期间对象可能又被修改了多次。传 key 的话，Worker 处理时通过 Lister 获取的总是**最新状态**，而传对象拿到的是入队时的**过时状态**。

### Key 相关函数

```go
// 从对象提取 key："namespace/name"
key, err := cache.MetaNamespaceKeyFunc(obj)

// 从 key 拆回 namespace 和 name
namespace, name, err := cache.SplitMetaNamespaceKey(key)

// 删除事件专用（处理 DeletedFinalStateUnknown 包装）
key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
```

### 小白问答

**Q: DeletionHandlingMetaNamespaceKeyFunc 和 MetaNamespaceKeyFunc 有什么区别？**

A: 当 Reflector 重新 LIST 时，如果发现某个对象在缓存中有但 LIST 结果中没有，说明它被删除了。但此时可能拿不到完整对象，只有一个 `DeletedFinalStateUnknown` 包装。`DeletionHandlingMetaNamespaceKeyFunc` 能正确处理这种情况。

---

## 4. 多 Worker 并发

```go
// 启动 5 个 Worker 并发处理
controller.Run(ctx, 5)
```

5 个 Worker 同时从队列 Get()。Workqueue 保证同一个 key 不会同时被两个 Worker 处理（通过 `processing` 集合）。

```
Worker 1: 处理 "default/pod-a"
Worker 2: 处理 "default/pod-b"
Worker 3: 处理 "kube-system/pod-c"
Worker 4: 阻塞等待（队列暂空）
Worker 5: 阻塞等待（队列暂空）

此时如果 queue.Add("default/pod-a") 被调用：
→ pod-a 正在 processing 中，只加入 dirty
→ Worker 1 Done("default/pod-a") 后，pod-a 会重新入队
→ 某个空闲的 Worker 拿到并处理
```

---

## 5. 优雅关闭

```
context.Cancel()
    │
    ▼
Informer 停止 Watch
    │
    ▼
queue.ShutDown()
    │
    ▼
所有 Worker 的 Get() 返回 quit=true
    │
    ▼
Worker 退出循环
    │
    ▼
控制器停止
```

---

## 6. 完整流程时序图

```
时间 ──────────────────────────────────────────────────────→

API Server    Reflector       DeltaFIFO      Informer        Queue         Worker
    │             │               │             │              │             │
    │  LIST pods  │               │             │              │             │
    │←────────────│               │             │              │             │
    │  [podA,B,C] │               │             │              │             │
    │             │  Replace()    │             │              │             │
    │             │──────────────→│             │              │             │
    │             │               │  Pop()      │              │             │
    │             │               │────────────→│              │             │
    │             │               │             │ HandleDeltas │             │
    │             │               │             │  indexer.Add  │             │
    │             │               │             │  OnAdd()     │             │
    │             │               │             │──────────────│             │
    │             │               │             │ Add("a/podA")│             │
    │             │               │             │              │  Get()      │
    │             │               │             │              │────────────→│
    │             │               │             │              │ "a/podA"    │
    │             │               │             │              │             │
    │             │               │             │              │   indexer   │
    │             │               │             │              │  .GetByKey()│
    │             │               │             │              │   syncHandler
    │             │               │             │              │   Done()    │
    │  WATCH      │               │             │              │   Forget()  │
    │←────────────│               │             │              │             │
    │  MODIFIED   │               │             │              │             │
    │  podA       │               │             │              │             │
    │────────────→│  Update()     │             │              │             │
    │             │──────────────→│             │              │             │
    │             │               │  Pop()      │              │             │
    │             │               │────────────→│  OnUpdate()  │             │
    │             │               │             │──────────────│             │
    │             │               │             │ Add("a/podA")│             │
    │             │               │             │              │  Get()      │
    │             │               │             │              │────────────→│
    ...           ...             ...           ...            ...          ...
```

---

## 7. 另一种写法：使用 SharedInformerFactory

上面的示例用了低层 API（`cache.NewIndexerInformer`）。实际项目中更常用 `SharedInformerFactory`：

```go
// 创建 Factory
factory := informers.NewSharedInformerFactory(clientset, 10*time.Minute)

// 获取 Pod Informer
podInformer := factory.Core().V1().Pods()

// 创建队列
queue := workqueue.NewTypedRateLimitingQueue(
    workqueue.DefaultTypedControllerRateLimiter[string]())

// 注册事件处理器
podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
    AddFunc: func(obj interface{}) {
        key, _ := cache.MetaNamespaceKeyFunc(obj)
        queue.Add(key)
    },
    UpdateFunc: func(old, new interface{}) {
        key, _ := cache.MetaNamespaceKeyFunc(new)
        queue.Add(key)
    },
    DeleteFunc: func(obj interface{}) {
        key, _ := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
        queue.Add(key)
    },
})

// 启动所有 Informer
factory.Start(ctx.Done())
factory.WaitForCacheSync(ctx.Done())

// Worker 中用 Lister 读缓存
pod, err := podInformer.Lister().Pods(namespace).Get(name)
```

**两种写法的区别：**

| | `cache.NewIndexerInformer` | `SharedInformerFactory` |
|---|---|---|
| 适合 | 简单场景、学习 | 生产项目 |
| 共享 | 每次创建新 Informer | 同类型资源共享 Informer |
| Lister | 需要自己操作 Indexer | 提供类型安全的 Lister |
| 多资源 | 每个资源手动创建 | Factory 统一管理 |

---

## 8. 控制器模板（可复制使用）

```go
package main

import (
    "context"
    "fmt"
    "time"

    v1 "k8s.io/api/core/v1"
    "k8s.io/apimachinery/pkg/util/runtime"
    "k8s.io/apimachinery/pkg/util/wait"
    "k8s.io/client-go/informers"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/tools/cache"
    "k8s.io/client-go/tools/clientcmd"
    "k8s.io/client-go/util/workqueue"
)

type Controller struct {
    informerFactory informers.SharedInformerFactory
    podInformer     cache.SharedIndexInformer
    queue           workqueue.TypedRateLimitingInterface[string]
}

func NewController(clientset kubernetes.Interface) *Controller {
    factory := informers.NewSharedInformerFactory(clientset, 10*time.Minute)
    podInformer := factory.Core().V1().Pods().Informer()
    queue := workqueue.NewTypedRateLimitingQueue(
        workqueue.DefaultTypedControllerRateLimiter[string]())

    podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc: func(obj interface{}) {
            key, _ := cache.MetaNamespaceKeyFunc(obj)
            queue.Add(key)
        },
        UpdateFunc: func(old, new interface{}) {
            key, _ := cache.MetaNamespaceKeyFunc(new)
            queue.Add(key)
        },
        DeleteFunc: func(obj interface{}) {
            key, _ := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
            queue.Add(key)
        },
    })

    return &Controller{
        informerFactory: factory,
        podInformer:     podInformer,
        queue:           queue,
    }
}

func (c *Controller) Run(ctx context.Context, workers int) error {
    defer runtime.HandleCrash()
    defer c.queue.ShutDown()

    c.informerFactory.Start(ctx.Done())

    if !cache.WaitForCacheSync(ctx.Done(), c.podInformer.HasSynced) {
        return fmt.Errorf("cache sync failed")
    }

    for i := 0; i < workers; i++ {
        go wait.UntilWithContext(ctx, c.runWorker, time.Second)
    }

    <-ctx.Done()
    return nil
}

func (c *Controller) runWorker(ctx context.Context) {
    for c.processNextItem() {}
}

func (c *Controller) processNextItem() bool {
    key, quit := c.queue.Get()
    if quit { return false }
    defer c.queue.Done(key)

    err := c.reconcile(key)
    if err == nil {
        c.queue.Forget(key)
        return true
    }

    if c.queue.NumRequeues(key) < 5 {
        c.queue.AddRateLimited(key)
        return true
    }

    c.queue.Forget(key)
    runtime.HandleError(err)
    return true
}

func (c *Controller) reconcile(key string) error {
    namespace, name, _ := cache.SplitMetaNamespaceKey(key)
    obj, exists, err := c.podInformer.GetStore().GetByKey(key)
    if err != nil { return err }
    if !exists {
        fmt.Printf("Pod deleted: %s/%s\n", namespace, name)
        return nil
    }
    pod := obj.(*v1.Pod)
    fmt.Printf("Reconcile: %s/%s (Phase: %s)\n", namespace, name, pod.Status.Phase)
    return nil
}

func main() {
    config, _ := clientcmd.BuildConfigFromFlags("", "/home/user/.kube/config")
    clientset, _ := kubernetes.NewForConfig(config)

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    controller := NewController(clientset)
    controller.Run(ctx, 3)
}
```

---

## 9. 源码阅读清单

| 顺序 | 文件 | 看什么 |
|------|------|--------|
| 1 | `examples/workqueue/main.go:40-53` | Controller 结构和构造 |
| 2 | `examples/workqueue/main.go:55-71` | processNextItem（Worker 核心循环） |
| 3 | `examples/workqueue/main.go:76-92` | syncToStdout（业务逻辑） |
| 4 | `examples/workqueue/main.go:95-118` | handleErr（错误处理标准模式） |
| 5 | `examples/workqueue/main.go:121-142` | Run（启动流程） |
| 6 | `examples/workqueue/main.go:149-224` | main（组装所有组件） |

---

## 10. 自测检查

- [ ] 一个控制器需要哪三个核心组件？
- [ ] 为什么 EventHandler 中传 key 而不是传对象？
- [ ] `WaitForCacheSync` 的作用是什么？不等会怎样？
- [ ] 画出一个事件从 API Server 到 Worker 的完整路径
- [ ] `DeletionHandlingMetaNamespaceKeyFunc` 解决什么问题？
- [ ] 多个 Worker 处理同一个 key 时会发生什么？
- [ ] 写出控制器中处理成功和失败时分别调用的方法

全部能答上来？进入下一篇 → [06-advanced-topics.md](06-advanced-topics.md)
