# 06 - 进阶主题：让控制器生产可用

> 你已经会写控制器了。这一篇讲三个生产级话题：高可用（Leader Election）、HTTP 传输链、测试。

---

## 你会学到什么

- Leader Election：多副本只允许一个活跃
- Transport RoundTripper：HTTP 请求经过了哪些中间件
- Fake Client：如何对控制器做单元测试
- 其他实用包（pager、record、portforward）

---

## 第一部分：Leader Election — 高可用控制器

### 1.1 问题：为什么需要 Leader Election？

你的控制器在生产中通常跑多个副本（保证高可用）。但如果所有副本同时运行业务逻辑，可能导致：
- 重复创建资源
- 竞争条件
- 不必要的 API Server 压力

解决方案：**只让一个副本（Leader）运行业务逻辑，其他副本（Standby）待命。Leader 挂了，Standby 自动接管。**

### 1.2 工作原理

```
                    ┌─────────────┐
                    │ Lease 对象   │    存储在 K8s API Server（etcd）
                    │ (锁)        │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
         ┌────────┐  ┌────────┐  ┌────────┐
         │ 副本 A  │  │ 副本 B  │  │ 副本 C  │
         │ Leader  │  │ Standby│  │ Standby│
         │ 运行中 ✓│  │ 等待中 │  │ 等待中 │
         └────────┘  └────────┘  └────────┘
              │
              │  定期续约
              ▼
         续约失败 → B 或 C 抢到锁 → 成为新 Leader
```

**核心机制：**
1. 副本们竞争更新一个 Lease 对象
2. 抢到的成为 Leader，获得一个"租约"（lease）
3. Leader 必须定期续约（renew）
4. 如果 Leader 在租约到期前没续上（比如进程崩溃），其他副本可以抢到

### 1.3 核心结构

**源码位置：** `tools/leaderelection/leaderelection.go`

```go
// leaderelection.go:116-166
type LeaderElectionConfig struct {
    // 锁资源（通常是 Lease 对象）
    Lock resourcelock.Interface

    // 租约时间：Leader 获得锁后的有效期
    LeaseDuration time.Duration   // 通常 15s

    // 续约截止时间：Leader 必须在这个时间内成功续约
    RenewDeadline time.Duration   // 通常 10s

    // 重试间隔：尝试获取/续约锁的间隔
    RetryPeriod time.Duration     // 通常 2s

    // 回调函数
    Callbacks LeaderCallbacks

    // 是否在 context 取消时主动释放锁
    ReleaseOnCancel bool

    // ...
}

// leaderelection.go:173-185
type LeaderCallbacks struct {
    // 成为 Leader 时调用（在这里启动控制器）
    OnStartedLeading func(context.Context)

    // 失去 Leader 时调用（在这里停止控制器）
    OnStoppedLeading func()

    // 有新 Leader 产生时调用（用于日志）
    OnNewLeader func(identity string)
}
```

### 1.4 使用示例

**参考：** `examples/leader-election/main.go`

```go
import (
    "k8s.io/client-go/tools/leaderelection"
    "k8s.io/client-go/tools/leaderelection/resourcelock"
)

// 创建 Lease 锁
lock := &resourcelock.LeaseLock{
    LeaseMeta: metav1.ObjectMeta{
        Name:      "my-controller-lock",    // 锁的名字
        Namespace: "default",               // 锁所在命名空间
    },
    Client: clientset.CoordinationV1(),
    LockConfig: resourcelock.ResourceLockConfig{
        Identity: hostname,  // 当前副本的唯一标识
    },
}

// 启动 Leader Election
leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
    Lock:            lock,
    LeaseDuration:   15 * time.Second,
    RenewDeadline:   10 * time.Second,
    RetryPeriod:     2 * time.Second,
    ReleaseOnCancel: true,
    Callbacks: leaderelection.LeaderCallbacks{
        OnStartedLeading: func(ctx context.Context) {
            // 成为 Leader，启动控制器
            controller.Run(ctx, 3)
        },
        OnStoppedLeading: func() {
            // 失去 Leader，退出进程
            klog.Info("lost leader, exiting")
            os.Exit(0)
        },
        OnNewLeader: func(identity string) {
            klog.Infof("new leader: %s", identity)
        },
    },
})
```

### 1.5 时间参数的关系

```
|←─────── LeaseDuration (15s) ────────→|
|←──── RenewDeadline (10s) ────→|
|←RetryPeriod (2s)→|

Leader 获得锁
    │
    ├── 每 2s 尝试续约
    ├── 如果 10s 内无法续约 → 认为自己失去 Leader
    └── 其他副本在 15s 后可以尝试获取锁
```

**为什么 RenewDeadline < LeaseDuration？** 这给了一个缓冲区。Leader 在认为自己失去 Leader 后（10s），到其他人能抢锁（15s），中间有 5s 的间隔，避免两个 Leader 同时运行（脑裂）。

### 1.6 源码关键方法

| 方法 | 行号 | 作用 |
|------|------|------|
| `RunOrDie()` | `leaderelection.go:227` | 启动选举（失败直接 Fatal） |
| `Run()` | `leaderelection.go:211` | 启动选举（返回错误） |
| `tryAcquireOrRenew()` | `leaderelection.go:427` | 尝试获取或续约锁 |
| `renew()` | `leaderelection.go:279` | 续约循环 |
| `release()` | `leaderelection.go:310` | 主动释放锁 |
| `IsLeader()` | `leaderelection.go:246` | 检查是否是当前 Leader |

---

## 第二部分：Transport — HTTP 中间件链

### 2.1 RoundTripper 链

每个 HTTP 请求在发送前会经过一系列 `http.RoundTripper` 中间件：

```
你的请求
    │
    ▼
┌──────────────────────┐
│ Impersonation        │  注入 Impersonate-User 头
├──────────────────────┤
│ Auth (Bearer Token)  │  注入 Authorization 头
├──────────────────────┤
│ Rate Limiter         │  客户端限速
├──────────────────────┤
│ Retry (429 handling) │  处理 429 Too Many Requests
├──────────────────────┤
│ Logging / Tracing    │  请求日志和追踪
├──────────────────────┤
│ Default Transport    │  标准 HTTP 发送
└──────────────────────┘
    │
    ▼
API Server
```

**源码位置：** `transport/round_trippers.go`

### 2.2 认证方式

client-go 支持多种认证方式，通过 `rest.Config` 中的字段配置：

| 认证方式 | Config 字段 | 说明 |
|----------|-----------|------|
| Bearer Token | `BearerToken` / `BearerTokenFile` | 最常用（ServiceAccount） |
| 客户端证书 | `TLSClientConfig.CertFile/KeyFile` | mTLS |
| Basic Auth | `Username` / `Password` | 不推荐 |
| Exec Plugin | `ExecProvider` | 调用外部程序获取凭证 |

### 2.3 认证插件

**源码位置：** `plugin/pkg/client/auth/`

```go
// 加载所有认证插件
import _ "k8s.io/client-go/plugin/pkg/client/auth"

// 或只加载特定的
import _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"   // GCP
import _ "k8s.io/client-go/plugin/pkg/client/auth/azure"  // Azure
import _ "k8s.io/client-go/plugin/pkg/client/auth/oidc"   // OIDC
```

---

## 第三部分：测试 — Fake Client

### 3.1 为什么需要 Fake Client？

你不可能每次跑测试都连一个真集群。client-go 提供了 Fake Client，它在内存中模拟 API Server 的行为。

### 3.2 使用方式

**参考：** `examples/fake-client/main_test.go`

```go
import (
    "k8s.io/client-go/kubernetes/fake"
    corev1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMyController(t *testing.T) {
    // 1. 创建 Fake Clientset（可以预置对象）
    clientset := fake.NewSimpleClientset(
        &corev1.Pod{
            ObjectMeta: metav1.ObjectMeta{
                Name:      "existing-pod",
                Namespace: "default",
            },
        },
    )

    // 2. 像真实客户端一样使用
    pod, err := clientset.CoreV1().Pods("default").Get(ctx, "existing-pod", metav1.GetOptions{})
    // pod 不为 nil，err == nil

    // 3. 创建新对象
    newPod := &corev1.Pod{
        ObjectMeta: metav1.ObjectMeta{Name: "new-pod", Namespace: "default"},
    }
    _, err = clientset.CoreV1().Pods("default").Create(ctx, newPod, metav1.CreateOptions{})
    // 成功

    // 4. 可以和 SharedInformerFactory 配合
    factory := informers.NewSharedInformerFactory(clientset, 0)
    podInformer := factory.Core().V1().Pods()
    // ...
}
```

### 3.3 Fake Client 的示例详解

`examples/fake-client/main_test.go` 展示了一个完整的测试模式：

```go
// main_test.go:41 — 创建 Fake Client
clientset := fake.NewSimpleClientset()

// main_test.go:43-56 — 注入自定义 Watch 行为
clientset.PrependWatchReactor("pods", func(action k8stesting.Action) (bool, watch.Interface, error) {
    watcher = watch.NewFake()
    return true, watcher, nil
})

// main_test.go:60 — 创建 Informer Factory
factory := informers.NewSharedInformerFactory(clientset, 0)

// main_test.go:61-68 — 注册事件处理器
factory.Core().V1().Pods().Informer().AddEventHandler(...)

// main_test.go:71-76 — 启动 Informer
factory.Start(ctx.Done())
factory.WaitForCacheSync(ctx.Done())

// main_test.go:88-92 — 通过 Fake Client 创建对象，触发事件
clientset.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})

// main_test.go:94-99 — 验证事件被接收
select {
case <-podCh:
    // 通过
case <-time.After(5 * time.Second):
    t.Error("timeout")
}
```

### 3.4 Fake Client 的局限性

| 局限 | 说明 |
|------|------|
| 不支持 resourceVersion | 不会自动递增版本号 |
| 不支持字段选择器 | `fieldSelector` 不起作用 |
| 不支持 SSA | Server-Side Apply 行为不完整 |
| 无 Webhook 验证 | 不执行 Admission Webhook |

对于复杂测试，考虑使用 `envtest`（controller-runtime 提供）运行一个真正的 API Server。

---

## 第四部分：其他实用包

### 4.1 tools/pager — 分页 LIST

当集群中有大量资源时，一次 LIST 可能返回 MB 级别的数据。Pager 实现自动分页：

```go
import "k8s.io/client-go/tools/pager"

pager := pager.New(func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
    return clientset.CoreV1().Pods("").List(ctx, opts)
})
pager.PageSize = 500  // 每页 500 个

// 自动分页遍历所有 Pod
err := pager.EachListItem(ctx, metav1.ListOptions{}, func(obj runtime.Object) error {
    pod := obj.(*v1.Pod)
    fmt.Println(pod.Name)
    return nil
})
```

### 4.2 tools/record — 事件记录

在 K8s 中记录 Event（`kubectl describe pod` 看到的那些事件）：

```go
import "k8s.io/client-go/tools/record"

broadcaster := record.NewBroadcaster()
broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
    Interface: clientset.CoreV1().Events(""),
})

recorder := broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "my-controller"})

// 记录事件
recorder.Event(pod, v1.EventTypeNormal, "Synced", "Pod synced successfully")
recorder.Eventf(pod, v1.EventTypeWarning, "SyncFailed", "sync failed: %v", err)
```

### 4.3 tools/portforward — 端口转发

程序化实现 `kubectl port-forward`：

**源码位置：** `tools/portforward/`

### 4.4 tools/remotecommand — 远程执行

程序化实现 `kubectl exec`：

**源码位置：** `tools/remotecommand/`

### 4.5 scale/ — 扩缩容

操作资源的 Scale 子资源：

```go
import "k8s.io/client-go/scale"

scaleClient, _ := scale.NewForConfig(config, ...)
currentScale, _ := scaleClient.Scales("default").Get(ctx, gvr, "my-deploy", metav1.GetOptions{})
currentScale.Spec.Replicas = 5
scaleClient.Scales("default").Update(ctx, gvr, currentScale, metav1.UpdateOptions{})
```

---

## 5. 源码阅读清单

| 顺序 | 文件 | 看什么 |
|------|------|--------|
| 1 | `tools/leaderelection/leaderelection.go:116-185` | Config 和 Callbacks 定义 |
| 2 | `tools/leaderelection/leaderelection.go:211-246` | Run/RunOrDie/IsLeader |
| 3 | `tools/leaderelection/leaderelection.go:427` | tryAcquireOrRenew（锁的获取逻辑） |
| 4 | `examples/leader-election/main.go` | 完整示例（171 行） |
| 5 | `examples/fake-client/main_test.go` | 测试示例（101 行） |
| 6 | `transport/round_trippers.go` | HTTP 中间件链（了解即可） |

---

## 6. 自测检查

- [ ] Leader Election 中 LeaseDuration、RenewDeadline、RetryPeriod 分别是什么？
- [ ] 为什么 RenewDeadline 要小于 LeaseDuration？
- [ ] OnStartedLeading 和 OnStoppedLeading 分别在什么时候被调用？
- [ ] Fake Client 怎么创建？能预置初始对象吗？
- [ ] Fake Client 有哪些局限性？
- [ ] HTTP 请求经过了哪些 RoundTripper？

---

## 恭喜！

你已经完成了 client-go 的系统学习。回顾一下你学到了什么：

```
01. 连接 API Server     → rest.Config, kubeconfig, InClusterConfig
02. 三种客户端          → Typed Clientset, Dynamic Client, Discovery Client
03. Informer 机制       → Reflector, DeltaFIFO, Indexer, SharedInformer
04. Workqueue          → 去重, 延迟, 限速, 指数退避
05. 控制器模式          → Informer + Queue + Worker = Controller
06. 进阶主题           → Leader Election, Transport, Fake Client
```

### 下一步建议

1. **写一个真实的控制器** —— 用你学到的知识，监控某种资源的变化并自动响应
2. **读 sample-controller** —— `k8s.io/sample-controller` 是 K8s 官方的控制器示例，比 `examples/workqueue` 更贴近生产
3. **学 controller-runtime** —— 如果你要写 Operator，`controller-runtime`（Kubebuilder 的基础）对 client-go 做了更高层的封装
4. **读真实的控制器源码** —— 比如 Deployment Controller、Job Controller，看 K8s 自己是怎么用 client-go 的
