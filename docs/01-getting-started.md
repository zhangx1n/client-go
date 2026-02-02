# 01 - 连接 API Server：一切的起点

> 在做任何事情之前，你的程序需要先跟 Kubernetes API Server "握手"。这一篇教你怎么建立连接。

---

## 你会学到什么

- `rest.Config` 是什么，里面有哪些关键配置
- 两种连接方式：集群外（开发）vs 集群内（生产）
- kubeconfig 文件的加载过程
- 限速配置（QPS / Burst）的含义

---

## 1. 核心概念：rest.Config

**一句话理解：** `rest.Config` 就是一张"连接 API Server 的名片"，里面写着去哪里连、怎么认证、速度限制是多少。

client-go 中所有客户端（Clientset、Dynamic Client、Discovery Client）都需要一个 `rest.Config` 来初始化。

### 源码位置

`rest/config.go:55-165`

```go
// rest/config.go:55
type Config struct {
    // API Server 地址，如 "https://192.168.1.100:6443"
    Host string

    // API 路径前缀，通常是 "/api" 或 "/apis"
    APIPath string

    // ---- 认证相关 ----
    Username string        // HTTP Basic Auth
    Password string
    BearerToken string     // Token 认证（最常用）
    BearerTokenFile string // 从文件读取 Token

    // TLS 证书配置
    TLSClientConfig TLSClientConfig

    // ---- 限速相关 ----
    QPS   float32           // 每秒请求数上限（默认 5）
    Burst int               // 突发请求数上限（默认 10）

    // ---- 其他 ----
    Timeout time.Duration   // 请求超时时间
    // ...更多字段
}
```

### 小白问答

**Q: 为什么需要 QPS 和 Burst？**

A: 想象 API Server 是一个餐厅，QPS 是你每秒最多点几道菜，Burst 是你瞬间最多能点几道菜。如果很多客户端不限速地疯狂请求，API Server 会被压垮。默认 QPS=5 意味着你的程序每秒最多发 5 个请求。

**Q: 默认的 QPS=5 够用吗？**

A: 对于简单的控制器够了。如果你管理的资源很多（比如几万个 Pod），需要调高：
```go
config.QPS = 100
config.Burst = 200
```

---

## 2. 两种连接方式

### 方式一：集群外连接（本地开发）

你在自己电脑上写代码调试，需要用 `~/.kube/config` 文件连接远程集群。

```go
import "k8s.io/client-go/tools/clientcmd"

config, err := clientcmd.BuildConfigFromFlags("", "/home/user/.kube/config")
if err != nil {
    panic(err)
}
```

**源码位置：** `tools/clientcmd/client_config.go:680`

```go
// tools/clientcmd/client_config.go:680
func BuildConfigFromFlags(masterUrl, kubeconfigPath string) (*restclient.Config, error) {
    // 如果给了 kubeconfig 路径或 masterUrl，就用它们
    // 否则尝试 InClusterConfig（集群内配置）
}
```

**对应示例：** `examples/out-of-cluster-client-configuration/main.go`
- 第 41-47 行：解析 kubeconfig 路径参数
- 第 50-53 行：调用 `BuildConfigFromFlags`

### 方式二：集群内连接（Pod 中运行）

你的程序跑在 K8s Pod 里，K8s 自动挂载了 ServiceAccount 的 Token 和 CA 证书。

```go
import "k8s.io/client-go/rest"

config, err := rest.InClusterConfig()
if err != nil {
    panic(err)
}
```

**源码位置：** `rest/config.go:543`

原理很简单——K8s 会自动把认证信息挂载到 Pod 中的固定路径：
- Token: `/var/run/secrets/kubernetes.io/serviceaccount/token`
- CA 证书: `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`
- API Server 地址: 环境变量 `KUBERNETES_SERVICE_HOST` + `KUBERNETES_SERVICE_PORT`

`InClusterConfig()` 就是读取这些文件和环境变量，拼出一个 `rest.Config`。

**对应示例：** `examples/in-cluster-client-configuration/main.go`
- 第 38-42 行：调用 `InClusterConfig()`

### 两种方式对比

| | 集群外 | 集群内 |
|---|---|---|
| 场景 | 本地开发、调试 | 生产部署（Pod 内运行） |
| 配置来源 | `~/.kube/config` 文件 | ServiceAccount 自动挂载 |
| 函数 | `clientcmd.BuildConfigFromFlags()` | `rest.InClusterConfig()` |
| 认证方式 | kubeconfig 中配置的（证书/Token/OIDC 等） | ServiceAccount Token |

### 通用写法（兼容两种场景）

实际项目中常这样写，先尝试集群内，失败就用 kubeconfig：

```go
config, err := rest.InClusterConfig()
if err != nil {
    // 不在集群内，用 kubeconfig
    config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
    if err != nil {
        panic(err)
    }
}
```

Leader Election 示例 (`examples/leader-election/main.go:38-52`) 就是这个模式。

---

## 3. kubeconfig 文件结构

了解 kubeconfig 文件对理解连接过程很有帮助：

```yaml
apiVersion: v1
kind: Config

# 集群列表：可以配置多个集群
clusters:
- cluster:
    server: https://192.168.1.100:6443   # API Server 地址
    certificate-authority-data: <base64>   # CA 证书
  name: my-cluster

# 用户认证信息
users:
- name: admin
  user:
    client-certificate-data: <base64>     # 客户端证书
    client-key-data: <base64>             # 客户端私钥

# 上下文：将 cluster + user 绑定
contexts:
- context:
    cluster: my-cluster
    user: admin
    namespace: default                     # 默认命名空间
  name: my-context

# 当前使用的上下文
current-context: my-context
```

### 加载流程（源码）

```
kubeconfig 文件
    │
    ▼
tools/clientcmd/loader.go          ← 解析 YAML 文件
    │
    ▼
tools/clientcmd/client_config.go   ← 合并多个 kubeconfig、应用 override
    │
    ▼
rest.Config                        ← 最终输出
```

**相关源码：**
- `tools/clientcmd/client_config.go:60-71` — `ClientConfig` 接口定义
- `tools/clientcmd/client_config.go:88-96` — `DirectClientConfig` 结构体
- `tools/clientcmd/client_config.go:172` — `ClientConfig()` 方法，将 kubeconfig 转为 `rest.Config`

---

## 4. REST 客户端：底层 HTTP 引擎

拿到 `rest.Config` 后，最底层会创建一个 `RESTClient`，它负责实际的 HTTP 通信。

### RESTClient 接口

**源码位置：** `rest/client.go:46-55`

```go
// rest/client.go:46
type Interface interface {
    GetRateLimiter() flowcontrol.RateLimiter
    Verb(verb string) *Request
    Post() *Request
    Put() *Request
    Patch(pt types.PatchType) *Request
    Get() *Request
    Delete() *Request
    APIVersion() schema.GroupVersion
}
```

看起来很简单——就是 HTTP 动词的封装。

### 链式请求构建

RESTClient 最优雅的设计是**链式调用**（Builder 模式）：

**源码位置：** `rest/request.go:96-131`（Request 结构体）

```go
// 获取 default 命名空间下名为 my-pod 的 Pod
result := client.Get().                    // HTTP GET
    Namespace("default").                  // 命名空间         (request.go:352)
    Resource("pods").                      // 资源类型         (request.go:241)
    Name("my-pod").                        // 资源名称         (request.go:331)
    Do(ctx)                                // 执行请求         (request.go:1129)

// 将结果反序列化到 Pod 对象
pod := &v1.Pod{}
err := result.Into(pod)                    //                  (request.go:1449)
```

### 请求执行流程

```
client.Get().Namespace("default").Resource("pods").Name("my-pod").Do(ctx)
    │
    ▼
构造 HTTP 请求: GET /api/v1/namespaces/default/pods/my-pod
    │
    ▼
经过 RateLimiter 限速检查
    │
    ▼
经过 Transport RoundTripper 链（注入认证头等）
    │
    ▼
发送到 API Server
    │
    ▼
返回 Result（包含 body, statusCode, err）
```

---

## 5. 源码阅读清单

按这个顺序看，每个文件只看标注的关键部分：

| 顺序 | 文件 | 看什么 | 预计时间 |
|------|------|--------|---------|
| 1 | `rest/config.go:55-165` | `Config` 结构体的字段 | 10 分钟 |
| 2 | `rest/config.go:543` | `InClusterConfig()` 函数 | 5 分钟 |
| 3 | `rest/client.go:46-55` | `Interface` 接口 | 2 分钟 |
| 4 | `rest/client.go:86-108` | `RESTClient` 结构体 | 5 分钟 |
| 5 | `rest/client.go:210-240` | `Verb/Get/Post/Put/Delete` 方法 | 3 分钟 |
| 6 | `rest/request.go:96-131` | `Request` 结构体 | 5 分钟 |
| 7 | `rest/request.go:1129` | `Do()` 方法 | 10 分钟 |
| 8 | `rest/request.go:1449` | `Into()` 方法 | 5 分钟 |
| 9 | `tools/clientcmd/client_config.go:680` | `BuildConfigFromFlags()` | 5 分钟 |

---

## 6. 自测检查

读完这一篇，你应该能回答：

- [ ] `rest.Config` 中最重要的 3 个字段是什么？
- [ ] 集群内和集群外的认证信息分别从哪里来？
- [ ] `InClusterConfig()` 读取了哪些文件和环境变量？
- [ ] QPS=5, Burst=10 意味着什么？
- [ ] `client.Get().Namespace("x").Resource("pods").Name("y").Do(ctx)` 会生成什么样的 HTTP 请求？
- [ ] `Do()` 和 `DoRaw()` 的区别是什么？

全部能答上来？进入下一篇 → [02-clients.md](02-clients.md)
