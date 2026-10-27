# h_rpc 设计说明

## 双执行引擎

`RpcProvider` 在 h_reactor 的 IO 线程里只做拆帧、反序列化和方法查找；业务方法
通过 `Executor::Execute` 投递到执行引擎。构造时用 `ExecutorType` 选择引擎：

- `ExecutorType::kCallback`：`CallbackExecutor`，默认 4 个工作线程。业务方法
  阻塞时只占用一个线程，4 线程 / 10ms ≈ 400 QPS 是吞吐上限。
- `ExecutorType::kCoro`：`CoroExecutor`，在 `Run` 拿到的 `EventLoop` 上调度协程。
  业务用 `co_await AsyncSleep(10ms)` 挂起，等待期间不占线程，因此并发请求数
  不再被 4 个线程限制。10 个并发请求 / 10ms ≈ 1000 QPS；100 个并发 ≈ 10000 QPS。

为什么不在 IO 线程直接执行：业务里有 10ms 级等待，阻塞会拖慢同一个 epoll loop
上的所有连接。

## 协程任务生命周期

- `CoroExecutor::Execute` 把 `std::function<void()>` 包成 `h_coro::Task<>`，按
  协程句柄地址登记到 `tasks_`，再投递到 EventLoop 启动。协程一开始处于
  `initial_suspend`，只有 loop 线程会执行它。
- 每次恢复都走 `CoroExecutor::Resume`：设置 thread_local 的 `Current()`，
  `handle.resume()`，返回后若 `handle.done()` 则把 Task 从表里摘除并销毁帧。
- 业务协程用 `Spawn` 启动，由同一个 executor 接管生命周期。

protobuf 生成的 `Service::Login` 签名固定返回 void，所以服务端采用
“void Login 入口 + `h_coro::Task<> DoLogin`”的桥接方式：`Login` 只负责
`CoroExecutor::Current()->Spawn(DoLogin(...))`，真正的 `co_await` 在 `DoLogin` 里。

## AsyncSleep 与定时器

`AsyncSleep` 的 awaiter 在 `await_suspend` 里调用
`CoroExecutor::ResumeAfter(delay, handle)`；后者通过新增的
`EventLoop::RunAfter` 注册定时器。`TimerQueue` 用按 deadline 排序的 multimap
驱动 `epoll_wait` 超时，到期回调在 loop 线程恢复协程。整个过程不阻塞线程、
不创建定时线程。

`AsyncSleep` 只能在 `CoroExecutor` 启动的协程里使用；检测不到 executor 时会
退化为阻塞等待，避免协程永远不被唤醒。

## 队列与背压

`CallbackExecutor` 使用无界 `std::queue`，没有背压：当请求速率长期高于业务处理
能力时，队列长度和内存会持续增长。协程引擎同样没有显式的在途请求上限
（连接数本身就是上限）。后续可以改成有界队列/信号量，队满时直接回包
“服务繁忙”。

## 连接与线程安全

- 一条连接上建议保持“一问一答”。响应由执行引擎线程调用
  `TcpConnection::Send`，h_reactor 会把实际 write 调度回连接所属的 IO 线程，
  避免跨线程操作 epoll。
- h_reactor 侧修复了高连接 churn 下的生命周期问题：读事件延后到 `shared_ptr`
  接管对象后开启、连接表改用自增 id、关闭时先从 poller 摘除再关 fd、连接延迟
  销毁、poller 的 fd 表加锁。原因见 h_reactor 源码中的 `坑：` 注释。
- 压测注意：`h_reactor/src/tcp_connection.cc` 里有逐包 `std::cout` 调试输出，
  会把报文二进制打到终端并显著影响 QPS。压测时把服务端 stdout 重定向到
  `/dev/null`，或删掉这些调试输出。

## QPS 验证

同一个 `benchmark_client`、同一台机器，只切换服务端引擎：

```bash
# 回调版
./build/rpc_server callback > /dev/null 2>&1 &
./build/benchmark_client 10 1000
./build/benchmark_client 100 100

# 协程版（先停掉上一个 server）
./build/rpc_server coro > /dev/null 2>&1 &
./build/benchmark_client 10 1000
./build/benchmark_client 100 100
```

本机实测（每次 10000 请求，全部成功）：

| 服务端 | 客户端线程 | 总请求 | 耗时 | QPS |
|---|---|---|---|---|
| callback | 10 | 10000 | 25.340 s | 394.63 |
| coro | 10 | 10000 | 11.176 s | 894.78 |
| callback | 100 | 10000 | 25.414 s | 393.48 |
| coro | 100 | 10000 | 1.255 s | 7965.06 |

协程路径也跑过 ASan（100 线程 × 20 请求，2000/2000 成功，无 use-after-free
报告），确认协程帧的恢复/回收没有踩内存。

解读：

- 回调版吞吐 ≈ 工作线程数 / 业务耗时 = `4 / 0.01 = 400 QPS`，增加客户端只会
  排队，所以 10 线程和 100 线程都停在 400 左右。
- 协程版吞吐 ≈ 在途请求数 / 业务耗时：10 个并发理论上限 1000 QPS（实测 895），
  100 个并发理论上限 10000 QPS（实测 7965）。协程越多，优势越明显。
- 当前协程引擎只用一个 EventLoop，继续加压会先撞到单 loop 的 CPU 上限；要再
  扩展可以按连接把协程分片到多个 loop/多个 CoroExecutor。
- 压测客户端每次请求都是一条新 TCP 连接（`RpcChannel` 的短连接实现），所以
  高并发下连接建立也是成本；两种引擎承担同样的成本，对比仍然公平。
