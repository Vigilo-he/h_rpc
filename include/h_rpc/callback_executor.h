// 基于 C++11 标准库的固定线程数任务池。RpcProvider 用它承载业务方法，
// 避免数据库等待一类的耗时操作阻塞 h_reactor 的 IO 线程。
#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "h_rpc/executor.h"

namespace h_rpc {

// 固定大小线程池。任务在构造时创建的 threads_num 个线程上执行，任务之间
// 无序且不保证公平；队列无界，业务积压时内存会持续增长（没有背压）。
// 生命周期：Execute 可在任意线程并发调用；析构会先拒绝新任务（调用方不得
// 再调用 Execute），再排空已入队任务并 join 所有工作线程，因此析构返回后
// 不会再有任务访问其捕获的对象。
// 调度模型与背压取舍见 docs/DESIGN.md。
class CallbackExecutor : public Executor {
 public:
    // threads_num < 1 时按 1 个线程处理，避免任务永远无人消费。
    explicit CallbackExecutor(int threads_num);
    ~CallbackExecutor() override;

    CallbackExecutor(const CallbackExecutor&) = delete;
    CallbackExecutor& operator=(const CallbackExecutor&) = delete;

    void Execute(std::function<void()> task) override;

 private:
    void WorkerLoop();

    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;

    // 坑：stop_ 必须与 tasks_ 共用 mutex_ 保护。只调用 notify_all 不能与
    // 析构线程建立 happens-before，无锁读写 stop_ 是数据竞争。
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stop_;
};

}  // namespace h_rpc
