// CallbackExecutor 实现：固定线程数 + 无界任务队列，只依赖 C++11 标准库。
#include "h_rpc/callback_executor.h"

#include <utility>

namespace h_rpc {

CallbackExecutor::CallbackExecutor(int threads_num) : stop_(false) {
    if (threads_num < 1) {
        threads_num = 1;
    }

    workers_.reserve(static_cast<std::size_t>(threads_num));
    for (int i = 0; i < threads_num; ++i) {
        workers_.emplace_back(&CallbackExecutor::WorkerLoop, this);
    }
}

CallbackExecutor::~CallbackExecutor() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    // 必须持锁改完 stop_ 再唤醒，否则工作线程可能看不到新值又重新睡下去。
    cv_.notify_all();

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void CallbackExecutor::Execute(std::function<void()> task) {
    if (!task) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        tasks_.push(std::move(task));
    }
    cv_.notify_one();
}

void CallbackExecutor::WorkerLoop() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });

            // 只有 stop_ 为 true 时才可能队列为空；此时任务已排空，可以退出。
            if (tasks_.empty()) {
                return;
            }

            task = std::move(tasks_.front());
            tasks_.pop();
        }

        task();
    }
}

}  // namespace h_rpc
