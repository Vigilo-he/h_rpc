// 压测客户端：默认 10 个线程、每线程串行发起 1000 次 Login，统计总耗时与
// QPS。可通过命令行覆盖并发数和每线程请求数，便于比较回调/协程引擎：
//   ./benchmark_client [threads] [requests_per_thread]
// 依赖 RpcChannel 的同步调用语义：stub.Login 返回时 response 已经写完。
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "h_rpc/rpc_channel.h"
#include "user.pb.h"

namespace {

constexpr int kDefaultThreadNum = 10;
constexpr int kDefaultRequestsPerThread = 1000;
constexpr const char* kServerIp = "127.0.0.1";
constexpr int kServerPort = 8000;

struct Stats {
    std::atomic<std::int64_t> success_{0};
    std::atomic<std::int64_t> failure_{0};
};

// 起跑闸门：所有线程就位后统一放行，避免先创建的线程提前发完请求，
// 导致总耗时偏小、QPS 被高估。
struct StartupGate {
    std::mutex mutex_;
    std::condition_variable cv_;
    int ready_{0};
    bool start_{false};
};

// 每个线程持有独立的 channel/stub，线程之间不共享任何可变状态。
void RunWorker(const std::string& ip, int port, int requests_per_thread,
               Stats* stats, StartupGate* gate) {
    h_rpc::RpcChannel channel(ip, port);
    h_rpc::UserServiceRpc_Stub stub(&channel);

    {
        std::unique_lock<std::mutex> lock(gate->mutex_);
        ++gate->ready_;
        gate->cv_.notify_all();
        gate->cv_.wait(lock, [gate] { return gate->start_; });
    }

    h_rpc::LoginRequest request;
    request.set_name("bench");
    request.set_pwd("123456");

    for (int i = 0; i < requests_per_thread; ++i) {
        h_rpc::LoginResponse response;
        // 同步通道会阻塞到响应解析完成；done 传 nullptr 即可。
        stub.Login(nullptr, &request, &response, nullptr);

        if (response.success()) {
            stats->success_.fetch_add(1, std::memory_order_relaxed);
        } else {
            stats->failure_.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

}  // namespace

int main(int argc, char** argv) {
    auto print_usage = [argv]() {
        std::cerr << "用法: " << argv[0]
                  << " [threads] [requests_per_thread]（默认 10 1000）"
                  << std::endl;
    };

    int thread_num = kDefaultThreadNum;
    int requests_per_thread = kDefaultRequestsPerThread;
    if (argc > 3) {
        print_usage();
        return 1;
    }
    try {
        if (argc > 1) {
            thread_num = std::stoi(argv[1]);
        }
        if (argc > 2) {
            requests_per_thread = std::stoi(argv[2]);
        }
    } catch (...) {
        print_usage();
        return 1;
    }
    if (thread_num <= 0 || requests_per_thread <= 0) {
        print_usage();
        return 1;
    }

    Stats stats;
    StartupGate gate;

    std::vector<std::thread> threads;
    threads.reserve(thread_num);
    for (int i = 0; i < thread_num; ++i) {
        threads.emplace_back(RunWorker, std::string(kServerIp), kServerPort,
                             requests_per_thread, &stats, &gate);
    }

    std::chrono::steady_clock::time_point begin;
    {
        std::unique_lock<std::mutex> lock(gate.mutex_);
        gate.cv_.wait(
            lock, [&gate, thread_num] { return gate.ready_ == thread_num; });
        begin = std::chrono::steady_clock::now();
        gate.start_ = true;
        gate.cv_.notify_all();
    }

    for (auto& thread : threads) {
        thread.join();
    }
    const auto end = std::chrono::steady_clock::now();

    const double seconds = std::chrono::duration<double>(end - begin).count();
    const std::int64_t total =
        static_cast<std::int64_t>(thread_num) * requests_per_thread;
    const std::int64_t success = stats.success_.load(std::memory_order_relaxed);
    const std::int64_t failure = stats.failure_.load(std::memory_order_relaxed);
    const double qps =
        seconds > 0.0 ? static_cast<double>(success) / seconds : 0.0;

    std::cout << "=== benchmark result ===" << std::endl;
    std::cout << "线程数: " << thread_num
              << ", 每线程请求数: " << requests_per_thread
              << ", 总请求数: " << total << std::endl;
    std::cout << "成功: " << success << ", 失败: " << failure << std::endl;
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "总耗时: " << seconds << " s" << std::endl;
    std::cout << "QPS: " << std::setprecision(2) << qps << std::endl;

    if (failure != 0) {
        std::cerr << "存在失败请求，请检查服务端是否已启动在 " << kServerIp
                  << ":" << kServerPort << std::endl;
        return 1;
    }
    return 0;
}
