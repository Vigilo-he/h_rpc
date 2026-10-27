cmake_minimum_required(VERSION 3.10)
project(h_rpc)

# 强制使用 C++20 标准
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 1. 查找 Protobuf 并引入头文件
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

# 2. 设置各模块的头文件路径
include_directories(${PROJECT_SOURCE_DIR}/include)
# 引入同级目录的 h_reactor 和 h_coro
include_directories(${PROJECT_SOURCE_DIR}/../h_reactor/include)
include_directories(${PROJECT_SOURCE_DIR}/../h_coro/include)

# 3. 自动编译所有的 .proto 文件
# (注意：这里一定要把 rpc_header.proto 也加上！)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS 
    proto/user.proto 
    proto/rpc_header.proto
)

# 4. 把生成的 .pb.h 所在的 build 目录加入头文件搜索路径
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# 5. 收集 RPC 的核心源文件（新增协程执行器后改成显式列表，避免 GLOB 漏文件）
set(SRC_FILES
    ${PROJECT_SOURCE_DIR}/src/callback_executor.cc
    ${PROJECT_SOURCE_DIR}/src/coro_executor.cc
    ${PROJECT_SOURCE_DIR}/src/rpc_channel.cc
    ${PROJECT_SOURCE_DIR}/src/rpc_provider.cc
)

# 6. 生成 RPC 静态库
# 👇 极其关键：必须把 ${PROTO_SRCS} 一起塞进库里，否则找不到 RpcHeader 的实现
add_library(h_rpc STATIC ${SRC_FILES} ${PROTO_SRCS})

# 7. 链接 Protobuf 和 线程库
target_link_libraries(h_rpc PRIVATE ${Protobuf_LIBRARIES} pthread)

# ==========================================
# 测试程序的编译配置
# ==========================================

# 指定 h_reactor 静态库的绝对路径 (请确保你已经在 h_reactor 里 make 过了)
set(REACTOR_LIB "${PROJECT_SOURCE_DIR}/../h_reactor/build/libh_reactor.a")

# 生成服务端程序
add_executable(rpc_server test/server.cpp)
target_link_libraries(rpc_server PRIVATE h_rpc ${REACTOR_LIB} pthread)

# 生成客户端程序
add_executable(rpc_client test/client.cpp)
target_link_libraries(rpc_client PRIVATE h_rpc pthread)

# 生成压测客户端（默认 10 线程 × 每线程 1000 次 Login，可用命令行覆盖）
add_executable(benchmark_client test/benchmark_client.cpp)
target_link_libraries(benchmark_client PRIVATE h_rpc pthread)