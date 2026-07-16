cmake_minimum_required(VERSION 3.12)
project(vllm_client)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)
find_package(CURL REQUIRED)

if(NOT DEFINED MINITSDB_ROOT OR MINITSDB_ROOT STREQUAL "")
    if(DEFINED ENV{MINITSDB_ROOT})
        set(MINITSDB_ROOT "$ENV{MINITSDB_ROOT}" CACHE PATH "Path to mini-tsdb checkout")
    else()
        set(MINITSDB_ROOT "${CMAKE_SOURCE_DIR}/../../third_party/mini-tsdb" CACHE PATH "Path to mini-tsdb checkout")
    endif()
endif()
set(MINITSDB_CORE_LIB "${MINITSDB_ROOT}/build-linux/libminitsdb_core.a" CACHE FILEPATH "mini-tsdb core static library")
if(NOT EXISTS ${MINITSDB_CORE_LIB})
    message(FATAL_ERROR "mini-tsdb core library not found: ${MINITSDB_CORE_LIB}. Set -DMINITSDB_ROOT=/path/to/mini-tsdb or export MINITSDB_ROOT.")
endif()

include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ../../hybrid-comm/include
    ../../infra-controller/include
    ../../utils
    ${MINITSDB_ROOT}/src
)

add_executable(${PROJECT_NAME}
    src/main.cpp
    src/observability_metrics.cpp
)

target_link_libraries(${PROJECT_NAME}
    PRIVATE
    CURL::libcurl
    ${CMAKE_SOURCE_DIR}/../../install/lib/libstackflow.a
    ${MINITSDB_CORE_LIB}
    zmq
    pthread
)