# Technology Stack Rationale

| Choice | Reason |
|---|---|
| C++17 | Low overhead, stable systems programming, direct control over threading and networking |
| ZeroMQ / pzmq | Lightweight in-process communication library without a separate broker |
| REQ/REP RPC | Suitable for lifecycle actions such as setup, taskinfo, exit, and reload_config |
| PUB/SUB channel | Suitable for async inference delivery to a per-work_id subscriber |
| PUSH/PULL | Suitable for point-to-point response delivery to the active TCP session |
| vLLM OpenAI-compatible API | Reuses a mature LLM serving backend while keeping middleware logic independent |
| libcurl | Mature C/C++ HTTP client used to call vLLM endpoints |
| eventpp | Lightweight event queue used by StackFlow for RPC event dispatch |
| simdjson | Fast JSON parsing on the access path |
| nlohmann/json | Convenient JSON construction and manipulation for control/config bodies |
| mini-tsdb | Lightweight local time-series observability backend for request and endpoint metrics |

Why not Kafka, RabbitMQ, or Redis by default: the project targets low-latency local or LAN inference middleware. It does not need durable message replay, complex consumer groups, or a centralized broker in the current scope. If the system evolves toward multi-region durable task queues, these tools can be reconsidered.
