# Architecture

VllmRoute is split into four layers.

## 1. Access Layer

`unit_manager` accepts TCP JSON requests from clients. It validates `request_id`, `work_id`, and `action`, then decides whether the request is a control-plane action or an inference data-plane action.

Control-plane actions include:

```text
setup
taskinfo
exit
reload_config
metrics_query
```

Inference requests use:

```text
action = inference
```

## 2. Communication Layer

`hybrid-comm` provides `pzmq`, a lightweight wrapper around ZeroMQ.

| Pattern | Project usage |
|---|---|
| REQ/REP | Control-plane RPC, such as `setup`, `taskinfo`, and `reload_config` |
| PUB/SUB | Data-plane inference delivery from `unit_manager` to a work_id channel |
| PUSH/PULL | Point-to-point response delivery back to the TCP session |

`pzmq_data` wraps `zmq_msg_t` and provides a small two-parameter packing helper used by RPC calls. The most common packed payload is:

```text
param0 = response ZMQ URL for the client session
param1 = original JSON request
```

## 3. Business Node Layer

`StackFlow` is the base class for a business node. It registers the common RPC actions and owns the `llm_channel_obj` instances.

`llm_channel_obj` is the per-work_id communication channel. It stores:

```text
request_id
work_id
inference_url
publisher_url
output_url
ZMQ sockets
```

In the vLLM node, `vllm_llm` extends `StackFlow`. During setup, it subscribes the channel to its `inference_url` and binds the callback to `task_user_data()`.

## 4. Scheduler Layer

`vllm_task` is the shared scheduler used by all sessions. It owns:

```text
pending priority queue
worker pool
endpoint runtime state
health checker
circuit breaker
metrics recorder
```

This design avoids creating a full scheduler per user session. Each session keeps lightweight metadata, while all sessions share endpoint capacity and worker resources.
