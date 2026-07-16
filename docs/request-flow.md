# Request Flow

## Setup Flow

```text
client setup request
  -> unit_manager::unit_action_match
  -> remote_call(com_id, json)
  -> pzmq("llm").call_rpc_action("setup", set_param(com_url, json))
  -> StackFlow::_rpc_setup
  -> event_queue
  -> StackFlow::_setup
  -> StackFlow::setup(zmq_url, raw)
  -> sys_register_unit("llm")
  -> unit_manager sys RPC allocates work_id, output_url, inference_url
  -> llm_channel_obj(output_url, inference_url, "llm")
  -> vllm_llm::setup(work_id, object, data)
  -> create SessionContext
  -> initialize shared vllm_task if needed
  -> channel.subscriber_work_id("", task_user_data)
```

## Inference Flow

```text
client inference request
  -> unit_manager::unit_action_match
  -> add zmq_com response URL into the JSON request
  -> zmq_bus_publisher_push(work_id, raw)
  -> unit_data::send_msg(raw)
  -> ZMQ PUB sends to inference_url
  -> llm_channel_obj ZMQ_SUB receives raw
  -> subscriber_event_call parses zmq_com/request_id/work_id
  -> vllm_llm::task_user_data(object, data)
  -> optional streaming input assembly
  -> apply session defaults
  -> vllm_task::inference(...)
  -> shared priority queue
  -> worker pool
  -> endpoint router
  -> vLLM /v1/chat/completions
```

## Output Flow

```text
vLLM response
  -> vllm_task output callback
  -> vllm_llm::task_output
  -> send_client_message
  -> llm_channel_obj::send_raw_for_url(output_url, raw)
  -> ZMQ PUSH to the TCP session response URL
  -> unit_manager writes result back to client
```

## Why Control Plane and Data Plane Are Split

Control-plane requests are short lifecycle or management operations. They use RPC because the caller expects a quick acknowledgement.

Inference requests are long-running and may be streamed. They use channel-based asynchronous delivery so that online inference does not block setup, taskinfo, reload, or exit operations.
