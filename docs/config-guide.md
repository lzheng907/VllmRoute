# Configuration Guide

## unit_manager

`unit-manager/master_config.json` controls the TCP entry port and local ZMQ URL format.

```json
{
  "config_tcp_server": 10001,
  "config_zmq_min_port": 5010,
  "config_zmq_max_port": 5555,
  "config_zmq_s_format": "ipc:///tmp/llm/%i.sock",
  "config_zmq_c_format": "ipc:///tmp/llm/%i.sock"
}
```

## vLLM Endpoints

Use `sample/vllm_endpoints.example.json` as a template.

Important fields:

| Field | Meaning |
|---|---|
| `name` | Stable endpoint name shown in taskinfo and metrics |
| `base_url` | vLLM server root URL, without `/v1/chat/completions` |
| `served_model` | Actual model name served by vLLM |
| `logical_models` | Logical model aliases accepted by the router |
| `role` | `primary`, `overflow`, or `fallback` |
| `weight` | Base routing score |
| `max_context_len` | Context length capacity |
| `max_concurrency` | Per-endpoint middleware concurrency limit |
| `health_path` | Endpoint health check path |

## Model Profiles

Use `sample/vllm_model_profiles.example.json` as a template. A profile maps business actions to model selection and fallback policy.

Example:

```json
{
  "name": "chat",
  "actions": ["chat", "qa"],
  "primary_model": "qwen3-8b",
  "fallback_models": ["qwen3-4b"],
  "default_priority": 6,
  "allow_model_fallback": true
}
```

## Runtime Reload

`reload_config` can update scheduler-side endpoint and model profile settings at runtime. Static fields that affect protocol shape are intentionally protected from unsafe hot changes.
