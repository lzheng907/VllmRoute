# Docker Deployment

VllmRoute provides two Docker modes.

## Mode 1: Mock Mode

Mock mode builds `unit_manager` and the lightweight test node. It is useful for validating the TCP, ZeroMQ, StackFlow, and work_id lifecycle without a real vLLM backend or mini-tsdb.

```bash
./scripts/docker_run.sh mock
```

Then send requests from the host:

```bash
python3 sample/test.py --host 127.0.0.1 --port 10001
```

The container uses host networking, so TCP port `10001` is available on the host.

## Mode 2: Real vLLM Mode

Real mode builds and runs `unit_manager` plus `node/vllm-client`. It requires mini-tsdb source because `vllm_client` links the observability module.

Prepare mini-tsdb:

```bash
mkdir -p third_party
cp -r /path/to/mini-tsdb third_party/mini-tsdb
```

Build and run:

```bash
./scripts/docker_run.sh real
```

In another terminal, replace endpoint placeholders in `sample/vllm_endpoints.example.json`, then run:

```bash
python3 sample/test.py \
  --host 127.0.0.1 \
  --port 10001 \
  --endpoints-file sample/vllm_endpoints.example.json \
  --model-profiles-file sample/vllm_model_profiles.example.json
```

## Build Only

```bash
./scripts/docker_build.sh mock
./scripts/docker_build.sh real
```

## Shell Into a Container

```bash
./scripts/docker_shell.sh mock
./scripts/docker_shell.sh real
```

For an already running container, the compatibility wrappers are also available:

```bash
docker/scripts/llm_docker_into.sh mock
docker/scripts/llm_docker_into.sh real
```

## Why Host Networking

The project uses local IPC sockets and exposes a TCP server on port `10001`. Running both middleware processes in the same container with host networking keeps the deployment simple and avoids cross-container IPC socket sharing issues.

## Network Requirement During Image Build

The Docker build installs system packages and fetches source dependencies such as eventpp and simdjson. If your build machine cannot access the internet, prebuild the image on another machine or mirror those dependencies in your own environment.
