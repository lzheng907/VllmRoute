#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-all}"
START_DELAY="${VLLMROUTE_START_DELAY:-1}"
UNIT_PID=""
NODE_PID=""

mkdir -p /tmp/llm /opt/vllmroute/runtime

cleanup() {
  local code=$?
  if [[ -n "${NODE_PID}" ]] && kill -0 "${NODE_PID}" 2>/dev/null; then
    kill "${NODE_PID}" 2>/dev/null || true
  fi
  if [[ -n "${UNIT_PID}" ]] && kill -0 "${UNIT_PID}" 2>/dev/null; then
    kill "${UNIT_PID}" 2>/dev/null || true
  fi
  wait 2>/dev/null || true
  exit "${code}"
}
trap cleanup INT TERM EXIT

start_unit_manager() {
  cd /opt/vllmroute/unit-manager/build
  ./unit_manager &
  UNIT_PID=$!
}

start_mock_node() {
  cd /opt/vllmroute/node/test/build
  ./test &
  NODE_PID=$!
}

start_vllm_client() {
  if [[ ! -x /opt/vllmroute/node/vllm-client/build/vllm_client ]]; then
    echo "vllm_client binary is not available in this image. Build the 'runtime' target with mini-tsdb." >&2
    exit 1
  fi
  cd /opt/vllmroute/node/vllm-client/build
  ./vllm_client &
  NODE_PID=$!
}

case "${MODE}" in
  all)
    start_unit_manager
    sleep "${START_DELAY}"
    start_vllm_client
    wait -n "${UNIT_PID}" "${NODE_PID}"
    ;;
  mock)
    start_unit_manager
    sleep "${START_DELAY}"
    start_mock_node
    wait -n "${UNIT_PID}" "${NODE_PID}"
    ;;
  unit-manager)
    cd /opt/vllmroute/unit-manager/build
    exec ./unit_manager
    ;;
  vllm-client)
    cd /opt/vllmroute/node/vllm-client/build
    exec ./vllm_client
    ;;
  mock-node|test-node)
    cd /opt/vllmroute/node/test/build
    exec ./test
    ;;
  bash|sh)
    exec "/bin/${MODE}"
    ;;
  *)
    exec "$@"
    ;;
esac
