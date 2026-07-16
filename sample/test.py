import socket
import json
import argparse


def create_tcp_connection(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    return sock


def send_json(sock, data):
    json_data = json.dumps(data, ensure_ascii=False) + '\n'
    sock.sendall(json_data.encode('utf-8'))


def receive_response(sock):
    response = ''
    while True:
        part = sock.recv(4096).decode('utf-8')
        response += part
        if '\n' in response:
            break
    return response.strip()


def close_connection(sock):
    if sock:
        sock.close()


def create_init_data(args):
    data = {
        "model": args.model,
        "response_format": "llm.utf-8.stream",
        "input": "llm.utf-8.stream",
        "enoutput": True,
        "max_token_len": args.max_tokens,
        "timeout_ms": args.timeout_ms,
        "deadline_ms": args.deadline_ms,
        "max_retries": args.max_retries,
        "prompt": args.prompt
    }
    if args.vllm_base_url:
        data["vllm_base_url"] = args.vllm_base_url
    if args.routing_strategy:
        data["routing_strategy"] = args.routing_strategy
    if args.default_action:
        data["default_action"] = args.default_action
    if args.worker_count:
        data["worker_count"] = args.worker_count
    if args.max_queue_size:
        data["max_queue_size"] = args.max_queue_size
    if args.low_priority_drop_threshold is not None:
        data["low_priority_drop_threshold"] = args.low_priority_drop_threshold
    if args.priority is not None:
        data["priority"] = args.priority
    if args.health_check_interval_ms:
        data["health_check_interval_ms"] = args.health_check_interval_ms
    if args.health_check_timeout_ms:
        data["health_check_timeout_ms"] = args.health_check_timeout_ms
    if args.unhealthy_threshold:
        data["unhealthy_threshold"] = args.unhealthy_threshold
    if args.recovery_threshold:
        data["recovery_threshold"] = args.recovery_threshold
    if args.circuit_failure_threshold:
        data["circuit_failure_threshold"] = args.circuit_failure_threshold
    if args.circuit_open_ms:
        data["circuit_open_ms"] = args.circuit_open_ms
    if args.allow_model_fallback is not None:
        data["allow_model_fallback"] = args.allow_model_fallback
    endpoints = load_endpoints(args)
    if endpoints is not None:
        data["endpoints"] = endpoints
    fallback_chain = load_fallback_chain(args)
    if fallback_chain is not None:
        data["fallback_chain"] = fallback_chain
    model_profiles = load_model_profiles(args)
    if model_profiles is not None:
        data["model_profiles"] = model_profiles
    if args.mock is not None:
        data["mock"] = args.mock

    return {
        "request_id": "llm_001",
        "work_id": "llm",
        "action": "setup",
        "object": "llm.setup",
        "data": data
    }


def load_endpoints(args):
    raw = read_json_source(args.endpoints_json, args.endpoints_file)
    if not raw:
        return None
    value = json.loads(raw)
    if not isinstance(value, list):
        raise ValueError('endpoints config must be a JSON array')
    return value


def load_model_profiles(args):
    raw = read_json_source(args.model_profiles_json, args.model_profiles_file)
    if not raw:
        return None
    value = json.loads(raw)
    if not isinstance(value, list):
        raise ValueError('model profiles config must be a JSON array')
    return value


def read_json_source(raw, path):
    if path:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    return raw


def load_fallback_chain(args):
    if not args.fallback_chain:
        return None
    return [item.strip() for item in args.fallback_chain.split(',') if item.strip()]


def parse_setup_response(response_data, sent_request_id):
    error = response_data.get('error')
    request_id = response_data.get('request_id')

    if request_id != sent_request_id:
        print(f"Request ID mismatch: sent {sent_request_id}, received {request_id}")
        return None

    if error and error.get('code') != 0:
        print(f"Error Code: {error['code']}, Message: {error['message']}")
        return None

    return response_data.get('work_id')


def setup(sock, init_data):
    sent_request_id = init_data['request_id']
    send_json(sock, init_data)
    response = receive_response(sock)
    response_data = json.loads(response)
    return parse_setup_response(response_data, sent_request_id)


def exit_session(sock, deinit_data):
    send_json(sock, deinit_data)
    response = receive_response(sock)
    response_data = json.loads(response)
    print("Exit Response:", response_data)


def taskinfo(sock, work_id, request_id='llm_taskinfo', query=None):
    send_json(sock, {
        "request_id": request_id,
        "work_id": work_id,
        "action": "taskinfo",
        "object": "llm.taskinfo",
        "data": query or {}
    })
    response = receive_response(sock)
    response_data = json.loads(response)
    print("Taskinfo Response:", json.dumps(response_data, ensure_ascii=False, indent=2))
    return response_data


def parse_inference_response(response_data):
    error = response_data.get('error')
    if error and error.get('code') != 0:
        print(f"Error Code: {error['code']}, Message: {error['message']}")
        return None

    return response_data.get('data')


def receive_json_stream(sock):
    """生成器函数，持续返回完整JSON对象"""
    buffer = ""
    while True:
        chunk = sock.recv(4096).decode('utf-8', errors='ignore')
        if not chunk:
            break
        buffer += chunk
        
        while '\n' in buffer:
            line, buffer = buffer.split('\n', 1)
            if line.strip():  
                yield line.strip()

def main(args):
    host = args.host
    port = args.port
    sock = create_tcp_connection(host, port)

    try:
        print("Setup LLM...")
        init_data = create_init_data(args)
        llm_work_id = setup(sock, init_data)
        print("Setup LLM finished.")
        if args.taskinfo_after_setup:
            taskinfo(sock, llm_work_id, query={"limit": args.taskinfo_limit})

        while True:
            try:
                user_input = input("Enter your message (or 'exit' to quit): ")
            except EOFError:
                break
            if user_input.lower() == 'exit':
                break

            send_json(sock, {
                "request_id": "llm_001",
                "work_id": llm_work_id,
                "action": "inference",
                "object": "llm.utf-8.stream",
                "data": {
                    "action": args.action,
                    "delta": user_input,
                    "index": 0,
                    "finish": True,
                    "max_tokens": args.max_tokens
                }
            })

            finished = False
            while not finished:
                for raw_response in receive_json_stream(sock):
              
                    response = json.loads(raw_response)
                    print(f"all response: {response}")
                    delta = response.get("data", {}).get("delta", "")
                    finish = response.get("data", {}).get("finish", False)
                    
                    if delta:
                        print(delta, end='', flush=True)
                        print() 
                    
                    if finish:
                        print()  
                        finished = True
                        break
            if args.taskinfo_after_inference:
                taskinfo(sock, llm_work_id, query={"limit": args.taskinfo_limit})
            
        exit_session(sock, {
            "request_id": "llm_exit",
            "work_id": llm_work_id,
            "action": "exit"
        })
    finally:
        close_connection(sock)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='TCP Client to send JSON data.')
    parser.add_argument('--host', type=str, default='localhost', help='Server hostname (default: localhost)')
    parser.add_argument('--port', type=int, default=10001, help='Server port (default: 10001)')
    parser.add_argument('--model', type=str, default='DeepSeek-R1-Distill-Qwen-1.5B', help='Model name for setup.')
    parser.add_argument('--vllm-base-url', type=str, default='', help='vLLM OpenAI-compatible base URL.')
    parser.add_argument('--timeout-ms', type=int, default=30000, help='vLLM request timeout in milliseconds.')
    parser.add_argument('--deadline-ms', type=int, default=70000, help='End-to-end request deadline in milliseconds.')
    parser.add_argument('--max-retries', type=int, default=0, help='Maximum retry count before returning failure.')
    parser.add_argument('--max-tokens', type=int, default=1023, help='Maximum completion tokens.')
    parser.add_argument('--prompt', type=str, default='You are a knowledgeable assistant capable of answering various questions and providing information.', help='System prompt.')
    parser.add_argument('--endpoints-json', type=str, default='', help='JSON array of vLLM endpoint configs.')
    parser.add_argument('--endpoints-file', type=str, default='', help='Path to a JSON file containing endpoint configs.')
    parser.add_argument('--fallback-chain', type=str, default='', help='Comma-separated fallback chain, for example qwen3-8b,qwen3-4b.')
    parser.add_argument('--model-profiles-json', type=str, default='', help='JSON array of model profile configs.')
    parser.add_argument('--model-profiles-file', type=str, default='', help='Path to a JSON file containing model profile configs.')
    parser.add_argument('--routing-strategy', type=str, default='', help='Routing strategy, default is weighted_least_queue.')
    parser.add_argument('--default-action', type=str, default='', help='Default orchestration action for this session.')
    parser.add_argument('--action', type=str, default='chat', help='Per-request orchestration action, for example chat, summary, code.')
    parser.add_argument('--worker-count', type=int, default=0, help='Worker pool size configured in setup.')
    parser.add_argument('--max-queue-size', type=int, default=0, help='Maximum queued inference requests.')
    parser.add_argument('--low-priority-drop-threshold', type=int, default=None, help='Queue length threshold for low-priority rejection.')
    parser.add_argument('--priority', type=int, default=None, help='Default task priority for this session.')
    parser.add_argument('--health-check-interval-ms', type=int, default=0, help='Health check interval in milliseconds.')
    parser.add_argument('--health-check-timeout-ms', type=int, default=0, help='Health check timeout in milliseconds.')
    parser.add_argument('--unhealthy-threshold', type=int, default=0, help='Consecutive health failures before marking unhealthy.')
    parser.add_argument('--recovery-threshold', type=int, default=0, help='Consecutive successes before marking recovered.')
    parser.add_argument('--circuit-failure-threshold', type=int, default=0, help='Consecutive request failures before opening circuit.')
    parser.add_argument('--circuit-open-ms', type=int, default=0, help='Circuit open duration in milliseconds.')
    parser.add_argument('--taskinfo-after-setup', action='store_true', help='Print taskinfo after setup.')
    parser.add_argument('--taskinfo-after-inference', action='store_true', help='Print taskinfo after each inference.')
    parser.add_argument('--taskinfo-limit', type=int, default=10, help='Recent task count in taskinfo query.')
    fallback_group = parser.add_mutually_exclusive_group()
    fallback_group.add_argument('--allow-model-fallback', dest='allow_model_fallback', action='store_true', help='Allow fallback to a lower capability served model.')
    fallback_group.add_argument('--no-model-fallback', dest='allow_model_fallback', action='store_false', help='Disable model fallback.')
    mock_group = parser.add_mutually_exclusive_group()
    mock_group.add_argument('--mock', dest='mock', action='store_true', help='Force vLLM client mock mode.')
    mock_group.add_argument('--no-mock', dest='mock', action='store_false', help='Disable vLLM client mock mode.')
    parser.set_defaults(mock=None)
    parser.set_defaults(allow_model_fallback=None)

    args = parser.parse_args()
    main(args)
