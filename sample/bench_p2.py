import argparse
import json
import socket
import statistics
import threading
import time
from pathlib import Path


def send_json(sock, data):
    sock.sendall((json.dumps(data, ensure_ascii=False) + "\n").encode("utf-8"))


class LineReader:
    def __init__(self, sock):
        self.sock = sock
        self.buffer = b""

    def recv_json(self, timeout=120):
        self.sock.settimeout(timeout)
        while b"\n" not in self.buffer:
            chunk = self.sock.recv(4096)
            if not chunk:
                return None
            self.buffer += chunk
        line, self.buffer = self.buffer.split(b"\n", 1)
        if not line.strip():
            return self.recv_json(timeout)
        return json.loads(line.decode("utf-8", errors="ignore"))


def load_json_array(path):
    if not path:
        return None
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{path} must contain a JSON array")
    return data


def load_prompts(path):
    if not path:
        return [
            "Reply with one short sentence about vLLM.",
            "Summarize the value of request routing in one sentence.",
            "Explain queue backpressure in one short sentence.",
        ]
    prompts = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                prompts.append(item.get("prompt") or item.get("text") or line)
            except json.JSONDecodeError:
                prompts.append(line)
    return prompts or ["Reply OK."]


def append_result(results, lock, item):
    with lock:
        results.append(item)


def append_worker_failures(args, worker_id, results, lock, error, stage):
    for i in range(args.requests_per_worker):
        append_result(
            results,
            lock,
            {
                "request_id": f"bench_{worker_id}_{i}",
                "ok": False,
                "error": error,
                "latency_ms": 0,
                "first_token_latency_ms": None,
                "endpoint": None,
                "served_model": None,
                "degraded": False,
                "action": args.action,
                "stage": stage,
            },
        )


def create_setup(args, worker_id):
    data = {
        "model": args.model,
        "response_format": "llm.utf-8.stream",
        "input": "llm.utf-8.stream",
        "enoutput": True,
        "mock": args.mock,
        "max_token_len": args.max_tokens,
        "timeout_ms": args.timeout_ms,
        "deadline_ms": args.deadline_ms,
        "max_retries": args.max_retries,
        "worker_count": args.worker_count,
        "max_queue_size": args.max_queue_size,
        "routing_strategy": args.routing_strategy,
        "fallback_chain": [x.strip() for x in args.fallback_chain.split(",") if x.strip()],
        "prompt": args.system_prompt,
    }
    if args.vllm_base_url:
        data["vllm_base_url"] = args.vllm_base_url
    endpoints = load_json_array(args.endpoints_file)
    if endpoints is not None:
        data["endpoints"] = endpoints
    profiles = load_json_array(args.model_profiles_file)
    if profiles is not None:
        data["model_profiles"] = profiles
    return {
        "request_id": f"bench_setup_{worker_id}",
        "work_id": "llm",
        "action": "setup",
        "object": "llm.setup",
        "data": data,
    }


def run_worker(args, worker_id, prompts, results, lock):
    sock = None
    reader = None
    work_id = None
    try:
        sock = socket.create_connection((args.host, args.port), timeout=20)
        reader = LineReader(sock)
        send_json(sock, create_setup(args, worker_id))
        setup = reader.recv_json(timeout=30)
        if not setup or setup.get("error", {}).get("code") != 0:
            raise RuntimeError(f"setup failed: {setup}")
        work_id = setup.get("work_id")
    except Exception as exc:
        append_worker_failures(
            args,
            worker_id,
            results,
            lock,
            f"{type(exc).__name__}: {exc}",
            "setup",
        )
        if sock:
            sock.close()
        return

    try:
        for i in range(args.requests_per_worker):
            prompt = prompts[(worker_id * args.requests_per_worker + i) % len(prompts)]
            request_id = f"bench_{worker_id}_{i}"
            start = time.time()
            first_delta_at = None
            finish_data = {}
            error = None
            try:
                send_json(
                    sock,
                    {
                        "request_id": request_id,
                        "work_id": work_id,
                        "action": "inference",
                        "object": "llm.utf-8.stream",
                        "data": {
                            "action": args.action,
                            "delta": prompt,
                            "index": 0,
                            "finish": True,
                            "max_tokens": args.max_tokens,
                        },
                    },
                )
                while True:
                    resp = reader.recv_json(timeout=args.deadline_ms / 1000 + 10)
                    if resp is None:
                        error = "connection closed"
                        break
                    code = resp.get("error", {}).get("code", 0)
                    if code != 0:
                        error = resp.get("error", {}).get("message", "error")
                        break
                    data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
                    if data.get("delta") and first_delta_at is None:
                        first_delta_at = time.time()
                    if data.get("finish"):
                        finish_data = data
                        break
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
            end = time.time()
            first_token_ms = finish_data.get("first_token_latency_ms")
            if first_token_ms is None and first_delta_at is not None:
                first_token_ms = int((first_delta_at - start) * 1000)
            item = {
                "request_id": request_id,
                "ok": error is None,
                "error": error,
                "latency_ms": int((end - start) * 1000),
                "first_token_latency_ms": first_token_ms,
                "endpoint": finish_data.get("endpoint"),
                "served_model": finish_data.get("served_model"),
                "degraded": finish_data.get("degraded", False),
                "action": args.action,
            }
            append_result(results, lock, item)
    finally:
        if work_id:
            try:
                send_json(sock, {"request_id": f"bench_exit_{worker_id}", "work_id": work_id, "action": "exit"})
                reader.recv_json(timeout=10)
            except Exception:
                pass
        if sock:
            sock.close()


def percentile(values, pct):
    if not values:
        return 0
    values = sorted(values)
    index = int((len(values) - 1) * pct)
    return values[index]


def summarize(results, elapsed):
    ok = [r for r in results if r["ok"]]
    failed = [r for r in results if not r["ok"]]
    latencies = [r["latency_ms"] for r in ok]
    first_tokens = [r["first_token_latency_ms"] for r in ok if r.get("first_token_latency_ms") is not None]
    degraded = [r for r in ok if r.get("degraded")]
    endpoint_distribution = {}
    served_model_distribution = {}
    failure_reasons = {}
    for item in ok:
        endpoint = item.get("endpoint") or "unknown"
        served_model = item.get("served_model") or "unknown"
        endpoint_distribution[endpoint] = endpoint_distribution.get(endpoint, 0) + 1
        served_model_distribution[served_model] = served_model_distribution.get(served_model, 0) + 1
    for item in failed:
        reason = item.get("error") or "unknown"
        failure_reasons[reason] = failure_reasons.get(reason, 0) + 1
    return {
        "total": len(results),
        "success": len(ok),
        "failed": len(failed),
        "success_rate": len(ok) / len(results) if results else 0,
        "qps": len(results) / elapsed if elapsed > 0 else 0,
        "avg_latency_ms": int(statistics.mean(latencies)) if latencies else 0,
        "p50_latency_ms": percentile(latencies, 0.50),
        "p95_latency_ms": percentile(latencies, 0.95),
        "p99_latency_ms": percentile(latencies, 0.99),
        "first_token_avg_ms": int(statistics.mean(first_tokens)) if first_tokens else 0,
        "first_token_p95_ms": percentile(first_tokens, 0.95),
        "degraded_requests": len(degraded),
        "endpoint_distribution": endpoint_distribution,
        "served_model_distribution": served_model_distribution,
        "failure_reasons": failure_reasons,
    }


def write_report(path, summary, result_path):
    lines = [
        "# VllmRoute P2 Bench Report",
        "",
        f"- Raw result: `{result_path}`",
        f"- Total requests: {summary['total']}",
        f"- Success: {summary['success']}",
        f"- Failed: {summary['failed']}",
        f"- Success rate: {summary['success_rate']:.2%}",
        f"- QPS: {summary['qps']:.2f}",
        f"- Avg latency: {summary['avg_latency_ms']} ms",
        f"- P50 latency: {summary['p50_latency_ms']} ms",
        f"- P95 latency: {summary['p95_latency_ms']} ms",
        f"- P99 latency: {summary['p99_latency_ms']} ms",
        f"- First token avg: {summary['first_token_avg_ms']} ms",
        f"- First token P95: {summary['first_token_p95_ms']} ms",
        f"- Degraded requests: {summary['degraded_requests']}",
        "",
        "## Endpoint Distribution",
        "",
        json.dumps(summary.get("endpoint_distribution", {}), ensure_ascii=False, indent=2),
        "",
        "## Served Model Distribution",
        "",
        json.dumps(summary.get("served_model_distribution", {}), ensure_ascii=False, indent=2),
        "",
    ]
    if summary.get("failure_reasons"):
        lines.extend(
            [
                "## Failure Reasons",
                "",
                json.dumps(summary["failure_reasons"], ensure_ascii=False, indent=2),
                "",
            ]
        )
    Path(path).write_text("\n".join(lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="P2 benchmark for VllmRoute.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=10001)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--requests", type=int, default=10)
    parser.add_argument("--model", default="qwen3-chat")
    parser.add_argument("--action", default="chat")
    parser.add_argument("--vllm-base-url", default="")
    parser.add_argument("--endpoints-file", default="sample/vllm_endpoints_demo.json")
    parser.add_argument("--model-profiles-file", default="sample/vllm_model_profiles_demo.json")
    parser.add_argument("--fallback-chain", default="qwen3-8b,qwen3-4b")
    parser.add_argument("--routing-strategy", default="weighted_least_queue")
    parser.add_argument("--worker-count", type=int, default=2)
    parser.add_argument("--max-queue-size", type=int, default=32)
    parser.add_argument("--timeout-ms", type=int, default=60000)
    parser.add_argument("--deadline-ms", type=int, default=70000)
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument("--max-tokens", type=int, default=64)
    parser.add_argument("--system-prompt", default="You are a concise assistant.")
    parser.add_argument("--prompt-file", default="")
    parser.add_argument("--output", default="/tmp/edge_llm_infra_bench_result.json")
    parser.add_argument("--report", default="/tmp/edge_llm_infra_bench_report.md")
    parser.add_argument("--mock", action="store_true")
    args = parser.parse_args()

    args.concurrency = max(1, min(args.concurrency, 3))
    args.requests_per_worker = max(1, (args.requests + args.concurrency - 1) // args.concurrency)
    prompts = load_prompts(args.prompt_file)
    results = []
    lock = threading.Lock()
    threads = []
    start = time.time()
    for worker_id in range(args.concurrency):
        t = threading.Thread(target=run_worker, args=(args, worker_id, prompts, results, lock), daemon=True)
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - start
    results = results[: args.requests]
    summary = summarize(results, elapsed)
    output = {"summary": summary, "results": results, "elapsed_sec": elapsed}
    Path(args.output).write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(args.report, summary, args.output)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"wrote {args.output}")
    print(f"wrote {args.report}")


if __name__ == "__main__":
    main()
