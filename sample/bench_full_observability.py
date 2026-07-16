#!/usr/bin/env python3
import argparse
import json
import os
import queue
import socket
import statistics
import threading
import time
from collections import Counter, defaultdict
from pathlib import Path


class LineReader:
    def __init__(self, sock):
        self.sock = sock
        self.buffer = b""

    def recv_json(self, timeout):
        self.sock.settimeout(timeout)
        while b"\n" not in self.buffer:
            chunk = self.sock.recv(65536)
            if not chunk:
                return None
            self.buffer += chunk
        line, self.buffer = self.buffer.split(b"\n", 1)
        if not line.strip():
            return self.recv_json(timeout)
        return json.loads(line.decode("utf-8", errors="ignore"))


def send_json(sock, data):
    sock.sendall((json.dumps(data, ensure_ascii=False) + "\n").encode("utf-8"))


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def percentile(values, pct):
    values = sorted(v for v in values if v is not None)
    if not values:
        return 0
    index = int((len(values) - 1) * pct)
    return values[index]


def mean_int(values):
    values = [v for v in values if v is not None]
    return int(statistics.mean(values)) if values else 0


def now_ms():
    return int(time.time() * 1000)


class TcpSession:
    def __init__(self, host, port, name):
        self.host = host
        self.port = port
        self.name = name
        self.sock = None
        self.reader = None
        self.work_id = None

    def connect(self, timeout=20):
        self.sock = socket.create_connection((self.host, self.port), timeout=timeout)
        self.reader = LineReader(self.sock)

    def setup(self, setup_body, request_id):
        send_json(self.sock, {
            "request_id": request_id,
            "work_id": "llm",
            "action": "setup",
            "object": "llm.setup",
            "data": setup_body,
        })
        resp = self.reader.recv_json(timeout=60)
        if not resp or resp.get("error", {}).get("code") != 0:
            raise RuntimeError(f"setup failed: {resp}")
        self.work_id = resp.get("work_id")
        return resp

    def taskinfo(self, limit=1, timeout=30):
        send_json(self.sock, {
            "request_id": f"taskinfo_{self.name}_{now_ms()}",
            "work_id": self.work_id,
            "action": "taskinfo",
            "object": "llm.taskinfo",
            "data": {"limit": limit},
        })
        return self.reader.recv_json(timeout=timeout)

    def inference(self, spec, timeout):
        request_id = spec["request_id"]
        start = time.time()
        first_delta_at = None
        finish_data = {}
        first_payload = {}
        error_code = 0
        error_message = ""
        chunks = 0
        output_chars = 0
        try:
            send_json(self.sock, {
                "request_id": request_id,
                "work_id": self.work_id,
                "action": "inference",
                "object": "llm.utf-8.stream",
                "data": {
                    "action": spec["action"],
                    "delta": spec["prompt"],
                    "index": 0,
                    "finish": True,
                    "max_tokens": spec["max_tokens"],
                    "priority": spec.get("priority", 5),
                },
            })
            while True:
                resp = self.reader.recv_json(timeout=timeout)
                if resp is None:
                    error_code = -999
                    error_message = "connection closed"
                    break
                if resp.get("request_id") != request_id:
                    # This session sends requests sequentially; ignore unrelated housekeeping responses if any.
                    continue
                err = resp.get("error") or {}
                code = err.get("code", 0)
                if code != 0:
                    error_code = code
                    error_message = err.get("message", "error")
                    break
                data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
                if data and not first_payload:
                    first_payload = data
                delta = data.get("delta") or ""
                if delta:
                    chunks += 1
                    output_chars += len(delta)
                    if first_delta_at is None:
                        first_delta_at = time.time()
                if data.get("finish"):
                    finish_data = data
                    break
        except Exception as exc:
            error_code = -998
            error_message = f"{type(exc).__name__}: {exc}"
        end = time.time()

        first_token_ms = finish_data.get("first_token_latency_ms")
        if first_token_ms is None and first_delta_at is not None:
            first_token_ms = int((first_delta_at - start) * 1000)
        endpoint = finish_data.get("endpoint") or first_payload.get("endpoint")
        served_model = finish_data.get("served_model") or first_payload.get("served_model")
        degraded = finish_data.get("degraded", first_payload.get("degraded", False))
        return {
            "request_id": request_id,
            "phase": spec["phase"],
            "action": spec["action"],
            "ok": error_code == 0,
            "error_code": error_code,
            "error_message": error_message,
            "client_latency_ms": int((end - start) * 1000),
            "server_total_latency_ms": finish_data.get("total_latency_ms"),
            "queue_wait_ms": finish_data.get("queue_wait_ms"),
            "first_token_latency_ms": first_token_ms,
            "endpoint": endpoint,
            "served_model": served_model,
            "degraded": degraded,
            "chunks": chunks,
            "output_chars": output_chars,
            "started_at_ms": int(start * 1000),
            "finished_at_ms": int(end * 1000),
        }

    def close(self):
        if self.sock and self.work_id:
            try:
                send_json(self.sock, {
                    "request_id": f"exit_{self.name}_{now_ms()}",
                    "work_id": self.work_id,
                    "action": "exit",
                })
                self.reader.recv_json(timeout=10)
            except Exception:
                pass
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass


def make_setup_body(args):
    return {
        "model": args.model,
        "response_format": "llm.utf-8.stream",
        "input": "llm.utf-8.stream",
        "enoutput": True,
        "mock": False,
        "max_token_len": args.max_tokens,
        "timeout_ms": args.timeout_ms,
        "deadline_ms": args.deadline_ms,
        "max_retries": args.max_retries,
        "worker_count": args.worker_count,
        "max_queue_size": args.max_queue_size,
        "low_priority_drop_threshold": args.low_priority_drop_threshold,
        "high_priority_threshold": args.high_priority_threshold,
        "routing_strategy": args.routing_strategy,
        "fallback_chain": [x.strip() for x in args.fallback_chain.split(",") if x.strip()],
        "prompt": args.system_prompt,
        "endpoints": load_json(args.endpoints_file),
        "model_profiles": load_json(args.model_profiles_file),
        "health_check_interval_ms": args.health_check_interval_ms,
        "health_check_timeout_ms": args.health_check_timeout_ms,
        "observability": {
            "enabled": True,
            "db_dir": args.observability_db,
            "flush_interval_ms": args.observability_flush_ms,
            "sync_wal": False,
        },
    }


def make_prompt(action, seq):
    if action == "code":
        return f"Write a C++ function that returns the square of an integer. Keep it under 5 lines. Case {seq}."
    if action == "summary":
        return f"Summarize in one sentence: distributed LLM inference needs routing, fallback, and backpressure. Case {seq}."
    if action == "qa":
        return f"Answer briefly: why does queue backpressure help an inference service? Case {seq}."
    return f"Reply with one concise sentence about request routing in LLM serving. Case {seq}."


def action_for_index(actions, index):
    return actions[index % len(actions)]


def build_phase_specs(phase, args):
    actions = [x.strip() for x in phase["actions"].split(",") if x.strip()]
    specs = []
    for i in range(phase["requests"]):
        action = action_for_index(actions, i)
        specs.append({
            "phase": phase["name"],
            "request_id": f"{phase['name']}_{i}_{now_ms()}",
            "action": action,
            "prompt": make_prompt(action, i),
            "max_tokens": phase.get("max_tokens", args.max_tokens),
            "priority": phase.get("priority", 5),
        })
    return specs


def summarize_phase(name, results, elapsed_sec, requested):
    ok = [r for r in results if r.get("ok")]
    failed = [r for r in results if not r.get("ok")]
    client_lat = [r.get("client_latency_ms") for r in ok]
    server_lat = [r.get("server_total_latency_ms") for r in ok]
    queue_wait = [r.get("queue_wait_ms") for r in ok]
    ttft = [r.get("first_token_latency_ms") for r in ok]
    return {
        "phase": name,
        "requested": requested,
        "completed": len(results),
        "success": len(ok),
        "failed": len(failed),
        "success_rate": len(ok) / len(results) if results else 0,
        "elapsed_sec": round(elapsed_sec, 3),
        "attempt_qps": round(len(results) / elapsed_sec, 3) if elapsed_sec > 0 else 0,
        "success_qps": round(len(ok) / elapsed_sec, 3) if elapsed_sec > 0 else 0,
        "client_latency_ms": {
            "avg": mean_int(client_lat),
            "p50": percentile(client_lat, 0.50),
            "p90": percentile(client_lat, 0.90),
            "p95": percentile(client_lat, 0.95),
            "p99": percentile(client_lat, 0.99),
            "max": max(client_lat) if client_lat else 0,
        },
        "server_total_latency_ms": {
            "avg": mean_int(server_lat),
            "p50": percentile(server_lat, 0.50),
            "p95": percentile(server_lat, 0.95),
            "p99": percentile(server_lat, 0.99),
            "max": max([v for v in server_lat if v is not None], default=0),
        },
        "queue_wait_ms": {
            "avg": mean_int(queue_wait),
            "p50": percentile(queue_wait, 0.50),
            "p95": percentile(queue_wait, 0.95),
            "p99": percentile(queue_wait, 0.99),
            "max": max([v for v in queue_wait if v is not None], default=0),
        },
        "first_token_latency_ms": {
            "avg": mean_int(ttft),
            "p50": percentile(ttft, 0.50),
            "p95": percentile(ttft, 0.95),
            "p99": percentile(ttft, 0.99),
            "max": max([v for v in ttft if v is not None], default=0),
        },
        "endpoint_distribution": dict(Counter(r.get("endpoint") or "unknown" for r in ok)),
        "served_model_distribution": dict(Counter(r.get("served_model") or "unknown" for r in ok)),
        "action_distribution": dict(Counter(r.get("action") or "unknown" for r in ok)),
        "degraded_success": sum(1 for r in ok if r.get("degraded")),
        "failure_reasons": dict(Counter(f"{r.get('error_code')}:{r.get('error_message')}" for r in failed)),
    }


def worker_loop(args, setup_body, worker_id, task_queue, results, result_lock):
    sess = TcpSession(args.host, args.port, f"worker{worker_id}")
    try:
        sess.connect()
        sess.setup(setup_body, f"setup_worker_{worker_id}_{now_ms()}")
    except Exception as exc:
        with result_lock:
            results.append({
                "phase": "setup",
                "request_id": f"setup_worker_{worker_id}",
                "action": "setup",
                "ok": False,
                "error_code": -997,
                "error_message": f"{type(exc).__name__}: {exc}",
                "client_latency_ms": 0,
            })
        sess.close()
        return

    try:
        while True:
            try:
                spec = task_queue.get_nowait()
            except queue.Empty:
                break
            item = sess.inference(spec, timeout=args.deadline_ms / 1000 + 30)
            with result_lock:
                results.append(item)
            task_queue.task_done()
    finally:
        sess.close()


def extract_scheduler_snapshot(resp):
    data = resp.get("data") if isinstance(resp, dict) else {}
    scheduler = data.get("shared_scheduler") if isinstance(data, dict) else {}
    if not isinstance(scheduler, dict):
        return {}
    endpoints = scheduler.get("endpoints") or []
    return {
        "ts_ms": now_ms(),
        "status": scheduler.get("status"),
        "pending": scheduler.get("pending"),
        "queue_stats": scheduler.get("queue_stats"),
        "metrics": scheduler.get("metrics"),
        "observability": scheduler.get("observability"),
        "endpoints": [
            {
                "name": ep.get("name"),
                "served_model": ep.get("served_model"),
                "healthy": ep.get("healthy"),
                "circuit_state": ep.get("circuit_state"),
                "inflight": ep.get("inflight"),
                "available_capacity": ep.get("available_capacity"),
                "success_count": ep.get("success_count"),
                "failure_count": ep.get("failure_count"),
                "avg_latency_ms": ep.get("avg_latency_ms"),
            }
            for ep in endpoints
        ],
    }


def write_markdown(path, run):
    lines = [
        "# VllmRoute Full Benchmark Report",
        "",
        f"- Run ID: `{run['run_id']}`",
        f"- Started at: `{run['started_at']}`",
        f"- Host: `{run['host']}:{run['port']}`",
        f"- Observability DB: `{run['observability_db']}`",
        f"- Raw JSON: `{run['raw_json']}`",
        "",
        "## Setup",
        "",
        f"- worker_count: {run['setup']['worker_count']}",
        f"- max_queue_size: {run['setup']['max_queue_size']}",
        f"- timeout_ms/deadline_ms: {run['setup']['timeout_ms']} / {run['setup']['deadline_ms']}",
        f"- max_tokens: {run['setup']['max_token_len']}",
        "",
        "## Phase Summary",
        "",
        "| phase | requested | success | failed | success rate | success QPS | client avg/p95/p99 ms | queue avg/p95 ms | TTFT avg/p95 ms | degraded |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for s in run["phase_summaries"]:
        lines.append(
            f"| {s['phase']} | {s['requested']} | {s['success']} | {s['failed']} | "
            f"{s['success_rate']:.2%} | {s['success_qps']:.2f} | "
            f"{s['client_latency_ms']['avg']}/{s['client_latency_ms']['p95']}/{s['client_latency_ms']['p99']} | "
            f"{s['queue_wait_ms']['avg']}/{s['queue_wait_ms']['p95']} | "
            f"{s['first_token_latency_ms']['avg']}/{s['first_token_latency_ms']['p95']} | "
            f"{s['degraded_success']} |"
        )
    lines.extend(["", "## Endpoint Distribution", ""])
    for s in run["phase_summaries"]:
        lines.append(f"### {s['phase']}")
        lines.append("```json")
        lines.append(json.dumps(s["endpoint_distribution"], ensure_ascii=False, indent=2))
        lines.append("```")
    lines.extend(["", "## Failure Reasons", ""])
    for s in run["phase_summaries"]:
        if s["failure_reasons"]:
            lines.append(f"### {s['phase']}")
            lines.append("```json")
            lines.append(json.dumps(s["failure_reasons"], ensure_ascii=False, indent=2))
            lines.append("```")
    lines.extend(["", "## Final Scheduler Snapshot", "", "```json"])
    lines.append(json.dumps(run.get("final_snapshot", {}), ensure_ascii=False, indent=2))
    lines.append("```")
    Path(path).write_text("\n".join(lines), encoding="utf-8")


def run_phase(args, setup_body, phase):
    specs = build_phase_specs(phase, args)
    q = queue.Queue()
    for spec in specs:
        q.put(spec)
    results = []
    result_lock = threading.Lock()
    threads = []
    start = time.time()
    for worker_id in range(phase["concurrency"]):
        t = threading.Thread(target=worker_loop, args=(args, setup_body, worker_id, q, results, result_lock), daemon=True)
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - start
    return results, summarize_phase(phase["name"], results, elapsed, phase["requests"])


def main():
    parser = argparse.ArgumentParser(description="Full end-to-end benchmark for VllmRoute vLLM backend.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=10001)
    parser.add_argument("--run-id", default=time.strftime("bench_%Y%m%d_%H%M%S"))
    parser.add_argument("--output-dir", default="runtime/bench")
    parser.add_argument("--model", default="qwen3-8b")
    parser.add_argument("--endpoints-file", default="sample/vllm_endpoints_demo.json")
    parser.add_argument("--model-profiles-file", default="sample/vllm_model_profiles_demo.json")
    parser.add_argument("--fallback-chain", default="qwen3-8b,qwen3-4b")
    parser.add_argument("--routing-strategy", default="weighted_least_queue")
    parser.add_argument("--worker-count", type=int, default=4)
    parser.add_argument("--max-queue-size", type=int, default=512)
    parser.add_argument("--low-priority-drop-threshold", type=int, default=480)
    parser.add_argument("--high-priority-threshold", type=int, default=7)
    parser.add_argument("--timeout-ms", type=int, default=120000)
    parser.add_argument("--deadline-ms", type=int, default=150000)
    parser.add_argument("--max-retries", type=int, default=0)
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--system-prompt", default="You are a concise assistant. Keep answers short.")
    parser.add_argument("--health-check-interval-ms", type=int, default=10000)
    parser.add_argument("--health-check-timeout-ms", type=int, default=3000)
    parser.add_argument("--observability-flush-ms", type=int, default=1000)
    parser.add_argument("--phases-json", default="")
    args = parser.parse_args()

    base_dir = Path(args.output_dir) / args.run_id
    base_dir.mkdir(parents=True, exist_ok=True)
    args.observability_db = str(base_dir / "observability-tsdb")

    if args.phases_json:
        phases = json.loads(args.phases_json)
    else:
        phases = [
            {"name": "warmup_chat", "requests": 12, "concurrency": 4, "actions": "chat", "max_tokens": 24},
            {"name": "sustained_chat", "requests": 160, "concurrency": 24, "actions": "chat", "max_tokens": 32},
            {"name": "mixed_actions", "requests": 150, "concurrency": 20, "actions": "chat,summary,code,qa", "max_tokens": 32},
        ]

    setup_body = make_setup_body(args)
    coordinator = TcpSession(args.host, args.port, "coordinator")
    coordinator.connect()
    setup_resp = coordinator.setup(setup_body, f"setup_coordinator_{now_ms()}")
    snapshots = []
    try:
        try:
            snapshots.append(extract_scheduler_snapshot(coordinator.taskinfo(limit=1)))
        except Exception as exc:
            snapshots.append({"ts_ms": now_ms(), "error": f"initial taskinfo failed: {exc}"})

        all_results = []
        phase_summaries = []
        for phase in phases:
            print(f"RUN_PHASE {phase['name']} requests={phase['requests']} concurrency={phase['concurrency']} actions={phase['actions']}", flush=True)
            phase_start_snapshot = {}
            try:
                phase_start_snapshot = extract_scheduler_snapshot(coordinator.taskinfo(limit=1))
            except Exception as exc:
                phase_start_snapshot = {"ts_ms": now_ms(), "error": str(exc)}
            results, summary = run_phase(args, setup_body, phase)
            all_results.extend(results)
            phase_summaries.append(summary)
            time.sleep(2.0)
            phase_end_snapshot = {}
            try:
                phase_end_snapshot = extract_scheduler_snapshot(coordinator.taskinfo(limit=1))
            except Exception as exc:
                phase_end_snapshot = {"ts_ms": now_ms(), "error": str(exc)}
            snapshots.extend([
                {"phase": phase["name"], "point": "start", **phase_start_snapshot},
                {"phase": phase["name"], "point": "end", **phase_end_snapshot},
            ])
            print(json.dumps(summary, ensure_ascii=False), flush=True)

        final_snapshot = {}
        try:
            final_snapshot = extract_scheduler_snapshot(coordinator.taskinfo(limit=5))
        except Exception as exc:
            final_snapshot = {"ts_ms": now_ms(), "error": str(exc)}

        raw_json = str(base_dir / "results.json")
        report_md = str(base_dir / "report.md")
        run = {
            "run_id": args.run_id,
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "host": args.host,
            "port": args.port,
            "observability_db": args.observability_db,
            "raw_json": raw_json,
            "report_md": report_md,
            "setup": setup_body,
            "setup_response": setup_resp,
            "phases": phases,
            "phase_summaries": phase_summaries,
            "snapshots": snapshots,
            "final_snapshot": final_snapshot,
            "results": all_results,
        }
        Path(raw_json).write_text(json.dumps(run, ensure_ascii=False, indent=2), encoding="utf-8")
        write_markdown(report_md, run)
        print(f"RAW_JSON {raw_json}")
        print(f"REPORT_MD {report_md}")
        print(json.dumps({"phase_summaries": phase_summaries, "final_snapshot": final_snapshot}, ensure_ascii=False, indent=2))
    finally:
        coordinator.close()


if __name__ == "__main__":
    main()
