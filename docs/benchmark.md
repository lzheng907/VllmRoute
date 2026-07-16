# Benchmark Notes

This document records a benchmark result used to validate the scheduler design.

## Environment Shape

```text
4 vLLM-compatible endpoints
mixed model profiles: chat, reasoning, summary, code
shared scheduler worker_count = 8
endpoint max_concurrency = 5 / 2 / 2 / 2
```

## Result Summary

| Metric | Result |
|---|---:|
| Total requests | 920 |
| Successful requests | 920 |
| Failed requests | 0 |
| Stable RPS | about 10.2 |
| c64 burst success | 384 / 384 |
| c64 P95 TTFT | about 119 ms |
| c64 P95 end-to-end latency | about 6.1 s |
| c64 P95 queue wait | about 5.3 s |
| Throughput improvement | about 90% over the initial config |

## Metric Meaning

- RPS means completed requests per second.
- TTFT means time to first token after backend dispatch.
- End-to-end latency includes middleware queue wait and backend generation.
- Queue wait is the time spent waiting for worker and endpoint capacity.

## Interpretation

The high c64 queue wait shows that the bottleneck has moved toward backend endpoint capacity rather than middleware dispatch overhead. Further capacity gains should come from backend scaling, endpoint concurrency tuning, shorter generation workloads, or more adaptive admission control.
