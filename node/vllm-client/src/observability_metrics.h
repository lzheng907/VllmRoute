#pragma once

#include "json.hpp"
#include "tsdb_engine.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace edge_observability {

using json = nlohmann::json;

struct ObservabilityConfig {
    bool enabled = false;
    std::string db_dir = "runtime/observability-tsdb";
    int flush_interval_ms = 1000;
    bool sync_wal = false;
    int64_t memtable_limit_bytes = 4 * 1024 * 1024;
    size_t block_cache_bytes = 512 * 1024;
    size_t max_samples_per_series = 4096;
};

struct RequestMetricsEvent {
    std::string action;
    std::string endpoint;
    bool ok = false;
    bool timeout = false;
    bool degraded = false;
    int error_code = 0;
    int64_t total_latency_ms = -1;
    int64_t queue_wait_ms = -1;
    int64_t first_token_latency_ms = -1;
};

struct QueueMetricsEvent {
    int pending = 0;
    int high = 0;
    int normal = 0;
    int background = 0;
    int max_queue_size = 0;
};

struct EndpointMetricsEvent {
    std::string endpoint;
    int inflight = 0;
    int available_capacity = 0;
    double avg_latency_ms = 0.0;
    bool healthy = false;
    int circuit_state = 0;
    uint64_t success_count = 0;
    uint64_t failure_count = 0;
    uint64_t timeout_count = 0;
    uint64_t circuit_open_count = 0;
};

class ObservabilityMetrics {
public:
    ObservabilityMetrics() = default;
    ~ObservabilityMetrics();

    ObservabilityMetrics(const ObservabilityMetrics &) = delete;
    ObservabilityMetrics &operator=(const ObservabilityMetrics &) = delete;

    bool configure(const ObservabilityConfig &config, std::string *error_message);
    void stop();

    void observe_request_finish(const RequestMetricsEvent &event);
    void observe_queue_reject(const std::string &action);
    void observe_queue_snapshot(const QueueMetricsEvent &event);
    void observe_endpoint(const EndpointMetricsEvent &event);

    json snapshot() const;
    json query(const json &query_body) const;

private:
    enum class SeriesMode {
        Sum,
        Avg,
        Max,
        Last,
        P95,
    };

    struct SeriesState {
        SeriesMode mode = SeriesMode::Last;
        double sum = 0.0;
        uint64_t count = 0;
        double max = 0.0;
        double last = 0.0;
        std::vector<double> samples;
    };

    void worker_loop();
    void flush_once();
    void add_sum_locked(const std::string &measurement, double value);
    void add_avg_locked(const std::string &measurement, double value);
    void add_max_locked(const std::string &measurement, double value);
    void add_last_locked(const std::string &measurement, double value);
    void add_p95_locked(const std::string &measurement, double value);

    static int64_t wall_now_ms();
    static std::string sanitize_segment(const std::string &value);
    static std::string format_value(double value);
    static bool series_flush_value(const SeriesState &state, double *value);
    static std::vector<std::string> measurements_from_query(const json &query_body);

    std::atomic<bool> enabled_{false};
    std::atomic<bool> running_{false};
    std::atomic<size_t> max_samples_per_series_{4096};

    mutable std::mutex state_mtx_;
    ObservabilityConfig config_;
    int64_t last_flush_ms_ = 0;
    uint64_t last_flush_points_ = 0;
    uint64_t total_flush_points_ = 0;
    uint64_t flush_failures_ = 0;
    std::string last_status_ = "disabled";
    std::string last_error_;

    mutable std::mutex series_mtx_;
    std::unordered_map<std::string, SeriesState> series_;

    mutable std::mutex engine_mtx_;
    std::unique_ptr<minitsdb::TsdbEngine> engine_;

    std::mutex wake_mtx_;
    std::condition_variable wake_cv_;
    std::thread worker_;
};

}  // namespace edge_observability