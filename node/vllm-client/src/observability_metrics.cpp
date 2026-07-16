#include "observability_metrics.h"

#include "tsdb_engine.h"
#include "write_batch.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <sstream>

namespace edge_observability {
namespace {

int64_t json_int64_value(const json &body, const std::string &key, int64_t default_value)
{
    if (!body.is_object() || !body.contains(key)) {
        return default_value;
    }
    const auto &value = body.at(key);
    if (value.is_number_integer()) {
        return value.get<int64_t>();
    }
    if (value.is_number_float()) {
        return static_cast<int64_t>(value.get<double>());
    }
    if (value.is_string()) {
        try {
            return std::stoll(value.get<std::string>());
        } catch (...) {
            return default_value;
        }
    }
    return default_value;
}

std::string json_string_value(const json &body, const std::string &key, const std::string &default_value = "")
{
    if (!body.is_object() || !body.contains(key) || !body.at(key).is_string()) {
        return default_value;
    }
    return body.at(key).get<std::string>();
}

minitsdb::TsdbEngine::Agg parse_agg(const std::string &agg)
{
    if (agg == "avg") {
        return minitsdb::TsdbEngine::Agg::kAvg;
    }
    if (agg == "max") {
        return minitsdb::TsdbEngine::Agg::kMax;
    }
    if (agg == "min") {
        return minitsdb::TsdbEngine::Agg::kMin;
    }
    return minitsdb::TsdbEngine::Agg::kLast;
}

}  // namespace

ObservabilityMetrics::~ObservabilityMetrics()
{
    stop();
}

bool ObservabilityMetrics::configure(const ObservabilityConfig &config, std::string *error_message)
{
    stop();

    ObservabilityConfig normalized = config;
    if (normalized.flush_interval_ms < 100) {
        normalized.flush_interval_ms = 100;
    }
    if (normalized.memtable_limit_bytes <= 0) {
        normalized.memtable_limit_bytes = 4 * 1024 * 1024;
    }
    if (normalized.block_cache_bytes == 0) {
        normalized.block_cache_bytes = 512 * 1024;
    }
    if (normalized.max_samples_per_series == 0) {
        normalized.max_samples_per_series = 4096;
    }
    max_samples_per_series_.store(normalized.max_samples_per_series, std::memory_order_relaxed);

    {
        std::lock_guard<std::mutex> lock(state_mtx_);
        config_ = normalized;
        last_flush_ms_ = 0;
        last_flush_points_ = 0;
        total_flush_points_ = 0;
        flush_failures_ = 0;
        last_error_.clear();
        last_status_ = normalized.enabled ? "starting" : "disabled";
    }

    if (!normalized.enabled) {
        return true;
    }
    if (normalized.db_dir.empty()) {
        const std::string message = "observability db_dir is empty";
        if (error_message != nullptr) {
            *error_message = message;
        }
        std::lock_guard<std::mutex> lock(state_mtx_);
        last_status_ = "error";
        last_error_ = message;
        return false;
    }

    try {
        std::filesystem::create_directories(normalized.db_dir);
        minitsdb::TsdbEngine::Options options;
        options.memtable_limit = normalized.memtable_limit_bytes;
        options.sync_wal = normalized.sync_wal;
        options.block_cache_bytes = normalized.block_cache_bytes;
        auto engine = std::make_unique<minitsdb::TsdbEngine>(normalized.db_dir, options);
        {
            std::lock_guard<std::mutex> lock(engine_mtx_);
            engine_ = std::move(engine);
        }
        enabled_.store(true, std::memory_order_release);
        running_.store(true, std::memory_order_release);
        worker_ = std::thread(&ObservabilityMetrics::worker_loop, this);
    } catch (const std::exception &e) {
        enabled_.store(false, std::memory_order_release);
        running_.store(false, std::memory_order_release);
        if (error_message != nullptr) {
            *error_message = e.what();
        }
        std::lock_guard<std::mutex> lock(state_mtx_);
        last_status_ = "error";
        last_error_ = e.what();
        return false;
    } catch (...) {
        enabled_.store(false, std::memory_order_release);
        running_.store(false, std::memory_order_release);
        const std::string message = "unknown observability initialization error";
        if (error_message != nullptr) {
            *error_message = message;
        }
        std::lock_guard<std::mutex> lock(state_mtx_);
        last_status_ = "error";
        last_error_ = message;
        return false;
    }

    std::lock_guard<std::mutex> lock(state_mtx_);
    last_status_ = "running";
    return true;
}

void ObservabilityMetrics::stop()
{
    enabled_.store(false, std::memory_order_release);
    running_.store(false, std::memory_order_release);
    wake_cv_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }

    flush_once();

    {
        std::lock_guard<std::mutex> lock(engine_mtx_);
        if (engine_) {
            try {
                engine_->Close();
            } catch (...) {
            }
            engine_.reset();
        }
    }
    {
        std::lock_guard<std::mutex> lock(state_mtx_);
        last_status_ = config_.enabled ? "stopped" : "disabled";
    }
}

void ObservabilityMetrics::observe_request_finish(const RequestMetricsEvent &event)
{
    if (!enabled_.load(std::memory_order_acquire)) {
        return;
    }
    const std::string action = sanitize_segment(event.action);
    const std::string endpoint = sanitize_segment(event.endpoint);

    std::lock_guard<std::mutex> lock(series_mtx_);
    add_sum_locked("llm.request.count.action." + action, 1.0);
    if (event.ok) {
        add_sum_locked("llm.request.success.count.action." + action, 1.0);
    } else {
        add_sum_locked("llm.request.error.count.action." + action + ".code." + sanitize_segment(std::to_string(event.error_code)), 1.0);
        if (event.timeout) {
            add_sum_locked("llm.request.timeout.count.action." + action, 1.0);
        }
    }
    if (event.degraded) {
        add_sum_locked("llm.request.degraded.count.action." + action, 1.0);
    }
    if (event.total_latency_ms >= 0) {
        add_avg_locked("llm.request.latency.avg_ms.action." + action, static_cast<double>(event.total_latency_ms));
        add_p95_locked("llm.request.latency.p95_ms.action." + action, static_cast<double>(event.total_latency_ms));
    }
    if (event.queue_wait_ms >= 0) {
        add_avg_locked("llm.request.queue_wait.avg_ms.action." + action, static_cast<double>(event.queue_wait_ms));
        add_p95_locked("llm.request.queue_wait.p95_ms.action." + action, static_cast<double>(event.queue_wait_ms));
    }
    if (event.first_token_latency_ms >= 0) {
        add_avg_locked("llm.request.ttft.avg_ms.action." + action, static_cast<double>(event.first_token_latency_ms));
        add_p95_locked("llm.request.ttft.p95_ms.action." + action, static_cast<double>(event.first_token_latency_ms));
    }
    if (endpoint != "unknown") {
        add_sum_locked("llm.endpoint.request.count." + endpoint, 1.0);
        add_sum_locked(std::string("llm.endpoint.request.") + (event.ok ? "success.count." : "error.count.") + endpoint, 1.0);
    }
}

void ObservabilityMetrics::observe_queue_reject(const std::string &action)
{
    if (!enabled_.load(std::memory_order_acquire)) {
        return;
    }
    std::lock_guard<std::mutex> lock(series_mtx_);
    add_sum_locked("llm.queue.reject.count.action." + sanitize_segment(action), 1.0);
}

void ObservabilityMetrics::observe_queue_snapshot(const QueueMetricsEvent &event)
{
    if (!enabled_.load(std::memory_order_acquire)) {
        return;
    }
    std::lock_guard<std::mutex> lock(series_mtx_);
    add_last_locked("llm.queue.pending", static_cast<double>(event.pending));
    add_last_locked("llm.queue.high", static_cast<double>(event.high));
    add_last_locked("llm.queue.normal", static_cast<double>(event.normal));
    add_last_locked("llm.queue.background", static_cast<double>(event.background));
    add_last_locked("llm.queue.max_size", static_cast<double>(event.max_queue_size));
}

void ObservabilityMetrics::observe_endpoint(const EndpointMetricsEvent &event)
{
    if (!enabled_.load(std::memory_order_acquire)) {
        return;
    }
    const std::string endpoint = sanitize_segment(event.endpoint);
    std::lock_guard<std::mutex> lock(series_mtx_);
    add_last_locked("llm.endpoint.inflight." + endpoint, static_cast<double>(event.inflight));
    add_last_locked("llm.endpoint.available_capacity." + endpoint, static_cast<double>(event.available_capacity));
    add_last_locked("llm.endpoint.avg_latency_ms." + endpoint, event.avg_latency_ms);
    add_last_locked("llm.endpoint.healthy." + endpoint, event.healthy ? 1.0 : 0.0);
    add_last_locked("llm.endpoint.circuit_state." + endpoint, static_cast<double>(event.circuit_state));
    add_last_locked("llm.endpoint.success.total." + endpoint, static_cast<double>(event.success_count));
    add_last_locked("llm.endpoint.failure.total." + endpoint, static_cast<double>(event.failure_count));
    add_last_locked("llm.endpoint.timeout.total." + endpoint, static_cast<double>(event.timeout_count));
    add_last_locked("llm.endpoint.circuit_open.total." + endpoint, static_cast<double>(event.circuit_open_count));
}

json ObservabilityMetrics::snapshot() const
{
    size_t buffered_series = 0;
    {
        std::lock_guard<std::mutex> lock(series_mtx_);
        buffered_series = series_.size();
    }

    std::lock_guard<std::mutex> lock(state_mtx_);
    json body;
    body["enabled"] = enabled_.load(std::memory_order_acquire);
    body["configured"] = config_.enabled;
    body["db_dir"] = config_.db_dir;
    body["flush_interval_ms"] = config_.flush_interval_ms;
    body["sync_wal"] = config_.sync_wal;
    body["memtable_limit_bytes"] = config_.memtable_limit_bytes;
    body["block_cache_bytes"] = config_.block_cache_bytes;
    body["buffered_series"] = buffered_series;
    body["last_flush_ms"] = last_flush_ms_;
    body["last_flush_points"] = last_flush_points_;
    body["total_flush_points"] = total_flush_points_;
    body["flush_failures"] = flush_failures_;
    body["status"] = last_status_;
    if (!last_error_.empty()) {
        body["last_error"] = last_error_;
    }
    return body;
}

json ObservabilityMetrics::query(const json &query_body) const
{
    json out = snapshot();
    out["points"] = json::array();

    const auto measurements = measurements_from_query(query_body);
    if (measurements.empty()) {
        out["error"] = "measurement is required";
        return out;
    }

    const int64_t end_ms = json_int64_value(query_body, "end_ms", json_int64_value(query_body, "end", wall_now_ms()));
    const int64_t lookback_ms = json_int64_value(query_body, "lookback_ms", 10 * 60 * 1000);
    const int64_t start_ms = json_int64_value(query_body, "start_ms", json_int64_value(query_body, "start", end_ms - lookback_ms));
    const int64_t downsample_ms = json_int64_value(query_body, "downsample_ms", 0);
    const std::string agg = json_string_value(query_body, "agg", "last");

    out["start_ms"] = start_ms;
    out["end_ms"] = end_ms;
    out["downsample_ms"] = downsample_ms;
    out["agg"] = agg;
    out["measurements"] = measurements;

    if (end_ms < start_ms) {
        out["error"] = "end_ms must be greater than or equal to start_ms";
        return out;
    }

    std::lock_guard<std::mutex> lock(engine_mtx_);
    if (!engine_) {
        out["error"] = "observability store is not open";
        return out;
    }

    try {
        for (const auto &measurement : measurements) {
            std::vector<minitsdb::Record> records;
            if (downsample_ms > 0) {
                engine_->Downsample(measurement, start_ms, end_ms, downsample_ms, parse_agg(agg), &records);
            } else {
                engine_->RangeQuery(measurement, start_ms, end_ms, &records);
            }
            for (const auto &record : records) {
                json item;
                item["measurement"] = record.measurement;
                item["timestamp_ms"] = record.timestamp;
                item["value"] = record.value;
                out["points"].push_back(item);
            }
        }
    } catch (const std::exception &e) {
        out["error"] = e.what();
    } catch (...) {
        out["error"] = "unknown observability query error";
    }
    return out;
}

void ObservabilityMetrics::worker_loop()
{
    while (running_.load(std::memory_order_acquire)) {
        int interval_ms = 1000;
        {
            std::lock_guard<std::mutex> lock(state_mtx_);
            interval_ms = config_.flush_interval_ms;
        }
        std::unique_lock<std::mutex> lock(wake_mtx_);
        wake_cv_.wait_for(lock, std::chrono::milliseconds(interval_ms), [this]() {
            return !running_.load(std::memory_order_acquire);
        });
        if (!running_.load(std::memory_order_acquire)) {
            break;
        }
        flush_once();
    }
    flush_once();
}

void ObservabilityMetrics::flush_once()
{
    std::unordered_map<std::string, SeriesState> snapshot;
    {
        std::lock_guard<std::mutex> lock(series_mtx_);
        if (series_.empty()) {
            return;
        }
        snapshot.swap(series_);
    }

    minitsdb::WriteBatch batch;
    const int64_t timestamp_ms = wall_now_ms();
    for (const auto &item : snapshot) {
        double value = 0.0;
        if (series_flush_value(item.second, &value)) {
            batch.Put(item.first, timestamp_ms, format_value(value));
        }
    }
    if (batch.Empty()) {
        return;
    }

    bool ok = false;
    std::string status;
    {
        std::lock_guard<std::mutex> lock(engine_mtx_);
        if (!engine_) {
            status = "observability store is not open";
        } else {
            auto write_status = engine_->Write(batch);
            ok = write_status.ok();
            status = write_status.ToString();
        }
    }

    std::lock_guard<std::mutex> lock(state_mtx_);
    last_flush_ms_ = timestamp_ms;
    last_flush_points_ = batch.Count();
    if (ok) {
        total_flush_points_ += batch.Count();
        last_status_ = "running";
        last_error_.clear();
    } else {
        flush_failures_++;
        last_status_ = "error";
        last_error_ = status;
    }
}

void ObservabilityMetrics::add_sum_locked(const std::string &measurement, double value)
{
    auto &series = series_[measurement];
    if (series.count == 0 && series.samples.empty()) {
        series.mode = SeriesMode::Sum;
    }
    if (series.mode != SeriesMode::Sum) {
        series = SeriesState{};
        series.mode = SeriesMode::Sum;
    }
    series.sum += value;
    series.count++;
}

void ObservabilityMetrics::add_avg_locked(const std::string &measurement, double value)
{
    auto &series = series_[measurement];
    if (series.count == 0 && series.samples.empty()) {
        series.mode = SeriesMode::Avg;
    }
    if (series.mode != SeriesMode::Avg) {
        series = SeriesState{};
        series.mode = SeriesMode::Avg;
    }
    series.sum += value;
    series.count++;
}

void ObservabilityMetrics::add_max_locked(const std::string &measurement, double value)
{
    auto &series = series_[measurement];
    if (series.count == 0 && series.samples.empty()) {
        series.mode = SeriesMode::Max;
        series.max = value;
    }
    if (series.mode != SeriesMode::Max) {
        series = SeriesState{};
        series.mode = SeriesMode::Max;
        series.max = value;
    }
    if (series.count == 0 || value > series.max) {
        series.max = value;
    }
    series.count++;
}

void ObservabilityMetrics::add_last_locked(const std::string &measurement, double value)
{
    auto &series = series_[measurement];
    if (series.count == 0 && series.samples.empty()) {
        series.mode = SeriesMode::Last;
    }
    if (series.mode != SeriesMode::Last) {
        series = SeriesState{};
        series.mode = SeriesMode::Last;
    }
    series.last = value;
    series.count++;
}

void ObservabilityMetrics::add_p95_locked(const std::string &measurement, double value)
{
    auto &series = series_[measurement];
    if (series.count == 0 && series.samples.empty()) {
        series.mode = SeriesMode::P95;
    }
    if (series.mode != SeriesMode::P95) {
        series = SeriesState{};
        series.mode = SeriesMode::P95;
    }
    const size_t sample_limit = max_samples_per_series_.load(std::memory_order_relaxed);
    if (series.samples.size() < sample_limit) {
        series.samples.push_back(value);
    }
    series.count++;
}

int64_t ObservabilityMetrics::wall_now_ms()
{
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

std::string ObservabilityMetrics::sanitize_segment(const std::string &value)
{
    std::string out;
    out.reserve(value.empty() ? 7 : value.size());
    for (const unsigned char ch : value) {
        if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-') {
            out.push_back(static_cast<char>(ch));
        } else {
            out.push_back('_');
        }
    }
    if (out.empty()) {
        return "unknown";
    }
    return out;
}

std::string ObservabilityMetrics::format_value(double value)
{
    if (!std::isfinite(value)) {
        return "0";
    }
    const double rounded = std::round(value);
    if (std::fabs(value - rounded) < 0.000001) {
        return std::to_string(static_cast<int64_t>(rounded));
    }
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(3) << value;
    return oss.str();
}

bool ObservabilityMetrics::series_flush_value(const SeriesState &state, double *value)
{
    if (state.count == 0 && state.samples.empty()) {
        return false;
    }
    switch (state.mode) {
        case SeriesMode::Sum:
            *value = state.sum;
            return true;
        case SeriesMode::Avg:
            if (state.count == 0) {
                return false;
            }
            *value = state.sum / static_cast<double>(state.count);
            return true;
        case SeriesMode::Max:
            *value = state.max;
            return true;
        case SeriesMode::Last:
            *value = state.last;
            return true;
        case SeriesMode::P95: {
            if (state.samples.empty()) {
                return false;
            }
            std::vector<double> sorted = state.samples;
            std::sort(sorted.begin(), sorted.end());
            size_t index = static_cast<size_t>(0.95 * static_cast<double>(sorted.size() - 1));
            if (index >= sorted.size()) {
                index = sorted.size() - 1;
            }
            *value = sorted[index];
            return true;
        }
    }
    return false;
}

std::vector<std::string> ObservabilityMetrics::measurements_from_query(const json &query_body)
{
    std::vector<std::string> measurements;
    if (!query_body.is_object()) {
        return measurements;
    }
    if (query_body.contains("measurement") && query_body["measurement"].is_string()) {
        measurements.push_back(query_body["measurement"].get<std::string>());
    }
    if (query_body.contains("measurements") && query_body["measurements"].is_array()) {
        for (const auto &item : query_body["measurements"]) {
            if (item.is_string()) {
                measurements.push_back(item.get<std::string>());
            }
        }
    }
    measurements.erase(std::remove_if(measurements.begin(), measurements.end(), [](const std::string &value) {
        return value.empty();
    }), measurements.end());
    std::sort(measurements.begin(), measurements.end());
    measurements.erase(std::unique(measurements.begin(), measurements.end()), measurements.end());
    return measurements;
}

}  // namespace edge_observability