/*
 * SPDX-FileCopyrightText: 2024 M5Stack Technology CO LTD
 *
 * SPDX-License-Identifier: MIT
 */
#include "StackFlow.h"
#include "channel.h"
#include "observability_metrics.h"

#include <curl/curl.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <cstring>
#include <deque>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace StackFlows;
using json = nlohmann::json;

int main_exit_flage = 0;

static void raise_open_file_limit(rlim_t target)
{
    struct rlimit limit;
    if (getrlimit(RLIMIT_NOFILE, &limit) != 0) {
        std::cerr << "getrlimit(RLIMIT_NOFILE) failed: " << std::strerror(errno) << std::endl;
        return;
    }

    if (limit.rlim_cur >= target) {
        return;
    }

    struct rlimit updated = limit;
    updated.rlim_cur = target;
    if (updated.rlim_max != RLIM_INFINITY && updated.rlim_cur > updated.rlim_max) {
        updated.rlim_cur = updated.rlim_max;
    }
    if (updated.rlim_cur <= limit.rlim_cur) {
        return;
    }

    if (setrlimit(RLIMIT_NOFILE, &updated) != 0) {
        std::cerr << "setrlimit(RLIMIT_NOFILE) failed: " << std::strerror(errno) << std::endl;
        return;
    }
    std::cerr << "raised RLIMIT_NOFILE from " << limit.rlim_cur << " to " << updated.rlim_cur << std::endl;
}

static void __sigint(int iSigNo)
{
    (void)iSigNo;
    main_exit_flage = 1;
}

static std::string getenv_or_empty(const char *name)
{
    const char *val = std::getenv(name);
    return val == nullptr ? std::string() : std::string(val);
}

static bool parse_bool_string(const std::string &val, bool default_value)
{
    if (val == "1" || val == "true" || val == "TRUE" || val == "on" || val == "ON") {
        return true;
    }
    if (val == "0" || val == "false" || val == "FALSE" || val == "off" || val == "OFF") {
        return false;
    }
    return default_value;
}

static bool json_bool_value(const json &body, const std::string &key, bool default_value)
{
    if (!body.contains(key)) {
        return default_value;
    }
    const auto &value = body.at(key);
    if (value.is_boolean()) {
        return value.get<bool>();
    }
    if (value.is_number_integer()) {
        return value.get<int>() != 0;
    }
    if (value.is_string()) {
        return parse_bool_string(value.get<std::string>(), default_value);
    }
    return default_value;
}

static int json_int_value(const json &body, const std::string &key, int default_value)
{
    if (!body.contains(key)) {
        return default_value;
    }
    const auto &value = body.at(key);
    if (value.is_number_integer()) {
        return value.get<int>();
    }
    if (value.is_string()) {
        try {
            return std::stoi(value.get<std::string>());
        } catch (...) {
            return default_value;
        }
    }
    return default_value;
}

static std::string json_string_value(const json &body, const std::string &key, const std::string &default_value = "")
{
    if (!body.contains(key) || !body.at(key).is_string()) {
        return default_value;
    }
    return body.at(key).get<std::string>();
}


static edge_observability::ObservabilityConfig observability_config_from_json(const json &config_body)
{
    edge_observability::ObservabilityConfig config;
    const json *source = nullptr;

    if (config_body.contains("observability")) {
        const auto &raw = config_body.at("observability");
        if (raw.is_object()) {
            source = &raw;
        } else if (raw.is_boolean()) {
            config.enabled = raw.get<bool>();
        } else if (raw.is_number_integer()) {
            config.enabled = raw.get<int>() != 0;
        } else if (raw.is_string()) {
            config.enabled = parse_bool_string(raw.get<std::string>(), config.enabled);
        }
    }

    if (source != nullptr) {
        config.enabled = json_bool_value(*source, "enabled", config.enabled);
        config.db_dir = json_string_value(*source, "db_dir", config.db_dir);
        config.flush_interval_ms = std::max(100, json_int_value(*source, "flush_interval_ms", config.flush_interval_ms));
        config.sync_wal = json_bool_value(*source, "sync_wal", config.sync_wal);
        config.memtable_limit_bytes = std::max<int64_t>(1024 * 1024,
                                                        json_int_value(*source, "memtable_limit_bytes",
                                                                       static_cast<int>(config.memtable_limit_bytes)));
        config.block_cache_bytes = static_cast<size_t>(std::max(1024, json_int_value(*source, "block_cache_bytes",
                                                                                      static_cast<int>(config.block_cache_bytes))));
        config.max_samples_per_series = static_cast<size_t>(std::max(1, json_int_value(*source, "max_samples_per_series",
                                                                                        static_cast<int>(config.max_samples_per_series))));
    }

    config.enabled = json_bool_value(config_body, "observability_enabled", config.enabled);
    return config;
}

static std::string trim_right_slash(std::string value)
{
    while (!value.empty() && value.back() == '/') {
        value.pop_back();
    }
    return value;
}

enum class TaskStatus {
    Created = 0,
    Starting,
    Ready,
    Running,
    Stopping,
    Stopped,
    Failed,
};

static std::string status_to_string(TaskStatus status)
{
    switch (status) {
        case TaskStatus::Created:
            return "created";
        case TaskStatus::Starting:
            return "starting";
        case TaskStatus::Ready:
            return "ready";
        case TaskStatus::Running:
            return "running";
        case TaskStatus::Stopping:
            return "stopping";
        case TaskStatus::Stopped:
            return "stopped";
        case TaskStatus::Failed:
            return "failed";
    }
    return "unknown";
}

static int64_t now_ms()
{
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

static bool string_in_vector(const std::vector<std::string> &values, const std::string &target)
{
    return std::find(values.begin(), values.end(), target) != values.end();
}

static std::vector<std::string> json_string_array_value(const json &body, const std::string &key)
{
    std::vector<std::string> values;
    if (!body.contains(key)) {
        return values;
    }
    const auto &raw = body.at(key);
    if (raw.is_string()) {
        values.push_back(raw.get<std::string>());
    } else if (raw.is_array()) {
        for (const auto &item : raw) {
            if (item.is_string()) {
                values.push_back(item.get<std::string>());
            }
        }
    }
    return values;
}

enum class CircuitState {
    Closed = 0,
    Open,
    HalfOpen,
};

static std::string circuit_to_string(CircuitState state)
{
    switch (state) {
        case CircuitState::Closed:
            return "closed";
        case CircuitState::Open:
            return "open";
        case CircuitState::HalfOpen:
            return "half_open";
    }
    return "unknown";
}

struct InferenceRequest {
    int task_id = 0;
    std::string request_id;
    std::string work_id;
    std::string output_url;
    std::string object;
    std::string action;
    std::string profile_name;
    std::string prompt;
    std::string system_prompt;
    std::string response_format = "llm.utf-8.stream";
    std::string session_default_action = "chat";
    std::string logical_model;
    std::string served_model;
    std::string endpoint_name;
    std::vector<std::string> fallback_chain;
    int priority = 5;
    int timeout_ms = 30000;
    int deadline_ms = 70000;
    int max_retries = 0;
    int max_tokens = 1024;
    int retry_count = 0;
    int64_t created_ms = 0;
    int64_t started_ms = 0;
    int64_t finished_ms = 0;
    int64_t deadline_at_ms = 0;
    int64_t queue_wait_ms = 0;
    int64_t first_token_latency_ms = -1;
    int64_t total_latency_ms = -1;
    bool allow_degrade = true;
    bool degraded = false;
    bool enoutput = true;
    bool enstream = true;
    std::string degrade_reason;
};

struct RequestOptions {
    std::string response_format = "llm.utf-8.stream";
    std::string system_prompt;
    std::string default_action = "chat";
    bool enoutput = true;
    bool enstream = true;
};

struct VllmEndpoint {
    std::string name;
    std::string base_url;
    std::string served_model;
    std::string role = "primary";
    std::string health_path = "/health";
    std::string api_key;
    std::vector<std::string> logical_models;
    int weight = 100;
    int max_context_len = 0;
    int max_concurrency = 1;
    bool healthy = true;
    CircuitState circuit_state = CircuitState::Closed;
    int inflight = 0;
    int consecutive_failures = 0;
    int consecutive_successes = 0;
    int half_open_inflight = 0;
    int64_t circuit_open_until_ms = 0;
    int64_t last_health_check_ms = 0;
    int64_t last_latency_ms = 0;
    double avg_latency_ms = 0.0;
    uint64_t success_count = 0;
    uint64_t failure_count = 0;
    uint64_t timeout_count = 0;
    uint64_t circuit_open_count = 0;
};


static int circuit_to_metric_value(CircuitState state)
{
    switch (state) {
        case CircuitState::Closed:
            return 0;
        case CircuitState::HalfOpen:
            return 1;
        case CircuitState::Open:
            return 2;
    }
    return -1;
}

static edge_observability::EndpointMetricsEvent endpoint_metrics_event(const VllmEndpoint &ep)
{
    edge_observability::EndpointMetricsEvent event;
    event.endpoint = ep.name;
    event.inflight = ep.inflight;
    event.available_capacity = std::max(0, ep.max_concurrency - ep.inflight);
    event.avg_latency_ms = ep.avg_latency_ms;
    event.healthy = ep.healthy;
    event.circuit_state = circuit_to_metric_value(ep.circuit_state);
    event.success_count = ep.success_count;
    event.failure_count = ep.failure_count;
    event.timeout_count = ep.timeout_count;
    event.circuit_open_count = ep.circuit_open_count;
    return event;
}

struct ModelProfile {
    std::string name;
    std::vector<std::string> actions;
    std::string primary_model;
    std::vector<std::string> fallback_models;
    int max_context_len = 0;
    bool stream = true;
    bool tool_calling = false;
    int default_max_tokens = 0;
    int default_timeout_ms = 0;
    int default_deadline_ms = 0;
    int default_priority = -1;
    bool allow_model_fallback = true;
};

enum class RequestStatus {
    Pending = 0,
    Running,
    Success,
    Failed,
    Timeout,
    Cancelled,
};

static std::string request_status_to_string(RequestStatus status)
{
    switch (status) {
        case RequestStatus::Pending:
            return "pending";
        case RequestStatus::Running:
            return "running";
        case RequestStatus::Success:
            return "success";
        case RequestStatus::Failed:
            return "failed";
        case RequestStatus::Timeout:
            return "timeout";
        case RequestStatus::Cancelled:
            return "cancelled";
    }
    return "unknown";
}

struct RequestRecord {
    int task_id = 0;
    std::string request_id;
    std::string work_id;
    std::string action;
    std::string profile_name;
    std::string logical_model;
    std::string served_model;
    std::string endpoint_name;
    RequestStatus status = RequestStatus::Pending;
    int priority = 5;
    int retry_count = 0;
    bool degraded = false;
    std::string error_message;
    int64_t created_ms = 0;
    int64_t started_ms = 0;
    int64_t finished_ms = 0;
    int64_t queue_wait_ms = 0;
    int64_t first_token_latency_ms = -1;
    int64_t total_latency_ms = -1;
};

class MetricsCollector {
public:
    void on_request_accepted(int64_t time_ms)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        total_requests_++;
        request_times_.push_back(time_ms);
        trim_times_locked(time_ms);
    }

    void on_queue_reject()
    {
        std::lock_guard<std::mutex> lock(mtx_);
        queue_reject_count_++;
        failed_requests_++;
    }

    void on_request_finish(bool ok, bool timeout, bool degraded, int64_t total_latency_ms, int64_t queue_wait_ms,
                           int64_t first_token_latency_ms)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (ok) {
            success_requests_++;
        } else if (timeout) {
            timeout_requests_++;
            failed_requests_++;
        } else {
            failed_requests_++;
        }
        if (degraded) {
            degraded_requests_++;
        }
        push_sample_locked(latencies_, total_latency_ms);
        push_sample_locked(queue_waits_, queue_wait_ms);
        if (first_token_latency_ms >= 0) {
            push_sample_locked(first_token_latencies_, first_token_latency_ms);
        }
    }

    json snapshot(int queue_size, int max_queue_size) const
    {
        std::lock_guard<std::mutex> lock(mtx_);
        json body;
        const int64_t current_ms = now_ms();
        int recent_count = 0;
        for (const auto &t : request_times_) {
            if (current_ms - t <= 60000) {
                recent_count++;
            }
        }
        body["total_requests"] = total_requests_;
        body["success_requests"] = success_requests_;
        body["failed_requests"] = failed_requests_;
        body["timeout_requests"] = timeout_requests_;
        body["degraded_requests"] = degraded_requests_;
        body["queue_reject_count"] = queue_reject_count_;
        body["qps_1m"] = static_cast<double>(recent_count) / 60.0;
        body["queue_size"] = queue_size;
        body["max_queue_size"] = max_queue_size;
        body["avg_latency_ms"] = average_locked(latencies_);
        body["p95_latency_ms"] = percentile_locked(latencies_, 0.95);
        body["queue_wait_avg_ms"] = average_locked(queue_waits_);
        body["queue_wait_p95_ms"] = percentile_locked(queue_waits_, 0.95);
        body["first_token_avg_ms"] = average_locked(first_token_latencies_);
        body["first_token_p95_ms"] = percentile_locked(first_token_latencies_, 0.95);
        return body;
    }

private:
    static void push_sample_locked(std::deque<int64_t> &values, int64_t value)
    {
        if (value < 0) {
            return;
        }
        values.push_back(value);
        while (values.size() > kMaxSamples) {
            values.pop_front();
        }
    }

    void trim_times_locked(int64_t current_ms)
    {
        while (!request_times_.empty() && current_ms - request_times_.front() > 60000) {
            request_times_.pop_front();
        }
    }

    static int64_t average_locked(const std::deque<int64_t> &values)
    {
        if (values.empty()) {
            return 0;
        }
        int64_t sum = 0;
        for (const auto &v : values) {
            sum += v;
        }
        return sum / static_cast<int64_t>(values.size());
    }

    static int64_t percentile_locked(const std::deque<int64_t> &values, double pct)
    {
        if (values.empty()) {
            return 0;
        }
        std::vector<int64_t> sorted(values.begin(), values.end());
        std::sort(sorted.begin(), sorted.end());
        size_t index = static_cast<size_t>(pct * static_cast<double>(sorted.size() - 1));
        if (index >= sorted.size()) {
            index = sorted.size() - 1;
        }
        return sorted[index];
    }

private:
    static const size_t kMaxSamples = 1024;
    mutable std::mutex mtx_;
    uint64_t total_requests_ = 0;
    uint64_t success_requests_ = 0;
    uint64_t failed_requests_ = 0;
    uint64_t timeout_requests_ = 0;
    uint64_t degraded_requests_ = 0;
    uint64_t queue_reject_count_ = 0;
    std::deque<int64_t> request_times_;
    std::deque<int64_t> latencies_;
    std::deque<int64_t> queue_waits_;
    std::deque<int64_t> first_token_latencies_;
};

using task_output_t = std::function<void(const std::string &request_id, const std::string &work_id,
                                         const std::string &output_url, const std::string &object, const json &data,
                                         int error_code, const std::string &error_message)>;

class vllm_task {
public:
    explicit vllm_task(const std::string &work_id) : work_id_(work_id) {}

    ~vllm_task()
    {
        stop();
    }

    void set_output(task_output_t out_callback)
    {
        out_callback_ = std::move(out_callback);
    }

    int load_model(const json &config_body)
    {
        status_.store(static_cast<int>(TaskStatus::Starting));
        if (parse_config(config_body)) {
            status_.store(static_cast<int>(TaskStatus::Failed));
            return -1;
        }
        status_.store(static_cast<int>(TaskStatus::Ready));
        return 0;
    }

    bool start()
    {
        if (!workers_.empty()) {
            return true;
        }
        stopping_.store(false);
        for (int i = 0; i < worker_count_; ++i) {
            workers_.emplace_back(&vllm_task::worker_loop, this);
        }
        if (!mock_mode_ && !endpoints_.empty()) {
            health_worker_ = std::thread(&vllm_task::health_loop, this);
        }
        return true;
    }

    void stop()
    {
        bool expected = false;
        if (!stopping_.compare_exchange_strong(expected, true) && workers_.empty() && !health_worker_.joinable()) {
            return;
        }
        status_.store(static_cast<int>(TaskStatus::Stopping));
        queue_cv_.notify_all();
        for (auto &worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        workers_.clear();
        if (health_worker_.joinable()) {
            health_worker_.join();
        }
        status_.store(static_cast<int>(TaskStatus::Stopped));
        observability_.stop();
    }

    edge_observability::QueueMetricsEvent queue_metrics_locked(int max_queue_size,
                                                               int high_priority_threshold) const
    {
        edge_observability::QueueMetricsEvent event;
        event.max_queue_size = max_queue_size;
        for (const auto &req : pending_) {
            if (req.priority >= high_priority_threshold) {
                event.high++;
            } else if (req.priority >= 4) {
                event.normal++;
            } else {
                event.background++;
            }
        }
        event.pending = event.high + event.normal + event.background;
        return event;
    }

    void inference(const std::string &request_id, const std::string &work_id, const std::string &output_url,
                   const std::string &object, const std::string &prompt, const json &meta,
                   const RequestOptions &options)
    {
        InferenceRequest req;
        req.task_id = ++task_id_counter_;
        req.request_id = request_id;
        req.work_id = work_id;
        req.output_url = output_url;
        req.object = object;
        req.prompt = prompt;
        req.system_prompt = json_string_value(meta, "prompt", options.system_prompt.empty() ? prompt_ : options.system_prompt);
        req.response_format = json_string_value(meta, "response_format",
                                                options.response_format.empty() ? response_format_ : options.response_format);
        req.session_default_action =
            json_string_value(meta, "default_action", options.default_action.empty() ? default_action_ : options.default_action);
        req.enoutput = json_bool_value(meta, "enoutput", options.enoutput);
        req.enstream = req.response_format.find("stream") != std::string::npos;
        req.enstream = json_bool_value(meta, "stream", json_bool_value(meta, "enstream", req.enstream));
        req.action = request_action(object, meta, req.session_default_action);
        apply_profile_to_request(&req, meta);
        req.created_ms = now_ms();
        req.deadline_at_ms = req.created_ms + req.deadline_ms;

        bool reject = false;
        int error_code = -40;
        std::string error_message;
        InferenceRequest record_req = req;
        int max_queue_size = 0;
        int low_priority_drop_threshold = 0;
        int high_priority_threshold = 0;
        edge_observability::QueueMetricsEvent queue_event;
        bool have_queue_event = false;
        {
            std::lock_guard<std::mutex> lock(config_mtx_);
            max_queue_size = max_queue_size_;
            low_priority_drop_threshold = low_priority_drop_threshold_;
            high_priority_threshold = high_priority_threshold_;
        }
        {
            std::lock_guard<std::mutex> lock(queue_mtx_);
            const int queue_size = static_cast<int>(pending_.size());
            const int overload_threshold = max_queue_size > 0 ? max_queue_size * 9 / 10 : 0;
            if (max_queue_size > 0 && queue_size >= max_queue_size) {
                reject = true;
                error_message = "task queue full";
            } else if (overload_threshold > 0 && queue_size >= overload_threshold &&
                       req.priority < high_priority_threshold) {
                reject = true;
                error_code = -43;
                error_message = "task queue is overloaded";
            } else if (low_priority_drop_threshold > 0 && queue_size >= low_priority_drop_threshold &&
                       req.priority < 4) {
                reject = true;
                error_code = -43;
                error_message = "background task rejected under queue pressure";
            } else {
                pending_.push_back(std::move(req));
            }
            queue_event = queue_metrics_locked(max_queue_size, high_priority_threshold);
            have_queue_event = true;
        }

        if (reject) {
            metrics_.on_queue_reject();
            observability_.observe_queue_reject(record_req.action);
            if (have_queue_event) {
                observability_.observe_queue_snapshot(queue_event);
            }
            emit_error(record_req, error_code, error_message);
            return;
        }
        metrics_.on_request_accepted(record_req.created_ms);
        record_pending(record_req);
        if (have_queue_event) {
            observability_.observe_queue_snapshot(queue_event);
        }
        queue_cv_.notify_one();
    }

    json taskinfo(const json &query) const
    {
        json body;
        int max_queue_size_snapshot = 0;
        {
            std::lock_guard<std::mutex> lock(config_mtx_);
            body["work_id"] = work_id_;
            body["model"] = model_;
            body["logical_model"] = model_;
            body["response_format"] = response_format_;
            body["enoutput"] = enoutput_;
            body["enstream"] = enstream_;
            body["inputs"] = inputs_;
            body["status"] = status_to_string(static_cast<TaskStatus>(status_.load()));
            body["timeout_ms"] = timeout_ms_;
            body["deadline_ms"] = deadline_ms_;
            body["retry_count"] = retry_count_.load();
            body["max_retries"] = max_retries_;
            body["mock"] = mock_mode_;
            body["max_queue_size"] = max_queue_size_;
            body["low_priority_drop_threshold"] = low_priority_drop_threshold_;
            body["high_priority_threshold"] = high_priority_threshold_;
            body["worker_count"] = worker_count_;
            body["endpoint_wait_ms"] = endpoint_wait_ms_;
            body["routing_strategy"] = routing_strategy_;
            body["default_action"] = default_action_;
            body["allow_model_fallback"] = allow_model_fallback_;
            body["fallback_chain"] = fallback_chain_;
            body["health_check_interval_ms"] = health_check_interval_ms_;
            body["health_check_timeout_ms"] = health_check_timeout_ms_;
            body["unhealthy_threshold"] = unhealthy_threshold_;
            body["recovery_threshold"] = recovery_threshold_;
            body["circuit_failure_threshold"] = circuit_failure_threshold_;
            body["circuit_open_ms"] = circuit_open_ms_;
            body["config_version"] = config_version_.load();
            body["last_config_update_ms"] = last_config_update_ms_.load();
            body["config_update_count"] = config_update_count_.load();
            body["model_profiles"] = model_profiles_json_unlocked();
            max_queue_size_snapshot = max_queue_size_;
        }
        body["base_url"] = primary_base_url();
        body["pending"] = pending_count();
        body["queue_size"] = pending_count();
        body["queue_stats"] = queue_stats();
        body["metrics"] = metrics_.snapshot(pending_count(), max_queue_size_snapshot);
        body["observability"] = observability_.snapshot();
        body["recent_tasks"] = records_json(query);

        json endpoint_list = json::array();
        {
            std::lock_guard<std::mutex> lock(endpoints_mtx_);
            for (const auto &ep : endpoints_) {
                json item;
                item["name"] = ep->name;
                item["base_url"] = ep->base_url;
                item["served_model"] = ep->served_model;
                item["role"] = ep->role;
                item["logical_models"] = ep->logical_models;
                item["weight"] = ep->weight;
                item["max_context_len"] = ep->max_context_len;
                item["max_concurrency"] = ep->max_concurrency;
                item["healthy"] = ep->healthy;
                item["circuit_state"] = circuit_to_string(ep->circuit_state);
                item["inflight"] = ep->inflight;
                item["available_capacity"] = std::max(0, ep->max_concurrency - ep->inflight);
                item["capacity_utilization"] =
                    ep->max_concurrency <= 0 ? 0.0 : static_cast<double>(ep->inflight) / ep->max_concurrency;
                item["avg_latency_ms"] = static_cast<int>(ep->avg_latency_ms);
                item["last_latency_ms"] = ep->last_latency_ms;
                item["last_health_check_ms"] = ep->last_health_check_ms;
                item["consecutive_failures"] = ep->consecutive_failures;
                item["success_count"] = ep->success_count;
                item["failure_count"] = ep->failure_count;
                item["timeout_count"] = ep->timeout_count;
                item["circuit_open_count"] = ep->circuit_open_count;
                endpoint_list.push_back(item);
            }
        }
        body["endpoints"] = endpoint_list;
        return body;
    }

    bool reload_config(const json &config_body, std::string *error_message)
    {
        std::lock_guard<std::mutex> reload_lock(reload_mtx_);
        if (!config_body.is_object()) {
            *error_message = "reload_config data must be a json object";
            return false;
        }

        std::string next_model;
        std::string next_response_format;
        std::string next_base_url;
        std::string next_api_key;
        std::string next_endpoint;
        std::string next_routing_strategy;
        std::vector<std::string> next_fallback_chain;
        std::vector<ModelProfile> next_model_profiles;
        bool next_mock_mode = true;
        bool next_allow_model_fallback = true;
        int next_worker_count = 1;
        int next_endpoint_wait_ms = 0;
        int next_max_queue_size = 0;
        int next_low_priority_drop_threshold = 0;
        int next_high_priority_threshold = 0;
        int next_health_check_interval_ms = 0;
        int next_health_check_timeout_ms = 0;
        int next_unhealthy_threshold = 0;
        int next_recovery_threshold = 0;
        int next_circuit_failure_threshold = 0;
        int next_circuit_open_ms = 0;

        {
            std::lock_guard<std::mutex> lock(config_mtx_);
            next_model = model_;
            next_response_format = response_format_;
            next_base_url = base_url_;
            next_api_key = api_key_;
            next_endpoint = endpoint_;
            next_routing_strategy = routing_strategy_;
            next_fallback_chain = fallback_chain_;
            next_model_profiles = model_profiles_;
            next_mock_mode = mock_mode_;
            next_allow_model_fallback = allow_model_fallback_;
            next_worker_count = worker_count_;
            next_endpoint_wait_ms = endpoint_wait_ms_;
            next_max_queue_size = max_queue_size_;
            next_low_priority_drop_threshold = low_priority_drop_threshold_;
            next_high_priority_threshold = high_priority_threshold_;
            next_health_check_interval_ms = health_check_interval_ms_;
            next_health_check_timeout_ms = health_check_timeout_ms_;
            next_unhealthy_threshold = unhealthy_threshold_;
            next_recovery_threshold = recovery_threshold_;
            next_circuit_failure_threshold = circuit_failure_threshold_;
            next_circuit_open_ms = circuit_open_ms_;
        }

        if (!validate_static_reload_fields(config_body, next_model, next_response_format, next_endpoint,
                                           next_worker_count, next_mock_mode, error_message)) {
            return false;
        }

        if (config_body.contains("routing_strategy")) {
            next_routing_strategy = json_string_value(config_body, "routing_strategy", next_routing_strategy);
        }
        next_endpoint_wait_ms = std::max(0, json_int_value(config_body, "endpoint_wait_ms", next_endpoint_wait_ms));
        next_max_queue_size = std::max(0, json_int_value(config_body, "max_queue_size", next_max_queue_size));
        next_low_priority_drop_threshold =
            std::max(0, json_int_value(config_body, "low_priority_drop_threshold", next_low_priority_drop_threshold));
        next_high_priority_threshold =
            std::max(0, json_int_value(config_body, "high_priority_threshold", next_high_priority_threshold));
        next_health_check_interval_ms =
            std::max(1, json_int_value(config_body, "health_check_interval_ms", next_health_check_interval_ms));
        next_health_check_timeout_ms =
            std::max(1, json_int_value(config_body, "health_check_timeout_ms", next_health_check_timeout_ms));
        next_unhealthy_threshold =
            std::max(1, json_int_value(config_body, "unhealthy_threshold", next_unhealthy_threshold));
        next_recovery_threshold = std::max(1, json_int_value(config_body, "recovery_threshold", next_recovery_threshold));
        next_circuit_failure_threshold =
            std::max(1, json_int_value(config_body, "circuit_failure_threshold", next_circuit_failure_threshold));
        next_circuit_open_ms = std::max(1, json_int_value(config_body, "circuit_open_ms", next_circuit_open_ms));

        if (config_body.contains("degrade_policy") && config_body["degrade_policy"].is_object()) {
            const auto &policy = config_body["degrade_policy"];
            next_allow_model_fallback = json_bool_value(policy, "allow_model_fallback", next_allow_model_fallback);
            if (policy.contains("fallback_chain")) {
                next_fallback_chain = json_string_array_value(policy, "fallback_chain");
            }
        } else {
            next_allow_model_fallback =
                json_bool_value(config_body, "allow_model_fallback", next_allow_model_fallback);
            if (config_body.contains("fallback_chain")) {
                next_fallback_chain = json_string_array_value(config_body, "fallback_chain");
            }
        }

        if (config_body.contains("model_profiles")) {
            if (build_model_profiles_from_config(config_body, next_model, &next_model_profiles, error_message)) {
                return false;
            }
        }

        const bool endpoints_changed = has_endpoint_reload_config(config_body);
        std::vector<std::shared_ptr<VllmEndpoint>> next_endpoints;
        if (endpoints_changed) {
            next_base_url = trim_right_slash(json_string_value(config_body, "vllm_base_url", next_base_url));
            if (build_endpoints_from_config(config_body, next_model, next_base_url, next_api_key, next_mock_mode,
                                            false, true, &next_endpoints, error_message)) {
                return false;
            }
        } else {
            std::lock_guard<std::mutex> lock(endpoints_mtx_);
            next_endpoints = endpoints_;
        }
        fill_default_fallback_chain(&next_fallback_chain, next_model, next_endpoints);

        {
            std::lock(config_mtx_, endpoints_mtx_);
            std::lock_guard<std::mutex> config_lock(config_mtx_, std::adopt_lock);
            std::lock_guard<std::mutex> endpoints_lock(endpoints_mtx_, std::adopt_lock);
            base_url_ = next_base_url;
            routing_strategy_ = next_routing_strategy;
            fallback_chain_ = next_fallback_chain;
            model_profiles_ = next_model_profiles;
            allow_model_fallback_ = next_allow_model_fallback;
            endpoint_wait_ms_ = next_endpoint_wait_ms;
            max_queue_size_ = next_max_queue_size;
            low_priority_drop_threshold_ = next_low_priority_drop_threshold;
            high_priority_threshold_ = next_high_priority_threshold;
            health_check_interval_ms_ = next_health_check_interval_ms;
            health_check_timeout_ms_ = next_health_check_timeout_ms;
            unhealthy_threshold_ = next_unhealthy_threshold;
            recovery_threshold_ = next_recovery_threshold;
            circuit_failure_threshold_ = next_circuit_failure_threshold;
            circuit_open_ms_ = next_circuit_open_ms;
            if (endpoints_changed) {
                endpoints_ = commit_reloaded_endpoints_locked(next_endpoints);
            }
            config_version_.fetch_add(1);
            config_update_count_.fetch_add(1);
            last_config_update_ms_.store(now_ms());
        }
        if (config_body.contains("observability") || config_body.contains("observability_enabled")) {
            const auto observability_config = observability_config_from_json(config_body);
            std::string observability_error;
            if (!observability_.configure(observability_config, &observability_error)) {
                std::cerr << "observability reload disabled: " << observability_error << std::endl;
            }
        }

        return true;
    }

    int config_version() const
    {
        return config_version_.load();
    }

    int config_update_count() const
    {
        return config_update_count_.load();
    }

    const std::string &last_error() const
    {
        return last_error_;
    }

    const std::string &response_format() const
    {
        return response_format_;
    }

    bool enoutput() const
    {
        return enoutput_;
    }

    bool enstream() const
    {
        return enstream_;
    }

    json metrics_query(const json &query) const
    {
        return observability_.query(query);
    }

private:
    struct StreamState {
        vllm_task *task = nullptr;
        InferenceRequest req;
        std::string buffer;
        int index = 0;
        bool done = false;
        bool failed = false;
        bool started = false;
        int64_t begin_ms = 0;
        int64_t first_token_latency_ms = -1;
        std::string error_message;
    };

    static size_t write_body_callback(char *ptr, size_t size, size_t nmemb, void *userdata)
    {
        auto *body = static_cast<std::string *>(userdata);
        const size_t bytes = size * nmemb;
        body->append(ptr, bytes);
        return bytes;
    }

    static size_t write_stream_callback(char *ptr, size_t size, size_t nmemb, void *userdata)
    {
        auto *state = static_cast<StreamState *>(userdata);
        const size_t bytes = size * nmemb;
        state->buffer.append(ptr, bytes);

        size_t pos = 0;
        while ((pos = state->buffer.find('\n')) != std::string::npos) {
            std::string line = state->buffer.substr(0, pos);
            state->buffer.erase(0, pos + 1);
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            state->task->handle_stream_line(state, line);
        }
        return bytes;
    }

    bool parse_config(const json &config_body)
    {
        model_ = json_string_value(config_body, "model");
        response_format_ = json_string_value(config_body, "response_format", "llm.utf-8");
        enoutput_ = json_bool_value(config_body, "enoutput", true);
        enstream_ = response_format_.find("stream") != std::string::npos;
        prompt_ = json_string_value(config_body, "prompt");
        timeout_ms_ = json_int_value(config_body, "timeout_ms", 30000);
        deadline_ms_ = json_int_value(config_body, "deadline_ms", timeout_ms_ + 10000);
        max_retries_ = json_int_value(config_body, "max_retries", json_int_value(config_body, "retry_count", 0));
        max_tokens_ = json_int_value(config_body, "max_token_len", json_int_value(config_body, "max_tokens", 1024));
        temperature_ = json_string_value(config_body, "temperature", "0.7");
        base_url_ = json_string_value(config_body, "vllm_base_url", getenv_or_empty("VLLM_BASE_URL"));
        api_key_ = json_string_value(config_body, "api_key", getenv_or_empty("VLLM_API_KEY"));
        endpoint_ = json_string_value(config_body, "endpoint", "/v1/chat/completions");
        routing_strategy_ = json_string_value(config_body, "routing_strategy", "weighted_least_queue");
        default_action_ = json_string_value(config_body, "default_action", "chat");
        worker_count_ = std::max(1, json_int_value(config_body, "worker_count", 2));
        endpoint_wait_ms_ = std::max(0, json_int_value(config_body, "endpoint_wait_ms", 2000));
        max_queue_size_ = json_int_value(config_body, "max_queue_size", 64);
        max_task_records_ = static_cast<size_t>(std::max(1, json_int_value(config_body, "max_task_records", 1000)));
        low_priority_drop_threshold_ =
            json_int_value(config_body, "low_priority_drop_threshold", std::max(0, max_queue_size_ - 16));
        high_priority_threshold_ = json_int_value(config_body, "high_priority_threshold", 7);
        default_priority_ = json_int_value(config_body, "priority", 5);
        health_check_interval_ms_ = json_int_value(config_body, "health_check_interval_ms", 5000);
        health_check_timeout_ms_ = json_int_value(config_body, "health_check_timeout_ms", 2000);
        unhealthy_threshold_ = json_int_value(config_body, "unhealthy_threshold", 3);
        recovery_threshold_ = json_int_value(config_body, "recovery_threshold", 2);
        circuit_failure_threshold_ = json_int_value(config_body, "circuit_failure_threshold", 5);
        circuit_open_ms_ = json_int_value(config_body, "circuit_open_ms", 30000);

        if (config_body.contains("degrade_policy") && config_body["degrade_policy"].is_object()) {
            const auto &policy = config_body["degrade_policy"];
            allow_model_fallback_ = json_bool_value(policy, "allow_model_fallback", true);
            fallback_chain_ = json_string_array_value(policy, "fallback_chain");
        } else {
            allow_model_fallback_ = json_bool_value(config_body, "allow_model_fallback", true);
            fallback_chain_ = json_string_array_value(config_body, "fallback_chain");
        }

        bool default_mock = base_url_.empty() && !(config_body.contains("endpoints") || config_body.contains("vllm_endpoints"));
        std::string env_mock = getenv_or_empty("VLLM_MOCK");
        if (!env_mock.empty()) {
            default_mock = parse_bool_string(env_mock, default_mock);
        }
        mock_mode_ = json_bool_value(config_body, "mock", default_mock);

        const auto observability_config = observability_config_from_json(config_body);
        std::string observability_error;
        if (!observability_.configure(observability_config, &observability_error)) {
            std::cerr << "observability disabled: " << observability_error << std::endl;
        }

        inputs_.clear();
        if (config_body.contains("input")) {
            inputs_ = json_string_array_value(config_body, "input");
        }

        if (model_.empty()) {
            last_error_ = "missing model";
            return true;
        }
        if (!endpoint_.empty() && endpoint_.front() != '/') {
            endpoint_.insert(endpoint_.begin(), '/');
        }
        base_url_ = trim_right_slash(base_url_);

        std::string parse_error;
        if (build_model_profiles_from_config(config_body, model_, &model_profiles_, &parse_error)) {
            last_error_ = parse_error;
            return true;
        }
        std::vector<std::shared_ptr<VllmEndpoint>> parsed_endpoints;
        if (build_endpoints_from_config(config_body, model_, base_url_, api_key_, mock_mode_, true, false,
                                        &parsed_endpoints, &last_error_)) {
            return true;
        }
        endpoints_ = parsed_endpoints;
        fill_default_fallback_chain(&fallback_chain_, model_, endpoints_);
        return false;
    }

    static bool build_model_profiles_from_config(const json &config_body, const std::string &default_model,
                                                 std::vector<ModelProfile> *profiles, std::string *error_message)
    {
        profiles->clear();
        if (!config_body.contains("model_profiles")) {
            return false;
        }
        if (!config_body["model_profiles"].is_array()) {
            *error_message = "model_profiles must be an array";
            return true;
        }
        for (const auto &item : config_body["model_profiles"]) {
            if (!item.is_object()) {
                continue;
            }
            ModelProfile profile;
            profile.name = json_string_value(item, "name");
            profile.actions = json_string_array_value(item, "actions");
            profile.primary_model = json_string_value(item, "primary_model", json_string_value(item, "model", default_model));
            profile.fallback_models = json_string_array_value(item, "fallback_models");
            profile.max_context_len = json_int_value(item, "max_context_len", 0);
            profile.stream = json_bool_value(item, "stream", true);
            profile.tool_calling = json_bool_value(item, "tool_calling", false);
            profile.default_max_tokens = json_int_value(item, "default_max_tokens", 0);
            profile.default_timeout_ms = json_int_value(item, "default_timeout_ms", 0);
            profile.default_deadline_ms = json_int_value(item, "default_deadline_ms", 0);
            profile.default_priority = json_int_value(item, "default_priority", -1);
            profile.allow_model_fallback = json_bool_value(item, "allow_model_fallback", true);
            if (profile.name.empty()) {
                profile.name = profile.primary_model;
            }
            if (profile.primary_model.empty()) {
                profile.primary_model = default_model;
            }
            if (profile.actions.empty()) {
                profile.actions.push_back(profile.name);
            }
            profiles->push_back(profile);
        }
        return false;
    }

    static bool build_endpoints_from_config(const json &config_body, const std::string &default_model,
                                            const std::string &default_base_url, const std::string &default_api_key,
                                            bool mock_mode, bool share_runtime, bool strict_items,
                                            std::vector<std::shared_ptr<VllmEndpoint>> *out_endpoints,
                                            std::string *error_message)
    {
        out_endpoints->clear();
        const json *endpoint_array = nullptr;
        if (config_body.contains("endpoints")) {
            if (!config_body["endpoints"].is_array()) {
                *error_message = "endpoints must be an array";
                return true;
            }
            endpoint_array = &config_body["endpoints"];
        } else if (config_body.contains("vllm_endpoints")) {
            if (!config_body["vllm_endpoints"].is_array()) {
                *error_message = "vllm_endpoints must be an array";
                return true;
            }
            endpoint_array = &config_body["vllm_endpoints"];
        }

        if (endpoint_array != nullptr) {
            int index = 0;
            for (const auto &item : *endpoint_array) {
                if (!item.is_object()) {
                    if (strict_items) {
                        *error_message = "endpoint item must be an object";
                        return true;
                    }
                    continue;
                }
                auto ep = std::make_shared<VllmEndpoint>();
                ep->name = json_string_value(item, "name", "endpoint-" + std::to_string(index));
                ep->base_url = trim_right_slash(json_string_value(item, "base_url"));
                ep->served_model = json_string_value(item, "served_model", json_string_value(item, "model", default_model));
                ep->role = json_string_value(item, "role", index == 0 ? "primary" : "fallback");
                ep->health_path = json_string_value(item, "health_path", "/health");
                ep->api_key = json_string_value(item, "api_key", default_api_key);
                ep->logical_models = json_string_array_value(item, "logical_models");
                ep->weight = json_int_value(item, "weight", index == 0 ? 100 : 10);
                ep->max_context_len = json_int_value(item, "max_context_len", 0);
                ep->max_concurrency = std::max(1, json_int_value(item, "max_concurrency", 1));
                if (ep->logical_models.empty()) {
                    ep->logical_models.push_back(default_model);
                }
                if (!string_in_vector(ep->logical_models, ep->served_model)) {
                    ep->logical_models.push_back(ep->served_model);
                }
                if (ep->base_url.empty() && !mock_mode) {
                    *error_message = "endpoint missing base_url: " + ep->name;
                    return true;
                }
                if (ep->served_model.empty()) {
                    *error_message = "endpoint missing served_model: " + ep->name;
                    return true;
                }
                out_endpoints->push_back(share_runtime ? share_endpoint_runtime(ep) : ep);
                ++index;
            }
        }

        if (out_endpoints->empty()) {
            auto ep = std::make_shared<VllmEndpoint>();
            ep->name = "default";
            ep->base_url = default_base_url;
            ep->served_model = default_model;
            ep->logical_models.push_back(default_model);
            ep->role = "primary";
            ep->api_key = default_api_key;
            ep->max_concurrency = 1;
            out_endpoints->push_back(share_runtime ? share_endpoint_runtime(ep) : ep);
        }

        if (!mock_mode) {
            for (const auto &ep : *out_endpoints) {
                if (ep->base_url.empty()) {
                    *error_message = "missing vllm_base_url, VLLM_BASE_URL, or endpoints[].base_url";
                    return true;
                }
            }
        }
        return false;
    }

    static void fill_default_fallback_chain(std::vector<std::string> *fallback_chain, const std::string &default_model,
                                            const std::vector<std::shared_ptr<VllmEndpoint>> &endpoints)
    {
        if (!fallback_chain->empty()) {
            return;
        }
        fallback_chain->push_back(default_model);
        for (const auto &ep : endpoints) {
            if (!string_in_vector(*fallback_chain, ep->served_model)) {
                fallback_chain->push_back(ep->served_model);
            }
        }
    }

    static bool has_endpoint_reload_config(const json &config_body)
    {
        return config_body.contains("endpoints") || config_body.contains("vllm_endpoints") ||
               config_body.contains("vllm_base_url");
    }

    static bool validate_static_reload_fields(const json &config_body, const std::string &current_model,
                                              const std::string &current_response_format,
                                              const std::string &current_endpoint, int current_worker_count,
                                              bool current_mock_mode, std::string *error_message)
    {
        if (config_body.contains("model") && json_string_value(config_body, "model", current_model) != current_model) {
            *error_message = "model cannot be changed by reload_config; create a new scheduler instead";
            return false;
        }
        if (config_body.contains("response_format") &&
            json_string_value(config_body, "response_format", current_response_format) != current_response_format) {
            *error_message = "response_format cannot be changed by reload_config";
            return false;
        }
        if (config_body.contains("endpoint") &&
            json_string_value(config_body, "endpoint", current_endpoint) != current_endpoint) {
            *error_message = "endpoint path cannot be changed by reload_config";
            return false;
        }
        if (config_body.contains("worker_count") &&
            json_int_value(config_body, "worker_count", current_worker_count) != current_worker_count) {
            *error_message = "worker_count cannot be changed by reload_config";
            return false;
        }
        if (config_body.contains("mock") &&
            json_bool_value(config_body, "mock", current_mock_mode) != current_mock_mode) {
            *error_message = "mock mode cannot be changed by reload_config";
            return false;
        }
        return true;
    }

    int pending_count() const
    {
        std::lock_guard<std::mutex> lock(queue_mtx_);
        return static_cast<int>(pending_.size());
    }

    std::string primary_base_url() const
    {
        std::lock_guard<std::mutex> lock(endpoints_mtx_);
        if (endpoints_.empty()) {
            return "None";
        }
        return endpoints_.front()->base_url.empty() ? std::string("None") : endpoints_.front()->base_url;
    }

    std::string request_action(const std::string &object, const json &meta, const std::string &fallback_action) const
    {
        std::string action = json_string_value(meta, "action");
        if (!action.empty()) {
            return action;
        }
        if (object.find("summary") != std::string::npos) {
            return "summary";
        }
        if (object.find("rewrite") != std::string::npos) {
            return "rewrite";
        }
        if (object.find("code") != std::string::npos || object.find("debug") != std::string::npos) {
            return "code";
        }
        if (object.find("qa") != std::string::npos) {
            return "qa";
        }
        return fallback_action.empty() ? default_action_ : fallback_action;
    }

    const ModelProfile *find_profile_for_action(const std::string &action) const
    {
        if (action.empty()) {
            return nullptr;
        }
        for (const auto &profile : model_profiles_) {
            if (profile.name == action || string_in_vector(profile.actions, action)) {
                return &profile;
            }
        }
        return nullptr;
    }

    void apply_profile_to_request(InferenceRequest *req, const json &meta) const
    {
        std::lock_guard<std::mutex> lock(config_mtx_);
        const ModelProfile *profile = find_profile_for_action(req->action);
        const std::string requested_model = json_string_value(meta, "model");
        req->profile_name = profile == nullptr ? std::string("default") : profile->name;
        req->logical_model = profile == nullptr ? (requested_model.empty() ? model_ : requested_model) : profile->primary_model;
        req->priority = profile == nullptr || profile->default_priority < 0 ? default_priority_ : profile->default_priority;
        req->timeout_ms = profile == nullptr || profile->default_timeout_ms <= 0 ? timeout_ms_ : profile->default_timeout_ms;
        req->deadline_ms =
            profile == nullptr || profile->default_deadline_ms <= 0 ? deadline_ms_ : profile->default_deadline_ms;
        req->max_retries = max_retries_;
        req->max_tokens = profile == nullptr || profile->default_max_tokens <= 0 ? max_tokens_ : profile->default_max_tokens;
        req->allow_degrade = allow_model_fallback_ && (profile == nullptr || profile->allow_model_fallback);

        if (profile != nullptr) {
            req->fallback_chain.push_back(profile->primary_model);
            for (const auto &fallback : profile->fallback_models) {
                if (!string_in_vector(req->fallback_chain, fallback)) {
                    req->fallback_chain.push_back(fallback);
                }
            }
        } else {
            req->fallback_chain = fallback_chain_;
        }
        if (profile == nullptr && !requested_model.empty() && !string_in_vector(req->fallback_chain, requested_model)) {
            req->fallback_chain.insert(req->fallback_chain.begin(), requested_model);
        }
        if (req->fallback_chain.empty()) {
            req->fallback_chain.push_back(req->logical_model);
        }

        req->priority = json_int_value(meta, "priority", req->priority);
        req->timeout_ms = json_int_value(meta, "timeout_ms", req->timeout_ms);
        req->deadline_ms = json_int_value(meta, "deadline_ms", req->deadline_ms);
        req->max_retries = json_int_value(meta, "max_retries", req->max_retries);
        req->max_tokens = json_int_value(meta, "max_tokens", json_int_value(meta, "max_token_len", req->max_tokens));
        req->allow_degrade = json_bool_value(meta, "allow_degrade", req->allow_degrade);
        req->allow_degrade = json_bool_value(meta, "allow_model_fallback", req->allow_degrade);
    }

    json queue_stats() const
    {
        json body;
        int high = 0;
        int normal = 0;
        int background = 0;
        int high_priority_threshold = 0;
        int max_queue_size = 0;
        {
            std::lock_guard<std::mutex> lock(config_mtx_);
            high_priority_threshold = high_priority_threshold_;
            max_queue_size = max_queue_size_;
        }
        {
            std::lock_guard<std::mutex> lock(queue_mtx_);
            for (const auto &req : pending_) {
                if (req.priority >= high_priority_threshold) {
                    high++;
                } else if (req.priority >= 4) {
                    normal++;
                } else {
                    background++;
                }
            }
        }
        body["high"] = high;
        body["normal"] = normal;
        body["background"] = background;
        body["total"] = high + normal + background;
        if (max_queue_size <= 0 || body["total"].get<int>() < max_queue_size * 6 / 10) {
            body["pressure"] = "normal";
        } else if (body["total"].get<int>() < max_queue_size * 9 / 10) {
            body["pressure"] = "pressure";
        } else if (body["total"].get<int>() < max_queue_size) {
            body["pressure"] = "overload";
        } else {
            body["pressure"] = "full";
        }
        return body;
    }

    json model_profiles_json() const
    {
        std::lock_guard<std::mutex> lock(config_mtx_);
        return model_profiles_json_unlocked();
    }

    json model_profiles_json_unlocked() const
    {
        json profiles = json::array();
        for (const auto &profile : model_profiles_) {
            json item;
            item["name"] = profile.name;
            item["actions"] = profile.actions;
            item["primary_model"] = profile.primary_model;
            item["fallback_models"] = profile.fallback_models;
            item["max_context_len"] = profile.max_context_len;
            item["stream"] = profile.stream;
            item["tool_calling"] = profile.tool_calling;
            item["default_max_tokens"] = profile.default_max_tokens;
            item["default_timeout_ms"] = profile.default_timeout_ms;
            item["default_deadline_ms"] = profile.default_deadline_ms;
            item["default_priority"] = profile.default_priority;
            item["allow_model_fallback"] = profile.allow_model_fallback;
            profiles.push_back(item);
        }
        return profiles;
    }

    static json record_to_json(const RequestRecord &record)
    {
        json item;
        item["task_id"] = record.task_id;
        item["request_id"] = record.request_id;
        item["work_id"] = record.work_id;
        item["action"] = record.action;
        item["profile_name"] = record.profile_name;
        item["logical_model"] = record.logical_model;
        item["served_model"] = record.served_model;
        item["endpoint"] = record.endpoint_name;
        item["status"] = request_status_to_string(record.status);
        item["priority"] = record.priority;
        item["retry_count"] = record.retry_count;
        item["degraded"] = record.degraded;
        item["error_message"] = record.error_message;
        item["created_ms"] = record.created_ms;
        item["started_ms"] = record.started_ms;
        item["finished_ms"] = record.finished_ms;
        item["queue_wait_ms"] = record.queue_wait_ms;
        item["first_token_latency_ms"] = record.first_token_latency_ms;
        item["total_latency_ms"] = record.total_latency_ms;
        return item;
    }

    json records_json(const json &query) const
    {
        json body = json::array();
        int limit = json_int_value(query, "limit", 20);
        if (limit <= 0) {
            limit = 20;
        }
        const int query_task_id = json_int_value(query, "task_id", -1);
        const std::string query_request_id = json_string_value(query, "request_id");
        const std::string query_action = json_string_value(query, "action_filter");

        std::lock_guard<std::mutex> lock(records_mtx_);
        int emitted = 0;
        for (auto it = records_.rbegin(); it != records_.rend() && emitted < limit; ++it) {
            if (query_task_id >= 0 && it->task_id != query_task_id) {
                continue;
            }
            if (!query_request_id.empty() && it->request_id != query_request_id) {
                continue;
            }
            if (!query_action.empty() && it->action != query_action) {
                continue;
            }
            body.push_back(record_to_json(*it));
            emitted++;
        }
        return body;
    }

    void record_pending(const InferenceRequest &req)
    {
        RequestRecord record;
        record.task_id = req.task_id;
        record.request_id = req.request_id;
        record.work_id = req.work_id;
        record.action = req.action;
        record.profile_name = req.profile_name;
        record.logical_model = req.logical_model;
        record.priority = req.priority;
        record.status = RequestStatus::Pending;
        record.created_ms = req.created_ms;
        std::lock_guard<std::mutex> lock(records_mtx_);
        records_.push_back(record);
        while (records_.size() > max_task_records_) {
            records_.pop_front();
        }
    }

    void update_record(const InferenceRequest &req, RequestStatus status, const std::string &error_message = "")
    {
        std::lock_guard<std::mutex> lock(records_mtx_);
        for (auto it = records_.rbegin(); it != records_.rend(); ++it) {
            if (it->task_id != req.task_id) {
                continue;
            }
            it->action = req.action;
            it->profile_name = req.profile_name;
            it->logical_model = req.logical_model;
            it->served_model = req.served_model;
            it->endpoint_name = req.endpoint_name;
            it->status = status;
            it->priority = req.priority;
            it->retry_count = req.retry_count;
            it->degraded = req.degraded;
            it->error_message = error_message;
            it->started_ms = req.started_ms;
            it->finished_ms = req.finished_ms;
            it->queue_wait_ms = req.queue_wait_ms;
            it->first_token_latency_ms = req.first_token_latency_ms;
            it->total_latency_ms = req.total_latency_ms;
            return;
        }
    }

    void observe_request_metrics(const InferenceRequest &req, bool ok, bool timeout, int error_code)
    {
        edge_observability::RequestMetricsEvent event;
        event.action = req.action;
        event.endpoint = req.endpoint_name;
        event.ok = ok;
        event.timeout = timeout;
        event.degraded = req.degraded;
        event.error_code = error_code;
        event.total_latency_ms = req.total_latency_ms;
        event.queue_wait_ms = req.queue_wait_ms;
        event.first_token_latency_ms = req.first_token_latency_ms;
        observability_.observe_request_finish(event);
    }

    static int priority_class(int priority, int high_priority_threshold)
    {
        if (priority >= high_priority_threshold) {
            return 0;
        }
        if (priority >= 4) {
            return 1;
        }
        return 2;
    }

    std::deque<InferenceRequest>::iterator find_best_request_in_class_locked(int target_class,
                                                                             int high_priority_threshold)
    {
        auto best = pending_.end();
        for (auto it = pending_.begin(); it != pending_.end(); ++it) {
            if (priority_class(it->priority, high_priority_threshold) != target_class) {
                continue;
            }
            if (best == pending_.end() || it->priority > best->priority ||
                (it->priority == best->priority && it->task_id < best->task_id)) {
                best = it;
            }
        }
        return best;
    }

    bool pop_next_request(InferenceRequest *req)
    {
        int high_priority_threshold = 0;
        int max_queue_size = 0;
        {
            std::lock_guard<std::mutex> config_lock(config_mtx_);
            high_priority_threshold = high_priority_threshold_;
            max_queue_size = max_queue_size_;
        }

        edge_observability::QueueMetricsEvent queue_event;
        bool have_queue_event = false;
        {
            std::unique_lock<std::mutex> lock(queue_mtx_);
            queue_cv_.wait(lock, [this]() { return stopping_.load() || !pending_.empty(); });
            if (stopping_.load() && pending_.empty()) {
                return false;
            }
            static const int kSchedule[] = {0, 0, 0, 0, 0, 1, 1, 1, 2};
            constexpr int kScheduleSize = sizeof(kSchedule) / sizeof(kSchedule[0]);
            auto best = pending_.end();
            for (int i = 0; i < kScheduleSize; ++i) {
                const int target_class = kSchedule[queue_schedule_slot_ % kScheduleSize];
                queue_schedule_slot_ = (queue_schedule_slot_ + 1) % kScheduleSize;
                best = find_best_request_in_class_locked(target_class, high_priority_threshold);
                if (best != pending_.end()) {
                    break;
                }
            }
            if (best == pending_.end()) {
                best = pending_.begin();
            }
            *req = std::move(*best);
            pending_.erase(best);
            queue_event = queue_metrics_locked(max_queue_size, high_priority_threshold);
            have_queue_event = true;
        }
        if (have_queue_event) {
            observability_.observe_queue_snapshot(queue_event);
        }
        return true;
    }

    void worker_loop()
    {
        while (true) {
            InferenceRequest req;
            if (!pop_next_request(&req)) {
                return;
            }
            req.started_ms = now_ms();
            req.queue_wait_ms = req.started_ms - req.created_ms;
            update_record(req, RequestStatus::Running);
            status_.store(static_cast<int>(TaskStatus::Running));
            handle_request(req);
            if (!stopping_.load()) {
                status_.store(static_cast<int>(TaskStatus::Ready));
            }
        }
    }

    void handle_request(InferenceRequest req)
    {
        if (mock_mode_) {
            InferenceRequest mock_req = req;
            mock_req.endpoint_name = "mock";
            mock_req.served_model = model_;
            bool ok = run_mock(mock_req);
            mock_req.finished_ms = now_ms();
            mock_req.total_latency_ms = mock_req.finished_ms - mock_req.started_ms;
            if (!ok) {
                update_record(mock_req, RequestStatus::Failed, "mock request failed");
                metrics_.on_request_finish(false, false, false, mock_req.total_latency_ms, mock_req.queue_wait_ms,
                                           mock_req.first_token_latency_ms);
                observe_request_metrics(mock_req, false, false, -31);
                emit_error(mock_req, -31, "mock request failed");
            } else {
                update_record(mock_req, RequestStatus::Success);
                metrics_.on_request_finish(true, false, false, mock_req.total_latency_ms, mock_req.queue_wait_ms,
                                           mock_req.first_token_latency_ms);
                observe_request_metrics(mock_req, true, false, 0);
            }
            return;
        }

        bool ok = false;
        bool stream_started = false;
        std::string error_message;
        InferenceRequest final_req = req;
        for (int attempt = 0; attempt <= req.max_retries && !stopping_.load(); ++attempt) {
            if (deadline_exceeded(req)) {
                error_message = "deadline exceeded";
                break;
            }

            auto endpoint = wait_for_endpoint(req);
            if (!endpoint) {
                error_message = "no healthy endpoint or endpoint capacity timeout";
                break;
            }

            InferenceRequest attempt_req = req;
            attempt_req.retry_count = attempt;
            prepare_request_for_endpoint(&attempt_req, endpoint);
            ok = call_vllm(&attempt_req, endpoint, &error_message, &stream_started);
            int64_t latency_ms = attempt_req.total_latency_ms >= 0 ? attempt_req.total_latency_ms : 0;
            update_endpoint_result(endpoint, ok, latency_ms);
            final_req = attempt_req;
            if (ok) {
                final_req.finished_ms = now_ms();
                update_record(final_req, RequestStatus::Success);
                metrics_.on_request_finish(true, false, final_req.degraded, final_req.total_latency_ms,
                                           final_req.queue_wait_ms, final_req.first_token_latency_ms);
                observe_request_metrics(final_req, true, false, 0);
                break;
            }

            retry_count_.fetch_add(1);
            if (stream_started) {
                error_message = "vLLM stream failed after output started: " + error_message;
                break;
            }
        }

        if (!ok && !stopping_.load()) {
            if (error_message.empty()) {
                error_message = "vLLM request failed";
            }
            int code = error_message == "deadline exceeded" ? -42 : -41;
            final_req.finished_ms = now_ms();
            final_req.total_latency_ms = final_req.finished_ms - final_req.started_ms;
            const bool timeout = code == -42;
            update_record(final_req, timeout ? RequestStatus::Timeout : RequestStatus::Failed, error_message);
            metrics_.on_request_finish(false, timeout, final_req.degraded, final_req.total_latency_ms,
                                       final_req.queue_wait_ms, final_req.first_token_latency_ms);
            observe_request_metrics(final_req, false, timeout, code);
            emit_error(final_req, code, error_message);
        }
    }

    bool run_mock(const InferenceRequest &req)
    {
        if (req.enstream) {
            emit_delta(req, req.prompt, false, 0);
            emit_delta(req, " [mock-vllm]", false, 1);
            emit_delta(req, "", true, 2);
        } else {
            emit_text(req, req.prompt + " [mock-vllm]");
        }
        return true;
    }

    bool deadline_exceeded(const InferenceRequest &req) const
    {
        return req.deadline_ms > 0 && now_ms() >= req.deadline_at_ms;
    }

    int estimate_prompt_tokens(const InferenceRequest &req) const
    {
        return static_cast<int>(req.prompt.size() / 4) + req.max_tokens;
    }

    bool fallback_allows(const std::string &requested_model, const std::string &served_model,
                         const std::vector<std::string> &fallback_chain) const
    {
        if (served_model == requested_model) {
            return true;
        }
        auto requested = std::find(fallback_chain.begin(), fallback_chain.end(), requested_model);
        auto served = std::find(fallback_chain.begin(), fallback_chain.end(), served_model);
        if (requested == fallback_chain.end() || served == fallback_chain.end()) {
            return false;
        }
        return served >= requested;
    }

    bool is_degraded_endpoint(const VllmEndpoint &ep, const InferenceRequest &req) const
    {
        if (ep.served_model == req.logical_model) {
            return false;
        }
        if (!req.fallback_chain.empty() && ep.served_model == req.fallback_chain.front()) {
            return false;
        }
        if (ep.role == "primary" && string_in_vector(ep.logical_models, req.logical_model)) {
            return false;
        }
        return true;
    }

    bool endpoint_supports_request(const VllmEndpoint &ep, const InferenceRequest &req) const
    {
        if (ep.served_model == req.logical_model || string_in_vector(ep.logical_models, req.logical_model)) {
            return true;
        }
        return req.allow_degrade && fallback_allows(req.logical_model, ep.served_model, req.fallback_chain);
    }

    static std::string endpoint_runtime_key(const VllmEndpoint &ep)
    {
        return ep.base_url + "|" + ep.served_model;
    }

    static std::shared_ptr<VllmEndpoint> share_endpoint_runtime(const std::shared_ptr<VllmEndpoint> &configured)
    {
        const std::string key = endpoint_runtime_key(*configured);
        std::lock_guard<std::mutex> lock(global_endpoints_mtx_);
        auto existing_it = global_endpoints_.find(key);
        if (existing_it != global_endpoints_.end()) {
            if (auto existing = existing_it->second.lock()) {
                existing->name = configured->name;
                existing->role = configured->role;
                existing->health_path = configured->health_path;
                existing->api_key = configured->api_key;
                existing->logical_models = configured->logical_models;
                existing->weight = configured->weight;
                existing->max_context_len = configured->max_context_len;
                existing->max_concurrency = configured->max_concurrency;
                return existing;
            }
        }
        global_endpoints_[key] = configured;
        return configured;
    }

    static void publish_endpoint_runtime(const std::shared_ptr<VllmEndpoint> &endpoint)
    {
        std::lock_guard<std::mutex> lock(global_endpoints_mtx_);
        global_endpoints_[endpoint_runtime_key(*endpoint)] = endpoint;
    }

    static void copy_endpoint_runtime_fields(const std::shared_ptr<VllmEndpoint> &target,
                                             const std::shared_ptr<VllmEndpoint> &source)
    {
        target->healthy = source->healthy;
        target->circuit_state = source->circuit_state;
        target->consecutive_failures = source->consecutive_failures;
        target->consecutive_successes = source->consecutive_successes;
        target->circuit_open_until_ms = source->circuit_open_until_ms;
        target->last_health_check_ms = source->last_health_check_ms;
        target->last_latency_ms = source->last_latency_ms;
        target->avg_latency_ms = source->avg_latency_ms;
        target->success_count = source->success_count;
        target->failure_count = source->failure_count;
        target->timeout_count = source->timeout_count;
        target->circuit_open_count = source->circuit_open_count;
        target->inflight = 0;
        target->half_open_inflight = 0;
    }

    std::vector<std::shared_ptr<VllmEndpoint>> commit_reloaded_endpoints_locked(
        const std::vector<std::shared_ptr<VllmEndpoint>> &configured_endpoints)
    {
        std::unordered_map<std::string, std::shared_ptr<VllmEndpoint>> old_by_key;
        for (const auto &endpoint : endpoints_) {
            old_by_key[endpoint_runtime_key(*endpoint)] = endpoint;
        }

        std::vector<std::shared_ptr<VllmEndpoint>> committed;
        committed.reserve(configured_endpoints.size());
        for (const auto &configured : configured_endpoints) {
            auto endpoint = std::make_shared<VllmEndpoint>(*configured);
            auto old_it = old_by_key.find(endpoint_runtime_key(*endpoint));
            if (old_it != old_by_key.end()) {
                copy_endpoint_runtime_fields(endpoint, old_it->second);
            }
            publish_endpoint_runtime(endpoint);
            committed.push_back(endpoint);
        }
        return committed;
    }

    int model_preference_rank(const VllmEndpoint &ep, const InferenceRequest &req) const
    {
        auto served = std::find(req.fallback_chain.begin(), req.fallback_chain.end(), ep.served_model);
        if (served != req.fallback_chain.end()) {
            return static_cast<int>(std::distance(req.fallback_chain.begin(), served));
        }
        if (ep.served_model == req.logical_model || string_in_vector(ep.logical_models, req.logical_model)) {
            return 0;
        }
        return INT_MAX;
    }

    bool endpoint_available_locked(const std::shared_ptr<VllmEndpoint> &ep, int64_t current_ms)
    {
        if (ep->circuit_state == CircuitState::Open) {
            if (current_ms >= ep->circuit_open_until_ms) {
                ep->circuit_state = CircuitState::HalfOpen;
                ep->half_open_inflight = 0;
            } else {
                return false;
            }
        }
        if (ep->circuit_state == CircuitState::HalfOpen && ep->half_open_inflight > 0) {
            return false;
        }
        if (!ep->healthy && ep->circuit_state != CircuitState::HalfOpen) {
            return false;
        }
        if (ep->inflight >= ep->max_concurrency) {
            return false;
        }
        return true;
    }

    std::shared_ptr<VllmEndpoint> select_endpoint(const InferenceRequest &req)
    {
        int high_priority_threshold = 0;
        {
            std::lock_guard<std::mutex> config_lock(config_mtx_);
            high_priority_threshold = high_priority_threshold_;
        }

        std::shared_ptr<VllmEndpoint> best;
        edge_observability::EndpointMetricsEvent endpoint_event;
        bool have_endpoint_event = false;
        {
            std::lock_guard<std::mutex> lock(endpoints_mtx_);
            int best_rank = INT_MAX;
            int best_score = INT_MIN;
            int64_t current_ms = now_ms();
            const int estimated_tokens = estimate_prompt_tokens(req);

            for (const auto &ep : endpoints_) {
                if (!endpoint_supports_request(*ep, req)) {
                    continue;
                }
                if (!endpoint_available_locked(ep, current_ms)) {
                    continue;
                }
                if (ep->max_context_len > 0 && estimated_tokens > ep->max_context_len) {
                    continue;
                }
                const int rank = model_preference_rank(*ep, req);
                if (rank == INT_MAX) {
                    continue;
                }
                if (best && rank > best_rank) {
                    continue;
                }
                if (!best || rank < best_rank) {
                    best.reset();
                    best_rank = rank;
                    best_score = INT_MIN;
                }
                const bool degraded = is_degraded_endpoint(*ep, req);
                if (degraded && !req.allow_degrade) {
                    continue;
                }
                int score = ep->weight * 10;
                score -= (ep->inflight * 500) / std::max(1, ep->max_concurrency);
                score -= static_cast<int>(ep->avg_latency_ms / 20.0);
                score -= ep->consecutive_failures * 250;
                if (degraded) {
                    score -= 2000;
                }
                if (ep->role == "fallback" && req.priority >= high_priority_threshold) {
                    score -= 500;
                }
                if (ep->role == "overflow") {
                    score -= 50;
                }
                if (ep->circuit_state == CircuitState::HalfOpen) {
                    score -= 300;
                }
                if (!best || score > best_score) {
                    best = ep;
                    best_score = score;
                }
            }

            if (best) {
                best->inflight++;
                if (best->circuit_state == CircuitState::HalfOpen) {
                    best->half_open_inflight++;
                }
                endpoint_event = endpoint_metrics_event(*best);
                have_endpoint_event = true;
            }
        }
        if (have_endpoint_event) {
            observability_.observe_endpoint(endpoint_event);
        }
        return best;
    }

    std::shared_ptr<VllmEndpoint> wait_for_endpoint(const InferenceRequest &req)
    {
        auto endpoint = select_endpoint(req);
        if (endpoint || endpoint_wait_ms_ <= 0) {
            return endpoint;
        }

        const int64_t wait_until_ms = std::min(req.deadline_at_ms, now_ms() + static_cast<int64_t>(endpoint_wait_ms_));
        while (!stopping_.load() && now_ms() < wait_until_ms) {
            std::unique_lock<std::mutex> lock(endpoint_wait_mtx_);
            endpoint_wait_cv_.wait_for(lock, std::chrono::milliseconds(20));
            endpoint = select_endpoint(req);
            if (endpoint) {
                return endpoint;
            }
            if (deadline_exceeded(req)) {
                break;
            }
        }
        return nullptr;
    }

    void prepare_request_for_endpoint(InferenceRequest *req, const std::shared_ptr<VllmEndpoint> &ep) const
    {
        req->endpoint_name = ep->name;
        req->served_model = ep->served_model;
        req->degraded = is_degraded_endpoint(*ep, *req);
        if (req->degraded) {
            if (ep->role == "fallback") {
                req->degrade_reason = "fallback_endpoint";
            } else {
                req->degrade_reason = "model_fallback";
            }
        }
    }

    void update_endpoint_result(const std::shared_ptr<VllmEndpoint> &ep, bool ok, int64_t latency_ms)
    {
        int recovery_threshold = 0;
        int unhealthy_threshold = 0;
        int circuit_failure_threshold = 0;
        int circuit_open_ms = 0;
        {
            std::lock_guard<std::mutex> config_lock(config_mtx_);
            recovery_threshold = recovery_threshold_;
            unhealthy_threshold = unhealthy_threshold_;
            circuit_failure_threshold = circuit_failure_threshold_;
            circuit_open_ms = circuit_open_ms_;
        }

        edge_observability::EndpointMetricsEvent endpoint_event;
        bool have_endpoint_event = false;
        {
            std::lock_guard<std::mutex> lock(endpoints_mtx_);
            if (ep->inflight > 0) {
                ep->inflight--;
            }
            if (ep->half_open_inflight > 0) {
                ep->half_open_inflight--;
            }
            ep->last_latency_ms = latency_ms;
            if (latency_ms > 0) {
                if (ep->avg_latency_ms <= 0.0) {
                    ep->avg_latency_ms = static_cast<double>(latency_ms);
                } else {
                    ep->avg_latency_ms = ep->avg_latency_ms * 0.8 + static_cast<double>(latency_ms) * 0.2;
                }
            }

            if (ok) {
                ep->success_count++;
                ep->consecutive_successes++;
                ep->consecutive_failures = 0;
                if (ep->consecutive_successes >= recovery_threshold) {
                    ep->healthy = true;
                    ep->circuit_state = CircuitState::Closed;
                }
            } else {
                ep->failure_count++;
                ep->consecutive_failures++;
                ep->consecutive_successes = 0;
                if (ep->consecutive_failures >= unhealthy_threshold) {
                    ep->healthy = false;
                }
                if (ep->consecutive_failures >= circuit_failure_threshold) {
                    if (ep->circuit_state != CircuitState::Open) {
                        ep->circuit_open_count++;
                    }
                    ep->circuit_state = CircuitState::Open;
                    ep->circuit_open_until_ms = now_ms() + circuit_open_ms;
                }
            }
            endpoint_event = endpoint_metrics_event(*ep);
            have_endpoint_event = true;
        }
        if (have_endpoint_event) {
            observability_.observe_endpoint(endpoint_event);
        }
        endpoint_wait_cv_.notify_all();
    }

    json make_request_body(const InferenceRequest &req, const std::string &served_model) const
    {
        json body;
        body["model"] = served_model;
        body["stream"] = req.enstream;
        body["max_tokens"] = req.max_tokens;
        try {
            body["temperature"] = std::stod(temperature_);
        } catch (...) {
            body["temperature"] = 0.7;
        }

        json messages = json::array();
        if (!req.system_prompt.empty()) {
            messages.push_back({{"role", "system"}, {"content", req.system_prompt}});
        }
        messages.push_back({{"role", "user"}, {"content", req.prompt}});
        body["messages"] = messages;
        return body;
    }

    bool call_vllm(InferenceRequest *req, const std::shared_ptr<VllmEndpoint> &ep, std::string *error_message,
                   bool *stream_started)
    {
        CURL *curl = curl_easy_init();
        if (curl == nullptr) {
            *error_message = "curl_easy_init failed";
            return false;
        }

        const std::string url = ep->base_url + endpoint_;
        const std::string request_body = make_request_body(*req, ep->served_model).dump();
        struct curl_slist *headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        const std::string &api_key = ep->api_key.empty() ? api_key_ : ep->api_key;
        if (!api_key.empty()) {
            const std::string auth = "Authorization: Bearer " + api_key;
            headers = curl_slist_append(headers, auth.c_str());
        }

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_body.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(request_body.size()));
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, static_cast<long>(req->timeout_ms));
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

        std::string response_body;
        StreamState stream_state;
        stream_state.task = this;
        stream_state.req = *req;
        stream_state.begin_ms = now_ms();

        if (req->enstream) {
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &vllm_task::write_stream_callback);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &stream_state);
        } else {
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &vllm_task::write_body_callback);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
        }

        CURLcode code = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        if (stream_started != nullptr) {
            *stream_started = stream_state.started;
        }
        req->finished_ms = now_ms();
        req->total_latency_ms = req->finished_ms - req->started_ms;
        req->first_token_latency_ms = stream_state.first_token_latency_ms;
        if (code != CURLE_OK) {
            *error_message = curl_easy_strerror(code);
            return false;
        }
        if (http_code < 200 || http_code >= 300) {
            *error_message = "HTTP " + std::to_string(http_code) + ": " + response_body;
            return false;
        }

        if (req->enstream) {
            if (stream_state.failed) {
                *error_message = stream_state.error_message;
                return false;
            }
            emit_delta(*req, "", true, stream_state.index);
            return true;
        }

        std::string text;
        if (!extract_response_text(response_body, &text, error_message)) {
            return false;
        }
        emit_text(*req, text);
        return true;
    }

    bool extract_response_text(const std::string &body, std::string *text, std::string *error_message) const
    {
        try {
            json parsed = json::parse(body);
            if (parsed.contains("error")) {
                if (parsed["error"].is_object() && parsed["error"].contains("message")) {
                    *error_message = parsed["error"]["message"].get<std::string>();
                } else {
                    *error_message = parsed["error"].dump();
                }
                return false;
            }
            if (parsed.contains("choices") && parsed["choices"].is_array() && !parsed["choices"].empty()) {
                const auto &choice = parsed["choices"][0];
                if (choice.contains("message") && choice["message"].contains("content") &&
                    choice["message"]["content"].is_string()) {
                    *text = choice["message"]["content"].get<std::string>();
                    return true;
                }
                if (choice.contains("text") && choice["text"].is_string()) {
                    *text = choice["text"].get<std::string>();
                    return true;
                }
            }
        } catch (const std::exception &e) {
            *error_message = e.what();
            return false;
        }
        *error_message = "missing choices content in vLLM response";
        return false;
    }

    void handle_stream_line(StreamState *state, const std::string &line)
    {
        if (line.empty()) {
            return;
        }
        const std::string prefix = "data:";
        if (line.compare(0, prefix.size(), prefix) != 0) {
            return;
        }

        std::string payload = line.substr(prefix.size());
        while (!payload.empty() && payload.front() == ' ') {
            payload.erase(payload.begin());
        }
        if (payload == "[DONE]") {
            state->done = true;
            return;
        }

        try {
            json parsed = json::parse(payload);
            if (parsed.contains("error")) {
                state->failed = true;
                state->error_message = parsed["error"].dump();
                return;
            }
            if (!parsed.contains("choices") || !parsed["choices"].is_array() || parsed["choices"].empty()) {
                return;
            }
            const auto &choice = parsed["choices"][0];
            std::string delta;
            if (choice.contains("delta") && choice["delta"].contains("content") &&
                choice["delta"]["content"].is_string()) {
                delta = choice["delta"]["content"].get<std::string>();
            } else if (choice.contains("text") && choice["text"].is_string()) {
                delta = choice["text"].get<std::string>();
            }
            if (!delta.empty()) {
                if (!state->started) {
                    state->first_token_latency_ms = now_ms() - state->begin_ms;
                }
                state->started = true;
                emit_delta(state->req, delta, false, state->index++);
            }
        } catch (const std::exception &e) {
            state->failed = true;
            state->error_message = e.what();
        }
    }

    void emit_text(const InferenceRequest &req, const std::string &text)
    {
        if (!req.enoutput || !out_callback_) {
            return;
        }
        out_callback_(req.request_id, req.work_id, req.output_url, req.response_format, text, 0, "");
    }

    void emit_delta(const InferenceRequest &req, const std::string &delta, bool finish, int index)
    {
        if (!req.enoutput || !out_callback_) {
            return;
        }
        json data_body;
        data_body["index"] = index;
        data_body["delta"] = finish ? std::string("") : delta;
        data_body["finish"] = finish;
        data_body["action"] = req.action;
        data_body["profile_name"] = req.profile_name;
        data_body["model"] = req.logical_model;
        data_body["served_model"] = req.served_model.empty() ? req.logical_model : req.served_model;
        data_body["endpoint"] = req.endpoint_name;
        data_body["degraded"] = req.degraded;
        if (finish) {
            data_body["queue_wait_ms"] = req.queue_wait_ms;
            data_body["first_token_latency_ms"] = req.first_token_latency_ms;
            data_body["total_latency_ms"] = req.total_latency_ms;
            data_body["retry_count"] = req.retry_count;
        }
        if (!req.degrade_reason.empty()) {
            data_body["degrade_reason"] = req.degrade_reason;
        }
        out_callback_(req.request_id, req.work_id, req.output_url, req.response_format, data_body, 0, "");
    }

    void emit_error(const InferenceRequest &req, int code, const std::string &message)
    {
        if (!out_callback_) {
            return;
        }
        out_callback_(req.request_id, req.work_id, req.output_url, "None", "None", code, message);
    }

    void health_loop()
    {
        while (!stopping_.load()) {
            std::vector<std::shared_ptr<VllmEndpoint>> snapshot;
            {
                std::lock_guard<std::mutex> lock(endpoints_mtx_);
                snapshot = endpoints_;
            }
            for (const auto &ep : snapshot) {
                if (stopping_.load()) {
                    break;
                }
                bool ok = false;
                int64_t latency = 0;
                std::string error;
                ok = check_endpoint_health(ep, &latency, &error);
                update_endpoint_health(ep, ok, latency);
            }
            const int sleep_step_ms = 200;
            int slept = 0;
            int health_check_interval_ms = 0;
            {
                std::lock_guard<std::mutex> config_lock(config_mtx_);
                health_check_interval_ms = health_check_interval_ms_;
            }
            while (!stopping_.load() && slept < health_check_interval_ms) {
                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_step_ms));
                slept += sleep_step_ms;
            }
        }
    }

    bool check_endpoint_health(const std::shared_ptr<VllmEndpoint> &ep, int64_t *latency_ms, std::string *error_message)
    {
        int health_check_timeout_ms = 0;
        {
            std::lock_guard<std::mutex> config_lock(config_mtx_);
            health_check_timeout_ms = health_check_timeout_ms_;
        }
        std::string body;
        long http_code = 0;
        int64_t begin_ms = now_ms();
        std::string health_path = ep->health_path.empty() ? "/health" : ep->health_path;
        if (health_path.front() != '/') {
            health_path.insert(health_path.begin(), '/');
        }
        bool ok = http_get(ep->base_url + health_path, ep->api_key, health_check_timeout_ms, &body, &http_code,
                           error_message);
        if (ok && http_code >= 200 && http_code < 300) {
            *latency_ms = now_ms() - begin_ms;
            return true;
        }

        body.clear();
        http_code = 0;
        ok = http_get(ep->base_url + "/v1/models", ep->api_key, health_check_timeout_ms, &body, &http_code,
                      error_message);
        *latency_ms = now_ms() - begin_ms;
        if (!(ok && http_code >= 200 && http_code < 300)) {
            return false;
        }
        if (ep->served_model.empty()) {
            return true;
        }
        try {
            json parsed = json::parse(body);
            if (parsed.contains("data") && parsed["data"].is_array()) {
                for (const auto &item : parsed["data"]) {
                    if (item.contains("id") && item["id"].is_string() &&
                        item["id"].get<std::string>() == ep->served_model) {
                        return true;
                    }
                }
            }
        } catch (...) {
            return false;
        }
        *error_message = "served model not found in /v1/models: " + ep->served_model;
        return false;
    }

    void update_endpoint_health(const std::shared_ptr<VllmEndpoint> &ep, bool ok, int64_t latency_ms)
    {
        int recovery_threshold = 0;
        int unhealthy_threshold = 0;
        int circuit_failure_threshold = 0;
        int circuit_open_ms = 0;
        {
            std::lock_guard<std::mutex> config_lock(config_mtx_);
            recovery_threshold = recovery_threshold_;
            unhealthy_threshold = unhealthy_threshold_;
            circuit_failure_threshold = circuit_failure_threshold_;
            circuit_open_ms = circuit_open_ms_;
        }

        edge_observability::EndpointMetricsEvent endpoint_event;
        bool have_endpoint_event = false;
        {
            std::lock_guard<std::mutex> lock(endpoints_mtx_);
            ep->last_health_check_ms = now_ms();
            if (latency_ms > 0) {
                ep->last_latency_ms = latency_ms;
                if (ep->avg_latency_ms <= 0.0) {
                    ep->avg_latency_ms = static_cast<double>(latency_ms);
                } else {
                    ep->avg_latency_ms = ep->avg_latency_ms * 0.9 + static_cast<double>(latency_ms) * 0.1;
                }
            }
            if (ok) {
                ep->consecutive_successes++;
                ep->consecutive_failures = 0;
                if (ep->consecutive_successes >= recovery_threshold) {
                    ep->healthy = true;
                    if (ep->circuit_state == CircuitState::HalfOpen || ep->circuit_state == CircuitState::Open) {
                        ep->circuit_state = CircuitState::Closed;
                    }
                }
            } else {
                ep->consecutive_failures++;
                ep->consecutive_successes = 0;
                if (ep->consecutive_failures >= unhealthy_threshold) {
                    ep->healthy = false;
                }
                if (ep->consecutive_failures >= circuit_failure_threshold) {
                    if (ep->circuit_state != CircuitState::Open) {
                        ep->circuit_open_count++;
                    }
                    ep->circuit_state = CircuitState::Open;
                    ep->circuit_open_until_ms = now_ms() + circuit_open_ms;
                }
            }
            endpoint_event = endpoint_metrics_event(*ep);
            have_endpoint_event = true;
        }
        if (have_endpoint_event) {
            observability_.observe_endpoint(endpoint_event);
        }
        endpoint_wait_cv_.notify_all();
    }

    static bool http_get(const std::string &url, const std::string &api_key, int timeout_ms, std::string *body,
                         long *http_code, std::string *error_message)
    {
        CURL *curl = curl_easy_init();
        if (curl == nullptr) {
            *error_message = "curl_easy_init failed";
            return false;
        }
        struct curl_slist *headers = nullptr;
        if (!api_key.empty()) {
            const std::string auth = "Authorization: Bearer " + api_key;
            headers = curl_slist_append(headers, auth.c_str());
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        }
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, static_cast<long>(timeout_ms));
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &vllm_task::write_body_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, body);
        CURLcode code = curl_easy_perform(curl);
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, http_code);
        if (headers != nullptr) {
            curl_slist_free_all(headers);
        }
        curl_easy_cleanup(curl);
        if (code != CURLE_OK) {
            *error_message = curl_easy_strerror(code);
            return false;
        }
        return true;
    }

private:
    std::string work_id_;
    std::mutex reload_mtx_;
    mutable std::mutex config_mtx_;
    std::string model_;
    std::string response_format_;
    std::string prompt_;
    std::string base_url_;
    std::string api_key_;
    std::string endpoint_;
    std::string temperature_;
    std::string last_error_;
    std::string routing_strategy_ = "weighted_least_queue";
    std::string default_action_ = "chat";
    std::vector<std::string> inputs_;
    std::vector<std::string> fallback_chain_;
    std::vector<ModelProfile> model_profiles_;
    bool enoutput_ = true;
    bool enstream_ = false;
    bool mock_mode_ = true;
    bool allow_model_fallback_ = true;
    int timeout_ms_ = 30000;
    int deadline_ms_ = 70000;
    int max_retries_ = 0;
    int max_tokens_ = 1024;
    int worker_count_ = 2;
    int endpoint_wait_ms_ = 2000;
    int max_queue_size_ = 64;
    int low_priority_drop_threshold_ = 48;
    int high_priority_threshold_ = 7;
    int default_priority_ = 5;
    int queue_schedule_slot_ = 0;
    int health_check_interval_ms_ = 5000;
    int health_check_timeout_ms_ = 2000;
    int unhealthy_threshold_ = 3;
    int recovery_threshold_ = 2;
    int circuit_failure_threshold_ = 5;
    int circuit_open_ms_ = 30000;
    std::atomic<int> config_version_{1};
    std::atomic<int64_t> last_config_update_ms_{0};
    std::atomic<int> config_update_count_{0};
    std::atomic<int> status_{static_cast<int>(TaskStatus::Created)};
    std::atomic<bool> stopping_{false};
    std::atomic<int> retry_count_{0};
    std::atomic<int> task_id_counter_{0};
    mutable std::mutex queue_mtx_;
    std::condition_variable queue_cv_;
    std::mutex endpoint_wait_mtx_;
    std::condition_variable endpoint_wait_cv_;
    std::deque<InferenceRequest> pending_;
    mutable std::mutex endpoints_mtx_;
    std::vector<std::shared_ptr<VllmEndpoint>> endpoints_;
    static std::mutex global_endpoints_mtx_;
    static std::unordered_map<std::string, std::weak_ptr<VllmEndpoint>> global_endpoints_;
    MetricsCollector metrics_;
    edge_observability::ObservabilityMetrics observability_;
    mutable std::mutex records_mtx_;
    std::deque<RequestRecord> records_;
    size_t max_task_records_ = 1000;
    std::vector<std::thread> workers_;
    std::thread health_worker_;
    task_output_t out_callback_;
};

std::mutex vllm_task::global_endpoints_mtx_;
std::unordered_map<std::string, std::weak_ptr<VllmEndpoint>> vllm_task::global_endpoints_;

struct SessionContext {
    int work_id_num = 0;
    std::string work_id;
    std::string model;
    std::string response_format = "llm.utf-8.stream";
    std::string prompt;
    std::string default_action = "chat";
    bool enoutput = true;
    bool enstream = true;
    int default_max_tokens = 0;
    int default_timeout_ms = 0;
    int default_deadline_ms = 0;
    int default_max_retries = -1;
    int default_priority = -1;
    bool allow_model_fallback = true;
};

class vllm_llm : public StackFlow {
public:
    vllm_llm() : StackFlow("llm")
    {
        rpc_ctx_->register_rpc_action(
            "reload_config", std::bind(&vllm_llm::_rpc_reload_config, this, std::placeholders::_1,
                                       std::placeholders::_2));
        rpc_ctx_->register_rpc_action(
            "metrics_query", std::bind(&vllm_llm::_rpc_metrics_query, this, std::placeholders::_1,
                                       std::placeholders::_2));
    }

    int setup(const std::string &work_id, const std::string &object, const std::string &data) override
    {
        (void)object;
        int work_id_num = sample_get_work_id_num(work_id);

        json config_body;
        try {
            config_body = json::parse(data);
        } catch (...) {
            send_error(work_id, -2, "json format error.");
            return -2;
        }

        SessionContext session = make_session_context(work_id_num, work_id, config_body);
        {
            std::lock_guard<std::mutex> lock(sessions_mtx_);
            if (sessions_.find(work_id_num) != sessions_.end()) {
                send_error(work_id, -22, "session already exists");
                return -1;
            }
            if (sessions_.size() >= max_sessions_) {
                send_error(work_id, -21, "session full");
                return -1;
            }
            if (!shared_task_) {
                auto new_task = std::make_shared<vllm_task>("shared");
                if (new_task->load_model(config_body) != 0) {
                    send_error(work_id, -5,
                               new_task->last_error().empty() ? "Model loading failed." : new_task->last_error());
                    return -1;
                }
                new_task->set_output(std::bind(&vllm_llm::task_output, this, std::placeholders::_1,
                                               std::placeholders::_2, std::placeholders::_3, std::placeholders::_4,
                                               std::placeholders::_5, std::placeholders::_6, std::placeholders::_7));
                new_task->start();
                shared_task_ = new_task;
            }
            sessions_[work_id_num] = session;
        }

        auto llm_channel = get_channel(work_id);
        llm_channel->set_output(session.enoutput);
        llm_channel->set_stream(session.enstream);
        llm_channel->subscriber_work_id(
            "",
            std::bind(&vllm_llm::task_user_data, this, std::weak_ptr<llm_channel_obj>(llm_channel),
                      std::placeholders::_1, std::placeholders::_2));
        send("None", "None", LLM_NO_ERROR, work_id);
        return 0;
    }

    void taskinfo(const std::string &work_id, const std::string &object, const std::string &data) override
    {
        (void)object;
        json query = json::object();
        if (!data.empty() && data != "None") {
            try {
                query = json::parse(data);
            } catch (...) {
                query = json::object();
            }
        }

        json req_body;
        std::shared_ptr<vllm_task> task;
        int work_id_num = sample_get_work_id_num(work_id);
        {
            std::lock_guard<std::mutex> lock(sessions_mtx_);
            task = shared_task_;
            req_body["session_count"] = sessions_.size();
            req_body["max_sessions"] = max_sessions_;
            req_body["shared_scheduler_initialized"] = static_cast<bool>(shared_task_);
            if (WORK_ID_NONE == work_id_num) {
                json session_list = json::array();
                for (const auto &item : sessions_) {
                    session_list.push_back(session_to_json(item.second));
                }
                req_body["sessions"] = session_list;
            } else {
                auto session_it = sessions_.find(work_id_num);
                if (session_it == sessions_.end()) {
                    send_error(work_id, -6, "Unit Does Not Exist");
                    return;
                }
                req_body["session"] = session_to_json(session_it->second);
            }
        }
        if (task) {
            req_body["shared_scheduler"] = task->taskinfo(query);
        } else {
            req_body["shared_scheduler"] = json{{"status", "not_initialized"}};
        }
        send(WORK_ID_NONE == work_id_num ? "llm.tasklist" : "llm.taskinfo", req_body, LLM_NO_ERROR, work_id);
    }

    int exit(const std::string &work_id, const std::string &object, const std::string &data) override
    {
        (void)object;
        (void)data;
        int work_id_num = sample_get_work_id_num(work_id);
        {
            std::lock_guard<std::mutex> lock(sessions_mtx_);
            if (sessions_.find(work_id_num) == sessions_.end()) {
                send_error(work_id, -6, "Unit Does Not Exist");
                return -1;
            }
            sessions_.erase(work_id_num);
        }

        auto llm_channel = get_channel(work_id_num);
        llm_channel->stop_subscriber("");
        send("None", "None", LLM_NO_ERROR, work_id);
        return 0;
    }

    ~vllm_llm()
    {
        std::vector<int> work_ids;
        std::shared_ptr<vllm_task> task;
        {
            std::lock_guard<std::mutex> lock(sessions_mtx_);
            for (const auto &item : sessions_) {
                work_ids.push_back(item.first);
            }
            sessions_.clear();
            task = shared_task_;
            shared_task_.reset();
        }
        for (const auto work_id_num : work_ids) {
            try {
                get_channel(work_id_num)->stop_subscriber("");
            } catch (...) {
            }
        }
        if (task) {
            task->stop();
        }
    }

private:
    std::string _rpc_metrics_query(pzmq *_pzmq, const std::shared_ptr<pzmq_data> &data)
    {
        (void)_pzmq;
        std::string zmq_url = data->get_param(0);
        std::string raw = data->get_param(1);
        request_id_ = sample_json_str_get(raw, "request_id");
        out_zmq_url_ = zmq_url;
        metrics_query(zmq_url, raw);
        return std::string("None");
    }

    void metrics_query(const std::string &zmq_url, const std::string &raw)
    {
        out_zmq_url_ = zmq_url;
        json root;
        std::string work_id = "None";
        try {
            root = json::parse(raw);
            work_id = json_string_value(root, "work_id", work_id);
            if (work_id.empty()) {
                work_id = "None";
            }
        } catch (...) {
            send_error(work_id, -2, "json format error.");
            return;
        }

        json query_body = json::object();
        try {
            if (root.contains("data")) {
                const auto &data = root["data"];
                if (data.is_object()) {
                    query_body = data;
                } else if (data.is_string()) {
                    const std::string data_str = data.get<std::string>();
                    if (!data_str.empty() && data_str != "None") {
                        query_body = json::parse(data_str);
                    }
                } else if (!data.is_null()) {
                    send_error(work_id, -2, "metrics_query data must be a json object or json string.");
                    return;
                }
            } else {
                query_body = root;
            }
        } catch (...) {
            send_error(work_id, -2, "metrics_query data json format error.");
            return;
        }

        std::shared_ptr<vllm_task> task;
        {
            std::lock_guard<std::mutex> lock(sessions_mtx_);
            task = shared_task_;
        }
        if (!task) {
            send_error(work_id, -45, "shared scheduler not initialized");
            return;
        }

        send("llm.metrics", task->metrics_query(query_body), LLM_NO_ERROR, work_id);
    }

    std::string _rpc_reload_config(pzmq *_pzmq, const std::shared_ptr<pzmq_data> &data)
    {
        (void)_pzmq;
        std::string zmq_url = data->get_param(0);
        std::string raw = data->get_param(1);
        request_id_ = sample_json_str_get(raw, "request_id");
        out_zmq_url_ = zmq_url;
        reload_config(zmq_url, raw);
        return std::string("None");
    }

    void reload_config(const std::string &zmq_url, const std::string &raw)
    {
        out_zmq_url_ = zmq_url;
        json root;
        std::string work_id = "None";
        try {
            root = json::parse(raw);
            work_id = json_string_value(root, "work_id", work_id);
            if (work_id.empty()) {
                work_id = "None";
            }
        } catch (...) {
            send_error(work_id, -2, "json format error.");
            return;
        }

        json config_body = json::object();
        try {
            if (root.contains("data")) {
                const auto &data = root["data"];
                if (data.is_object()) {
                    config_body = data;
                } else if (data.is_string()) {
                    const std::string data_str = data.get<std::string>();
                    if (!data_str.empty() && data_str != "None") {
                        config_body = json::parse(data_str);
                    }
                } else if (!data.is_null()) {
                    send_error(work_id, -2, "reload_config data must be a json object or json string.");
                    return;
                }
            } else {
                config_body = root;
            }
        } catch (...) {
            send_error(work_id, -2, "reload_config data json format error.");
            return;
        }

        std::shared_ptr<vllm_task> task;
        {
            std::lock_guard<std::mutex> lock(sessions_mtx_);
            task = shared_task_;
        }
        if (!task) {
            send_error(work_id, -45, "shared scheduler not initialized");
            return;
        }

        std::string error_message;
        if (!task->reload_config(config_body, &error_message)) {
            send_error(work_id, -46, error_message.empty() ? "reload_config failed" : error_message);
            return;
        }

        json out_body;
        out_body["config_version"] = task->config_version();
        out_body["config_update_count"] = task->config_update_count();
        send("llm.reload_config", out_body, LLM_NO_ERROR, work_id);
    }

    int send_error(const std::string &work_id, int code, const std::string &message)
    {
        json out_body;
        out_body["request_id"] = request_id_;
        out_body["work_id"] = work_id;
        out_body["created"] = time(NULL);
        out_body["object"] = "None";
        out_body["data"] = "None";
        out_body["error"]["code"] = code;
        out_body["error"]["message"] = message;
        pzmq _zmq(out_zmq_url_, ZMQ_PUSH);
        std::string out = out_body.dump();
        out += "\n";
        return _zmq.send_data(out);
    }

    static std::string scheduler_config_signature(const json &config_body)
    {
        static const std::vector<std::string> keys = {
            "endpoints",
            "vllm_endpoints",
            "model_profiles",
            "fallback_chain",
            "degrade_policy",
            "vllm_base_url",
            "api_key",
            "endpoint",
            "routing_strategy",
            "worker_count",
            "endpoint_wait_ms",
            "max_queue_size",
            "low_priority_drop_threshold",
            "high_priority_threshold",
            "health_check_interval_ms",
            "health_check_timeout_ms",
            "unhealthy_threshold",
            "recovery_threshold",
            "circuit_failure_threshold",
            "circuit_open_ms",
            "mock",
            "temperature",
            "observability",
            "observability_enabled",
        };
        json signature = json::object();
        for (const auto &key : keys) {
            if (config_body.contains(key)) {
                signature[key] = config_body.at(key);
            }
        }
        return signature.empty() ? std::string() : signature.dump();
    }

    static SessionContext make_session_context(int work_id_num, const std::string &work_id, const json &config_body)
    {
        SessionContext session;
        session.work_id_num = work_id_num;
        session.work_id = work_id;
        session.model = json_string_value(config_body, "model");
        session.response_format = json_string_value(config_body, "response_format", "llm.utf-8.stream");
        session.prompt = json_string_value(config_body, "prompt");
        session.default_action = json_string_value(config_body, "default_action", "chat");
        session.enoutput = json_bool_value(config_body, "enoutput", true);
        session.enstream = session.response_format.find("stream") != std::string::npos;
        session.enstream = json_bool_value(config_body, "stream", json_bool_value(config_body, "enstream", session.enstream));
        session.default_max_tokens =
            json_int_value(config_body, "max_tokens", json_int_value(config_body, "max_token_len", 0));
        session.default_timeout_ms = json_int_value(config_body, "timeout_ms", 0);
        session.default_deadline_ms = json_int_value(config_body, "deadline_ms", 0);
        session.default_max_retries =
            json_int_value(config_body, "max_retries", json_int_value(config_body, "retry_count", -1));
        session.default_priority = json_int_value(config_body, "priority", -1);
        if (config_body.contains("degrade_policy") && config_body["degrade_policy"].is_object()) {
            session.allow_model_fallback = json_bool_value(config_body["degrade_policy"], "allow_model_fallback", true);
        } else {
            session.allow_model_fallback = json_bool_value(config_body, "allow_model_fallback", true);
        }
        return session;
    }

    static void apply_session_defaults(json *meta, const SessionContext &session)
    {
        if (!session.model.empty() && !meta->contains("model")) {
            (*meta)["model"] = session.model;
        }
        if (session.default_max_tokens > 0 && !meta->contains("max_tokens") && !meta->contains("max_token_len")) {
            (*meta)["max_tokens"] = session.default_max_tokens;
        }
        if (session.default_timeout_ms > 0 && !meta->contains("timeout_ms")) {
            (*meta)["timeout_ms"] = session.default_timeout_ms;
        }
        if (session.default_deadline_ms > 0 && !meta->contains("deadline_ms")) {
            (*meta)["deadline_ms"] = session.default_deadline_ms;
        }
        if (session.default_max_retries >= 0 && !meta->contains("max_retries")) {
            (*meta)["max_retries"] = session.default_max_retries;
        }
        if (session.default_priority >= 0 && !meta->contains("priority")) {
            (*meta)["priority"] = session.default_priority;
        }
        if (!meta->contains("allow_model_fallback") && !meta->contains("allow_degrade")) {
            (*meta)["allow_model_fallback"] = session.allow_model_fallback;
        }
    }

    static json session_to_json(const SessionContext &session)
    {
        json body;
        body["work_id_num"] = session.work_id_num;
        body["work_id"] = session.work_id;
        body["model"] = session.model;
        body["response_format"] = session.response_format;
        body["default_action"] = session.default_action;
        body["enoutput"] = session.enoutput;
        body["enstream"] = session.enstream;
        body["default_max_tokens"] = session.default_max_tokens;
        body["default_timeout_ms"] = session.default_timeout_ms;
        body["default_deadline_ms"] = session.default_deadline_ms;
        body["default_max_retries"] = session.default_max_retries;
        body["default_priority"] = session.default_priority;
        body["allow_model_fallback"] = session.allow_model_fallback;
        return body;
    }

    struct StreamInputState {
        std::mutex mtx;
        std::unordered_map<int, std::string> chunks;
        std::atomic<int64_t> updated_ms{0};
        size_t total_bytes = 0;
    };

    static constexpr int64_t kStreamInputTtlMs = 60000;
    static constexpr size_t kMaxStreamInputChunks = 4096;
    static constexpr size_t kMaxStreamInputBytes = 8 * 1024 * 1024;

    void cleanup_expired_stream_states_locked(int64_t current_ms)
    {
        for (auto it = stream_states_.begin(); it != stream_states_.end();) {
            auto state = it->second;
            if (!state || current_ms - state->updated_ms.load() > kStreamInputTtlMs) {
                it = stream_states_.erase(it);
            } else {
                ++it;
            }
        }
    }

    std::shared_ptr<StreamInputState> stream_input_state(const std::string &stream_key, int64_t current_ms)
    {
        std::lock_guard<std::mutex> lock(stream_states_mtx_);
        cleanup_expired_stream_states_locked(current_ms);
        auto &state = stream_states_[stream_key];
        if (!state) {
            state = std::make_shared<StreamInputState>();
            state->updated_ms.store(current_ms);
        }
        return state;
    }

    void erase_stream_input_state(const std::string &stream_key, const std::shared_ptr<StreamInputState> &state)
    {
        std::lock_guard<std::mutex> lock(stream_states_mtx_);
        auto it = stream_states_.find(stream_key);
        if (it != stream_states_.end() && it->second == state) {
            stream_states_.erase(it);
        }
    }

    void erase_stream_input_state(const std::string &stream_key)
    {
        std::lock_guard<std::mutex> lock(stream_states_mtx_);
        stream_states_.erase(stream_key);
    }

    static int stream_chunk_index(const json &meta)
    {
        if (!meta.contains("index")) {
            throw std::runtime_error("missing stream index");
        }
        const auto &value = meta.at("index");
        if (value.is_number_integer()) {
            return value.get<int>();
        }
        if (value.is_string()) {
            return std::stoi(value.get<std::string>());
        }
        throw std::runtime_error("invalid stream index");
    }

    bool assemble_stream_input(const std::string &stream_key, const json &meta, std::string *out,
                               std::string *error_message)
    {
        const int index = stream_chunk_index(meta);
        if (index < 0) {
            *error_message = "stream index must be non-negative";
            return false;
        }
        const bool finish = json_bool_value(meta, "finish", false);
        const std::string delta = json_string_value(meta, "delta");
        const int64_t current_ms = now_ms();
        auto state = stream_input_state(stream_key, current_ms);

        std::unordered_map<int, std::string> completed_chunks;
        size_t completed_bytes = 0;
        {
            std::lock_guard<std::mutex> lock(state->mtx);
            auto old_chunk = state->chunks.find(index);
            if (old_chunk != state->chunks.end()) {
                state->total_bytes -= old_chunk->second.size();
            }
            state->chunks[index] = delta;
            state->total_bytes += delta.size();
            state->updated_ms.store(current_ms);

            if (state->chunks.size() > kMaxStreamInputChunks) {
                *error_message = "stream input has too many chunks";
                return false;
            }
            if (state->total_bytes > kMaxStreamInputBytes) {
                *error_message = "stream input is too large";
                return false;
            }
            if (!finish) {
                return true;
            }

            completed_bytes = state->total_bytes;
            completed_chunks.swap(state->chunks);
            state->total_bytes = 0;
        }

        erase_stream_input_state(stream_key, state);
        out->clear();
        out->reserve(completed_bytes);
        for (size_t i = 0; i < completed_chunks.size(); ++i) {
            auto chunk = completed_chunks.find(static_cast<int>(i));
            if (chunk == completed_chunks.end()) {
                *error_message = "stream input missing chunk index " + std::to_string(i);
                return false;
            }
            *out += chunk->second;
        }
        return false;
    }

    void task_user_data(const std::weak_ptr<llm_channel_obj> &channel_weak, const std::string &object,
                        const std::string &data)
    {
        auto channel = channel_weak.lock();
        if (!channel) {
            return;
        }
        std::shared_ptr<vllm_task> task;
        SessionContext session;
        {
            std::lock_guard<std::mutex> lock(sessions_mtx_);
            task = shared_task_;
            auto session_it = sessions_.find(sample_get_work_id_num(channel->work_id_));
            if (!(task && session_it != sessions_.end())) {
                send_client_message(channel->request_id_, channel->work_id_, channel->output_url_, "None", "None",
                                    -6, "Unit Does Not Exist");
                return;
            }
            session = session_it->second;
        }
        if (data.empty() || data == "None") {
            send_client_message(channel->request_id_, channel->work_id_, channel->output_url_, "None", "None", -24,
                                "The inference data is empty.");
            return;
        }

        const std::string *next_data = &data;
        std::string tmp_msg;
        json request_meta = json::object();
        try {
            request_meta = json::parse(data);
        } catch (...) {
            request_meta = json::object();
        }
        if (object.find("stream") != std::string::npos) {
            const std::string stream_key = channel->work_id_ + ":" + channel->request_id_;
            std::string stream_error;
            try {
                if (assemble_stream_input(stream_key, request_meta, &tmp_msg, &stream_error)) {
                    return;
                }
            } catch (...) {
                stream_error = "Stream data index error.";
            }
            if (!stream_error.empty()) {
                erase_stream_input_state(stream_key);
                send_client_message(channel->request_id_, channel->work_id_, channel->output_url_, "None", "None",
                                    -25, stream_error);
                return;
            }
            next_data = &tmp_msg;
        }

        apply_session_defaults(&request_meta, session);
        RequestOptions options;
        options.response_format = session.response_format;
        options.system_prompt = session.prompt;
        options.default_action = session.default_action;
        options.enoutput = session.enoutput;
        options.enstream = session.enstream;
        task->inference(channel->request_id_, channel->work_id_, channel->output_url_, object, *next_data,
                        request_meta, options);
    }

    void task_output(const std::string &request_id, const std::string &work_id, const std::string &output_url,
                     const std::string &object, const json &data, int error_code, const std::string &error_message)
    {
        send_client_message(request_id, work_id, output_url, object, data, error_code, error_message);
    }

    static void send_client_message(const std::string &request_id, const std::string &work_id,
                                    const std::string &output_url, const std::string &object, const json &data,
                                    int error_code, const std::string &error_message)
    {
        if (output_url.empty()) {
            return;
        }
        json out_body;
        out_body["request_id"] = request_id;
        out_body["work_id"] = work_id;
        out_body["created"] = time(NULL);
        out_body["object"] = object;
        out_body["data"] = data;
        out_body["error"]["code"] = error_code;
        out_body["error"]["message"] = error_message;
        std::string out = out_body.dump();
        out += "\n";
        llm_channel_obj::send_raw_for_url(output_url, out);
    }

private:
    const size_t max_sessions_ = 1024;
    std::shared_ptr<vllm_task> shared_task_;
    std::unordered_map<int, SessionContext> sessions_;
    std::mutex sessions_mtx_;
    std::mutex stream_states_mtx_;
    std::unordered_map<std::string, std::shared_ptr<StreamInputState>> stream_states_;
    std::string scheduler_config_signature_;
};

int main(int argc, char *argv[])
{
    (void)argc;
    (void)argv;
    raise_open_file_limit(65535);
    signal(SIGTERM, __sigint);
    signal(SIGINT, __sigint);
    mkdir("/tmp/llm", 0777);
    curl_global_init(CURL_GLOBAL_DEFAULT);
    {
        vllm_llm llm;
        while (!main_exit_flage) {
            sleep(1);
        }
    }
    curl_global_cleanup();
    return 0;
}
