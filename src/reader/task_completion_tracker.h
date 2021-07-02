//
// Created by Ankush J on 2/3/21.
//

#pragma once

namespace pdlfs {
namespace plfsio {
class TaskCompletionTracker {
#define MAP_HAS(m, k) ((m).find(k) != (m).end())
 public:
  explicit TaskCompletionTracker(Env* env)
      : cv_(&mutex_), env_(env), tasks_completed_(0) {}

  void Reset() {
    tasks_completed_ = 0;
    ts_beg_map_.clear();
    ts_io_map_.clear();
    time_io_.clear();
    time_total_.clear();
  }

  int MarkBegin(pid_t tid) {
    MutexLock ml(&mutex_);
    int id;

    do {
      id = rand();
    } while (MAP_HAS(ts_beg_map_, id));

    tid_rid_map_[tid] = id;
    ts_beg_map_[id] = env_->NowMicros();

    return id;
  }

  void MarkIOCompleted(int id) {
    MutexLock ml(&mutex_);
    ts_io_map_[id] = env_->NowMicros();
  }

  void MarkCompleted(int id) {
    MutexLock ml(&mutex_);
    tasks_completed_++;

    if (id && MAP_HAS(ts_beg_map_, id)) {
      uint64_t now = env_->NowMicros();
      uint64_t cur_io = ts_io_map_[id] - ts_beg_map_[id];
      uint64_t cur_total = now - ts_beg_map_[id];
      time_io_.push_back(cur_io);
      time_total_.push_back(cur_total);

      pid_t tid = tid_rid_map_[id];
      if (!MAP_HAS(tid_total_map_, tid)) {
        tid_total_map_[tid] = 0;
      }

      tid_total_map_[tid] += cur_total;
    }

    cv_.Signal();
  }

  void WaitUntilCompleted(uint32_t target) {
    MutexLock ml(&mutex_);
    while (tasks_completed_ < target) {
      cv_.TimedWait(1e3);
      //      logf(LOG_INFO, "Waiting... \n");
    }
  }

  void AnalyzeTimes() {
    uint64_t io_sum = VecSum(time_io_);
    uint64_t total_sum = VecSum(time_total_);

    logf(LOG_INFO, "IO Sum: %.1fs, Total Sum: %.1fs", io_sum / 1e6,
         total_sum / 1e6);

    std::vector<uint64_t> all_times;
    uint64_t all_total = 0;
    std::map<pid_t, uint64_t>::const_iterator it = tid_total_map_.cbegin();
    for (; it != tid_total_map_.cend(); it++) {
      uint64_t time_sec = it->second / 1e6;
      all_times.push_back(time_sec);
      all_total += time_sec;
    }

    std::sort(all_times.begin(), all_times.end());
    std::string all_times_str;

    for (size_t i = 0; i < all_times.size(); i++) {
      all_times_str += std::to_string(all_times[i]) + " ";
    }

    logf(LOG_INFO,
         "Thread-wise times, avg: %.1fs\n"
         "Individual times: %s",
         all_total * 1.0f / all_times.size(), all_times_str.c_str());
  }

 private:
  uint64_t VecSum(const std::vector<uint64_t>& v) const {
    uint64_t sum = 0;
    uint64_t sum_sq = 0;
    for (size_t i = 0; i < v.size(); i++) {
      sum += v[i];
      sum_sq += v[i] * v[i];
    }

    double n = v.size();
    double avg = sum / n;
    double var = (sum_sq / n) - (avg * avg);
    double std = pow(var, 0.5);

    logf(LOG_INFO, "Vec Stats, Avg: %.1lf, Std: %.1lf", avg, std);

    return sum;
  }

  port::Mutex mutex_;
  port::CondVar cv_;
  Env* const env_;
  uint32_t tasks_completed_;
  std::map<int, uint64_t> ts_beg_map_;
  std::map<int, uint64_t> ts_io_map_;
  std::map<pid_t, int> tid_rid_map_;
  std::map<pid_t, uint64_t> tid_total_map_;
  std::vector<uint64_t> time_io_;
  std::vector<uint64_t> time_total_;
};
}  // namespace plfsio
}  // namespace pdlfs
