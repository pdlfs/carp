//
// Created by Ankush J on 2/1/21.
//

#pragma once
namespace pdlfs {
namespace plfsio {
class RangeReaderPerfLogger {
 public:
  explicit RangeReaderPerfLogger(Env* env) : env_(env){};

  void RegisterBegin(const char* key) { ts_begin_[key] = env_->NowMicros(); }

  void RegisterEnd(const char* key) { ts_end_[key] = env_->NowMicros(); }

  void PrintStats() {
    std::map<const char*, uint64_t>::iterator it = ts_begin_.begin();

    uint64_t intvl_total = 0;

    for (; it != ts_begin_.end(); it++) {
      const char* key = it->first;
      uint64_t intvl_us = ts_end_[key] - ts_begin_[key];
      intvl_total += intvl_us;

      logf(LOG_INFO, "Event %s: %.2lf ms\n", it->first, intvl_us * 1e-3);
    }
    logf(LOG_INFO, "Event TOTAL: %.2lf ms\n", intvl_total * 1e-3);
  }

 private:
  Env* const env_;
  std::map<const char*, uint64_t> ts_begin_;
  std::map<const char*, uint64_t> ts_end_;
};
}  // namespace plfsio
}  // namespace pdlfs
