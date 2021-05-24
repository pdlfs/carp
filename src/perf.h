//
// Created by Ankush J on 2/1/21.
//

#pragma once

#include <map>

#define MICROS(us) ((us)*1e-3)

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
      intvl_total += PrintSingleStat(it->first);
    }

    logf(LOG_INFO, "Event TOTAL: %.2lf ms\n", MICROS(intvl_total));
  }

  uint64_t PrintSingleStat(const char* evt_name) {
    uint64_t event_delta_us = GetEventDelta(evt_name);
    logf(LOG_INFO, "Event %s: %.2lf ms\n", evt_name, MICROS(event_delta_us));
    return event_delta_us;
  }

  Status LogQuery(const char* dir_path, int epoch, float qbeg, float qend,
                  float qsel) {
    Status s = Status::OK();

    const char* events[] = {"SSTREAD", "SORT"};
    size_t n_events = sizeof(events) / sizeof(char*);

    std::string ts_str;

    for (size_t i = 0; i < n_events; i++) {
      uint64_t event_delta = GetEventDelta(events[i]);
      if (!ts_str.empty()) {
        ts_str += ",";
      }

      ts_str += std::to_string(event_delta);
    }

    char log_buf[1024];
    size_t log_bufsz = snprintf(log_buf, 1024, "%s,%d,%f,%f,%f,%s\n", dir_path,
                                epoch, qbeg, qend, qsel, ts_str.c_str());

    /* ugly, but pdlfs::env doesn't support appendable files yet? */
    const char* const kQueryFname = "querylog.csv";
    std::string data;
    if (env_->FileExists(kQueryFname)) {
      s = ReadFileToString(env_, kQueryFname, &data);
      if (!s.ok()) return s;
    } else {
      data = "plfspath,epoch,qbegin,qend,qselectivity,qreadus,qsortus\n";
    }

    data += std::string(log_buf, log_bufsz);
    Slice log_sl(data);
    s = WriteStringToFile(env_, log_sl, kQueryFname);

    return s;
  }

 private:
  uint64_t GetEventDelta(const char* event) {
    uint64_t res = 0;
#define MAP_HAS(m, k) ((m).find(k) != (m).end())
    if (MAP_HAS(ts_begin_, event) and MAP_HAS(ts_end_, event)) {
      res = ts_end_[event] - ts_begin_[event];
    }
#undef MAP_HAS
    return res;
  }
  Env* const env_;
  std::map<const char*, uint64_t> ts_begin_;
  std::map<const char*, uint64_t> ts_end_;
};
}  // namespace plfsio
}  // namespace pdlfs
