//
// Created by Ankush J on 2/3/21.
//

#pragma once

namespace pdlfs {
namespace plfsio {
class TaskCompletionTracker {
 public:
  TaskCompletionTracker() : cv_(&mutex_), tasks_completed_(0) {}

  void Reset() { tasks_completed_ = 0; }

  void MarkCompleted() {
    mutex_.Lock();
    tasks_completed_++;
    cv_.Signal();
    mutex_.Unlock();
  }

  void WaitUntilCompleted(uint32_t target) {
    mutex_.Lock();
    while (tasks_completed_ < target) {
      cv_.TimedWait(1e3);
//      logf(LOG_INFO, "Waiting... \n");
    }
    mutex_.Unlock();
  }

 private:
  port::Mutex mutex_;
  port::CondVar cv_;
  uint32_t tasks_completed_;
};
}  // namespace plfsio
}  // namespace pdlfs