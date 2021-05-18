//
// Created by Ankush on 5/17/2021.
//

#pragma once

namespace pdlfs {
namespace plfsio {

class PlfsWriter {
 public:
  PlfsWriter()
      : path_(nullptr),
        rank_(0),
        epoch_(0),
        cur_rank_wrcnt_(0),
        plfs_(nullptr) {}

  Status OpenDir(const char* path) {
    Status s = Status::OK();
    path_ = path;
    plfs_ = nullptr;
    s = AdvanceDir();
    return s;
  }

  bool IsOpen() const { return (plfs_ != nullptr); }

  Status EpochFlush() {
    Status s = Status::OK();
    if (plfs_) s = plfs_->EpochFlush();
    epoch_++;
    return s;
  }

  Status Flush() {
    Status s = Status::OK();
    if (plfs_) s = plfs_->Flush();
    return s;
  }

  Status Append(float key, Slice& val) {
    Status s = Status::OK();
    if (plfs_) {
      s = plfs_->Append(key, val);
      cur_rank_wrcnt_++;
    }

    if (cur_rank_wrcnt_ == kMaxRankWrcnt) {
      s = AdvanceDir();
    }

    return s;
  }

  Status CloseDir() {
    Status s = Status::OK();

    if (plfs_) {
      s = plfs_->Flush();
      if (!s.ok()) return s;

      s = plfs_->CloseDir();
      if (!s.ok()) return s;

      delete plfs_;
      plfs_ = nullptr;
    }

    return s;
  }

 private:
  Status AdvanceDir() {
    Status s = Status::OK();

    s = CloseDir();
    if (!s.ok()) return s;

    plfs_ = new PlfsWrapper();
    plfs_->OpenDir(path_, rank_);
    cur_rank_wrcnt_ = 0;
    rank_++;

    for (int advcnt = 0; advcnt < epoch_ + 1; advcnt++) {
      plfs_->EpochFlush();
    }

    return s;
  }

  const char* path_;
  int rank_;
  int epoch_;
  uint64_t cur_rank_wrcnt_;
#define MILLION(n) (1000000 * (n))
  static const uint64_t kMaxRankWrcnt = MILLION(200);
#undef MILLION
  PlfsWrapper* plfs_;
};
}  // namespace plfsio

}  // namespace pdlfs
