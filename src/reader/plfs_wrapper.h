//
// Created by Ankush on 5/12/2021.
//

#pragma once

#include <fcntl.h>
#include <deltafs/deltafs_api.h>
#include <pdlfs-common/status.h>

struct PlfsOpts;

namespace pdlfs {
namespace plfsio {
struct PlfsOpts {
  int key_size;
  int bits_per_key;
  const char* memtable_size;
  const char* env;
  int force_leveldb_format;
  int unordered_storage;
  int skip_checksums;
  int io_engine;
  int particle_size;
  int particle_buf_size;
  int particle_count;
  int bgdepth;
  int pre_flushing_wait;
  int pre_flushing_sync;
};

class PlfsWrapper {
 public:
  PlfsWrapper()
      : epoch_(0),
        plfsparts_(0),
        plfstp_(nullptr),
        plfsenv_(nullptr),
        plfshdl_(nullptr) {}

  Status OpenDir(const char* path);

  bool IsOpen() const { return plfshdl_ != nullptr; }

  // opendir
  Status EpochFlush() {
    Status s = Status::OK();
    int rv = deltafs_plfsdir_epoch_flush(plfshdl_, epoch_);
    if (rv != 0) {
      s = Status::IOError("epoch_flush: failed");
      return s;
    }

    epoch_++;
    return s;
  }

  // closedir
  Status Flush() {
    Status s = Status::OK();
    int rv = deltafs_plfsdir_flush(plfshdl_, epoch_);
    if (rv != 0) {
      s = Status::IOError("epoch_flush: failed");
      return s;
    }

    if (opts_.pre_flushing_wait) {
      rv = deltafs_plfsdir_wait(plfshdl_);
      if (rv != 0) {
        s = Status::IOError("pre_flushing_wait: failed");
        return s;
      }
    }

    if (opts_.pre_flushing_sync) {
      rv = deltafs_plfsdir_sync(plfshdl_);
      if (rv != 0) {
        s = Status::IOError("pre_flushing_sync: failed");
        return s;
      }
    }

    return s;
  }

  Status Append(float key, Slice& val) {
    Status s = Status::OK();
    char* key_ptr = (char*)(&key);
    ssize_t n = deltafs_plfsdir_append(plfshdl_, key_ptr, epoch_, val.data(),
                                       val.size());
    if (n != val.size()) {
      s = Status::IOError("plfsdir write failed");
    }

    return s;
  }

  Status CloseDir() {
    Status s = Status::OK();
    if (plfshdl_ == nullptr) {
      s = Status::InvalidFileDescriptor("plfshdl: is nullptr");
      return s;
    }

    deltafs_plfsdir_finish(plfshdl_);
    deltafs_plfsdir_free_handle(plfshdl_);

    if (plfsenv_ != nullptr) {
      deltafs_env_close(plfsenv_);
      plfsenv_ = nullptr;
    }

    if (plfstp_ != nullptr) {
      deltafs_tp_close(plfstp_);
      plfstp_ = nullptr;
    }

    plfshdl_ = nullptr;

    return s;
  }

 private:
  Status OpenDirInternal(const char* path);

  PlfsOpts opts_;
  int epoch_;
  int plfsparts_;
  deltafs_tp* plfstp_;
  deltafs_env* plfsenv_;
  deltafs_plfsdir* plfshdl_;
};
}  // namespace plfsio
}  // namespace pdlfs
