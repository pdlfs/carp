//
// Created by Ankush on 5/12/2021.
//

#include "plfs_wrapper.h"

#include "common.h"

static void plfsdir_error_printer(const char* msg, void*) {
  logf(LOG_ERRO, msg);
}

static std::string gen_plfsdir_conf(pdlfs::plfsio::PlfsOpts& dirc, int rank) {
  char tmp[500];
  int n;

  n = snprintf(tmp, sizeof(tmp), "rank=%d", rank);

  n += snprintf(tmp + n, sizeof(tmp) - n, "&key_size=%d", dirc.key_size);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&value_size=%d", dirc.particle_size);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&memtable_size=%s",
                dirc.memtable_size);
  n += snprintf(tmp + n, sizeof(tmp) - n, "&bf_bits_per_key=%d",
                dirc.bits_per_key);

  return tmp;
}

namespace pdlfs {
namespace plfsio {

Status PlfsWrapper::OpenDir(const char* path) {
  Status s = Status::OK();

  opts_.key_size = 4;
  opts_.bits_per_key = 14;
  opts_.memtable_size = "48MiB";
  opts_.env = "posix.unbufferedio";
  opts_.force_leveldb_format = 0;
  opts_.unordered_storage = 0;
  opts_.skip_checksums = 1;
  opts_.io_engine = DELTAFS_PLFSDIR_RANGEDB;
  // XXX: tmp, hardcoded
  opts_.particle_size = 60;
  opts_.particle_buf_size = 2097152;
  opts_.particle_count = 1;  // not used
  opts_.bgdepth = 4;
  opts_.pre_flushing_wait = 0;
  opts_.pre_flushing_sync = 0;

  OpenDirInternal(path);
  return s;
}

Status PlfsWrapper::OpenDirInternal(const char* path) {
  Status s = Status::OK();
  int rv = 0;

  std::string conf;
  conf = gen_plfsdir_conf(opts_, /*rank*/ 0);
  plfshdl_ =
      deltafs_plfsdir_create_handle(conf.c_str(), O_WRONLY, opts_.io_engine);
  deltafs_plfsdir_set_fixed_kv(plfshdl_, 1);
  deltafs_plfsdir_force_leveldb_fmt(plfshdl_, opts_.force_leveldb_format);
  deltafs_plfsdir_set_unordered(plfshdl_, opts_.unordered_storage);
  deltafs_plfsdir_set_side_io_buf_size(plfshdl_, opts_.particle_buf_size);
  deltafs_plfsdir_set_side_filter_size(plfshdl_, opts_.particle_count);
  plfsparts_ = deltafs_plfsdir_get_memparts(plfshdl_);
  plfstp_ = deltafs_tp_init(opts_.bgdepth);
  deltafs_plfsdir_set_thread_pool(plfshdl_, plfstp_);
  plfsenv_ = deltafs_env_init(
      1, reinterpret_cast<void**>(const_cast<char**>(&opts_.env)));
  deltafs_plfsdir_set_env(plfshdl_, plfsenv_);
  deltafs_plfsdir_set_err_printer(plfshdl_, &plfsdir_error_printer, NULL);
  rv = deltafs_plfsdir_open(plfshdl_, path);
  if (rv != 0) {
    s = Status::IOError("cannot open plfsdir");
  } else {
    logf(LOG_INFO,
         "plfsdir (via deltafs-LT, env=%s, io_engine=%d, "
         "unordered=%d, leveldb_fmt=%d) opened (rank 0)\n>>> bg "
         "thread pool size: %d",
         opts_.env, opts_.io_engine, opts_.unordered_storage,
         opts_.force_leveldb_format, opts_.bgdepth);
  }

  return s;
}

}  // namespace plfsio
}  // namespace pdlfs