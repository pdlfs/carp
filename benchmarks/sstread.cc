//
// Created by Ankush on 5/24/2021.
//

#include "common.h"

#include <carp/manifest.h>
#include <reader/query_utils.h>
#include <reader/range_reader.h>

#define KB(x) (1024ull * (x))
#define MB(x) (1024ull * KB(x))

namespace pdlfs {
namespace plfsio {

template <typename T>
class SimpleReader {
 public:
  SimpleReader(const RdbOptions& options)
      : options_(options),
        dir_path_(options.data_path),
        num_ranks_(0),
        fdcache_(options.env),
        thpool_(nullptr),
        task_tracker_(options.env) {
    thpool_ = ThreadPool::NewFixed(options.parallelism);
  }

  ~SimpleReader() {
    if (thpool_) {
      delete thpool_;
      thpool_ = nullptr;
    }
  }

  void BenchmarkSuite() {
    srand(time(nullptr));

    SingleBenchmark(0, 1, 64, 5);
    SingleBenchmark(1, 2, 64, 10);

    int start_rank = (rand() % 5) * 100;
    SingleBenchmark(start_rank, start_rank + 64, 1, 10);

    logf(LOG_WARN, "Size estimate only valid if each RDB has sufficient data");
  }

  void SingleBenchmark(int first_rank, int last_rank, int items_per_rank,
                       int size_item_mb) {
    Prepare();
    PartitionManifestMatch match;
    std::vector<KeyPair> query_results;

    for (int rank = first_rank; rank < last_rank; rank++) {
      GenRandomMatches(rank, size_item_mb, items_per_rank, match);
    }

    uint64_t read_begin_us = options_.env->NowMicros();
    //    Status s = RankwiseReadSSTs(match, query_results);
    Status s = ReadSSTs(match, query_results);
    uint64_t read_end_us = options_.env->NowMicros();

    if (s.ok() and !query_results.empty()) {
#define VECFMT(i) ((int)(query_results[i].key))
      printf("[ReadPreview] %zu items (first: %d, last: %d)\n",
             query_results.size(), VECFMT(0) % 9,
             VECFMT(query_results.size() - 1) % 9);
    } else if (!s.ok()) {
      printf("error: %s\n", s.ToString().c_str());
    }

#define USTOSEC(x) ((x)*1e-6)

    logf(LOG_INFO,
         "[SingleBenchmark] First rank: %d, last rank: %d, total data read: %d "
         "MB, time taken: %.3fs\n",
         first_rank, last_rank,
         (last_rank - first_rank) * items_per_rank * size_item_mb,
         USTOSEC(read_end_us - read_begin_us));
  }

 private:
  void GenRandomMatches(int rank, int size_mb, uint64_t num_items,
                        PartitionManifestMatch& match) {
    if (rank >= num_ranks_) return;

    const size_t key_sz = 4;
    const size_t val_sz = 60;
    match.SetKVSizes(key_sz, val_sz);

    RandomAccessFile* fh;
    uint64_t file_sz;
    uint64_t sst_sz = MB(size_mb);
    uint64_t sst_cnt = sst_sz / (key_sz + val_sz);

    fdcache_.GetFileHandle(rank, &fh, &file_sz, false);

    uint64_t num_ssts = file_sz / sst_sz;
    num_items = std::min(num_items, num_ssts);

    int offset_max = (num_ssts - num_items) / 2;
    int offset_mb = offset_max ? rand() % offset_max : 0;
    uint64_t offset_sz = MB(offset_mb);

    for (uint64_t i = 0; i < num_items; i++) {
      PartitionManifestItem item;
      item.epoch = 0;
      item.rank = rank;
      item.offset = offset_sz;
      item.expected = Range(0, 0);
      item.observed = Range(0, 0);
      item.updcnt = 0;
      item.part_item_count = sst_cnt;
      item.part_item_oob = 0;

      offset_sz += sst_sz;

      match.AddItem(item);
      logf(LOG_DBUG, "%s\n", item.ToString().c_str());
    }
  }

  Status Prepare() {
    Status s = Status::OK();

    if (num_ranks_ == 0) {
      s = fdcache_.ReadDirectory(dir_path_, num_ranks_);
    }

    if (!s.ok()) {
      printf("error: %s\n", s.ToString().c_str());
    }

    return s;
  }

  Status ReadSSTs(PartitionManifestMatch& match,
                  std::vector<KeyPair>& query_results) {
    QueryUtils::ThreadSafetyWarning<T>();

    Slice slice;
    std::string scratch;

    std::vector<SSTReadWorkItem<T>> work_items;
    work_items.resize(match.Size());
    query_results.resize(match.TotalMass());
    task_tracker_.Reset();

    uint64_t mass_sum = 0;
    uint64_t key_sz, val_sz;
    match.GetKVSizes(key_sz, val_sz);

    for (uint32_t i = 0; i < match.Size(); i++) {
      PartitionManifestItem& item = match[i];
      work_items[i].item = &item;
      work_items[i].key_sz = key_sz;
      work_items[i].val_sz = val_sz;

      work_items[i].query_results = &query_results;
      work_items[i].qrvec_offset = mass_sum;
      mass_sum += item.part_item_count;

      work_items[i].fdcache = &fdcache_;
      work_items[i].task_tracker = &task_tracker_;

      thpool_->Schedule(QueryUtils::SSTReadWorker<T>, (void*)&work_items[i]);
    }

    assert(mass_sum == match.TotalMass());

    task_tracker_.WaitUntilCompleted(work_items.size());

    return Status::OK();
  }

  Status RankwiseReadSSTs(PartitionManifestMatch& match,
                          std::vector<KeyPair>& query_results) {
    Slice slice;
    std::string scratch;

    task_tracker_.Reset();

    uint64_t mass_sum = 0;
    uint64_t key_sz, val_sz;
    match.GetKVSizes(key_sz, val_sz);

    std::vector<int> ranks;
    match.GetUniqueRanks(ranks);

    std::vector<RankwiseSSTReadWorkItem<T>> work_items;
    work_items.resize(ranks.size());
    query_results.resize(match.TotalMass());

    logf(LOG_INFO, "Matching ranks: %zu\n", ranks.size());

    for (uint32_t i = 0; i < ranks.size(); i++) {
      int rank = ranks[i];
      work_items[i].rank = rank;
      uint64_t mass_rank = match.GetMatchesByRank(rank, work_items[i].wi_vec);
      work_items[i].key_sz = key_sz;
      work_items[i].val_sz = val_sz;

      work_items[i].query_results = &query_results;
      work_items[i].qrvec_offset = mass_sum;
      mass_sum += mass_rank;

      work_items[i].fdcache = &fdcache_;
      work_items[i].task_tracker = &task_tracker_;

      thpool_->Schedule(QueryUtils::RankwiseSSTReadWorker<T>,
                        (void*)&work_items[i]);
    }

    assert(mass_sum == match.TotalMass());

    task_tracker_.WaitUntilCompleted(work_items.size());

    return Status::OK();
  }

  const RdbOptions options_;
  const std::string dir_path_;
  int num_ranks_;
  ThreadPool* thpool_;
  TaskCompletionTracker task_tracker_;
  CachingDirReader<RandomAccessFile> fdcache_;
};

void RunBenchmark(RdbOptions& options) {
  SimpleReader<RandomAccessFile> simple_reader(options);
  simple_reader.BenchmarkSuite();
}
}  // namespace plfsio
}  // namespace pdlfs

void PrintHelp() { printf("./prog [-p parallelism ] -i plfs_dir\n"); }

void ParseOptions(int argc, char* argv[], pdlfs::plfsio::RdbOptions& options) {
  extern char* optarg;
  extern int optind;
  int c;

  while ((c = getopt(argc, argv, "i:p:aqb:e:x:y:h")) != -1) {
    switch (c) {
      case 'i':
        options.data_path = optarg;
        break;
      case 'p':
        options.parallelism = std::stoi(optarg);
        break;
      case 'h':
      default:
        PrintHelp();
        exit(0);
        break;
    }
  }
}

int main(int argc, char* argv[]) {
  pdlfs::plfsio::RdbOptions options;
  ParseOptions(argc, argv, options);

  options.env = pdlfs::port::PosixGetDefaultEnv();

  if (!options.env->FileExists(options.data_path.c_str())) {
    printf("Input directory does not exist\n");
    exit(EXIT_FAILURE);
  }

  pdlfs::plfsio::RunBenchmark(options);

  return 0;
}
