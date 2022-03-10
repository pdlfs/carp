#include "common.h"

#include <H5Part.h>
#include <algorithm>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <vector>

// Hack for zero-or-more variadic args
#define VA_ARGS(...) , ##__VA_ARGS__
#define LOG(lvl, fmt, ...)                 \
  do {                                     \
    if (rank_ == 0) {                      \
      logf(lvl, fmt VA_ARGS(__VA_ARGS__)); \
    }                                      \
  } while (0)

struct VPICWriterOpts {
  int my_rank;
  int num_ranks;
  std::string dir_in;
  std::string file_out;
  size_t ts_out;  // dump first `ts_out` timesteps
};

double NowMicros() {
  timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  double tusec = t.tv_sec * 1e6 + t.tv_nsec * 1e-3;
  return tusec;
}

class VPICWriter {
 public:
  VPICWriter(const VPICWriterOpts& opts)
      : rank_(opts.my_rank),
        num_ranks_(opts.num_ranks),
        dir_in_(opts.dir_in),
        file_out_(opts.file_out),
        ts_out_(opts.ts_out) {}

  void Run() {
    std::vector< int > timesteps;
    double total_time = 0;
    int timesteps_processed = 0;

    timesteps = DiscoverTimesteps(dir_in_);

    size_t nts_out = std::min(ts_out_, timesteps.size());

    for (size_t tidx = 0; tidx < nts_out; tidx++) {
      double ts_start = NowMicros();

      int timestep = timesteps[tidx];
      LOG(LOG_INFO, "Currently reading: timestep %d\n", timestep);

      CopyTimestep(timestep);

      double ts_end = NowMicros();
      total_time += (ts_end - ts_start);
      timesteps_processed++;
      // break;
    }

    LOG(LOG_INFO, "Timesteps processed: %d, avg time/ts: %.2lfs",
        timesteps_processed, total_time * 1e-6 / timesteps_processed);
  }

  void RunTemp() {
    H5PartFile* h5pf;
    h5part_int64_t ierr;
    h5pf =
        H5PartOpenFileParallel(file_out_.c_str(), H5PART_WRITE, MPI_COMM_WORLD);
    if (h5pf == NULL) {
      LOG(LOG_ERRO, "OpenFile failed!");
      return;
    }

    h5part_int64_t step = 200;
    ierr = H5PartSetStep(h5pf, step);
    if (ierr != H5PART_SUCCESS)
      LOG(LOG_ERRO, "Error occured in H5PartSetStep. Error code: %" PRIu64,
          ierr);

    h5part_int64_t np_local = 6400000;
    ierr = H5PartSetNumParticles(h5pf, np_local);
    if (ierr != H5PART_SUCCESS)
      LOG(LOG_INFO,
          "Error occured in setting the # of tracers; Error code: %" PRIu64,
          ierr);

    ierr = H5PartCloseFile(h5pf);
    if (ierr != H5PART_SUCCESS)
      LOG(LOG_ERRO, "Error occured in closing %s. Error code: %" PRIu64,
          file_out_.c_str(), ierr);
  }

 private:
  void CopyTimestep(int timestep) {
    std::string local_file = RankToFile(timestep);
    uint64_t num_particles = NumParticles(local_file);
    // // std::string local_file = "Abc";
    // num_particles = 6000000 + rand() % 100000;

    logf(LOG_DBUG, "Rank %d: %" PRIu64 " particles, %s\n", rank_, num_particles,
         local_file.c_str());

    float* real_valf = new float[num_particles];
    float* rand_valf = new float[num_particles];
    int* rand_vali = new int[num_particles];

    ReadEntireFile(local_file.c_str(), real_valf,
                   num_particles * sizeof(float));
    GenerateRandomFloats(rand_valf, num_particles);
    GenerateRandomInts(rand_vali, num_particles);

    WriteH5Part(timestep, num_particles, real_valf, rand_valf, rand_vali);
    // WriteH5Part(timestep, num_particles, rand_valf, rand_valf, rand_vali);

    delete[] rand_vali;
    rand_vali = nullptr;
    delete[] rand_valf;
    rand_valf = nullptr;
    delete[] real_valf;
    real_valf = nullptr;
  }

  void WriteH5Part(const int ts, const int num_particles, float* real_dataf,
                   float* rand_dataf, int* rand_datai) const;

  void ReadEntireFile(const char* fpath, float* fdata, size_t bytes) {
    int fd = open(fpath, O_RDONLY);

    if (fd < 0) {
      logf(LOG_ERRO, "Open failed: %s", strerror(errno));
    }

    ssize_t bytes_read = read(fd, (void*)fdata, bytes);

    if (bytes_read != bytes) {
      logf(LOG_ERRO, "ALERT! bytes read: %zd (Expected: %zu)", bytes_read,
           bytes);
    }

    int ret = close(fd);
    if (ret < 0) {
      logf(LOG_ERRO, "close failed: %s", strerror(errno));
    }
  }

  static void GenerateRandomFloats(float* fdata, const size_t nelems) {
    for (size_t i = 0; i < nelems; i++) {
      fdata[i] = rand() % 689 / 50.0f;
    }
  }

  static void GenerateRandomInts(int* data, const size_t nelems) {
    for (size_t i = 0; i < nelems; i++) {
      data[i] = rand() % 689;
    }
  }

  uint64_t NumParticles(const std::string& fpath) {
    return FileSize(fpath) / sizeof(float);
  }

  uint64_t FileSize(const std::string& fpath) {
    struct stat st;
    logf(LOG_INFO, "file size: %s\n", fpath.c_str());
    if (stat(fpath.c_str(), &st) != 0) {
      logf(LOG_ERRO, "Stat Error: %s\n", strerror(errno));
    }

    fprintf(stderr, "rank %d size: %lld\n", rank_, (long long)st.st_size);

    return st.st_size;
  }

  std::string RankToFile(int timestep) {
    char fpath[1024];
    snprintf(fpath, 1024, "%s/T.%d/eparticle.%d.%d", dir_in_.c_str(), timestep,
             timestep, rank_);
    return fpath;
  }

  std::vector< int > DiscoverTimesteps(std::string dir_path) {
    std::vector< int > timesteps;

    DIR* dir = opendir(dir_path.c_str());

    if (dir == nullptr) {
      logf(LOG_ERRO, "opendir failed: %s (reason: %s\n)\n", dir_path.c_str(),
           strerror(errno));
      return timesteps;
    }

    struct dirent* ent;
    while ((ent = readdir(dir)) != nullptr) {
      if (strncmp(ent->d_name, "T.", 2) == 0) {
        char* endptr;
        // long to int, should be okay
        int timestep = strtol(ent->d_name + 2, &endptr, 10);
        if (errno < 0) {
          logf(LOG_ERRO, "strtol: %s", strerror(errno));
          continue;
        }

        timesteps.push_back(timestep);
      }
    }

    closedir(dir);

    std::sort(timesteps.begin(), timesteps.end());

    LOG(LOG_INFO, "%zu timesteps found.", timesteps.size());

    return timesteps;
  }

  const int rank_;
  const int num_ranks_;
  uint64_t np_total_;
  uint64_t np_local_;
  const std::string dir_in_;
  const std::string file_out_;
  size_t ts_out_;
};

void VPICWriter::WriteH5Part(const int ts, const int num_particles,
                             float* real_dataf, float* rand_dataf,
                             int* rand_datai) const {
  H5PartFile* h5pf;
  h5part_float32_t* pf_real = reinterpret_cast< h5part_float32_t* >(real_dataf);
  h5part_float32_t* pf_rand = reinterpret_cast< h5part_float32_t* >(rand_dataf);
  h5part_int32_t* pi_rand = reinterpret_cast< h5part_int32_t* >(rand_datai);
  h5part_int64_t ierr;
  h5part_int64_t step = ts;
  h5part_int64_t np_local = num_particles;
  const char* fname = file_out_.c_str();

  // h5pf = H5PartOpenFileParallel(fname, H5PART_WRITE, MPI_COMM_WORLD);
  h5pf = H5PartOpenFileParallel(fname, H5PART_APPEND, MPI_COMM_WORLD);
  if (h5pf == nullptr) {
    LOG(LOG_ERRO, "Failed to open H5Part file");
    return;
  }

  ierr = H5PartSetStep(h5pf, step);
  // ierr = H5PartSetNumParticlesStrided(h5pf, np_local, 8);
  ierr = H5PartSetNumParticles(h5pf, np_local);
  if (ierr != H5PART_SUCCESS)
    LOG(LOG_INFO,
        "Error occured in setting the # of tracers; Error code: %" PRIu64,
        ierr);

  ierr = H5PartWriteDataFloat32(h5pf, "dX", rand_dataf);
  if (ierr != H5PART_SUCCESS)
    LOG(LOG_INFO, "Error occured in writing dX in tracer; Error code: %" PRIu64,
        ierr);
  LOG(LOG_INFO, "Finished writing dX");

  ierr = H5PartWriteDataFloat32(h5pf, "dY", rand_dataf);
  if (ierr != H5PART_SUCCESS)
    LOG(LOG_INFO, "Error occured in writing dY in tracer; Error code: %" PRIu64,
        ierr);
  LOG(LOG_INFO, "Finished writing dY");

  ierr = H5PartWriteDataFloat32(h5pf, "dZ", rand_dataf);
  if (ierr != H5PART_SUCCESS)
    LOG(LOG_INFO, "Error occured in writing dZ in tracer; Error code: %" PRIu64,
        ierr);
  LOG(LOG_INFO, "Finished writing dZ");

  ierr = H5PartWriteDataInt32(h5pf, "i", rand_datai);
  if (ierr != H5PART_SUCCESS)
    LOG(LOG_INFO, "Error occured in writing i in tracer; Error code: %" PRIu64,
        ierr);
  LOG(LOG_INFO, "Finished writing i");

  ierr = H5PartWriteDataFloat32(h5pf, "Ux", real_dataf);
  if (ierr != H5PART_SUCCESS)
    LOG(LOG_INFO, "Error occured in writing Ux in tracer; Error code: %" PRIu64,
        ierr);
  LOG(LOG_INFO, "Finished writing Ux");

  ierr = H5PartWriteDataFloat32(h5pf, "Uy", real_dataf);
  if (ierr != H5PART_SUCCESS)
    LOG(LOG_INFO, "Error occured in writing Uy in tracer; Error code: %" PRIu64,
        ierr);
  LOG(LOG_INFO, "Finished writing Uy");

  ierr = H5PartWriteDataFloat32(h5pf, "Uz", real_dataf);
  if (ierr != H5PART_SUCCESS)
    LOG(LOG_INFO, "Error occured in writing Uz in tracer; Error code: %" PRIu64,
        ierr);
  LOG(LOG_INFO, "Finished writing Uz");

  ierr = H5PartWriteDataInt32(h5pf, "q", rand_datai);
  if (ierr != H5PART_SUCCESS)
    LOG(LOG_INFO, "Error occured in writing q in tracer; Error code: %" PRIu64,
        ierr);

  ierr = H5PartCloseFile(h5pf);
  if (ierr != H5PART_SUCCESS)
    LOG(LOG_INFO, "Error occured in closing %s. Error code: %" PRIu64, fname,
        ierr);
}
