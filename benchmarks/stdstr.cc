//
// Created by Ankush J on 2/15/21.
//

#include <math.h>
#include <pdlfs-common/env.h>
#include <pdlfs-common/port.h>
#include <vector>

namespace pdlfs {
class StdStringTest {
 public:
  StdStringTest() : env_(NULL) { env_ = port::PosixGetDefaultEnv(); }
  void TestStringVecAlloc(size_t fill_bytes = 0) {
    uint64_t order;
    for (int mag = 3; mag <= 8; mag++) {
      uint64_t tmag = 0;
      order = pow(10, mag);
      uint64_t tbeg = env_->NowMicros();
      for (int iter = 0; iter < 3; iter++) {
        TestStringVecAllocImpl(order, fill_bytes);
      }
      uint64_t tend = env_->NowMicros();
      tmag = (tend - tbeg) / 3.0;
      if (!fill_bytes) {
        fprintf(stderr,
                "[String Alloc] Order: 1e%d, Time: %.3f s, Per: %.2lf us\n",
                mag, tmag * 1e-6f, tmag * 1.0 / order);
      } else {
        fprintf(stderr,
                "[String Init] Order: 1e%d, Bytes: %zu, Time: %.3f s, Per: "
                "%.2lf us\n",
                mag, fill_bytes, tmag * 1e-6f, tmag * 1.0 / order);
      }
      order *= 10;
    }
  }
  void Run() {
    TestStringVecAlloc();
    TestStringVecAlloc(4);
    TestStringVecAlloc(16);
    TestStringVecAlloc(64);
    fprintf(stderr, "Anti Opt: %zu\n", randbuf_.size());
  }

 private:
  void TestStringVecAllocImpl(uint64_t sz, size_t fill_bytes = 0) {
    std::vector<std::string> strvec;
    strvec.resize(sz);
    strvec[sz - 1] = std::to_string(sz);

    if (fill_bytes) {
      FillStringRandImpl(strvec, fill_bytes);
    }

    randbuf_.append(
        std::to_string(reinterpret_cast<uint64_t>(&strvec[sz - 1])));
  }

  void FillStringRandImpl(std::vector<std::string>& svec, size_t bytes) {
    char buf[2 * bytes];
    for (size_t i = 0; i < 2 * bytes; i++) {
      buf[i] = rand() % 255;
    }

    for (size_t i = 0; i < svec.size(); i++) {
      size_t rand_idx = rand() % bytes;
      svec[i] = std::string(buf + rand_idx, bytes);
    }
  }
  Env* env_;
  std::string randbuf_;
};
}  // namespace pdlfs

int main() {
  pdlfs::StdStringTest test;
  test.Run();
  return 0;
}
