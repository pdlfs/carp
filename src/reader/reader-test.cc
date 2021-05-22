//
// Created by Ankush on 5/13/2021.
//

#include "compactor.h"

#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {
namespace plfsio {
class ReaderTest {
 public:
  void Hello() { printf("hello world!\n"); }
};

TEST(ReaderTest, PlfsTest) {
  Status s = Status::OK();

  std::string dir = "/tmp/particle";
  PlfsWrapper writer;

  s = writer.OpenDir(dir.c_str(), 0);
  ASSERT_TRUE(s.ok());

  s = writer.EpochFlush();
  ASSERT_TRUE(s.ok());

  for (int i = 0; i < 100; i++) {
    float key = i;
    char val[56];
    Slice val_sl(val, 56);
    s = writer.Append(key, val_sl);
    ASSERT_TRUE(s.ok());
  }

  s = writer.Flush();
  ASSERT_TRUE(s.ok());
  s = writer.EpochFlush();
  ASSERT_TRUE(s.ok());
  s = writer.CloseDir();
  ASSERT_TRUE(s.ok());
}

TEST(ReaderTest, QuerySanityCheck) {
  Query q1(0, 0.5, 10);
  Query q2 (0, 0, 1);
  ASSERT_TRUE(q1.Overlaps(q2));

  Query q3(1, 1, 2);
  ASSERT_FALSE(q3.Overlaps(q2));
}
}  // namespace plfsio
}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return pdlfs::test::RunAllTests(&argc, &argv);
}