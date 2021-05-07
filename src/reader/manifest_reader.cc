//
// Created by Ankush J on 2/3/21.
//

#include "manifest_reader.h"

#include "common.h"
#include "range_reader.h"

namespace pdlfs {
namespace plfsio {
PartitionManifestReader::PartitionManifestReader(PartitionManifest& manifest)
    : manifest_(manifest) {
  size_t entry_init[] = {sizeof(uint64_t), sizeof(uint64_t), sizeof(float),
                         sizeof(float),    sizeof(float),    sizeof(float),
                         sizeof(uint32_t), sizeof(uint32_t), sizeof(uint32_t)};
  std::copy(entry_init, entry_init + num_entries_, entry_sizes_);
  ComputeInternalOffsets(entry_sizes_, offsets_, num_entries_, item_sz_);
}

Status PartitionManifestReader::ReadManifest(int rank, Slice& footer_data,
                                             const uint64_t footer_sz) {
  uint64_t epoch_offset = 0;
  Status s;

  FILE* file_out = NULL;

  if (!output_path_.empty()) {
    char fpath[1024];
    snprintf(fpath, 1024, "%s/vpic-manifest.%d", output_path_.c_str(), rank);
    file_out = fopen(fpath, "w+");
  }

  while (epoch_offset < footer_sz) {
    uint32_t num_ep_written = DecodeFixed32(&footer_data[epoch_offset]);
    uint64_t off_prev =
        DecodeFixed64(&footer_data[epoch_offset + sizeof(uint32_t)]);
    logf(LOG_DBUG, "[EPOCH] Index: %u, Offset: %lu\n", num_ep_written,
         off_prev);

    ReadFooterEpoch(num_ep_written, rank, footer_data, epoch_offset + 12,
                    off_prev, file_out);

    epoch_offset += off_prev + 12;
  }

  if (file_out) {
    fclose(file_out);
    file_out = NULL;
  }

  return s;
}

void PartitionManifestReader::ReadFooterEpoch(int epoch, int rank, Slice& data,
                                              const uint64_t epoch_offset,
                                              const uint64_t epoch_sz,
                                              FILE* file_out) {
  int num_items = epoch_sz / item_sz_;

  uint64_t cur_offset = epoch_offset;

  for (int i = 0; i < num_items; i++) {
    PartitionManifestItem item;
    assert(i == DecodeFixed64(&data[cur_offset + offsets_[0]]));

    item.epoch = epoch;
    item.rank = rank;
    item.offset = DecodeFixed64(&data[cur_offset + offsets_[1]]);
    item.part_range_begin = DecodeFloat32(&data[cur_offset + offsets_[2]]);
    item.part_range_end = DecodeFloat32(&data[cur_offset + offsets_[3]]);
    item.part_expected_begin = DecodeFloat32(&data[cur_offset + offsets_[4]]);
    item.part_expected_end = DecodeFloat32(&data[cur_offset + offsets_[5]]);
    item.updcnt = DecodeFixed32(&data[cur_offset + offsets_[6]]);
    item.part_item_count = DecodeFixed32(&data[cur_offset + offsets_[7]]);
    item.part_item_oob = DecodeFixed32(&data[cur_offset + offsets_[8]]);

    std::string item_dbg = item.ToString();
    logf(LOG_DBG2, "%s\n", item_dbg.c_str());

    if (file_out) {
      std::string item_csv = item.ToCSVString();
      fprintf(file_out, "%s\n", item_csv.c_str());
    }

    // Note to self: if this is a bottleneck,
    // pre-allocate manifest and write to an index without locking
    manifest_mutex_.Lock();
    manifest_.AddItem(item);
    manifest_mutex_.Unlock();

    cur_offset += item_sz_;
  }
}
}  // namespace plfsio
}  // namespace pdlfs
