/* Copyright (c) 2016 Hewlett Packard Enterprise Development LP */

#ifndef PARQUET_CXX09API_READER_H
#define PARQUET_CXX09API_READER_H

#include <algorithm>
#include <list>
#include <string>
#include <vector>

#include "parquet/types.h"
//#include "arrow/io/memory.h"
#include "parquet/util/memory.h"

namespace parquet {

class MuteBuffer {
 public:
  static std::shared_ptr<MuteBuffer> getBuffer(
      int64_t, MemoryAllocator* pool = default_allocator());
  virtual uint8_t* GetBufferPtr() = 0;
};

// ----------------------------------------------------------------------
// A stream-like object that reads from an ExternalInputStream
class StreamSource : public RandomAccessSource {
 public:
  explicit StreamSource(ExternalInputStream* stream, const MemoryAllocator* pool);
  virtual void Close() {}
  virtual int64_t Tell() const;
  virtual void Seek(int64_t pos);
  virtual int64_t Read(int64_t nbytes, uint8_t* out);
  virtual std::shared_ptr<Buffer> Read(int64_t nbytes);
  virtual const std::string& source_name() const;
  virtual std::shared_ptr<Buffer> ReadAt(int64_t position, int64_t nbytes);
  virtual int64_t ReadAt(int64_t position, int64_t nbytes, uint8_t* out);
 private:
  // parquet-cpp should not manage this object
  ExternalInputStream* stream_;
  int64_t offset_;
  int64_t size_;
  const MemoryAllocator* pool_;
};

}  // namespace parquet

#endif  // PARQUET_CXX09API_READER_H
