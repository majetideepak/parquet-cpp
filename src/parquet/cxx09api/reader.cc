/* Copyright (c) 2016 Hewlett Packard Enterprise Development LP */

#include <parquet/cxx09api/reader.h>

#include <algorithm>
#include <memory>

#include "parquet/api/reader.h"
#include "parquet/column/reader.h"
#include "parquet/column/scanner.h"
//#include "parquet/util/mem-allocator.h"
#include "parquet/util/visibility.h"

namespace parquet {

class MuteBufferImpl : public MuteBuffer {
 public:
  MuteBufferImpl(int64_t size, MemoryAllocator* pool)
      : buffer_(AllocateBuffer(pool, size)) {}
  uint8_t* GetBufferPtr() { return buffer_->mutable_data(); }

 private:
  std::shared_ptr<PoolBuffer> buffer_;
};

std::shared_ptr<MuteBuffer> MuteBuffer::getBuffer(int64_t size, MemoryAllocator* pool) {
  return std::shared_ptr<MuteBuffer>(new MuteBufferImpl(size, pool));
}

// ----------------------------------------------------------------------
// StreamSource
StreamSource::StreamSource(ExternalInputStream* stream, const MemoryAllocator* pool)
    : stream_(stream), offset_(0), pool_(pool) {
  size_ = stream->GetLength();
}

int64_t StreamSource::Tell() const {
  return offset_;
}

const std::string& StreamSource::source_name() const {
  return stream_->GetName();
}

void StreamSource::Seek(int64_t pos) {
  if (pos < 0 || pos >= size_) {
    std::stringstream ss;
    ss << "Cannot seek to " << pos << ". File length is " << size_;
    throw ParquetException(ss.str());
  }
  offset_ = pos;
}

int64_t StreamSource::Read(int64_t nbytes, uint8_t* out) {
  int64_t bytes_read = 0;
  int64_t bytes_available = std::min(nbytes, size_ - offset_);
  bytes_read = stream_->Read(bytes_available, offset_, out);
  offset_ += bytes_read;
  return bytes_read;
}

std::shared_ptr<Buffer> StreamSource::Read(int64_t nbytes) {
  throw ParquetException("Not Implemented");
}

std::shared_ptr<Buffer> StreamSource::ReadAt(int64_t position, int64_t nbytes) {
  throw ParquetException("Not Implemented");
}

int64_t StreamSource::ReadAt(int64_t position, int64_t nbytes, uint8_t* out) {
  Seek(position);
  return Read(nbytes, out);
}

}  // namespace parquet
