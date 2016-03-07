/* Copyright (c) 2016 Hewlett Packard Enterprise Development LP */

#include <parquet/cxx09api/reader.h>

#include <algorithm>
#include <memory>

#include "parquet/api/reader.h"
#include "parquet/column/reader.h"
#include "parquet/column/scanner.h"
#include "parquet/types.h"
#include "parquet/util/mem-allocator.h"
#include "parquet/util/visibility.h"

namespace parquet {

class MuteBufferImpl : public MuteBuffer {
 public:
  MuteBufferImpl(int64_t size, MemoryAllocator* pool)
      : buffer_(new OwnedMutableBuffer(size, pool)) {}
  uint8_t* GetBufferPtr() { return buffer_->mutable_data(); }

 private:
  std::unique_ptr<OwnedMutableBuffer> buffer_;
};

boost::shared_ptr<MuteBuffer> MuteBuffer::getBuffer(int64_t size, MemoryAllocator* pool) {
  return boost::shared_ptr<MuteBuffer>(new MuteBufferImpl(size, pool));
}

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

 private:
  // parquet-cpp should not manage this object
  ExternalInputStream* stream_;
  int64_t offset_;
  const MemoryAllocator* pool_;
};

// ----------------------------------------------------------------------
// StreamSource
StreamSource::StreamSource(ExternalInputStream* stream, const MemoryAllocator* pool)
    : stream_(stream), offset_(0), pool_(pool) {
  size_ = stream->GetLength();
}

int64_t StreamSource::Tell() const {
  return offset_;
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
  int64_t bytes_available = std::min(nbytes, size_ - offset_);
  auto result = std::make_shared<OwnedMutableBuffer>(
      bytes_available, const_cast<MemoryAllocator*>(pool_));

  int64_t bytes_read = 0;
  bytes_read = stream_->Read(bytes_available, offset_, result->mutable_data());
  if (bytes_read < bytes_available) { result->Resize(bytes_read); }
  offset_ += bytes_read;
  return result;
}

template <typename CRType>
class ParquetScannerAPI : public ParquetScanner {
 public:
  typedef typename CRType::T Type;

  ParquetScannerAPI(std::shared_ptr<ColumnReader>& creader)
      : reader_(std::static_pointer_cast<CRType>(creader)) {}

  virtual int64_t SkipValues(int64_t num_values) { return reader_->Skip(num_values); }

  virtual int64_t NextValues(uint8_t* val, int64_t* values_buffered, int16_t* def_levels,
      int16_t* rep_levels, bool* is_null, int64_t batch_size = 128) {
    return reader_->ReadBatch(static_cast<int32_t>(batch_size), def_levels, rep_levels,
        reinterpret_cast<Type*>(val), values_buffered, is_null);
  }

  virtual int64_t getRemaining() { return reader_->GetRemaining(); }

  std::shared_ptr<CRType> reader_;
};

class RowGroupAPI : public RowGroup {
 public:
  explicit RowGroupAPI(std::shared_ptr<RowGroupReader>& greader,
      std::unique_ptr<RowGroupMetaData> metadata, const MemoryAllocator* pool)
      : group_reader_(greader), metadata_(std::move(metadata)), pool_(pool) {}

  ~RowGroupAPI() {}

  virtual int64_t NumRows() const { return metadata_->num_rows(); }

  virtual bool HasStats(int col_id) const {
    return metadata_->ColumnChunk(col_id)->is_stats_set();
  }

  virtual const std::string* GetMin(int col_id) const {
    return metadata_->ColumnChunk(col_id)->statistics().min;
  }

  virtual const std::string* GetMax(int col_id) const {
    return metadata_->ColumnChunk(col_id)->statistics().max;
  }

  virtual int64_t GetOffset() const { return metadata_->ColumnChunk(0)->file_offset(); }

  virtual int64_t GetLength() const { return metadata_->total_byte_size(); }

  virtual int GetNullCount(int col_id) const {
    return metadata_->ColumnChunk(col_id)->statistics().null_count;
  }

  virtual boost::shared_ptr<ParquetScanner> GetScanner(int i);

 private:
  std::shared_ptr<RowGroupReader> group_reader_;
  std::unique_ptr<RowGroupMetaData> metadata_;
  const MemoryAllocator* pool_;
};

// Parquet Reader
class ReaderAPI : public Reader {
 public:
  ReaderAPI(ExternalInputStream* stream, ReaderProperties props)
      : stream_(stream), properties_(props) {
    source_.reset(new StreamSource(stream, props.allocator()));
    reader_ = ParquetFileReader::Open(std::move(source_), props);
    metadata_ = reader_->metadata();
    schema_ = metadata_->schema();
  }

  virtual Type::type GetSchemaType(int i) const {
    return schema_->Column(i)->physical_type();
  }

  virtual int GetTypeLength(int i) const { return schema_->Column(i)->type_length(); }

  virtual int GetTypePrecision(int i) const {
    return schema_->Column(i)->type_precision();
  }

  virtual int GetTypeScale(int i) const { return schema_->Column(i)->type_scale(); }

  virtual int NumRealColumns() const { return schema_->group_node()->field_count(); }

  virtual bool IsFlatSchema() const { return (NumColumns() == NumRealColumns()); }

  virtual std::string& GetStreamName() const { return stream_->GetName(); }

  virtual int NumColumns() const { return metadata_->num_columns(); }

  virtual int NumRowGroups() const { return metadata_->num_row_groups(); }

  virtual int64_t NumRows() const { return metadata_->num_rows(); }

  virtual int64_t EstimateMemoryUsage(std::list<int>& selected_columns, int row_group,
      int64_t batch_size, int64_t col_reader_size) const {
    return reader_
        ->EstimateMemoryUsage(selected_columns, row_group, batch_size, col_reader_size)
        .memory;
  }

  boost::shared_ptr<RowGroup> GetRowGroup(int i) {
    auto group_reader = reader_->RowGroup(i);
    return boost::shared_ptr<RowGroup>(new RowGroupAPI(
        group_reader, std::move(metadata_->RowGroup(i)), properties_.allocator()));
  }

 private:
  std::unique_ptr<ParquetFileReader> reader_;
  ExternalInputStream* stream_;
  std::unique_ptr<RandomAccessSource> source_;
  ReaderProperties properties_;
  const FileMetaData* metadata_;
  const SchemaDescriptor* schema_;
};

boost::shared_ptr<Reader> Reader::getReader(
    ExternalInputStream* stream, int64_t column_buffer_size, MemoryAllocator* pool) {
  ReaderProperties props(pool);
  props.enable_buffered_stream();
  props.set_buffer_size(column_buffer_size);

  return boost::shared_ptr<Reader>(new ReaderAPI(stream, props));
}

// RowGroup
boost::shared_ptr<ParquetScanner> RowGroupAPI::GetScanner(int i) {
  auto column_reader = group_reader_->Column(i);
  switch (column_reader->type()) {
    case Type::BOOLEAN: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetScannerAPI<BoolReader>(column_reader));
    }
    case Type::INT32: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetScannerAPI<Int32Reader>(column_reader));
    }
    case Type::INT64: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetScannerAPI<Int64Reader>(column_reader));
    }
    case Type::INT96: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetScannerAPI<Int96Reader>(column_reader));
    }
    case Type::FLOAT: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetScannerAPI<FloatReader>(column_reader));
    }
    case Type::DOUBLE: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetScannerAPI<DoubleReader>(column_reader));
    }
    case Type::BYTE_ARRAY: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetScannerAPI<ByteArrayReader>(column_reader));
    }
    case Type::FIXED_LEN_BYTE_ARRAY: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetScannerAPI<FixedLenByteArrayReader>(column_reader));
    }
    default: { return boost::shared_ptr<ParquetScanner>(); }
  }
  return boost::shared_ptr<ParquetScanner>();
}

}  // namespace parquet
