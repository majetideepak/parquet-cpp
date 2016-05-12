/* Copyright (c) 2016 Hewlett Packard Enterprise Development LP */

#include <parquet/cxx09api/reader.h>

#include <memory>
#include <algorithm>

#include "parquet/api/reader.h"
#include "parquet/column/reader.h"
#include "parquet/column/scanner.h"
#include "parquet/types.h"
#include "parquet/schema/types.h"
#include "parquet/util/mem-allocator.h"

namespace parquet {

class VGroupNode : public VNode {
 public:
  VGroupNode(const parquet::schema::GroupNode* node) : gnode_(node) {}
  virtual bool IsGroupNode() {return true;}
  virtual int GetScannerOffset() {return -1;}
  virtual int GetLevelOffset() {return -1;}
  bool IsRepeated() {return gnode_->is_repeated();}
  bool IsRequired() {return gnode_->is_required();}
  bool IsOptional() {return gnode_->is_optional();}
  const parquet::schema::GroupNode* gnode_;
  std::vector<VNodePtr> children_; 
};

class VLeafNode : public VNode {
 public:
  VLeafNode(int soffset, int loffset) : SOffset_(soffset), LOffset_(loffset) {}
  virtual bool IsGroupNode() {return false;}
  virtual int GetScannerOffset() {return SOffset_;}
  virtual int GetLevelOffset() {return LOffset_;}
  int SOffset_; 
  int LOffset_; 
};

class MuteBufferImpl : public MuteBuffer {
 public:
  MuteBufferImpl(int64_t size, MemoryAllocator* pool)
      : buffer_(new OwnedMutableBuffer(size, pool)) {}
  uint8_t* GetBufferPtr() {return buffer_->mutable_data();}
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

template <typename DType>
class ParquetTypedScanner : public ParquetScanner {
 public:
  typedef typename type_traits<DType::type_num>::value_type T;
  ParquetTypedScanner(
      std::shared_ptr<ColumnReader> creader, int batch_size, MemoryAllocator* pool)
      : reader_(creader) {
    //scanner_ = std::make_shared<TypedScanner<DType>>(creader, batch_size, pool);
    typed_reader_ = static_cast<TypedColumnReader<DType>*>(creader.get());
  }
//  bool NextValue(uint8_t* val, bool* is_null) {
//    return scanner_->NextValue(reinterpret_cast<T*>(val), is_null);
//  }
  int64_t NextBulkValues(uint8_t* val, int64_t* values_buffered, int16_t* def_levels,
      int16_t* rep_levels, bool* is_null=NULL, int64_t batch_size = 0,
      ReadMode mode = NO_NULLS) {
    int level_index = 0;
    int value_index = 0;
    int64_t values_to_read = batch_size;

    while (batch_size > 0) {
      if (!reader_->HasNext()) {
        memset(is_null + level_index, true, batch_size);
        return values_to_read - batch_size;
      }
      int64_t values_buffered = 0;
      int64_t levels_buffered = typed_reader_->ReadBatch(
          static_cast<int32_t>(batch_size), def_levels + level_index,
          rep_levels + level_index, reinterpret_cast<T*>(val) + value_index,
          &values_buffered, is_null + level_index, mode);

      level_index += levels_buffered;
      batch_size -= levels_buffered;
      value_index += values_buffered;
    }
    *values_buffered = value_index;
    return values_to_read - batch_size;
  }
//  std::shared_ptr<TypedScanner<DType>> scanner_;
  TypedColumnReader<DType>* typed_reader_;
  std::shared_ptr<ColumnReader> reader_;
};

typedef ParquetTypedScanner<BooleanType> ParquetBoolScanner;
typedef ParquetTypedScanner<Int32Type> ParquetInt32Scanner;
typedef ParquetTypedScanner<Int64Type> ParquetInt64Scanner;
typedef ParquetTypedScanner<Int96Type> ParquetInt96Scanner;
typedef ParquetTypedScanner<FloatType> ParquetFloatScanner;
typedef ParquetTypedScanner<DoubleType> ParquetDoubleScanner;
typedef ParquetTypedScanner<ByteArrayType> ParquetBAScanner;
typedef ParquetTypedScanner<FLBAType> ParquetFLBAScanner;

class RowGroupAPI : public RowGroup {
 public:
  explicit RowGroupAPI(
      std::shared_ptr<RowGroupReader>& greader, const MemoryAllocator* pool)
      : group_reader_(greader), pool_(pool) {}

  int64_t NumRows() { return group_reader_->num_rows(); }

  bool HasStats(int col_id) { return group_reader_->IsColumnStatsSet(col_id); }

  const std::string* GetMin(int col_id) {
    return group_reader_->GetColumnStats(col_id).min;
  }

  const std::string* GetMax(int col_id) {
    return group_reader_->GetColumnStats(col_id).max;
  }

  int64_t GetOffset() { return group_reader_->GetFileOffset(); }

  int GetNullCount(int col_id) {
    return group_reader_->GetColumnStats(col_id).null_count;
  }

  boost::shared_ptr<ParquetScanner> GetScanner(
      int i, int64_t scan_size);

 private:
  std::shared_ptr<RowGroupReader> group_reader_;
  const MemoryAllocator* pool_;
};

// Parquet Reader
class ReaderAPI : public Reader {
 public:
  ReaderAPI(ExternalInputStream* stream, ReaderProperties props)
      : stream_(stream), properties_(props) {
    source_.reset(new StreamSource(stream, props.allocator()));
    reader_ =
        ParquetFileReader::Open(std::move(source_), props);
  }

  Type::type GetSchemaType(int i) { return reader_->column_schema(i)->physical_type(); }

  int GetTypeLength(int i) { return reader_->column_schema(i)->type_length(); }

  int GetTypePrecision(int i) { return reader_->column_schema(i)->type_precision(); }

  int GetTypeScale(int i) { return reader_->column_schema(i)->type_scale(); }

  bool IsComplexColumn(int i) { return reader_->descr()->GetColumnRoot(i)->is_group(); }

  VNodePtr BuildComplexColumnTree(int i, int SOffset, int LOffset);

  bool GetNumColumnSiblings(int i) { return reader_->descr()->GetColumnRoot(i)->get_num_leaves(); }

  bool IsFlatSchema() { return reader_->descr()->no_group_nodes(); }

  std::string& GetStreamName() { return stream_->GetName(); }

  int NumColumns() { return reader_->num_columns(); }

  int NumVirtualColumns() { return reader_->num_virtual_columns(); }

  int NumRowGroups() { return reader_->num_row_groups(); }

  int64_t NumRows() { return reader_->num_rows(); }

  int64_t EstimateMemoryUsage(std::list<int>& selected_columns, int row_group,
      int64_t batch_size, int64_t col_reader_size) {
    return reader_->EstimateMemoryUsage(
                      selected_columns, row_group, batch_size, col_reader_size)
        .memory;
  }

  boost::shared_ptr<RowGroup> GetRowGroup(int i) {
    auto group_reader = reader_->RowGroup(i);
    return boost::shared_ptr<RowGroup>(
        new RowGroupAPI(group_reader, properties_.allocator()));
  }

 private:
  std::unique_ptr<ParquetFileReader> reader_;
  ExternalInputStream* stream_;
  std::unique_ptr<RandomAccessSource> source_;
  ReaderProperties properties_;
};

void buildComplexTree(VGroupNode* root, int soffset, int loffset, int& child_id) {

   for (int i = 0; i < root->gnode_->field_count(); i++) {
        if (root->gnode_->field(i)->is_group()) {
            const parquet::schema::GroupNode* field =
                static_cast<const parquet::schema::GroupNode*>(root->gnode_->field(i).get());
            VNodePtr vfield = boost::shared_ptr<VGroupNode>(new VGroupNode(field));
            root->children_.push_back(vfield);
            buildComplexTree(static_cast<VGroupNode*>(vfield.get()), soffset, loffset, child_id);
        } else {
            root->children_.push_back(boost::shared_ptr<VLeafNode>(new VLeafNode(soffset + child_id, loffset + child_id)));
            child_id++;
        }
   } 

}

VNodePtr ReaderAPI::BuildComplexColumnTree(int i, int SOffset, int LOffset) {
  // The root is already verified to be a groupnode
  const parquet::schema::GroupNode* root =
      static_cast<const parquet::schema::GroupNode*>(reader_->descr()->GetColumnRoot(i).get());
  VNodePtr vroot = boost::shared_ptr<VGroupNode>(new VGroupNode(root));
  int child_id = 0;
  buildComplexTree(static_cast<VGroupNode*>(vroot.get()), SOffset, LOffset, child_id);
  return vroot;
}

boost::shared_ptr<Reader> Reader::getReader(
    ExternalInputStream* stream, int64_t column_buffer_size, MemoryAllocator* pool) {
  ReaderProperties props(pool);
  props.enable_buffered_stream();
  props.set_buffer_size(column_buffer_size);

  return boost::shared_ptr<Reader>(new ReaderAPI(stream, props));
}

// RowGroup
boost::shared_ptr<ParquetScanner> RowGroupAPI::GetScanner(
    int i, int64_t batch_size) {
  MemoryAllocator* pool = const_cast<MemoryAllocator*>(pool_);
  auto column_reader = group_reader_->Column(i);
  switch (column_reader->type()) {
    case Type::BOOLEAN: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetBoolScanner(column_reader, batch_size, pool));
    }
    case Type::INT32: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetInt32Scanner(column_reader, batch_size, pool));
    }
    case Type::INT64: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetInt64Scanner(column_reader, batch_size, pool));
    }
    case Type::INT96: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetInt96Scanner(column_reader, batch_size, pool));
    }
    case Type::FLOAT: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetFloatScanner(column_reader, batch_size, pool));
    }
    case Type::DOUBLE: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetDoubleScanner(column_reader, batch_size, pool));
    }
    case Type::BYTE_ARRAY: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetBAScanner(column_reader, batch_size, pool));
    }
    case Type::FIXED_LEN_BYTE_ARRAY: {
      return boost::shared_ptr<ParquetScanner>(
          new ParquetFLBAScanner(column_reader, batch_size, pool));
    }
    default: { return boost::shared_ptr<ParquetScanner>(); }
  }
  return boost::shared_ptr<ParquetScanner>();
}

}  // namespace parquet
