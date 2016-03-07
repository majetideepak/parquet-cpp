/* Copyright (c) 2016 Hewlett Packard Enterprise Development LP */

#ifndef PARQUET_CXX09API_READER_H
#define PARQUET_CXX09API_READER_H

#include <boost/shared_ptr.hpp>

#include <algorithm>
#include <list>
#include <string>
#include <vector>

#include "parquet/cxx09api/types.h"
#include "parquet/util/mem-allocator.h"

namespace parquet {

class MuteBuffer {
 public:
  static boost::shared_ptr<MuteBuffer>
      getBuffer(int64_t, MemoryAllocator* pool = default_allocator());
  virtual uint8_t* GetBufferPtr() = 0;
};

class ParquetScanner {
 public:
//  virtual bool NextValue(uint8_t* val, bool* is_null) = 0;
  virtual int64_t NextBulkValues(uint8_t* val, int64_t* values_buffered, int16_t* def_levels, int16_t* rep_levels,
      bool* is_null, int64_t batch_size, ReadMode mode) = 0;
};

class RowGroup {
 public:
  virtual bool HasStats(int col_id) = 0;
  virtual const std::string* GetMin(int col_id) = 0;
  virtual const std::string* GetMax(int col_id) = 0;
  virtual int GetNullCount(int col_id) = 0;
  virtual int64_t GetOffset() = 0;
  virtual int64_t NumRows() = 0;
  virtual boost::shared_ptr<ParquetScanner> GetScanner(
      int i, int64_t scan_size) = 0;
};

class Reader {
 public:
  virtual Type::type GetSchemaType(int i) = 0;
  virtual int GetTypeLength(int i) = 0;
  virtual int GetTypePrecision(int i) = 0;
  virtual int GetTypeScale(int i) = 0;
  virtual std::string& GetStreamName() = 0;
  virtual int NumColumns() = 0;
  virtual int NumRowGroups() = 0;
  virtual int64_t NumRows() = 0;
  virtual bool IsFlatSchema() = 0;
  virtual boost::shared_ptr<RowGroup> GetRowGroup(int i) = 0;
  // API to return a Reader
  static boost::shared_ptr<Reader> getReader(
      ExternalInputStream* stream, int64_t column_chunk_size,
      MemoryAllocator* pool = default_allocator());
  virtual int64_t EstimateMemoryUsage(std::list<int>& selected_columns, int row_group,
      int64_t batch_size, int64_t col_reader_size) = 0;
};

}  // namespace parquet

#endif  // PARQUET_CXX09API_READER_H
