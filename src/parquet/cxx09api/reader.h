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
  static boost::shared_ptr<MuteBuffer> getBuffer(
      int64_t, MemoryAllocator* pool = default_allocator());
  virtual uint8_t* GetBufferPtr() = 0;
};

class ParquetScanner {
 public:
  // Guarantees values_buffered = batch_size if there are more sufficient values
  virtual int64_t SkipValues(int64_t batch_size) = 0;

  // This is mainly because BA and FLBA values do not own memory. The lifetime is limited
  // to a DataPage
  // So we cannot read past multiple data pages
  virtual int64_t NextValues(uint8_t* val, int64_t* values_buffered, int16_t* def_levels,
      int16_t* rep_levels, bool* is_null, int64_t batch_size) = 0;

  virtual int64_t getRemaining() = 0;
};

class RowGroup {
 public:
  virtual bool HasStats(int col_id) const = 0;
  virtual const std::string* GetMin(int col_id) const = 0;
  virtual const std::string* GetMax(int col_id) const = 0;
  virtual int GetNullCount(int col_id) const = 0;
  virtual int64_t GetOffset() const = 0;
  virtual int64_t GetLength() const = 0;
  virtual int64_t NumRows() const = 0;
  virtual boost::shared_ptr<ParquetScanner> GetScanner(int i) = 0;
};

class Reader {
 public:
  virtual Type::type GetSchemaType(int i) const = 0;
  virtual int GetTypeLength(int i) const = 0;
  virtual int GetTypePrecision(int i) const = 0;
  virtual int GetTypeScale(int i) const = 0;
  virtual std::string& GetStreamName() const = 0;
  virtual int NumColumns() const = 0;
  virtual int NumRealColumns() const = 0;
  virtual int NumRowGroups() const = 0;
  virtual int64_t NumRows() const = 0;
  virtual bool IsFlatSchema() const = 0;
  virtual boost::shared_ptr<RowGroup> GetRowGroup(int i) = 0;
  // API to return a Reader
  static boost::shared_ptr<Reader> getReader(ExternalInputStream* stream,
      int64_t column_chunk_size, MemoryAllocator* pool = default_allocator());
  virtual int64_t EstimateMemoryUsage(std::list<int>& selected_columns, int row_group,
      int64_t batch_size, int64_t col_reader_size) const = 0;
};

}  // namespace parquet

#endif  // PARQUET_CXX09API_READER_H
