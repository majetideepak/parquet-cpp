// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef PARQUET_FILE_READER_H
#define PARQUET_FILE_READER_H

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <list>
#include <string>
#include <vector>

#include "parquet/column/page.h"
#include "parquet/column/properties.h"
#include "parquet/schema/descriptor.h"
#include "parquet/util/input.h"

namespace parquet {

class ColumnReader;
class RandomAccessSource;

struct RowGroupStatistics {
  int64_t num_values;
  int64_t null_count;
  int64_t distinct_count;
  const std::string* min;
  const std::string* max;
};

class RowGroupReader {
 public:
  // Forward declare the PIMPL
  struct Contents {
    virtual int num_columns() const = 0;
    virtual int64_t num_rows() const = 0;
    virtual RowGroupStatistics GetColumnStats(int i) = 0;
    virtual bool IsColumnStatsSet(int i) = 0;
    virtual int64_t GetFileOffset() = 0;
    virtual std::unique_ptr<PageReader> GetColumnPageReader(int i) = 0;
    virtual RowGroupStatistics GetColumnStats(int i) const = 0;
    virtual bool IsColumnStatsSet(int i) const = 0;
    virtual Compression::type GetColumnCompression(int i) const = 0;
    virtual std::vector<Encoding::type> GetColumnEncodings(int i) const = 0;
    virtual int64_t GetColumnCompressedSize(int i) const = 0;
    virtual int64_t GetColumnUnCompressedSize(int i) const = 0;
  };

  RowGroupReader(const SchemaDescriptor* schema, std::unique_ptr<Contents> contents,
      MemoryAllocator* allocator);

  // Construct a ColumnReader for the indicated row group-relative
  // column. Ownership is shared with the RowGroupReader.
  std::shared_ptr<ColumnReader> Column(int i);
  int num_columns() const;
  int64_t num_rows() const;

  RowGroupStatistics GetColumnStats(int i) const;
  Compression::type GetColumnCompression(int i) const;
  std::vector<Encoding::type> GetColumnEncodings(int i) const;
  int64_t GetColumnCompressedSize(int i) const;
  int64_t GetColumnUnCompressedSize(int i) const;
  bool HasColumnStats(int i) const;
  int64_t GetFileOffset() const;

 private:
  // Owned by the parent ParquetFileReader
  const SchemaDescriptor* schema_;

  // PIMPL idiom
  // This is declared in the .cc file so that we can hide compiled Thrift
  // headers from the public API and also more easily create test fixtures.
  std::unique_ptr<Contents> contents_;

  MemoryAllocator* allocator_;
};

class ParquetFileReader {
 public:
  struct MemoryUsage {
    // Estimated dynamic memory usage (not including memory usage for dictionaries).
    int64_t memory;
    // This flag is set if any column is dictionary-encoded.
    bool has_dictionary;
    MemoryUsage() : memory(0), has_dictionary(false) {}
  };

  // Forward declare the PIMPL
  struct Contents {
    virtual ~Contents() {}
    // Perform any cleanup associated with the file contents
    virtual void Close() = 0;

    virtual std::shared_ptr<RowGroupReader> GetRowGroup(int i) = 0;

    virtual int64_t num_rows() const = 0;
    virtual int num_columns() const = 0;
    virtual bool is_compressed_column(int, int) const = 0;
    virtual int num_row_groups() const = 0;
    virtual int64_t metadata_length() const = 0;

    // Estimate dynamic memory usage from FileMetaData
    virtual MemoryUsage EstimateMemoryUsage(
        int row_group, std::list<int>& selected_columns) = 0;

    // Return const-poitner to make it clear that this object is not to be copied
    const SchemaDescriptor* schema() const { return &schema_; }
    SchemaDescriptor schema_;
  };

  explicit ParquetFileReader(MemoryAllocator* allocator = default_allocator());
  ~ParquetFileReader();

  // API Convenience to open a serialized Parquet file on disk
  static std::unique_ptr<ParquetFileReader> OpenFile(const std::string& path,
      bool memory_map = true, ReaderProperties props = default_reader_properties());

  static std::unique_ptr<ParquetFileReader> Open(
      std::unique_ptr<RandomAccessSource> source,
      ReaderProperties props = default_reader_properties());

  void Open(std::unique_ptr<Contents> contents);
  void Close();

  // The RowGroupReader is owned by the FileReader
  std::shared_ptr<RowGroupReader> RowGroup(int i);

  bool is_compressed_column(int, int);
  int num_columns() const;
  int64_t num_rows() const;
  int num_row_groups() const;

  // Returns the file schema descriptor
  const SchemaDescriptor* descr() { return schema_; }

  const ColumnDescriptor* column_schema(int i) const { return schema_->Column(i); }

  // Estimate dynamic memory usage
  MemoryUsage EstimateMemoryUsage(std::list<int>& selected_columns, int row_group,
      int64_t batch_size, int64_t column_reader_size = 0);

  void DebugPrint(std::ostream& stream, std::list<int>& selected_columns,
      int64_t batch_size, bool print_values = true);

 private:
  // PIMPL idiom
  // This is declared in the .cc file so that we can hide compiled Thrift
  // headers from the public API and also more easily create test fixtures.
  std::unique_ptr<Contents> contents_;

  // The SchemaDescriptor is provided by the Contents impl
  const SchemaDescriptor* schema_;

  MemoryAllocator* allocator_;

  void CheckSelectedColumns(std::list<int>& selected_columns);
};

}  // namespace parquet

#endif  // PARQUET_FILE_READER_H
