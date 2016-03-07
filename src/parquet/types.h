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

#ifndef PARQUET_TYPES_H
#define PARQUET_TYPES_H

#include <algorithm>
#include <cstdint>
#include <string>

#include "parquet/cxx09api/types.h"
#include "parquet/util/compiler-util.h"
#include "parquet/util/visibility.h"

namespace parquet {

// parquet::PageType
struct PageType {
  enum type { DATA_PAGE, INDEX_PAGE, DICTIONARY_PAGE, DATA_PAGE_V2 };
};

template <int TYPE>
struct type_traits {};

template <>
struct type_traits<Type::BOOLEAN> {
  typedef bool value_type;
  static constexpr int value_byte_size = 1;

  static constexpr const char* printf_code = "d";
};

template <>
struct type_traits<Type::INT32> {
  typedef int32_t value_type;

  static constexpr int value_byte_size = 4;
  static constexpr const char* printf_code = "d";
};

template <>
struct type_traits<Type::INT64> {
  typedef int64_t value_type;

  static constexpr int value_byte_size = 8;
  static constexpr const char* printf_code = "ld";
};

template <>
struct type_traits<Type::INT96> {
  typedef Int96 value_type;

  static constexpr int value_byte_size = 12;
  static constexpr const char* printf_code = "s";
};

template <>
struct type_traits<Type::FLOAT> {
  typedef float value_type;

  static constexpr int value_byte_size = 4;
  static constexpr const char* printf_code = "f";
};

template <>
struct type_traits<Type::DOUBLE> {
  typedef double value_type;

  static constexpr int value_byte_size = 8;
  static constexpr const char* printf_code = "lf";
};

template <>
struct type_traits<Type::BYTE_ARRAY> {
  typedef ByteArray value_type;

  static constexpr int value_byte_size = sizeof(ByteArray);
  static constexpr const char* printf_code = "s";
};

template <>
struct type_traits<Type::FIXED_LEN_BYTE_ARRAY> {
  typedef FixedLenByteArray value_type;

  static constexpr int value_byte_size = sizeof(FixedLenByteArray);
  static constexpr const char* printf_code = "s";
};

template <typename Type>
inline std::string format_fwf(int width) {
  std::stringstream ss;
  ss << "%-" << width << type_traits<Type::type_num>::printf_code;
  return ss.str();
}

template <Type::type TYPE>
struct DataType {
  static constexpr Type::type type_num = TYPE;
  typedef typename type_traits<TYPE>::value_type c_type;
};

typedef DataType<Type::BOOLEAN> BooleanType;
typedef DataType<Type::INT32> Int32Type;
typedef DataType<Type::INT64> Int64Type;
typedef DataType<Type::INT96> Int96Type;
typedef DataType<Type::FLOAT> FloatType;
typedef DataType<Type::DOUBLE> DoubleType;
typedef DataType<Type::BYTE_ARRAY> ByteArrayType;
typedef DataType<Type::FIXED_LEN_BYTE_ARRAY> FLBAType;

}  // namespace parquet

#endif  // PARQUET_TYPES_H
