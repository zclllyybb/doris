
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

#include "vec/data_types/serde/data_type_serde.h"

#include <gen_cpp/types.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <math.h>
#include <stdlib.h>
#include <time.h>

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "olap/hll.h"
#include "util/bitmap_value.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "util/quantile_state.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_quantilestate.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

inline void column_to_pb(const DataTypePtr data_type, const IColumn& col, PValues* result) {
    const DataTypeSerDeSPtr serde = data_type->get_serde();
    static_cast<void>(serde->write_column_to_pb(col, *result, 0, col.size()));
}

inline void pb_to_column(const DataTypePtr data_type, PValues& result, IColumn& col) {
    auto serde = data_type->get_serde();
    static_cast<void>(serde->read_column_from_pb(col, result));
}

inline void check_pb_col(const DataTypePtr data_type, const IColumn& col) {
    PValues pv = PValues();
    column_to_pb(data_type, col, &pv);
    std::string s1 = pv.DebugString();

    auto col1 = data_type->create_column();
    pb_to_column(data_type, pv, *col1);
    PValues as_pv = PValues();
    column_to_pb(data_type, *col1, &as_pv);

    std::string s2 = as_pv.DebugString();
    EXPECT_EQ(s1, s2);
}

inline void serialize_and_deserialize_pb_test() {
    // int
    {
        auto vec = vectorized::ColumnInt32::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
        check_pb_col(data_type, *vec.get());
    }
    // string
    {
        auto strcol = vectorized::ColumnString::create();
        for (int i = 0; i < 1024; ++i) {
            std::string is = std::to_string(i);
            strcol->insert_data(is.c_str(), is.size());
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
        check_pb_col(data_type, *strcol.get());
    }
    // decimal
    {
        vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9, true));
        auto decimal_column = decimal_data_type->create_column();
        auto& data = ((vectorized::ColumnDecimal128V3*)decimal_column.get())->get_data();
        for (int i = 0; i < 1024; ++i) {
            __int128_t value = __int128_t(i * pow(10, 9) + i * pow(10, 8));
            data.push_back(value);
        }
        check_pb_col(decimal_data_type, *decimal_column.get());
    }
    // bitmap
    {
        vectorized::DataTypePtr bitmap_data_type(std::make_shared<vectorized::DataTypeBitMap>());
        auto bitmap_column = bitmap_data_type->create_column();
        std::vector<BitmapValue>& container =
                ((vectorized::ColumnBitmap*)bitmap_column.get())->get_data();
        for (int i = 0; i < 4; ++i) {
            BitmapValue bv;
            for (int j = 0; j <= i; ++j) {
                bv.add(j);
            }
            container.push_back(bv);
        }
        check_pb_col(bitmap_data_type, *bitmap_column.get());
    }
    // hll
    {
        vectorized::DataTypePtr hll_data_type(std::make_shared<vectorized::DataTypeHLL>());
        auto hll_column = hll_data_type->create_column();
        std::vector<HyperLogLog>& container =
                ((vectorized::ColumnHLL*)hll_column.get())->get_data();
        for (int i = 0; i < 4; ++i) {
            HyperLogLog hll;
            hll.update(i);
            container.push_back(hll);
        }
        check_pb_col(hll_data_type, *hll_column.get());
    }
    // quantilestate
    {
        vectorized::DataTypePtr quantile_data_type(
                std::make_shared<vectorized::DataTypeQuantileState>());
        auto quantile_column = quantile_data_type->create_column();
        std::vector<QuantileState>& container =
                ((vectorized::ColumnQuantileState*)quantile_column.get())->get_data();
        const long max_rand = 1000000L;
        double lower_bound = 0;
        double upper_bound = 100;
        srandom(time(nullptr));
        for (int i = 0; i < 1024; ++i) {
            QuantileState q;
            double random_double =
                    lower_bound + (upper_bound - lower_bound) * (random() % max_rand) / max_rand;
            q.add_value(random_double);
            container.push_back(q);
        }
        check_pb_col(quantile_data_type, *quantile_column.get());
    }
    // nullable string
    {
        vectorized::DataTypePtr string_data_type(std::make_shared<vectorized::DataTypeString>());
        vectorized::DataTypePtr nullable_data_type(
                std::make_shared<vectorized::DataTypeNullable>(string_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*)nullable_column.get())->insert_many_defaults(1024);
        check_pb_col(nullable_data_type, *nullable_column.get());
    }
    // nullable decimal
    {
        vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9, true));
        vectorized::DataTypePtr nullable_data_type(
                std::make_shared<vectorized::DataTypeNullable>(decimal_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*)nullable_column.get())->insert_many_defaults(1024);
        check_pb_col(nullable_data_type, *nullable_column.get());
    }
    // int with 1024 batch size
    {
        auto vec = vectorized::ColumnInt32::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        std::cout << vec->size() << std::endl;
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::DataTypePtr nullable_data_type(
                std::make_shared<vectorized::DataTypeNullable>(data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*)nullable_column.get())
                ->insert_range_from_not_nullable(*vec, 0, 1024);
        check_pb_col(nullable_data_type, *nullable_column.get());
    }
    // ipv4
    {
        auto vec = vectorized::ColumnIPv4 ::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeIPv4>());
        check_pb_col(data_type, *vec.get());
    }
    // ipv6
    {
        auto vec = vectorized::ColumnIPv6::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeIPv6>());
        check_pb_col(data_type, *vec.get());
    }
}

TEST(DataTypeSerDeTest, DataTypeScalaSerDeTest) {
    serialize_and_deserialize_pb_test();
}

TEST(DataTypeSerDeTest, DataTypeRowStoreSerDeTest) {
    // ipv6
    {
        std::string ip = "5be8:dde9:7f0b:d5a7:bd01:b3be:9c69:573b";
        auto vec = vectorized::ColumnIPv6::create();
        IPv6Value ipv6;
        EXPECT_TRUE(ipv6.from_string(ip));
        vec->insert(Field::create_field<TYPE_IPV6>(ipv6.value()));

        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeIPv6>());
        auto serde = data_type->get_serde(0);
        JsonbWriterT<JsonbOutStream> jsonb_writer;
        Arena pool;
        jsonb_writer.writeStartObject();
        serde->write_one_cell_to_jsonb(*vec, jsonb_writer, pool, 0, 0);
        jsonb_writer.writeEndObject();
        auto jsonb_column = ColumnString::create();
        jsonb_column->insert_data(jsonb_writer.getOutput()->getBuffer(),
                                  jsonb_writer.getOutput()->getSize());
        StringRef jsonb_data = jsonb_column->get_data_at(0);
        JsonbDocument* pdoc = nullptr;
        auto st = JsonbDocument::checkAndCreateDocument(jsonb_data.data, jsonb_data.size, &pdoc);
        ASSERT_TRUE(st.ok()) << "checkAndCreateDocument failed: " << st.to_string();
        JsonbDocument& doc = *pdoc;
        for (auto it = doc->begin(); it != doc->end(); ++it) {
            serde->read_one_cell_from_jsonb(*vec, it->value());
        }
        EXPECT_TRUE(vec->size() == 2);
        IPv6 data = vec->get_element(1);
        IPv6Value ipv6_value(data);
        EXPECT_EQ(ipv6_value.to_string(), ip);
    }

    // ipv4
    {
        std::string ip = "192.0.0.1";
        auto vec = vectorized::ColumnIPv4::create();
        IPv4Value ipv4;
        EXPECT_TRUE(ipv4.from_string(ip));
        vec->insert(Field::create_field<TYPE_IPV4>(ipv4.value()));

        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeIPv4>());
        auto serde = data_type->get_serde(0);
        JsonbWriterT<JsonbOutStream> jsonb_writer;
        Arena pool;
        jsonb_writer.writeStartObject();
        serde->write_one_cell_to_jsonb(*vec, jsonb_writer, pool, 0, 0);
        jsonb_writer.writeEndObject();
        auto jsonb_column = ColumnString::create();
        jsonb_column->insert_data(jsonb_writer.getOutput()->getBuffer(),
                                  jsonb_writer.getOutput()->getSize());
        StringRef jsonb_data = jsonb_column->get_data_at(0);
        JsonbDocument* pdoc = nullptr;
        auto st = JsonbDocument::checkAndCreateDocument(jsonb_data.data, jsonb_data.size, &pdoc);
        ASSERT_TRUE(st.ok()) << "checkAndCreateDocument failed: " << st.to_string();
        JsonbDocument& doc = *pdoc;
        for (auto it = doc->begin(); it != doc->end(); ++it) {
            serde->read_one_cell_from_jsonb(*vec, it->value());
        }
        EXPECT_TRUE(vec->size() == 2);
        IPv4 data = vec->get_element(1);
        IPv4Value ipv4_value(data);
        EXPECT_EQ(ipv4_value.to_string(), ip);
    }
}

} // namespace doris::vectorized
