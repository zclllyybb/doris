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

#pragma once

#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"

namespace doris {

/**
 * CastToTimestampTz wraps CastToDatetimeV2 with DataTimeCastEnumType::TIMESTAMP_TZ hardcoded,
 * so external callers never need to specify DataTimeCastEnumType explicitly.
 *
 * The difference from CastToDatetimeV2:
 * - When timezone info is present in the string, TIMESTAMP_TZ converts to UTC;
 *   DATE_TIME converts to local_time_zone.
 * - When timezone info is absent, TIMESTAMP_TZ treats input as local time and converts to UTC;
 *   DATE_TIME keeps the time as-is.
 */
struct CastToTimestampTz {
    template <bool IsStrict>
    static inline bool from_string_strict_mode(const StringRef& str,
                                               DateV2Value<DateTimeV2ValueType>& res,
                                               const cctz::time_zone* local_time_zone,
                                               uint32_t to_scale, CastParameters& params) {
        return CastToDatetimeV2::from_string_strict_mode_internal<
                IsStrict, DataTimeCastEnumType::TIMESTAMP_TZ>(str, res, local_time_zone, to_scale,
                                                              params);
    }

    static inline bool from_string_non_strict_mode_impl(const StringRef& str,
                                                        DateV2Value<DateTimeV2ValueType>& res,
                                                        const cctz::time_zone* local_time_zone,
                                                        uint32_t to_scale,
                                                        CastParameters& params) {
        return CastToDatetimeV2::from_string_non_strict_mode_internal<
                DataTimeCastEnumType::TIMESTAMP_TZ>(str, res, local_time_zone, to_scale, params);
    }

    static inline bool from_string_non_strict_mode(const StringRef& str,
                                                   DateV2Value<DateTimeV2ValueType>& res,
                                                   const cctz::time_zone* local_time_zone,
                                                   uint32_t to_scale, CastParameters& params) {
        return from_string_strict_mode<false>(str, res, local_time_zone, to_scale, params) ||
               from_string_non_strict_mode_impl(str, res, local_time_zone, to_scale, params);
    }

    // Auto dispatch based on params.is_strict
    static inline bool from_string(const StringRef& str, DateV2Value<DateTimeV2ValueType>& res,
                                   const cctz::time_zone* local_time_zone, uint32_t to_scale,
                                   CastParameters& params) {
        if (params.is_strict) {
            return from_string_strict_mode<true>(str, res, local_time_zone, to_scale, params);
        } else {
            return from_string_non_strict_mode(str, res, local_time_zone, to_scale, params);
        }
    }
};

} // namespace doris
