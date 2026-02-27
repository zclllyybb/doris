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

suite("test_money_format") {
    qt_money_format "SELECT money_format(NULL);"
    qt_money_format "SELECT money_format(0);"
    qt_money_format "SELECT money_format(0.000);"
    qt_money_format "select money_format(-123.125);"
    qt_money_format "select money_format(-17014116);"
    qt_money_format "select money_format(1123.456);"
    qt_money_format "select money_format(1123.4);"
    qt_money_format "select money_format(1.1249);"

    qt_money_format_dec32_2_1 "select money_format(-0.1);"
    qt_money_format_dec32_3_2 "select money_format(-0.11);"
    qt_money_format_dec32_4_3 "select money_format(-0.114);"
    qt_money_format_dec32_4_3 "select money_format(-0.115);"
    
    qt_money_format_dec32_9_0 """select money_format(cast(concat(repeat('9', 9)) as DECIMALV3(9, 0)));"""
    qt_money_format_dec32_9_0_negative """select money_format(cast(concat('-', repeat('9', 9)) as DECIMALV3(9, 0)));"""
    qt_money_format_dec32_9_1 """select money_format(cast(concat(repeat('9', 8), '.', repeat('9', 1)) as DECIMALV3(9, 1)));"""
    qt_money_format_dec32_9_1_negative """select money_format(cast(concat('-',repeat('9', 7), '.', repeat('9', 1)) as DECIMALV3(9, 1)));"""
    qt_money_format_dec32_9_2 """select money_format(cast(concat(repeat('9', 7), '.', repeat('9', 2)) as DECIMALV3(9, 2)));"""
    qt_money_format_dec32_9_2_negative """select money_format(cast(concat('-', repeat('9', 7), '.', repeat('9', 2)) as DECIMALV3(9, 2)));"""
    qt_money_format_dec32_9_9 """select money_format(cast(concat('0.', repeat('9', 9)) as DECIMALV3(9, 9)));"""
    qt_money_format_dec32_9_9_negative """select money_format(cast(concat('-', '0.', repeat('9', 9)) as DECIMALV3(9, 9)));"""

    qt_money_format_dec64_18_0 """select money_format(cast(concat(repeat('9', 18)) as DECIMALV3(18, 0)));"""
    qt_money_format_dec64_18_0_negative """select money_format(cast(concat('-', repeat('9', 18)) as DECIMALV3(18, 0)));"""
    qt_money_format_dec64_18_1 """select money_format(cast(concat(repeat('9', 17), '.', repeat('9', 1)) as DECIMALV3(18, 1)));"""
    qt_money_format_dec64_18_1_negative """select money_format(cast(concat('-',repeat('9', 17), '.', repeat('9', 1)) as DECIMALV3(18, 1)));"""
    qt_money_format_dec64_18_2 """select money_format(cast(concat(repeat('9', 15), '.', repeat('9', 2)) as DECIMALV3(18, 2)));"""
    qt_money_format_dec64_18_2_negative """select money_format(cast(concat('-', repeat('9', 15), '.', repeat('9', 2)) as DECIMALV3(18, 2)));"""
    qt_money_format_dec64_18_17 """select money_format(cast(concat('9.', repeat('9', 17)) as DECIMALV3(18, 17)))"""
    qt_money_format_dec64_18_17_negative """select money_format(cast(concat('-', '9.', repeat('9', 17)) as DECIMALV3(18, 17)))"""
    qt_money_format_dec64_18_18 """select money_format(cast(concat('0.', repeat('9', 18)) as DECIMALV3(18, 18)));"""
    qt_money_format_dec64_18_18_negative """select money_format(cast(concat('-', '0.', repeat('9', 18)) as DECIMALV3(18, 18)));"""
    
    qt_money_format_dec128_38_0 """select money_format(cast(concat(repeat('9', 38)) as DECIMALV3(38, 0)));"""
    qt_money_format_dec128_38_0_negative """select money_format(cast(concat('-', repeat('9', 38)) as DECIMALV3(38, 0)));"""
    qt_money_format_dec128_38_1 """select money_format(cast(concat(repeat('9', 37), '.', repeat('9', 1)) as DECIMALV3(38, 1)));"""
    qt_money_format_dec128_38_1_negative """select money_format(cast(concat('-',repeat('9', 37), '.', repeat('9', 1)) as DECIMALV3(38, 1)));"""
    qt_money_format_dec128_38_2 """select money_format(cast(concat(repeat('9', 36), '.', repeat('9', 2)) as DECIMALV3(38, 2)));"""
    qt_money_format_dec128_38_2_negative """select money_format(cast(concat('-', repeat('9', 36), '.', repeat('9', 2)) as DECIMALV3(38, 2)));"""
    qt_money_format_dec128_38_38 """select money_format(cast(concat('0.', repeat('9', 38)) as DECIMALV3(38, 38)));"""
    qt_money_format_dec128_38_38_negative """select money_format(cast(concat('-', '0.', repeat('9', 38)) as DECIMALV3(38, 38)));"""

    sql "set enable_decimal256=true;"
    qt_money_format_dec256_76_0 """select money_format(cast(concat(repeat('9', 76)) as DECIMALV3(76, 0)));"""
    qt_money_format_dec256_76_0_negative """select money_format(cast(concat('-', repeat('9', 76)) as DECIMALV3(76, 0)));"""
    qt_money_format_dec256_76_1 """select money_format(cast(concat(repeat('9', 75), '.', repeat('9', 1)) as DECIMALV3(76, 1)));"""
    qt_money_format_dec256_76_1_negative """select money_format(cast(concat('-', repeat('9', 75), '.', repeat('9', 1)) as DECIMALV3(76, 1)));"""
    qt_money_format_dec256_76_2 """select money_format(cast(concat(repeat('9', 74), '.', repeat('9', 2)) as DECIMALV3(76, 2)));"""
    qt_money_format_dec256_76_2_negative """select money_format(cast(concat('-', repeat('9', 74), '.', repeat('9', 2)) as DECIMALV3(76, 2)));"""
    // zero value: both scale=0 and scale=2 should produce "0.00"
    qt_money_format_dec256_zero_scale0 """select money_format(cast('0' as DECIMALV3(76, 0)));"""
    qt_money_format_dec256_zero_scale2 """select money_format(cast('0.00' as DECIMALV3(76, 2)));"""
    // mid-range scale=10: all-nines values round up with integer carry
    qt_money_format_dec256_76_10 """select money_format(cast(concat(repeat('9', 66), '.', repeat('9', 10)) as DECIMALV3(76, 10)));"""
    qt_money_format_dec256_76_10_negative """select money_format(cast(concat('-', repeat('9', 66), '.', repeat('9', 10)) as DECIMALV3(76, 10)));"""
    // mid-range scale=40: 9.999...40 nines rounds to 10.00
    qt_money_format_dec256_76_40 """select money_format(cast(concat('9.', repeat('9', 40)) as DECIMALV3(76, 40)));"""
    qt_money_format_dec256_76_40_negative """select money_format(cast(concat('-', '9.', repeat('9', 40)) as DECIMALV3(76, 40)));"""
    qt_money_format_dec256_76_76 """select money_format(cast(concat('0.', repeat('9', 76)) as DECIMALV3(76, 76)));"""
    qt_money_format_dec256_76_76_negative """select money_format(cast(concat('-', '0.', repeat('9', 76)) as DECIMALV3(76, 76)));"""
    // -0.xxx: integer part is 0 but value is negative, exercises append_sign_manually logic
    qt_money_format_dec256_neg_zero_frac """select money_format(cast('-0.5' as DECIMALV3(76, 1)));"""
    // rounding with carry: 0.999 (scale=3) rounds to 1.00, and 1234.999 carries into the integer part
    qt_money_format_dec256_76_3_round_carry """select money_format(cast('0.999' as DECIMALV3(76, 3)));"""
    qt_money_format_dec256_76_3_round_carry_negative """select money_format(cast('-0.999' as DECIMALV3(76, 3)));"""
    qt_money_format_dec256_76_3_round_int_carry """select money_format(cast('1234.999' as DECIMALV3(76, 3)));"""
    sql "set enable_decimal256=false;"
    // when enable_decimal256=false, FE rejects creating a DECIMALV3 with precision > 38
    test {
        sql """select money_format(cast('123.45' as DECIMALV3(76, 2)));"""
        exception "is disabled by default"
    }

    qt_money_format_interger "select money_format(1);"
    qt_money_format_interger "select money_format(-1);"
    qt_money_format_interger "select money_format(1233456789);"
    qt_money_format_interger "select money_format(-1233456789);"
    qt_money_format_interger """select money_format(cast("9223372036854775807" as BigInt))"""
    qt_money_format_interger """select money_format(cast("-9223372036854775808" as BigInt))"""
    qt_money_format_interger_int128_min """select money_format(-170141183460469231731687303715884105728);"""
    qt_money_format_interger_int128_max """select money_format(170141183460469231731687303715884105727);"""

    qt_money_format_double """select cast("1.2323" as Double), money_format(cast("1.2323" as Double));"""
    qt_money_format_double """select cast("1.2353" as Double), money_format(cast("1.2353" as Double));"""
    qt_money_format_double """select cast("-1.2353" as Double), money_format(cast("-1.2353" as Double));"""
    qt_money_format_double """select cast("-123456789.2353" as Double), money_format(cast("-123456789.2353" as Double));"""
    qt_money_format_double """select cast("-0.2353" as Double), money_format(cast("-0.2353" as Double));"""
}

