/*
 *  Fast Positive Tables (libfpta), aka Позитивные Таблицы.
 *  Copyright 2016-2020 Leonid Yuriev <leo@yuriev.ru>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include "fpta_test.h"
#include "keygen.hpp"

/* Кол-во проверочных точек в диапазонах значений индексируемых типов.
 *
 * Значение не может быть больше чем 65536, так как это предел кол-ва
 * уникальных значений для fptu_uint16.
 *
 * Но для парного генератора не может быть больше 65536/NDUP,
 * так как для не-уникальных вторичных индексов нам требуются дубликаты,
 * что требует больше уникальных значений для первичного ключа.
 *
 * Использовать тут большие значения смысла нет. Время работы тестов
 * растет примерно линейно (чуть быстрее), тогда как вероятность
 * проявления каких-либо ошибок растет в лучшем случае как Log(NNN),
 * а скорее даже как SquareRoot(Log(NNN)).
 */
static constexpr int NDUP = 5;
#ifdef FPTA_CURSOR_UT_LONG
static constexpr int NNN = 13103; // около часа в /dev/shm/
#else
static constexpr int NNN = 41; // порядка 10-15 секунд в /dev/shm/
#endif

static const char testdb_name[] = TEST_DB_DIR "ut_cursor_secondary1.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_cursor_secondary1.fpta" MDBX_LOCK_SUFFIX;

#include "cursor_secondary.hpp"

//----------------------------------------------------------------------------

#ifdef INSTANTIATE_TEST_SUITE_P
INSTANTIATE_TEST_SUITE_P(
    Combine, CursorSecondary,
    ::testing::Combine(
        ::testing::Values(fpta_primary_unique_ordered_obverse,
                          fpta_primary_unique_ordered_reverse,
                          fpta_primary_withdups_ordered_obverse,
                          fpta_primary_withdups_ordered_reverse,
                          fpta_primary_unique_unordered,
                          fpta_primary_withdups_unordered),
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_96, fptu_128, fptu_160, fptu_datetime, fptu_256,
                          fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_secondary_unique_ordered_obverse,
                          fpta_secondary_unique_ordered_reverse,
                          fpta_secondary_unique_unordered),
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_96, fptu_128, fptu_160, fptu_datetime, fptu_256,
                          fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_unsorted, fpta_ascending, fpta_descending)));
#else
INSTANTIATE_TEST_CASE_P(
    Combine, CursorSecondary,
    ::testing::Combine(
        ::testing::Values(fpta_primary_unique_ordered_obverse,
                          fpta_primary_unique_ordered_reverse,
                          fpta_primary_withdups_ordered_obverse,
                          fpta_primary_withdups_ordered_reverse,
                          fpta_primary_unique_unordered,
                          fpta_primary_withdups_unordered),
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_96, fptu_128, fptu_160, fptu_datetime, fptu_256,
                          fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_secondary_unique_ordered_obverse,
                          fpta_secondary_unique_ordered_reverse,
                          fpta_secondary_unique_unordered),
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_96, fptu_128, fptu_160, fptu_datetime, fptu_256,
                          fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_unsorted, fpta_ascending, fpta_descending)));
#endif

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
