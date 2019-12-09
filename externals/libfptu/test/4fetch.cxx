/*
 *  Fast Positive Tuples (libfptu), aka Позитивные Кортежи
 *  Copyright 2016-2019 Leonid Yuriev <leo@yuriev.ru>
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

#include "fptu_test.h"

TEST(Fetch, Invalid) {
  fptu_ro ro;
  ro.total_bytes = 0;
  ro.units = nullptr;

  EXPECT_EQ(nullptr, fptu_fetch(ro, nullptr, 0, 0));
  EXPECT_EQ(nullptr, fptu_fetch(ro, nullptr, fptu_max_tuple_bytes / 2,
                                fptu_max_fields / 2));
  EXPECT_EQ(nullptr,
            fptu_fetch(ro, nullptr, fptu_max_tuple_bytes, fptu_max_fields));
  EXPECT_EQ(nullptr, fptu_fetch(ro, nullptr, ~0u, ~0u));

  char space_exactly_noitems[sizeof(fptu_rw)];
  EXPECT_EQ(nullptr, fptu_fetch(ro, space_exactly_noitems,
                                sizeof(space_exactly_noitems), 1));
  EXPECT_EQ(nullptr,
            fptu_fetch(ro, space_exactly_noitems, sizeof(space_exactly_noitems),
                       fptu_max_fields));
  EXPECT_EQ(nullptr, fptu_fetch(ro, nullptr, sizeof(space_exactly_noitems), 0));
  EXPECT_EQ(nullptr, fptu_fetch(ro, space_exactly_noitems,
                                sizeof(space_exactly_noitems) - 1, 0));
  EXPECT_EQ(nullptr, fptu_fetch(ro, space_exactly_noitems, 0, 0));
  EXPECT_EQ(nullptr, fptu_fetch(ro, space_exactly_noitems, 0, 1));
  EXPECT_EQ(nullptr, fptu_fetch(ro, space_exactly_noitems, 0, fptu_max_fields));
  EXPECT_EQ(nullptr,
            fptu_fetch(ro, space_exactly_noitems, 0, fptu_max_fields * 2));
  EXPECT_EQ(nullptr, fptu_fetch(ro, space_exactly_noitems, 0, ~0u));

  char space_maximum[sizeof(fptu_rw) + fptu_max_tuple_bytes];
  EXPECT_EQ(nullptr, fptu_fetch(ro, space_maximum, sizeof(space_maximum),
                                fptu_max_fields + 1));
  EXPECT_EQ(nullptr, fptu_fetch(ro, nullptr, sizeof(space_maximum), 0));
  EXPECT_EQ(nullptr, fptu_fetch(ro, space_exactly_noitems, ~0u, 1));
  ASSERT_EQ(nullptr, fptu_fetch(ro, space_exactly_noitems,
                                fptu_buffer_limit + 1, fptu_max_fields));

  fptu_rw *pt;
  pt = fptu_fetch(ro, space_exactly_noitems, sizeof(space_exactly_noitems), 0);
  ASSERT_NE(nullptr, pt);
  EXPECT_STREQ(nullptr, fptu::check(pt));
  pt = fptu_fetch(ro, space_maximum, sizeof(space_maximum), 0);
  ASSERT_NE(nullptr, pt);
  EXPECT_STREQ(nullptr, fptu::check(pt));
  pt = fptu_fetch(ro, space_maximum, sizeof(space_maximum), 1);
  ASSERT_NE(nullptr, pt);
  EXPECT_STREQ(nullptr, fptu::check(pt));
  pt =
      fptu_fetch(ro, space_maximum, sizeof(space_maximum), fptu_max_fields / 2);
  ASSERT_NE(nullptr, pt);
  EXPECT_STREQ(nullptr, fptu::check(pt));
  pt = fptu_fetch(ro, space_maximum, sizeof(space_maximum), fptu_max_fields);
  ASSERT_NE(nullptr, pt);
  EXPECT_STREQ(nullptr, fptu::check(pt));
}

TEST(Fetch, Base) {
  char origin_space[fptu_buffer_enough];
  char fetched_space[fptu_buffer_enough];
  fptu_ro origin_ro, fetched_ro;
  fptu_rw *origin_pt, *fetched_pt;

  origin_pt = fptu_init(origin_space, sizeof(origin_space), fptu_max_fields);
  ASSERT_NE(nullptr, origin_pt);
  EXPECT_STREQ(nullptr, fptu::check(origin_pt));
  origin_ro = fptu_take_noshrink(origin_pt);
  ASSERT_STREQ(nullptr, fptu::check(origin_ro));
  EXPECT_EQ(fptu_unit_size, origin_ro.total_bytes);

  // check empty without more-items
  fetched_pt = fptu_fetch(origin_ro, fetched_space, sizeof(fetched_space), 0);
  ASSERT_NE(nullptr, fetched_pt);
  EXPECT_STREQ(nullptr, fptu::check(fetched_pt));

  fetched_ro = fptu_take_noshrink(fetched_pt);
  ASSERT_STREQ(nullptr, fptu::check(fetched_ro));
  ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
  EXPECT_EQ(0,
            memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));

  // check empty with max-more-items
  fetched_pt = fptu_fetch(origin_ro, fetched_space, sizeof(fetched_space),
                          fptu_max_fields);
  ASSERT_NE(nullptr, fetched_pt);
  EXPECT_STREQ(nullptr, fptu::check(fetched_pt));

  fetched_ro = fptu_take_noshrink(fetched_pt);
  ASSERT_STREQ(nullptr, fptu::check(fetched_ro));
  ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
  EXPECT_EQ(0,
            memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));

  EXPECT_EQ(origin_pt->end, fetched_pt->end);
  EXPECT_EQ(origin_pt->pivot, fetched_pt->pivot);
  EXPECT_EQ(origin_pt->tail, fetched_pt->tail);
  EXPECT_EQ(origin_pt->head, fetched_pt->head);
  EXPECT_EQ(origin_pt->junk, fetched_pt->junk);

  // adds header-only fields and check
  EXPECT_EQ(FPTU_OK, fptu_insert_uint16(origin_pt, fptu_max_cols, 42));
  ASSERT_STREQ(nullptr, fptu::check(origin_pt));
  origin_ro = fptu_take_noshrink(origin_pt);
  ASSERT_STREQ(nullptr, fptu::check(origin_ro));
  EXPECT_EQ(fptu_unit_size * 2u, origin_ro.total_bytes);

  // check with max-more-items
  fetched_pt = fptu_fetch(origin_ro, fetched_space, sizeof(fetched_space),
                          fptu_max_fields);
  ASSERT_NE(nullptr, fetched_pt);
  EXPECT_STREQ(nullptr, fptu::check(fetched_pt));

  fetched_ro = fptu_take_noshrink(fetched_pt);
  ASSERT_STREQ(nullptr, fptu::check(fetched_ro));
  ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
  EXPECT_EQ(0,
            memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));

  EXPECT_EQ(origin_pt->end, fetched_pt->end);
  EXPECT_EQ(origin_pt->pivot, fetched_pt->pivot);
  EXPECT_EQ(origin_pt->tail, fetched_pt->tail);
  EXPECT_EQ(origin_pt->head, fetched_pt->head);
  EXPECT_EQ(origin_pt->junk, fetched_pt->junk);

  // check without more-items
  fetched_pt = fptu_fetch(origin_ro, fetched_space, sizeof(fetched_space), 0);
  ASSERT_NE(nullptr, fetched_pt);
  EXPECT_STREQ(nullptr, fptu::check(fetched_pt));

  fetched_ro = fptu_take_noshrink(fetched_pt);
  ASSERT_STREQ(nullptr, fptu::check(fetched_ro));
  ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
  EXPECT_EQ(0,
            memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));

  // re-create, adds fileds with payload and check
  origin_pt = fptu_init(origin_space, sizeof(origin_space), fptu_max_fields);
  ASSERT_NE(nullptr, origin_pt);
  EXPECT_STREQ(nullptr, fptu::check(origin_pt));

  EXPECT_EQ(FPTU_OK, fptu_insert_uint32(origin_pt, fptu_max_cols, 42));
  ASSERT_STREQ(nullptr, fptu::check(origin_pt));
  origin_ro = fptu_take_noshrink(origin_pt);
  ASSERT_STREQ(nullptr, fptu::check(origin_ro));
  EXPECT_EQ(fptu_unit_size * 3u, origin_ro.total_bytes);

  // check with max-more-items
  fetched_pt = fptu_fetch(origin_ro, fetched_space, sizeof(fetched_space),
                          fptu_max_fields);
  ASSERT_NE(nullptr, fetched_pt);
  EXPECT_STREQ(nullptr, fptu::check(fetched_pt));

  fetched_ro = fptu_take_noshrink(fetched_pt);
  ASSERT_STREQ(nullptr, fptu::check(fetched_ro));
  ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
  EXPECT_EQ(0,
            memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));

  EXPECT_EQ(origin_pt->end, fetched_pt->end);
  EXPECT_EQ(origin_pt->pivot, fetched_pt->pivot);
  EXPECT_EQ(origin_pt->tail, fetched_pt->tail);
  EXPECT_EQ(origin_pt->head, fetched_pt->head);
  EXPECT_EQ(origin_pt->junk, fetched_pt->junk);

  // check without more-items
  fetched_pt = fptu_fetch(origin_ro, fetched_space, sizeof(fetched_space), 0);
  ASSERT_NE(nullptr, fetched_pt);
  EXPECT_STREQ(nullptr, fptu::check(fetched_pt));

  fetched_ro = fptu_take_noshrink(fetched_pt);
  ASSERT_STREQ(nullptr, fptu::check(fetched_ro));
  ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
  EXPECT_EQ(0,
            memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));
}

TEST(Fetch, Variate) {
  char origin_space[fptu_buffer_enough];
  char fetched_space[fptu_buffer_enough];
  fptu_ro origin_ro, fetched_ro;
  fptu_rw *origin_pt, *fetched_pt;

  static const size_t space_cases[] = {
      /* clang-format off */
        4, 5, 6, 7, 8, 9, 42, sizeof(fptu_rw),
        fptu_max_tuple_bytes / 3, fptu_max_tuple_bytes / 2,
        fptu_max_tuple_bytes
      /* clang-format on */
  };

  static const unsigned items_cases[] = {
      /* clang-format off */
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 42, ~0u, fptu_max_fields / 3,
        fptu_max_fields / 2, fptu_max_fields, fptu_max_fields + 1,
        fptu_max_fields * 2
      /* clang-format on */
  };

  for (auto fetch_buffer_space : space_cases) {
    const size_t bytes = sizeof(fptu_rw) + fetch_buffer_space;
    ASSERT_LE(bytes, sizeof(fetched_space));

    for (auto more_items : items_cases) {

      origin_pt =
          fptu_init(origin_space, sizeof(origin_space), fptu_max_fields);
      ASSERT_NE(nullptr, origin_pt);
      EXPECT_STREQ(nullptr, fptu::check(origin_pt));
      origin_ro = fptu_take_noshrink(origin_pt);
      ASSERT_STREQ(nullptr, fptu::check(origin_ro));
      EXPECT_EQ(fptu_unit_size, origin_ro.total_bytes);

      // check empty
      size_t origin_items =
          (size_t)(fptu_end_ro(origin_ro) - fptu_begin_ro(origin_ro));
      size_t origin_payload_bytes =
          origin_ro.total_bytes - units2bytes(origin_items) - fptu_unit_size;
      SCOPED_TRACE(
          "origin.items " + std::to_string(origin_items) +
          ", origin.payload_bytes " + std::to_string(origin_payload_bytes) +
          ", fetch.buffer_space " + std::to_string(fetch_buffer_space) +
          ", fetch.more_items " + std::to_string(more_items));
      fetched_pt = fptu_fetch(origin_ro, fetched_space, bytes, more_items);
      if (more_items > fptu_max_fields ||
          bytes < fptu_space(origin_items + more_items, origin_payload_bytes)) {
        EXPECT_EQ(nullptr, fetched_pt);
      } else {
        EXPECT_NE(nullptr, fetched_pt);
      }
      if (!fetched_pt)
        continue;

      EXPECT_GE(0, (int)fptu::check_and_get_buffer_size(origin_ro, more_items,
                                                        0, nullptr));
      const char *error = "clean me";
      EXPECT_GE(bytes, fptu::check_and_get_buffer_size(origin_ro, more_items, 0,
                                                       &error));
      EXPECT_STREQ(nullptr, error);
      EXPECT_STREQ(nullptr, fptu::check(fetched_pt));
      fetched_ro = fptu_take_noshrink(fetched_pt);
      ASSERT_STREQ(nullptr, fptu::check(fetched_ro));
      ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
      EXPECT_EQ(
          0, memcmp(origin_ro.units, fetched_ro.units, origin_ro.total_bytes));

      if (more_items + origin_items >= fptu_max_fields) {
        EXPECT_EQ(origin_pt->pivot, fetched_pt->pivot);
        EXPECT_EQ(origin_pt->tail, fetched_pt->tail);
        EXPECT_EQ(origin_pt->head, fetched_pt->head);
        EXPECT_EQ(origin_pt->junk, fetched_pt->junk);
      }
      if (bytes == fptu_buffer_enough) {
        EXPECT_EQ(origin_pt->end, fetched_pt->end);
      }

      // adds header-only fields and check
      for (unsigned n = 1; n < 11; ++n) {
        SCOPED_TRACE("header-only, n = " + std::to_string(n));

        EXPECT_EQ(FPTU_OK, fptu_insert_uint16(origin_pt, fptu_max_cols, n));
        ASSERT_STREQ(nullptr, fptu::check(origin_pt));
        origin_ro = fptu_take_noshrink(origin_pt);
        ASSERT_STREQ(nullptr, fptu::check(origin_ro));
        EXPECT_EQ(fptu_unit_size * (n + 1), origin_ro.total_bytes);

        origin_items =
            (size_t)(fptu_end_ro(origin_ro) - fptu_begin_ro(origin_ro));
        origin_payload_bytes =
            origin_ro.total_bytes - units2bytes(origin_items) - fptu_unit_size;
        SCOPED_TRACE("origin.items " + std::to_string(origin_items) +
                     ", origin.payload_bytes " +
                     std::to_string(origin_payload_bytes) + ", fetch.space " +
                     std::to_string(fetch_buffer_space) + ", more_items " +
                     std::to_string(more_items));
        fetched_pt = fptu_fetch(origin_ro, fetched_space, bytes, more_items);
        if (more_items > fptu_max_fields ||
            bytes <
                fptu_space(origin_items + more_items, origin_payload_bytes)) {
          EXPECT_EQ(nullptr, fetched_pt);
        } else {
          EXPECT_NE(nullptr, fetched_pt);
        }
        if (!fetched_pt)
          continue;

        EXPECT_GE(bytes, fptu::check_and_get_buffer_size(origin_ro, more_items,
                                                         0, &error));
        EXPECT_STREQ(nullptr, error);
        EXPECT_STREQ(nullptr, fptu::check(fetched_pt));
        fetched_ro = fptu_take_noshrink(fetched_pt);
        ASSERT_STREQ(nullptr, fptu::check(fetched_ro));
        ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
        EXPECT_EQ(0, memcmp(origin_ro.units, fetched_ro.units,
                            origin_ro.total_bytes));

        if (more_items + origin_items >= fptu_max_fields) {
          EXPECT_EQ(origin_pt->pivot, fetched_pt->pivot);
          EXPECT_EQ(origin_pt->tail, fetched_pt->tail);
          EXPECT_EQ(origin_pt->head, fetched_pt->head);
          EXPECT_EQ(origin_pt->junk, fetched_pt->junk);
        }
        if (bytes == fptu_buffer_enough) {
          EXPECT_EQ(origin_pt->end, fetched_pt->end);
        }
      }

      origin_pt =
          fptu_init(origin_space, sizeof(origin_space), fptu_max_fields);
      ASSERT_NE(nullptr, origin_pt);
      EXPECT_STREQ(nullptr, fptu::check(origin_pt));

      // adds fileds with payload and check
      for (unsigned n = 1; n < 11; ++n) {
        SCOPED_TRACE("with-payload, n = " + std::to_string(n));

        EXPECT_EQ(FPTU_OK, fptu_insert_uint32(origin_pt, fptu_max_cols, n));
        ASSERT_STREQ(nullptr, fptu::check(origin_pt));
        origin_ro = fptu_take_noshrink(origin_pt);
        ASSERT_STREQ(nullptr, fptu::check(origin_ro));
        EXPECT_EQ(fptu_unit_size * (n + n + 1), origin_ro.total_bytes);

        origin_items =
            (size_t)(fptu_end_ro(origin_ro) - fptu_begin_ro(origin_ro));
        origin_payload_bytes =
            origin_ro.total_bytes - units2bytes(origin_items) - fptu_unit_size;
        SCOPED_TRACE("origin.items " + std::to_string(origin_items) +
                     ", origin.payload_bytes " +
                     std::to_string(origin_payload_bytes) + ", fetch.space " +
                     std::to_string(fetch_buffer_space) + ", more_items " +
                     std::to_string(more_items));
        fetched_pt = fptu_fetch(origin_ro, fetched_space, bytes, more_items);
        if (more_items > fptu_max_fields ||
            bytes <
                fptu_space(origin_items + more_items, origin_payload_bytes)) {
          EXPECT_EQ(nullptr, fetched_pt);
        } else {
          EXPECT_NE(nullptr, fetched_pt);
        }
        if (!fetched_pt)
          continue;

        EXPECT_GE(bytes, fptu::check_and_get_buffer_size(origin_ro, more_items,
                                                         0, &error));
        EXPECT_STREQ(nullptr, error);
        EXPECT_STREQ(nullptr, fptu::check(fetched_pt));
        fetched_ro = fptu_take_noshrink(fetched_pt);
        ASSERT_STREQ(nullptr, fptu::check(fetched_ro));
        ASSERT_EQ(origin_ro.total_bytes, fetched_ro.total_bytes);
        EXPECT_EQ(0, memcmp(origin_ro.units, fetched_ro.units,
                            origin_ro.total_bytes));

        if (more_items + origin_items >= fptu_max_fields) {
          EXPECT_EQ(origin_pt->pivot, fetched_pt->pivot);
          EXPECT_EQ(origin_pt->tail, fetched_pt->tail);
          EXPECT_EQ(origin_pt->head, fetched_pt->head);
          EXPECT_EQ(origin_pt->junk, fetched_pt->junk);
        }
        if (bytes == fptu_buffer_enough) {
          EXPECT_EQ(origin_pt->end, fetched_pt->end);
        }
      }
    }
  }
}

TEST(Fetch, DeNils) {
  EXPECT_EQ(fptu_null, fptu_field_type(nullptr));
  EXPECT_EQ(-1, fptu_field_column(nullptr));

  EXPECT_EQ((unsigned)UINT16_MAX /*FPTU_DENIL_UINT16*/,
            fptu_field_uint16(nullptr));
  EXPECT_EQ(INT32_MIN /*FPTU_DENIL_INT32*/, fptu_field_int32(nullptr));
  EXPECT_EQ(UINT32_MAX /*FPTU_DENIL_UINT32*/, fptu_field_uint32(nullptr));
  EXPECT_EQ(INT64_MIN /*FPTU_DENIL_INT64*/, fptu_field_int64(nullptr));
  EXPECT_EQ(UINT64_MAX /*FPTU_DENIL_UINT64*/, fptu_field_uint64(nullptr));
  EXPECT_TRUE(std::isnan(/*FPTU_DENIL_FP32*/ fptu_field_fp32(nullptr)));
  EXPECT_TRUE(std::isnan(/*FPTU_DENIL_FP64*/ fptu_field_fp64(nullptr)));
  EXPECT_EQ(0u /*FPTU_DENIL_TIME*/, fptu_field_datetime(nullptr).fixedpoint);
  EXPECT_EQ(nullptr /*FPTU_DENIL_CSTR*/, fptu_field_cstr(nullptr));

  EXPECT_EQ(nullptr /*FPTU_DENIL_FIXBIN*/, fptu_field_96(nullptr));
  EXPECT_EQ(nullptr /*FPTU_DENIL_FIXBIN*/, fptu_field_128(nullptr));
  EXPECT_EQ(nullptr /*FPTU_DENIL_FIXBIN*/, fptu_field_160(nullptr));
  EXPECT_EQ(nullptr, fptu_field_opaque(nullptr).iov_base);
  EXPECT_EQ(0u, fptu_field_opaque(nullptr).iov_len);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
