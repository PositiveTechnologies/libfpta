/*
 *  Fast Positive Tuples (libfptu), aka Позитивные Кортежи
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

#include "fptu_test.h"

#include "shuffle6.hpp"

static bool field_filter_any(const fptu_field *, void *context, void *param) {
  (void)context;
  (void)param;
  return true;
}

TEST(Remove, Base) {
  char space[fptu_buffer_enough];
  fptu_rw *pt = fptu_init(space, sizeof(space), fptu_max_fields);
  ASSERT_NE(nullptr, pt);

  // try to remove non-present field
  ASSERT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(0, fptu::erase(pt, 0, fptu_uint32));
  EXPECT_STREQ(nullptr, fptu::check(pt));

  // insert/delete one header-only field
  ASSERT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0, 0));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(1u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(1, fptu::erase(pt, 0, fptu_uint16));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(0, fptu::erase(pt, 0, fptu_uint32));
  EXPECT_STREQ(nullptr, fptu::check(pt));

  EXPECT_EQ(0u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0u, pt->junk);
  EXPECT_EQ(pt->pivot, pt->head);
  EXPECT_EQ(pt->pivot, pt->tail);

  // insert header-only a,b; then delete b,a
  ASSERT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0xA, 0));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0xB, 0));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(2u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));

  EXPECT_EQ(1, fptu::erase(pt, 0xB, fptu_uint16));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(1u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0u, pt->junk);

  EXPECT_EQ(1, fptu::erase(pt, 0xA, fptu_uint16));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(0u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0u, pt->junk);
  EXPECT_EQ(pt->pivot, pt->head);
  EXPECT_EQ(pt->pivot, pt->tail);

  // insert header-only a,b; then delete a,b
  ASSERT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0xA, 0));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0xB, 0));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(2u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));

  EXPECT_EQ(1, fptu::erase(pt, 0xA, fptu_uint16));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(1u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(1u, pt->junk);

  EXPECT_EQ(1, fptu::erase(pt, 0xB, fptu_uint16));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(0u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0u, pt->junk);
  EXPECT_EQ(pt->pivot, pt->head);
  EXPECT_EQ(pt->pivot, pt->tail);

  // insert a,b; then delete b,a
  ASSERT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, 0xA, 0));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, 0xB, 0));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(2u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));

  EXPECT_EQ(1, fptu::erase(pt, 0xB, fptu_uint32));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(1u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0u, pt->junk);

  EXPECT_EQ(1, fptu::erase(pt, 0xA, fptu_uint32));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(0u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0u, pt->junk);
  EXPECT_EQ(pt->pivot, pt->head);
  EXPECT_EQ(pt->pivot, pt->tail);

  // insert a,b; then delete a,b
  ASSERT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, 0xA, 0));
  EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, 0xB, 0));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(2u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));

  EXPECT_EQ(1, fptu::erase(pt, 0xA, fptu_uint32));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(1u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(2u, pt->junk);

  EXPECT_EQ(1, fptu::erase(pt, 0xB, fptu_uint32));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(0u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0u, pt->junk);
  EXPECT_EQ(pt->pivot, pt->head);
  EXPECT_EQ(pt->pivot, pt->tail);
}

TEST(Remove, Serie) {
  char space[fptu_buffer_enough];
  fptu_rw *pt = fptu_init(space, sizeof(space), fptu_max_fields);
  ASSERT_NE(nullptr, pt);

  for (unsigned n = 1; n < 11; ++n) {
    ASSERT_STREQ(nullptr, fptu::check(pt));
    for (unsigned i = 0; i < n; ++i) {
      EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0, i));
      EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, 0, i));
      EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, 1, i));
      EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 1, i));
    }
    ASSERT_STREQ(nullptr, fptu::check(pt));
    EXPECT_EQ(n * 4, fptu::field_count(pt, field_filter_any, nullptr, nullptr));

    EXPECT_EQ((int)n,
              fptu::erase(pt, 1, fptu_ffilter | fptu_filter_mask(fptu_uint16)));
    EXPECT_STREQ(nullptr, fptu::check(pt));
    EXPECT_EQ(n * 3, fptu::field_count(pt, field_filter_any, nullptr, nullptr));

    EXPECT_EQ((int)n,
              fptu::erase(pt, 1, fptu_ffilter | fptu_filter_mask(fptu_uint32)));
    EXPECT_STREQ(nullptr, fptu::check(pt));
    EXPECT_EQ(n * 2, fptu::field_count(pt, field_filter_any, nullptr, nullptr));

    for (unsigned i = 0; i < n; ++i) {
      EXPECT_EQ(1, fptu::erase(pt, 0, fptu_uint16));
      EXPECT_STREQ(nullptr, fptu::check(pt));
      EXPECT_EQ((n - i) * 2 - 1,
                fptu::field_count(pt, field_filter_any, nullptr, nullptr));

      EXPECT_EQ(1, fptu::erase(pt, 0, fptu_uint32));
      EXPECT_STREQ(nullptr, fptu::check(pt));
      EXPECT_EQ((n - i) * 2 - 2,
                fptu::field_count(pt, field_filter_any, nullptr, nullptr));
    }

    EXPECT_STREQ(nullptr, fptu::check(pt));
    ASSERT_EQ(0u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
    ASSERT_EQ(0u, fptu_junkspace(pt));
  }
}

TEST(Remove, Shuffle) {
  char space[fptu_buffer_enough];

  ASSERT_TRUE(shuffle6::selftest());

  for (unsigned create_iter = 0; create_iter < (1 << 6); ++create_iter) {
    unsigned create_mask = gray_code(create_iter);

    for (unsigned n = 0; n < shuffle6::factorial; ++n) {
      fptu_rw *pt = fptu_init(space, sizeof(space), fptu_max_fields);
      ASSERT_NE(nullptr, pt);

      SCOPED_TRACE("shuffle #" + std::to_string(n) + ", create-mask " +
                   std::to_string(create_mask));

      unsigned created_count = 0;
      for (unsigned i = 0; i < 6; ++i) {
        if (create_mask & (1 << i)) {
          switch (i % 3) {
          default:
            assert(false);
          case 0:
            EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, i, i));
            break;
          case 1:
            EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, i, i));
            break;
          case 2:
            EXPECT_EQ(FPTU_OK, fptu_insert_uint64(pt, i, i));
            break;
          }
          created_count++;
        }
      }

      ASSERT_STREQ(nullptr, fptu::check(pt));
      EXPECT_EQ(0u, fptu_junkspace(pt));
      EXPECT_EQ(created_count,
                fptu::field_count(pt, field_filter_any, nullptr, nullptr));

      int removed_count = 0;
      shuffle6 order(n);
      while (!order.empty()) {
        unsigned i = order.next();
        SCOPED_TRACE("shuffle-item #" + std::to_string(i));
        ASSERT_TRUE(i < 6);

        int present = (create_mask & (1 << i)) ? 1 : 0;
        switch (i % 3) {
        default:
          ASSERT_TRUE(false);
          break;
        case 0:
          EXPECT_EQ(present, fptu::erase(pt, i, fptu_uint16));
          break;
        case 1:
          EXPECT_EQ(present, fptu::erase(pt, i, fptu_uint32));
          break;
        case 2:
          EXPECT_EQ(present, fptu::erase(pt, i, fptu_uint64));
          break;
        }
        removed_count += present;

        ASSERT_STREQ(nullptr, fptu::check(pt));
        ASSERT_EQ(created_count - removed_count,
                  fptu::field_count(pt, field_filter_any, nullptr, nullptr));
      }

      ASSERT_EQ(0u, fptu_junkspace(pt));
    }
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
