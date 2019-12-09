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

#include "shuffle6.hpp"

static bool field_filter_any(const fptu_field *, void *context, void *param) {
  (void)context;
  (void)param;
  return true;
}

TEST(Shrink, Base) {
  char space[fptu_buffer_enough];
  fptu_rw *pt = fptu_init(space, sizeof(space), fptu_max_fields);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu::check(pt));

  // shrink empty
  fptu_shrink(pt);
  ASSERT_STREQ(nullptr, fptu::check(pt));

  // shrink one header-only field
  EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0xA, 0xAA42));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(1u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  fptu_shrink(pt);
  ASSERT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(1u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0u, pt->junk);

  // add one more header-only and erase first
  EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, 0xB, 0xBB43));
  EXPECT_EQ(1, fptu::erase(pt, 0xA, fptu_uint16));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(1u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(1u, pt->junk);
  fptu_shrink(pt);
  ASSERT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(1u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0u, pt->junk);
  fptu_field *fp = fptu::lookup(pt, 0xB, fptu_uint16);
  ASSERT_NE(nullptr, fp);
  EXPECT_EQ(0xBB43u, fptu_field_uint16(fp));

  // add thrid field and erase previous
  EXPECT_EQ(FPTU_OK, fptu_insert_uint32(pt, 0xC, 42));
  EXPECT_EQ(1, fptu::erase(pt, 0xB, fptu_uint16));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(1u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(1u, pt->junk);
  fptu_shrink(pt);
  ASSERT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(1u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0u, pt->junk);
  fp = fptu::lookup(pt, 0xC, fptu_uint32);
  ASSERT_NE(nullptr, fp);
  EXPECT_EQ(42u, fptu_field_uint32(fp));

  // add fourth field and erase previous
  EXPECT_EQ(FPTU_OK, fptu_insert_int64(pt, 0xD, -555));
  EXPECT_EQ(1, fptu::erase(pt, 0xC, fptu_uint32));
  EXPECT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(1u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(2u, pt->junk);
  fptu_shrink(pt);
  ASSERT_STREQ(nullptr, fptu::check(pt));
  EXPECT_EQ(1u, fptu::field_count(pt, field_filter_any, nullptr, nullptr));
  EXPECT_EQ(0u, pt->junk);
  fp = fptu::lookup(pt, 0xD, fptu_int64);
  ASSERT_NE(nullptr, fp);
  EXPECT_EQ(-555, fptu_field_int64(fp));
}

TEST(Shrink, Shuffle) {
  char space[fptu_buffer_enough];

  ASSERT_TRUE(shuffle6::selftest());

  for (unsigned create_iter = 0; create_iter < (1 << 6); ++create_iter) {
    const unsigned create_mask = gray_code(create_iter);
    for (unsigned n = 0; n < shuffle6::factorial; ++n) {
      shuffle6 order(n);
      while (!order.empty()) {
        fptu_rw *pt = fptu_init(space, sizeof(space), fptu_max_fields);
        ASSERT_NE(nullptr, pt);

        unsigned count = 0;
        for (unsigned i = 0; i < 6; ++i) {
          if (create_mask & (1 << i)) {
            switch (i % 3) {
            default:
              assert(false);
            case 0:
              EXPECT_EQ(FPTU_OK, fptu_insert_uint16(pt, i, 7717 * i));
              break;
            case 1:
              EXPECT_EQ(FPTU_OK, fptu_insert_int32(pt, i, -14427139 * (int)i));
              break;
            case 2:
              EXPECT_EQ(FPTU_OK, fptu_insert_uint64(
                                     pt, i, UINT64_C(53299271467827031) * i));
              break;
            }
            count++;
          }
        }

        ASSERT_STREQ(nullptr, fptu::check(pt));
        EXPECT_EQ(0u, fptu_junkspace(pt));
        EXPECT_EQ(count,
                  fptu::field_count(pt, field_filter_any, nullptr, nullptr));

        unsigned present_mask = create_mask;
        unsigned o = order.next();
        ASSERT_TRUE(o < 6);

        unsigned present = (create_mask & (1u << o)) ? 1u : 0u;
        switch (o % 3) {
        default:
          assert(false);
        case 0:
          EXPECT_EQ((int)present, fptu::erase(pt, o, fptu_uint16));
          break;
        case 1:
          EXPECT_EQ((int)present, fptu::erase(pt, o, fptu_int32));
          break;
        case 2:
          EXPECT_EQ((int)present, fptu::erase(pt, o, fptu_uint64));
          break;
        }

        if (present) {
          assert(count >= present);
          count -= 1;
          present_mask -= 1 << o;
        }

        SCOPED_TRACE("shuffle #" + std::to_string(n) + ", create-mask " +
                     std::to_string(create_mask) + ", shuffle-item #" +
                     std::to_string(o) + ", present-mask #" +
                     std::to_string(present_mask));

        ASSERT_STREQ(nullptr, fptu::check(pt));
        ASSERT_EQ(count,
                  fptu::field_count(pt, field_filter_any, nullptr, nullptr));

        fptu_shrink(pt);
        ASSERT_STREQ(nullptr, fptu::check(pt));
        ASSERT_EQ(count,
                  fptu::field_count(pt, field_filter_any, nullptr, nullptr));
        EXPECT_EQ(0u, pt->junk);

        if (count) {
          for (unsigned i = 0; i < 6; ++i) {
            if (present_mask & (1 << i)) {
              fptu_field *fp;
              switch (i % 3) {
              default:
                ASSERT_TRUE(false);
                break;
              case 0:
                fp = fptu::lookup(pt, i, fptu_uint16);
                ASSERT_NE(nullptr, fp);
                EXPECT_EQ(7717 * i, fptu_field_uint16(fp));
                break;
              case 1:
                fp = fptu::lookup(pt, i, fptu_int32);
                ASSERT_NE(nullptr, fp);
                EXPECT_EQ(INT32_C(-14427139) * (int)i, fptu_field_int32(fp));
                break;
              case 2:
                fp = fptu::lookup(pt, i, fptu_uint64);
                ASSERT_NE(nullptr, fp);
                EXPECT_EQ(UINT64_C(53299271467827031) * i,
                          fptu_field_uint64(fp));
                break;
              }
            }
          }
        }
      }
    }
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
