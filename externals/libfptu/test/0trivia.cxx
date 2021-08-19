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

#include <cmath>

#if defined(_WIN32) || defined(_WIN64)
#pragma warning(push, 1)
#include <windows.h>
#pragma warning(pop)

#pragma warning(disable : 4244) /* 'initializing' : conversion from 'double'   \
                                   to 'uint32_t', possible loss of data */
#pragma warning(disable : 4365) /* 'initializing' : conversion from 'int' to   \
                                   'uint32_t', signed / unsigned mismatch */

static void usleep(unsigned usec) {
  HANDLE timer;
  LARGE_INTEGER li;

  /* Convert to 100 nanosecond interval,
   * negative value indicates relative time */
  li.QuadPart = -(10 * (int64_t)usec);

  timer = CreateWaitableTimer(NULL, TRUE, NULL);
  SetWaitableTimer(timer, &li, 0, NULL, NULL, 0);
  WaitForSingleObject(timer, INFINITE);
  CloseHandle(timer);
}
#endif /* _MSC_VER */

const auto ms100 = fptu_time::ms2fractional(100);

TEST(Trivia, Denil) {
  union {
    double f;
    uint64_t u;
  } denil64;
  denil64 = {FPTU_DENIL_FP64};
  EXPECT_EQ(FPTU_DENIL_FP64_BIN, denil64.u);
  denil64 = {fptu_fp64_denil()};
  EXPECT_EQ(FPTU_DENIL_FP64_BIN, denil64.u);
#ifdef HAVE_nan
  denil64 = {-nan("0x000FffffFFFFffff")};
#if defined(FPTU_DENIL_FP64_MAS) || defined(__LCC__)
  EXPECT_EQ(FPTU_DENIL_FP64_BIN, denil64.u);
#else
  EXPECT_NE(FPTU_DENIL_FP64_BIN, denil64.u);
#endif
#endif /* HAVE_nan */
  denil64 = {fptu_fp32_denil()};
  EXPECT_NE(FPTU_DENIL_FP64_BIN, denil64.u);

  union {
    float f;
    uint32_t u;
  } denil32;
  denil32 = {FPTU_DENIL_FP32};
  EXPECT_EQ(FPTU_DENIL_FP32_BIN, denil32.u);
  denil32 = {fptu_fp32_denil()};
  EXPECT_EQ(FPTU_DENIL_FP32_BIN, denil32.u);
#ifdef HAVE_nanf
  denil32 = {-nanf("0x007FFFFF")};
#if defined(FPTU_DENIL_FP32_MAS) || defined(__LCC__)
  EXPECT_EQ(FPTU_DENIL_FP32_BIN, denil32.u);
#else
  EXPECT_NE(FPTU_DENIL_FP32_BIN, denil32.u);
#endif
#endif /* HAVE_nanf */
  denil32 = {static_cast<float>(fptu_fp64_denil())};
  EXPECT_EQ(FPTU_DENIL_FP32_BIN, denil32.u);
}

TEST(Trivia, Apriory) {
  ASSERT_EQ(sizeof(uint16_t) * CHAR_BIT, fptu_bits);
  ASSERT_EQ(fptu_unit_size * CHAR_BIT / 2, fptu_bits);
  ASSERT_EQ(UINT16_MAX, (uint16_t)-1ll);
  ASSERT_EQ(UINT32_MAX, (uint32_t)-1ll);
  ASSERT_GE(UINT16_MAX, fptu_limit);
  ASSERT_EQ(UINT16_MAX, fptu_limit);
  ASSERT_TRUE(FPT_IS_POWER2(fptu_bits));
  ASSERT_TRUE(FPT_IS_POWER2(fptu_unit_size));
  ASSERT_EQ(fptu_unit_size, 1 << fptu_unit_shift);

  ASSERT_EQ(fptu_bits, fptu_typeid_bits + fptu_ct_reserve_bits + fptu_co_bits);
  ASSERT_EQ(fptu_bits, fptu_lx_bits + fptu_lt_bits);

  ASSERT_LE(fptu_max_cols, fptu_max_fields);
  ASSERT_LE(fptu_max_field_bytes, fptu_limit * fptu_unit_size);
  ASSERT_LE(fptu_max_opaque_bytes, fptu_max_field_bytes - fptu_unit_size);

  ASSERT_LE(fptu_max_array_len, fptu_max_fields);
  ASSERT_LE(fptu_max_array_len, fptu_max_field_bytes / fptu_unit_size - 1);
  ASSERT_GE(fptu_max_field_bytes, fptu_max_fields * fptu_unit_size);
  ASSERT_GE(fptu_max_tuple_bytes, fptu_max_field_bytes + fptu_unit_size * 2);
  ASSERT_GE(fptu_max_tuple_bytes, (fptu_max_fields + 1) * fptu_unit_size * 2);
  ASSERT_LE(fptu_buffer_enough, fptu_buffer_limit);

  ASSERT_EQ(fptu_ty_mask, fptu_farray | fptu_nested);
  ASSERT_GT(fptu_fr_mask, fptu_ty_mask);
  ASSERT_LT(fptu_fr_mask, 1 << fptu_co_shift);
  ASSERT_GT(fptu_limit, fptu_max_cols << fptu_co_shift);

  ASSERT_GT((size_t)fptu_ffilter, (size_t)fptu_ty_mask);
  ASSERT_EQ(fptu_ffilter, fptu_ffilter & fptu_any);

  ASSERT_EQ(0u, tag_elem_size(fptu_null));
  ASSERT_EQ(0u, tag_elem_size(fptu_uint16));
  ASSERT_EQ(0u, tag_elem_size(fptu_16));

  ASSERT_EQ(4u, tag_elem_size(fptu_int32));
  ASSERT_EQ(4u, tag_elem_size(fptu_uint32));
  ASSERT_EQ(4u, tag_elem_size(fptu_fp32));
  ASSERT_EQ(4u, tag_elem_size(fptu_32));

  ASSERT_EQ(8u, tag_elem_size(fptu_int64));
  ASSERT_EQ(8u, tag_elem_size(fptu_uint64));
  ASSERT_EQ(8u, tag_elem_size(fptu_fp64));
  ASSERT_EQ(8u, tag_elem_size(fptu_64));

  ASSERT_EQ(12u, tag_elem_size(fptu_96));
  ASSERT_EQ(16u, tag_elem_size(fptu_128));
  ASSERT_EQ(20u, tag_elem_size(fptu_160));
  ASSERT_EQ(8u, tag_elem_size(fptu_datetime));
  ASSERT_EQ(32u, tag_elem_size(fptu_256));

  ASSERT_EQ(bytes2units(tag_elem_size(fptu_null)),
            fptu_internal_map_t2u[fptu_null]);
  ASSERT_EQ(bytes2units(tag_elem_size(fptu_uint16)),
            fptu_internal_map_t2u[fptu_uint16]);
  ASSERT_EQ(bytes2units(tag_elem_size(fptu_16)),
            fptu_internal_map_t2u[fptu_16]);

  ASSERT_EQ(bytes2units(tag_elem_size(fptu_int32)),
            fptu_internal_map_t2u[fptu_int32]);
  ASSERT_EQ(bytes2units(tag_elem_size(fptu_uint32)),
            fptu_internal_map_t2u[fptu_uint32]);
  ASSERT_EQ(bytes2units(tag_elem_size(fptu_fp32)),
            fptu_internal_map_t2u[fptu_fp32]);
  ASSERT_EQ(bytes2units(tag_elem_size(fptu_32)),
            fptu_internal_map_t2u[fptu_32]);

  ASSERT_EQ(bytes2units(tag_elem_size(fptu_int64)),
            fptu_internal_map_t2u[fptu_int64]);
  ASSERT_EQ(bytes2units(tag_elem_size(fptu_uint64)),
            fptu_internal_map_t2u[fptu_uint64]);
  ASSERT_EQ(bytes2units(tag_elem_size(fptu_fp64)),
            fptu_internal_map_t2u[fptu_fp64]);
  ASSERT_EQ(bytes2units(tag_elem_size(fptu_64)),
            fptu_internal_map_t2u[fptu_64]);

  ASSERT_EQ(bytes2units(tag_elem_size(fptu_96)),
            fptu_internal_map_t2u[fptu_96]);
  ASSERT_EQ(bytes2units(tag_elem_size(fptu_128)),
            fptu_internal_map_t2u[fptu_128]);
  ASSERT_EQ(bytes2units(tag_elem_size(fptu_160)),
            fptu_internal_map_t2u[fptu_160]);
  ASSERT_EQ(bytes2units(tag_elem_size(fptu_datetime)),
            fptu_internal_map_t2u[fptu_datetime]);
  ASSERT_EQ(bytes2units(tag_elem_size(fptu_256)),
            fptu_internal_map_t2u[fptu_256]);

  ASSERT_EQ(4u, sizeof(fptu_varlen));
  ASSERT_EQ(4u, sizeof(fptu_field));
  ASSERT_EQ(4u, sizeof(fptu_unit));
  ASSERT_EQ(sizeof(struct iovec), sizeof(fptu_ro));

  ASSERT_EQ(sizeof(fptu_rw), fptu_space(0, 0));
}

TEST(Trivia, ColType) {
  uint_fast16_t tag;
  tag = fptu::make_tag(0, fptu_null);
  ASSERT_EQ(0u, tag);
  ASSERT_GT(fptu_limit, tag);
  EXPECT_EQ(0u, fptu::get_colnum(tag));
  EXPECT_EQ(fptu_null, fptu::get_type(tag));

  tag = fptu::make_tag(42, fptu_int64);
  ASSERT_NE(0u, tag);
  ASSERT_GT(fptu_limit, tag);
  EXPECT_EQ(42u, fptu::get_colnum(tag));
  EXPECT_EQ(fptu_int64, fptu::get_type(tag));

  tag = fptu::make_tag(fptu_max_cols, fptu_array_cstr);
  ASSERT_NE(0u, tag);
  ASSERT_GT(fptu_limit, tag);
  EXPECT_EQ(fptu_max_cols, fptu::get_colnum(tag));
  EXPECT_EQ(fptu_cstr | fptu_farray, fptu::get_type(tag));
}

TEST(Trivia, cmp2int) {
  EXPECT_EQ(0, fptu_cmp2int(41, 41));
  EXPECT_EQ(1, fptu_cmp2int(42, 41));
  EXPECT_EQ(-1, fptu_cmp2int(41, 42));

  EXPECT_EQ(0, fptu_cmp2int(-41, -41));
  EXPECT_EQ(1, fptu_cmp2int(0, -41));
  EXPECT_EQ(-1, fptu_cmp2int(-41, 0));

  EXPECT_EQ(1, fptu_cmp2int(42, -42));
  EXPECT_EQ(-1, fptu_cmp2int(-42, 42));
}

TEST(Trivia, cmp2lge) {
  EXPECT_EQ(fptu_eq, fptu_cmp2lge(41, 41));
  EXPECT_EQ(fptu_gt, fptu_cmp2lge(42, 41));
  EXPECT_EQ(fptu_lt, fptu_cmp2lge(41, 42));

  EXPECT_EQ(fptu_eq, fptu_cmp2lge(-41, -41));
  EXPECT_EQ(fptu_gt, fptu_cmp2lge(0, -41));
  EXPECT_EQ(fptu_lt, fptu_cmp2lge(-41, 0));

  EXPECT_EQ(fptu_gt, fptu_cmp2lge(42, -42));
  EXPECT_EQ(fptu_lt, fptu_cmp2lge(-42, 42));
}

TEST(Trivia, diff2lge) {
  EXPECT_EQ(fptu_eq, fptu_diff2lge(0));
  EXPECT_EQ(fptu_gt, fptu_diff2lge(1));
  EXPECT_EQ(fptu_gt, fptu_diff2lge(INT_MAX));
  EXPECT_EQ(fptu_gt, fptu_diff2lge(LONG_MAX));
  EXPECT_EQ(fptu_gt, fptu_diff2lge(ULONG_MAX));
  EXPECT_EQ(fptu_lt, fptu_diff2lge(-1));
  EXPECT_EQ(fptu_lt, fptu_diff2lge(INT_MIN));
  EXPECT_EQ(fptu_lt, fptu_diff2lge(LONG_MIN));
}

TEST(Trivia, iovec) {
  ASSERT_EQ(sizeof(struct iovec), sizeof(fptu_ro));

  fptu_ro serialized;
  serialized.sys.iov_len = 42;
  serialized.sys.iov_base = &serialized;

  ASSERT_EQ(&serialized.total_bytes, &serialized.sys.iov_len);
  ASSERT_EQ(sizeof(serialized.total_bytes), sizeof(serialized.sys.iov_len));
  ASSERT_EQ(serialized.total_bytes, serialized.sys.iov_len);

  ASSERT_EQ((void *)&serialized.units, &serialized.sys.iov_base);
  ASSERT_EQ(sizeof(serialized.units), sizeof(serialized.sys.iov_base));
  ASSERT_EQ(serialized.units, serialized.sys.iov_base);
}

//----------------------------------------------------------------------------

TEST(Trivia, time_ns2fractional) {
  const double scale = exp2(32) / 1e9;
  for (int base_2log = 0; base_2log < 32; ++base_2log) {
    for (int offset_42 = -42; offset_42 <= 42; ++offset_42) {
      SCOPED_TRACE("base_2log " + std::to_string(base_2log) + ", offset_42 " +
                   std::to_string(offset_42));
      const uint64_t ns = (uint64_t(1) << base_2log) + offset_42;
      if (ns >= 1000000000)
        continue;
      SCOPED_TRACE("ns " + std::to_string(ns) + ", factional " +
                   std::to_string(ns * scale));
      const uint64_t probe = floor(ns * scale);
      ASSERT_EQ(probe, fptu_time::ns2fractional(ns));
    }
  }
}

TEST(Trivia, time_fractional2ns) {
  const double scale = 1e9 / exp2(32);
  for (int base_2log = 0; base_2log < 32; ++base_2log) {
    for (int offset_42 = -42; offset_42 <= 42; ++offset_42) {
      SCOPED_TRACE("base_2log " + std::to_string(base_2log) + ", offset_42 " +
                   std::to_string(offset_42));
      const uint64_t fractional =
          uint32_t((uint64_t(1) << base_2log) + offset_42);
      SCOPED_TRACE("fractional " + std::to_string(fractional) + ", ns " +
                   std::to_string(fractional * scale));
      const uint64_t probe = floor(fractional * scale);
      ASSERT_EQ(probe, fptu_time::fractional2ns(fractional));
    }
  }
}

TEST(Trivia, time_us2fractional) {
  const double scale = exp2(32) / 1e6;
  for (int base_2log = 0; base_2log < 32; ++base_2log) {
    for (int offset_42 = -42; offset_42 <= 42; ++offset_42) {
      SCOPED_TRACE("base_2log " + std::to_string(base_2log) + ", offset_42 " +
                   std::to_string(offset_42));
      const uint64_t us = (uint64_t(1) << base_2log) + offset_42;
      if (us >= 1000000)
        continue;
      SCOPED_TRACE("us " + std::to_string(us) + ", factional " +
                   std::to_string(us * scale));
      const uint64_t probe = floor(us * scale);
      ASSERT_EQ(probe, fptu_time::us2fractional(us));
    }
  }
}

TEST(Trivia, time_fractional2us) {
  const double scale = 1e6 / exp2(32);
  for (int base_2log = 0; base_2log < 32; ++base_2log) {
    for (int offset_42 = -42; offset_42 <= 42; ++offset_42) {
      SCOPED_TRACE("base_2log " + std::to_string(base_2log) + ", offset_42 " +
                   std::to_string(offset_42));
      const uint64_t fractional =
          uint32_t((uint64_t(1) << base_2log) + offset_42);
      SCOPED_TRACE("fractional " + std::to_string(fractional) + ", us " +
                   std::to_string(fractional * scale));
      const uint64_t probe = floor(fractional * scale);
      ASSERT_EQ(probe, fptu_time::fractional2us(fractional));
    }
  }
}

TEST(Trivia, time_ms2fractional) {
  const double scale = exp2(32) / 1e3;
  for (int base_2log = 0; base_2log < 32; ++base_2log) {
    for (int offset_42 = -42; offset_42 <= 42; ++offset_42) {
      SCOPED_TRACE("base_2log " + std::to_string(base_2log) + ", offset_42 " +
                   std::to_string(offset_42));
      const uint64_t ms = (uint64_t(1) << base_2log) + offset_42;
      if (ms >= 1000)
        continue;
      SCOPED_TRACE("ms " + std::to_string(ms) + ", factional " +
                   std::to_string(ms * scale));
      const uint64_t probe = floor(ms * scale);
      ASSERT_EQ(probe, fptu_time::ms2fractional(ms));
    }
  }
}

TEST(Trivia, time_fractional2ms) {
  const double scale = 1e3 / exp2(32);
  for (int base_2log = 0; base_2log < 32; ++base_2log) {
    for (int offset_42 = -42; offset_42 <= 42; ++offset_42) {
      SCOPED_TRACE("base_2log " + std::to_string(base_2log) + ", offset_42 " +
                   std::to_string(offset_42));
      const uint64_t fractional =
          uint32_t((uint64_t(1) << base_2log) + offset_42);
      SCOPED_TRACE("fractional " + std::to_string(fractional) + ", ms " +
                   std::to_string(fractional * scale));
      const uint64_t probe = floor(fractional * scale);
      ASSERT_EQ(probe, fptu_time::fractional2ms(fractional));
    }
  }
}

TEST(Trivia, time_coarse) {
  auto prev = fptu_now_coarse();
  for (auto n = 0; n < 42; ++n) {
    auto now = fptu_now_coarse();
    ASSERT_GE(now.fixedpoint, prev.fixedpoint);
    prev = now;
    usleep(137);
  }
}

TEST(Trivia, time_fine) {
  auto prev = fptu_now_fine();
  for (auto n = 0; n < 42; ++n) {
    auto now = fptu_now_fine();
    ASSERT_GE(now.fixedpoint, prev.fixedpoint);
    prev = now;
    usleep(137);
  }
}

TEST(Trivia, time_coarse_vs_fine) {
  for (auto n = 0; n < 42; ++n) {
    auto coarse = fptu_now_coarse();
    auto fine = fptu_now_fine();
    ASSERT_GE(fine.fixedpoint, coarse.fixedpoint);
    ASSERT_GT(ms100, fine.fixedpoint - coarse.fixedpoint);
    usleep(137);
  }
}

namespace std {
template <typename T> std::string to_hex(const T &v) {
  std::stringstream stream;
  stream << std::setfill('0') << std::setw(sizeof(T) * 2) << std::hex << v;
  return stream.str();
}
} // namespace std

TEST(Trivia, time_grain) {
  for (int grain = -32; grain < 0; ++grain) {
    SCOPED_TRACE("grain " + std::to_string(grain));

    auto prev = fptu_now(grain);
    for (auto n = 0; n < 42; ++n) {
      auto grained = fptu_now(grain);
      ASSERT_GE(grained.fixedpoint, prev.fixedpoint);
      prev = grained;
      auto fine = fptu_now_fine();
      SCOPED_TRACE("grained.hex " + std::to_hex(grained.fractional) +
                   ", fine.hex " + std::to_hex(fine.fractional));
      ASSERT_GE(fine.fixedpoint, grained.fixedpoint);
      for (int bit = 0; - bit > grain; ++bit) {
        SCOPED_TRACE("bit " + std::to_string(bit));
        EXPECT_EQ(0u, grained.fractional & (1 << bit));
      }
      usleep(37);
    }
  }
}

//----------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
