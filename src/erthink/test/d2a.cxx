/*
 *  Copyright (c) 1994-2018 Leonid Yuriev <leo@yuriev.ru>.
 *  https://github.com/leo-yuriev/erthink
 *  ZLib License
 *
 *  This software is provided 'as-is', without any express or implied
 *  warranty. In no event will the authors be held liable for any damages
 *  arising from the use of this software.
 *
 *  Permission is granted to anyone to use this software for any purpose,
 *  including commercial applications, and to alter it and redistribute it
 *  freely, subject to the following restrictions:
 *
 *  1. The origin of this software must not be misrepresented; you must not
 *     claim that you wrote the original software. If you use this software
 *     in a product, an acknowledgement in the product documentation would be
 *     appreciated but is not required.
 *  2. Altered source versions must be plainly marked as such, and must not be
 *     misrepresented as being the original software.
 *  3. This notice may not be removed or altered from any source distribution.
 */

#ifdef _MSC_VER
#define _USE_MATH_DEFINES
#endif

#include "testing.h"

#include "erthink_d2a.h"
#include "erthink_defs.h"

#ifdef _MSC_VER
#pragma warning(push, 1)
#pragma warning(disable : 4710) /* function not inlined */
#endif
#include <cfloat> // for FLT_MAX, etc
#include <cmath>  // for M_PI, etc
#ifdef _MSC_VER
#pragma warning(pop)
#endif

__hot __dll_export __noinline char *_d2a(const double value, char *ptr) {
  return erthink::d2a(value, ptr);
}

//----------------------------------------------------------------------------

static void probe_d2a(char (&buffer)[23 + 1], const double value) {
  char *d2a_end = _d2a(value, buffer);
  ASSERT_LT(buffer, d2a_end);
  ASSERT_GT(erthink::array_end(buffer), d2a_end);
  *d2a_end = '\0';

  char *strtod_end = nullptr;
  double probe = strtod(buffer, &strtod_end);
  EXPECT_EQ(d2a_end, strtod_end);
  EXPECT_EQ(value, probe);
}

TEST(d2a, trivia) {
  char buffer[23 + 1];
  char *end = _d2a(0, buffer);
  EXPECT_EQ(1, end - buffer);
  EXPECT_EQ(buffer[0], '0');

  probe_d2a(buffer, 0.0);
  probe_d2a(buffer, 1.0);
  probe_d2a(buffer, 2.0);
  probe_d2a(buffer, 3.0);
  probe_d2a(buffer, -0.0);
  probe_d2a(buffer, -1.0);
  probe_d2a(buffer, -2.0);
  probe_d2a(buffer, -3.0);
  probe_d2a(buffer, M_PI);
  probe_d2a(buffer, -M_PI);

  probe_d2a(buffer, INT32_MIN);
  probe_d2a(buffer, INT32_MAX);
  probe_d2a(buffer, UINT16_MAX);
  probe_d2a(buffer, UINT32_MAX);
  probe_d2a(buffer, FLT_MAX);
  probe_d2a(buffer, -FLT_MAX);
  probe_d2a(buffer, FLT_MAX);
  probe_d2a(buffer, -FLT_MAX);
  probe_d2a(buffer, FLT_MIN);
  probe_d2a(buffer, -FLT_MIN);
  probe_d2a(buffer, FLT_MAX * M_PI);
  probe_d2a(buffer, -FLT_MAX * M_PI);
  probe_d2a(buffer, FLT_MIN * M_PI);
  probe_d2a(buffer, -FLT_MIN * M_PI);

  probe_d2a(buffer, DBL_MAX);
  probe_d2a(buffer, -DBL_MAX);
  probe_d2a(buffer, DBL_MIN);
  probe_d2a(buffer, -DBL_MIN);
  probe_d2a(buffer, DBL_MAX / M_PI);
  probe_d2a(buffer, -DBL_MAX / M_PI);
  probe_d2a(buffer, DBL_MIN * M_PI);
  probe_d2a(buffer, -DBL_MIN * M_PI);
}

TEST(d2a, stairwell) {
  char buffer[23 + 1];
  const double up = 1.1283791670955125739 /* 2/sqrt(pi) */;
  for (double value = DBL_MIN * up; value < DBL_MAX / up; value *= up) {
    probe_d2a(buffer, value);
    const float f32 = static_cast<float>(value);
    if (!std::isinf(f32))
      probe_d2a(buffer, f32);
  }

  const double down = 0.91893853320467274178 /* ln(sqrt(2pi)) */;
  for (double value = DBL_MAX * down; value > DBL_MIN / down; value *= down) {
    probe_d2a(buffer, value);
    const float f32 = static_cast<float>(value);
    if (!std::isinf(f32))
      probe_d2a(buffer, f32);
  }
}

TEST(d2a, random3e6) {
  char buffer[23 + 1];
  erthink::grisu::casting_union prng(uint64_t(time(0)));
  SCOPED_TRACE("PGNG seed" + std::to_string(prng.u));
  for (int i = 0; i < 3000000;) {
    switch (std::fpclassify(prng.f)) {
    case FP_NAN:
    case FP_INFINITE:
      break;
    default:
      probe_d2a(buffer, prng.f);
      const float f32 = static_cast<float>(prng.f);
      if (!std::isinf(f32)) {
        probe_d2a(buffer, f32);
        ++i;
      }
    }
    prng.u *= UINT64_C(6364136223846793005);
    prng.u += UINT64_C(1442695040888963407);
  }
}

//----------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
