/*
 *  Copyright (c) 1994-2021 Leonid Yuriev <leo@yuriev.ru>.
 *  https://github.com/erthink/erthink
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

#define ERTHINK_USE_NATIVE_128 0

#include "testing.h++"

#include "erthink.h++"
#include <algorithm> // for std::random_shuffle
#include <numeric>   // for std::iota

#ifdef ERTHINK_NATIVE_U128_TYPE
using native_u128 = ERTHINK_NATIVE_U128_TYPE;

struct uint64_lcg {
  using result_type = uint64_t;
  result_type state;

  constexpr uint64_lcg(uint64_t seed) noexcept : state(seed) {}
  static constexpr result_type min() noexcept { return 0; }
  static constexpr result_type max() noexcept { return ~result_type(0); }
  cxx14_constexpr result_type operator()() {
    const result_type r = (state += UINT64_C(1442695040888963407));
    state *= UINT64_C(6364136223846793005);
    return r;
  }
  cxx14_constexpr result_type operator()(size_t range) {
    return operator()() % range;
  }
};

static uint64_lcg lcg(uint64_t(time(0)));

static std::array<unsigned, 128> random_shuffle_0_127() noexcept {
  std::array<unsigned, 128> r;
  std::iota(r.begin(), r.end(), 0);
  std::shuffle(r.begin(), r.end(), lcg);
  return r;
}

static uint64_t N;

static void probe(const erthink::uint128_t &a, const erthink::uint128_t &b) {
  ++N;
  ASSERT_EQ((a > b), (native_u128(a) > native_u128(b)));
  ASSERT_EQ((a >= b), (native_u128(a) >= native_u128(b)));
  ASSERT_EQ((a == b), (native_u128(a) == native_u128(b)));
  ASSERT_EQ((a != b), (native_u128(a) != native_u128(b)));
  ASSERT_EQ((a < b), (native_u128(a) < native_u128(b)));
  ASSERT_EQ((a <= b), (native_u128(a) <= native_u128(b)));

  ASSERT_EQ(native_u128(a + b), native_u128(a) + native_u128(b));
  ASSERT_EQ(native_u128(a - b), native_u128(a) - native_u128(b));
  ASSERT_EQ(native_u128(a ^ b), native_u128(a) ^ native_u128(b));
  ASSERT_EQ(native_u128(a | b), native_u128(a) | native_u128(b));
  ASSERT_EQ(native_u128(a & b), native_u128(a) & native_u128(b));
  ASSERT_EQ(native_u128(a * b), native_u128(a) * native_u128(b));

  ASSERT_EQ(native_u128(-a), -native_u128(a));
  ASSERT_EQ(native_u128(~a), ~native_u128(a));
  ASSERT_EQ(!native_u128(!a), !!native_u128(a));

  if (b) {
    const auto pair = erthink::uint128_t::divmod(a, b);
    const auto q = native_u128(a) / native_u128(b);
    const auto r = native_u128(a) % native_u128(b);
    ASSERT_EQ(native_u128(pair.first), q);
    ASSERT_EQ(native_u128(pair.second), r);
  }

  const auto s = unsigned(b) & 127;
  ASSERT_EQ(native_u128(a >> s), native_u128(a) >> s);
  ASSERT_EQ(native_u128(a << s), native_u128(a) << s);
}

static void probe_full(const erthink::uint128_t &a,
                       const erthink::uint128_t &b) {
  ASSERT_NE(native_u128(a), native_u128(a) + 1);
  ASSERT_NE(native_u128(b), native_u128(b) - 1);
  probe(a, b);

  auto t = a;
  ASSERT_EQ(native_u128(t += b), native_u128(a) + native_u128(b));
  t = a;
  ASSERT_EQ(native_u128(t -= b), native_u128(a) - native_u128(b));
  t = a;
  ASSERT_EQ(native_u128(t ^= b), native_u128(a) ^ native_u128(b));
  t = a;
  ASSERT_EQ(native_u128(t |= b), native_u128(a) | native_u128(b));
  t = a;
  ASSERT_EQ(native_u128(t &= b), native_u128(a) & native_u128(b));
  t = a;
  ASSERT_EQ(native_u128(t *= b), native_u128(a) * native_u128(b));

  if (b) {
    t = a;
    ASSERT_EQ(native_u128(t /= b), native_u128(a) / native_u128(b));
    t = a;
    ASSERT_EQ(native_u128(t %= b), native_u128(a) % native_u128(b));
  }

  const auto s = unsigned(b) & 127;
  t = a;
  ASSERT_EQ(native_u128(t >>= s), native_u128(a) >> s);
  t = a;
  ASSERT_EQ(native_u128(t <<= s), native_u128(a) << s);

  ASSERT_EQ(native_u128(ror(a, s)), erthink::ror(native_u128(a), s));
  ASSERT_EQ(native_u128(rol(a, s)), erthink::rol(native_u128(a), s));

  t = a;
  ASSERT_EQ(native_u128(t++), native_u128(a));
  ASSERT_EQ(native_u128(t), native_u128(a) + 1);
  t = a;
  ASSERT_EQ(native_u128(t--), native_u128(a));
  ASSERT_EQ(native_u128(t), native_u128(a) - 1);
  t = a;
  ASSERT_EQ(native_u128(++t), native_u128(a) + 1);
  ASSERT_EQ(native_u128(t), native_u128(a) + 1);
  t = a;
  ASSERT_EQ(native_u128(--t), native_u128(a) - 1);
  ASSERT_EQ(native_u128(t), native_u128(a) - 1);
}

#endif /* ERTHINK_NATIVE_U128_TYPE */

//------------------------------------------------------------------------------

TEST(u128, trivia) {
#ifndef ERTHINK_NATIVE_U128_TYPE
  GTEST_SKIP();
#else
  probe_full(0, 0);
  probe_full(~native_u128(0), ~native_u128(0));
  probe_full(~native_u128(0), 11);
  probe_full(7, ~native_u128(0));
  probe_full(1, 0);
  probe_full(0, -2);
  probe_full(3, 42);
  probe_full(~0, 421);
  probe_full(~42, 5);
  probe_full(~421, INT_MAX);
  probe_full(UINT64_C(13632396072180810313), UINT64_C(4895412794877399892));
  probe_full(UINT64_C(5008002785836588600), UINT64_C(6364136223846793005));

  double a = DBL_MAX, b = DBL_MAX;
  while (a + b > 1) {
    a /= 1.1283791670955125739 /* 2/sqrt(pi) */;
    probe_full(native_u128(std::fmod(a, std::ldexp(1.0, 128))),
               native_u128(std::fmod(b, std::ldexp(1.0, 128))));
    probe_full(native_u128(std::fmod(b, std::ldexp(1.0, 128))),
               native_u128(std::fmod(a, std::ldexp(1.0, 128))));
    b *= 0.91893853320467274178 /* ln(sqrt(2pi)) */;
    probe_full(native_u128(std::fmod(a, std::ldexp(1.0, 128))),
               native_u128(std::fmod(b, std::ldexp(1.0, 128))));
    probe_full(native_u128(std::fmod(b, std::ldexp(1.0, 128))),
               native_u128(std::fmod(a, std::ldexp(1.0, 128))));
  }
#endif /* ERTHINK_NATIVE_U128_TYPE */
}

TEST(u128, stairwell) {
#ifndef ERTHINK_NATIVE_U128_TYPE
  GTEST_SKIP();
#else
  SCOPED_TRACE("PRNG seed=" + std::to_string(lcg.state));
  const auto outer = random_shuffle_0_127();
  const auto inner = random_shuffle_0_127();
  // Total 1`065`418`752 probe() calls
  for (const auto i : outer) {
    const auto base_a = (~native_u128(0)) >> i;
    for (const auto j : inner) {
      const auto base_b = (~native_u128(0)) >> j;
      for (auto offset_a = base_a; offset_a >>= 1;) {
        for (auto offset_b = base_b; offset_b >>= 1;) {
          probe(base_a + offset_a, base_b + offset_b);
          probe(base_a + offset_a, base_b - offset_b);
          probe(base_a - offset_a, base_b + offset_b);
          probe(base_a - offset_a, base_b - offset_b);

          probe(base_a + offset_a, ~base_b + offset_b);
          probe(base_a + offset_a, ~base_b - offset_b);
          probe(base_a - offset_a, ~base_b + offset_b);
          probe(base_a - offset_a, ~base_b - offset_b);

          probe(~base_a + offset_a, base_b + offset_b);
          probe(~base_a + offset_a, base_b - offset_b);
          probe(~base_a - offset_a, base_b + offset_b);
          probe(~base_a - offset_a, base_b - offset_b);

          probe(~base_a + offset_a, ~base_b + offset_b);
          probe(~base_a + offset_a, ~base_b - offset_b);
          probe(~base_a - offset_a, ~base_b + offset_b);
          probe(~base_a - offset_a, ~base_b - offset_b);
        }
        probe(base_a + offset_a, base_b);
        probe(base_a - offset_a, base_b);
        probe(base_a + offset_a, ~base_b);
        probe(base_a - offset_a, ~base_b);
        probe(~base_a + offset_a, base_b);
        probe(~base_a - offset_a, base_b);
        probe(~base_a + offset_a, ~base_b);
        probe(~base_a - offset_a, ~base_b);
      }
      probe(base_a, base_b);
      probe(base_a, ~base_b);
      probe(~base_a, base_b);
      probe(~base_a, ~base_b);
      if (GTEST_IS_EXECUTION_TIMEOUT())
        break;
    }
  }
#endif /* ERTHINK_NATIVE_U128_TYPE */
}

//------------------------------------------------------------------------------

TEST(u128, random3e7) {
#ifndef ERTHINK_NATIVE_U128_TYPE
  GTEST_SKIP();
#else
  SCOPED_TRACE("PRNG seed=" + std::to_string(lcg.state));
  for (auto i = 0; i < 333333; ++i) {
    probe_full(lcg(), lcg());
    probe_full({lcg(), lcg()}, lcg());
    probe_full(lcg(), {lcg(), lcg()});
    probe_full({lcg(), lcg()}, {lcg(), lcg()});

    probe_full({lcg(), 0}, {lcg(), lcg()});
    probe_full({lcg(), lcg()}, {lcg(), 0});
    probe_full({lcg(), 0}, {lcg(), 0});

    probe_full({lcg(), 0}, lcg());
    probe_full(lcg(), {lcg(), 0});

    if (GTEST_IS_EXECUTION_TIMEOUT())
      break;
  }
#endif /* ERTHINK_NATIVE_U128_TYPE */
}

//------------------------------------------------------------------------------

runtime_limiter ci_runtime_limiter;

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
