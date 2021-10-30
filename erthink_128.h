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

#pragma once

#include "erthink_arch.h"
#include "erthink_byteorder.h"
#include "erthink_defs.h"
#include "erthink_intrin.h"
#include "erthink_mul.h"
#include "erthink_rot.h"

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4201) /* nonstandard extension used :                \
                                   nameless struct / union */
#endif                          /* _MSC_VER (warnings) */

#ifdef __cplusplus
namespace erthink {
#endif /* __cplusplus */

#ifndef ERTHINK_USE_NATIVE_U128
#if defined(__SIZEOF_INT128__) ||                                              \
    (defined(_INTEGRAL_MAX_BITS) && _INTEGRAL_MAX_BITS >= 128)
#define ERTHINK_USE_NATIVE_U128 1
#else
#define ERTHINK_USE_NATIVE_U128 0
#endif
#endif /* ERTHINK_USE_NATIVE_U128 */

typedef union uint128_t {
  uint32_t u64[2];
  uint32_t u32[4];
  uint16_t u16[8];
  uint8_t u8[16];
#ifdef ERTHINK_NATIVE_U128_TYPE
  ERTHINK_NATIVE_U128_TYPE u128;
#endif
  struct {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    uint64_t l, h;
#else
    uint64_t h, l;
#endif
  };
} uint128_t;

#ifndef __cplusplus
typedef union uint128_t uint128_t;
#endif

static __always_inline uint128_t not128(const uint128_t v) {
  uint128_t r;
#if ERTHINK_USE_NATIVE_U128
  r.u128 = ~v.u128;
#else
  r.l = ~v.l;
  r.h = ~v.h;
#endif
  return r;
}

static __always_inline uint128_t left128(const uint128_t v, unsigned s) {
  uint128_t r;
  assert(s < 128);
#if ERTHINK_USE_NATIVE_U128
  r.u128 = v.u128 << s;
#else
  r.l = (s < 64) ? v.l << s : 0;
  r.h = (s < 64) ? (v.h << s) | (s ? v.l >> (64 - s) : 0) : v.l << (s - 64);
#endif
  return r;
}

static __always_inline uint128_t right128(const uint128_t v, unsigned s) {
  uint128_t r;
  assert(s < 128);
#if ERTHINK_USE_NATIVE_U128
  r.u128 = v.u128 >> s;
#else
  r.l = (s < 64) ? (s ? v.h << (64 - s) : 0) | (v.l >> s) : v.h >> (s - 64);
  r.h = (s < 64) ? v.h >> s : 0;
#endif
  return r;
}

static __always_inline uint128_t or128(uint128_t x, uint128_t y) {
  uint128_t r;
#if ERTHINK_USE_NATIVE_U128
  r.u128 = x.u128 | y.u128;
#else
  r.l = x.l | y.l;
  r.h = x.h | y.h;
#endif
  return r;
}

static __always_inline uint128_t xor128(uint128_t x, uint128_t y) {
  uint128_t r;
#if ERTHINK_USE_NATIVE_U128
  r.u128 = x.u128 ^ y.u128;
#else
  r.l = x.l ^ y.l;
  r.h = x.h ^ y.h;
#endif
  return r;
}

static __always_inline uint128_t ror128(uint128_t v, unsigned s) {
  s &= 127;
#if ERTHINK_USE_NATIVE_U128
  v.u128 = (v.u128 << (128 - s)) | (v.u128 >> s);
  return v;
#else
  return s ? or128(left128(v, 128 - s), right128(v, s)) : v;
#endif
}

static __always_inline uint128_t rol128(uint128_t v, unsigned s) {
  return ror128(v, 128 - s);
}

static __always_inline uint128_t add128(uint128_t x, uint128_t y) {
  uint128_t r;
#if ERTHINK_USE_NATIVE_U128
  r.u128 = x.u128 + y.u128;
#else
  add64carry_last(add64carry_first(x.l, y.l, &r.l), x.h, y.h, &r.h);
#endif
  return r;
}

static __always_inline uint128_t mul128(uint128_t x, uint128_t y) {
  uint128_t r;
#if ERTHINK_USE_NATIVE_U128
  r.u128 = x.u128 * y.u128;
#else
  r.l = mul_64x64_128(x.l, y.l, &r.h);
  r.h += x.l * y.h + y.l * x.h;
#endif
  return r;
}

#ifdef __cplusplus
} // namespace erthink
#endif

#ifdef _MSC_VER
#pragma warning(pop)
#endif
