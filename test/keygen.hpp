﻿/*
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

#pragma once
#include "fpta_test.h"
#include "tools.hpp"

/* "хорошие" значения float: близкие к представимым, но НЕ превосходящие их */
static cxx11_constexpr_var float flt_neg_below =
    (float)(-FLT_MAX + (double)FLT_MAX * FLT_EPSILON);
static cxx11_constexpr_var float flt_pos_below =
    (float)(FLT_MAX - (double)FLT_MAX * FLT_EPSILON);
static_assert(flt_neg_below > -FLT_MAX, "unexpected precision loss");
static_assert(flt_pos_below < FLT_MAX, "unexpected precision loss");

/* "плохие" значения float: немного вне представимого диапазона */
static cxx11_constexpr_var double flt_neg_over =
    -FLT_MAX - (double)FLT_MAX * FLT_EPSILON;
static cxx11_constexpr_var double flt_pos_over =
    FLT_MAX + (double)FLT_MAX * FLT_EPSILON;
static_assert(flt_neg_over <= -FLT_MAX, "unexpected precision loss");
static_assert(flt_pos_over >= FLT_MAX, "unexpected precision loss");

//----------------------------------------------------------------------------

template <typename container>
bool is_properly_ordered(const container &probe, bool descending = false) {
  if (!descending) {
    return std::is_sorted(probe.begin(), probe.end(),
                          [](const typename container::value_type &left,
                             const typename container::value_type &right) {
                            EXPECT_GT(left.second, right.second);
                            return left.second < right.second;
                          });
  } else {
    return std::is_sorted(probe.begin(), probe.end(),
                          [](const typename container::value_type &left,
                             const typename container::value_type &right) {
                            EXPECT_LT(left.second, right.second);
                            return left.second > right.second;
                          });
  }
}

inline fpta_value order_checksum(int order, fptu_type type,
                                 fpta_index_type index) {
  auto signature = fpta_column_shove(0, type, index);
  return fpta_value_uint(
      t1ha2_atonce(&signature, sizeof(signature), (uint64_t)order));
}

template <fptu_type data_type, fpta_index_type index_type> struct probe_key {
  fpta_key key;

  static fpta_shove_t shove() {
    return fpta_column_shove(0, data_type, index_type);
  }

  probe_key(const fpta_value &value) {
    fpta_pollute(&key, sizeof(key), 0);
    EXPECT_EQ(FPTA_OK, value2key(shove(), value, key));
  }

  const probe_key &operator=(const probe_key &) = delete;
  probe_key(const probe_key &ones) = delete;
  const probe_key &operator=(const probe_key &&) = delete;
  probe_key(const probe_key &&ones) = delete;

  int compare(const probe_key &right) const {
    auto comparator = shove2comparator(shove());
    return comparator(&key.mdbx, &right.key.mdbx);
  }

  bool operator==(const probe_key &right) const { return compare(right) == 0; }

  bool operator!=(const probe_key &right) const { return compare(right) != 0; }

  bool operator<(const probe_key &right) const { return compare(right) < 0; }

  bool operator>(const probe_key &right) const { return compare(right) > 0; }
};

template <fptu_type data_type> struct probe_triplet {
  typedef probe_key<data_type, fpta_primary_unique_ordered_obverse> obverse_key;
  typedef std::map<obverse_key, int> obverse_map;

  typedef probe_key<data_type, fpta_primary_unique_unordered> unordered_key;
  typedef std::map<unordered_key, int> unordered_map;

  typedef probe_key<data_type, fpta_primary_unique_ordered_reverse> reverse_key;
  typedef std::map<reverse_key, int> reverse_map;

  obverse_map obverse;
  unordered_map unordered;
  reverse_map reverse;
  int n;

  probe_triplet() : n(0) {}

  static bool has_reverse() { return data_type >= fptu_96; }

  void operator()(const fpta_value &key, int order, bool duplicate = false) {
    if (!duplicate)
      ++n;
    EXPECT_EQ(!duplicate,
              obverse
                  .emplace(std::piecewise_construct, std::forward_as_tuple(key),
                           std::forward_as_tuple(order))
                  .second);

    EXPECT_EQ(!duplicate,
              unordered
                  .emplace(std::piecewise_construct, std::forward_as_tuple(key),
                           std::forward_as_tuple(order))
                  .second);

    // повторяем для проверки сравнения (эти вставки не должны произойти)
    EXPECT_FALSE(obverse
                     .emplace(std::piecewise_construct,
                              std::forward_as_tuple(key),
                              std::forward_as_tuple(INT_MIN))
                     .second);
    EXPECT_FALSE(unordered
                     .emplace(std::piecewise_construct,
                              std::forward_as_tuple(key),
                              std::forward_as_tuple(INT_MIN))
                     .second);

    if (has_reverse()) {
      assert(key.type == fpta_binary || key.type == fpta_string);
      uint8_t *begin = (uint8_t *)key.binary_data;
      uint8_t *end = begin + key.binary_length;
      std::reverse(begin, end);
      EXPECT_EQ(!duplicate, reverse
                                .emplace(std::piecewise_construct,
                                         std::forward_as_tuple(key),
                                         std::forward_as_tuple(order))
                                .second);

      // повторяем для проверки сравнения (эта вставка не должна произойти)
      EXPECT_FALSE(reverse
                       .emplace(std::piecewise_construct,
                                std::forward_as_tuple(key),
                                std::forward_as_tuple(INT_MIN))
                       .second);
    }
  }

  void check(int expected) {
    EXPECT_EQ(expected, n);

    // паранойя на случай повреждения ключей
    ASSERT_TRUE(std::is_sorted(obverse.begin(), obverse.end()));
    ASSERT_TRUE(std::is_sorted(reverse.begin(), reverse.end()));

    // наборы должны содержать все значения
    EXPECT_EQ(expected, (int)unordered.size());
    EXPECT_EQ(expected, (int)obverse.size());
    if (has_reverse()) {
      EXPECT_EQ(expected, (int)reverse.size());
    }

    // а ordered должны быть также упорядочены
    EXPECT_TRUE(is_properly_ordered(obverse));
    EXPECT_TRUE(is_properly_ordered(reverse));
  }

  void check() { check(n); }
};

//----------------------------------------------------------------------------

template <bool printable>
/* Генератор для получения ключей в виде байтовых строк различной длины,
 * которые при этом упорядоченные по параметру order при сравнении
 * посредством memcmp().
 *
 * Для этого старшие биты первого символа кодирует исходную "ширину" order,
 * а младшие биты и последующие символы - значение order в порядке от старших
 * разрядов к младшим. */
bool string_keygen(const size_t len, uint32_t order, uint8_t *buf,
                   uint32_t tailseed = 0) {
  /* параметры алфавита */
  cxx11_constexpr_var int alphabet_bits = printable ? 6 : 8;
  cxx11_constexpr_var unsigned alphabet_mask = (1 << alphabet_bits) - 1;
  cxx11_constexpr_var unsigned alphabet_base = printable ? '0' : 0;

  /* кол-во бит под кодирование "ширины" */
  cxx11_constexpr_var unsigned rle_bits = 5;
  /* максимальная "ширина" order */
  cxx11_constexpr_var int max_bits = 1 << rle_bits;
  /* максимальнное значение order */
  cxx11_constexpr_var uint64_t max_order = (1ull << max_bits) - 1;
  /* остаток битов в первом символе после "ширины" */
  cxx11_constexpr_var int first_left = alphabet_bits - rle_bits;
  cxx11_constexpr_var unsigned first_mask = (1 << first_left) - 1;
  assert(len > 0);
  assert(order <= max_order);
  (void)max_order;

  /* считаем ширину order */
  int width = 0;
  while (order >> width) {
    ++width;
    assert(width <= max_bits);
  }

  /* кодируем длину */
  uint8_t rle_val = (uint8_t)((width ? width - 1 : 0) << first_left);
  /* вычисляем сколько битов значения остается для остальных символов */
  int left = (width > first_left) ? width - first_left : 0;
  /* первый символ с длиной и самыми старшими разрядами значения */
  *buf = alphabet_base + rle_val + ((order >> left) & first_mask);

  for (auto end = buf + len; ++buf < end;) {
    if (left > 0) {
      left = (left > alphabet_bits) ? left - alphabet_bits : 0;
      *buf = alphabet_base + ((order >> left) & alphabet_mask);
    } else {
      /* дополняем но нужной длины псевдослучайными данными */
      tailseed = tailseed * 1664525u + 1013904223u;
      *buf = alphabet_base + ((tailseed >> 23) & alphabet_mask);
    }
  }

  return left > 0;
}

template <bool printable>
/* Тест описанного выше генератора. */
void string_keygen_test(const size_t keylen_min, const size_t keylen_max) {
  assert(keylen_min > 0);
  assert(keylen_max >= keylen_min);

  SCOPED_TRACE(std::string("string_keygen_test: ") +
               (printable ? "string" : "binary") + ", keylen " +
               std::to_string(keylen_min) + "..." + std::to_string(keylen_max));

  const size_t bufsize = keylen_max + 1 + printable;
#ifdef _MSC_VER /* FIXME: mustdie */
  uint8_t *const buffer_a = (uint8_t *)_alloca(bufsize);
  memset(buffer_a, 0xAA, bufsize);
#else
  uint8_t buffer_a[bufsize];
  memset(buffer_a, 0xAA, sizeof(buffer_a));
#endif
  uint8_t *prev = buffer_a;

#ifdef _MSC_VER /* FIXME: mustdie */
  uint8_t *const buffer_b = (uint8_t *)_alloca(bufsize);
  memset(buffer_b, 0xBB, bufsize);
#else
  uint8_t buffer_b[bufsize];
  memset(buffer_b, 0xBB, sizeof(buffer_b));
#endif
  uint8_t *next = buffer_b;

  size_t keylen = keylen_min;
  EXPECT_FALSE((string_keygen<printable>(keylen, 0, prev)));

#ifdef _MSC_VER /* FIXME: mustdie */
  uint8_t *const buffer_c = (uint8_t *)_alloca(bufsize);
  memset(buffer_c, 0xCC, bufsize);
#else
  uint8_t buffer_c[bufsize];
  memset(buffer_c, 0xCC, sizeof(buffer_c));
#endif

  if (keylen < keylen_max) {
    EXPECT_FALSE((string_keygen<printable>(keylen + 1, 0, buffer_c)));
    ASSERT_LE(0, memcmp(prev, buffer_c, keylen));
  }

  unsigned order = 1;
  while (keylen <= keylen_max && order < INT32_MAX) {
    memset(next, 0, keylen_max);
    bool key_is_too_short = string_keygen<printable>(keylen, order, next);
    if (key_is_too_short) {
      keylen++;
      continue;
    }

    auto memcmp_result = memcmp(prev, next, keylen_max);
    if (memcmp_result >= 0) {
      SCOPED_TRACE("keylen " + std::to_string(keylen) + ", order " +
                   std::to_string(order));
      ASSERT_LT(0, memcmp_result);
    }

    if (keylen < keylen_max) {
      memset(buffer_c, -1, keylen + 1);
      EXPECT_FALSE((string_keygen<printable>(keylen + 1, order, buffer_c)));
      memcmp_result = memcmp(buffer_c, next, keylen);
      if (memcmp_result < 0) {
        SCOPED_TRACE("keylen " + std::to_string(keylen) + ", order " +
                     std::to_string(order));
        ASSERT_GE(0, memcmp_result);
      }
    }

    std::swap(prev, next);
    order += (order & (1024 + 2048 + 4096)) ? (113 + order / 16) : 1;
    if (order >= INT32_MAX / keylen_max * keylen)
      keylen++;
  }
}

//----------------------------------------------------------------------------

template <typename type>
/* Позволяет за N шагов "простучать" весь диапазон значений type,
 * явно включая крайние точки, нуль и бесконечности (при наличии). */
struct scalar_range_stepper {
  typedef std::map<type, int> container4test;

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4723) /* potential divide by 0 */
#pragma warning(disable : 4146) /* unary minus operator applied to unsigned    \
                                   type, result still unsigned */
#endif                          /* _MSC_VER */

#ifdef __LCC__
#pragma GCC diagnostic push
#pragma diag_suppress divide_by_zero
#endif /* __LCC__ */

  /* max integer round-trip convertible to float */
  static constexpr type safe_max =
      static_cast<type>(std::numeric_limits<int32_t>::max() - 127);

  static type value(int order, int const N) {
    assert(N > 0);
    const int scope_neg =
        std::is_signed<type>()
            ? (N - 1) / 2 - std::numeric_limits<type>::has_infinity
            : 0;
    const int scope_pos =
        N - scope_neg - 1 - std::numeric_limits<type>::has_infinity * 2;

    assert(
        (!std::is_signed<type>() || std::numeric_limits<type>::lowest() < 0) &&
        "expected lowest() < 0 for signed types");
    assert(scope_pos > 1 && "seems N is too small");
    static_assert(std::numeric_limits<type>::max() > safe_max, "Oops!");
    assert(safe_max > double(scope_pos) && "seems N is too big");

    if (std::is_signed<type>()) {
      if (std::numeric_limits<type>::has_infinity && order-- == 0)
        return (type)-std::numeric_limits<type>::infinity();
      if (order < scope_neg && scope_neg) {
        type shift = (std::numeric_limits<type>::max() < safe_max)
                         ? (type)(std::numeric_limits<type>::lowest() * order /
                                  scope_neg)
                         : (type)(std::numeric_limits<type>::lowest() /
                                  scope_neg * order);
        /* division by 0 (scope_neg) NOT possible here, but MSVC is stupid... */
        return (type)(std::numeric_limits<type>::lowest() - shift);
      }
      order -= scope_neg;
    }

    if (order == 0)
      return type(0);

    if (std::numeric_limits<type>::has_infinity && order > scope_pos)
      return std::numeric_limits<type>::infinity();
    if (order == scope_pos || scope_pos == 0)
      return std::numeric_limits<type>::max();
    return (std::numeric_limits<type>::max() < safe_max)
               ? (type)(std::numeric_limits<type>::max() * order / scope_pos)
               : (type)(std::numeric_limits<type>::max() / scope_pos * order);
  }

  static void test(int const N) {
    SCOPED_TRACE(std::string("scalar_range_stepper: ") +
                 std::string(::testing::internal::GetTypeName<type>()) +
                 ", N=" + std::to_string(N));

    container4test probe;
    for (int i = 0; i < N; ++i)
      probe[value(i, N)] = i;

    bool is_properly_ordered =
        std::is_sorted(probe.begin(), probe.end(),
                       [](const typename container4test::value_type &left,
                          const typename container4test::value_type &right) {
                         return left.second < right.second;
                       });

    EXPECT_TRUE(is_properly_ordered);
    if (std::numeric_limits<type>::has_infinity) {
      EXPECT_EQ(1u, probe.count((type)-std::numeric_limits<type>::infinity()));
      EXPECT_EQ(1u, probe.count(std::numeric_limits<type>::infinity()));
    }

#ifdef _MSC_VER
#pragma warning(pop)
#endif /* _MSC_VER */

#ifdef __LCC__
#pragma GCC diagnostic pop
#endif /* __LCC__ */

    EXPECT_EQ(N, (int)probe.size());
    EXPECT_EQ(1u, probe.count(type(0)));
    EXPECT_EQ(1u, probe.count(std::numeric_limits<type>::max()));
    EXPECT_EQ(1u, probe.count(std::numeric_limits<type>::lowest()));
  }
};

//----------------------------------------------------------------------------

template <fpta_index_type index, fptu_type type> struct keygen_base {
  static fpta_value invalid(int order) {
    switch (order) {
    case 0:
      return fpta_value_null();
    case 1:
      if (type == fptu_int32 || type == fptu_int64)
        break;
      return fpta_value_sint(-1);
    case 2:
      if (type == fptu_int32 || type == fptu_int64 || type == fptu_uint32 ||
          type == fptu_uint64)
        break;
      return fpta_value_uint(INT16_MAX + 1);
    case 3:
      if (type == fptu_fp32 || type == fptu_fp64)
        break;
      return fpta_value_float(42);
    case 4:
      if (type == fptu_cstr)
        break;
      return fpta_value_cstr("42");
    case 5:
      if (type == fptu_opaque)
        break;
      return fpta_value_binary("42", 2);
    default:
      // возвращаем end как признак окончания итерации по order
      return fpta_value_end();
    }
    // возвращаем begin как признак пропуска текущей итерации по order
    return fpta_value_begin();
  }
};

template <fpta_index_type index, fptu_type type>
struct keygen : public keygen_base<index, type> {
  static fpta_value make(int order, int const N) {
    (void)N;
    SCOPED_TRACE("FIXME: make(), type " + std::to_string(type) + ", index " +
                 std::to_string(index) +
                 ", " __FILE__ ": " FPT_STRINGIFY(__LINE__));
    (void)order;
    ADD_FAILURE();
    return fpta_value_end();
  }
};

//----------------------------------------------------------------------------

template <unsigned keylen> struct fixbin_stepper {
  typedef std::array<uint8_t, keylen> fixbin_type;

  static fpta_value make(int order, bool reverse, int const N) {
    assert(N > 2);
    const int scope = (int)N - 2;
    /* нужен буфер, ибо внутри fpta_value только указатель на данные */
    static fixbin_type holder;

    if (order == 0)
      memset(&holder, 0, keylen);
    else if (order > scope)
      memset(&holder, ~0, keylen);
    else {
      bool key_is_too_short = string_keygen<false>(
          keylen, (unsigned)INT32_MAX / scope * (order - 1), &holder[0]);
      EXPECT_FALSE(key_is_too_short);
    }

    if (reverse)
      std::reverse(holder.begin(), holder.end());

    return fpta_value_binary(&holder, sizeof(holder));
  }

  static void test(int const N) {
    SCOPED_TRACE(std::string("fixbin_stepper: keylen ") +
                 std::to_string(keylen) + ", N=" + std::to_string(N));

    std::map<fixbin_type, int> probe;
    for (int i = 0; i < N; ++i) {
      fixbin_type *value = (fixbin_type *)make(i, false, N).binary_data;
      probe[*value] = i;
    }

    EXPECT_TRUE(is_properly_ordered(probe));
    EXPECT_EQ(N, (int)probe.size());

    fixbin_type value;
    memset(&value, 0, sizeof(value));
    EXPECT_EQ(1u, probe.count(value));

    memset(&value, 0xff, sizeof(value));
    EXPECT_EQ(1u, probe.count(value));

    memset(&value, 0x42, sizeof(value));
    EXPECT_EQ(0u, probe.count(value));
  }
};

//----------------------------------------------------------------------------

template <fptu_type data_type>
fpta_value fpta_value_binstr(const void *pattern, size_t length) {
  return (data_type == fptu_cstr)
             ? fpta_value_string((const char *)pattern, length)
             : fpta_value_binary(pattern, length);
}

template <fptu_type data_type> struct varbin_stepper {
  static cxx11_constexpr_var size_t keylen_max = fpta_max_keylen * 3 / 2;
  typedef std::array<uint8_t, keylen_max> varbin_type;

  static fpta_value make(int order, bool reverse, int const N) {
    assert(N > 2);
    const int scope = (int)N - 2;
    /* нужен буфер, ибо внутри fpta_value только указатель на данные */
    static varbin_type holder;

    if (order == 0)
      return fpta_value_binstr<data_type>(nullptr, 0);

    if (order > scope) {
      memset(&holder, ~0, keylen_max);
      return fpta_value_binstr<data_type>(&holder[0], keylen_max);
    }

    size_t keylen = 1 + ((order - 1) % 37) * (keylen_max - 1) / 37;
    while (keylen <= keylen_max) {
      bool key_is_too_short = string_keygen<data_type == fptu_cstr>(
          keylen, (unsigned)INT32_MAX / scope * (order - 1), &holder[0]);
      if (!key_is_too_short)
        break;
      keylen++;
    }

    EXPECT_TRUE(keylen <= keylen_max);
    if (reverse)
      std::reverse(&holder[0], &holder[keylen]);

    return fpta_value_binstr<data_type>(&holder[0], keylen);
  }

  static void test(int const N) {
    SCOPED_TRACE(std::string("varbin_stepper: ") + std::to_string(data_type) +
                 ", N=" + std::to_string(N));

    std::map<std::vector<uint8_t>, int> probe;
    for (int i = 0; i < N; ++i) {
      auto value = make(i, false, N);
      uint8_t *ptr = (uint8_t *)value.binary_data;
      probe[std::vector<uint8_t>(ptr, ptr + value.binary_length)] = i;
    }

    EXPECT_TRUE(is_properly_ordered(probe));
    EXPECT_EQ(N, (int)probe.size());

    std::vector<uint8_t> value;
    EXPECT_EQ(1u, probe.count(value));

    value.resize(keylen_max, 255);
    EXPECT_EQ(1u, probe.count(value));

    value.clear();
    value.resize(keylen_max / 2, 42);
    EXPECT_EQ(0u, probe.count(value));
  }
};

//----------------------------------------------------------------------------

template <fpta_index_type index>
struct keygen<index, fptu_uint16> : public scalar_range_stepper<uint16_t> {
  static cxx11_constexpr_var fptu_type type = fptu_uint16;
  static fpta_value make(int order, int const N) {
    return fpta_value_uint(scalar_range_stepper<uint16_t>::value(order, N));
  }
};

template <fpta_index_type index>
struct keygen<index, fptu_uint32> : public scalar_range_stepper<uint32_t> {
  static cxx11_constexpr_var fptu_type type = fptu_uint32;
  static fpta_value make(int order, int const N) {
    return fpta_value_uint(scalar_range_stepper<uint32_t>::value(order, N));
  }
};

template <fpta_index_type index>
struct keygen<index, fptu_uint64> : public scalar_range_stepper<uint64_t> {
  static cxx11_constexpr_var fptu_type type = fptu_uint64;
  static fpta_value make(int order, int const N) {
    return fpta_value_uint(scalar_range_stepper<uint64_t>::value(order, N));
  }
};

template <fpta_index_type index>
struct keygen<index, fptu_int32> : public scalar_range_stepper<int32_t> {
  static cxx11_constexpr_var fptu_type type = fptu_int32;
  static fpta_value make(int order, int const N) {
    return fpta_value_sint(scalar_range_stepper<int32_t>::value(order, N));
  }
};

template <fpta_index_type index>
struct keygen<index, fptu_int64> : public scalar_range_stepper<int64_t> {
  static cxx11_constexpr_var fptu_type type = fptu_int64;
  static fpta_value make(int order, int const N) {
    return fpta_value_sint(scalar_range_stepper<int64_t>::value(order, N));
  }
};

template <fpta_index_type index>
struct keygen<index, fptu_fp32> : public scalar_range_stepper<float> {
  static cxx11_constexpr_var fptu_type type = fptu_fp32;
  static fpta_value make(int order, int const N) {
    return fpta_value_float(scalar_range_stepper<float>::value(order, N));
  }
};

template <fpta_index_type index>
struct keygen<index, fptu_fp64> : public scalar_range_stepper<double> {
  static cxx11_constexpr_var fptu_type type = fptu_fp64;
  static fpta_value make(int order, int const N) {
    return fpta_value_float(scalar_range_stepper<double>::value(order, N));
  }
};

template <fpta_index_type index>
struct keygen<index, fptu_96> : public fixbin_stepper<96 / 8>,
                                keygen_base<index, fptu_96> {
  static cxx11_constexpr_var fptu_type type = fptu_96;
  static fpta_value make(int order, int const N) {
    return fixbin_stepper<96 / 8>::make(order, fpta_index_is_reverse(index), N);
  }
};

template <fpta_index_type index>
struct keygen<index, fptu_128> : public fixbin_stepper<128 / 8> {
  static cxx11_constexpr_var fptu_type type = fptu_128;
  static fpta_value make(int order, int const N) {
    return fixbin_stepper<128 / 8>::make(order, fpta_index_is_reverse(index),
                                         N);
  }
};

template <fpta_index_type index>
struct keygen<index, fptu_160> : public fixbin_stepper<160 / 8> {
  static cxx11_constexpr_var fptu_type type = fptu_160;
  static fpta_value make(int order, int const N) {
    return fixbin_stepper<160 / 8>::make(order, fpta_index_is_reverse(index),
                                         N);
  }
};

template <fpta_index_type index>
struct keygen<index, fptu_datetime> : public scalar_range_stepper<uint64_t> {
  static cxx11_constexpr_var fptu_type type = fptu_datetime;
  static fpta_value make(int order, int const N) {
    fptu_time datetime;
    datetime.fixedpoint = scalar_range_stepper<uint64_t>::value(order, N);
    return fpta_value_datetime(datetime);
  }
};

template <fpta_index_type index>
struct keygen<index, fptu_256> : public fixbin_stepper<256 / 8> {
  static cxx11_constexpr_var fptu_type type = fptu_256;
  static fpta_value make(int order, int const N) {
    return fixbin_stepper<256 / 8>::make(order, fpta_index_is_reverse(index),
                                         N);
  }
};

template <fpta_index_type index>
struct keygen<index, fptu_cstr> : public varbin_stepper<fptu_cstr> {
  static cxx11_constexpr_var fptu_type type = fptu_cstr;
  static fpta_value make(int order, int const N) {
    return varbin_stepper<fptu_cstr>::make(order, fpta_index_is_reverse(index),
                                           N);
  }
};

template <fpta_index_type index>
struct keygen<index, fptu_opaque> : public varbin_stepper<fptu_opaque> {
  static cxx11_constexpr_var fptu_type type = fptu_opaque;
  static fpta_value make(int order, int const N) {
    return varbin_stepper<fptu_opaque>::make(order,
                                             fpta_index_is_reverse(index), N);
  }
};

//----------------------------------------------------------------------------

/* Карманный бильярд для закатки keygen-шаблонов в один не-шаблонный класс */
class any_keygen {
  typedef fpta_value (*maker_type)(int order, int const N);
  const fptu_type type;
  const fpta_index_type index;
  const maker_type maker;

  struct init_tier {
    fptu_type type;
    fpta_index_type index;
    maker_type maker;

    static fpta_value end(int order, int const N) {
      (void)order;
      (void)N;
      return fpta_value_end();
    }

    void stub(fptu_type _type, fpta_index_type _index) {
      type = _type;
      index = _index;
      maker = end;
    }

    template <fpta_index_type _index, fptu_type _type> void glue();
    template <fpta_index_type _index> void unroll(fptu_type _type);
    init_tier(fptu_type _type, fpta_index_type _index);
  };

  any_keygen(const init_tier &init, fptu_type type, fpta_index_type index);

public:
  any_keygen(fptu_type type, fpta_index_type index);
  fpta_value make(int order, int const N) const { return maker(order, N); }

  fptu_type get_type() const { return type; }
  fpta_index_type get_index() const { return index; }

  any_keygen(const any_keygen &) = delete;
  any_keygen(const any_keygen &&) = delete;
  const any_keygen &operator=(const any_keygen &) = delete;
};

//----------------------------------------------------------------------------

/* Генератор пар primary/secondary для тестирования вторичных индексов.
 * Суть в том, что значения для первичного ключа должна быть уникальные,
 * а для вторичного ключа для проверки не-уникальных индексов нужны дубликаты.
 */
struct coupled_keygen {
  const fpta_index_type se_index;
  any_keygen primary;
  any_keygen secondary;

  coupled_keygen(fpta_index_type pk_index, fptu_type pk_type,
                 fpta_index_type se_index, fptu_type se_type)
      : se_index(se_index), primary(pk_type, pk_index),
        secondary(se_type, se_index) {}

  fpta_value make_primary(int order, int const N) {
    if (fpta_index_is_unique(se_index))
      return primary.make(order, N);

    if (order % 3)
      return primary.make(order * 2, N * 2);
    return primary.make(order * 2 + 1, N * 2);
  }

  fpta_value make_primary_4dup(int order, int const N) {
    if (fpta_index_is_unique(se_index))
      return fpta_value_null();

    if (order % 3)
      return primary.make(order * 2 + 1, N * 2);
    return primary.make(order * 2, N * 2);
  }

  fpta_value make_secondary(int order, int const N) {
    return secondary.make(order, N);
  }

  coupled_keygen(const coupled_keygen &) = delete;
  coupled_keygen(const coupled_keygen &&) = delete;
  const coupled_keygen &operator=(const coupled_keygen &) = delete;
};
