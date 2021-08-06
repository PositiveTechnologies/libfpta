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

#include "fast_positive/tuples_internal.h"

#ifdef _MSC_VER
#pragma warning(push, 1)
#pragma warning(disable : 4530) /* C++ exception handler used, but             \
                                   unwind semantics are not enabled. Specify   \
                                   /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception           \
                                   handling mode specified; termination on     \
                                   exception is not guaranteed. Specify /EHsc  \
                                   */
#endif                          /* _MSC_VER (warnings) */

#include <algorithm>

#ifdef _MSC_VER
#pragma warning(pop)
#endif

/* LY: temporary workaround for Elbrus's memcmp() bug. */
#if defined(__e2k__) && !__GLIBC_PREREQ(2, 24)
FPTU_API int __hot __attribute__((__weak__))
mdbx_e2k_memcmp_bug_workaround(const void *s1, const void *s2, size_t n) {
  if (unlikely(n > 42
               /* LY: align followed access if reasonable possible */
               && (((uintptr_t)s1) & 7) != 0 &&
               (((uintptr_t)s1) & 7) == (((uintptr_t)s2) & 7))) {
    if (((uintptr_t)s1) & 1) {
      const int diff = *(uint8_t *)s1 - *(uint8_t *)s2;
      if (diff)
        return diff;
      s1 = (char *)s1 + 1;
      s2 = (char *)s2 + 1;
      n -= 1;
    }

    if (((uintptr_t)s1) & 2) {
      const uint16_t a = *(uint16_t *)s1;
      const uint16_t b = *(uint16_t *)s2;
      if (likely(a != b))
        return (__builtin_bswap16(a) > __builtin_bswap16(b)) ? 1 : -1;
      s1 = (char *)s1 + 2;
      s2 = (char *)s2 + 2;
      n -= 2;
    }

    if (((uintptr_t)s1) & 4) {
      const uint32_t a = *(uint32_t *)s1;
      const uint32_t b = *(uint32_t *)s2;
      if (likely(a != b))
        return (__builtin_bswap32(a) > __builtin_bswap32(b)) ? 1 : -1;
      s1 = (char *)s1 + 4;
      s2 = (char *)s2 + 4;
      n -= 4;
    }
  }

  while (n >= 8) {
    const uint64_t a = *(uint64_t *)s1;
    const uint64_t b = *(uint64_t *)s2;
    if (likely(a != b))
      return (__builtin_bswap64(a) > __builtin_bswap64(b)) ? 1 : -1;
    s1 = (char *)s1 + 8;
    s2 = (char *)s2 + 8;
    n -= 8;
  }

  if (n & 4) {
    const uint32_t a = *(uint32_t *)s1;
    const uint32_t b = *(uint32_t *)s2;
    if (likely(a != b))
      return (__builtin_bswap32(a) > __builtin_bswap32(b)) ? 1 : -1;
    s1 = (char *)s1 + 4;
    s2 = (char *)s2 + 4;
  }

  if (n & 2) {
    const uint16_t a = *(uint16_t *)s1;
    const uint16_t b = *(uint16_t *)s2;
    if (likely(a != b))
      return (__builtin_bswap16(a) > __builtin_bswap16(b)) ? 1 : -1;
    s1 = (char *)s1 + 2;
    s2 = (char *)s2 + 2;
  }

  return (n & 1) ? *(uint8_t *)s1 - *(uint8_t *)s2 : 0;
}

FPTU_API int __hot __attribute__((__weak__))
mdbx_e2k_strcmp_bug_workaround(const char *s1, const char *s2) {
  while (true) {
    int diff = *(uint8_t *)s1 - *(uint8_t *)s2;
    if (likely(diff != 0) || *s1 == '\0')
      return diff;
    s1 += 1;
    s2 += 1;
  }
}

FPTU_API int __hot __attribute__((__weak__))
mdbx_e2k_strncmp_bug_workaround(const char *s1, const char *s2, size_t n) {
  while (n > 0) {
    int diff = *(uint8_t *)s1 - *(uint8_t *)s2;
    if (likely(diff != 0) || *s1 == '\0')
      return diff;
    s1 += 1;
    s2 += 1;
    n -= 1;
  }
  return 0;
}

FPTU_API size_t __hot __attribute__((__weak__))
mdbx_e2k_strlen_bug_workaround(const char *s) {
  size_t n = 0;
  while (*s) {
    s += 1;
    n += 1;
  }
  return n;
}

FPTU_API size_t __hot __attribute__((__weak__))
mdbx_e2k_strnlen_bug_workaround(const char *s, size_t maxlen) {
  size_t n = 0;
  while (maxlen > n && *s) {
    s += 1;
    n += 1;
  }
  return n;
}
#endif /* Elbrus's memcmp() bug. */

static __inline fptu_lge cmpbin(const void *a, const void *b, size_t bytes) {
  return fptu_diff2lge(memcmp(a, b, bytes));
}

fptu_lge __hot fptu_cmp_binary(const void *left_data, size_t left_len,
                               const void *right_data, size_t right_len) {
  size_t shorten = std::min(left_len, right_len);
  int diff = shorten ? memcmp(left_data, right_data, shorten) : 0;
  if (diff == 0)
    diff = fptu_cmp2int(left_len, right_len);
  return fptu_diff2lge(diff);
}

//----------------------------------------------------------------------------

fptu_lge fptu_cmp_96(fptu_ro ro, unsigned column, const uint8_t *value) {
  if (unlikely(value == nullptr))
    return fptu_ic;

  const fptu_field *pf = fptu::lookup(ro, column, fptu_96);
  if (unlikely(pf == nullptr))
    return fptu_ic;

  return cmpbin(pf->payload()->fixbin, value, 12);
}

fptu_lge fptu_cmp_128(fptu_ro ro, unsigned column, const uint8_t *value) {
  if (unlikely(value == nullptr))
    return fptu_ic;

  const fptu_field *pf = fptu::lookup(ro, column, fptu_128);
  if (unlikely(pf == nullptr))
    return fptu_ic;

  return cmpbin(pf->payload()->fixbin, value, 16);
}

fptu_lge fptu_cmp_160(fptu_ro ro, unsigned column, const uint8_t *value) {
  if (unlikely(value == nullptr))
    return fptu_ic;

  const fptu_field *pf = fptu::lookup(ro, column, fptu_160);
  if (unlikely(pf == nullptr))
    return fptu_ic;

  return cmpbin(pf->payload()->fixbin, value, 20);
}

fptu_lge fptu_cmp_256(fptu_ro ro, unsigned column, const uint8_t *value) {
  if (unlikely(value == nullptr))
    return fptu_ic;

  const fptu_field *pf = fptu::lookup(ro, column, fptu_256);
  if (unlikely(pf == nullptr))
    return fptu_ic;

  return cmpbin(pf->payload()->fixbin, value, 32);
}

//----------------------------------------------------------------------------

fptu_lge fptu_cmp_opaque(fptu_ro ro, unsigned column, const void *value,
                         size_t bytes) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_opaque);
  if (pf == nullptr)
    return bytes ? fptu_ic : fptu_eq;

  const struct iovec iov = fptu_field_opaque(pf);
  return fptu_cmp_binary(iov.iov_base, iov.iov_len, value, bytes);
}

fptu_lge fptu_cmp_opaque_iov(fptu_ro ro, unsigned column,
                             const struct iovec value) {
  return fptu_cmp_opaque(ro, column, value.iov_base, value.iov_len);
}

//----------------------------------------------------------------------------

__hot static fptu_lge fptu_cmp_fields_same_type(const fptu_field *left,
                                                const fptu_field *right) {
  assert(left != nullptr && right != nullptr);
  assert(left->type() == right->type());

  auto payload_left = left->payload();
  auto payload_right = right->payload();

  switch (left->type()) {
  case fptu_null:
    return fptu_eq;

  case fptu_uint16:
    return fptu_cmp2lge(left->get_payload_uint16(),
                        right->get_payload_uint16());
  case fptu_int32:
    return fptu_cmp2lge(payload_left->peek_i32(), payload_right->peek_i32());
  case fptu_uint32:
    return fptu_cmp2lge(payload_left->peek_u32(), payload_right->peek_u32());
  case fptu_fp32:
    return fptu_cmp2lge(payload_left->peek_fp32(), payload_right->peek_fp32());

  case fptu_int64:
    return fptu_cmp2lge(payload_left->peek_i64(), payload_right->peek_i64());
  case fptu_uint64:
  case fptu_datetime:
    return fptu_cmp2lge(payload_left->peek_u64(), payload_right->peek_u64());
  case fptu_fp64:
    return fptu_cmp2lge(payload_left->peek_fp64(), payload_right->peek_fp64());

  case fptu_96:
    return cmpbin(payload_left->fixbin, payload_right->fixbin, 12);
  case fptu_128:
    return cmpbin(payload_left->fixbin, payload_right->fixbin, 16);
  case fptu_160:
    return cmpbin(payload_left->fixbin, payload_right->fixbin, 20);
  case fptu_256:
    return cmpbin(payload_left->fixbin, payload_right->fixbin, 32);

  case fptu_cstr:
    return fptu_diff2lge(strcmp(payload_left->cstr, payload_right->cstr));

  case fptu_opaque:
    return fptu_cmp_binary(
        payload_left->inner_begin(), payload_left->varlen_opaque_bytes(),
        payload_right->inner_begin(), payload_right->varlen_opaque_bytes());

  case fptu_nested:
    return fptu_cmp_tuples(fptu_field_nested(left), fptu_field_nested(right));

  default:
    /* fptu_farray */
    // TODO: лексикографическое сравнение
    return fptu_ic;
  }
}

fptu_lge fptu_cmp_fields(const fptu_field *left, const fptu_field *right) {
  if (unlikely(left == nullptr))
    return right ? fptu_lt : fptu_eq;
  if (unlikely(right == nullptr))
    return fptu_gt;

  if (likely(left->type() == right->type()))
    return fptu_cmp_fields_same_type(left, right);

  // TODO: сравнение с кастингом?
  return fptu_ic;
}

//----------------------------------------------------------------------------

// сравнение посредством фильтрации и сортировки тэгов
static __hot fptu_lge fptu_cmp_tuples_slowpath(const fptu_field *const l_begin,
                                               const fptu_field *const l_end,
                                               const fptu_field *const r_begin,
                                               const fptu_field *const r_end) {
  assert(l_end > l_begin);
  assert(r_end > r_begin);
  const auto l_size = l_end - l_begin;
  const auto r_size = r_end - r_begin;

// буфер на стеке под сортированные теги полей
#ifdef _MSC_VER /* FIXME: mustdie */
  uint16_t *const buffer =
      (uint16_t *)_malloca(sizeof(uint16_t) * (l_size + r_size));
#else
  uint16_t buffer[l_size + r_size];
#endif

  // получаем отсортированные теги слева
  uint16_t *const tags_l_begin = buffer;
  uint16_t *const tags_l_end = fptu_tags(tags_l_begin, l_begin, l_end);
  assert(tags_l_end >= tags_l_begin && tags_l_end <= tags_l_end + l_size);

  // получаем отсортированные теги справа
  uint16_t *const tags_r_begin = tags_l_end;
  uint16_t *const tags_r_end = fptu_tags(tags_r_begin, r_begin, r_end);
  assert(tags_r_end >= tags_r_begin && tags_r_end <= tags_r_end + r_size);

  // идем по отсортированным тегам
  auto tags_l = tags_l_begin, tags_r = tags_r_begin;
  for (;;) {
    // если уперлись в конец слева или справа
    {
      const bool left_depleted = (tags_l == tags_l_end);
      const bool right_depleted = (tags_r == tags_r_end);
      if (left_depleted | right_depleted)
        return fptu_cmp2lge(!left_depleted, !right_depleted);
    }

    // в тегах не должно быть удаленных элементов
    assert(!fptu_tag_is_dead(*tags_l));
    assert(!fptu_tag_is_dead(*tags_r));

    // если слева и справа разные теги
    if (*tags_l != *tags_r)
      /* "обратный результат", так как `*tags_r > *tags_l` означает, что в
       * `tags_r` отсутствует тэг (поле), которое есть в `tags_l` */
      return fptu_cmp2lge(*tags_r, *tags_l);

    /* сканируем и сравниваем все поля с текущим тегом, в каждом кортеже
     * таких полей может быть несколько, ибо поддерживаются коллекции.
     *
     * ВАЖНО:
     *  - для сравнения коллекций (нескольких полей с одинаковыми тегами)
     *    требуется перебор их элементов, в устойчивом/воспроизводимом
     *    порядке;
     *  - в качестве такого порядка (критерия) можно использовать только
     *    физический порядок расположения полей в кортеже, ибо нет
     *    каких-либо других атрибутов;
     *  - однако, в общем случае, физический порядок расположения полей
     *    зависит от истории изменения кортежа (так как вставка новых
     *    полей может производиться в "дыры" образовавшиеся в результате
     *    предыдущих удалений);
     *  - таким образом, нет способа стабильно сравнивать кортежи
     *    содержащие коллекции (повторы), без частичной утраты
     *    эффективности при модификации кортежей.
     *
     * ПОЭТОМУ:
     *  - сравниваем коллекции опираясь на физический порядок полей, т.е.
     *    результат сравнения может зависеть от истории изменений кортежа;
     *  - элементы добавленные первыми считаются наиболее значимыми, при
     *    этом для предотвращения неоднозначности есть несколько
     *    вариантов:
     *      1) Не создавать коллекции, т.е. не использовать функции
     *         вида fptu_insert_xyz(), либо использовать массивы.
     *      2) Перед fptu_insert_xyz() вызывать fptu_cond_shrink(),
     *         в результате чего физический порядок полей и элементов
     *         коллекций будет определяться порядком их добавления.
     *      3) Реализовать и использовать свою функцию сравнения.
     */
    const uint16_t tag = *tags_l;

    // ищем первое вхождение слева, оно обязано быть
    const fptu_field *field_l = l_end;
    do
      --field_l;
    while (field_l->tag != tag);
    assert(field_l >= l_begin);

    // ищем первое вхождение справа, оно обязано быть
    const fptu_field *field_r = r_end;
    do
      --field_r;
    while (field_r->tag != tag);
    assert(field_r >= r_begin);

    for (;;) {
      // сравниваем найденные экземпляры
      fptu_lge cmp = fptu_cmp_fields_same_type(field_l, field_r);
      if (cmp != fptu_eq)
        return cmp;

      // ищем следующее слева
      while (--field_l >= l_begin && field_l->tag != tag)
        ;

      // ищем следующее справа
      while (--field_r >= r_begin && field_r->tag != tag)
        ;

      // если дошли до конца слева или справа
      const bool left_depleted = (field_l < l_begin);
      const bool right_depleted = (field_r < r_begin);
      if (left_depleted | right_depleted) {
        if (left_depleted != right_depleted)
          return left_depleted ? fptu_lt : fptu_gt;
        break;
      }
    }

    ++tags_l, ++tags_r;
  }
}

// когда в обоих кортежах поля уже упорядоченные по тегам.
static __hot fptu_lge fptu_cmp_tuples_fastpath(const fptu_field *const l_begin,
                                               const fptu_field *const l_end,
                                               const fptu_field *const r_begin,
                                               const fptu_field *const r_end) {
  // кортежи не пустые
  assert(l_end > l_begin && r_end > r_begin);
  // кортежи не перекрываются, либо полностью совпадают (в юнит-тестах с NDEBUG)
  assert(l_begin > r_end || r_begin > l_end ||
         (l_begin == r_begin && l_end == r_end));
  // кортежи упорядоченны
  assert(fptu_is_ordered(l_begin, l_end));
  assert(fptu_is_ordered(r_begin, r_end));

  // идем по полям, которые упорядочены по тегам
  auto l = l_end - 1, r = r_end - 1;
  for (;;) {
    // если уперлись в конец слева или справа
    const bool left_depleted = (l < l_begin);
    const bool right_depleted = (r < r_begin);
    if (left_depleted | right_depleted)
      return fptu_cmp2lge(!left_depleted, !right_depleted);

    // не должно быть удаленных элементов
    assert(!l->is_dead());
    assert(!r->is_dead());

    // если слева и справа у полей разные теги
    if (l->tag != r->tag)
      /* "обратный результат", так как `r->ct > l->ct` означает, что в
       * `r` отсутствует тэг (поле), которое есть в `l` */
      return fptu_cmp2lge(r->tag, l->tag);

    fptu_lge cmp = fptu_cmp_fields_same_type(l, r);
    if (cmp != fptu_eq)
      return cmp;

    // пропускаем удаленные элементы
    while (unlikely((--l)->is_dead()) && l >= l_begin)
      ;
    while (unlikely((--r)->is_dead()) && r >= r_begin)
      ;
  }
}

__hot fptu_lge fptu_cmp_tuples(fptu_ro left, fptu_ro right) {
#ifdef NDEBUG /* только при выключенной отладке, ради тестирования */
  // fastpath если кортежи полностью равны "как есть"
  if (left.sys.iov_len == right.sys.iov_len &&
      (left.sys.iov_len == 0 ||
       memcmp(left.sys.iov_base, right.sys.iov_base, left.sys.iov_len) == 0))
    return fptu_eq;
#endif /* NDEBUG */

  // начало и конец дескрипторов слева
  auto l_begin = fptu_begin_ro(left);
  auto l_end = fptu_end_ro(left);
  // пропускаем удаленные элементы
  while (likely(l_begin < l_end) && unlikely(l_begin->is_dead()))
    ++l_begin;
  while (likely(l_begin < l_end) && unlikely(l_end[-1].is_dead()))
    --l_end;

  // начало и конец дескрипторов справа
  auto r_begin = fptu_begin_ro(right);
  auto r_end = fptu_end_ro(right);
  // пропускаем удаленные элементы
  while (likely(r_begin < r_end) && unlikely(r_begin->is_dead()))
    ++r_begin;
  while (likely(r_begin < r_end) && unlikely(r_end[-1].is_dead()))
    --r_end;

  // fastpath если хотя-бы один из кортежей пуст
  if (unlikely(l_begin == l_end || r_begin == r_end))
    return fptu_cmp2lge(l_begin != l_end, r_begin != r_end);

  // fastpath если оба кортежа уже упорядоченные.
  if (likely(fptu_is_ordered(l_begin, l_end) &&
             fptu_is_ordered(r_begin, r_end)))
    return fptu_cmp_tuples_fastpath(l_begin, l_end, r_begin, r_end);

  // TODO: account perfomance penalty.
  return fptu_cmp_tuples_slowpath(l_begin, l_end, r_begin, r_end);
}
