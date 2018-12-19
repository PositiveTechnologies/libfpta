/*
 * Copyright 2016-2018 libfptu authors: please see AUTHORS file.
 *
 * This file is part of libfptu, aka "Fast Positive Tuples".
 *
 * libfptu is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * libfptu is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with libfptu.  If not, see <http://www.gnu.org/licenses/>.
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
#include <functional>

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#include "bitset4tags.h"

using namespace fptu;

__hot bool fptu_is_ordered(const fptu_field *begin, const fptu_field *end) {
  if (likely(end > begin + 1)) {
    /* При формировании кортежа дескрипторы полей физически размещаются
     * в обратном порядке. Считаем что они в правильном порядке, когда
     * сначала добавляются поля с меньшими номерами. Соответственно,
     * при движении от begin к end, при правильном порядке номера полей
     * будут уменьшаться.
     *
     * Сканируем дескрипторы в направлении от begin к end, от недавно
     * добавленных к первым, ибо предположительно порядок чаще будет
     * нарушаться в результате последних изменений. */
    --end;
    auto scan = begin;
    do {
      auto next = scan;
      ++next;
      if (scan->tag < next->tag && likely(!next->is_dead()))
        return false;
      scan = next;
    } while (scan < end);
  }
  return true;
}

//----------------------------------------------------------------------------

/* Подзадача:
 *  - необходимо формировать сортированный список тегов (тип и номер) полей;
 *  - с фильтрацией дубликатов;
 *  - быстро, с минимумом накладных расходов.
 *  - есть вероятность наличия порядка в полях по тегам;
 *  - высока вероятность что часть полей (начало или конец) отсортированы.
 *
 * Решение:
 * 1) Двигаемся по дескрипторам полей, пока они упорядочены, при этом:
 *    - сразу формируем результирующий вектор;
 *    - фильтруем дубликаты просто пропуская повторяющиеся значения.
 *
 * 2) Встречая нарушения порядка переходим на slowpath:
 *    - фильтруем дубликаты посредством битовой карты;
 *    - в конце сортируем полученный вектор.
 */

static __noinline uint16_t *fptu_tags_slowpath(uint16_t *const first,
                                               uint16_t *tail,
                                               const fptu_field *const pos,
                                               const fptu_field *const end,
                                               unsigned have) {
  assert(std::is_sorted(first, tail, std::less_equal<uint16_t>()));

  bitset4tags::minimize params(pos, end, have);
  bitset4tags bitmask(params, alloca(params.bytes()));

  /* отмечаем обработанное */
  for (auto i = first; i < tail; ++i)
    bitmask.set(*i);

  /* обрабатываем неупорядоченный остаток */
  for (auto i = pos; i < end; ++i)
    if (likely(!i->is_dead()) && !bitmask.test_and_set(i->tag))
      *tail++ = i->tag;

  std::sort(first, tail);
  assert(std::is_sorted(first, tail, std::less_equal<uint16_t>()));
  return tail;
}

uint16_t *fptu_tags(uint16_t *const first, const fptu_field *begin,
                    const fptu_field *end) {
  /* Формирует в буфере упорядоченный список тегов полей без дубликатов. */
  uint16_t *tail = first;

  // пропускаем удаленные элементы
  while (likely(begin < end) && unlikely(begin->is_dead()))
    ++begin;
  while (likely(begin < end) && unlikely(end[-1].is_dead()))
    --end;

  if (end > begin) {
    const fptu_field *i;
    unsigned have = 0;

    /* Пытаемся угадать текущий порядок и переливаем в буфер
     * пропуская дубликаты. */
    if (begin->tag >= end[-1].tag) {
      for (i = end - 1, *tail++ = i->tag; --i >= begin;) {
        if (unlikely(i->is_dead()))
          continue;
        if (i->tag != tail[-1]) {
          if (unlikely(i->tag < tail[-1]))
            return fptu_tags_slowpath(first, tail, begin, i + 1, have);
          have |= (*tail++ = i->tag);
        }
      }
    } else {
      for (i = begin, *tail++ = i->tag; ++i < end;) {
        if (unlikely(i->is_dead()))
          continue;
        if (i->tag != tail[-1]) {
          if (unlikely(i->tag < tail[-1]))
            return fptu_tags_slowpath(first, tail, i, end, have);
          have |= (*tail++ = i->tag);
        }
      }
    }
    assert(std::is_sorted(first, tail, std::less_equal<uint16_t>()));
  }
  return tail;
}
