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

#include "fast_positive/tuples_internal.h"

static __inline bool fptu_is_tailed(fptu_rw *pt, fptu_field *pf, size_t units) {
  assert(pf == &pt->units[pt->head].field);

  return units == 0 ||
         &pf->body[pf->offset + units] == &pt->units[pt->tail].data;
}

void fptu_erase_field(fptu_rw *pt, fptu_field *pf) {
  if (unlikely(pf->is_dead()))
    return;

  // mark field as `dead`
  pf->tag |= fptu_co_dead << fptu_co_shift;
  size_t units = fptu_field_units(pf);

  // head & tail optimization
  if (pf != &pt->units[pt->head].field || !fptu_is_tailed(pt, pf, units)) {
    // account junk
    pt->junk += (unsigned)units + 1;
    return;
  }

  // cutoff head and tail
  pt->head += 1;
  pt->tail -= (unsigned)units;

  // continue cutting junk
  while (pt->head < pt->pivot) {
    pf = &pt->units[pt->head].field;
    if (!pf->is_dead())
      break;

    units = fptu_field_units(pf);
    if (!fptu_is_tailed(pt, pf, units))
      break;

    assert(pt->junk >= units + 1);
    pt->junk -= (unsigned)units + 1;
    pt->head += 1;
    pt->tail -= (unsigned)units;
  }
}

int fptu_erase(fptu_rw *pt, unsigned column,
               fptu_type_or_filter type_or_filter) {
  if (unlikely(column > fptu_max_cols)) {
    static_assert(FPTU_EINVAL > 0, "should be positive");
    return -FPTU_EINVAL;
  }

  if (is_filter(type_or_filter)) {
    int count = 0;
    fptu_field *begin = &pt->units[pt->head].field;
    fptu_field *pivot = &pt->units[pt->pivot].field;
    for (fptu_field *pf = begin; pf < pivot; ++pf) {
      if (match(pf, column, type_or_filter)) {
        fptu_erase_field(pt, pf);
        count++;
      }
    }
    return count;
  }

  fptu_field *pf =
      fptu_lookup_tag(pt, fptu_make_tag(column, (fptu_type)type_or_filter));
  if (pf == nullptr)
    return 0;

  fptu_erase_field(pt, pf);
  return 1;
}
