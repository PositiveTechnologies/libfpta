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

__hot const fptu_field *fptu_first(const fptu_field *begin,
                                   const fptu_field *end, unsigned column,
                                   fptu_type_or_filter type_or_filter) {
  if (is_filter(type_or_filter)) {
    for (const fptu_field *pf = begin; pf < end; ++pf) {
      if (match(pf, column, type_or_filter))
        return pf;
    }
  } else {
    uint_fast16_t ct = fptu_make_tag(column, (fptu_type)type_or_filter);
    for (const fptu_field *pf = begin; pf < end; ++pf) {
      if (pf->tag == ct)
        return pf;
    }
  }
  return end;
}

__hot const fptu_field *fptu_next(const fptu_field *from, const fptu_field *end,
                                  unsigned column,
                                  fptu_type_or_filter type_or_filter) {
  return fptu_first(from + 1, end, column, type_or_filter);
}

//----------------------------------------------------------------------------

__hot const fptu_field *fptu_first_ex(const fptu_field *begin,
                                      const fptu_field *end,
                                      fptu_field_filter filter, void *context,
                                      void *param) {
  for (const fptu_field *pf = begin; pf < end; ++pf) {
    if (pf->is_dead())
      continue;
    if (filter(pf, context, param))
      return pf;
  }
  return end;
}

__hot const fptu_field *fptu_next_ex(const fptu_field *from,
                                     const fptu_field *end,
                                     fptu_field_filter filter, void *context,
                                     void *param) {
  return fptu_first_ex(from + 1, end, filter, context, param);
}

//----------------------------------------------------------------------------

__hot bool fptu_is_empty_ro(fptu_ro ro) {
  if (unlikely(ro.total_bytes < fptu_unit_size))
    return true;
  if (unlikely(ro.total_bytes !=
               fptu_unit_size + units2bytes(ro.units[0].varlen.brutto)))
    return true;

  return (ro.units[0].varlen.tuple_items & fptu_lt_mask) == 0;
}

__hot const fptu_field *fptu_begin_ro(fptu_ro ro) {
  if (unlikely(ro.total_bytes < fptu_unit_size))
    return nullptr;
  if (unlikely(ro.total_bytes !=
               fptu_unit_size + units2bytes(ro.units[0].varlen.brutto)))
    return nullptr;

  return &ro.units[1].field;
}

__hot const fptu_field *fptu_end_ro(fptu_ro ro) {
  if (unlikely(ro.total_bytes < fptu_unit_size))
    return nullptr;
  if (unlikely(ro.total_bytes !=
               fptu_unit_size + units2bytes(ro.units[0].varlen.brutto)))
    return nullptr;

  size_t items = (size_t)ro.units[0].varlen.tuple_items & fptu_lt_mask;
  return &ro.units[1 + items].field;
}

//----------------------------------------------------------------------------

__hot const fptu_field *fptu_begin_rw(const fptu_rw *pt) {
  return &pt->units[pt->head].field;
}

__hot const fptu_field *fptu_end_rw(const fptu_rw *pt) {
  return &pt->units[pt->pivot].field;
}

//----------------------------------------------------------------------------

size_t fptu_field_count_rw(const fptu_rw *pt, unsigned column,
                           fptu_type_or_filter type_or_filter) {
  const fptu_field *end = fptu_end_rw(pt);
  const fptu_field *begin = fptu_begin_rw(pt);
  const fptu_field *pf = fptu_first(begin, end, column, type_or_filter);

  size_t count;
  for (count = 0; pf != end; pf = fptu_next(pf, end, column, type_or_filter))
    count++;

  return count;
}

size_t fptu_field_count_ro(fptu_ro ro, unsigned column,
                           fptu_type_or_filter type_or_filter) {
  const fptu_field *end = fptu_end_ro(ro);
  const fptu_field *begin = fptu_begin_ro(ro);
  const fptu_field *pf = fptu_first(begin, end, column, type_or_filter);

  size_t count;
  for (count = 0; pf != end; pf = fptu_next(pf, end, column, type_or_filter))
    count++;

  return count;
}

size_t fptu_field_count_rw_ex(const fptu_rw *pt, fptu_field_filter filter,
                              void *context, void *param) {
  const fptu_field *end = fptu_end_rw(pt);
  const fptu_field *begin = fptu_begin_rw(pt);
  const fptu_field *pf = fptu_first_ex(begin, end, filter, context, param);

  size_t count;
  for (count = 0; pf != end; pf = fptu_next_ex(pf, end, filter, context, param))
    count++;

  return count;
}

size_t fptu_field_count_ro_ex(fptu_ro ro, fptu_field_filter filter,
                              void *context, void *param) {
  const fptu_field *end = fptu_end_ro(ro);
  const fptu_field *begin = fptu_begin_ro(ro);
  const fptu_field *pf = fptu_first_ex(begin, end, filter, context, param);

  size_t count;
  for (count = 0; pf != end; pf = fptu_next_ex(pf, end, filter, context, param))
    count++;

  return count;
}
