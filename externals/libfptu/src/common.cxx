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

#if defined(__GNUC__) && __GNUC__ == 8
__noinline
#endif /* workaround for GCC 8.x bug */
    __hot size_t
    fptu_field_units(const fptu_field *pf) {
  fptu_type type = pf->type();
  if (likely(type < fptu_cstr)) {
    // fixed length type
    return fptu_internal_map_t2u[type];
  }

  // variable length type
  const fptu_payload *payload = pf->payload();
  if (type == fptu_cstr) {
    // length is not stored, but zero terminated
    return bytes2units(strlen(payload->cstr) + 1);
  }

  // length is stored
  return payload->varlen_brutto_units();
}

//----------------------------------------------------------------------------

__hot const fptu_field *fptu_lookup_ro(fptu_ro ro, unsigned column,
                                       fptu_type_or_filter type_or_filter) {
  if (unlikely(ro.total_bytes < fptu_unit_size))
    return nullptr;
  if (unlikely(ro.total_bytes != ro.units[0].varlen.brutto_size()))
    return nullptr;
  if (unlikely(column > fptu_max_cols))
    return nullptr;

  const fptu_field *begin = &ro.units[1].field;
  const size_t items = ro.units[0].varlen.tuple_items();
  const fptu_field *end = begin + (items & size_t(fptu_lt_mask));

  if (fptu_lx_mask & items) {
    // TODO: support for ordered tuples
  }

  if (is_filter(type_or_filter)) {
    for (const fptu_field *pf = begin; pf < end; ++pf) {
      if (match(pf, column, type_or_filter))
        return pf;
    }
  } else {
    uint_fast16_t tag = fptu_make_tag(column, (fptu_type)type_or_filter);
    for (const fptu_field *pf = begin; pf < end; ++pf) {
      if (pf->tag == tag)
        return pf;
    }
  }
  return nullptr;
}

__hot fptu_field *fptu_lookup_tag(fptu_rw *pt, uint_fast16_t tag) {
  const fptu_field *begin = &pt->units[pt->head].field;
  const fptu_field *pivot = &pt->units[pt->pivot].field;
  for (const fptu_field *pf = begin; pf < pivot; ++pf) {
    if (pf->tag == tag)
      return (fptu_field *)pf;
  }
  return nullptr;
}

__hot fptu_field *fptu_lookup_rw(fptu_rw *pt, unsigned column,
                                 fptu_type_or_filter type_or_filter) {
  if (unlikely(column > fptu_max_cols))
    return nullptr;

  if (is_filter(type_or_filter)) {
    const fptu_field *begin = &pt->units[pt->head].field;
    const fptu_field *pivot = &pt->units[pt->pivot].field;
    for (const fptu_field *pf = begin; pf < pivot; ++pf) {
      if (match(pf, column, type_or_filter))
        return (fptu_field *)pf;
    }
    return nullptr;
  }

  return fptu_lookup_tag(pt, fptu_make_tag(column, (fptu_type)type_or_filter));
}

//----------------------------------------------------------------------------

__hot fptu_ro fptu_take_noshrink(const fptu_rw *pt) {
  static_assert(offsetof(fptu_ro, units) == offsetof(iovec, iov_base) &&
                    offsetof(fptu_ro, total_bytes) ==
                        offsetof(iovec, iov_len) &&
                    sizeof(fptu_ro) == sizeof(iovec),
                "unexpected struct iovec");

  fptu_ro tuple;
  assert(pt->head > 0);
  assert(pt->tail - pt->head <= UINT16_MAX);
  fptu_payload *payload = (fptu_payload *)&pt->units[pt->head - 1];
  fptu::poke_unaligned(&payload->other.varlen.unaligned_netto_units,
                       uint16_t(pt->tail - pt->head));
  fptu::poke_unaligned(&payload->other.varlen.unaligned_tuple_items,
                       uint16_t(pt->pivot - pt->head));
  // TODO: support for ordered tuples
  tuple.units = (const fptu_unit *)payload;
  tuple.total_bytes = (size_t)((char *)&pt->units[pt->tail] - (char *)payload);
  return tuple;
}
