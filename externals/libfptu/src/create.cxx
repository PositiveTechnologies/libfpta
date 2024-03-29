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

size_t fptu_space(size_t items, size_t data_bytes) {
  if (items > fptu_max_fields)
    items = fptu_max_fields;
  if (data_bytes > fptu_max_tuple_bytes)
    data_bytes = fptu_max_tuple_bytes;

  return sizeof(fptu_rw) + items * fptu_unit_size +
         FPT_ALIGN_CEIL(data_bytes, fptu_unit_size);
}

fptu_rw *fptu_init(void *space, size_t buffer_bytes, size_t items_limit) {
  if (unlikely(space == nullptr || items_limit > fptu_max_fields))
    return nullptr;

  if (unlikely(buffer_bytes < sizeof(fptu_rw) + fptu_unit_size * items_limit))
    return nullptr;

  if (unlikely(buffer_bytes > fptu_buffer_limit))
    return nullptr;

  fptu_rw *pt = (fptu_rw *)space;
  // make a empty tuple
  pt->end = (unsigned)(buffer_bytes - sizeof(fptu_rw)) / fptu_unit_size + 1;
  pt->head = pt->tail = pt->pivot = (unsigned)items_limit + 1;
  pt->junk = 0;
  return pt;
}

fptu_error fptu_clear(fptu_rw *pt) {
  if (unlikely(pt == nullptr))
    return FPTU_EINVAL;
  if (unlikely(pt->pivot < 1 || pt->pivot > fptu_max_fields + 1 ||
               pt->pivot >= pt->end ||
               pt->end > bytes2units(fptu_buffer_limit)))
    return FPTU_EINVAL;

  pt->head = pt->tail = pt->pivot;
  pt->junk = 0;
  return FPTU_OK;
}

size_t fptu_space4items(const fptu_rw *pt) {
  return (pt->head > 0) ? pt->head - 1 : 0;
}

size_t fptu_space4data(const fptu_rw *pt) {
  return units2bytes(pt->end - pt->tail);
}

size_t fptu_junkspace(const fptu_rw *pt) { return units2bytes(pt->junk); }

//----------------------------------------------------------------------------

fptu_rw *fptu_fetch(fptu_ro ro, void *space, size_t buffer_bytes,
                    unsigned more_items) {
  if (ro.total_bytes == 0)
    return fptu_init(space, buffer_bytes, more_items);

  if (unlikely(ro.units == nullptr))
    return nullptr;
  if (unlikely(ro.total_bytes < fptu_unit_size))
    return nullptr;
  if (unlikely(ro.total_bytes > fptu_max_tuple_bytes))
    return nullptr;
  if (unlikely(ro.total_bytes != ro.units[0].varlen.brutto_size()))
    return nullptr;

  size_t items = ro.units[0].varlen.tuple_items() & size_t(fptu_lt_mask);
  if (unlikely(items > fptu_max_fields))
    return nullptr;
  if (unlikely(space == nullptr || more_items > fptu_max_fields))
    return nullptr;
  if (unlikely(buffer_bytes > fptu_buffer_limit))
    return nullptr;

  const char *end = (const char *)ro.units + ro.total_bytes;
  const char *begin = (const char *)&ro.units[1];
  const char *pivot = (const char *)begin + units2bytes(items);
  if (unlikely(pivot > end))
    return nullptr;

  size_t reserve_items = items + more_items;
  if (reserve_items > fptu_max_fields)
    reserve_items = fptu_max_fields;

  ptrdiff_t payload_bytes = end - pivot;
  if (unlikely(buffer_bytes <
               sizeof(fptu_rw) + units2bytes(reserve_items) + payload_bytes))
    return nullptr;

  fptu_rw *pt = (fptu_rw *)space;
  pt->end = (unsigned)(buffer_bytes - sizeof(fptu_rw)) / fptu_unit_size + 1;
  pt->pivot = (unsigned)reserve_items + 1;
  pt->head = pt->pivot - (unsigned)items;
  pt->tail = pt->pivot + (unsigned)(payload_bytes >> fptu_unit_shift);
  pt->junk = 0;

  memcpy(&pt->units[pt->head], begin, ro.total_bytes - fptu_unit_size);
  return pt;
}

static size_t more_buffer_size(const fptu_ro &ro, unsigned more_items,
                               unsigned more_payload) {
  size_t items = ro.units[0].varlen.tuple_items() & size_t(fptu_lt_mask);
  size_t payload_bytes = ro.total_bytes - units2bytes(items + 1);
  return fptu_space(items + more_items, payload_bytes + more_payload);
}

size_t fptu_check_and_get_buffer_size(fptu_ro ro, unsigned more_items,
                                      unsigned more_payload,
                                      const char **error) {
  if (unlikely(error == nullptr))
    return ~(size_t)0;

  *error = fptu_check_ro(ro);
  if (unlikely(*error != nullptr))
    return 0;

  if (unlikely(more_items > fptu_max_fields)) {
    *error = "more_items > fptu_max_fields";
    return 0;
  }
  if (unlikely(more_payload > fptu_max_tuple_bytes)) {
    *error = "more_payload > fptu_max_tuple_bytes";
    return 0;
  }

  return more_buffer_size(ro, more_items, more_payload);
}

size_t fptu_get_buffer_size(fptu_ro ro, unsigned more_items,
                            unsigned more_payload) {
  if (more_items > fptu_max_fields)
    more_items = fptu_max_fields;
  if (more_payload > fptu_max_tuple_bytes)
    more_payload = fptu_max_tuple_bytes;
  return more_buffer_size(ro, more_items, more_payload);
}

//----------------------------------------------------------------------------

// TODO: split out
fptu_rw *fptu_alloc(size_t items_limit, size_t data_bytes) {
  if (unlikely(items_limit > fptu_max_fields ||
               data_bytes > fptu_max_tuple_bytes))
    return nullptr;

  size_t size = fptu_space(items_limit, data_bytes);
  void *buffer = malloc(size);
  if (unlikely(!buffer))
    return nullptr;

  fptu_rw *pt = fptu_init(buffer, size, items_limit);
  assert(pt != nullptr);

  return pt;
}

fptu_rw *fptu_rw::create(size_t items_limit, size_t data_bytes) {
  if (unlikely(items_limit > fptu_max_fields ||
               data_bytes > fptu_max_tuple_bytes))
    throw std::invalid_argument(
        "fptu::alloc_tuple_c(): items_limit and/or data_bytes is invalid");

  fptu_rw *pt = fptu_alloc(items_limit, data_bytes);
  if (unlikely(!pt))
    throw std::bad_alloc();

  return pt;
}
