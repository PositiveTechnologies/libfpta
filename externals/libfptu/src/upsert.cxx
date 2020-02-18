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

static __hot fptu_field *fptu_find_dead(fptu_rw *pt, size_t units) {
  fptu_field *end = &pt->units[pt->pivot].field;
  for (fptu_field *pf = &pt->units[pt->head].field; pf < end; ++pf) {
    if (pf->is_dead() && fptu_field_units(pf) == units)
      return pf;
  }
  return nullptr;
}

static __hot fptu_field *fptu_append(fptu_rw *pt, uint_fast16_t ct,
                                     size_t units) {
  fptu_field *pf = fptu_find_dead(pt, units);
  if (pf) {
    pf->tag = (uint16_t)ct;
    assert(pt->junk > 1 + units);
    pt->junk -= 1 + (unsigned)units;
    return pf;
  }

  if (unlikely(pt->head < 2 || pt->tail + units > pt->end))
    return nullptr;

  pt->head -= 1;
  pf = &pt->units[pt->head].field;
  if (likely(units)) {
    size_t offset = (size_t)(&pt->units[pt->tail].data - pf->body);
    if (unlikely(offset > fptu_limit))
      return nullptr;
    pf->offset = (uint16_t)offset;
    pt->tail += (unsigned)units;
  } else {
    pf->offset = UINT16_MAX;
  }

  pf->tag = (uint16_t)ct;
  return pf;
}

static __hot fptu_field *fptu_emplace(fptu_rw *pt, uint_fast16_t ct,
                                      size_t units) {
  fptu_field *pf = fptu_lookup_tag(pt, ct);
  if (pf) {
    size_t avail = fptu_field_units(pf);
    if (likely(avail == units))
      return pf;

    assert(pf->tag == ct);
    unsigned save_head = pt->head;
    unsigned save_tail = pt->tail;
    unsigned save_junk = pt->junk;

    fptu_erase_field(pt, pf);
    fptu_field *fresh = fptu_append(pt, ct, units);
    if (unlikely(fresh == nullptr)) {
      // undo erase
      // TODO: unit test for this case
      pf->tag = (uint16_t)ct;
      assert(pt->head >= save_head);
      assert(pt->tail <= save_tail);
      assert(pt->junk >= save_junk);
      pt->head = save_head;
      pt->tail = save_tail;
      pt->junk = save_junk;
    }

    return fresh;
  }

  return fptu_append(pt, ct, units);
}

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4820) /* 'fptu_takeover_result' : '4' bytes          \
                                   padding added after data member             \
                                   'fptu_takeover_result::error' */
#endif

struct fptu_takeover_result {
  fptu_field *pf;
  fptu_error error;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

static __hot fptu_takeover_result fptu_takeover(fptu_rw *pt, uint_fast16_t ct,
                                                size_t units) {
  fptu_takeover_result result;

  result.pf = fptu_lookup_tag(pt, ct);
  if (unlikely(result.pf == nullptr)) {
    result.error = FPTU_ENOFIELD;
    return result;
  }

  size_t avail = fptu_field_units(result.pf);
  if (likely(avail == units)) {
    result.error = FPTU_SUCCESS;
    return result;
  }

  fptu_erase_field(pt, result.pf);
  result.pf = fptu_append(pt, ct, units);
  result.error = likely(result.pf != nullptr) ? FPTU_OK : FPTU_ENOSPACE;
  return result;
}

static __inline void fptu_cstrcpy(fptu_field *pf, size_t units,
                                  const char *text, size_t length) {
  assert(units > 0);
  assert(strnlen(text, length) == length);
  assert(bytes2units(length + 1) == units);
  uint32_t *payload = (uint32_t *)fptu_field_payload(pf);
  payload[units - 1] = 0; // clean last unit
  memcpy(payload, text, length);
}

//============================================================================

fptu_error fptu_upsert_null(fptu_rw *pt, unsigned col) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_field *pf = fptu_emplace(pt, fptu_make_tag(col, fptu_null), 0);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  return FPTU_SUCCESS;
}

fptu_error fptu_upsert_uint16(fptu_rw *pt, unsigned col, uint_fast16_t value) {
  assert(value <= UINT16_MAX);
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_field *pf = fptu_emplace(pt, fptu_make_tag(col, fptu_uint16), 0);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  pf->offset = (uint16_t)value;
  return FPTU_SUCCESS;
}

//----------------------------------------------------------------------------

static fptu_error fptu_upsert_32(fptu_rw *pt, uint_fast16_t tag,
                                 uint_fast32_t value) {
  assert(tag_match_fixedsize(tag, 1));
  assert(!fptu_tag_is_dead(tag));

  fptu_field *pf = fptu_emplace(pt, tag, 1);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  fptu_field_payload(pf)->u32 = value;
  return FPTU_SUCCESS;
}

fptu_error fptu_upsert_int32(fptu_rw *pt, unsigned col, int_fast32_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;
  return fptu_upsert_32(pt, fptu_make_tag(col, fptu_int32), (uint32_t)value);
}

fptu_error fptu_upsert_uint32(fptu_rw *pt, unsigned col, uint_fast32_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;
  return fptu_upsert_32(pt, fptu_make_tag(col, fptu_uint32), value);
}

//----------------------------------------------------------------------------

static fptu_error fptu_upsert_64(fptu_rw *pt, uint_fast16_t tag,
                                 uint_fast64_t value) {
  assert(tag_match_fixedsize(tag, 2));
  assert(!fptu_tag_is_dead(tag));

  fptu_field *pf = fptu_emplace(pt, tag, 2);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  fptu_field_payload(pf)->u64 = value;
  return FPTU_SUCCESS;
}

fptu_error fptu_upsert_int64(fptu_rw *pt, unsigned col, int_fast64_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;
  return fptu_upsert_64(pt, fptu_make_tag(col, fptu_int64),
                        (uint_fast64_t)value);
}

fptu_error fptu_upsert_uint64(fptu_rw *pt, unsigned col, uint_fast64_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;
  return fptu_upsert_64(pt, fptu_make_tag(col, fptu_uint64), value);
}

fptu_error fptu_upsert_datetime(fptu_rw *pt, unsigned col, fptu_time value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;
  return fptu_upsert_64(pt, fptu_make_tag(col, fptu_datetime),
                        value.fixedpoint);
}

//----------------------------------------------------------------------------

fptu_error fptu_upsert_fp32(fptu_rw *pt, unsigned col, float_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  union {
    uint32_t u32;
    float fp32;
  } v;

  v.fp32 = value;
  return fptu_upsert_32(pt, fptu_make_tag(col, fptu_fp32), v.u32);
}

fptu_error fptu_upsert_fp64(fptu_rw *pt, unsigned col, double_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  union {
    uint64_t u64;
    double fp64;
  } v;

  v.fp64 = value;
  return fptu_upsert_64(pt, fptu_make_tag(col, fptu_fp64), v.u64);
}

//----------------------------------------------------------------------------

fptu_error fptu_upsert_96(fptu_rw *pt, unsigned col, const void *data) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_field *pf = fptu_emplace(pt, fptu_make_tag(col, fptu_96), 3);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  memcpy(fptu_field_payload(pf), data, 12);
  return FPTU_SUCCESS;
}

fptu_error fptu_upsert_128(fptu_rw *pt, unsigned col, const void *data) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_field *pf = fptu_emplace(pt, fptu_make_tag(col, fptu_128), 4);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  memcpy(fptu_field_payload(pf), data, 16);
  return FPTU_SUCCESS;
}

fptu_error fptu_upsert_160(fptu_rw *pt, unsigned col, const void *data) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_field *pf = fptu_emplace(pt, fptu_make_tag(col, fptu_160), 5);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  memcpy(fptu_field_payload(pf), data, 20);
  return FPTU_SUCCESS;
}

fptu_error fptu_upsert_256(fptu_rw *pt, unsigned col, const void *data) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_field *pf = fptu_emplace(pt, fptu_make_tag(col, fptu_256), 8);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  memcpy(fptu_field_payload(pf), data, 32);
  return FPTU_SUCCESS;
}

//----------------------------------------------------------------------------

fptu_error fptu_upsert_string(fptu_rw *pt, unsigned col, const char *text,
                              size_t length) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  if (unlikely(length >= fptu_max_field_bytes))
    return FPTU_EINVAL;

  size_t units = bytes2units(length + 1);
  fptu_field *pf = fptu_emplace(pt, fptu_make_tag(col, fptu_cstr), units);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  fptu_cstrcpy(pf, units, text, length);
  return FPTU_SUCCESS;
}

fptu_error fptu_upsert_opaque(fptu_rw *pt, unsigned col, const void *value,
                              size_t bytes) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  if (unlikely(bytes > fptu_max_opaque_bytes))
    return FPTU_EINVAL;

  if (unlikely(value == nullptr && bytes != 0))
    return FPTU_EINVAL;

  size_t units = bytes2units(bytes) + 1;
  fptu_field *pf = fptu_emplace(pt, fptu_make_tag(col, fptu_opaque), units);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  fptu_payload *payload = fptu_field_payload(pf);
  payload->other.varlen.brutto = (uint16_t)(units - 1);
  payload->other.varlen.opaque_bytes = (uint16_t)bytes;

  ((uint32_t *)payload)[units - 1] =
      0; // clear a padding for rid an `uninitialized` from memory-checkers.
  memcpy(payload->other.data, value, bytes);
  return FPTU_SUCCESS;
}

fptu_error fptu_upsert_opaque_iov(fptu_rw *pt, unsigned column,
                                  const struct iovec value) {
  return fptu_upsert_opaque(pt, column, value.iov_base, value.iov_len);
}

fptu_error fptu_upsert_nested(fptu_rw *pt, unsigned col, fptu_ro ro) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  if (unlikely(ro.total_bytes > fptu_max_opaque_bytes))
    return FPTU_EINVAL;

  if (unlikely(ro.total_bytes < fptu_unit_size))
    return FPTU_EINVAL;

  if (unlikely(ro.units == nullptr))
    return FPTU_EINVAL;

  size_t units = (size_t)ro.units[0].varlen.brutto + 1;
  if (unlikely(ro.total_bytes != units2bytes(units)))
    return FPTU_EINVAL;

  fptu_field *pf = fptu_emplace(pt, fptu_make_tag(col, fptu_nested), units);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  memcpy(fptu_field_payload(pf), ro.units, ro.total_bytes);
  return FPTU_SUCCESS;
}

//----------------------------------------------------------------------------

// fptu_error fptu_upsert_array_int32(fptu_rw* pt, uint_fast16_t ct, size_t
// array_length,
// const int32_t* array_data);
// fptu_error fptu_upsert_array_uint32(fptu_rw* pt, uint_fast16_t ct, size_t
// array_length,
// const uint32_t* array_data);
// fptu_error fptu_upsert_array_int64(fptu_rw* pt, uint_fast16_t ct, size_t
// array_length,
// const int64_t* array_data);
// fptu_error fptu_upsert_array_uint64(fptu_rw* pt, uint_fast16_t ct, size_t
// array_length,
// const uint64_t* array_data);
// fptu_error fptu_upsert_array_str(fptu_rw* pt, uint_fast16_t ct, size_t
// array_length, const char* array_data[]);

//============================================================================

fptu_error fptu_update_uint16(fptu_rw *pt, unsigned col, uint_fast16_t value) {
  assert(value <= UINT16_MAX);
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_takeover_result result =
      fptu_takeover(pt, fptu_make_tag(col, fptu_uint16), 0);
  if (likely(result.error == FPTU_SUCCESS)) {
    assert(result.pf != nullptr);
    result.pf->offset = (uint16_t)value;
  }
  return result.error;
}

//----------------------------------------------------------------------------

static fptu_error fptu_update_32(fptu_rw *pt, uint_fast16_t tag,
                                 uint_fast32_t value) {
  assert(tag_match_fixedsize(tag, 1));
  assert(!fptu_tag_is_dead(tag));

  fptu_takeover_result result = fptu_takeover(pt, tag, 1);
  if (likely(result.error == FPTU_SUCCESS)) {
    assert(result.pf != nullptr);
    fptu_field_payload(result.pf)->u32 = value;
  }
  return result.error;
}

fptu_error fptu_update_int32(fptu_rw *pt, unsigned col, int_fast32_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;
  return fptu_update_32(pt, fptu_make_tag(col, fptu_int32), (uint32_t)value);
}

fptu_error fptu_update_uint32(fptu_rw *pt, unsigned col, uint_fast32_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;
  return fptu_update_32(pt, fptu_make_tag(col, fptu_uint32), value);
}

//----------------------------------------------------------------------------

static fptu_error fptu_update_64(fptu_rw *pt, uint_fast16_t tag,
                                 uint_fast64_t value) {
  assert(tag_match_fixedsize(tag, 2));
  assert(!fptu_tag_is_dead(tag));

  fptu_takeover_result result = fptu_takeover(pt, tag, 2);
  if (likely(result.error == FPTU_SUCCESS)) {
    assert(result.pf != nullptr);
    fptu_field_payload(result.pf)->u64 = value;
  }
  return result.error;
}

fptu_error fptu_update_int64(fptu_rw *pt, unsigned col, int_fast64_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;
  return fptu_update_64(pt, fptu_make_tag(col, fptu_int64), (uint64_t)value);
}

fptu_error fptu_update_uint64(fptu_rw *pt, unsigned col, uint_fast64_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;
  return fptu_update_64(pt, fptu_make_tag(col, fptu_uint64), value);
}

fptu_error fptu_update_datetime(fptu_rw *pt, unsigned col, fptu_time value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;
  return fptu_update_64(pt, fptu_make_tag(col, fptu_datetime),
                        value.fixedpoint);
}

//----------------------------------------------------------------------------

fptu_error fptu_update_fp32(fptu_rw *pt, unsigned col, float_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  union {
    uint32_t u32;
    float fp32;
  } v;

  v.fp32 = value;
  return fptu_update_32(pt, fptu_make_tag(col, fptu_fp32), v.u32);
}

fptu_error fptu_update_fp64(fptu_rw *pt, unsigned col, double_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  union {
    uint64_t u64;
    double fp64;
  } v;

  v.fp64 = value;
  return fptu_update_64(pt, fptu_make_tag(col, fptu_fp64), v.u64);
}

//----------------------------------------------------------------------------

fptu_error fptu_update_96(fptu_rw *pt, unsigned col, const void *data) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_takeover_result result =
      fptu_takeover(pt, fptu_make_tag(col, fptu_96), 3);
  if (likely(result.error == FPTU_SUCCESS)) {
    assert(result.pf != nullptr);
    memcpy(fptu_field_payload(result.pf), data, 12);
  }
  return result.error;
}

fptu_error fptu_update_128(fptu_rw *pt, unsigned col, const void *data) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_takeover_result result =
      fptu_takeover(pt, fptu_make_tag(col, fptu_128), 4);
  if (likely(result.error == FPTU_SUCCESS)) {
    assert(result.pf != nullptr);
    memcpy(fptu_field_payload(result.pf), data, 16);
  }
  return result.error;
}

fptu_error fptu_update_160(fptu_rw *pt, unsigned col, const void *data) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_takeover_result result =
      fptu_takeover(pt, fptu_make_tag(col, fptu_160), 5);
  if (likely(result.error == FPTU_SUCCESS)) {
    assert(result.pf != nullptr);
    memcpy(fptu_field_payload(result.pf), data, 20);
  }
  return result.error;
}

fptu_error fptu_update_256(fptu_rw *pt, unsigned col, const void *data) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_takeover_result result =
      fptu_takeover(pt, fptu_make_tag(col, fptu_256), 8);
  if (likely(result.error == FPTU_SUCCESS)) {
    assert(result.pf != nullptr);
    memcpy(fptu_field_payload(result.pf), data, 32);
  }
  return result.error;
}

//----------------------------------------------------------------------------

fptu_error fptu_update_string(fptu_rw *pt, unsigned col, const char *text,
                              size_t length) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  if (unlikely(length >= fptu_max_field_bytes))
    return FPTU_EINVAL;

  size_t units = bytes2units(length + 1);
  fptu_takeover_result result =
      fptu_takeover(pt, fptu_make_tag(col, fptu_cstr), units);
  if (likely(result.error == FPTU_SUCCESS)) {
    assert(result.pf != nullptr);
    fptu_cstrcpy(result.pf, units, text, length);
  }
  return result.error;
}

fptu_error fptu_update_opaque(fptu_rw *pt, unsigned col, const void *value,
                              size_t bytes) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  if (unlikely(bytes > fptu_max_opaque_bytes))
    return FPTU_EINVAL;

  size_t units = bytes2units(bytes) + 1;
  fptu_takeover_result result =
      fptu_takeover(pt, fptu_make_tag(col, fptu_opaque), units);
  if (likely(result.error == FPTU_SUCCESS)) {
    assert(result.pf != nullptr);
    fptu_payload *payload = fptu_field_payload(result.pf);
    payload->other.varlen.brutto = (uint16_t)(units - 1);
    payload->other.varlen.opaque_bytes = (uint16_t)bytes;

    ((uint32_t *)payload)[units - 1] =
        0; // clear a padding for rid an `uninitialized` from memory-checkers.
    memcpy(payload->other.data, value, bytes);
  }
  return result.error;
}

fptu_error fptu_update_opaque_iov(fptu_rw *pt, unsigned column,
                                  const struct iovec value) {
  return fptu_update_opaque(pt, column, value.iov_base, value.iov_len);
}

fptu_error fptu_update_nested(fptu_rw *pt, unsigned col, fptu_ro ro) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  if (unlikely(ro.total_bytes > fptu_max_opaque_bytes))
    return FPTU_EINVAL;

  if (unlikely(ro.total_bytes < fptu_unit_size))
    return FPTU_EINVAL;

  if (unlikely(ro.units == nullptr))
    return FPTU_EINVAL;

  size_t units = (size_t)ro.units[0].varlen.brutto + 1;
  if (unlikely(ro.total_bytes != units2bytes(units)))
    return FPTU_EINVAL;

  fptu_takeover_result result =
      fptu_takeover(pt, fptu_make_tag(col, fptu_nested), units);
  if (likely(result.error == FPTU_SUCCESS)) {
    assert(result.pf != nullptr);
    memcpy(fptu_field_payload(result.pf), ro.units, ro.total_bytes);
  }
  return result.error;
}

//============================================================================

fptu_error fptu_insert_uint16(fptu_rw *pt, unsigned col, uint_fast16_t value) {
  assert(value <= UINT16_MAX);
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_field *pf = fptu_append(pt, fptu_make_tag(col, fptu_uint16), 0);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  pf->offset = (uint16_t)value;
  return FPTU_SUCCESS;
}

//----------------------------------------------------------------------------

static fptu_error fptu_insert_32(fptu_rw *pt, uint_fast16_t tag,
                                 uint_fast32_t v) {
  assert(tag_match_fixedsize(tag, 1));
  assert(!fptu_tag_is_dead(tag));

  fptu_field *pf = fptu_append(pt, tag, 1);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  fptu_field_payload(pf)->u32 = v;
  return FPTU_SUCCESS;
}

fptu_error fptu_insert_int32(fptu_rw *pt, unsigned col, int_fast32_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;
  return fptu_insert_32(pt, fptu_make_tag(col, fptu_int32), (uint32_t)value);
}

fptu_error fptu_insert_uint32(fptu_rw *pt, unsigned col, uint_fast32_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;
  return fptu_insert_32(pt, fptu_make_tag(col, fptu_uint32), value);
}

//----------------------------------------------------------------------------

static fptu_error fptu_insert_64(fptu_rw *pt, uint_fast16_t tag,
                                 uint_fast64_t v) {
  assert(tag_match_fixedsize(tag, 2));
  assert(!fptu_tag_is_dead(tag));

  fptu_field *pf = fptu_append(pt, tag, 2);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  fptu_field_payload(pf)->u64 = v;
  return FPTU_SUCCESS;
}

fptu_error fptu_insert_int64(fptu_rw *pt, unsigned col, int_fast64_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;
  return fptu_insert_64(pt, fptu_make_tag(col, fptu_int64), (uint64_t)value);
}

fptu_error fptu_insert_uint64(fptu_rw *pt, unsigned col, uint_fast64_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;
  return fptu_insert_64(pt, fptu_make_tag(col, fptu_uint64), value);
}

fptu_error fptu_insert_datetime(fptu_rw *pt, unsigned col, fptu_time value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;
  return fptu_insert_64(pt, fptu_make_tag(col, fptu_datetime),
                        value.fixedpoint);
}

//----------------------------------------------------------------------------

fptu_error fptu_insert_fp32(fptu_rw *pt, unsigned col, float_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  union {
    uint32_t u32;
    float fp32;
  } v;

  v.fp32 = value;
  return fptu_insert_32(pt, fptu_make_tag(col, fptu_fp32), v.u32);
}

fptu_error fptu_insert_fp64(fptu_rw *pt, unsigned col, double_t value) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  union {
    uint64_t u64;
    double fp64;
  } v;

  v.fp64 = value;
  return fptu_insert_64(pt, fptu_make_tag(col, fptu_fp64), v.u64);
}

//----------------------------------------------------------------------------

fptu_error fptu_insert_96(fptu_rw *pt, unsigned col, const void *data) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_field *pf = fptu_append(pt, fptu_make_tag(col, fptu_96), 3);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  memcpy(fptu_field_payload(pf), data, 12);
  return FPTU_SUCCESS;
}

fptu_error fptu_insert_128(fptu_rw *pt, unsigned col, const void *data) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_field *pf = fptu_append(pt, fptu_make_tag(col, fptu_128), 4);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  memcpy(fptu_field_payload(pf), data, 16);
  return FPTU_SUCCESS;
}

fptu_error fptu_insert_160(fptu_rw *pt, unsigned col, const void *data) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_field *pf = fptu_append(pt, fptu_make_tag(col, fptu_160), 5);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  memcpy(fptu_field_payload(pf), data, 20);
  return FPTU_SUCCESS;
}

fptu_error fptu_insert_256(fptu_rw *pt, unsigned col, const void *data) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  fptu_field *pf = fptu_append(pt, fptu_make_tag(col, fptu_256), 8);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  memcpy(fptu_field_payload(pf), data, 32);
  return FPTU_SUCCESS;
}

//----------------------------------------------------------------------------

fptu_error fptu_insert_string(fptu_rw *pt, unsigned col, const char *text,
                              size_t length) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  if (unlikely(length >= fptu_max_field_bytes))
    return FPTU_EINVAL;

  size_t units = bytes2units(length + 1);
  fptu_field *pf = fptu_append(pt, fptu_make_tag(col, fptu_cstr), units);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  fptu_cstrcpy(pf, units, text, length);
  return FPTU_SUCCESS;
}

fptu_error fptu_insert_opaque(fptu_rw *pt, unsigned col, const void *value,
                              size_t bytes) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  if (unlikely(bytes > fptu_max_opaque_bytes))
    return FPTU_EINVAL;

  size_t units = bytes2units(bytes) + 1;
  fptu_field *pf = fptu_append(pt, fptu_make_tag(col, fptu_opaque), units);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  fptu_payload *payload = fptu_field_payload(pf);
  payload->other.varlen.brutto = (uint16_t)(units - 1);
  payload->other.varlen.opaque_bytes = (uint16_t)bytes;

  ((uint32_t *)payload)[units - 1] =
      0; // clear a padding for rid an `uninitialized` from memory-checkers.
  memcpy(payload->other.data, value, bytes);
  return FPTU_SUCCESS;
}

fptu_error fptu_insert_opaque_iov(fptu_rw *pt, unsigned column,
                                  const struct iovec value) {
  return fptu_insert_opaque(pt, column, value.iov_base, value.iov_len);
}

fptu_error fptu_insert_nested(fptu_rw *pt, unsigned col, fptu_ro ro) {
  if (unlikely(col > fptu_max_cols))
    return FPTU_EINVAL;

  if (unlikely(ro.total_bytes > fptu_max_opaque_bytes))
    return FPTU_EINVAL;

  if (unlikely(ro.total_bytes < fptu_unit_size))
    return FPTU_EINVAL;

  if (unlikely(ro.units == nullptr))
    return FPTU_EINVAL;

  size_t units = (size_t)ro.units[0].varlen.brutto + 1;
  if (unlikely(ro.total_bytes != units2bytes(units)))
    return FPTU_EINVAL;

  fptu_field *pf = fptu_append(pt, fptu_make_tag(col, fptu_nested), units);
  if (unlikely(pf == nullptr))
    return FPTU_ENOSPACE;

  memcpy(fptu_field_payload(pf), ro.units, ro.total_bytes);
  return FPTU_SUCCESS;
}
