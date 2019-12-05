﻿/*
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

#ifdef _MSC_VER
#pragma warning(disable : 4738) /* storing 32-bit float result in memory,      \
                                   possible loss of performance */
#endif                          /* _MSC_VER (warnings) */

fptu_type fptu_field_type(const fptu_field *pf) {
  if (unlikely(fptu_field_is_dead(pf)))
    return fptu_null;

  return fptu_get_type(pf->tag);
}

int fptu_field_column(const fptu_field *pf) {
  if (unlikely(fptu_field_is_dead(pf)))
    return -1;

  return (int)fptu_get_colnum(pf->tag);
}

//----------------------------------------------------------------------------

uint_fast16_t fptu_field_uint16(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_uint16))
    return FPTU_DENIL_UINT16;

  return pf->offset;
}

int_fast32_t fptu_field_int32(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_int32))
    return FPTU_DENIL_SINT32;

  return pf->payload()->i32;
}

uint_fast32_t fptu_field_uint32(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_uint32))
    return FPTU_DENIL_UINT32;

  return pf->payload()->u32;
}

int_fast64_t fptu_field_int64(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_int64))
    return FPTU_DENIL_SINT64;

  return pf->payload()->i64;
}

uint_fast64_t fptu_field_uint64(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_uint64))
    return FPTU_DENIL_UINT64;

  return pf->payload()->u64;
}

double_t fptu_field_fp64(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_fp64))
    return FPTU_DENIL_FP64;

  return pf->payload()->fp64;
}

float_t fptu_field_fp32(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_fp32))
    return FPTU_DENIL_FP32;

  return pf->payload()->fp32;
}

fptu_time fptu_field_datetime(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_datetime))
    return FPTU_DENIL_TIME;

  fptu_time result = {pf->payload()->u64};
  return result;
}

const char *fptu_field_cstr(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_cstr))
    return FPTU_DENIL_CSTR;

  return pf->payload()->cstr;
}

const uint8_t *fptu_field_96(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_96))
    return FPTU_DENIL_FIXBIN;

  return pf->payload()->fixbin;
}

const uint8_t *fptu_field_128(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_128))
    return FPTU_DENIL_FIXBIN;

  return pf->payload()->fixbin;
}

const uint8_t *fptu_field_160(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_160))
    return FPTU_DENIL_FIXBIN;

  return pf->payload()->fixbin;
}

const uint8_t *fptu_field_256(const fptu_field *pf) {
  if (unlikely(fptu_field_type(pf) != fptu_256))
    return FPTU_DENIL_FIXBIN;
  return pf->payload()->fixbin;
}

struct iovec fptu_field_opaque(const fptu_field *pf) {
  iovec io;
  if (unlikely(fptu_field_type(pf) != fptu_opaque)) {
    io.iov_base = FPTU_DENIL_FIXBIN;
    io.iov_len = 0;
  } else {
    const fptu_payload *payload = pf->payload();
    io.iov_len = payload->other.varlen.opaque_bytes;
    io.iov_base = (void *)payload->other.data;
  }
  return io;
}

struct iovec fptu_field_as_iovec(const fptu_field *pf) {
  struct iovec opaque;
  const fptu_payload *payload;
  const fptu_type type = fptu_field_type(pf);

  switch (type) {
  default:
    if (likely(type < fptu_farray)) {
      assert(type < fptu_cstr);
      opaque.iov_len = fptu_internal_map_t2b[type];
      opaque.iov_base = (void *)pf->payload();
      break;
    }
    // TODO: array support
    payload = pf->payload();
    opaque.iov_base = (void *)payload->other.data;
    opaque.iov_len = units2bytes(payload->other.varlen.brutto);
    break;
  case fptu_null:
    opaque.iov_base = nullptr;
    opaque.iov_len = 0;
    break;
  case fptu_uint16:
    opaque.iov_base = (void *)&pf->offset;
    opaque.iov_len = 2;
    break;
  case fptu_opaque:
    payload = pf->payload();
    opaque.iov_len = payload->other.varlen.opaque_bytes;
    opaque.iov_base = (void *)payload->other.data;
    break;
  case fptu_cstr:
    payload = pf->payload();
    opaque.iov_len = strlen(payload->cstr);
    opaque.iov_base = (void *)payload->cstr;
    break;
  case fptu_nested:
    payload = pf->payload();
    opaque.iov_len = units2bytes(payload->other.varlen.brutto);
    opaque.iov_base = (void *)payload;
    break;
  }

  return opaque;
}

fptu_ro fptu_field_nested(const fptu_field *pf) {
  fptu_ro tuple;

  if (unlikely(fptu_field_type(pf) != fptu_nested)) {
    tuple.total_bytes = 0;
    tuple.units = nullptr;
    return tuple;
  }

  const fptu_payload *payload = pf->payload();
  tuple.total_bytes = units2bytes(payload->other.varlen.brutto + (size_t)1);
  tuple.units = (const fptu_unit *)payload;
  return tuple;
}

//----------------------------------------------------------------------------

uint_fast16_t fptu_get_uint16(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_uint16);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_uint16(pf);
}

int_fast32_t fptu_get_int32(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_int32);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_int32(pf);
}

uint_fast32_t fptu_get_uint32(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_uint32);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_uint32(pf);
}

int_fast64_t fptu_get_int64(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_int64);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_int64(pf);
}

uint_fast64_t fptu_get_uint64(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_uint64);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_uint64(pf);
}

double_t fptu_get_fp64(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_fp64);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_fp64(pf);
}

float_t fptu_get_fp32(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_fp32);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_fp32(pf);
}

//----------------------------------------------------------------------------

int_fast64_t fptu_get_sint(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_any_int);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;

  switch (fptu_field_type(pf)) {
  default:
    return FPTU_DENIL_SINT64;
  case fptu_int32:
    return pf->payload()->i32;
  case fptu_int64:
    return pf->payload()->i64;
  }
}

uint_fast64_t fptu_get_uint(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_any_uint);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;

  switch (fptu_field_type(pf)) {
  default:
    return FPTU_DENIL_UINT64;
  case fptu_uint16:
    return pf->get_payload_uint16();
  case fptu_uint32:
    return pf->payload()->u32;
  case fptu_uint64:
    return pf->payload()->u64;
  }
}

double_t fptu_get_fp(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_any_fp);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;

  switch (fptu_field_type(pf)) {
  default:
    return FPTU_DENIL_FP64;
  case fptu_fp32:
    return pf->payload()->fp32;
  case fptu_fp64:
    return pf->payload()->fp64;
  }
}

fptu_time fptu_get_datetime(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_datetime);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;

  return fptu_field_datetime(pf);
}

//----------------------------------------------------------------------------

const uint8_t *fptu_get_96(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_96);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_96(pf);
}

const uint8_t *fptu_get_128(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_128);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_128(pf);
}

const uint8_t *fptu_get_160(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_160);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_160(pf);
}

const uint8_t *fptu_get_256(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_256);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_256(pf);
}

const char *fptu_get_cstr(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_cstr);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_cstr(pf);
}

struct iovec fptu_get_opaque(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_opaque);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_opaque(pf);
}

fptu_ro fptu_get_nested(fptu_ro ro, unsigned column, int *error) {
  const fptu_field *pf = fptu::lookup(ro, column, fptu_nested);
  if (error)
    *error = pf ? FPTU_SUCCESS : FPTU_ENOFIELD;
  return fptu_field_nested(pf);
}
