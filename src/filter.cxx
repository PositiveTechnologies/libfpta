/*
 * Copyright 2016-2019 libfpta authors: please see AUTHORS file.
 *
 * This file is part of libfpta, aka "Fast Positive Tables".
 *
 * libfpta is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * libfpta is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with libfpta.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "details.h"

static __hot fptu_lge fpta_cmp_null(const fptu_field *left) {
  const auto payload = left->payload();

  switch (left->type()) {
  case fptu_null /* here is/should not a composite column/index */:
    return fptu_eq;
  case fptu_opaque:
    if (payload->other.varlen.opaque_bytes == 0)
      return fptu_eq;
    __fallthrough;
  default:
    return fptu_ic;
  }
}

static __hot fptu_lge fpta_cmp_sint(const fptu_field *left, int64_t right) {
  const auto payload = left->payload();

  switch (left->type()) {
  case fptu_uint16:
    return fptu_cmp2lge<int64_t>(left->get_payload_uint16(), right);

  case fptu_uint32:
    return fptu_cmp2lge<int64_t>(payload->u32, right);

  case fptu_int32:
    return fptu_cmp2lge<int64_t>(payload->i32, right);

  case fptu_uint64:
    if (right < 0)
      return fptu_gt;
    return fptu_cmp2lge(payload->u64, (uint64_t)right);

  case fptu_int64:
    return fptu_cmp2lge(payload->i64, right);

  case fptu_fp32:
    return fptu_cmp2lge<double>(payload->fp32, (double)right);

  case fptu_fp64:
    return fptu_cmp2lge<double>(payload->fp64, (double)right);

  default:
    return fptu_ic;
  }
}

static __hot fptu_lge fpta_cmp_uint(const fptu_field *left, uint64_t right) {
  const auto payload = left->payload();

  switch (left->type()) {
  case fptu_uint16:
    return fptu_cmp2lge<uint64_t>(left->get_payload_uint16(), right);

  case fptu_int32:
    if (payload->i32 < 0)
      return fptu_lt;
    __fallthrough;
  case fptu_uint32:
    return fptu_cmp2lge<uint64_t>(payload->u32, right);

  case fptu_int64:
    if (payload->i64 < 0)
      return fptu_lt;
    __fallthrough;
  case fptu_uint64:
    return fptu_cmp2lge(payload->u64, right);

  case fptu_fp32:
    return fptu_cmp2lge<double>(payload->fp32, (double)right);

  case fptu_fp64:
    return fptu_cmp2lge<double>(payload->fp64, (double)right);

  default:
    return fptu_ic;
  }
}

static __hot fptu_lge fpta_cmp_fp(const fptu_field *left, double right) {
  const auto payload = left->payload();

  switch (left->type()) {
  case fptu_uint16:
    return fptu_cmp2lge<double>(left->get_payload_uint16(), right);

  case fptu_int32:
    return fptu_cmp2lge<double>(payload->i32, right);

  case fptu_uint32:
    return fptu_cmp2lge<double>(payload->u32, right);

  case fptu_int64:
    return fptu_cmp2lge<double>((double)payload->i64, right);

  case fptu_uint64:
    return fptu_cmp2lge<double>((double)payload->u64, right);

  case fptu_fp32:
    return fptu_cmp2lge<double>(payload->fp32, right);

  case fptu_fp64:
    return fptu_cmp2lge(payload->fp64, right);

  default:
    return fptu_ic;
  }
}

static __hot fptu_lge fpta_cmp_datetime(const fptu_field *left,
                                        const fptu_time right) {

  if (unlikely(left->type() != fptu_datetime))
    return fptu_ic;

  const auto payload = left->payload();
  return fptu_cmp2lge(payload->u64, right.fixedpoint);
}

static __hot fptu_lge fpta_cmp_string(const fptu_field *left, const char *right,
                                      size_t length) {
  const auto payload = left->payload();

  switch (left->type()) {
  default:
    return fptu_ic;

  case fptu_cstr:
    return fptu_cmp_str_binary(payload->cstr, right, length);

  case fptu_opaque:
    return fptu_cmp_binary(payload->other.data,
                           payload->other.varlen.opaque_bytes, right, length);
  }
}

static __hot fptu_lge fpta_cmp_binary(const fptu_field *left,
                                      const void *right_data,
                                      size_t right_len) {
  const auto payload = left->payload();
  const void *left_data = payload->fixbin;
  size_t left_len;

  switch (left->type()) {
  case fptu_null /* here is/should not a composite column/index */:
    return (right_len == 0) ? fptu_eq : fptu_ic;

  case fptu_uint16:
  case fptu_uint32:
  case fptu_int32:
  case fptu_fp32:
  case fptu_uint64:
  case fptu_int64:
  case fptu_fp64:
  case fptu_datetime:
    return fptu_ic;

  case fptu_96:
    left_len = 12;
    break;
  case fptu_128:
    left_len = 16;
    break;
  case fptu_160:
    left_len = 20;
    break;
  case fptu_256:
    left_len = 32;
    break;

  case fptu_cstr:
    return fptu_cmp_str_binary(payload->cstr, right_data, right_len);

  case fptu_opaque:
    left_len = payload->other.varlen.opaque_bytes;
    left_data = payload->other.data;
    break;

  case fptu_nested:
  default: /* fptu_farray */
    left_len = units2bytes(payload->other.varlen.brutto);
    left_data = payload->other.data;
    break;
  }

  return fptu_cmp_binary(left_data, left_len, right_data, right_len);
}

//----------------------------------------------------------------------------

static __hot fptu_lge fpta_filter_cmp(const fptu_field *pf,
                                      const fpta_value &right) {
  if ((unlikely(pf == nullptr)))
    return (right.type == fpta_null) ? fptu_eq : fptu_ic;

  switch (right.type) {
  case fpta_null:
    return fpta_cmp_null(pf);

  case fpta_signed_int:
    return fpta_cmp_sint(pf, right.sint);

  case fpta_unsigned_int:
    return fpta_cmp_uint(pf, right.uint);

  case fpta_float_point:
    return fpta_cmp_fp(pf, right.fp);

  case fpta_datetime:
    return fpta_cmp_datetime(pf, right.datetime);

  case fpta_string:
    return fpta_cmp_string(pf, right.str, right.binary_length);

  case fpta_binary:
  case fpta_shoved:
    return fpta_cmp_binary(pf, right.binary_data, right.binary_length);

  default:
    assert(false);
    return fptu_ic;
  }
}

fptu_lge __fpta_filter_cmp(const fptu_field *pf, const fpta_value *right) {
  return fpta_filter_cmp(pf, *right);
}

__hot bool fpta_filter_match(const fpta_filter *fn, fptu_ro tuple) {

tail_recursion:

  if (unlikely(fn == nullptr))
    // empty filter
    return true;

  switch (fn->type) {
  case fpta_node_not:
    return !fpta_filter_match(fn->node_not, tuple);

  case fpta_node_or:
    if (fpta_filter_match(fn->node_or.a, tuple))
      return true;
    fn = fn->node_or.b;
    goto tail_recursion;

  case fpta_node_and:
    if (!fpta_filter_match(fn->node_and.a, tuple))
      return false;
    fn = fn->node_and.b;
    goto tail_recursion;

  case fpta_node_fncol:
    return fn->node_fncol.predicate(
        fptu::lookup(tuple, fn->node_fncol.column_id->column.num,
                     fpta_id2type(fn->node_fncol.column_id)),
        fn->node_fncol.arg);

  case fpta_node_fnrow:
    return fn->node_fnrow.predicate(&tuple, fn->node_fnrow.context,
                                    fn->node_fnrow.arg);

  default:
    int cmp_bits =
        fpta_filter_cmp(fptu::lookup(tuple, fn->node_cmp.left_id->column.num,
                                     fpta_id2type(fn->node_cmp.left_id)),
                        fn->node_cmp.right_value);

    return (cmp_bits & fn->type) != 0;
  }
}

//----------------------------------------------------------------------------

bool fpta_filter_validate(const fpta_filter *filter) {
  int rc;

tail_recursion:

  if (!filter)
    return true;

  switch (filter->type) {
  default:
    return false;

  case fpta_node_fncol:
    rc = fpta_id_validate(filter->node_fncol.column_id, fpta_column);
    if (unlikely(rc != FPTA_SUCCESS))
      return false;
    if (unlikely(fpta_column_is_composite(filter->node_fncol.column_id)))
      return false;
    if (unlikely(!filter->node_fncol.predicate))
      return false;
    return true;

  case fpta_node_fnrow:
    if (unlikely(!filter->node_fnrow.predicate))
      return false;
    return true;

  case fpta_node_not:
    filter = filter->node_not;
    goto tail_recursion;

  case fpta_node_or:
  case fpta_node_and:
    if (unlikely(!fpta_filter_validate(filter->node_and.a)))
      return false;
    filter = filter->node_and.b;
    goto tail_recursion;

  case fpta_node_lt:
  case fpta_node_gt:
  case fpta_node_le:
  case fpta_node_ge:
  case fpta_node_eq:
  case fpta_node_ne:
    rc = fpta_id_validate(filter->node_cmp.left_id, fpta_column);
    if (unlikely(rc != FPTA_SUCCESS))
      return false;
    if (unlikely(fpta_column_is_composite(filter->node_cmp.left_id)))
      return false;

    if (unlikely(filter->node_cmp.right_value.type == fpta_begin ||
                 filter->node_cmp.right_value.type == fpta_end))
      return false;

    /* FIXME: проверка на совместимость типов node_cmp.left_id и
     * node_cmp.right_value*/
    return true;
  }
}

//----------------------------------------------------------------------------

int fpta_name_refresh_filter(fpta_txn *txn, fpta_name *table_id,
                             fpta_filter *filter) {
tail_recursion:
  int rc = FPTA_SUCCESS;
  if (filter) {
    switch (filter->type) {
    default:
      break;

    case fpta_node_fncol:
      rc =
          fpta_name_refresh_couple(txn, table_id, filter->node_fncol.column_id);
      break;

    case fpta_node_not:
      filter = filter->node_not;
      goto tail_recursion;

    case fpta_node_or:
    case fpta_node_and:
      rc = fpta_name_refresh_filter(txn, table_id, filter->node_and.a);
      if (unlikely(rc != FPTA_SUCCESS))
        break;
      filter = filter->node_and.b;
      goto tail_recursion;

    case fpta_node_lt:
    case fpta_node_gt:
    case fpta_node_le:
    case fpta_node_ge:
    case fpta_node_eq:
    case fpta_node_ne:
      rc = fpta_name_refresh_couple(txn, table_id, filter->node_cmp.left_id);
      break;
    }
  }

  return rc;
}

//----------------------------------------------------------------------------

FPTA_API int fpta_estimate(fpta_txn *txn, unsigned items_count,
                           fpta_estimate_item *items_vector) {
  if (unlikely(items_count < 1 || items_count > fpta_max_indexes ||
               items_vector == nullptr))
    return FPTA_EINVAL;

  int rc = fpta_txn_validate(txn, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_estimate_item *const vector_begin = items_vector;
  fpta_estimate_item *const vector_end = items_vector + items_count;
  for (fpta_estimate_item *i = vector_begin; i != vector_end; ++i) {
    i->items = SIZE_MAX / 2;
    rc = fpta_id_validate(i->column, fpta_column);
    if (unlikely(rc != FPTA_SUCCESS)) {
      i->error = rc;
      continue;
    }
    rc = fpta_name_refresh(txn, i->column);
    if (unlikely(rc != FPTA_SUCCESS)) {
      i->error = rc;
      continue;
    }
    if (!fpta_is_indexed(i->column->shove)) {
      i->error = FPTA_NO_INDEX;
      continue;
    }

    MDBX_dbi tbl_handle, idx_handle;
    rc = fpta_open_column(txn, i->column, tbl_handle, idx_handle);
    if (unlikely(rc != FPTA_SUCCESS)) {
      i->error = rc;
      continue;
    }

    fpta_key begin_key;
    if (i->begin.type != fpta_begin) {
      rc = fpta_index_value2key(i->column->shove, i->begin, begin_key);
      if (unlikely(rc != FPTA_SUCCESS)) {
        i->error = rc;
        continue;
      }
    }

    fpta_key end_key;
    if (i->end.type != fpta_end) {
      rc = fpta_index_value2key(i->column->shove, i->end, end_key);
      if (unlikely(rc != FPTA_SUCCESS)) {
        i->error = rc;
        continue;
      }
    }

    ptrdiff_t items = 0xDEADBEEF;
    if (i->begin.type == fpta_begin && i->end.type == fpta_end) {
      // entire table
      rc = mdbx_estimate_range(txn->mdbx_txn, idx_handle, nullptr, nullptr,
                               nullptr, nullptr, &items);
    } else if (i->begin.type != fpta_begin && i->end.type != fpta_end &&
               fpta_is_same(begin_key.mdbx, end_key.mdbx)) {
      // single value
      rc = mdbx_estimate_range(txn->mdbx_txn, idx_handle, &begin_key.mdbx,
                               nullptr, &begin_key.mdbx, nullptr, &items);
    } else {
      if (fpta_index_is_unordered(i->column->shove))
        continue;
      // range (maybe inverted)
      rc = mdbx_estimate_range(
          txn->mdbx_txn, idx_handle,
          (i->begin.type != fpta_begin) ? &begin_key.mdbx : nullptr, nullptr,
          (i->end.type != fpta_end) ? &end_key.mdbx : nullptr, nullptr, &items);
    }

    if (unlikely(rc != FPTA_SUCCESS)) {
      i->error = rc;
      if (rc == FPTA_NOTFOUND)
        i->items = 0;
      continue;
    }

    assert(items >= 0);
    i->items = items;
    i->error = FPTA_SUCCESS;
  }

  return FPTA_SUCCESS;
}
