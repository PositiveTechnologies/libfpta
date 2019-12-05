/*
 *  Fast Positive Tables (libfpta), aka Позитивные Таблицы.
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
                           fpta_estimate_item *items_vector,
                           fpta_cursor_options options) {
  if (unlikely(items_count < 1 || items_count > fpta_max_indexes ||
               items_vector == nullptr))
    return FPTA_EINVAL;

  int err = fpta_txn_validate(txn, fpta_read);
  if (unlikely(err != FPTA_SUCCESS))
    return err;

  fpta_estimate_item *const vector_begin = items_vector;
  fpta_estimate_item *const vector_end = items_vector + items_count;
  int rc = FPTA_NODATA;
  for (fpta_estimate_item *i = vector_begin; i != vector_end; ++i) {
    i->estimated_rows = PTRDIFF_MAX;
    err = fpta_id_validate(i->column_id, fpta_column);
    if (unlikely(err != FPTA_SUCCESS)) {
      i->error = err;
      continue;
    }
    err = fpta_name_refresh(txn, i->column_id);
    if (unlikely(err != FPTA_SUCCESS)) {
      i->error = err;
      continue;
    }

    if (unlikely(!fpta_is_indexed(i->column_id->shove))) {
      i->error = FPTA_NO_INDEX;
      continue;
    }

    MDBX_dbi tbl_handle, idx_handle;
    /* Не стоит открывать хендлы до проверки всех аргументов, однако:
        - раннее открытие не создает заметных пользователю сторонних эффектов;
        - упрощает код, устраняя дублирование и условные переходы. */
    err = fpta_open_column(txn, i->column_id, tbl_handle, idx_handle);
    if (unlikely(err != FPTA_SUCCESS)) {
      i->error = err;
      continue;
    }

    fpta_key begin_key;
    MDBX_val *mdbx_begin_key;
    switch (i->range_from.type) {
    case fpta_begin:
      mdbx_begin_key = nullptr;
      break;

    case fpta_epsilon:
      if (unlikely(i->range_to.type == fpta_epsilon)) {
        i->error = FPTA_EINVAL;
        continue;
      }
      mdbx_begin_key = MDBX_EPSILON;
      break;

    default:
      err = fpta_index_value2key(i->column_id->shove, i->range_from, begin_key);
      if (unlikely(err != FPTA_SUCCESS)) {
        i->error = err;
        continue;
      }
      mdbx_begin_key = &begin_key.mdbx;
      break;
    }

    fpta_key end_key;
    MDBX_val *mdbx_end_key;
    switch (i->range_to.type) {
    case fpta_end:
      mdbx_end_key = nullptr;
      break;

    case fpta_epsilon:
      assert(i->range_from.type != fpta_epsilon);
      mdbx_end_key = MDBX_EPSILON;
      break;

    default:
      err = fpta_index_value2key(i->column_id->shove, i->range_to, end_key);
      if (unlikely(err != FPTA_SUCCESS)) {
        i->error = err;
        continue;
      }
      mdbx_end_key = &end_key.mdbx;
      break;
    }

    if (i->range_from.type <= fpta_shoved && i->range_to.type <= fpta_shoved) {
      if (fpta_is_same(begin_key.mdbx, end_key.mdbx)) {
        /* range_from == range_to */
        if ((options & fpta_zeroed_range_is_point) == 0) {
          i->estimated_rows = 0;
          i->error = FPTA_SUCCESS;
          continue;
        }
      } else if (fpta_index_is_unordered(i->column_id->shove)) {
        i->error = FPTA_NO_INDEX;
        continue;
      }
    }

    err =
        mdbx_estimate_range(txn->mdbx_txn, idx_handle, mdbx_begin_key, nullptr,
                            mdbx_end_key, nullptr, &i->estimated_rows);
    if (unlikely(err != FPTA_SUCCESS)) {
      i->error = err;
      continue;
    }

    i->error = rc = FPTA_SUCCESS;
  }

  return rc;
}
