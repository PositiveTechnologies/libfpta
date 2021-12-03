/*
 *  Fast Positive Tables (libfpta), aka Позитивные Таблицы.
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

#include "details.h"

static inline bool fpta_cmp_is_compat(fptu_type data_type,
                                      fpta_value_type value_type) {
  /* Смысл функции в отбраковке фильтров, в которых используются сравнения
   * дающие постоянный результат, не зависящий от данных. Критерий сравнимости:
   *  - недопустимо сравнение для:
   *     1. составных колонок (data_type == fptu_null) с чем-либо
   *     2. специальных значений fpta_begin, fpta_end, fpta_epsilon с чем-либо;
   *     3. чисел с не-числами;
   *  - допускается сравнивать:
   *     1. взаимо-однозначные типы;
   *     2. числа с числами в любой комбинации (со знаком,
   *        беззнаковые, целые, с плавающей точкой);
   *  - ограниченные/дополнительные/специальные случаи:
   *     1. fpta_null == fptu_opaque нулевой длины;
   *     2. fpta_string == fptu_opaque, при по-байтном равенстве;
   *     3. fpta_binary == fptu_cstr, при по-байтном равенстве;
   *     4. fpta_binary == fptu_96/fptu_128/fptu_160/fptu_256,
   *        при по-байтном равенстве
   *     5. fpta_shoved трактуется как fpta_binary, т.е. они равнозначны;
   */
  if (unlikely(value_type > fpta_shoved))
    return false;

  static const int32_t bits[fpta_shoved + 1] = {
      /* fpta_null */
      1 << fptu_opaque,

      /* fpta_signed_int */
      fptu_any_number,

      /* fpta_unsigned_int */
      fptu_any_number,

      /* fpta_datetime */
      1 << fptu_datetime,

      /* fpta_float_point */
      fptu_any_number,

      /* fpta_string */
      1 << fptu_cstr | 1 << fptu_opaque,

      /* fpta_binary */
      1 << fptu_96 | 1 << fptu_128 | 1 << fptu_160 | 1 << fptu_256 |
          1 << fptu_cstr | 1 << fptu_opaque | 1 << fptu_nested,

      /* fpta_shoved */
      1 << fptu_96 | 1 << fptu_128 | 1 << fptu_160 | 1 << fptu_256 |
          1 << fptu_cstr | 1 << fptu_opaque | 1 << fptu_nested,
  };

  return (bits[value_type] & (1 << data_type)) != 0;
}

static __hot fptu_lge fpta_cmp_null(const fptu_field *left) {
  const auto payload = left->payload();

  switch (left->type()) {
  case fptu_null /* here is/should not a composite column/index */:
    return fptu_eq;
  case fptu_opaque:
    if (payload->varlen_opaque_bytes() == 0)
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
    return fptu_cmp2lge<int64_t>(payload->peek_u32(), right);

  case fptu_int32:
    return fptu_cmp2lge<int64_t>(payload->peek_i32(), right);

  case fptu_uint64:
    if (right < 0)
      return fptu_gt;
    return fptu_cmp2lge(payload->peek_u64(), uint64_t(right));

  case fptu_int64:
    return fptu_cmp2lge(payload->peek_i64(), right);

  case fptu_fp32:
    return fptu_cmp2lge<double>(payload->peek_fp32(), double(right));

  case fptu_fp64:
    return fptu_cmp2lge<double>(payload->peek_fp64(), double(right));

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
    if (payload->peek_i32() < 0)
      return fptu_lt;
    __fallthrough;
  case fptu_uint32:
    return fptu_cmp2lge<uint64_t>(payload->peek_u32(), right);

  case fptu_int64:
    if (payload->peek_i64() < 0)
      return fptu_lt;
    __fallthrough;
  case fptu_uint64:
    return fptu_cmp2lge(payload->peek_u64(), right);

  case fptu_fp32:
    return fptu_cmp2lge<double>(payload->peek_fp32(), double(right));

  case fptu_fp64:
    return fptu_cmp2lge<double>(payload->peek_fp64(), double(right));

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
    return fptu_cmp2lge<double>(payload->peek_i32(), right);

  case fptu_uint32:
    return fptu_cmp2lge<double>(payload->peek_u32(), right);

  case fptu_int64:
    return fptu_cmp2lge<double>(double(payload->peek_i64()), right);

  case fptu_uint64:
    return fptu_cmp2lge<double>(double(payload->peek_u64()), right);

  case fptu_fp32:
    return fptu_cmp2lge<double>(payload->peek_fp32(), right);

  case fptu_fp64:
    return fptu_cmp2lge(payload->peek_fp64(), right);

  default:
    return fptu_ic;
  }
}

static __hot fptu_lge fpta_cmp_datetime(const fptu_field *left,
                                        const fptu_time right) {

  if (unlikely(left->type() != fptu_datetime))
    return fptu_ic;

  const auto payload = left->payload();
  return fptu_cmp2lge(payload->peek_u64(), right.fixedpoint);
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
    return fptu_cmp_binary(payload->inner_begin(),
                           payload->varlen_opaque_bytes(), right, length);
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
    left_len = payload->varlen_opaque_bytes();
    left_data = payload->inner_begin();
    break;

  case fptu_nested:
    left_len = payload->varlen_brutto_size();
    left_data = payload;
    break;

  default: /* fptu_farray */
    left_len = payload->varlen_netto_size();
    left_data = payload->inner_begin();
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

#if FPTA_ENABLE_TESTS
fptu_lge __fpta_filter_cmp(const fptu_field *pf, const fpta_value *right) {
  return fpta_filter_cmp(pf, *right);
}
#endif /* FPTA_ENABLE_TESTS */

__hot bool fpta_filter_match(const fpta_filter *fn, fptu_ro tuple) {

tail_recursion:

  if (unlikely(fn == nullptr))
    // empty filter
    return true;

  switch (fn->type) {
  case fpta_node_collapsed_true:
  case fpta_node_cond_true:
    return true;
  case fpta_node_collapsed_false:
  case fpta_node_cond_false:
    return false;

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

#define FILTER_PROPAGATE_TRUE (FPTA_ERRROR_LAST + 11)
#define FILTER_PROPAGATE_FALSE (FPTA_ERRROR_LAST + 12)

static int fpta_filter_rewrite_on_error(fpta_filter *filter, int err) {
  assert(err != FPTA_SUCCESS);

  if (err == FILTER_PROPAGATE_FALSE) {
    switch (filter->type) {
    default:
      assert(false);
      return FPTA_EOOPS;

    case fpta_node_not:
      filter->type = fpta_node_collapsed_true;
      return FILTER_PROPAGATE_TRUE;

    case fpta_node_or:
      if (filter->node_or.a->type != fpta_node_collapsed_false &&
          filter->node_or.a->type != fpta_node_cond_false)
        return FPTA_SUCCESS;
      if (filter->node_or.b->type != fpta_node_collapsed_false &&
          filter->node_or.b->type != fpta_node_cond_false)
        return FPTA_SUCCESS;
      /* fallthrough */
      __fallthrough;

    case fpta_node_and:
      filter->type = fpta_node_collapsed_false;
      return FILTER_PROPAGATE_FALSE;
    }
  }

  if (err == FILTER_PROPAGATE_TRUE) {
    switch (filter->type) {
    default:
      assert(false);
      return FPTA_EOOPS;

    case fpta_node_not:
      filter->type = fpta_node_collapsed_false;
      return FILTER_PROPAGATE_FALSE;

    case fpta_node_and:
      if (filter->node_and.a->type != fpta_node_collapsed_true &&
          filter->node_and.a->type != fpta_node_cond_true)
        return FPTA_SUCCESS;
      if (filter->node_and.b->type != fpta_node_collapsed_true &&
          filter->node_and.b->type != fpta_node_cond_true)
        return FPTA_SUCCESS;
      /* fallthrough */
      __fallthrough;

    case fpta_node_or:
      filter->type = fpta_node_collapsed_true;
      return FILTER_PROPAGATE_TRUE;
    }
  }

  return err;
}

int fpta_filter_validate(fpta_filter *filter) {
  int rc;

  switch (filter->type) {
  default:
    return FPTA_EINVAL;

  case fpta_node_collapsed_true:
  case fpta_node_collapsed_false:
  case fpta_node_cond_true:
  case fpta_node_cond_false:
    return FPTA_SUCCESS;

  case fpta_node_fncol:
    rc = fpta_id_validate(filter->node_fncol.column_id, fpta_column);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
    if (unlikely(fpta_column_is_composite(filter->node_fncol.column_id)))
      return FPTA_ETYPE;
    if (unlikely(!filter->node_fncol.predicate))
      return FPTA_EINVAL;
    return FPTA_SUCCESS;

  case fpta_node_fnrow:
    if (unlikely(!filter->node_fnrow.predicate))
      return FPTA_EINVAL;
    return FPTA_SUCCESS;

  case fpta_node_not:
    assert(filter->node_not == filter->node_or.a &&
           filter->node_not == filter->node_and.a &&
           filter->node_or.b == filter->node_and.b);
    if (unlikely(!filter->node_not))
      return FPTA_EINVAL;
    rc = fpta_filter_validate(filter->node_not);
    if (unlikely(rc != FPTA_SUCCESS))
      return fpta_filter_rewrite_on_error(filter, rc);
    return FPTA_SUCCESS;

  case fpta_node_or:
  case fpta_node_and:
    assert(filter->node_or.a == filter->node_and.a &&
           filter->node_or.b == filter->node_and.b);
    if (unlikely(!filter->node_and.a || !filter->node_and.b))
      return FPTA_EINVAL;
    rc = fpta_filter_validate(filter->node_and.a);
    if (unlikely(rc != FPTA_SUCCESS))
      return fpta_filter_rewrite_on_error(filter, rc);
    rc = fpta_filter_validate(filter->node_and.b);
    if (unlikely(rc != FPTA_SUCCESS))
      return fpta_filter_rewrite_on_error(filter, rc);
    return FPTA_SUCCESS;

  case fpta_node_lt:
  case fpta_node_gt:
  case fpta_node_le:
  case fpta_node_ge:
  case fpta_node_eq:
  case fpta_node_ne:
    rc = fpta_id_validate(filter->node_cmp.left_id, fpta_column);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;

    if (unlikely(filter->node_cmp.right_value.type == fpta_null) &&
        fpta_column_is_nullable(filter->node_cmp.left_id->shove))
      return FPTA_SUCCESS;

    if (likely(fpta_cmp_is_compat(fpta_name_coltype(filter->node_cmp.left_id),
                                  filter->node_cmp.right_value.type)))
      return FPTA_SUCCESS;

    if (filter->type != fpta_node_ne) {
      filter->type = fpta_node_cond_false;
      return FILTER_PROPAGATE_FALSE;
    } else {
      filter->type = fpta_node_cond_true;
      return FILTER_PROPAGATE_TRUE;
    }
  }
}

//----------------------------------------------------------------------------

__hot int fpta_name_refresh_filter(fpta_name *table_id, fpta_filter *filter) {
  /* Функция fpta_name_refresh_filter() не доступна пользователю
   * и вызывается только из fpta_cursor_open(). Причем до вызова
   * refresh_filter() вызывается fpta_name_refresh_couple(), которая проверяет
   * транзакцию, table_id и при необходимости подгружает и обновляет схему.
   * Поэтому здесь достаточно fpta_name_refresh_column(), что позволяет
   * избавиться от повторения проверок. */
tail_recursion:
  int rc = FPTA_SUCCESS;
  if (filter) {
    switch (filter->type) {
    default:
      return FPTA_EINVAL;

    case fpta_node_fnrow:
      break;

    case fpta_node_fncol:
      rc = fpta_name_refresh_column(table_id, filter->node_fncol.column_id);
      break;

    case fpta_node_not:
      filter = filter->node_not;
      goto tail_recursion;

    case fpta_node_or:
    case fpta_node_and:
      rc = fpta_name_refresh_filter(table_id, filter->node_and.a);
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
      rc = fpta_name_refresh_column(table_id, filter->node_cmp.left_id);
      break;
    }
  }

  return rc;
}

//----------------------------------------------------------------------------

FPTA_API int fpta_estimate(fpta_txn *txn, unsigned items_count,
                           fpta_estimate_item *items_vector,
                           fpta_cursor_options options) {
  if (unlikely(items_count < 1 || items_count > fpta_max_cols ||
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
