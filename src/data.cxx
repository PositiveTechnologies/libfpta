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

/*FPTA_API*/ const fpta_fp32_t fpta_fp32_denil = {FPTA_DENIL_FP32_BIN};
/*FPTA_API*/ const fpta_fp32_t fpta_fp32_qsnan = {FPTA_QSNAN_FP32_BIN};
/*FPTA_API*/ const fpta_fp64_t fpta_fp64_denil = {FPTA_DENIL_FP64_BIN};
/*FPTA_API*/ const fpta_fp64_t fpta_fp32x64_denil = {FPTA_DENIL_FP32x64_BIN};
/*FPTA_API*/ const fpta_fp64_t fpta_fp32x64_qsnan = {FPTA_QSNAN_FP32x64_BIN};

/* Подставляется в качестве адреса для ключей нулевой длины,
 * с тем чтобы отличать от nullptr */
const char fpta_NIL = '\0';

//----------------------------------------------------------------------------

static fpta_value fpta_field2value_ex(const fptu_field *field,
                                      const fpta_index_type index) {
  fpta_value result = {fpta_null, 0, {0}};

  if (unlikely(!field))
    return result;

  const fptu_payload *payload = field->payload();
  switch (field->type()) {
  default:
  case fptu_nested:
    result.binary_length = unsigned(payload->varlen_brutto_size());
    result.binary_data = const_cast<void *>(static_cast<const void *>(payload));
    result.type = fpta_binary;
    break;

  case fptu_opaque:
    result.binary_length = unsigned(payload->varlen_opaque_bytes());
    result.binary_data = const_cast<void *>(payload->inner_begin());
    result.type = fpta_binary;
    break;

  case fptu_null /* here is not a composite, but invalid */:
    break;

  case fptu_uint16:
    if (fpta_is_indexed_and_nullable(index)) {
      const uint_fast16_t denil = numeric_traits<fptu_uint16>::denil(index);
      if (FPTA_CLEAN_DENIL && unlikely(field->get_payload_uint16() == denil))
        break;
      assert(field->get_payload_uint16() != denil);
      (void)denil;
    }
    result.type = fpta_unsigned_int;
    result.uint = field->get_payload_uint16();
    break;

  case fptu_int32:
    if (fpta_is_indexed_and_nullable(index)) {
      const int_fast32_t denil = numeric_traits<fptu_int32>::denil(index);
      if (FPTA_CLEAN_DENIL && unlikely(payload->peek_i32() == denil))
        break;
      assert(payload->peek_i32() != denil);
      (void)denil;
    }
    result.type = fpta_signed_int;
    result.sint = payload->peek_i32();
    break;

  case fptu_uint32:
    if (fpta_is_indexed_and_nullable(index)) {
      const uint_fast32_t denil = numeric_traits<fptu_uint32>::denil(index);
      if (FPTA_CLEAN_DENIL && unlikely(payload->peek_u32() == denil))
        break;
      assert(payload->peek_u32() != denil);
      (void)denil;
    }
    result.type = fpta_unsigned_int;
    result.uint = payload->peek_u32();
    break;

  case fptu_fp32:
    if (fpta_is_indexed_and_nullable(index)) {
      const uint_fast32_t denil = FPTA_DENIL_FP32_BIN;
      if (FPTA_CLEAN_DENIL && unlikely(payload->peek_u32() == denil))
        break;
      assert(fpta_fp32_denil.__i == FPTA_DENIL_FP32_BIN);
      assert(binary_ne(payload->peek_fp32(), fpta_fp32_denil.__f));
      (void)denil;
    }
    result.type = fpta_float_point;
    result.fp = payload->peek_fp32();
    break;

  case fptu_int64:
    if (fpta_is_indexed_and_nullable(index)) {
      const int64_t denil = numeric_traits<fptu_int64>::denil(index);
      if (FPTA_CLEAN_DENIL && unlikely(payload->peek_i64() == denil))
        break;
      assert(payload->peek_i64() != denil);
      (void)denil;
    }
    result.type = fpta_signed_int;
    result.sint = payload->peek_i64();
    break;

  case fptu_uint64:
    if (fpta_is_indexed_and_nullable(index)) {
      const uint64_t denil = numeric_traits<fptu_uint64>::denil(index);
      if (FPTA_CLEAN_DENIL && unlikely(payload->peek_u64() == denil))
        break;
      assert(payload->peek_u64() != denil);
      (void)denil;
    }
    result.type = fpta_unsigned_int;
    result.uint = payload->peek_u64();
    break;

  case fptu_fp64:
    if (fpta_is_indexed_and_nullable(index)) {
      const uint64_t denil = FPTA_DENIL_FP64_BIN;
      if (FPTA_CLEAN_DENIL && unlikely(payload->peek_u64() == denil))
        break;
      assert(fpta_fp64_denil.__i == FPTA_DENIL_FP64_BIN);
      assert(binary_ne(payload->peek_fp64(), fpta_fp64_denil.__d));
      (void)denil;
    }
    result.type = fpta_float_point;
    result.fp = payload->peek_fp64();
    break;

  case fptu_datetime:
    if (fpta_is_indexed_and_nullable(index)) {
      const uint64_t denil = FPTA_DENIL_DATETIME_BIN;
      if (FPTA_CLEAN_DENIL && unlikely(payload->peek_u64() == denil))
        break;
      assert(payload->peek_u64() != denil);
      (void)denil;
    }
    result.type = fpta_datetime;
    result.datetime.fixedpoint = payload->peek_u64();
    break;

  case fptu_96:
    if (fpta_is_indexed_and_nullable(index)) {
      if (FPTA_CLEAN_DENIL && is_fixbin_denil<fptu_96>(index, payload->fixbin))
        break;
      // coverity[overrun-call : FALSE]
      assert(check_fixbin_not_denil(index, payload, 96 / 8));
    }
    result.type = fpta_binary;
    result.binary_length = 96 / 8;
    result.binary_data = (void *)payload->fixbin;
    break;

  case fptu_128:
    if (fpta_is_indexed_and_nullable(index)) {
      if (FPTA_CLEAN_DENIL && is_fixbin_denil<fptu_128>(index, payload->fixbin))
        break;
      // coverity[overrun-call : FALSE]
      assert(check_fixbin_not_denil(index, payload, 128 / 8));
    }
    result.type = fpta_binary;
    result.binary_length = 128 / 8;
    result.binary_data = (void *)payload->fixbin;
    break;

  case fptu_160:
    if (fpta_is_indexed_and_nullable(index)) {
      if (FPTA_CLEAN_DENIL && is_fixbin_denil<fptu_160>(index, payload->fixbin))
        break;
      // coverity[overrun-call : FALSE]
      assert(check_fixbin_not_denil(index, payload, 160 / 8));
    }
    result.type = fpta_binary;
    result.binary_length = 160 / 8;
    result.binary_data = (void *)payload->fixbin;
    break;

  case fptu_256:
    if (fpta_is_indexed_and_nullable(index)) {
      if (FPTA_CLEAN_DENIL && is_fixbin_denil<fptu_256>(index, payload->fixbin))
        break;
      // coverity[overrun-call : FALSE]
      assert(check_fixbin_not_denil(index, payload, 256 / 8));
    }
    result.type = fpta_binary;
    result.binary_length = 256 / 8;
    result.binary_data = (void *)payload->fixbin;
    break;

  case fptu_cstr:
    result.type = fpta_string;
    result.str = payload->cstr;
    result.binary_length = (unsigned)strlen(result.str);
    break;
  }
  return result;
}

fpta_value fpta_field2value(const fptu_field *field) {
  return fpta_field2value_ex(field, fpta_index_none);
}

int fpta_get_column(fptu_ro row, const fpta_name *column_id,
                    fpta_value *value) {
  if (unlikely(value == nullptr))
    return FPTA_EINVAL;
  int rc = fpta_id_validate(column_id, fpta_column_with_schema);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;
  if (unlikely(fpta_column_is_composite(column_id)))
    return FPTA_EINVAL;

  const fptu_field *field =
      fptu::lookup(row, column_id->column.num, fpta_name_coltype(column_id));
  *value = fpta_field2value_ex(field, fpta_name_colindex(column_id));
  return field ? FPTA_SUCCESS : FPTA_NODATA;
}

int fpta_get_column2buffer(fptu_ro row, const fpta_name *column_id,
                           fpta_value *value, void *buffer,
                           size_t buffer_length) {
  if (unlikely(value == nullptr))
    return FPTA_EINVAL;
  int rc = fpta_id_validate(column_id, fpta_column_with_schema);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;
  if (unlikely(buffer == nullptr && buffer_length))
    return FPTA_EINVAL;

  if (fpta_column_is_composite(column_id)) {
    static_assert(sizeof(fpta_key) == fpta_keybuf_len, "expect equal");
    if (unlikely(buffer_length < sizeof(fpta_key))) {
      value->binary_length = sizeof(fpta_key);
      value->type = fpta_invalid;
      value->binary_data = nullptr;
      return FPTA_DATALEN_MISMATCH;
    }

    fpta_key *key = (fpta_key *)buffer;
    const fpta_table_schema *table_schema =
        column_id->column.table->table_schema;
    rc = fpta_composite_row2key(table_schema, column_id->column.num, row, *key);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;

    value->type = fpta_shoved;
    value->binary_length = (unsigned)key->mdbx.iov_len;
    value->binary_data = key->mdbx.iov_base;
    return FPTA_SUCCESS;
  }

  const fptu_field *field =
      fptu::lookup(row, column_id->column.num, fpta_name_coltype(column_id));
  *value = fpta_field2value_ex(field, fpta_name_colindex(column_id));
  if (unlikely(field == nullptr))
    return FPTA_NODATA;

  if (value->type >= fpta_string) {
    assert(value->type <= fpta_binary);
    unsigned needed_bytes = value->binary_length +
                            (fptu_cstr == fpta_name_coltype(column_id) ? 1 : 0);
    assert((value->type == fpta_string) ==
           (fptu_cstr == fpta_name_coltype(column_id)));
    if (unlikely(needed_bytes > buffer_length)) {
      value->binary_length = needed_bytes;
      value->type = fpta_invalid;
      value->binary_data = nullptr;
      return FPTA_DATALEN_MISMATCH;
    }

    if (likely(needed_bytes > 0))
      value->binary_data = memcpy(buffer, value->binary_data, needed_bytes);
  }
  return FPTA_SUCCESS;
}

int fpta_upsert_column(fptu_rw *pt, const fpta_name *column_id,
                       fpta_value value) {
  return fpta_upsert_column_ex(pt, column_id, value,
                               !FPTA_PROHIBIT_UPSERT_DENIL);
}

int fpta_upsert_column_ex(fptu_rw *pt, const fpta_name *column_id,
                          fpta_value value, bool erase_on_denil) {
  if (unlikely(!pt))
    return FPTA_EINVAL;
  int rc = fpta_id_validate(column_id, fpta_column_with_schema);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  const unsigned colnum = column_id->column.num;
  assert(colnum <= fpta_max_cols);
  const fptu_type coltype = fpta_shove2type(column_id->shove);
  const fpta_index_type index = fpta_name_colindex(column_id);

  if (unlikely(value.type == fpta_null))
    goto erase_field;

  switch (coltype) {
  default:
    /* TODO: проверить корректность размера для fptu_farray */
    if (unlikely(value.type != fpta_binary))
      return FPTA_ETYPE;
    return FPTA_ENOIMP;

  case fptu_nested: {
    fptu_ro tuple;
    if (unlikely(value.type != fpta_binary))
      return FPTA_ETYPE;
    tuple.sys.iov_len = value.binary_length;
    tuple.sys.iov_base = value.binary_data;
    return fptu_upsert_nested(pt, colnum, tuple);
  }

  case fptu_opaque:
    if (unlikely(value.type != fpta_binary))
      return FPTA_ETYPE;
    return fptu_upsert_opaque(pt, colnum, value.binary_data,
                              value.binary_length);

  case fptu_null /* composite */:
    return FPTA_EINVAL;

  case fptu_uint16:
    switch (value.type) {
    default:
      return FPTA_ETYPE;
    case fpta_signed_int:
      if (unlikely(value.sint < 0))
        return FPTA_EVALUE;
      __fallthrough;
    case fpta_unsigned_int:
      if (fpta_is_indexed_and_nullable(index)) {
        const uint_fast16_t denil = numeric_traits<fptu_uint16>::denil(index);
        if (unlikely(value.uint == denil))
          goto denil_catched;
      }
    }
    if (unlikely(value.uint > UINT16_MAX))
      return FPTA_EVALUE;
    return fptu_upsert_uint16(pt, colnum, (uint_fast16_t)value.uint);

  case fptu_int32:
    switch (value.type) {
    default:
      return FPTA_ETYPE;
    case fpta_unsigned_int:
      if (unlikely(value.uint > INT32_MAX))
        return FPTA_EVALUE;
      __fallthrough;
    case fpta_signed_int:
      if (fpta_is_indexed_and_nullable(index)) {
        const int_fast32_t denil = numeric_traits<fptu_int32>::denil(index);
        if (unlikely(value.sint == denil))
          goto denil_catched;
      }
    }
    if (unlikely(value.sint != (int32_t)value.sint))
      return FPTA_EVALUE;
    return fptu_upsert_int32(pt, colnum, (int_fast32_t)value.sint);

  case fptu_uint32:
    switch (value.type) {
    default:
      return FPTA_ETYPE;
    case fpta_signed_int:
      if (unlikely(value.sint < 0))
        return FPTA_EVALUE;
      __fallthrough;
    case fpta_unsigned_int:
      if (fpta_is_indexed_and_nullable(index)) {
        const uint_fast32_t denil = numeric_traits<fptu_uint32>::denil(index);
        if (unlikely(value.uint == denil))
          goto denil_catched;
      }
      if (unlikely(value.uint > UINT32_MAX))
        return FPTA_EVALUE;
    }
    return fptu_upsert_uint32(pt, colnum, (uint_fast32_t)value.uint);

  case fptu_fp32: {
    if (unlikely(value.type != fpta_float_point))
      return FPTA_ETYPE;
    if (fpta_is_indexed_and_nullable(index) &&
        /* LY: проверяем на DENIL с учетом усечения при конвертации во float */
        unlikely(value.uint >= FPTA_DENIL_FP32x64_BIN)) {
      if (value.uint == FPTA_DENIL_FP32x64_BIN)
        goto denil_catched;
      /* LY: подставляем значение, которое не даст FPTA_DENIL_FP32
       * при конвертации во float */
      value.uint = FPTA_QSNAN_FP32x64_BIN;
    }
    const auto fpc(erthink::fpclassify_from_uint(value.uint));
    if (unlikely(fpc.is_nan())) {
      if (FPTA_PROHIBIT_UPSERT_NAN)
        return FPTA_EVALUE;
    } else if (!std::is_same<decltype(value.fp), float>::value &&
               unlikely(std::abs(value.fp) > FLT_MAX) && !fpc.is_infinity())
      return FPTA_EVALUE;
    return fptu_upsert_fp32(pt, colnum, float(value.fp));
  }

  case fptu_int64:
    switch (value.type) {
    default:
      return FPTA_ETYPE;
    case fpta_unsigned_int:
      if (unlikely(value.uint > INT64_MAX))
        return FPTA_EVALUE;
      __fallthrough;
    case fpta_signed_int:
      if (fpta_is_indexed_and_nullable(index)) {
        const int64_t denil = numeric_traits<fptu_int64>::denil(index);
        if (unlikely(value.sint == denil))
          goto denil_catched;
      }
    }
    return fptu_upsert_int64(pt, colnum, value.sint);

  case fptu_uint64:
    switch (value.type) {
    default:
      return FPTA_ETYPE;
    case fpta_signed_int:
      if (unlikely(value.sint < 0))
        return FPTA_EVALUE;
      __fallthrough;
    case fpta_unsigned_int:
      if (fpta_is_indexed_and_nullable(index)) {
        const uint64_t denil = numeric_traits<fptu_uint64>::denil(index);
        if (unlikely(value.uint == denil))
          goto denil_catched;
      }
    }
    return fptu_upsert_uint64(pt, colnum, value.uint);

  case fptu_fp64: {
    if (unlikely(value.type != fpta_float_point))
      return FPTA_ETYPE;
    if (fpta_is_indexed_and_nullable(index)) {
      const uint64_t denil = FPTA_DENIL_FP64_BIN;
      if (unlikely(value.uint == denil))
        goto denil_catched;
    }
    const auto fpc(erthink::fpclassify_from_uint(value.uint));
    if (unlikely(fpc.is_nan())) {
      if (FPTA_PROHIBIT_UPSERT_NAN)
        return FPTA_EVALUE;
    } else if (!std::is_same<decltype(value.fp), double>::value &&
               unlikely(std::abs(value.fp) > DBL_MAX) && !fpc.is_infinity())
      return FPTA_EVALUE;
    return fptu_upsert_fp64(pt, colnum, value.fp);
  }

  case fptu_datetime:
    if (value.type != fpta_datetime)
      return FPTA_ETYPE;
    if (fpta_is_indexed_and_nullable(index)) {
      const uint64_t denil = FPTA_DENIL_DATETIME_BIN;
      if (unlikely(value.datetime.fixedpoint == denil))
        goto denil_catched;
    }
    return fptu_upsert_datetime(pt, colnum, value.datetime);

  case fptu_96:
    if (unlikely(value.type != fpta_binary))
      return FPTA_ETYPE;
    if (unlikely(value.binary_length != 96 / 8))
      return FPTA_DATALEN_MISMATCH;
    if (unlikely(!value.binary_data))
      return FPTA_EINVAL;
    if (fpta_is_indexed_and_nullable(index) &&
        is_fixbin_denil<fptu_96>(index, value.binary_data))
      goto denil_catched;
    return fptu_upsert_96(pt, colnum, value.binary_data);

  case fptu_128:
    if (unlikely(value.type != fpta_binary))
      return FPTA_ETYPE;
    if (unlikely(value.binary_length != 128 / 8))
      return FPTA_DATALEN_MISMATCH;
    if (unlikely(!value.binary_data))
      return FPTA_EINVAL;
    if (fpta_is_indexed_and_nullable(index) &&
        is_fixbin_denil<fptu_128>(index, value.binary_data))
      goto denil_catched;
    return fptu_upsert_128(pt, colnum, value.binary_data);

  case fptu_160:
    if (unlikely(value.type != fpta_binary))
      return FPTA_ETYPE;
    if (unlikely(value.binary_length != 160 / 8))
      return FPTA_DATALEN_MISMATCH;
    if (unlikely(!value.binary_data))
      return FPTA_EINVAL;
    if (fpta_is_indexed_and_nullable(index) &&
        is_fixbin_denil<fptu_160>(index, value.binary_data))
      goto denil_catched;
    return fptu_upsert_160(pt, colnum, value.binary_data);

  case fptu_256:
    if (unlikely(value.type != fpta_binary))
      return FPTA_ETYPE;
    if (unlikely(value.binary_length != 256 / 8))
      return FPTA_DATALEN_MISMATCH;
    if (unlikely(!value.binary_data))
      return FPTA_EINVAL;
    if (fpta_is_indexed_and_nullable(index) &&
        is_fixbin_denil<fptu_160>(index, value.binary_data))
      goto denil_catched;
    return fptu_upsert_256(pt, colnum, value.binary_data);

  case fptu_cstr:
    if (unlikely(value.type != fpta_string))
      return FPTA_ETYPE;
    return fptu_upsert_string(pt, colnum, value.str, value.binary_length);
  }

denil_catched:
  if (!erase_on_denil)
    return FPTA_EVALUE;

erase_field:
  rc = fptu::erase(pt, colnum, fptu_any);
  assert(rc >= 0);
  (void)rc;
  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_validate_put(fpta_txn *txn, fpta_name *table_id, fptu_ro row_value,
                      fpta_put_options op) {
  if (unlikely(op < fpta_insert ||
               op > (fpta_upsert | fpta_skip_nonnullable_check)))
    return FPTA_EFLAG;

  int rc = fpta_name_refresh_couple(txn, table_id, nullptr);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_table_schema *table_def = table_id->table_schema;
  fpta_key pk_key;
  rc = fpta_index_row2key(table_def, 0, row_value, pk_key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (op & fpta_skip_nonnullable_check)
    op = (fpta_put_options)(op - fpta_skip_nonnullable_check);
  else {
    rc = fpta_check_nonnullable(table_def, row_value);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
  }

  MDBX_dbi handle;
  rc = fpta_open_table(txn, table_def, handle);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fptu_ro present_row;
  size_t rows_with_same_key;
  rc = mdbx_get_ex(txn->mdbx_txn, handle, &pk_key.mdbx, &present_row.sys,
                   &rows_with_same_key);
  if (rc != MDBX_SUCCESS) {
    if (unlikely(rc != MDBX_NOTFOUND))
      return rc;
    present_row.sys.iov_base = nullptr;
    present_row.sys.iov_len = 0;
  }

  switch (op) {
  default:
    assert(false && "unreachable");
    __unreachable();
    return FPTA_EOOPS;
  case fpta_insert:
    if (fpta_index_is_unique(table_def->table_pk())) {
      if (present_row.sys.iov_base)
        /* запись с таким PK уже есть, вставка НЕ возможна */
        return FPTA_KEYEXIST;
    }
    break;

  case fpta_update:
    if (!present_row.sys.iov_base)
      /* нет записи с таким PK, обновлять нечего */
      return FPTA_NOTFOUND;
    /* no break here */
    __fallthrough;
  case fpta_upsert:
    if (rows_with_same_key > 1)
      /* обновление НЕ возможно, если первичный ключ НЕ уникален */
      return FPTA_KEYEXIST;
  }

  if (present_row.sys.iov_base) {
    if (present_row.total_bytes == row_value.total_bytes &&
        !memcmp(present_row.units, row_value.units, present_row.total_bytes))
      /* если полный дубликат записи */
      return (op == fpta_insert) ? FPTA_KEYEXIST : FPTA_SUCCESS;
  }

  if (!table_def->has_secondary())
    return FPTA_SUCCESS;

  return fpta_check_secondary_uniq(txn, table_def, present_row, row_value, 0);
}

int fpta_put(fpta_txn *txn, fpta_name *table_id, fptu_ro row,
             fpta_put_options op) {
  int rc = fpta_name_refresh_couple(txn, table_id, nullptr);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_table_schema *table_def = table_id->table_schema;
  MDBX_put_flags_t flags = MDBX_NODUPDATA;
  switch (op) {
  default:
    return FPTA_EFLAG;
  case fpta_insert:
    if (fpta_index_is_unique(table_def->table_pk()))
      flags |= MDBX_NOOVERWRITE;
    break;
  case fpta_update:
    flags |= MDBX_CURRENT;
    break;
  case fpta_upsert:
    if (!fpta_index_is_unique(table_def->table_pk()))
      flags |= MDBX_NOOVERWRITE;
    break;
  }

  rc = fpta_check_nonnullable(table_def, row);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_key pk_key;
  rc = fpta_index_row2key(table_def, 0, row, pk_key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDBX_dbi handle;
  rc = fpta_open_table(txn, table_def, handle);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (!table_def->has_secondary())
    return mdbx_put(txn->mdbx_txn, handle, &pk_key.mdbx, &row.sys, flags);

  fptu_ro old_row;
#if defined(NDEBUG)
  cxx11_constexpr_var size_t likely_enough = 64u * 42u;
#else
  const size_t likely_enough = (time(nullptr) & 1) ? 11u : 64u * 42u;
#endif /* NDEBUG */
  void *buffer = alloca(likely_enough);
  old_row.sys.iov_base = buffer;
  old_row.sys.iov_len = likely_enough;

  rc = mdbx_replace(txn->mdbx_txn, handle, &pk_key.mdbx, &row.sys, &old_row.sys,
                    flags);
  if (unlikely(rc == MDBX_RESULT_TRUE)) {
    assert(old_row.sys.iov_base == nullptr &&
           old_row.sys.iov_len > likely_enough);
    old_row.sys.iov_base = alloca(old_row.sys.iov_len);
    rc = mdbx_replace(txn->mdbx_txn, handle, &pk_key.mdbx, &row.sys,
                      &old_row.sys, flags);
  }
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = fpta_secondary_upsert(txn, table_def, pk_key.mdbx, old_row, pk_key.mdbx,
                             row, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    return fpta_internal_abort(txn, rc);

  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_delete(fpta_txn *txn, fpta_name *table_id, fptu_ro row) {
  int rc = fpta_name_refresh_couple(txn, table_id, nullptr);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_table_schema *table_def = table_id->table_schema;
  if (row.sys.iov_len && table_def->has_secondary() &&
      mdbx_is_dirty(txn->mdbx_txn, row.sys.iov_base)) {
    /* LY: Делаем копию строки, так как удаление в основной таблице
     * уничтожит текущее значение при перезаписи "грязной" страницы.
     * Соответственно, будут утрачены значения необходимые для чистки
     * вторичных индексов.
     *
     * FIXME: На самом деле можно не делать копию, а просто почистить
     * вторичные индексы перед удалением из основной таблице. Однако,
     * при этом сложно правильно обрабатывать ошибки. Оптимальным же будет
     * такой вариант:
     *  - открываем mdbx-курсор и устанавливаем его на удаляемую строку;
     *  - при этом обрабатываем ситуацию отсутствия удаляемой строки;
     *  - затем чистим вторичные индексы, при этом любая ошибка должна
     *    обрабатываться также как сейчас;
     *  - в конце удаляем строку из главной таблицы.
     * Но для этого варианта нужен API быстрого (inplace) открытия курсора,
     * без выделения памяти. Иначе накладные расходы будут больше экономии. */
    void *buffer = alloca(row.sys.iov_len);
    row.sys.iov_base = memcpy(buffer, row.sys.iov_base, row.sys.iov_len);
  }

  fpta_key key;
  rc = fpta_index_row2key(table_def, 0, row, key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDBX_dbi handle;
  rc = fpta_open_table(txn, table_def, handle);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  rc = mdbx_del(txn->mdbx_txn, handle, &key.mdbx, &row.sys);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (table_def->has_secondary()) {
    rc = fpta_secondary_remove(txn, table_def, key.mdbx, row, 0);
    if (unlikely(rc != MDBX_SUCCESS))
      return fpta_internal_abort(txn, rc);
  }

  return FPTA_SUCCESS;
}

int fpta_get(fpta_txn *txn, fpta_name *column_id,
             const fpta_value *column_value, fptu_ro *row) {
  if (unlikely(row == nullptr))
    return FPTA_EINVAL;

  row->units = nullptr;
  row->total_bytes = 0;

  if (unlikely(column_value == nullptr))
    return FPTA_EINVAL;
  int rc = fpta_id_validate(column_id, fpta_column);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_name *table_id = column_id->column.table;
  rc = fpta_name_refresh_couple(txn, table_id, column_id);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(!fpta_is_indexed(column_id->shove)))
    return FPTA_NO_INDEX;

  const fpta_index_type index = fpta_shove2index(column_id->shove);
  if (unlikely(!fpta_index_is_unique(index)))
    return FPTA_NO_INDEX;

  fpta_key column_key;
  rc = fpta_index_value2key(column_id->shove, *column_value, column_key, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDBX_dbi tbl_handle, idx_handle;
  rc = fpta_open_column(txn, column_id, tbl_handle, idx_handle);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (fpta_index_is_primary(index))
    return mdbx_get(txn->mdbx_txn, idx_handle, &column_key.mdbx, &row->sys);

  MDBX_val pk_key;
  rc = mdbx_get(txn->mdbx_txn, idx_handle, &column_key.mdbx, &pk_key);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = mdbx_get(txn->mdbx_txn, tbl_handle, &pk_key, &row->sys);
  if (unlikely(rc == MDBX_NOTFOUND))
    return FPTA_INDEX_CORRUPTED;

  return rc;
}
