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
#include "externals/libfptu/src/erthink/erthink_casting.h"

//----------------------------------------------------------------------------

static __hot int fpta_normalize_key(const fpta_index_type index, fpta_key &key,
                                    bool copy) {
  static_assert(fpta_max_keylen % sizeof(uint64_t) == 0,
                "wrong fpta_max_keylen");

  assert(key.mdbx.iov_base != &key.place);
  if (unlikely(key.mdbx.iov_base == nullptr) && key.mdbx.iov_len)
    return FPTA_EINVAL;

  if (fpta_index_is_unordered(index)) {
    // хешируем ключ для неупорядоченного индекса
    key.place.u64 = t1ha2_atonce(key.mdbx.iov_base, key.mdbx.iov_len, 2018);
    key.mdbx.iov_base = &key.place.u64;
    key.mdbx.iov_len = sizeof(key.place.u64);
    return FPTA_SUCCESS;
  }

  static_assert(fpta_max_keylen == sizeof(key.place.longkey_obverse.head),
                "something wrong");
  static_assert(fpta_max_keylen == sizeof(key.place.longkey_reverse.tail),
                "something wrong");

  //--------------------------------------------------------------------------

  if (fpta_is_indexed_and_nullable(index)) {
    /* Чтобы отличать NIL от ключей нулевой длины и при этом сохранить
     * упорядоченность для всех ключей - нужно дополнить ключ префиксом
     * в порядка сравнения байтов ключа.
     *
     * Для этого ключ придется копировать, а при превышении (с учетом
     * добавленного префикса) лимита fpta_max_keylen также выполнить
     * подрезку и дополнение хэш-значением.
     */
    if (likely(key.mdbx.iov_len < fpta_max_keylen)) {
      /* ключ (вместе с префиксом) не слишком длинный, дополнение
       * хешем не нужно, просто добавляем префикс и копируем ключ. */
      uint8_t *nillable = (uint8_t *)&key.place;
      if (fpta_index_is_obverse(index)) {
        *nillable = fpta_notnil_prefix_byte;
        nillable += fpta_notnil_prefix_length;
      } else {
        nillable[key.mdbx.iov_len] = fpta_notnil_prefix_byte;
      }
      memcpy(nillable, key.mdbx.iov_base, key.mdbx.iov_len);
      key.mdbx.iov_len += fpta_notnil_prefix_length;
      key.mdbx.iov_base = &key.place;
      return FPTA_SUCCESS;
    }

    const size_t chunk = fpta_max_keylen - unsigned(fpta_notnil_prefix_length);
    if (fpta_index_is_obverse(index)) {
      /* ключ сравнивается от головы к хвосту (как memcpy),
       * копируем начало и хэшируем хвост. */
      uint8_t *nillable = (uint8_t *)&key.place.longkey_obverse.head;
      *nillable = fpta_notnil_prefix_byte;
      nillable += fpta_notnil_prefix_length;
      memcpy(nillable, key.mdbx.iov_base, chunk);
      key.place.longkey_obverse.tailhash =
          t1ha2_atonce((const uint8_t *)key.mdbx.iov_base + chunk,
                       key.mdbx.iov_len - chunk, 0);
    } else {
      /* ключ сравнивается от хвоста к голове,
       * копируем хвост и хэшируем начало. */
      uint8_t *nillable = (uint8_t *)&key.place.longkey_reverse.tail;
      nillable[chunk] = fpta_notnil_prefix_byte;
      memcpy(nillable,
             (const uint8_t *)key.mdbx.iov_base + key.mdbx.iov_len - chunk,
             chunk);
      key.place.longkey_reverse.headhash = t1ha2_atonce(
          (const uint8_t *)key.mdbx.iov_base, key.mdbx.iov_len - chunk, 0);
    }
    key.mdbx.iov_len = sizeof(key.place);
    key.mdbx.iov_base = &key.place;
    return FPTA_SUCCESS;
  }

  //--------------------------------------------------------------------------

  if (likely(key.mdbx.iov_len <= fpta_max_keylen)) {
    /* ключ не слишком длинный, делаем копию только если запрошено */
    if (copy && key.mdbx.iov_len)
      key.mdbx.iov_base =
          memcpy(&key.place, key.mdbx.iov_base, key.mdbx.iov_len);
    return FPTA_SUCCESS;
  }

  /* ключ слишком большой, сохраняем сколько допустимо остальное хэшируем */
  if (fpta_index_is_obverse(index)) {
    /* ключ сравнивается от головы к хвосту (как memcpy),
     * копируем начало и хэшируем хвост. */
    memcpy(key.place.longkey_obverse.head, key.mdbx.iov_base, fpta_max_keylen);
    key.place.longkey_obverse.tailhash =
        t1ha2_atonce((const uint8_t *)key.mdbx.iov_base + fpta_max_keylen,
                     key.mdbx.iov_len - fpta_max_keylen, 0);
  } else {
    /* ключ сравнивается от хвоста к голове,
     * копируем хвост и хэшируем начало. */
    key.place.longkey_reverse.headhash =
        t1ha2_atonce((const uint8_t *)key.mdbx.iov_base,
                     key.mdbx.iov_len - fpta_max_keylen, 0);
    memcpy(key.place.longkey_reverse.tail,
           (const uint8_t *)key.mdbx.iov_base + key.mdbx.iov_len -
               fpta_max_keylen,
           fpta_max_keylen);
  }

  static_assert(sizeof(key.place.longkey_obverse) == sizeof(key.place),
                "something wrong");
  static_assert(sizeof(key.place.longkey_reverse) == sizeof(key.place),
                "something wrong");
  key.mdbx.iov_len = sizeof(key.place);
  key.mdbx.iov_base = &key.place;
  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

static __inline MDBX_db_flags_t shove2dbiflags(fpta_shove_t shove) {
  assert(fpta_is_indexed(shove));
  const fptu_type type = fpta_shove2type(shove);
  const fpta_index_type index = fpta_shove2index(shove);

  MDBX_db_flags_t dbi_flags =
      fpta_index_is_unique(index) ? MDBX_DB_DEFAULTS : MDBX_DUPSORT;
  if ((type != /* composite */ fptu_null && type < fptu_96) ||
      fpta_index_is_unordered(index))
    dbi_flags |= MDBX_INTEGERKEY;
  else if (fpta_index_is_reverse(index) &&
           (type >= fptu_96 || type == /* composite */ fptu_null))
    dbi_flags |= MDBX_REVERSEKEY;

  return dbi_flags;
}

MDBX_db_flags_t fpta_index_shove2primary_dbiflags(fpta_shove_t pk_shove) {
  assert(fpta_index_is_primary(fpta_shove2index(pk_shove)));
  return shove2dbiflags(pk_shove);
}

MDBX_db_flags_t fpta_index_shove2secondary_dbiflags(fpta_shove_t pk_shove,
                                                    fpta_shove_t sk_shove) {
  assert(fpta_index_is_primary(fpta_shove2index(pk_shove)));
  assert(fpta_index_is_secondary(fpta_shove2index(sk_shove)));

  fptu_type pk_type = fpta_shove2type(pk_shove);
  fpta_index_type pk_index = fpta_shove2index(pk_shove);
  MDBX_db_flags_t dbi_flags = shove2dbiflags(sk_shove);
  if (dbi_flags & MDBX_DUPSORT) {
    if (pk_type < fptu_cstr && pk_type != /* composite */ fptu_null)
      dbi_flags |= MDBX_DUPFIXED;
    if ((pk_type < fptu_96 && pk_type != /* composite */ fptu_null) ||
        fpta_index_is_unordered(pk_index))
      dbi_flags |= MDBX_INTEGERDUP | MDBX_DUPFIXED;
    else if (fpta_index_is_reverse(pk_index) &&
             (pk_type >= fptu_96 || pk_type == /* composite */ fptu_null))
      dbi_flags |= MDBX_REVERSEDUP;
  }
  return dbi_flags;
}

static bool fpta_index_ordered_is_compat(fptu_type data_type,
                                         fpta_value_type value_type) {
  /* Критерий сравнимости:
   *  - все индексы коротких типов (использующие MDBX_INTEGERKEY) могут быть
   *    использованы только со значениями РАВНОГО фиксированного размера.
   *  - МОЖНО "смешивать" signed и unsigned, так как fpta_index_value2key()
   *    преобразует значение, либо вернет ошибку.
   *  - но НЕ допускается смешивать integer и float.
   *  - shoved допустим только при возможности больших ключей.
   */
  static const int32_t bits[fpta_invalid] = {
      /* fpta_null */
      0,

      /* fpta_signed_int */
      1 << fptu_uint16 | 1 << fptu_uint32 | 1 << fptu_uint64 | 1 << fptu_int32 |
          1 << fptu_int64,

      /* fpta_unsigned_int */
      1 << fptu_uint16 | 1 << fptu_uint32 | 1 << fptu_uint64 | 1 << fptu_int32 |
          1 << fptu_int64,

      /* fpta_datetime */
      1 << fptu_datetime,

      /* fpta_float_point */
      1 << fptu_fp32 | 1 << fptu_fp64,

      /* fpta_string */
      1 << fptu_cstr,

      /* fpta_binary */
      ~(1 << fptu_null | 1 << fptu_int32 | 1 << fptu_int64 |
        1 << fptu_datetime | 1 << fptu_uint16 | 1 << fptu_uint32 |
        1 << fptu_uint64 | 1 << fptu_fp32 | 1 << fptu_fp64 | 1 << fptu_cstr),

      /* fpta_shoved */
      ~(1 << fptu_int32 | 1 << fptu_int64 | 1 << fptu_datetime |
        1 << fptu_uint16 | 1 << fptu_uint32 | 1 << fptu_uint64 |
        1 << fptu_fp32 | 1 << fptu_fp64 | 1 << fptu_96 | 1 << fptu_128 |
        1 << fptu_160 | 1 << fptu_256),

      /* fpta_begin */
      ~0,

      /* fpta_end */
      ~0,

      /* fpta_epsilon */
      ~0};

  return (bits[value_type] & (1 << data_type)) != 0;
}

static bool fpta_index_unordered_is_compat(fptu_type data_type,
                                           fpta_value_type value_type) {
  /* Критерий сравнимости:
   *  - все индексы коротких типов (использующие MDBX_INTEGERKEY) могут быть
   *    использованы только со значениями РАВНОГО фиксированного размера.
   *  - МОЖНО "смешивать" signed и unsigned, так как fpta_index_value2key()
   *    преобразует значение, либо вернет ошибку.
   *  - но НЕ допускается смешивать integer и float.
   *  - shoved для всех типов, которые могут быть длиннее 8. */
  static const int32_t bits[fpta_invalid] = {
      /* fpta_null */
      0,

      /* fpta_signed_int */
      1 << fptu_uint16 | 1 << fptu_uint32 | 1 << fptu_uint64 | 1 << fptu_int32 |
          1 << fptu_int64,

      /* fpta_unsigned_int */
      1 << fptu_uint16 | 1 << fptu_uint32 | 1 << fptu_uint64 | 1 << fptu_int32 |
          1 << fptu_int64,

      /* fpta_date_time */
      1 << fptu_datetime,

      /* fpta_float_point */
      1 << fptu_fp32 | 1 << fptu_fp64,

      /* fpta_string */
      1 << fptu_cstr,

      /* fpta_binary */
      ~(1 << fptu_int32 | 1 << fptu_int64 | 1 << fptu_datetime |
        1 << fptu_uint16 | 1 << fptu_uint32 | 1 << fptu_uint64 |
        1 << fptu_fp32 | 1 << fptu_fp64 | 1 << fptu_cstr),

      /* fpta_shoved */
      ~(1 << fptu_null | 1 << fptu_int32 | 1 << fptu_int64 |
        1 << fptu_datetime | 1 << fptu_uint16 | 1 << fptu_uint32 |
        1 << fptu_uint64 | 1 << fptu_fp32 | 1 << fptu_fp64),

      /* fpta_begin */
      ~0,

      /* fpta_end */
      ~0,

      /* fpta_epsilon */
      ~0};

  return (bits[value_type] & (1 << data_type)) != 0;
}

bool fpta_index_is_compat(fpta_shove_t shove, const fpta_value &value) {
  if (unlikely(value.type == fpta_null))
    return fpta_column_is_nullable(shove);

  fptu_type type = fpta_shove2type(shove);
  fpta_index_type index = fpta_shove2index(shove);

  if (fpta_index_is_ordered(index))
    return fpta_index_ordered_is_compat(type, value.type);

  return fpta_index_unordered_is_compat(type, value.type);
}

//----------------------------------------------------------------------------

static int fpta_denil_key(const fpta_shove_t shove, fpta_key &key) {
  const fptu_type type = fpta_shove2type(shove);
  switch (type) {
  case fptu_null | fptu_farray:
    return FPTA_EOOPS;

  default:
    if (fpta_index_is_ordered(shove)) {
      if (type >= fptu_cstr) {
        key.mdbx.iov_len = 0;
        key.mdbx.iov_base = (void *)&fpta_NIL;
        return FPTA_SUCCESS;
      }
      assert(type >= fptu_96 && type <= fptu_256);

      const int fillbyte = fpta_index_is_obverse(shove)
                               ? FPTA_DENIL_FIXBIN_OBVERSE
                               : FPTA_DENIL_FIXBIN_REVERSE;
      key.mdbx.iov_base = &key.place;
      switch (type) {
      case fptu_96:
        memset(&key.place, fillbyte, key.mdbx.iov_len = 96 / 8);
        break;
      case fptu_128:
        memset(&key.place, fillbyte, key.mdbx.iov_len = 128 / 8);
        break;
      case fptu_160:
        memset(&key.place, fillbyte, key.mdbx.iov_len = 160 / 8);
        break;
      case fptu_256:
        memset(&key.place, fillbyte, key.mdbx.iov_len = 256 / 8);
        break;
      default:
        assert(false && "unexpected field type");
        __unreachable();
      }
      return FPTA_SUCCESS;
    }
    /* make unordered "super nil" */;
    key.place.u64 = 0;
    key.mdbx.iov_len = sizeof(key.place.u64);
    key.mdbx.iov_base = &key.place.u64;
    return FPTA_SUCCESS;

  case fptu_datetime:
    key.place.u64 = FPTA_DENIL_DATETIME_BIN;
    key.mdbx.iov_len = sizeof(key.place.u64);
    key.mdbx.iov_base = &key.place.u64;
    return FPTA_SUCCESS;

  case fptu_uint16:
    key.place.u32 = fpta_index_is_obverse(shove)
                        ? unsigned(FPTA_DENIL_UINT16_OBVERSE)
                        : unsigned(FPTA_DENIL_UINT16_REVERSE);
    key.mdbx.iov_len = sizeof(key.place.u32);
    key.mdbx.iov_base = &key.place.u32;
    return FPTA_SUCCESS;

  case fptu_int32:
  case fptu_uint32:
    key.place.u32 = fpta_index_is_obverse(shove) ? FPTA_DENIL_UINT32_OBVERSE
                                                 : FPTA_DENIL_UINT32_REVERSE;
    key.mdbx.iov_len = sizeof(key.place.u32);
    key.mdbx.iov_base = &key.place.u32;
    return FPTA_SUCCESS;

  case fptu_int64:
  case fptu_uint64:
    key.place.u64 = fpta_index_is_obverse(shove) ? FPTA_DENIL_UINT64_OBVERSE
                                                 : FPTA_DENIL_UINT64_REVERSE;
    key.mdbx.iov_len = sizeof(key.place.u64);
    key.mdbx.iov_base = &key.place.u64;
    return FPTA_SUCCESS;

  case fptu_fp32:
    key.place.u32 = 0;
    assert(mdbx_key_from_ptrfloat(&fpta_fp32_denil.__f) == key.place.u32);
    key.mdbx.iov_len = sizeof(key.place.u32);
    key.mdbx.iov_base = &key.place.u32;
    return FPTA_SUCCESS;

  case fptu_fp64:
    key.place.u64 = 0;
    assert(mdbx_key_from_ptrdouble(&fpta_fp64_denil.__d) == key.place.u64);
    key.mdbx.iov_len = sizeof(key.place.u64);
    key.mdbx.iov_base = &key.place.u64;
    return FPTA_SUCCESS;
  }
#ifndef _MSC_VER
  assert(false && "unreachable point");
  __unreachable();
#endif
}

int fpta_index_value2key(fpta_shove_t shove, const fpta_value &value,
                         fpta_key &key, bool copy) {
  if (unlikely(value.type == fpta_begin || value.type == fpta_end))
    return FPTA_ETYPE;

  if (unlikely(!fpta_is_indexed(shove)))
    return FPTA_EOOPS;

  if (unlikely(value.type == fpta_null)) {
    if (unlikely(!fpta_column_is_nullable(shove)))
      return FPTA_ETYPE;
    return fpta_denil_key(shove, key);
  }

  const fptu_type type = fpta_shove2type(shove);
  const fpta_index_type index = fpta_shove2index(shove);
  if (fpta_index_is_ordered(index)) {
    // упорядоченный индекс
    if (unlikely(!fpta_index_ordered_is_compat(type, value.type)))
      return FPTA_ETYPE;

    if (value.type == fpta_shoved) {
      // значение уже преобразовано в формат ключа

      if (unlikely(value.binary_length > sizeof(key.place)))
        return FPTA_DATALEN_MISMATCH;
      if (unlikely(value.binary_data == nullptr))
        return FPTA_EINVAL;

      key.mdbx.iov_len = value.binary_length;
      key.mdbx.iov_base = value.binary_data;
      if (copy) {
        memcpy(&key.place, key.mdbx.iov_base, key.mdbx.iov_len);
        key.mdbx.iov_base = &key.place;
      }
      return FPTA_SUCCESS;
    }
  } else {
    // неупорядоченный индекс (ключи всегда хешируются)
    if (unlikely(!fpta_index_unordered_is_compat(type, value.type)))
      return FPTA_ETYPE;

    if (value.type == fpta_shoved) {
      // значение уже преобразовано в формат ключа

      if (unlikely(value.binary_length != sizeof(key.place.u64)))
        return FPTA_DATALEN_MISMATCH;
      if (unlikely(value.binary_data == nullptr))
        return FPTA_EINVAL;

      key.mdbx.iov_len = sizeof(key.place.u64);
      key.mdbx.iov_base = value.binary_data;
      if (copy) {
        memcpy(&key.place, key.mdbx.iov_base, sizeof(key.place.u64));
        key.mdbx.iov_base = &key.place;
      }
      return FPTA_SUCCESS;
    }
  }

  switch (type) {
  case fptu_nested:
    // TODO: додумать как лучше преобразовывать кортеж в ключ.
    return FPTA_ENOIMP;

  default:
  /* TODO: проверить корректность размера для fptu_farray */
  case fptu_opaque:
    /* не позволяем смешивать string и opaque/binary, в том числе
     * чтобы избежать путаницы между строками в utf8 и unicode,
     * а также прочих последствий излишней гибкости. */
    assert(value.type != fpta_string);
    if (unlikely(value.type == fpta_string))
      return FPTA_EOOPS;
    if (unlikely(value.binary_data == nullptr) && value.binary_length)
      return FPTA_EINVAL;
    key.mdbx.iov_len = value.binary_length;
    key.mdbx.iov_base = value.binary_data;
    break;

  case fptu_null /* composite */:
    /* Для составных индексов/колонок должно быть передано fpta_shoved,
     * что обрабатывается чуть выше. Поэтому здесь только возврат ошибки. */
    return FPTA_ETYPE;

  case fptu_uint16:
    key.place.u32 = uint16_t(value.sint);
    if (unlikely(value.sint != key.place.u32))
      return FPTA_EVALUE;
    key.mdbx.iov_len = sizeof(key.place.u32);
    key.mdbx.iov_base = &key.place.u32;
    return FPTA_SUCCESS;

  case fptu_uint32:
    key.place.u32 = uint32_t(value.sint);
    if (unlikely(value.sint != key.place.u32))
      return FPTA_EVALUE;
    key.mdbx.iov_len = sizeof(key.place.u32);
    key.mdbx.iov_base = &key.place.u32;
    return FPTA_SUCCESS;

  case fptu_int32:
    if (unlikely(int32_t(value.sint) != value.sint))
      return FPTA_EVALUE;
    key.place.u32 = mdbx_key_from_int32(int32_t(value.sint));
    key.mdbx.iov_len = sizeof(key.place.u32);
    key.mdbx.iov_base = &key.place.u32;
    assert(mdbx_int32_from_key(key.mdbx) == value.sint);
    return FPTA_SUCCESS;

  case fptu_fp32: {
    const erthink::fpclassify<decltype(value.fp)> fpc(value.uint);
    if (unlikely(fpc.is_nan()))
      return FPTA_EVALUE;
    if (unlikely(std::abs(value.fp) > FLT_MAX) && !fpc.is_infinity())
      return FPTA_EVALUE;
    const float fp = unlikely(std::abs(value.fp) < FLT_MIN)
                         ? /* -0.0 => 0 */ float(0)
                         : float(value.fp);
    if (FPTA_PROHIBIT_LOSS_PRECISION &&
        !std::is_same<decltype(value.fp), float>::value &&
        unlikely(value.fp != fp))
      return FPTA_EVALUE;
    key.place.u32 = mdbx_key_from_ptrfloat(&fp);
    key.mdbx.iov_len = sizeof(key.place.u32);
    key.mdbx.iov_base = &key.place.u32;
    assert(mdbx_float_from_key(key.mdbx) == fp);
    return FPTA_SUCCESS;
  }

  case fptu_int64:
    if (unlikely(value.type == fpta_unsigned_int && value.uint > INT64_MAX))
      return FPTA_EVALUE;
    key.place.u64 = mdbx_key_from_int64(value.sint);
    key.mdbx.iov_len = sizeof(key.place.u64);
    key.mdbx.iov_base = &key.place.u64;
    assert(mdbx_int64_from_key(key.mdbx) == value.sint);
    return FPTA_SUCCESS;

  case fptu_uint64:
    if (unlikely(value.type == fpta_signed_int && value.sint < 0))
      return FPTA_EVALUE;
    key.place.u64 = value.uint;
    key.mdbx.iov_len = sizeof(key.place.u64);
    key.mdbx.iov_base = &key.place.u64;
    return FPTA_SUCCESS;

  case fptu_fp64: {
    const erthink::fpclassify<decltype(value.fp)> fpc(value.uint);
    if (unlikely(fpc.is_nan()))
      return FPTA_EVALUE;
    if (unlikely(std::abs(value.fp) > DBL_MAX) && !fpc.is_infinity())
      return FPTA_EVALUE;
    const double fp = unlikely(std::abs(value.fp) < DBL_MIN)
                          ? /* -0.0 => 0 */ double(0)
                          : double(value.fp);
    if (FPTA_PROHIBIT_LOSS_PRECISION &&
        !std::is_same<decltype(value.fp), double>::value &&
        unlikely(value.fp != fp))
      return FPTA_EVALUE;
    key.place.u64 = mdbx_key_from_ptrdouble(&fp);
    key.mdbx.iov_len = sizeof(key.place.u64);
    key.mdbx.iov_base = &key.place.u64;
    assert(mdbx_double_from_key(key.mdbx) == fp);
    return FPTA_SUCCESS;
  }

  case fptu_datetime:
    assert(value.type == fpta_datetime);
    key.place.u64 = value.uint;
    key.mdbx.iov_len = sizeof(key.place.u64);
    key.mdbx.iov_base = &key.place.u64;
    return FPTA_SUCCESS;

  case fptu_cstr:
    /* не позволяем смешивать string и opaque/binary, в том числе
     * чтобы избежать путаницы между строками в utf8 и unicode,
     * а также прочих последствий излишней гибкости. */
    assert(value.type == fpta_string);
    if (unlikely(value.type != fpta_string))
      return FPTA_EOOPS;
    if (unlikely(value.str == nullptr) && value.binary_length)
      return FPTA_EINVAL;
    key.mdbx.iov_len = value.binary_length;
    key.mdbx.iov_base = (void *)value.str;
    assert(key.mdbx.iov_len == 0 ||
           strnlen(value.str, key.mdbx.iov_len) == key.mdbx.iov_len);
    break;

  case fptu_96:
    key.mdbx.iov_len = value.binary_length;
    key.mdbx.iov_base = value.binary_data;
    if (unlikely(value.binary_length != 96 / 8))
      return FPTA_DATALEN_MISMATCH;
    break;

  case fptu_128:
    key.mdbx.iov_len = value.binary_length;
    key.mdbx.iov_base = value.binary_data;
    if (unlikely(value.binary_length != 128 / 8))
      return FPTA_DATALEN_MISMATCH;
    break;

  case fptu_160:
    key.mdbx.iov_len = value.binary_length;
    key.mdbx.iov_base = value.binary_data;
    if (unlikely(value.binary_length != 160 / 8))
      return FPTA_DATALEN_MISMATCH;
    break;

  case fptu_256:
    key.mdbx.iov_len = value.binary_length;
    key.mdbx.iov_base = value.binary_data;
    if (unlikely(value.binary_length != 256 / 8))
      return FPTA_DATALEN_MISMATCH;
    break;
  }

  return fpta_normalize_key(index, key, copy);
}

//----------------------------------------------------------------------------

int fpta_index_key2value(fpta_shove_t shove, MDBX_val mdbx, fpta_value &value) {
  const fptu_type type = fpta_shove2type(shove);
  const fpta_index_type index = fpta_shove2index(shove);

  if (fpta_index_is_unordered(index) &&
      (type >= fptu_96 || type == /* composite */ fptu_null)) {
    if (unlikely(mdbx.iov_len != sizeof(uint64_t)))
      goto return_corrupted;

    value.binary_data = mdbx.iov_base;
    value.binary_length = sizeof(uint64_t);
    value.type = fpta_shoved;
    return FPTA_SUCCESS;
  }

  if (type >= fptu_cstr) {
    if (mdbx.iov_len > (unsigned)fpta_max_keylen) {
      if (unlikely(mdbx.iov_len != (unsigned)fpta_shoved_keylen))
        goto return_corrupted;
      value.type = fpta_shoved;
      value.binary_data = mdbx.iov_base;
      value.binary_length = fpta_shoved_keylen;
      return FPTA_SUCCESS;
    }

    if (fpta_is_indexed_and_nullable(index)) {
      // null если ключ нулевой длины
      if (mdbx.iov_len == 0)
        goto return_null;

      // проверяем и отрезаем добавленый not-null префикс
      const uint8_t *body = (const uint8_t *)mdbx.iov_base;
      mdbx.iov_len -= fpta_notnil_prefix_length;
      if (fpta_index_is_obverse(index)) {
        if (unlikely(body[0] != fpta_notnil_prefix_byte))
          goto return_corrupted;
        mdbx.iov_base = (void *)(body + fpta_notnil_prefix_length);
      } else {
        if (unlikely(body[mdbx.iov_len] != fpta_notnil_prefix_byte))
          goto return_corrupted;
      }
    }

    switch (type) {
    default:
    /* TODO: проверить корректность размера для fptu_farray */
    case fptu_nested:
      if (unlikely(mdbx.iov_len % sizeof(fptu_unit)))
        goto return_corrupted;
      __fallthrough;
    case fptu_opaque:
      value.type = fpta_binary;
      value.binary_data = mdbx.iov_base;
      value.binary_length = (unsigned)mdbx.iov_len;
      return FPTA_SUCCESS;

    case fptu_cstr:
      value.type = fpta_string;
      value.binary_data = mdbx.iov_base;
      value.binary_length = (unsigned)mdbx.iov_len;
      return FPTA_SUCCESS;
    }
  }

  switch (type) {
  default:
    assert(false && "unreachable");
    __unreachable();
    return FPTA_EOOPS;

  case fptu_null /* composite */:
    if (mdbx.iov_len > (unsigned)fpta_max_keylen &&
        unlikely(mdbx.iov_len != (unsigned)fpta_shoved_keylen))
      goto return_corrupted;
    value.type = fpta_shoved;
    value.binary_data = mdbx.iov_base;
    value.binary_length = unsigned(mdbx.iov_len);
    return FPTA_SUCCESS;

  case fptu_uint16: {
    if (unlikely(mdbx.iov_len != sizeof(uint32_t)))
      goto return_corrupted;
    uint32_t u32;
    memcpy(&u32, mdbx.iov_base, 4);
    if (unlikely(u32 > UINT16_MAX))
      goto return_corrupted;
    if (fpta_is_indexed_and_nullable(index) &&
        unlikely(u32 == numeric_traits<fptu_uint16>::denil(index)))
      goto return_null;
    value.uint = u32;
    value.type = fpta_unsigned_int;
    value.binary_length = sizeof(uint32_t);
    return FPTA_SUCCESS;
  }

  case fptu_uint32: {
    if (unlikely(mdbx.iov_len != sizeof(uint32_t)))
      goto return_corrupted;
    uint32_t u32;
    memcpy(&u32, mdbx.iov_base, 4);
    if (fpta_is_indexed_and_nullable(index) &&
        unlikely(u32 == numeric_traits<fptu_uint32>::denil(index)))
      goto return_null;
    value.uint = u32;
    value.type = fpta_unsigned_int;
    value.binary_length = sizeof(uint32_t);
    return FPTA_SUCCESS;
  }

  case fptu_int32: {
    if (unlikely(mdbx.iov_len != sizeof(int32_t)))
      goto return_corrupted;
    value.sint = mdbx_int32_from_key(mdbx);
    if (fpta_is_indexed_and_nullable(index) &&
        unlikely(value.sint == numeric_traits<fptu_int32>::denil(index)))
      goto return_null;
    value.type = fpta_signed_int;
    value.binary_length = sizeof(int32_t);
    return FPTA_SUCCESS;
  }

  case fptu_fp32: {
    if (unlikely(mdbx.iov_len != sizeof(uint32_t)))
      goto return_corrupted;
    const float fp = mdbx_float_from_key(mdbx);
    if (fpta_is_indexed_and_nullable(index) &&
        unlikely(erthink::bit_cast<uint32_t>(fp) == FPTA_DENIL_FP32_BIN))
      goto return_null;
    value.fp = fp;
    value.type = fpta_float_point;
    value.binary_length = sizeof(fp);
    return FPTA_SUCCESS;
  }

  case fptu_fp64: {
    if (unlikely(mdbx.iov_len != sizeof(uint64_t)))
      goto return_corrupted;
    const double fp = mdbx_double_from_key(mdbx);
    if (fpta_is_indexed_and_nullable(index) &&
        unlikely(erthink::bit_cast<uint64_t>(fp) == FPTA_DENIL_FP64_BIN))
      goto return_null;
    value.fp = fp;
    value.type = fpta_float_point;
    value.binary_length = sizeof(double);
    return FPTA_SUCCESS;
  }

  case fptu_uint64: {
    if (unlikely(mdbx.iov_len != sizeof(uint64_t)))
      goto return_corrupted;
    memcpy(&value.uint, mdbx.iov_base, 8);
    if (fpta_is_indexed_and_nullable(index) &&
        unlikely(value.uint == numeric_traits<fptu_uint64>::denil(index)))
      goto return_null;
    value.type = fpta_unsigned_int;
    value.binary_length = sizeof(uint64_t);
    return FPTA_SUCCESS;
  }

  case fptu_int64: {
    if (unlikely(mdbx.iov_len != sizeof(int64_t)))
      goto return_corrupted;
    value.sint = mdbx_int64_from_key(mdbx);
    if (fpta_is_indexed_and_nullable(index) &&
        unlikely(value.sint == numeric_traits<fptu_int64>::denil(index)))
      goto return_null;
    value.type = fpta_signed_int;
    value.binary_length = sizeof(int64_t);
    return FPTA_SUCCESS;
  }

  case fptu_datetime: {
    if (unlikely(mdbx.iov_len != sizeof(uint64_t)))
      goto return_corrupted;
    memcpy(&value.datetime.fixedpoint, mdbx.iov_base, 8);
    if (fpta_is_indexed_and_nullable(index) &&
        unlikely(value.datetime.fixedpoint == FPTA_DENIL_DATETIME_BIN))
      goto return_null;
    value.type = fpta_datetime;
    value.binary_length = sizeof(uint64_t);
    return FPTA_SUCCESS;
  }

  case fptu_96:
    if (unlikely(mdbx.iov_len != 96 / 8))
      goto return_corrupted;
    if (fpta_is_indexed_and_nullable(index) &&
        is_fixbin_denil<fptu_96>(index, mdbx.iov_base))
      goto return_null;
    break;

  case fptu_128:
    if (unlikely(mdbx.iov_len != 128 / 8))
      goto return_corrupted;
    if (fpta_is_indexed_and_nullable(index) &&
        is_fixbin_denil<fptu_128>(index, mdbx.iov_base))
      goto return_null;
    break;

  case fptu_160:
    if (unlikely(mdbx.iov_len != 160 / 8))
      goto return_corrupted;
    if (fpta_is_indexed_and_nullable(index) &&
        is_fixbin_denil<fptu_160>(index, mdbx.iov_base))
      goto return_null;
    break;

  case fptu_256:
    if (unlikely(mdbx.iov_len != 256 / 8))
      goto return_corrupted;
    if (fpta_is_indexed_and_nullable(index) &&
        is_fixbin_denil<fptu_256>(index, mdbx.iov_base))
      goto return_null;
    break;
  }

  value.type = fpta_binary;
  value.binary_data = mdbx.iov_base;
  value.binary_length = (unsigned)mdbx.iov_len;
  return FPTA_SUCCESS;

return_null:
  value.type = fpta_null;
  value.binary_data = nullptr;
  value.binary_length = 0;
  return FPTA_SUCCESS;

return_corrupted:
  value.type = fpta_invalid;
  value.binary_data = nullptr;
  value.binary_length = ~0u;
  return FPTA_INDEX_CORRUPTED;
}

//----------------------------------------------------------------------------

__hot int fpta_index_row2key(const fpta_table_schema *const schema,
                             size_t column, const fptu_ro &row, fpta_key &key,
                             bool copy) {
#ifndef NDEBUG
  fpta_pollute(&key, sizeof(key), 0);
#endif

  assert(column < schema->column_count());
  const fpta_shove_t shove = schema->column_shove(column);
  const fptu_type type = fpta_shove2type(shove);
  const fpta_index_type index = fpta_shove2index(shove);
  if (unlikely(type == /* composite */ fptu_null)) {
    /* composite pseudo-column */
    return fpta_composite_row2key(schema, column, row, key);
  }

  const fptu_field *field = fptu::lookup(row, (unsigned)column, type);
  if (unlikely(field == nullptr)) {
    if (!fpta_is_indexed_and_nullable(index))
      return FPTA_COLUMN_MISSING;

    return fpta_denil_key(shove, key);
  }

  const fptu_payload *payload = field->payload();
  switch (type) {
  case fptu_nested:
    // TODO: додумать как лучше преобразовывать кортеж в ключ.
    return FPTA_ENOIMP;

  default:
    /* TODO: проверить корректность размера для fptu_farray */
    key.mdbx.iov_len = payload->varlen_netto_size();
    key.mdbx.iov_base = const_cast<void *>(payload->inner_begin());
    break;

  case fptu_opaque:
    key.mdbx.iov_len = payload->varlen_opaque_bytes();
    key.mdbx.iov_base = const_cast<void *>(payload->inner_begin());
    break;

  case fptu_uint16:
    key.place.u32 = field->get_payload_uint16();
    key.mdbx.iov_len = sizeof(key.place.u32);
    key.mdbx.iov_base = &key.place.u32;
    return FPTA_SUCCESS;

  case fptu_uint32:
    key.place.u32 = payload->peek_u32();
    key.mdbx.iov_len = sizeof(key.place.u32);
    key.mdbx.iov_base = &key.place.u32;
    return FPTA_SUCCESS;

  case fptu_datetime:
    static_assert(sizeof(payload->unaligned_dt) ==
                      sizeof(payload->unaligned_u64),
                  "WTF?");
    __fallthrough;
  case fptu_uint64:
    key.place.u64 = payload->peek_u64();
    key.mdbx.iov_len = sizeof(key.place.u64);
    key.mdbx.iov_base = &key.place.u64;
    return FPTA_SUCCESS;

  case fptu_int32:
    key.place.u32 = mdbx_key_from_int32(payload->peek_i32());
    key.mdbx.iov_len = sizeof(key.place.u32);
    key.mdbx.iov_base = &key.place.u32;
    assert(mdbx_int32_from_key(key.mdbx) == payload->peek_i32());
    return FPTA_SUCCESS;

  case fptu_fp32:
    key.place.u32 = mdbx_key_from_float(payload->peek_fp32());
    key.mdbx.iov_len = sizeof(key.place.u32);
    key.mdbx.iov_base = &key.place.u32;
    assert(mdbx_float_from_key(key.mdbx) == payload->peek_fp32());
    return FPTA_SUCCESS;

  case fptu_int64:
    key.place.u64 = mdbx_key_from_int64(payload->peek_i64());
    key.mdbx.iov_len = sizeof(key.place.u64);
    key.mdbx.iov_base = &key.place.u64;
    assert(mdbx_int64_from_key(key.mdbx) == payload->peek_i64());
    return FPTA_SUCCESS;

  case fptu_fp64:
    key.place.u64 = mdbx_key_from_double(payload->peek_fp64());
    key.mdbx.iov_len = sizeof(key.place.u64);
    key.mdbx.iov_base = &key.place.u64;
    assert(mdbx_double_from_key(key.mdbx) == payload->peek_fp64());
    return FPTA_SUCCESS;

  case fptu_cstr:
    key.mdbx.iov_base = (void *)payload->cstr;
    key.mdbx.iov_len = strlen(payload->cstr);
    break;

  case fptu_96:
    key.mdbx.iov_len = 96 / 8;
    key.mdbx.iov_base = (void *)payload->fixbin;
    break;

  case fptu_128:
    key.mdbx.iov_len = 128 / 8;
    key.mdbx.iov_base = (void *)payload->fixbin;
    break;

  case fptu_160:
    key.mdbx.iov_len = 160 / 8;
    key.mdbx.iov_base = (void *)payload->fixbin;
    break;

  case fptu_256:
    key.mdbx.iov_len = 256 / 8;
    key.mdbx.iov_base = (void *)payload->fixbin;
    break;
  }

  return fpta_normalize_key(index, key, copy);
}

//----------------------------------------------------------------------------

#if FPTA_ENABLE_TESTS

static inline MDBX_cmp_func *index_shove2comparator(fpta_shove_t shove) {
  const fpta_index_type index = fpta_shove2index(shove);
  if (fpta_index_is_unordered(index))
    return mdbx_get_keycmp(MDBX_INTEGERKEY);

  const fptu_type type = fpta_shove2type(shove);
  if (type >= fptu_96 || type == /* composite */ fptu_null)
    return mdbx_get_keycmp(fpta_index_is_reverse(index) ? MDBX_REVERSEKEY
                                                        : MDBX_DB_DEFAULTS);
  return mdbx_get_keycmp(MDBX_INTEGERKEY);
}

const void *__fpta_index_shove2comparator(fpta_shove_t shove) {
  return (const void *)index_shove2comparator(shove);
}

int __fpta_index_value2key(fpta_shove_t shove, const fpta_value *value,
                           void *key) {
  return fpta_index_value2key(shove, *value, *(fpta_key *)key, true);
}

#endif /* FPTA_ENABLE_TESTS */
