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

#pragma once
#include "fast_positive/tables_internal.h"
#include "osal.h"

#include <algorithm>
#include <atomic>
#include <functional>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4820) /* bytes padding added after data member       \
                                   for aligment */
#endif                          /* _MSC_VER (warnings) */

using namespace fpta;

struct fpta_db {
  fpta_db(const fpta_db &) = delete;
  MDBX_env *mdbx_env;
  bool alterable_schema;
  MDBX_dbi schema_dbi;
  fpta_rwl_t schema_rwlock;
  uint64_t schema_tsn;
  fpta_regime_flags regime_flags;

  struct app_version_info {
    uint64_t hash;
    uint32_t oldest, newest;

    app_version_info(const fpta_appcontent_info *appcontent = nullptr)
        : hash((appcontent && appcontent->signature)
                   ? t1ha2_atonce(appcontent->signature,
                                  strlen(appcontent->signature) + 1,
                                  UINT64_C(20200804151731))
                   : 0),
          oldest(appcontent ? appcontent->oldest : 0),
          newest(appcontent ? appcontent->newest : 0) {
      assert(oldest <= newest);
    }
  };
  app_version_info app_version;

  struct version_info {
    uint32_t signature;
    uint32_t format;
    app_version_info app;

    static version_info legacy_default() {
      version_info r;
      r.signature = fpta_db_version_signature;
      r.format = 3 /* 0.3 stub value for prior to app-version */;
      r.app = app_version_info(nullptr);
      return r;
    }

    static version_info current(const app_version_info &running) {
      version_info r;
      r.signature = fpta_db_version_signature;
      r.format = fpta_db_format_version;
      r.app = running;
      return r;
    }

    static version_info fetch(MDBX_val &dict_record) {
      /* Информация о версии формата и версии приложении опционально хранится
       * в начале dict-записи. Если данных достаточно и сигнатура совпадает, то:
       *  - используем эти данные для проверки версии формата fpta-базы
       *    и версии приложения;
       *  - пропускаем их при загрузке словаря;
       *  - при этом значение сигнатуры таково, что её невозможно спутать
       *    с началом словаря схемы.
       * Иначе для версии формата и приложения подставляем значения
       * по-умолчанию, которые соответствуют ситуации до добавления
       * fpta_appcontent_info. */
      if (dict_record.iov_len >= sizeof(version_info)) {
        uint32_t check;
        memcpy(&check, dict_record.iov_base, sizeof(check));
        if (check == fpta_db_version_signature) {
          version_info r;
          memcpy(&r, dict_record.iov_base, sizeof(version_info));
          dict_record.iov_len -= sizeof(version_info);
          dict_record.iov_base =
              (char *)dict_record.iov_base + sizeof(version_info);
          return r;
        }
      }
      return legacy_default();
    }

    static version_info merge(MDBX_val db_dict_record,
                              const app_version_info &running) {
      const version_info db = fetch(db_dict_record);
      version_info r;
      r.signature = fpta_db_version_signature;
      /* При изменении схемы в существующей БД информация о версии приложении и
       * формате базы перезаписывается. При этом (в логике текущей реализации)
       * версия формата сохраняется, а маркеры совместимости двигаются вперед
       * к версии приложения. */
      r.format = db.format;
      r.app.hash = running.hash;
      r.app.newest = std::max(db.app.newest, running.newest);
      r.app.oldest = std::max(db.app.oldest, running.oldest);
      return r;
    }
  };

  fpta_error is_compatible(const version_info &db) const {
    if (unlikely(db.format != fpta_db_format_version))
      return FPTA_FORMAT_MISMATCH;

    if (unlikely(db.app.hash != app_version.hash ||
                 db.app.oldest > app_version.newest ||
                 db.app.newest < app_version.oldest))
      return FPTA_APP_MISMATCH;

    return FPTA_OK;
  }

  fpta_mutex_t dbi_mutex /* TODO: убрать мьютекс и перевести на atomic */;
  fpta_shove_t dbi_shoves[fpta_dbi_cache_size];
  uint64_t dbi_tsns[fpta_dbi_cache_size];
  MDBX_dbi dbi_handles[fpta_dbi_cache_size];
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

enum fpta_schema_item {
  fpta_table,
  fpta_column,
  fpta_table_with_schema,
  fpta_column_with_schema
};

/* Подставляется в качестве адреса для ключей нулевой длины,
 * с тем чтобы отличать от nullptr */
extern const char fpta_NIL;

//----------------------------------------------------------------------------

class fpta_lock_guard {
  fpta_lock_guard(const fpta_lock_guard &) = delete;
  fpta_mutex_t *_mutex;

public:
  fpta_lock_guard() : _mutex(nullptr) {}

  int lock(fpta_mutex_t *mutex) {
    assert(_mutex == nullptr);
    int err = fpta_mutex_lock(mutex);
    if (likely(err == 0))
      _mutex = mutex;
    return err;
  }

  void unlock() {
    if (_mutex) {
      int err = fpta_mutex_unlock(_mutex);
      assert(err == 0);
      _mutex = nullptr;
      (void)err;
    }
  }

  ~fpta_lock_guard() { unlock(); }
};

//----------------------------------------------------------------------------

bool fpta_filter_validate(const fpta_filter *filter);

static __inline bool fpta_db_validate(const fpta_db *db) {
  if (unlikely(db == nullptr || db->mdbx_env == nullptr))
    return false;

  // TODO
  return true;
}

static __inline int fpta_txn_validate(const fpta_txn *txn,
                                      fpta_level min_level) {
  if (unlikely(txn == nullptr || !fpta_db_validate(txn->db)))
    return FPTA_EINVAL;
  if (unlikely(txn->level < min_level || txn->level > fpta_schema))
    return FPTA_EPERM;

  if (unlikely(txn->mdbx_txn == nullptr))
    return FPTA_TXN_CANCELLED;

  return FPTA_OK;
}

static cxx14_constexpr int fpta_id_validate(const fpta_name *id,
                                            fpta_schema_item schema_item) {
  if (unlikely(id == nullptr))
    return FPTA_EINVAL;

  switch (schema_item) {
  default:
    return FPTA_EOOPS;

  case fpta_table:
  case fpta_table_with_schema:
    if (unlikely(fpta_shove2index(id->shove) !=
                 (fpta_index_type)fpta_flag_table))
      return FPTA_EINVAL;

    if (schema_item > fpta_table) {
      const fpta_table_schema *table_schema = id->table_schema;
      if (unlikely(table_schema == nullptr))
        return FPTA_EINVAL;
      if (unlikely(table_schema->signature() != FTPA_SCHEMA_SIGNATURE))
        return FPTA_SCHEMA_CORRUPTED;
      if (unlikely(table_schema->table_shove() != id->shove))
        return FPTA_SCHEMA_CORRUPTED;
      assert(id->version_tsn >= table_schema->version_tsn());
    }
    return FPTA_SUCCESS;

  case fpta_column:
  case fpta_column_with_schema:
    if (unlikely(fpta_shove2index(id->shove) ==
                 (fpta_index_type)fpta_flag_table))
      return FPTA_EINVAL;

    if (schema_item > fpta_column) {
      if (unlikely(id->column.num > fpta_max_cols))
        return FPTA_EINVAL;
      int rc = fpta_id_validate(id->column.table, fpta_table_with_schema);
      if (unlikely(rc != FPTA_SUCCESS))
        return rc;
      const fpta_table_schema *table_schema = id->column.table->table_schema;
      if (unlikely(id->column.num > table_schema->column_count()))
        return FPTA_SCHEMA_CORRUPTED;
      if (unlikely(table_schema->column_shove(id->column.num) != id->shove))
        return FPTA_SCHEMA_CORRUPTED;
    }
    return FPTA_SUCCESS;
  }
}

static __inline int fpta_cursor_validate(const fpta_cursor *cursor,
                                         fpta_level min_level) {
  if (unlikely(cursor == nullptr || cursor->mdbx_cursor == nullptr))
    return FPTA_EINVAL;

  return fpta_txn_validate(cursor->txn, min_level);
}

//----------------------------------------------------------------------------

struct fpta_dbi_name {
  char cstr[(64 + 6 - 1) / 6 /* 64-битный хэш */ + 1 /* терминирующий 0 */];
};

void fpta_shove2str(fpta_shove_t shove, fpta_dbi_name *name);

fpta_shove_t fpta_name_validate_and_shove(const fpta::string_view &);

inline fpta_shove_t fpta_shove_name(const char *name,
                                    enum fpta_schema_item type) {
  const fpta_shove_t shove =
      fpta_name_validate_and_shove(fpta::string_view(name));
  return (shove && type == fpta_table) ? shove | fpta_flag_table : shove;
}

static __inline bool fpta_dbi_shove_is_pk(const fpta_shove_t dbi_shove) {
  return 0 == (dbi_shove & (fpta_column_typeid_mask | fpta_column_index_mask));
}

static __inline fpta_shove_t fpta_dbi_shove(const fpta_shove_t table_shove,
                                            const size_t index_id) {
  assert(table_shove > fpta_flag_table);
  assert(index_id < fpta_max_indexes);

  fpta_shove_t dbi_shove = table_shove - fpta_flag_table;
  assert(0 == (dbi_shove & (fpta_column_typeid_mask | fpta_column_index_mask)));
  dbi_shove += index_id;

  assert(fpta_shove_eq(table_shove, dbi_shove));
  assert(fpta_dbi_shove_is_pk(dbi_shove) == (index_id == 0));
  return dbi_shove;
}

static __inline MDBX_db_flags_t fpta_dbi_flags(const fpta_shove_t *shoves_defs,
                                               const size_t n) {
  const MDBX_db_flags_t dbi_flags =
      (n == 0)
          ? fpta_index_shove2primary_dbiflags(shoves_defs[0])
          : fpta_index_shove2secondary_dbiflags(shoves_defs[0], shoves_defs[n]);
  return dbi_flags;
}

static __inline fpta_shove_t fpta_data_shove(const fpta_shove_t *shoves_defs,
                                             const size_t n) {
  const fpta_shove_t data_shove =
      n ? shoves_defs[0]
        : fpta_column_shove(0, fptu_nested,
                            fpta_primary_unique_ordered_obverse);
  return data_shove;
}

int fpta_dbi_open(fpta_txn *txn, const fpta_shove_t dbi_shove,
                  MDBX_dbi &__restrict handle, const MDBX_db_flags_t dbi_flags);

int fpta_dbicache_open(fpta_txn *txn, const fpta_shove_t shove,
                       MDBX_dbi &handle, const MDBX_db_flags_t dbi_flags,
                       unsigned *const cache_hint);

MDBX_dbi fpta_dbicache_remove(fpta_db *db, const fpta_shove_t shove,
                              unsigned *const cache_hint = nullptr);
int fpta_dbicache_cleanup(fpta_txn *txn, fpta_table_schema *def);

//----------------------------------------------------------------------------

template <fptu_type type> struct numeric_traits;

template <> struct numeric_traits<fptu_uint16> {
  typedef uint16_t native;
  typedef uint_fast16_t fast;
  enum { has_native_saturation = false };
  typedef std::numeric_limits<native> native_limits;
  static fast denil(const fpta_shove_t shove) {
    assert(fpta_column_is_nullable(shove));
    return fpta_index_is_obverse(shove) ? (fast)FPTA_DENIL_UINT16_OBVERSE
                                        : (fast)FPTA_DENIL_UINT16_REVERSE;
  }
  static fpta_value_type value_type() { return fpta_unsigned_int; }
  static fpta_value make_value(const fast value) {
    return fpta_value_uint(value);
  }
};

template <> struct numeric_traits<fptu_uint32> {
  typedef uint32_t native;
  typedef uint_fast32_t fast;
  enum { has_native_saturation = false };
  typedef std::numeric_limits<native> native_limits;
  static fast denil(const fpta_shove_t shove) {
    assert(fpta_column_is_nullable(shove));
    return fpta_index_is_obverse(shove) ? FPTA_DENIL_UINT32_OBVERSE
                                        : FPTA_DENIL_UINT32_REVERSE;
  }
  static fpta_value_type value_type() { return fpta_unsigned_int; }
  static fpta_value make_value(const fast value) {
    return fpta_value_uint(value);
  }
};

template <> struct numeric_traits<fptu_uint64> {
  typedef uint64_t native;
  typedef uint_fast64_t fast;
  enum { has_native_saturation = false };
  typedef std::numeric_limits<native> native_limits;
  static fast denil(const fpta_shove_t shove) {
    assert(fpta_column_is_nullable(shove));
    return fpta_index_is_obverse(shove) ? FPTA_DENIL_UINT64_OBVERSE
                                        : FPTA_DENIL_UINT64_REVERSE;
  }
  static fpta_value_type value_type() { return fpta_unsigned_int; }
  static fpta_value make_value(const fast value) {
    return fpta_value_uint(value);
  }
};

template <> struct numeric_traits<fptu_int32> {
  typedef int32_t native;
  typedef int_fast32_t fast;
  enum { has_native_saturation = false };
  typedef std::numeric_limits<native> native_limits;
  static fast denil(const fpta_shove_t shove) {
    assert(fpta_column_is_nullable(shove));
    (void)shove;
    return FPTA_DENIL_SINT32;
  }
  static fpta_value_type value_type() { return fpta_signed_int; }
  static fpta_value make_value(const fast value) {
    return fpta_value_sint(value);
  }
};

template <> struct numeric_traits<fptu_int64> {
  typedef int64_t native;
  typedef int_fast64_t fast;
  enum { has_native_saturation = false };
  typedef std::numeric_limits<native> native_limits;
  static fast denil(const fpta_shove_t shove) {
    assert(fpta_column_is_nullable(shove));
    (void)shove;
    return FPTA_DENIL_SINT64;
  }
  static fpta_value_type value_type() { return fpta_signed_int; }
  static fpta_value make_value(const fast value) {
    return fpta_value_sint(value);
  }
};

template <> struct numeric_traits<fptu_fp32> {
  typedef float native;
  typedef float_t fast;
  enum { has_native_saturation = true };
  typedef std::numeric_limits<native> native_limits;
  static fast denil(const fpta_shove_t shove) {
    assert(fpta_column_is_nullable(shove));
    (void)shove;
    return FPTA_DENIL_FP32;
  }
  static fpta_value_type value_type() { return fpta_float_point; }
  static fpta_value make_value(const fast value) {
    return fpta_value_float(value);
  }
};

template <> struct numeric_traits<fptu_fp64> {
  typedef double native;
  typedef double_t fast;
  enum { has_native_saturation = true };
  typedef std::numeric_limits<native> native_limits;
  static fast denil(const fpta_shove_t shove) {
    assert(fpta_column_is_nullable(shove));
    (void)shove;
    return FPTA_DENIL_FP64;
  }
  static fpta_value_type value_type() { return fpta_float_point; }
  static fpta_value make_value(const fast value) {
    return fpta_value_float(value);
  }
};
