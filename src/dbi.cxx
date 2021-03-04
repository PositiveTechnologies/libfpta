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

/* Вспомогательный компаратор для сравнения строк таблиц (кортежей).
 * Используется для сверки данных, например при удалении строк по заданному
 * образцу. В отличие от memcmp() результат сравнения не зависит от физического
 * порядка полей в кортеже. */
static __hot int cmp_rows(const MDBX_val *a, const MDBX_val *b) noexcept {
  switch (fptu_cmp_tuples(*(const fptu_ro *)a, *(const fptu_ro *)b)) {
  case fptu_eq:
    return 0;
  case fptu_lt:
    return -1;
  case fptu_gt:
    return 1;
  default:
    assert(0 && "incomparable tuples");
    return 42;
  }
}

void fpta_shove2str(fpta_shove_t shove, fpta_dbi_name *name) {
  const static char aplhabet[65] =
      "@0123456789qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM_";

  for (ptrdiff_t i = FPT_ARRAY_LENGTH(name->cstr) - 1; --i >= 0;) {
    name->cstr[i] = aplhabet[shove & 63];
    shove >>= 6;
  }

  name->cstr[FPT_ARRAY_LENGTH(name->cstr) - 1] = '\0';
}

static __inline MDBX_dbi fpta_dbicache_peek(const fpta_txn *txn,
                                            const fpta_shove_t shove,
                                            const unsigned cache_hint,
                                            const uint64_t current_tsn) {
  if (likely(cache_hint < fpta_dbi_cache_size)) {
    const fpta_db *db = txn->db;
    if (likely(db->dbi_shoves[cache_hint] == shove &&
               db->dbi_tsns[cache_hint] == current_tsn))
      return db->dbi_handles[cache_hint];
  }
  return 0;
}

static __hot MDBX_dbi fpta_dbicache_lookup(fpta_db *db, fpta_shove_t shove,
                                           unsigned *__restrict cache_hint) {
  if (likely(*cache_hint < fpta_dbi_cache_size)) {
    if (likely(db->dbi_shoves[*cache_hint] == shove))
      return db->dbi_handles[*cache_hint];
    *cache_hint = ~0u;
  }

  const size_t n = shove % fpta_dbi_cache_size;
  size_t i = n;
  do {
    if (db->dbi_shoves[i] == shove) {
      *cache_hint = (unsigned)i;
      return db->dbi_handles[i];
    }
    i = (i + 1) % fpta_dbi_cache_size;
  } while (i != n && db->dbi_shoves[i]);

  return 0;
}

static unsigned fpta_dbicache_update(fpta_db *db, const fpta_shove_t shove,
                                     const MDBX_dbi dbi, const uint64_t tsn) {
  assert(shove > 0);

  const size_t n = shove % fpta_dbi_cache_size;
  size_t i = n;
  do {
    assert(db->dbi_shoves[i] != shove);
    if (db->dbi_shoves[i] == 0) {
      db->dbi_handles[i] = dbi;
      db->dbi_tsns[i] = tsn;
      db->dbi_shoves[i] = shove;
      return (unsigned)i;
    }
    i = (i + 1) % fpta_dbi_cache_size;
  } while (i != n);

  /* TODO: прокричать что кэш переполнен (слишком много таблиц и индексов) */
  return ~0u;
}

__cold MDBX_dbi fpta_dbicache_remove(fpta_db *db, const fpta_shove_t shove,
                                     unsigned *__restrict const cache_hint) {
  assert(shove > 0);

  if (cache_hint) {
    const size_t i = *cache_hint;
    if (i < fpta_dbi_cache_size) {
      *cache_hint = ~0u;
      if (db->dbi_shoves[i] == shove) {
        MDBX_dbi dbi = db->dbi_handles[i];
        db->dbi_handles[i] = 0;
        db->dbi_shoves[i] = 0;
        return dbi;
      }
    }
    return 0;
  }

  const size_t n = shove % fpta_dbi_cache_size;
  size_t i = n;
  do {
    if (db->dbi_shoves[i] == shove) {
      MDBX_dbi dbi = db->dbi_handles[i];
      db->dbi_handles[i] = 0;
      db->dbi_shoves[i] = 0;
      return dbi;
    }
    i = (i + 1) % fpta_dbi_cache_size;
  } while (i != n && db->dbi_shoves[i]);

  return 0;
}

__cold int fpta_dbi_open(fpta_txn *txn, const fpta_shove_t dbi_shove,
                         MDBX_dbi &__restrict handle,
                         const MDBX_db_flags_t dbi_flags) {
  fpta_dbi_name dbi_name;
  fpta_shove2str(dbi_shove, &dbi_name);
  int rc = mdbx_dbi_open_ex(
      txn->mdbx_txn, dbi_name.cstr, dbi_flags, &handle,
      /* для ключей всегда используются компараторы mdbx */ nullptr,
      fpta_dbi_shove_is_pk(dbi_shove)
          ? /* сравнение строк таблицы */ cmp_rows
          : /* компаратор mdbx для сравнения первичных ключей
               во вторичных индексах */
          nullptr);
  assert((handle != 0) == (rc == FPTA_SUCCESS));
  return rc;
}

static __cold int
fpta_dbicache_validate_locked(fpta_txn *txn, const fpta_shove_t dbi_shove,
                              const MDBX_db_flags_t dbi_flags,
                              unsigned *__restrict const cache_hint) {
  assert(cache_hint);
  fpta_db *db = txn->db;
  if (likely(*cache_hint < fpta_dbi_cache_size &&
             db->dbi_shoves[*cache_hint] == dbi_shove &&
             db->dbi_handles[*cache_hint])) {

    if (likely(db->dbi_tsns[*cache_hint] == txn->schema_tsn()))
      return FPTA_SUCCESS;
    if (db->dbi_tsns[*cache_hint] > txn->schema_tsn()) {
      if (db->dbi_tsns[*cache_hint] < db->schema_tsn ||
          txn->schema_tsn() != db->schema_tsn)
        return FPTA_SCHEMA_CHANGED;
      db->dbi_tsns[*cache_hint] = txn->schema_tsn();
      return MDBX_SUCCESS;
    }

    MDBX_dbi handle;
    int rc = fpta_dbi_open(txn, dbi_shove, handle, dbi_flags);
    if (likely(rc == MDBX_SUCCESS)) {
      assert(handle == db->dbi_handles[*cache_hint]);
      db->dbi_tsns[*cache_hint] = txn->schema_tsn();
      return MDBX_SUCCESS;
    }

    if (rc != MDBX_INCOMPATIBLE)
      return rc;

    MDBX_envinfo info;
    rc = mdbx_env_info_ex(nullptr, txn->mdbx_txn, &info, sizeof(info));
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;

    if (info.mi_self_latter_reader_txnid < txn->schema_tsn())
      return FPTA_TARDY_DBI /* handle may be used by other txn */;

    rc = mdbx_dbi_close(db->mdbx_env,
                        fpta_dbicache_remove(db, dbi_shove, cache_hint));
    if (rc != MDBX_SUCCESS && rc != MDBX_BAD_DBI)
      return rc;
  }

  *cache_hint = ~0u;
  return FPTA_NODATA;
}

__cold int fpta_dbicache_open(fpta_txn *txn, const fpta_shove_t dbi_shove,
                              MDBX_dbi &__restrict handle,
                              const MDBX_db_flags_t dbi_flags,
                              unsigned *__restrict const cache_hint) {
  assert(fpta_txn_validate(txn, fpta_read) == FPTA_SUCCESS);
  assert(cache_hint != nullptr);
  fpta_lock_guard guard;
  fpta_db *db = txn->db;

  if (txn->level < fpta_schema) {
    int err = guard.lock(&db->dbi_mutex);
    if (unlikely(err != 0))
      return err;
  }

  handle = fpta_dbicache_lookup(db, dbi_shove, cache_hint);
  if (likely(handle)) {
    int rc =
        fpta_dbicache_validate_locked(txn, dbi_shove, dbi_flags, cache_hint);
    if (likely(rc != FPTA_NODATA)) {
      if (rc == FPTA_SUCCESS) {
        assert(*cache_hint < fpta_dbi_cache_size);
        assert(handle == db->dbi_handles[*cache_hint]);
      }
      return rc;
    }
  }

  int rc = fpta_dbi_open(txn, dbi_shove, handle, dbi_flags);
  if (likely(rc == FPTA_SUCCESS))
    *cache_hint =
        fpta_dbicache_update(db, dbi_shove, handle, txn->schema_tsn());
  return rc;
}

__cold int fpta_dbicache_cleanup(fpta_txn *txn, fpta_table_schema *table_def) {
  fpta_db *db = txn->db;
  if (likely(db->schema_tsn >= txn->schema_tsn()))
    return (db->schema_tsn == txn->schema_tsn()) ? FPTA_SUCCESS
                                                 : FPTA_SCHEMA_CHANGED;

  fpta_lock_guard guard;
  if (txn->level < fpta_schema) {
    int err = guard.lock(&db->dbi_mutex);
    if (unlikely(err != 0))
      return err;
    if (unlikely(db->schema_tsn >= txn->schema_tsn()))
      return (db->schema_tsn == txn->schema_tsn()) ? FPTA_SUCCESS
                                                   : FPTA_SCHEMA_CHANGED;
  }

  MDBX_envinfo info;
  int rc = mdbx_env_info_ex(nullptr, txn->mdbx_txn, &info, sizeof(info));
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  const uint64_t tardy_tsn =
      (info.mi_self_latter_reader_txnid &&
       info.mi_self_latter_reader_txnid < txn->schema_tsn())
          ? info.mi_latter_reader_txnid
          : txn->schema_tsn();

  if (table_def) {
    rc = fpta_dbicache_validate_locked(
        txn, fpta_dbi_shove(table_def->table_shove(), 0),
        fpta_dbi_flags(table_def->column_shoves_array(), 0),
        &table_def->handle_cache(0));
    if (unlikely(rc != FPTA_SUCCESS && rc != FPTA_NODATA))
      return rc;

    for (size_t i = 1; i < table_def->column_count(); ++i) {
      const fpta_shove_t shove = table_def->column_shove(i);
      if (!fpta_is_indexed(shove))
        break;

      rc = fpta_dbicache_validate_locked(
          txn, fpta_dbi_shove(table_def->table_shove(), i),
          fpta_dbi_flags(table_def->column_shoves_array(), i),
          &table_def->handle_cache(i));
      if (unlikely(rc != FPTA_SUCCESS && rc != FPTA_NODATA))
        return rc;
    }
  }

  if (tardy_tsn == txn->schema_tsn() && db->schema_tsn != txn->schema_tsn()) {
    for (size_t i = 0; i < fpta_dbi_cache_size; ++i) {
      if (!db->dbi_handles[i] || db->dbi_tsns[i] >= tardy_tsn)
        continue;

      rc = mdbx_dbi_close(db->mdbx_env, db->dbi_handles[i]);
      if (rc != MDBX_SUCCESS && rc != MDBX_BAD_DBI)
        return rc;
      db->dbi_handles[i] = 0;
      db->dbi_shoves[i] = 0;
    }
  }

  if (!table_def)
    db->schema_tsn = txn->schema_tsn();

  return MDBX_SUCCESS;
}

//----------------------------------------------------------------------------

int __hot fpta_open_table(fpta_txn *txn, fpta_table_schema *table_def,
                          MDBX_dbi &handle) {
  const MDBX_db_flags_t dbi_flags =
      fpta_dbi_flags(table_def->column_shoves_array(), 0);
  const fpta_shove_t dbi_shove = fpta_dbi_shove(table_def->table_shove(), 0);
  handle = fpta_dbicache_peek(txn, dbi_shove, table_def->handle_cache(0),
                              table_def->version_tsn());
  if (likely(handle > 0))
    return FPTA_OK;

  return fpta_dbicache_open(txn, dbi_shove, handle, dbi_flags,
                            &table_def->handle_cache(0));
}

int __hot fpta_open_column(fpta_txn *txn, fpta_name *column_id,
                           MDBX_dbi &tbl_handle, MDBX_dbi &idx_handle) {
  assert(fpta_id_validate(column_id, fpta_column) == FPTA_SUCCESS);

  fpta_table_schema *table_def = column_id->column.table->table_schema;
  int rc = fpta_open_table(txn, table_def, tbl_handle);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (column_id->column.num == 0) {
    idx_handle = tbl_handle;
    return FPTA_SUCCESS;
  }

  const MDBX_db_flags_t dbi_flags =
      fpta_dbi_flags(table_def->column_shoves_array(), column_id->column.num);
  fpta_shove_t dbi_shove =
      fpta_dbi_shove(table_def->table_shove(), column_id->column.num);
  idx_handle = fpta_dbicache_peek(
      txn, dbi_shove, table_def->handle_cache(column_id->column.num),
      table_def->version_tsn());
  if (likely(idx_handle > 0))
    return FPTA_OK;

  return fpta_dbicache_open(txn, dbi_shove, idx_handle, dbi_flags,
                            &table_def->handle_cache(column_id->column.num));
}

int __hot fpta_open_secondaries(fpta_txn *txn, fpta_table_schema *table_def,
                                MDBX_dbi *dbi_array) {
  int rc = fpta_open_table(txn, table_def, dbi_array[0]);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  for (size_t i = 1; i < table_def->column_count(); ++i) {
    const fpta_shove_t shove = table_def->column_shove(i);
    if (!fpta_is_indexed(shove))
      break;

    const MDBX_db_flags_t dbi_flags =
        fpta_dbi_flags(table_def->column_shoves_array(), i);
    const fpta_shove_t dbi_shove = fpta_dbi_shove(table_def->table_shove(), i);

    dbi_array[i] = fpta_dbicache_peek(
        txn, dbi_shove, table_def->handle_cache(i), table_def->version_tsn());
    if (unlikely(dbi_array[i] == 0)) {
      rc = fpta_dbicache_open(txn, dbi_shove, dbi_array[i], dbi_flags,
                              &table_def->handle_cache(i));
      if (unlikely(rc != FPTA_SUCCESS))
        return rc;
    }
  }

  return FPTA_SUCCESS;
}
