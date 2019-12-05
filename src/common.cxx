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

static int fpta_db_lock(fpta_db *db, fpta_level level) {
  assert(level >= fpta_read && level <= fpta_schema);

  int rc;
  if (db->alterable_schema) {
    if (level < fpta_schema)
      rc = fpta_rwl_sharedlock(&db->schema_rwlock);
    else
      rc = fpta_rwl_exclusivelock(&db->schema_rwlock);
    assert(rc == FPTA_SUCCESS);
  } else {
    rc = (level < fpta_schema) ? FPTA_SUCCESS : FPTA_EPERM;
  }

  return rc;
}

static int fpta_db_unlock(fpta_db *db, fpta_level level) {
  (void)level;
  assert(level >= fpta_read && level <= fpta_schema);

  int rc;
  if (db->alterable_schema) {
    rc = fpta_rwl_unlock(&db->schema_rwlock);
  } else {
    rc = (level < fpta_schema) ? FPTA_SUCCESS : FPTA_EOOPS;
  }
  assert(rc == FPTA_SUCCESS);
  return rc;
}

static fpta_txn *fpta_txn_alloc(fpta_db *db, fpta_level level) {
  // TODO: use pool
  (void)level;
  fpta_txn *txn = (fpta_txn *)calloc(1, sizeof(fpta_txn));
  if (likely(txn)) {
    txn->db = db;
    txn->level = level;
  }
  return txn;
}

static void fpta_txn_free(fpta_db *db, fpta_txn *txn) {
  // TODO: use pool
  (void)db;
  if (likely(txn)) {
    assert(txn->db == db);
    txn->db = nullptr;
    free(txn);
  }
}

fpta_cursor *fpta_cursor_alloc(fpta_db *db) {
  // TODO: use pool
  fpta_cursor *cursor = (fpta_cursor *)calloc(1, sizeof(fpta_cursor));
  if (likely(cursor))
    cursor->db = db;
  return cursor;
}

void fpta_cursor_free(fpta_db *db, fpta_cursor *cursor) {
  // TODO: use pool
  if (likely(cursor)) {
    assert(cursor->db == db);
    (void)db;
    cursor->db = nullptr;
    free(cursor);
  }
}

//----------------------------------------------------------------------------

int fpta_db_create_or_open(const char *path, fpta_durability durability,
                           fpta_regime_flags regime_flags,
                           bool alterable_schema, fpta_db **pdb,
                           fpta_db_creation_params_t *creation_params) {

  if (unlikely(pdb == nullptr))
    return FPTA_EINVAL;
  *pdb = nullptr;

  if (unlikely(t1ha_selfcheck__t1ha2()))
    return FPTA_EOOPS;

  if (unlikely(path == nullptr || *path == '\0'))
    return FPTA_EINVAL;

  if (creation_params) {
    if (unlikely(durability == fpta_readonly ||
                 creation_params->params_size !=
                     sizeof(fpta_db_creation_params_t)))
      return FPTA_EINVAL;
  }

  unsigned mdbx_flags = MDBX_NOSUBDIR;
  switch (durability) {
  default:
    return FPTA_EFLAG;
  case fpta_readonly:
    mdbx_flags |= MDBX_RDONLY;
    break;
  case fpta_weak:
    mdbx_flags |= MDBX_UTTERLY_NOSYNC;
  /* fall through */
  case fpta_lazy:
    mdbx_flags |= MDBX_NOSYNC | MDBX_NOMETASYNC;
    if (0 == (regime_flags & fpta_saferam))
      mdbx_flags |= MDBX_WRITEMAP;
  /* fall through */
  case fpta_sync:
    if (regime_flags & fpta_frendly4hdd) {
      // LY: nothing for now
    } else {
      if (regime_flags & fpta_frendly4writeback)
        mdbx_flags |= MDBX_LIFORECLAIM;
      if (regime_flags & fpta_frendly4compaction)
        mdbx_flags |= MDBX_COALESCE;
    }
    break;
  }

  fpta_db *db = (fpta_db *)calloc(1, sizeof(fpta_db));
  if (unlikely(db == nullptr))
    return FPTA_ENOMEM;
  db->regime_flags = regime_flags;

  int rc;
  db->alterable_schema = alterable_schema;
  if (db->alterable_schema) {
    rc = fpta_rwl_init(&db->schema_rwlock);
    if (unlikely(rc != 0)) {
      free(db);
      return (fpta_error)rc;
    }
  }

  rc = fpta_mutex_init(&db->dbi_mutex);
  if (unlikely(rc != 0)) {
    int err = fpta_rwl_destroy(&db->schema_rwlock);
    assert(err == 0);
    (void)err;
    free(db);
    return (fpta_error)rc;
  }

  if (unlikely(regime_flags & fpta_madness4testing)) {
    mdbx_setup_debug(MDBX_LOG_WARN,
                     MDBX_DBG_ASSERT | MDBX_DBG_AUDIT | MDBX_DBG_DUMP |
                         MDBX_DBG_LEGACY_MULTIOPEN | MDBX_DBG_JITTER,
                     reinterpret_cast<MDBX_debug_func *>(
                         intptr_t(-1 /* means "don't change" */)));
  }

  rc = mdbx_env_create(&db->mdbx_env);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  rc = mdbx_env_set_userctx(db->mdbx_env, db);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  rc = mdbx_env_set_maxreaders(db->mdbx_env, 42 /* FIXME */);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  rc = mdbx_env_set_maxdbs(db->mdbx_env, fpta_tables_max);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  if (creation_params) {
    rc = mdbx_env_set_geometry(
        db->mdbx_env, creation_params->size_lower,
        -1 /* current/initial size = default */, creation_params->size_upper,
        creation_params->growth_step, creation_params->shrink_threshold,
        creation_params->pagesize);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
  }

  rc = mdbx_env_open(db->mdbx_env, path, mdbx_flags,
                     creation_params ? creation_params->file_mode : 0);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  *pdb = db;
  return FPTA_SUCCESS;

bailout:
  if (db->mdbx_env) {
    int err = mdbx_env_close_ex(db->mdbx_env, true /* don't touch/save/sync */);
    assert(err == MDBX_SUCCESS);
    (void)err;
  }

  int err = fpta_mutex_destroy(&db->dbi_mutex);
  assert(err == 0);
  if (alterable_schema) {
    err = fpta_rwl_destroy(&db->schema_rwlock);
    assert(err == 0);
  }
  (void)err;

  free(db);
  return (fpta_error)rc;
}

int fpta_db_close(fpta_db *db) {
  if (unlikely(!fpta_db_validate(db)))
    return FPTA_EINVAL;

  int rc = fpta_db_lock(db, db->alterable_schema ? fpta_schema : fpta_write);
  if (unlikely(rc != 0))
    return (fpta_error)rc;

  rc = fpta_mutex_lock(&db->dbi_mutex);
  if (unlikely(rc != 0)) {
    int err = fpta_db_unlock(db, fpta_schema);
    assert(err == 0);
    (void)err;
    return (fpta_error)rc;
  }

  rc = (fpta_error)mdbx_env_close_ex(db->mdbx_env, false);
  assert(rc == MDBX_SUCCESS);
  db->mdbx_env = nullptr;

  int err = fpta_mutex_unlock(&db->dbi_mutex);
  assert(err == 0);
  err = fpta_mutex_destroy(&db->dbi_mutex);
  assert(err == 0);

  err = fpta_db_unlock(db, db->alterable_schema ? fpta_schema : fpta_write);
  assert(err == 0);
  if (db->alterable_schema) {
    err = fpta_rwl_destroy(&db->schema_rwlock);
    assert(err == 0);
  }
  (void)err;

  free(db);
  return (fpta_error)rc;
}

//----------------------------------------------------------------------------

int fpta_transaction_begin(fpta_db *db, fpta_level level, fpta_txn **ptxn) {
  if (unlikely(ptxn == nullptr))
    return FPTA_EINVAL;
  *ptxn = nullptr;

  if (unlikely(level < fpta_read || level > fpta_schema))
    return FPTA_EFLAG;

  if (unlikely(!fpta_db_validate(db)))
    return FPTA_EINVAL;

  int err = fpta_db_lock(db, level);
  if (unlikely(err != 0))
    return err;

  int rc = FPTA_ENOMEM;
  fpta_txn *txn = fpta_txn_alloc(db, level);
  if (unlikely(txn == nullptr))
    goto bailout;

  rc = mdbx_txn_begin(db->mdbx_env, nullptr,
                      (level == fpta_read) ? (unsigned)MDBX_RDONLY : 0u,
                      &txn->mdbx_txn);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

retry:
  rc = mdbx_canary_get(txn->mdbx_txn, &txn->canary);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout_abort;

  txn->db_version = mdbx_txn_id(txn->mdbx_txn);
  assert(txn->schema_tsn() <=
         ((level > fpta_read) ? txn->db_version - 1 : txn->db_version));

  if (unlikely(db->schema_tsn != txn->schema_tsn())) {
    fpta_lock_guard guard;
    if (txn->level < fpta_schema) {
      rc = guard.lock(&db->dbi_mutex);
      if (unlikely(rc != 0))
        goto bailout_abort;
    }

    if (db->schema_tsn > txn->schema_tsn() && level == fpta_read) {
      rc = mdbx_txn_reset(txn->mdbx_txn);
      if (likely(rc == MDBX_SUCCESS))
        rc = mdbx_txn_renew(txn->mdbx_txn);
      if (likely(rc == MDBX_SUCCESS))
        goto retry;
      rc = fpta_internal_abort(txn, rc, true);
      goto bailout;
    }

    rc = fpta_dbicache_cleanup(txn, nullptr, true);
    if (unlikely(rc != FPTA_SUCCESS))
      goto bailout_abort;

    db->schema_tsn = txn->schema_tsn();
  }

  *ptxn = txn;
  return FPTA_SUCCESS;

bailout_abort:
  rc = fpta_internal_abort(txn, rc, false);

bailout:
  err = fpta_db_unlock(db, level);
  assert(err == 0);
  (void)err;
  fpta_txn_free(db, txn);
  *ptxn = nullptr;
  return rc;
}

int fpta_transaction_end(fpta_txn *txn, bool abort) {
  int rc = fpta_txn_validate(txn, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS)) {
    if (rc == FPTA_TXN_CANCELLED)
      goto cancelled;
    return rc;
  }

  if (txn->level == fpta_read) {
    // TODO: reuse txn with mdbx_txn_reset(), but pool needed...
    rc = mdbx_txn_commit(txn->mdbx_txn);
  } else if (unlikely(abort)) {
    rc = fpta_internal_abort(txn, FPTA_OK);
  } else {
    rc = mdbx_canary_put(txn->mdbx_txn, &txn->canary);
    if (likely(rc == MDBX_SUCCESS))
      rc = mdbx_txn_commit(txn->mdbx_txn);
    if (unlikely(rc != MDBX_SUCCESS))
      rc = fpta_internal_abort(txn, rc, true);
  }
  txn->mdbx_txn = nullptr;

cancelled:
  int err = fpta_db_unlock(txn->db, txn->level);
  assert(err == 0);
  (void)err;
  fpta_txn_free(txn->db, txn);

  return (fpta_error)rc;
}

int fpta_transaction_versions(fpta_txn *txn, uint64_t *db_version,
                              uint64_t *schema_version) {
  int rc = fpta_txn_validate(txn, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (likely(db_version))
    *db_version = txn->db_version;
  if (likely(schema_version))
    *schema_version = txn->schema_tsn();
  return FPTA_SUCCESS;
}

int fpta_db_sequence(fpta_txn *txn, uint64_t *result, uint64_t increment) {
  if (unlikely(result == nullptr))
    return FPTA_EINVAL;

  int rc = fpta_txn_validate(txn, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  *result = txn->db_sequence();
  if (increment) {
    if (unlikely(txn->level < fpta_write))
      return FPTA_EPERM;

    uint64_t value = txn->db_sequence() + increment;
    if (value < increment) {
      static_assert(FPTA_NODATA == MDBX_RESULT_TRUE, "expect equal");
      return FPTA_NODATA;
    }

    assert(txn->db_sequence() < value);
    txn->db_sequence() = value;
  }

  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int
#if defined(__GNUC__) || __has_attribute(weak)
    __attribute__((weak))
#endif
    fpta_panic(int errnum_initial, int errnum_fatal) {
  (void)errnum_initial;
  (void)errnum_fatal;
  return (FPTA_ENABLE_ABORT_ON_PANIC) ? 0 : -1;
}

int fpta_internal_abort(fpta_txn *txn, int errnum, bool txn_maybe_dead) {
  /* Некоторые ошибки (например переполнение БД) могут происходить когда
   * мы выполнили лишь часть операций. В таких случаях можно лишь
   * прервать/откатить всю транзакцию, что и делает эта функция.
   *
   * Однако, могут быть ошибки отката транзакции, что потенциально является
   * более серьезной проблемой. */

  if (txn->level > fpta_read) {
    /* Чистим кеш dbi-хендлов покалеченных таблиц */
    bool dbi_locked = false;
    fpta_db *db = txn->db;
    for (size_t i = 0; i < fpta_dbi_cache_size; ++i) {
      const MDBX_dbi dbi = db->dbi_handles[i];
      const fpta_shove_t shove = db->dbi_shoves[i];
      if (shove && dbi) {
        unsigned tbl_flags = 0, tbl_state = 0;
        int err = mdbx_dbi_flags_ex(txn->mdbx_txn, dbi, &tbl_flags, &tbl_state);
        if (err != MDBX_SUCCESS || (tbl_state & MDBX_TBL_CREAT)) {
          if (!dbi_locked && txn->level < fpta_schema) {
            err = fpta_mutex_lock(&db->dbi_mutex);
            if (unlikely(err != 0))
              return err;
            dbi_locked = true;
          }

          if (shove == db->dbi_shoves[i] && dbi == db->dbi_handles[i]) {
            db->dbi_shoves[i] = 0;
            db->dbi_handles[i] = 0;
          }
        }
      }
    }

    if (db->schema_dbi > 0) {
      unsigned tbl_flags = 0, tbl_state = 0;
      int err = mdbx_dbi_flags_ex(txn->mdbx_txn, db->schema_dbi, &tbl_flags,
                                  &tbl_state);
      if (err != MDBX_SUCCESS || (tbl_state & MDBX_TBL_CREAT)) {
        if (!dbi_locked && txn->level < fpta_schema) {
          err = fpta_mutex_lock(&db->dbi_mutex);
          if (unlikely(err != 0))
            return err;
          dbi_locked = true;
        }
        db->schema_dbi = 0;
      }
    }

    if (dbi_locked) {
      int err = fpta_mutex_unlock(&db->dbi_mutex);
      assert(err == 0);
      (void)err;
    }
  }

  int rc = mdbx_txn_abort(txn->mdbx_txn);
  if (unlikely(rc != MDBX_SUCCESS)) {
    switch (rc) {
    case MDBX_EBADSIGN /* already aborted read-only txn */:
    /* fallthrough */
    case MDBX_BAD_TXN /* already aborted read-write txn */:
    /* fallthrough */
    case MDBX_THREAD_MISMATCH /* already aborted and started in other thread */:
      if (txn_maybe_dead)
        break;
    /* fallthrough */
    default:
      if (!fpta_panic(errnum, rc))
        abort();
      errnum = FPTA_WANNA_DIE;
    }
  }

  txn->mdbx_txn = nullptr;
  return errnum;
}

MDBX_env *fpta_mdbx_env(fpta_db *db) {
  return likely(fpta_db_validate(db)) ? db->mdbx_env : nullptr;
}

MDBX_txn *fpta_mdbx_txn(fpta_txn *txn) {
  return likely(fpta_txn_validate(txn, fpta_read) == FPTA_SUCCESS)
             ? txn->mdbx_txn
             : nullptr;
}

int fpta_transaction_lag(fpta_txn *txn, unsigned *lag, unsigned *percent) {
  int err = fpta_txn_validate(txn, fpta_read);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  if (unlikely(txn->level != fpta_read))
    return FPTA_EPERM;

  if (unlikely(!lag))
    return FPTA_EINVAL;

  MDBX_txn_info info;
  err = mdbx_txn_info(txn->mdbx_txn, &info, false);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  *lag = (unsigned)info.txn_reader_lag;
  if (percent)
    *percent = (unsigned)(info.txn_space_used * 100 /
                          (info.txn_space_used + info.txn_space_leftover));

  return FPTA_SUCCESS;
}

int fpta_transaction_restart(fpta_txn *txn) {
  int err = fpta_txn_validate(txn, fpta_read);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  if (unlikely(txn->level != fpta_read))
    return FPTA_EPERM;

  MDBX_txn_info info;
  err = mdbx_txn_info(txn->mdbx_txn, &info, false);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  if (info.txn_reader_lag == 0)
    return FPTA_SUCCESS;

retry:
  err = mdbx_txn_reset(txn->mdbx_txn);
  if (likely(err == MDBX_SUCCESS))
    err = mdbx_txn_renew(txn->mdbx_txn);
  if (unlikely(err != MDBX_SUCCESS)) {
  bailout:
    return fpta_internal_abort(txn, err);
  }

  err = mdbx_canary_get(txn->mdbx_txn, &txn->canary);
  if (unlikely(err != MDBX_SUCCESS))
    goto bailout;

  txn->db_version = mdbx_txn_id(txn->mdbx_txn);
  assert(txn->schema_tsn() <= txn->db_version);

  if (unlikely(txn->db->schema_tsn != txn->schema_tsn())) {
    fpta_lock_guard guard;
    if (txn->level < fpta_schema) {
      err = guard.lock(&txn->db->dbi_mutex);
      if (unlikely(err != MDBX_SUCCESS))
        goto bailout;
    }

    if (txn->db->schema_tsn > txn->schema_tsn())
      goto retry;

    err = fpta_dbicache_cleanup(txn, nullptr, true);
    if (unlikely(err != MDBX_SUCCESS))
      goto bailout;

    txn->db->schema_tsn = txn->schema_tsn();
  }

  return FPTA_SUCCESS;
}
