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

struct db_deleter {
  void operator()(fpta_db *db) const {
    if (db) {
      EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
    }
  }
};

struct txn_deleter {
  void operator()(fpta_txn *txn) const {
    if (txn) {
      ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, true));
    }
  }
};

struct cursor_deleter {
  void operator()(fpta_cursor *cursor) const {
    if (cursor) {
      ASSERT_EQ(FPTA_OK, fpta_cursor_close(cursor));
    }
  }
};

struct ptrw_deleter {
  void operator()(fptu_rw *ptrw) const { free(ptrw); }
};

typedef std::unique_ptr<fpta_db, db_deleter> scoped_db_guard;
typedef std::unique_ptr<fpta_txn, txn_deleter> scoped_txn_guard;
typedef std::unique_ptr<fpta_cursor, cursor_deleter> scoped_cursor_guard;
typedef std::unique_ptr<fptu_rw, ptrw_deleter> scoped_ptrw_guard;

//----------------------------------------------------------------------------

/* простейший медленный тест на простоту */
bool isPrime(unsigned number);

/* кол-во единичных бит */
unsigned hamming_weight(unsigned number);

//----------------------------------------------------------------------------

static __inline int value2key(fpta_shove_t shove, const fpta_value &value,
                              fpta_key &key) {
  return __fpta_index_value2key(shove, &value, &key);
}

static __inline MDBX_cmp_func *shove2comparator(fpta_shove_t shove) {
  return (MDBX_cmp_func *)__fpta_index_shove2comparator(shove);
}

//----------------------------------------------------------------------------

inline bool is_valid4primary(fptu_type type, fpta_index_type index) {
  if (!fpta_is_indexed(index) || fpta_index_is_secondary(index))
    return false;

  if (type <= fptu_null || type >= fptu_farray)
    return false;

  if (fpta_index_is_reverse(index) && type < fptu_96) {
    if (!fpta_is_indexed_and_nullable(index) ||
        !fpta_nullable_reverse_sensitive(type))
      return false;
  }

  return true;
}

inline bool is_valid4cursor(fpta_index_type index, fpta_cursor_options cursor) {
  if (!fpta_is_indexed(index))
    return false;

  if (fpta_cursor_is_ordered(cursor) && fpta_index_is_unordered(index))
    return false;

  return true;
}

inline bool is_valid4secondary(fptu_type pk_type, fpta_index_type pk_index,
                               fptu_type se_type, fpta_index_type se_index) {
  (void)pk_type;
  if (!fpta_is_indexed(pk_index) || !fpta_index_is_unique(pk_index))
    return false;

  if (!fpta_is_indexed(se_index) || fpta_index_is_primary(se_index))
    return false;

  if (se_type <= fptu_null || se_type >= fptu_farray)
    return false;

  if (fpta_index_is_reverse(se_index) && se_type < fptu_96) {
    if (!fpta_is_indexed_and_nullable(se_index) ||
        !fpta_nullable_reverse_sensitive(se_type))
      return false;
  }

  return true;
}
