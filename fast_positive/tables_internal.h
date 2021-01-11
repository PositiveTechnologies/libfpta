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

/*
 * libfpta = { Fast Positive Tables, aka Позитивные Таблицы }
 *
 * Ultra fast, compact, embeddable storage engine for (semi)structured data:
 * multiprocessing with zero-overhead, full ACID semantics with MVCC,
 * variety of indexes, saturation, sequences and much more.
 * Please see README.md at https://github.com/erthink/libfpta
 *
 * The Future will (be) Positive. Всё будет хорошо.
 *
 * "Позитивные таблицы" предназначены для построения высокоскоростных
 * локальных хранилищ структурированных данных, с целевой производительностью
 * до 1.000.000 запросов в секунду на каждое ядро процессора.
 */

#pragma once
#define FPTA_INTERNALS

#ifdef _MSC_VER
#if _MSC_VER < 1900
#error FPTA required 'Microsoft Visual Studio 2015' or newer.
#endif

#pragma warning(disable : 4514) /* 'xyz': unreferenced inline function         \
                                   has been removed */
#pragma warning(disable : 4710) /* 'xyz': function not inlined */
#pragma warning(disable : 4711) /* function 'xyz' selected for                 \
                                   automatic inline expansion */
#pragma warning(disable : 4061) /* enumerator 'abc' in switch of enum 'xyz' is \
                                   not explicitly handled by a case label */
#pragma warning(disable : 4201) /* nonstandard extension used :                \
                                   nameless struct / union */
#pragma warning(disable : 4127) /* conditional expression is constant */
#pragma warning(disable : 4996) /* std::xyz::_Unchecked_iterators::_Deprecate. \
                                   Bla-bla-bla. See documentation on how to    \
                                   use Visual C++ 'Checked Iterators' */
#pragma warning(disable : 4702) /* unreachable code */
#endif                          /* _MSC_VER (warnings) */

#include "fast_positive/config.h"
#include "fast_positive/tables.h"
#include "fast_positive/tuples_internal.h"

#ifdef _MSC_VER
#if _MSC_VER >= 1900            /* MSVC 2015/2017 compilers are mad */
#pragma warning(disable : 4770) /* partially validated enum used as index */
#endif

#if _MSC_VER > 1913
#pragma warning(disable : 5045) /* will insert Spectre mitigation... */
#endif

#ifndef _WIN64 /* We don't worry about padding for 32-bit builds */
#pragma warning(disable : 4820) /* 4 bytes padding added after data member */
#endif

#pragma warning(push, 1)
#pragma warning(disable : 4548) /* expression before comma has no effect;      \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                   semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling  \
                                   mode specified; termination on exception    \
                                   is not guaranteed. Specify /EHsc */
#endif                          /* _MSC_VER (warnings) */

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <time.h>
#if !(defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__) ||   \
      defined(__BSD__) || defined(__NETBSD__) || defined(__bsdi__) ||          \
      defined(__DragonFly__) || defined(__APPLE__) || defined(__MACH__))
#include <malloc.h>
#endif /* xBSD */

__extern_C int_fast32_t mrand64(void);

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#else
static __inline int_fast32_t mrand48(void) { return mrand64(); }
#endif

#include <algorithm>
#include <cfloat> // for float limits
#include <cmath>  // for fabs()
#include <limits> // for numeric_limits<>
#include <vector> // for vector<>

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#define MDBX_DEPRECATED
#include "libmdbx/mdbx.h"
#include "t1ha/t1ha.h"

#if defined(__cland__) && !__CLANG_PREREQ(3, 8)
// LY: workaround for https://llvm.org/bugs/show_bug.cgi?id=18402
extern "C" char *gets(char *);
#endif

//----------------------------------------------------------------------------

static cxx11_constexpr fpta_shove_t fpta_column_shove(
    fpta_shove_t shove, fptu_type data_type, fpta_index_type index_type) {
  constexpr_assert((data_type & ~fpta_column_typeid_mask) == 0);
  constexpr_assert((index_type & ~fpta_column_index_mask) == 0);
  constexpr_assert((shove & ((1 << fpta_name_hash_shift) - 1)) == 0);
  return shove | data_type | index_type;
}

static cxx11_constexpr bool fpta_shove_eq(fpta_shove_t a, fpta_shove_t b) {
  static_assert(fpta_name_hash_shift > 0, "expect hash/shove is shifted");
  /* A равно B, если отличия только в бладших битах */
  return (a ^ b) < (1u << fpta_name_hash_shift);
}

static cxx11_constexpr fptu_type fpta_shove2type(fpta_shove_t shove) {
  static_assert(fpta_column_typeid_shift == 0,
                "expecting column_typeid_shift is zero");
  return fptu_type(shove & fpta_column_typeid_mask);
}

static cxx11_constexpr fpta_index_type fpta_shove2index(fpta_shove_t shove) {
  static_assert((int)fpta_primary_unique_ordered_obverse <
                    fpta_column_index_mask,
                "check fpta_column_index_mask");
  static_assert((int)fpta_primary_unique_ordered_obverse >
                    (1 << fpta_column_index_shift) - 1,
                "expect fpta_primary_unique_ordered_obverse is shifted");
  static_assert((fpta_column_index_mask & fpta_column_typeid_mask) == 0,
                "seems a bug");
  return fpta_index_type(shove & fpta_column_index_mask);
}

static cxx11_constexpr bool fpta_is_composite(fpta_shove_t shove) {
  return fpta_shove2type(shove) == /* composite */ fptu_null;
}

static cxx11_constexpr fptu_type fpta_id2type(const fpta_name *id) {
  return fpta_shove2type(id->shove);
}

static cxx11_constexpr fpta_index_type fpta_id2index(const fpta_name *id) {
  return fpta_shove2index(id->shove);
}

bool fpta_index_is_valid(const fpta_index_type index_type);

static cxx11_constexpr bool fpta_is_indexed(const fpta_shove_t index) {
  return (index & (fpta_column_index_mask - fpta_index_fnullable)) != 0;
}

static cxx11_constexpr bool fpta_index_is_unique(const fpta_shove_t index) {
  constexpr_assert(fpta_is_indexed(index));
  return (index & fpta_index_funique) != 0;
}

static cxx11_constexpr bool fpta_index_is_ordered(const fpta_shove_t index) {
  constexpr_assert(fpta_is_indexed(index));
  return (index & fpta_index_fordered) != 0;
}

static cxx11_constexpr bool fpta_index_is_unordered(const fpta_shove_t index) {
  return !fpta_index_is_ordered(index);
}

static cxx11_constexpr bool fpta_index_is_obverse(const fpta_shove_t index) {
  return (index & fpta_index_fobverse) != 0;
}

static cxx11_constexpr bool fpta_index_is_reverse(const fpta_shove_t index) {
  return (index & fpta_index_fobverse) == 0;
}

static cxx11_constexpr bool fpta_index_is_primary(const fpta_shove_t index) {
  constexpr_assert(fpta_is_indexed(index));
  return (index & fpta_index_fsecondary) == 0;
}

static cxx11_constexpr bool fpta_index_is_secondary(const fpta_shove_t index) {
  return (index & fpta_index_fsecondary) != 0;
}

static cxx11_constexpr bool fpta_index_is_ordinal(fpta_shove_t shove) {
  return fpta_index_is_unordered(shove) ||
         (fpta_shove2type(shove) > fptu_null &&
          fpta_shove2type(shove) < fptu_cstr);
}

static cxx11_constexpr bool
fpta_is_indexed_and_nullable(const fpta_index_type index) {
  constexpr_assert(index == (index & fpta_column_index_mask));
  return index > fpta_index_fnullable;
}

static cxx11_constexpr bool fpta_column_is_nullable(const fpta_shove_t shove) {
  return (shove & fpta_index_fnullable) != 0;
}

static inline bool fpta_cursor_is_ordered(const fpta_cursor_options op) {
  return (op & (fpta_descending | fpta_ascending)) != fpta_unsorted;
}

static inline bool fpta_cursor_is_descending(const fpta_cursor_options op) {
  return (op & (fpta_descending | fpta_ascending)) == fpta_descending;
}

static inline bool fpta_cursor_is_ascending(const fpta_cursor_options op) {
  return (op & (fpta_descending | fpta_ascending)) == fpta_ascending;
}

//----------------------------------------------------------------------------

struct fpta_table_stored_schema {
  uint64_t checksum;
  uint32_t signature;
  uint32_t count;
  uint64_t version_tsn;
  fpta_shove_t columns[1];
};

static cxx11_constexpr bool fpta_is_intersected(const void *left_begin,
                                                const void *left_end,
                                                const void *right_begin,
                                                const void *right_end) {
  constexpr_assert(left_begin <= left_end);
  constexpr_assert(right_begin <= right_end);

  return !(left_begin >= right_end || right_begin >= left_end);
}

struct fpta_table_schema final {
  fpta_shove_t _key;
  unsigned _cache_hints[fpta_max_cols]; /* подсказки для кэша дескрипторов */

  static cxx11_constexpr size_t header_size() {
    return sizeof(fpta_table_stored_schema) -
           sizeof(fpta_table_stored_schema::columns);
  }

  cxx11_constexpr uint64_t checksum() const { return _stored.checksum; }
  cxx11_constexpr uint32_t signature() const { return _stored.signature; }
  cxx11_constexpr fpta_shove_t table_shove() const { return _key; }
  cxx11_constexpr uint64_t version_tsn() const { return _stored.version_tsn; }
  cxx11_constexpr size_t column_count() const { return _stored.count; }
  cxx11_constexpr fpta_shove_t column_shove(size_t number) const {
    constexpr_assert(number < _stored.count);
    return _stored.columns[number];
  }
  cxx11_constexpr const fpta_shove_t *column_shoves_array() const {
    return _stored.columns;
  }
  cxx11_constexpr fpta_shove_t table_pk() const { return column_shove(0); }

  unsigned &handle_cache(size_t number) {
    assert(number < _stored.count);
    return _cache_hints[number];
  }
  unsigned handle_cache(size_t number) const {
    assert(number < _stored.count);
    return _cache_hints[number];
  }

  typedef uint16_t composite_item_t;
  typedef const composite_item_t *composite_iter_t;
  composite_iter_t _composite_offsets;

  composite_iter_t composites_begin() const {
    return (composite_iter_t)&_stored.columns[_stored.count];
  }
  composite_iter_t composites_end() const { return _composite_offsets; }

  int composite_list(size_t number, composite_iter_t &list_begin,
                     composite_iter_t &list_end) const {
    assert(fpta_is_composite(column_shove(number)));
    const composite_iter_t composite =
        composites_begin() + _composite_offsets[number];
    if (unlikely(composite >= composites_end() || *composite < 2))
      return FPTA_EOOPS;

    list_begin = composite + 1;
    list_end = list_begin + *composite;
    assert(list_begin < list_end);
    return FPTA_SUCCESS;
  }

  cxx11_constexpr bool has_secondary() const {
    return column_count() > 1 && fpta_index_is_secondary(column_shove(1));
  }

  fpta_table_stored_schema _stored; /* must be last field (dynamic size) */
};

enum fpta_internals {
  /* используем некорретный для индекса набор флагов, чтобы в fpta_name
   * отличать таблицу от колонки, у таблицы в internal будет fpta_ftable. */
  fpta_flag_table = fpta_index_fsecondary,
  fpta_dbi_cache_size = 6619 /* простое число ближайшее
                              * к golten_ratio * fpta_max_dbi = 6627.467 */
  ,
  FTPA_SCHEMA_SIGNATURE = 1636722823,
  FTPA_SCHEMA_CHECKSEED = 67413473,
  fpta_shoved_keylen = fpta_max_keylen + 8,
  fpta_notnil_prefix_byte = 42,
  fpta_notnil_prefix_length = 1
};

//----------------------------------------------------------------------------

struct fpta_txn {
  fpta_txn(const fpta_txn &) = delete;
  fpta_db *db;
  MDBX_txn *mdbx_txn;
  fpta_level level;
  int unused_gap;
  uint64_t db_version;
  uint64_t schema_tsn_;

  uint64_t &schema_tsn() { return schema_tsn_; }
  uint64_t schema_tsn() const { return schema_tsn_; }
};

struct fpta_key {
  fpta_key() {
#ifndef NDEBUG
    fpta_pollute(this, sizeof(fpta_key), 0);
#endif
    mdbx.iov_base = nullptr; /* hush coverity */
    mdbx.iov_len = ~0u;
  }
  fpta_key(const fpta_key &) = delete;

  MDBX_val mdbx;
  union {
    int32_t i32;
    uint32_t u32;
    int64_t i64;
    uint64_t u64;
    float f32;
    double f64;

    struct {
      uint64_t head[fpta_max_keylen / sizeof(uint64_t)];
      uint64_t tailhash;
    } longkey_obverse;
    struct {
      uint64_t headhash;
      uint64_t tail[fpta_max_keylen / sizeof(uint64_t)];
    } longkey_reverse;
  } place;
};

struct fpta_cursor {
  fpta_cursor(const fpta_cursor &) = delete;
  MDBX_cursor *mdbx_cursor;
  MDBX_val current;

  struct {
    size_t results;
    size_t searches;
    size_t scans;
    size_t pk_lookups;
    size_t uniq_checks;
    size_t upserts;
    size_t deletions;
  } metrics;
  int bring(MDBX_val *key, MDBX_val *data, const MDBX_cursor_op op);

  static constexpr void *poor = nullptr;
  bool is_poor() const { return current.iov_base == poor; }
  void set_poor() { current.iov_base = poor; }

  enum eof_mode : uintptr_t { before_first = 1, after_last = 2 };

#if FPTA_ENABLE_RETURN_INTO_RANGE
  static void *eof(eof_mode mode = after_last) { return (void *)mode; }
  bool is_filled() const { return current.iov_base > eof(); }

  int unladed_state() const {
    assert(!is_filled());
    return current.iov_base ? FPTA_NODATA : FPTA_ECURSOR;
  }

  bool is_before_first() const { return current.iov_base == eof(before_first); }
  bool is_after_last() const { return current.iov_base == eof(after_last); }
  void set_eof(eof_mode mode) { current.iov_base = eof(mode); }
#else
  bool is_filled() const { return !is_poor(); }
  int unladed_state() const {
    assert(!is_filled());
    return FPTA_ECURSOR;
  }
  void set_eof(eof_mode mode) {
    (void)mode;
    set_poor();
  }
  bool is_before_first() const { return false; }
  bool is_after_last() const { return false; }
#endif

  const fpta_filter *filter;
  fpta_txn *txn;

  fpta_name *table_id;
  unsigned column_number;
  /* uint8_t */ fpta_cursor_options options;
  uint8_t seek_range_state;
  uint8_t seek_range_flags;
  MDBX_dbi tbl_handle, idx_handle;

  fpta_table_schema *table_schema() const { return table_id->table_schema; }
  fpta_shove_t index_shove() const {
    return table_schema()->column_shove(column_number);
  }

  enum : uint8_t {
    need_cmp_range_from = 1,
    need_cmp_range_to = 2,
    need_cmp_range_both = need_cmp_range_from | need_cmp_range_to,
    need_key4epsilon = 4,
  };

  fpta_key range_from_key;
  fpta_key range_to_key;
  fpta_db *db;
};

//----------------------------------------------------------------------------

MDBX_cmp_func *fpta_index_shove2comparator(fpta_shove_t shove);
MDBX_db_flags_t fpta_index_shove2primary_dbiflags(fpta_shove_t pk_shove);
MDBX_db_flags_t fpta_index_shove2secondary_dbiflags(fpta_shove_t pk_shove,
                                                    fpta_shove_t sk_shove);

bool fpta_index_is_compat(fpta_shove_t shove, const fpta_value &value);

int fpta_index_value2key(fpta_shove_t shove, const fpta_value &value,
                         fpta_key &key, bool copy = false);
int fpta_index_key2value(fpta_shove_t shove, MDBX_val mdbx_key,
                         fpta_value &key_value);

int fpta_index_row2key(const fpta_table_schema *const schema, size_t column,
                       const fptu_ro &row, fpta_key &key, bool copy = false);

int fpta_composite_row2key(const fpta_table_schema *const schema, size_t column,
                           const fptu_ro &row, fpta_key &key);

int fpta_secondary_upsert(fpta_txn *txn, fpta_table_schema *table_def,
                          MDBX_val old_pk_key, const fptu_ro &old_row,
                          MDBX_val new_pk_key, const fptu_ro &new_row,
                          const unsigned stepover);

int fpta_check_secondary_uniq(fpta_txn *txn, fpta_table_schema *table_def,
                              const fptu_ro &row_old, const fptu_ro &row_new,
                              const unsigned stepover);

int fpta_secondary_remove(fpta_txn *txn, fpta_table_schema *table_def,
                          MDBX_val &pk_key, const fptu_ro &row,
                          const unsigned stepover);

int fpta_check_nonnullable(const fpta_table_schema *table_def,
                           const fptu_ro &row);

int fpta_column_set_add(fpta_column_set *column_set, const char *column_name,
                        fptu_type data_type, fpta_index_type index_type);

int fpta_composite_index_validate(
    const fpta_index_type index_type,
    const fpta_table_schema::composite_item_t *const items_begin,
    const fpta_table_schema::composite_item_t *const items_end,
    const fpta_shove_t *const columns_shoves, const size_t column_count,
    const fpta_table_schema::composite_item_t *const composites_begin,
    const fpta_table_schema::composite_item_t *const composites_end,
    const fpta_shove_t skipself);

int fpta_name_refresh_filter(fpta_txn *txn, fpta_name *table_id,
                             fpta_filter *filter);

//----------------------------------------------------------------------------

int fpta_open_table(fpta_txn *txn, fpta_table_schema *table_def,
                    MDBX_dbi &handle);
int fpta_open_column(fpta_txn *txn, fpta_name *column_id, MDBX_dbi &tbl_handle,
                     MDBX_dbi &idx_handle);
int fpta_open_secondaries(fpta_txn *txn, fpta_table_schema *table_def,
                          MDBX_dbi *dbi_array);

fpta_cursor *fpta_cursor_alloc(fpta_db *db);
void fpta_cursor_free(fpta_db *db, fpta_cursor *cursor);

//----------------------------------------------------------------------------

int fpta_internal_abort(fpta_txn *txn, int errnum, bool txn_maybe_dead = false);

FPTA_API std::ostream &operator<<(std::ostream &out, const MDBX_val &);
FPTA_API std::ostream &operator<<(std::ostream &out, const fpta_key &);

static __inline bool fpta_is_same(const MDBX_val &a, const MDBX_val &b) {
  return a.iov_len == b.iov_len &&
         memcmp(a.iov_base, b.iov_base, a.iov_len) == 0;
}

template <typename type>
static __inline bool binary_eq(const type &a, const type &b) {
  return memcmp(&a, &b, sizeof(type)) == 0;
}

template <typename type>
static __inline bool binary_ne(const type &a, const type &b) {
  return memcmp(&a, &b, sizeof(type)) != 0;
}

//----------------------------------------------------------------------------

typedef union {
  uint32_t __i;
  float __f;
} fpta_fp32_t;

#define FPTA_DENIL_FP32_BIN UINT32_C(0xFFFFffff)
FPTA_API extern const fpta_fp32_t fpta_fp32_denil;

#define FPTA_QSNAN_FP32_BIN UINT32_C(0xFFFFfffE)
FPTA_API extern const fpta_fp32_t fpta_fp32_qsnan;

#define FPTA_DENIL_FP32x64_BIN UINT64_C(0xFFFFffffE0000000)
FPTA_API extern const fpta_fp64_t fpta_fp32x64_denil;

#define FPTA_QSNAN_FP32x64_BIN UINT64_C(0xFFFFffffC0000000)
FPTA_API extern const fpta_fp64_t fpta_fp32x64_qsnan;

#if !defined(_MSC_VER) /* MSVC provides invalid nanf(), leave it undefined */  \
    &&                                                                         \
    !defined(__LCC__) /* https://bugs.mcst.ru/bugzilla/show_bug.cgi?id=5094 */
#define FPTA_DENIL_FP32_MAS "0x007FFFFF"
#define FPTA_QSNAN_FP32_MAS "0x007FFFFE"
#define FPTA_DENIL_FP32x64_MAS "0x000FffffE0000000"
#define FPTA_QSNAN_FP32x64_MAS "0x000FffffC0000000"
#endif /* !MSVC && !LCC */

#if (__GNUC_PREREQ(3, 3) || __CLANG_PREREQ(3, 6)) &&                           \
    defined(FPTA_DENIL_FP32_MAS)
#define FPTA_DENIL_FP32 (-__builtin_nanf(FPTA_DENIL_FP32_MAS))
#define FPTA_QSNAN_FP32 (-__builtin_nanf(FPTA_QSNAN_FP32_MAS))
#else
#define FPTA_DENIL_FP32 (fpta_fp32_denil.__f)
#define FPTA_QSNAN_FP32 (fpta_fp32_qsnan.__f)
#endif
#define FPTA_DENIL_FP64 FPTA_DENIL_FP

template <fptu_type type>
static __inline bool is_fixbin_denil(const fpta_index_type index,
                                     const void *fixbin) {
  assert(fpta_is_indexed_and_nullable(index));
  const uint64_t denil = fpta_index_is_obverse(index)
                             ? FPTA_DENIL_FIXBIN_OBVERSE |
                                   (uint64_t)FPTA_DENIL_FIXBIN_OBVERSE << 8 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_OBVERSE << 16 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_OBVERSE << 24 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_OBVERSE << 32 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_OBVERSE << 40 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_OBVERSE << 48 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_OBVERSE << 56
                             : FPTA_DENIL_FIXBIN_REVERSE |
                                   (uint64_t)FPTA_DENIL_FIXBIN_REVERSE << 8 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_REVERSE << 16 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_REVERSE << 24 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_REVERSE << 32 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_REVERSE << 40 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_REVERSE << 48 |
                                   (uint64_t)FPTA_DENIL_FIXBIN_REVERSE << 56;

  /* FIXME: unaligned access */
  const uint64_t *by64 = (const uint64_t *)fixbin;
  const uint32_t *by32 = (const uint32_t *)fixbin;

  switch (type) {
  case fptu_96:
    return by64[0] == denil && by32[2] == (uint32_t)denil;
  case fptu_128:
    return by64[0] == denil && by64[1] == denil;
  case fptu_160:
    return by64[0] == denil && by64[1] == denil && by32[4] == (uint32_t)denil;
  case fptu_256:
    return by64[0] == denil && by64[1] == denil && by64[2] == denil &&
           by64[3] == denil;
  default:
    assert(false && "unexpected column type");
    __unreachable();
    return true;
  }
}

static __inline bool check_fixbin_not_denil(const fpta_index_type index,
                                            const fptu_payload *payload,
                                            const size_t bytes) {
  assert(fpta_is_indexed_and_nullable(index));
  for (size_t i = 0; i < bytes; i++)
    if (payload->fixbin[i] != (fpta_index_is_obverse(index)
                                   ? FPTA_DENIL_FIXBIN_OBVERSE
                                   : FPTA_DENIL_FIXBIN_REVERSE))
      return true;
  return false;
}

static __inline bool fpta_nullable_reverse_sensitive(const fptu_type type) {
  return type == fptu_uint16 || type == fptu_uint32 || type == fptu_uint64 ||
         (type >= fptu_96 && type <= fptu_256);
}

//------------------------------------------------------------------------------

namespace std {

inline string to_string(const MDBX_val &value) {
  ostringstream out;
  out << value;
  return out.str();
}

inline string to_string(const fpta_key &value) {
  ostringstream out;
  out << value;
  return out.str();
}

} // namespace std
