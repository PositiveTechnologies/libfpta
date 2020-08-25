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
#include <iomanip>
#include <sstream>

#include "externals/libfptu/src/erthink/erthink.h"

#if defined(_WIN32) || defined(_WIN64)
#ifdef _MSC_VER
#pragma warning(push, 1)
#endif
#include <windows.h>
#ifdef _MSC_VER
#pragma warning(pop)
#endif
#endif /* must die */

#define FIXME "FIXME: " __FILE__ ", " FPT_STRINGIFY(__LINE__)

#if __GNUC_PREREQ(8, 0) && !__GNUC_PREREQ(9, 0)
/* LY: workaround to false-positive warnings about __cold */
#pragma GCC diagnostic ignored "-Wattributes"
#endif /* GCC 8.x */

using fptu::string_view;

static __cold const char *error2cp(int32_t errcode) {
#if defined(_WIN32) || defined(_WIN64)
  static_assert(FPTA_ENOMEM == ERROR_OUTOFMEMORY, "error code mismatch");
  static_assert(FPTA_ENOIMP == ERROR_NOT_SUPPORTED, "error code mismatch");
  static_assert(FPTA_EVALUE == ERROR_INVALID_DATA, "error code mismatch");
  static_assert(FPTA_OVERFLOW == ERROR_ARITHMETIC_OVERFLOW,
                "error code mismatch");
  static_assert(FPTA_EEXIST == ERROR_ALREADY_EXISTS, "error code mismatch");
  static_assert(FPTA_ENOENT == ERROR_NOT_FOUND, "error code mismatch");
  static_assert(FPTA_EPERM == ERROR_INVALID_FUNCTION, "error code mismatch");
  static_assert(FPTA_EBUSY == ERROR_BUSY, "error code mismatch");
  static_assert(FPTA_ENAME == ERROR_INVALID_NAME, "error code mismatch");
  static_assert(FPTA_EFLAG == ERROR_INVALID_FLAG_NUMBER, "error code mismatch");
#endif /* static_asserts for Windows */

  static const char *const msgs[] = {
      "FPTA_EOOPS: Internal unexpected Oops",
      "FPTA_SCHEMA_CORRUPTED: Schema is invalid or corrupted",
      "FPTA_ETYPE: Type mismatch (given value vs column/field or index",
      "FPTA_DATALEN_MISMATCH: Data length mismatch (given value vs data type",
      "FPTA_KEY_MISMATCH: Key mismatch while updating row via cursor",
      "FPTA_COLUMN_MISSING: Required column missing",
      "FPTA_INDEX_CORRUPTED: Index is inconsistent or corrupted",
      "FPTA_NO_INDEX: No (such) index for given column",
      "FPTA_SCHEMA_CHANGED: Schema changed (transaction should be restared",
      "FPTA_ECURSOR: Cursor is not positioned",
      "FPTA_TOOMANY: Too many tables, columns or indexes (one of libfpta's "
      "limits reached)",
      "FPTA_WANNA_DIE: Failure while transaction rollback (wanna die)",
      "FPTA_TXN_CANCELLED: Transaction already cancelled",
      "FPTA_SIMILAR_INDEX: Adding index which is similar to one of the "
      "existing",
      "FPTA_TARDY_DBI: Another thread still use handle(s) that should be "
      "reopened",
      "FPTA_CLUMSY_INDEX: Adding index which is too clumsy",
      "FPTA_FORMAT_MISMATCH: Database format mismatch the libfpta version",
      "FPTA_APP_MISMATCH: Applicaton version mismatch the database content"};

  static_assert(erthink::array_length(msgs) ==
                    FPTA_ERRROR_LAST - FPTA_ERRROR_BASE,
                "WTF?");

  switch (errcode) {
  case FPTA_SUCCESS:
    return "FPTA_SUCCESS";
  case FPTA_NODATA:
    return "FPTA_NODATA: No data or EOF was reached";
  case int32_t(FPTA_DEADBEEF):
    return "FPTA_DEADBEEF: No value returned";
  default:
    if (errcode > FPTA_ERRROR_BASE && errcode <= FPTA_ERRROR_LAST)
      return msgs[errcode - FPTA_ERRROR_BASE - 1];
  }
  return nullptr;
}

__cold const char *fpta_strerror(int errcode) {
  const char *msg = error2cp(errcode);
  return msg ? msg : mdbx_strerror(errcode);
}

__cold const char *fpta_strerror_r(int errcode, char *buf, size_t buflen) {
  const char *msg = error2cp(errcode);
  return msg ? msg : mdbx_strerror_r(errcode, buf, buflen);
}

//------------------------------------------------------------------------------

namespace std {

static __cold ostream &invalid(ostream &out, const char *name,
                               const intptr_t value) {
  return out << "invalid(fpta::" << name << "=" << value << ")";
}

#define FPTA_TOSTRING_IMP(type)                                                \
  __cold string to_string(type value) {                                        \
    ostringstream out;                                                         \
    out << value;                                                              \
    return out.str();                                                          \
  }

static __cold string_view error2sv(const fpta_error value) {
  return string_view(fpta_strerror(value));
}
__cold ostream &operator<<(ostream &out, const fpta_error value) {
  return out << error2sv(value);
}
__cold string to_string(const fpta_error value) {
  return string(error2sv(value));
}

static __cold string_view value_type2sv(const fpta_value_type value) {
  static const char *const names[] = {"null",     "signed_int",  "unsigned_int",
                                      "datetime", "float_point", "string",
                                      "binary",   "shoved",      "<begin>",
                                      "<end>",    "<epsilon>"};

  static_assert(erthink::array_length(names) == size_t(fpta_invalid), "WTF?");
  return (value >= fpta_null && value < fpta_invalid)
             ? string_view(names[size_t(value) - fpta_null])
             : string_view("invalid");
}
__cold ostream &operator<<(ostream &out, const fpta_value_type value) {
  return out << value_type2sv(value);
}
__cold string to_string(const fpta_value_type value) {
  return string(value_type2sv(value));
}

__cold ostream &operator<<(ostream &out, const fpta_value *value) {
  if (!value)
    return out << "nullptr";

  out << value->type;
  switch (value->type) {
  default:
    assert(false);
  case fpta_null:
  case fpta_begin:
  case fpta_end:
  case fpta_epsilon:
    return out;

  case fpta_signed_int:
    if (value->sint >= 0)
      out << "+";
    return out << value->sint;

  case fpta_unsigned_int:
    return out << value->uint;

  case fpta_datetime:
    return out << value->datetime;

  case fpta_float_point:
    return out << erthink::output_double<true>(value->fp);

  case fpta_string:
    return out << "\""
               << string_view((const char *)value->binary_data,
                              value->binary_length)
               << "\"";

  case fpta_binary:
    return out << fptu::output_hexadecimal(value->binary_data,
                                           value->binary_length);

  case fpta_shoved:
    return out << "@"
               << fptu::output_hexadecimal(value->binary_data,
                                           value->binary_length);
  }
}
FPTA_TOSTRING_IMP(const fpta_value *);

__cold ostream &operator<<(ostream &out, const fpta_durability value) {
  switch (value) {
  default:
    return invalid(out, "durability", value);
  case fpta_readonly:
    return out << "mode-readonly";
  case fpta_sync:
    return out << "mode-sync";
  case fpta_lazy:
    return out << "mode-lazy";
  case fpta_weak:
    return out << "mode-weak";
  }
}
FPTA_TOSTRING_IMP(const fpta_durability &);

__cold ostream &operator<<(ostream &out, const fpta_level value) {
  switch (value) {
  default:
    return invalid(out, "level", value);
  case fpta_read:
    return out << "level-read";
  case fpta_write:
    return out << "level-write";
  case fpta_schema:
    return out << "level-schema";
  }
}
FPTA_TOSTRING_IMP(const fpta_level &);

__cold ostream &operator<<(ostream &out, const fpta_index_type value) {
  if (unlikely(!fpta_index_is_valid(value)))
    return invalid(out, "index", value);

  if (!fpta_is_indexed(value))
    out << "noindex";
  else {
    out << (fpta_index_is_primary(value) ? "primary" : "secondary")
        << (fpta_index_is_unique(value) ? "-unique" : "-withdups")
        << (fpta_index_is_ordered(value) ? "-ordered" : "-unordered")
        << (fpta_index_is_obverse(value) ? "-obverse" : "-reverse");
  }
  if (fpta_column_is_nullable(value))
    out << ".nullable";
  return out;
}
FPTA_TOSTRING_IMP(const fpta_index_type);

__cold ostream &operator<<(ostream &out, const fpta_filter_bits value) {
  switch (value) {
  default:
    return invalid(out, "filter_bits", value);
  case fpta_node_not:
    return out << "NOT";
  case fpta_node_or:
    return out << "OR";
  case fpta_node_and:
    return out << "AND";
  case fpta_node_fncol:
    return out << "FN_COLUMN()";
  case fpta_node_fnrow:
    return out << "FN_ROW()";
  case fpta_node_lt:
  case fpta_node_gt:
  case fpta_node_le:
  case fpta_node_ge:
  case fpta_node_eq:
  case fpta_node_ne:
    return out << fptu_lge(value);
  }
}
FPTA_TOSTRING_IMP(const fpta_filter_bits);

__cold ostream &operator<<(ostream &out, const fpta_cursor_options value) {
  switch (value & ~(fpta_dont_fetch | fpta_zeroed_range_is_point)) {
  default:
    return invalid(out, "cursor_options", value);
  case fpta_unsorted:
    out << "unsorted";
    break;
  case fpta_ascending:
    out << "ascending";
    break;
  case fpta_descending:
    out << "descending";
    break;
  }
  if (value & fpta_zeroed_range_is_point)
    out << ".zeroed_range_is_point";
  if (value & fpta_dont_fetch)
    out << ".dont_fetch";
  return out;
}
FPTA_TOSTRING_IMP(const fpta_cursor_options);

__cold ostream &operator<<(ostream &out, const fpta_seek_operations value) {
  switch (value) {
  default:
    return invalid(out, "seek_operations", value);
  case fpta_first:
    return out << "row.first";
  case fpta_last:
    return out << "row.last";
  case fpta_next:
    return out << "row.next";
  case fpta_prev:
    return out << "row.prev";

  case fpta_dup_first:
    return out << "dup.first";
  case fpta_dup_last:
    return out << "dup.last";
  case fpta_dup_next:
    return out << "dup.next";
  case fpta_dup_prev:
    return out << "dup.prev";

  case fpta_key_next:
    return out << "key.next";
  case fpta_key_prev:
    return out << "key.prev";
  }
}
FPTA_TOSTRING_IMP(const fpta_seek_operations);

__cold ostream &operator<<(ostream &out, const fpta_put_options value) {
  switch (value & ~fpta_skip_nonnullable_check) {
  default:
    return invalid(out, "put_options", value);
  case fpta_insert:
    out << "insert";
    break;
  case fpta_update:
    out << "update";
    break;
  case fpta_upsert:
    out << "upsert";
    break;
  }
  if (value & fpta_skip_nonnullable_check)
    out << ".skip_nonnullable_check";
  return out;
}
FPTA_TOSTRING_IMP(const fpta_put_options);

__cold ostream &operator<<(ostream &out, const fpta_name *value) {
  out << "name_";
  if (!value)
    return out << "nullptr";

  const bool is_table =
      fpta_shove2index(value->shove) == fpta_index_type(fpta_flag_table);

  out << (is_table ? "table." : "column.") << static_cast<const void *>(value)
      << "@{" << std::hex << value->shove << std::dec << ", v"
      << value->version_tsn;

  if (is_table) {
    const fpta_table_schema *table_def = value->table_schema;
    if (table_def == nullptr)
      return out << ", no-schema}";

    const fpta_index_type index = fpta_shove2index(table_def->table_pk());
    const fptu_type type = fpta_shove2type(table_def->table_pk());
    return out << ", " << index << "." << type << ", dbi-hint#"
               << table_def->handle_cache(0) << "}";
  }

  const fpta_name *table_id = value->column.table;
  if (!table_id)
    return out << ", orphan}";

  const fpta_table_schema *table_def = table_id->table_schema;
  if (!table_def)
    return out << ", table." << static_cast<const void *>(table_id) << "@"
               << std::hex << table_id->shove << std::dec << ", no-schema}";

  const fpta_index_type index = fpta_name_colindex(value);
  out << ", col#" << value->column.num << ", table."
      << static_cast<const void *>(table_id) << "@" << std::hex
      << table_id->shove << std::dec << ", " << index << ", ";

  if (fpta_column_is_composite(value))
    out << "composite";
  else
    out << fpta_name_coltype(value);
  return out << ", dbi-hint#" << table_def->handle_cache(value->column.num)
             << "}";
}
FPTA_TOSTRING_IMP(const fpta_name *);

__cold ostream &operator<<(ostream &out, const fpta_filter *filter) {
  if (!filter)
    return out << "TRUE";

  switch (filter->type) {
  default:
    return invalid(out, "filter-type", filter->type);
  case fpta_node_not:
    return out << "NOT (" << filter->node_not << ")";
  case fpta_node_or:
    return out << "(" << filter->node_or.a << " OR " << filter->node_or.b
               << ")";
  case fpta_node_and:
    return out << "(" << filter->node_or.a << " AND " << filter->node_or.b
               << ")";
  case fpta_node_fncol:
    return out << "FN_COLUMN." << filter->node_fncol.predicate << "("
               << filter->node_fncol.column_id << ", arg."
               << filter->node_fncol.arg << ")";
  case fpta_node_fnrow:
    return out << "FN_ROW." << filter->node_fnrow.predicate << "(context."
               << filter->node_fnrow.context << ", arg."
               << filter->node_fnrow.arg << ")";

  case fpta_node_lt:
  case fpta_node_gt:
  case fpta_node_le:
  case fpta_node_ge:
  case fpta_node_eq:
  case fpta_node_ne:
    return out << filter->node_cmp.left_id << " " << fptu_lge(filter->type)
               << " " << filter->node_cmp.right_value;
  }
}
FPTA_TOSTRING_IMP(const fpta_filter *);

__cold ostream &operator<<(ostream &out, const fpta_column_set *value) {
  return out << "column_set." << static_cast<const void *>(value) << "@" FIXME;
}
FPTA_TOSTRING_IMP(const fpta_column_set *);

__cold ostream &operator<<(ostream &out, const fpta_db *value) {
  return out << "db." << static_cast<const void *>(value) << "@" FIXME;
}
FPTA_TOSTRING_IMP(const fpta_db *);

__cold ostream &operator<<(ostream &out, const fpta_txn *value) {
  return out << "txn." << static_cast<const void *>(value) << "@" FIXME;
}
FPTA_TOSTRING_IMP(const fpta_txn *);

__cold ostream &operator<<(ostream &out, const fpta_cursor *cursor) {
  out << "cursor.";
  if (!cursor)
    return out << "nullptr";

  out << static_cast<const void *>(cursor)
      << "={\n"
         "\tmdbx "
      << static_cast<const void *>(cursor->mdbx_cursor)
      << ",\n"
         "\toptions "
      << cursor->options;

  if (cursor->is_filled())
    out << ",\n"
           "\tcurrent "
        << cursor->current;
  else if (cursor->is_before_first())
    out << ",\n"
           "\tstate before-first (FPTA_NODATA)";
  else if (cursor->is_after_last())
    out << ",\n"
           "\tstate after-last (FPTA_NODATA)";
  else
    out << ",\n"
           "\tstate non-positioned (FPTA_ECURSOR)";

  const fpta_shove_t shove = cursor->index_shove();
  const fpta_index_type index = fpta_shove2index(shove);
  const fptu_type type = fpta_shove2type(shove);

  out << ",\n"
         "\t"
      << cursor->table_id
      << ",\n"
         "\tindex {@"
      << hex << shove << dec << ", " << index << ", ";
  if (type)
    out << type;
  else
    out << "composite";

  return out << ", col#" << cursor->column_number << ", dbi#"
             << cursor->tbl_handle << "_" << cursor->idx_handle
             << "},\n"
                "\trange-from-key "
             << cursor->range_from_key
             << ",\n"
                "\trange-to-key "
             << cursor->range_to_key
             << ",\n"
                "\tfilter "
             << cursor->filter
             << ",\n"
                "\ttxn "
             << cursor->txn
             << ",\n"
                "\tdb "
             << cursor->db << "\n}";
}
FPTA_TOSTRING_IMP(const fpta_cursor *);

__cold ostream &operator<<(ostream &out, const struct fpta_table_schema *def) {
  out << "table_schema.";
  if (!def)
    return out << "nullptr";

  out << static_cast<const void *>(def) << "={v" << def->version_tsn() << ", $"
      << hex << def->signature() << "_" << def->checksum() << ", @"
      << def->table_shove() << dec << ", " << def->column_count() << "=[";

  fptu::format("%p={v%" PRIu64 ", $%" PRIx32 "_%" PRIx64 ", @%" PRIx64
               ", %" PRIuSIZE "=[",
               def, def->version_tsn(), def->signature(), def->checksum(),
               def->table_shove(), def->column_count());

  for (size_t i = 0; i < def->column_count(); ++i) {
    const fpta_shove_t shove = def->column_shove(i);
    const fpta_index_type index = fpta_shove2index(shove);
    const fptu_type type = fpta_shove2type(shove);
    if (i)
      out << ", ";
    out << "{@" << hex << shove << ", " << index << ", ";
    if (type)
      out << type;
    else
      out << "composite";
    out << "}";
  }

  return out << "]}";
}
FPTA_TOSTRING_IMP(const fpta_table_schema *);

FPTA_API ostream &operator<<(ostream &out, const MDBX_val &value) {
  out << value.iov_len << "_" << value.iov_base;
  if (value.iov_len && value.iov_base)
    out << "=" << fptu::output_hexadecimal(value.iov_base, value.iov_len);
  return out;
}
FPTA_TOSTRING_IMP(const MDBX_val &);

FPTA_API ostream &operator<<(ostream &out, const fpta_key &value) {
  if (value.mdbx.iov_len) {
    const char *const begin = static_cast<const char *>(value.mdbx.iov_base);
    const char *const end = begin + value.mdbx.iov_len;
    const char *const inplace_begin =
        reinterpret_cast<const char *>(&value.place);
    const char *const inplace_end = inplace_begin + sizeof(value.place);
    if ((begin >= inplace_begin && begin < inplace_end) ||
        (end > inplace_begin && end <= inplace_end)) {
      out << "inplace_";
      if (begin == inplace_begin && end == inplace_end)
        out << "whole_";
      else if (begin > inplace_begin && end < inplace_end)
        out << "middle_";
      else if (begin == inplace_begin && end < inplace_end)
        out << "head_";
      else if (begin > inplace_begin && end == inplace_end)
        out << "tail_";
      else
        out << "invalid_";
    }
  } else {
    out << "empty_";
  }
  return out << value.mdbx;
}
FPTA_TOSTRING_IMP(const fpta_key &);

} // namespace std

//------------------------------------------------------------------------------

int_fast32_t mrand64(void) {
  static uint_fast64_t state;
  state = state * UINT64_C(6364136223846793005) + UINT64_C(1442695040888963407);
  return (int_fast32_t)(state >> 32);
}

void fpta_pollute(void *ptr, size_t bytes, uintptr_t xormask) {
  if (xormask) {
    while (bytes >= sizeof(uintptr_t)) {
      *((uintptr_t *)ptr) ^= xormask;
      ptr = (char *)ptr + sizeof(uintptr_t);
      bytes -= sizeof(uintptr_t);
    }

    if (bytes) {
      uintptr_t tail;
      memcpy(&tail, ptr, bytes);
      tail ^= xormask;
      memcpy(ptr, &tail, bytes);
    }
  } else {
    while (bytes >= sizeof(uint32_t)) {
      *((uint32_t *)ptr) = (uint32_t)mrand48();
      ptr = (char *)ptr + sizeof(uint32_t);
      bytes -= sizeof(uint32_t);
    }

    if (bytes) {
      uint32_t tail = (uint32_t)mrand48();
      memcpy(ptr, &tail, bytes);
    }
  }
}

//----------------------------------------------------------------------------

#ifdef __SANITIZE_ADDRESS__
extern "C" FPTA_API __attribute__((weak)) const char *__asan_default_options() {
  return "symbolize=1:allow_addr2line=1:"
#ifdef _DEBUG
         "debug=1:"
#endif /* _DEBUG */
         "report_globals=1:"
         "replace_str=1:replace_intrin=1:"
         "malloc_context_size=9:"
         "detect_leaks=1:"
         "check_printf=1:"
         "detect_deadlocks=1:"
#ifndef LTO_ENABLED
         "check_initialization_order=1:"
#endif
         "detect_stack_use_after_return=1:"
         "intercept_tls_get_addr=1:"
         "decorate_proc_maps=1:"
         "abort_on_error=1";
}
#endif /* __SANITIZE_ADDRESS__ */
