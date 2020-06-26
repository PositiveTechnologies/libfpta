/*
 *  Fast Positive Tuples (libfptu), aka Позитивные Кортежи
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

#if defined(_MSC_VER) && !defined(_CRT_SECURE_NO_WARNINGS)
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "erthink/erthink.h"
#include "fast_positive/tuples_internal.h"

#ifdef _MSC_VER
#pragma warning(push, 1)
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                   semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling  \
                                    mode specified; termination on exception   \
                                    is not guaranteed. Specify /EHsc */
#endif                          /* _MSC_VER (warnings) */
#include <iomanip>
#include <sstream>
#ifdef _MSC_VER
#pragma warning(pop)
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                   semantics are not enabled. Specify /EHsc */
#endif

bool fptu_is_under_valgrind(void) {
#ifdef RUNNING_ON_VALGRIND
  if (RUNNING_ON_VALGRIND)
    return true;
#endif
  const char *str = getenv("RUNNING_ON_VALGRIND");
  if (str)
    return strcmp(str, "0") != 0;
  return false;
}

namespace {

/* access to std::streambuf protected methods */
class streambuf_helper : public std::streambuf {
  static const streambuf_helper *cast(const std::streambuf *sb) {
    return reinterpret_cast<const streambuf_helper *>(sb);
  }
  static streambuf_helper *cast(std::streambuf *sb) {
    return reinterpret_cast<streambuf_helper *>(sb);
  }

public:
  streambuf_helper() = delete;
  static ptrdiff_t space_avail(const std::streambuf *sb) {
    return cast(sb)->epptr() - cast(sb)->pptr();
  }
  static ptrdiff_t space_whole(const std::streambuf *sb) {
    return cast(sb)->epptr() - cast(sb)->pbase();
  }
  static char *ptr(const std::streambuf *sb) { return cast(sb)->pptr(); }
  static void bump(std::streambuf *sb, int n) { return cast(sb)->pbump(n); }
};

} // namespace

namespace fptu {

__cold std::string format_va(const char *fmt, va_list ap) {
  va_list ones;
  va_copy(ones, ap);
#ifdef _MSC_VER
  int needed = _vscprintf(fmt, ap);
#else
  int needed = vsnprintf(nullptr, 0, fmt, ap);
#endif
  assert(needed >= 0);
  std::string result;
  result.reserve((size_t)needed + 1);
  result.resize((size_t)needed, '\0');
  assert((int)result.capacity() > needed);
  int actual = vsnprintf((char *)result.data(), result.capacity(), fmt, ones);
  assert(actual == needed);
  (void)actual;
  va_end(ones);
  return result;
}

__cold std::ostream &format_va(std::ostream &out, const char *fmt, va_list ap) {
  if (likely(!out.bad())) {
    va_list ones;
    va_copy(ones, ap);
#ifdef _MSC_VER
    int needed = _vscprintf(fmt, ap);
#else
    int needed = vsnprintf(nullptr, 0, fmt, ap);
#endif
    if (likely(needed > 0)) {
      std::streambuf *buf = out.rdbuf();
      if (likely(buf)) {
        const ptrdiff_t avail = streambuf_helper::space_avail(buf);
        if (avail >= needed) {
          int actual =
              vsnprintf(streambuf_helper::ptr(buf), size_t(avail), fmt, ones);
          assert(actual == needed);
          streambuf_helper::bump(buf, actual);
          fmt = nullptr;
        } else if (needed > 42) {
          const ptrdiff_t whole = streambuf_helper::space_whole(buf);
          if (whole >= needed) {
            out.flush();
            if (!out.bad()) {
              assert(streambuf_helper::space_avail(buf) == whole);
              int actual =
                  vsnprintf(streambuf_helper::ptr(buf),
                            streambuf_helper::space_avail(buf), fmt, ones);
              assert(actual == needed);
              streambuf_helper::bump(buf, actual);
              fmt = nullptr;
            }
          }
        }
      }

      if (fmt) {
        if (needed < 1024) {
          char tmp[1024];
          int actual = vsnprintf(tmp, erthink::array_length(tmp), fmt, ones);
          assert(actual == needed);
          out.write(tmp, actual);
        } else {
          std::string tmp;
          tmp.reserve((size_t)needed + 1);
          tmp.resize((size_t)needed, '\0');
          assert((int)tmp.capacity() > needed);
          int actual = vsnprintf((char *)tmp.data(), tmp.capacity(), fmt, ones);
          assert(actual == needed);
          out.write(tmp.data(), actual);
        }
      }
    }

    va_end(ones);
  }
  return out;
}

__cold std::string format(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  std::string result = format_va(fmt, ap);
  va_end(ap);
  return result;
}

__cold std::ostream &format(std::ostream &out, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  format_va(out, fmt, ap);
  va_end(ap);
  return out;
}

__cold std::string hexadecimal_string(const void *data, size_t bytes) {
  std::string result;
  if (bytes > 0) {
    result.reserve(bytes * 2);
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *const end = ptr + bytes;
    do {
      char high = *ptr >> 4;
      char low = *ptr & 15;
      result.push_back((high < 10) ? high + '0' : high - 10 + 'a');
      result.push_back((low < 10) ? low + '0' : low - 10 + 'a');
    } while (++ptr < end);
  }
  return result;
}

__cold std::ostream &hexadecimal_dump(std::ostream &out, const void *data,
                                      size_t bytes) {
  if (bytes > 0) {
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *const end = ptr + bytes;
    do {
      char high = *ptr >> 4;
      char low = *ptr & 15;
      out.put((high < 10) ? high + '0' : high - 10 + 'a');
      out.put((low < 10) ? low + '0' : low - 10 + 'a');
    } while (++ptr < end);
  }
  return out;
}

bad_tuple::bad_tuple(const fptu_ro &ro)
    : std::invalid_argument(
          format("fptu: Invalid ro-tuple '%s'", fptu_check_ro(ro))) {}

bad_tuple::bad_tuple(const fptu_rw *rw)
    : std::invalid_argument(
          format("fptu: Invalid rw-tuple '%s'", fptu_check_rw(rw))) {}

__cold void throw_error(fptu_error err) {
  assert(err != FPTU_SUCCESS);
  switch (err) {
  case FPTU_SUCCESS:
    return;
  case FPTU_ENOFIELD:
    throw std::runtime_error("fptu: No such field");
  case FPTU_EINVAL:
    throw std::invalid_argument("fptu: Invalid agrument");
  case FPTU_ENOSPACE:
    throw std::runtime_error("fptu: No space for field or value");
  default:
    throw std::runtime_error(strerror(err));
  }
}

cxx14_constexpr string_view::const_reference
string_view::at(string_view::size_type pos) const {
  if (unlikely(pos >= size()))
    throw std::out_of_range("fptu::string_view::at(): pos >= size()");
  return str[pos];
}

} /* namespace fptu */

//----------------------------------------------------------------------------

__cold const char *fptu_type_name(const fptu_type value) {
  switch (int(/* hush 'not in enumerated' */ value)) {
  default: {
    static __thread char buf[32];
    snprintf(buf, sizeof(buf), "invalid(fptu::type=%i)", int(value));
    return buf;
  }
  case fptu_null:
    return "null";
  case fptu_uint16:
    return "uint16";
  case fptu_int32:
    return "int32";
  case fptu_uint32:
    return "uint32";
  case fptu_fp32:
    return "fp32";
  case fptu_int64:
    return "int64";
  case fptu_uint64:
    return "uint64";
  case fptu_fp64:
    return "fp64";
  case fptu_datetime:
    return "datetime";
  case fptu_96:
    return "b96";
  case fptu_128:
    return "b128";
  case fptu_160:
    return "b160";
  case fptu_256:
    return "b256";
  case fptu_cstr:
    return "cstr";
  case fptu_opaque:
    return "opaque";
  case fptu_nested:
    return "nested";

  case fptu_farray:
    return "invalid-null[]";
  case fptu_array_uint16:
    return "uint16[]";
  case fptu_array_int32:
    return "int32[]";
  case fptu_array_uint32:
    return "uint32[]";
  case fptu_array_fp32:
    return "fp32[]";
  case fptu_array_int64:
    return "int64[]";
  case fptu_array_uint64:
    return "uint64[]";
  case fptu_array_fp64:
    return "fp64[]";
  case fptu_array_datetime:
    return "datetime[]";
  case fptu_array_96:
    return "b96[]";
  case fptu_array_128:
    return "b128[]";
  case fptu_array_160:
    return "b160[]";
  case fptu_array_256:
    return "b256[]";
  case fptu_array_cstr:
    return "cstr[]";
  case fptu_array_opaque:
    return "opaque[]";
  case fptu_array_nested:
    return "nested[]";
  }
}

namespace std {

static __cold ostream &invalid(ostream &out, const char *name,
                               const intptr_t value) {
  return out << "invalid(fptu::" << name << "=" << value << ")";
}

#define FPTU_TOSTRING_IMP(type)                                                \
  __cold string to_string(type value) {                                        \
    ostringstream out;                                                         \
    out << value;                                                              \
    return out.str();                                                          \
  }

__cold ostream &operator<<(ostream &out, const fptu_error value) {
  switch (value) {
  case FPTU_SUCCESS:
    return out << "FPTU: Success";
  case FPTU_ENOFIELD:
    return out << "FPTU: No such field (ENOENT)";
  case FPTU_EINVAL:
    return out << "FPTU: Invalid argument (EINVAL)";
  case FPTU_ENOSPACE:
    return out << "FPTU: No space left in tuple (ENOSPC)";
  default:
    return invalid(out, "error", value);
  }
}
FPTU_TOSTRING_IMP(const fptu_error)

__cold ostream &operator<<(ostream &out, const fptu_type value) {
  return out << fptu_type_name(value);
}
__cold string to_string(const fptu_type value) {
  return std::string(fptu_type_name(value));
}

template <typename native>
static inline void output_array_native(ostream &out,
                                       const fptu_payload *const payload,
                                       const char *const tuple_end) {
  const char *const array_begin =
      erthink::constexpr_pointer_cast<const char *>(&payload->other.data[1]);
  const char *const array_end = erthink::constexpr_pointer_cast<const char *>(
      &payload->other.data[units2bytes(payload->other.varlen.brutto)]);
  const char *const detent =
      (tuple_end && array_end > tuple_end) ? tuple_end : array_end;
  const native *item =
      erthink::constexpr_pointer_cast<const native *>(array_begin);
  for (unsigned i = 0; i < payload->other.varlen.array_length; ++i, ++item) {
    if (i)
      out << ",";
    if (unlikely(erthink::constexpr_pointer_cast<const char *>(item) >=
                 detent)) {
      out << ((tuple_end &&
               erthink::constexpr_pointer_cast<const char *>(item) > tuple_end)
                  ? "<broken-tuple>"
                  : "<broken-array>");
      break;
    }
    out << *item;
  }
}

__cold static void output_array_fixbin(ostream &out,
                                       const fptu_payload *payload,
                                       const unsigned itemsize,
                                       const char *const tuple_end) {
  const char *const array_begin =
      erthink::constexpr_pointer_cast<const char *>(&payload->other.data[1]);
  const char *const array_end = erthink::constexpr_pointer_cast<const char *>(
      &payload->other.data[units2bytes(payload->other.varlen.brutto)]);
  const char *const detent =
      (tuple_end && array_end > tuple_end) ? tuple_end : array_end;
  const char *item = array_begin;
  for (unsigned i = 0; i < payload->other.varlen.array_length;
       ++i, item += itemsize) {
    if (i)
      out << ",";
    if (unlikely(item >= detent)) {
      out << ((tuple_end && item > tuple_end) ? "<broken-tuple>"
                                              : "<broken-array>");
      break;
    }
    out << fptu::output_hexadecimal(item, itemsize);
  }
}

__cold ostream &operator<<(ostream &out, const fptu_field &field) {
  const fptu_type type_complete = field.type();
  const fptu_type type_base = fptu_type(int(type_complete) & ~int(fptu_farray));
  const auto payload = field.payload();
  const char *const tuple_end = nullptr /* TODO */;

  out << "{" << field.colnum() << "."
      << ((type_complete != fptu_farray) ? fptu_type_name(type_base)
                                         : "invalid-null");
  if (type_complete & fptu_farray)
    out << "[" << payload->other.varlen.array_length << "("
        << units2bytes(payload->other.varlen.brutto) << ")]";

  if (type_base != fptu_null) {
    out << "=";

    switch (int(/* hush 'not in enumerated' */ type_complete)) {
    case fptu_uint16:
      out << field.get_payload_uint16();
      break;
    case fptu_int32:
      out << payload->i32;
      break;
    case fptu_uint32:
      out << payload->u32;
      break;
    case fptu_fp32:
      out << payload->fp32;
      break;
    case fptu_int64:
      out << payload->i64;
      break;
    case fptu_uint64:
      out << payload->u64;
      break;
    case fptu_fp64:
      out << payload->fp64;
      break;

    case fptu_datetime:
      out << payload->dt;
      break;

    case fptu_96:
      out << fptu::output_hexadecimal(payload->fixbin, 96 / 8);
      break;
    case fptu_128:
      out << fptu::output_hexadecimal(payload->fixbin, 128 / 8);
      break;
    case fptu_160:
      out << fptu::output_hexadecimal(payload->fixbin, 160 / 8);
      break;
    case fptu_256:
      out << fptu::output_hexadecimal(payload->fixbin, 256 / 8);
      break;

    case fptu_cstr:
      out << payload->cstr;
      break;
    case fptu_opaque:
      out << fptu::output_hexadecimal(payload->other.data,
                                      payload->other.varlen.opaque_bytes);
      break;

    case fptu_nested:
      out << fptu_field_nested(&field);
      break;

    case fptu_array_uint16:
      output_array_native<uint16_t>(out, payload, tuple_end);
      break;
    case fptu_array_int32:
      output_array_native<int32_t>(out, payload, tuple_end);
      break;
    case fptu_array_uint32:
      output_array_native<uint32_t>(out, payload, tuple_end);
      break;
    case fptu_array_fp32:
      output_array_native<float>(out, payload, tuple_end);
      break;
    case fptu_array_int64:
      output_array_native<int64_t>(out, payload, tuple_end);
      break;
    case fptu_array_uint64:
      output_array_native<uint64_t>(out, payload, tuple_end);
      break;
    case fptu_array_fp64:
      output_array_native<double>(out, payload, tuple_end);
      break;
    case fptu_array_datetime:
      output_array_native<fptu_time>(out, payload, tuple_end);
      break;

    case fptu_array_96:
      output_array_fixbin(out, payload, 96 / 8, tuple_end);
      break;
    case fptu_array_128:
      output_array_fixbin(out, payload, 128 / 8, tuple_end);
      break;
    case fptu_array_160:
      output_array_fixbin(out, payload, 160 / 8, tuple_end);
      break;
    case fptu_array_256:
      output_array_fixbin(out, payload, 256 / 8, tuple_end);
      break;

    case fptu_array_cstr: {
      const char *const array_begin =
          erthink::constexpr_pointer_cast<const char *>(
              &payload->other.data[1]);
      const char *const array_end =
          erthink::constexpr_pointer_cast<const char *>(
              &payload->other.data[units2bytes(payload->other.varlen.brutto)]);
      const char *const detent =
          (tuple_end && array_end > tuple_end) ? tuple_end : array_end;
      const char *item = array_begin;
      for (unsigned i = 0; i < payload->other.varlen.array_length; ++i) {
        if (i)
          out << ",";
        if (unlikely(item >= detent)) {
          out << ((tuple_end && item > tuple_end) ? "<broken-tuple>"
                                                  : "<broken-array>");
          break;
        }
        out << item;
        item += strlen(item) + 1;
      }
    } break;

    case fptu_array_opaque:
    case fptu_array_nested: {
      const char *const array_begin =
          erthink::constexpr_pointer_cast<const char *>(
              &payload->other.data[1]);
      const char *const array_end =
          erthink::constexpr_pointer_cast<const char *>(
              &payload->other.data[units2bytes(payload->other.varlen.brutto)]);
      const fptu_unit *const detent =
          erthink::constexpr_pointer_cast<const fptu_unit *>(
              (tuple_end && array_end > tuple_end) ? tuple_end : array_end);
      const fptu_unit *item =
          erthink::constexpr_pointer_cast<const fptu_unit *>(array_begin);
      for (unsigned i = 0; i < payload->other.varlen.array_length; ++i) {
        if (i)
          out << ",";
        if (unlikely(item >= detent)) {
          out << ((tuple_end && erthink::constexpr_pointer_cast<const char *>(
                                    item) > tuple_end)
                      ? "<broken-tuple>"
                      : "<broken-array>");
          break;
        }
        if (type_complete == fptu_array_nested)
          out << fptu_field_nested(&item->field);
        else
          out << fptu::output_hexadecimal(item->field.body,
                                          item->varlen.opaque_bytes);
        item += item->varlen.brutto;
      }
    } break;

    default:
      assert(false);
      __unreachable();
      break;
    }
  }
  return out << "}";
}

FPTU_TOSTRING_IMP(const fptu_field &)

__cold ostream &operator<<(ostream &out, const fptu_ro &ro) {
  const fptu_field *const begin = fptu::begin(ro);
  const fptu_field *const end = fptu::end(ro);
  out << "(" << ro.total_bytes << " bytes, " << end - begin << " fields, "
      << static_cast<const void *>(ro.units) << ")={";
  for (auto i = begin; i != end; ++i) {
    if (i != begin)
      out << ", ";
    out << *i;
  }
  return out << "}";
}

FPTU_TOSTRING_IMP(const fptu_ro &)

__cold ostream &operator<<(ostream &out, const fptu_rw &rw) {
  const void *addr = std::addressof(rw);
  const fptu_field *const begin = fptu::begin(rw);
  const fptu_field *const end = fptu::end(rw);
  out << "(" << addr << ", " << end - begin << " fields, "
      << units2bytes(rw.tail - rw.head) << " bytes, " << fptu_junkspace(&rw)
      << " junk, " << fptu_space4items(&rw) << "/" << fptu_space4data(&rw)
      << " space, H" << rw.head << "_P" << rw.pivot << "_T" << rw.tail << "_E"
      << rw.end << ")={";
  for (auto i = begin; i != end; ++i) {
    if (i != begin)
      out << ", ";
    out << *i;
  }
  return out << "}";
}

FPTU_TOSTRING_IMP(const fptu_rw &)

__cold ostream &operator<<(ostream &out, const fptu_lge value) {
  switch (value) {
  default:
    return invalid(out, "lge", value);
  case fptu_ic:
    return out << "><";
  case fptu_eq:
    return out << "==";
  case fptu_lt:
    return out << "<";
  case fptu_gt:
    return out << ">";
  case fptu_ne:
    return out << "!=";
  case fptu_le:
    return out << "<=";
  case fptu_ge:
    return out << ">=";
  }
}

FPTU_TOSTRING_IMP(const fptu_lge)

__cold ostream &operator<<(ostream &out, const fptu_time &value) {
  int year_offset = 1900;
  struct tm utc_tm;
#ifdef _MSC_VER
  const __time64_t utc64 = value.utc;
  const errno_t gmtime_rc = _gmtime64_s(&utc_tm, &utc64);
  assert(gmtime_rc == 0 && utc_tm.tm_year > 69);
  (void)gmtime_rc;
#else
  time_t utc_sec = value.utc;
  for (;;) {
    const struct tm *gmtime_rc = gmtime_r(&utc_sec, &utc_tm);
    assert(gmtime_rc != nullptr);
    (void)gmtime_rc;
    if (sizeof(time_t) > 4 || utc_tm.tm_year > 69) {
      assert(utc_tm.tm_year > 69);
      break;
    }
    year_offset += 28;
    utc_sec -= (utc_tm.tm_year < 8 || year_offset != 1984)
                   ? (28 * 365 + 7) * 24 * 3600
                   : (28 * 365 + 6) * 24 * 3600 /* correction for >= 2100 */;
  }
#endif

  const auto save_fmtfl = out.flags();
  const auto safe_fill = out.fill('0');
  out << setw(4) << utc_tm.tm_year + year_offset << "-" << setw(2)
      << utc_tm.tm_mon + 1 << "-" << setw(2) << utc_tm.tm_mday << "T" << setw(2)
      << utc_tm.tm_hour << ":" << setw(2) << utc_tm.tm_min << ":" << setw(2)
      << utc_tm.tm_sec;

  if (value.fractional) {
    char buffer[erthink::grisu::fractional_printer::max_chars];
    erthink::grisu::fractional_printer printer(buffer,
                                               erthink::array_end(buffer));
    erthink::grisu::convert(
        printer, erthink::grisu::diy_fp::fixedpoint(value.fractional, -32));
    const auto end = printer.finalize_and_get().second;
    const ptrdiff_t length = end - buffer;
    assert(length > 0 && length < ptrdiff_t(sizeof(buffer)));
    out.write(buffer, length);
  }

  out.fill(safe_fill);
  out.flags(save_fmtfl);
  return out;
}

FPTU_TOSTRING_IMP(const fptu_time &)

/* #define FIXME "FIXME: " __FILE__ ", " FPT_STRINGIFY(__LINE__) */

} /* namespace std */

//----------------------------------------------------------------------------

#ifdef __SANITIZE_ADDRESS__
extern "C" FPTU_API __attribute__((__weak__)) const char *
__asan_default_options() {
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
