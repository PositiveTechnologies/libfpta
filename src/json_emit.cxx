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

#ifdef _MSC_VER
#pragma warning(disable : 4710) /* function not inlined */
#endif

#include "erthink/erthink_d2a.h"
#include "erthink/erthink_u2a.h"

#include "bitset4tags.h"
#include "fast_positive/tuples_internal.h"

#ifdef _MSC_VER
#pragma warning(disable : 4774) /* '_snprintf_s' : format string expected in   \
                                   argument 4 is not a string literal */
#pragma warning(push, 1)
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                   semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling  \
                                    mode specified; termination on exception   \
                                    is not guaranteed. Specify /EHsc */
#endif                          /* _MSC_VER (warnings) */
#include <algorithm>            // for std::max, etc
#include <ostream>
#include <sstream>
#ifdef _MSC_VER
#pragma warning(pop)
#endif

using namespace fptu;

typedef int (*fptu_name2tag_func)(void *schema_ctx, const char *name);
typedef int (*fptu_enum2value_func)(void *schema_ctx, uint_fast16_t colnum,
                                    const char *name);

namespace {

/* Basic emitter (no any json specific). This emitter should be reused in the
 * implementation of output to other text format like YAML. */
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4820) /* FOO bytes padding added                     \
                                   after data member BAR */
#endif
struct emitter {
  void *const output_ctx;
  const fptu_emit_func output;
  const string_view indent_str;

  unsigned depth;
  unsigned fill;
  char buffer[42];
  fptu_error err;
  bool indented;

  emitter(fptu_emit_func output, void *output_ctx, const string_view &indent,
          unsigned depth)
      : output_ctx(output_ctx), output(output), indent_str(indent),
        depth(depth), fill(0), err(FPTU_SUCCESS), indented(false) {}
  emitter(const emitter &) = delete;
  emitter &operator=(const emitter &) = delete;

  fptu_error flush();
  fptu_error push(size_t length, const char *text);
  void push(const string_view &str) { push(str.length(), str.data()); }
  void push(const char byte);
  void linefeed(int depth_delta);
  void indent(void);
  void space();
  char *wanna(size_t space);

  void number(uint32_t);
  void number(int32_t);
  void number(uint64_t);
  void number(int64_t i64);
  void number(double);

  template <size_t LENGTH> void push(const char (&text)[LENGTH]) {
    push(text[LENGTH - 1] ? LENGTH : LENGTH - 1, &text[0]);
  }

  template <typename... Args>
  void format(size_t max_width, const char *format, Args... args) {
    assert(max_width > 0 && max_width < sizeof(buffer));
    const int n = snprintf(wanna(max_width), max_width, format, args...);
    assert(n > 0 && n < (int)max_width);
    fill += std::max(0, n) /* paranoia for glibc < 2.0.6 */;
    assert(fill < sizeof(buffer) - 1);
  }
};
#ifdef _MSC_VER
#pragma warning(pop)
#endif

fptu_error emitter::flush() {
  assert(fill <= sizeof(buffer));
  if (likely(fill)) {
    if (likely(err == FPTU_SUCCESS))
      err = (fptu_error)output(output_ctx, buffer, fill);
    fill = 0;
  }
  return err;
}

void emitter::space() {
  // assume no spaces are required if no indentation
  if (!indent_str.empty())
    push(' ');
}

void emitter::linefeed(int depth_delta = 0) {
  // assume no linefeeds are required if no indentation
  if (!indent_str.empty()) {
    push('\n');
    indented = false;
    depth += depth_delta;
    if (depth_delta > 0)
      indent();
  } else {
    // just for depth tracking
    depth += depth_delta;
  }
}

void emitter::indent(void) {
  if (!indent_str.empty() && !indented) {
    for (unsigned i = 0; i < depth; ++i)
      push(indent_str);
    indented = true;
  }
}

fptu_error emitter::push(size_t length, const char *text) {
  assert(strnlen(text, length) == length);
  assert(fill < sizeof(buffer));
  if (likely(length < sizeof(buffer))) {
    if (likely(length > 0)) {
      const size_t space = sizeof(buffer) - fill;
      const size_t chunk = (length > space) ? space : length;
      memcpy(buffer + fill, text, chunk);
      fill += (unsigned)chunk;
      if (fill == sizeof(buffer)) {
        flush();
        fill = (unsigned)(length - chunk);
        assert(fill < sizeof(buffer));
        memcpy(buffer, text + chunk, fill);
      } else {
        assert(chunk == length);
      }
    }
  } else {
    flush();
    if (likely(err == FPTU_SUCCESS))
      err = (fptu_error)output(output_ctx, text, length);
  }
  return err;
}

void emitter::push(const char byte) {
  assert(fill < sizeof(buffer));
  buffer[fill] = byte;
  if (++fill == sizeof(buffer))
    flush();
}

char *emitter::wanna(size_t space) {
  assert(fill < sizeof(buffer));
  assert(space < sizeof(buffer));
  if (space >= sizeof(buffer) - fill)
    flush();
  return buffer + fill;
}

//----------------------------------------------------------------------------

void emitter::number(uint32_t u32) {
  // max 10 chars for 4`294`967`295
  char *const begin = wanna(10);
  char *const end = erthink::u2a(u32, begin);
  assert(end > begin && end <= begin + 10);
  fill += static_cast<unsigned>(end - begin);
}

void emitter::number(int32_t i32) {
  // max 11 chars for -2`147`483`648
  char *const begin = wanna(11);
  char *const end = erthink::i2a(i32, begin);
  assert(end > begin && end <= begin + 10);
  fill += static_cast<unsigned>(end - begin);
}

void emitter::number(uint64_t u64) {
  // max 20 digits for 18`446`744`073`709`551`615
  char *const begin = wanna(20);
  char *const end = erthink::u2a(u64, begin);
  assert(end > begin && end <= begin + 20);
  fill += static_cast<unsigned>(end - begin);
}

void emitter::number(int64_t i64) {
  // max 20 chars for -9`223`372`036`854`775`808
  char *const begin = wanna(20);
  char *const end = erthink::i2a(i64, begin);
  assert(end > begin && end <= begin + 20);
  fill += static_cast<unsigned>(end - begin);
}

void emitter::number(double value) {
  // max 24 chars for -2225073.8585072014e-314
  char *const begin = wanna(24);
  char *const end = erthink::d2a(value, begin);
  assert(end > begin && end <= begin + 32);
  fill += static_cast<unsigned>(end - begin);
}

//----------------------------------------------------------------------------
/* JSON emitter */
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4820) /* FOO bytes padding added                     \
                                   after data member BAR */
#endif

#include "gperf_ECMAScript_keywords.h"

struct json : public emitter {
  const void *const schema_ctx;
  const fptu_tag2name_func tag2name;
  const fptu_value2enum_func value2enum;
  const fptu_json_options options;

  enum { ObjectName_colnum = 0, ObjectValue_colnum = 1 };

  json(void *output_ctx, fptu_emit_func output, const string_view &indent,
       unsigned depth, const void *schema_ctx, fptu_tag2name_func tag2name,
       fptu_value2enum_func value2enum, const fptu_json_options options)
      : emitter(output, output_ctx, indent, depth), schema_ctx(schema_ctx),
        tag2name(tag2name), value2enum(value2enum), options(options) {}
  json(const json &) = delete;
  json &operator=(const json &) = delete;

  bool is_json5() const {
    return (options & fptu_json_disable_JSON5) ? false : true;
  }

  static bool is_valid_ECMAScript_identifier(const string_view &str);

  void comma(bool no_linefeed = false);
  void null() { push("null"); }
  void string(const string_view &str);
  void key_name(const string_view &key_name);
  void value_uint16_and_enum(uint_fast16_t colnum, uint_fast16_t value);
  void value_sint32(const int32_t &value);
  void value_uint32(const uint32_t &value);
  void value_sint64(const int64_t &value);
  void value_uint64(const uint64_t &value);
  void value_fp32(const fptu_payload *payload);
  void value_fp64(const fptu_payload *payload);
  void value_dateime(const fptu_time &value);
  void value_hexadecimal(const uint8_t *data, size_t length);
  void field_value(const fptu_field *field_value);
  void field_single_or_collection(const fptu_field *field,
                                  const fptu_field *const begin);
  void tuple(const fptu_ro &tuple);
};
#ifdef _MSC_VER
#pragma warning(pop)
#endif

void json::comma(bool no_linefeed) {
  push(',');
  if (no_linefeed)
    space();
  else
    linefeed();
}

bool json::is_valid_ECMAScript_identifier(const string_view &str) {
  if (str.empty())
    return false;

  for (const char c : str) {
    if (!isalnum(c) && c != '_' && c != '$')
      return false;
  }

  /* ECMAScript >= 5.x */
  return ECMAScript_keywords::in_word_set(str.data(), str.length()) ? false
                                                                    : true;
}

void json::key_name(const string_view &name) {
  if (is_json5() && is_valid_ECMAScript_identifier(name))
    push(name);
  else
    string(name);
}

void json::string(const string_view &str) {
  push('"');
  for (const char c : str) {
    if (unlikely(err != FPTU_SUCCESS))
      return;

    switch (c) {
    case '"':
      push("\\\"");
      continue;
    case '\\':
      push("\\\\");
      continue;
    case '\b':
      push("\\b");
      continue;
    case '\f':
      push("\\f");
      continue;
    case '\n':
      push("\\n");
      continue;
    case '\r':
      push("\\r");
      continue;
    case '\t':
      push("\\t");
      continue;
    default:
      if (likely((uint8_t)c >= ' '))
        push(c);
      else {
        char *const begin = wanna(6);
        memcpy(begin, "\\u", 2);
        char *ptr = erthink::dec4((uint8_t)c, begin + 2, true);
        assert(ptr - begin == 6);
        (void)ptr;
        fill += 6;
      }
    }
  }
  push('"');
}

void json::value_uint16_and_enum(uint_fast16_t colnum,
                                 uint_fast16_t enum_value) {
  if (enum_value != FPTU_DENIL_UINT16) {
    const char *name =
        value2enum ? value2enum(schema_ctx, colnum, enum_value) : nullptr;
    if (name) {
      if (*name)
        string(string_view(name));
      else if (enum_value)
        push("true");
      else
        push("false");
    } else
      number(enum_value);
  } else
    null();
}

void json::value_sint32(const int32_t &value) {
  if (value != FPTU_DENIL_SINT32)
    number(value);
  else
    null();
}

void json::value_uint32(const uint32_t &value) {
  if (value != FPTU_DENIL_UINT32)
    number(value);
  else
    null();
}

void json::value_sint64(const int64_t &value) {
  if (value != FPTU_DENIL_SINT64)
    number(value);
  else
    null();
}

void json::value_uint64(const uint64_t &value) {
  if (value != FPTU_DENIL_UINT64)
    number(value);
  else
    null();
}

void json::value_fp32(const fptu_payload *payload) {
  if (likely(payload->u32 != FPTU_DENIL_FP32_BIN)) {
    switch (std::fpclassify(payload->fp32)) {
    case FP_NAN:
      if (is_json5()) {
        push("NaN");
        return;
      }
      break;
    case FP_INFINITE:
      if (is_json5()) {
        push(std::signbit(payload->fp32) ? '-' : '+');
        push("Infinity");
        return;
      }
      break;
    default:
      number(payload->fp32);
      return;
    }
  }
  null();
}

void json::value_fp64(const fptu_payload *payload) {
  if (likely(payload->u64 != FPTU_DENIL_FP64_BIN)) {
    switch (std::fpclassify(payload->fp64)) {
    case FP_NAN:
      if (is_json5()) {
        push("NaN");
        return;
      }
      break;
    case FP_INFINITE:
      if (is_json5()) {
        push(std::signbit(payload->fp64) ? '-' : '+');
        push("Infinity");
        return;
      }
      break;
    default:
      number(payload->fp64);
      return;
    }
  }
  null();
}

void json::value_dateime(const fptu_time &value) {
  if (value.fixedpoint != FPTU_DENIL_TIME_BIN) {
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

    format(24, "\"%04d-%02d-%02dT%02d:%02d:%02d", utc_tm.tm_year + year_offset,
           utc_tm.tm_mon + 1, utc_tm.tm_mday, utc_tm.tm_hour, utc_tm.tm_min,
           utc_tm.tm_sec);

    if (value.fractional) {
      int exponent;
      char *const begin = wanna(32) + 1;
      begin[-1] = '.';
      char *end = erthink::grisu::convert(
          erthink::grisu::diy_fp::fixedpoint(value.fractional, -32), begin,
          exponent);
      assert(end > begin && end < begin + 31);
      assert(-exponent >= end - begin);
      const ptrdiff_t zero_needed = -exponent - (end - begin);
      assert(zero_needed >= 0 && zero_needed < 31 - (end - begin));
      if (zero_needed > 0) {
        memmove(begin + zero_needed, begin, size_t(end - begin));
        memset(begin, '0', size_t(zero_needed));
      } else
        while (end[-1] == '0')
          --end;
      fill +=
          static_cast<unsigned>(end - begin + zero_needed + /* the dot */ 1);
      assert(fill < sizeof(buffer));
    }
    push('"');
  } else
    null();
}

void json::value_hexadecimal(const uint8_t *data, size_t length) {
  push('"');
  for (size_t i = 0; i < length; ++i) {
    char high = data[i] >> 4;
    char low = data[i] & 15;
    push((high < 10) ? high + '0' : high - 10 + 'a');
    push((low < 10) ? low + '0' : low - 10 + 'a');
  }
  push('"');
}

void json::field_value(const fptu_field *field) {
  const fptu_payload *payload = field->payload();
  switch (field->type()) {
  case fptu_null:
    push("null");
    break;
  case fptu_uint16:
    value_uint16_and_enum(field->tag, field->get_payload_uint16());
    break;

  case fptu_int32:
    value_sint32(payload->i32);
    break;
  case fptu_uint32:
    value_uint32(payload->u32);
    break;

  case fptu_int64:
    value_sint64(payload->i64);
    break;
  case fptu_uint64:
    value_uint64(payload->u64);
    break;

  case fptu_fp32:
    value_fp32(payload);
    break;
  case fptu_fp64:
    value_fp64(payload);
    break;

  case fptu_datetime:
    value_dateime(payload->dt);
    break;
  case fptu_96:
    value_hexadecimal(payload->fixbin, 96 / 8);
    break;
  case fptu_128:
    value_hexadecimal(payload->fixbin, 128 / 8);
    break;
  case fptu_160:
    value_hexadecimal(payload->fixbin, 160 / 8);
    break;
  case fptu_256:
    value_hexadecimal(payload->fixbin, 256 / 8);
    break;
  case fptu_cstr:
    string(string_view(payload->cstr));
    break;
  case fptu_opaque:
    value_hexadecimal((uint8_t *)payload->other.data,
                      payload->other.varlen.opaque_bytes);
    break;
  case fptu_nested:
    tuple(fptu_field_nested(field));
    break;

  default:
    const size_t length = payload->array_length();
    const uint8_t *const begin = (const uint8_t *)payload->inner_begin();
    const uint8_t *const end = (const uint8_t *)payload->inner_end();

    push('[');
    if (length > 1)
      linefeed(+1);

    const uint8_t *ptr = begin;
    for (size_t i = 0; i < length; ++i) {
      if (unlikely(err != FPTU_SUCCESS))
        return;

      if (i > 0)
        push(',');

      if (ptr >= end) {
        null();
        continue;
      }
      switch (field->type()) {
      default:
        err = FPTU_EINVAL;
        return;
      case fptu_uint16 | fptu_farray:
        value_uint16_and_enum(field->tag, *(const uint16_t *)ptr);
        static_assert(sizeof(uint16_t) == 2, "unexpected type size");
        ptr += sizeof(uint16_t);
        continue;

      case fptu_int32 | fptu_farray:
        value_sint32(*(const int32_t *)ptr);
        static_assert(sizeof(int32_t) == 4, "unexpected type size");
        ptr += sizeof(int32_t);
        continue;
      case fptu_uint32 | fptu_farray:
        value_uint32(*(const uint32_t *)ptr);
        static_assert(sizeof(uint32_t) == 4, "unexpected type size");
        ptr += sizeof(uint32_t);
        continue;

      case fptu_int64 | fptu_farray:
        value_sint64(*(const int64_t *)ptr);
        static_assert(sizeof(int64_t) == 8, "unexpected type size");
        ptr += sizeof(int64_t);
        continue;
      case fptu_uint64 | fptu_farray:
        value_uint64(*(const uint64_t *)ptr);
        static_assert(sizeof(uint64_t) == 8, "unexpected type size");
        ptr += sizeof(uint64_t);
        continue;

      case fptu_fp32 | fptu_farray:
        value_fp32((const fptu_payload *)ptr);
        static_assert(sizeof(float) == 4, "unexpected type size");
        ptr += sizeof(float);
        continue;
      case fptu_fp64 | fptu_farray:
        value_fp64((const fptu_payload *)ptr);
        static_assert(sizeof(double) == 8, "unexpected type size");
        ptr += sizeof(double);
        continue;

      case fptu_datetime | fptu_farray:
        value_dateime(*(const fptu_time *)ptr);
        static_assert(sizeof(fptu_time) == 8, "unexpected type size");
        ptr += sizeof(fptu_datetime);
        continue;
      case fptu_96 | fptu_farray:
        value_hexadecimal(ptr, 96 / 8);
        ptr += 96 / 8;
        continue;
      case fptu_128 | fptu_farray:
        value_hexadecimal(ptr, 128 / 8);
        ptr += 128 / 8;
        continue;
      case fptu_160 | fptu_farray:
        value_hexadecimal(ptr, 160 / 8);
        ptr += 160 / 8;
        continue;
      case fptu_256 | fptu_farray:
        value_hexadecimal(ptr, 256 / 8);
        ptr += 256 / 8;
        continue;

      case fptu_cstr | fptu_farray:
        string(string_view((const char *)ptr));
        ptr += strlen((const char *)ptr) + 1;
        continue;
      case fptu_opaque | fptu_farray:
        payload = (const fptu_payload *)ptr;
        value_hexadecimal((uint8_t *)payload->other.data,
                          payload->other.varlen.opaque_bytes);
        ptr = (const uint8_t *)payload->inner_end();
        continue;
      case fptu_nested | fptu_farray:
        payload = (const fptu_payload *)ptr;
        const fptu_ro ro = {{(const fptu_unit *)ptr,
                             units2bytes(payload->other.varlen.brutto)}};
        tuple(ro);
        ptr = (const uint8_t *)payload->inner_end();
        continue;
      }
    }

    if (length > 1)
      linefeed(-1);

    indent();
    push(']');
  }
}

void json::tuple(const fptu_ro &tuple) {
  unsigned count = 0;
  const auto begin = fptu_begin_ro(tuple);
  const auto end = fptu_end_ro(tuple);
  bitset4tags::minimize params(begin, end, 0);
  bitset4tags bitmask(params, alloca(params.bytes()));
  // итерируем в обратном порядке, чтобы выводить поля
  // ближе к порядку их добавления
  for (auto i = end; --i >= begin;) {
    if (unlikely(err != FPTU_SUCCESS))
      return;

    // пропускаем удаленные
    if (i->is_dead())
      continue;

    // пропускаем уже обработанные элементы коллекций
    if (bitmask.test_and_set(i->tag))
      continue;

    const char *name = tag2name ? tag2name(schema_ctx, i->tag) : nullptr;
    if (name && unlikely(*name == '\0')) {
      // пропускаем скрытое поле
      continue;
    }

    if (count++) {
      comma();
    } else {
      indent();
      push('{');
      if (begin + 1 < end)
        linefeed(+1);
    }

    indent();
    // выводим имя поля
    if (name)
      key_name(string_view(name));
    else
      format(16, "\"@%u\"", i->tag);
    push(':');
    space();

    if (options & fptu_json_disable_Collections)
      field_value(i);
    else {
      // пробуем найти повтор
      auto next = i;
      while (--next >= begin && next->tag != i->tag)
        ;
      if (next < begin) {
        // выводим одиночное поле
        field_value(i);
      } else {
        // повтор поля, выводим коллекцию как JSON-массив
        push('[');
        linefeed(+1);
        field_value(i);
        do {
          if (unlikely(err != FPTU_SUCCESS))
            return;
          push(',');
          linefeed();
          indent();
          field_value(next);
          // ищем следующий элемент
          while (--next >= begin && next->tag != i->tag)
            ;
        } while (next >= begin);
        linefeed(-1);
        indent();
        push(']');
      }
    }
  }

  if (count) {
    if (begin + 1 < end)
      linefeed(-1);
    indent();
    push('}');
  } else
    null();
}

} // namespace

fptu_error fptu_tuple2json(fptu_ro tuple, fptu_emit_func output,
                           void *output_ctx, const char *indent, unsigned depth,
                           const void *schema_ctx, fptu_tag2name_func tag2name,
                           fptu_value2enum_func value2enum,
                           const fptu_json_options options) {
  json out(output_ctx, output, string_view(indent), depth, schema_ctx, tag2name,
           value2enum, options);
  out.tuple(tuple);
  return out.flush();
}

static int fptu_emit2FILE(void *emiter_ctx, const char *text, size_t length) {
  assert(strlen(text) == length);
  (void)length;
  return (fputs(text, static_cast<FILE *>(emiter_ctx)) < 0) ? (fptu_error)errno
                                                            : FPTU_SUCCESS;
}

fptu_error fptu_tuple2json_FILE(fptu_ro tuple, FILE *file, const char *indent,
                                unsigned depth, const void *schema_ctx,
                                fptu_tag2name_func tag2name,
                                fptu_value2enum_func value2enum,
                                const fptu_json_options options) {
  json out(file, fptu_emit2FILE, string_view(indent), depth, schema_ctx,
           tag2name, value2enum, options);
  out.tuple(tuple);
  return out.flush();
}

namespace fptu {

int tuple2json(const fptu_ro &tuple, fptu_emit_func output, void *output_ctx,
               const string_view &indent, unsigned depth,
               const void *schema_ctx, fptu_tag2name_func tag2name,
               fptu_value2enum_func value2enum,
               const fptu_json_options options) {
  json out(output_ctx, output, indent, depth, schema_ctx, tag2name, value2enum,
           options);
  out.tuple(tuple);
  return out.flush();
}

static int emit2stream(void *emiter_ctx, const char *text, size_t length) {
  assert(strnlen(text, length) == length);
  std::ostream *stream = static_cast<std::ostream *>(emiter_ctx);
  stream->write(text, static_cast<std::streamsize>(length));
  return stream->good() ? FPTU_SUCCESS : -1;
}

int tuple2json(const fptu_ro &tuple, std::ostream &stream,
               const string_view &indent, unsigned depth,
               const void *schema_ctx, fptu_tag2name_func tag2name,
               fptu_value2enum_func value2enum,
               const fptu_json_options options) {
  json out(&stream, emit2stream, indent, depth, schema_ctx, tag2name,
           value2enum, options);
  out.tuple(tuple);
  return out.flush();
}

std::string tuple2json(const fptu_ro &tuple, const string_view &indent,
                       unsigned depth, const void *schema_ctx,
                       fptu_tag2name_func tag2name,
                       fptu_value2enum_func value2enum,
                       const fptu_json_options options) {
  std::stringstream sink;
  sink.exceptions(std::ios::failbit | std::ios::badbit);
  const int err = tuple2json(tuple, sink, indent, depth, schema_ctx, tag2name,
                             value2enum, options);
  if (err != FPTU_SUCCESS) {
    assert(err == FPTU_EINVAL);
    throw bad_tuple(tuple);
  }
  return sink.str();
}

} // namespace fptu
