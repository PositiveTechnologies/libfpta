/*
 * Copyright 2016-2018 libfptu authors: please see AUTHORS file.
 *
 * This file is part of libfptu, aka "Fast Positive Tuples".
 *
 * libfptu is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * libfptu is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with libfptu.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "fptu_test.h"
#include "shuffle6.hpp"

#ifdef _MSC_VER
#pragma warning(push, 1)
#if _MSC_VER < 1900
/* LY: workaround for dead code:
       microsoft visual studio 12.0\vc\include\xtree(1826) */
#pragma warning(disable : 4702) /* unreachable code */
#endif
#endif /* _MSC_VER */

#include <algorithm>
#include <array>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>

#ifdef _MSC_VER
#pragma warning(pop)
#endif

class schema_dict {
public:
  template <typename A, typename B> struct hash_pair {
    cxx14_constexpr std::size_t operator()(std::pair<A, B> const &v) const {
      const std::size_t a = std::hash<A>()(v.first);
      const std::size_t b = std::hash<B>()(v.second);
      return a * 3139864919 ^ (~b + (a >> 11));
    }
  };

  schema_dict() {}
  schema_dict(const schema_dict &) = delete;
  schema_dict &operator=(const schema_dict &) = default;
  schema_dict(schema_dict &&) = default;
  schema_dict &operator=(schema_dict &&) = default;

#if defined(_MSC_VER) && _MSC_VER < 1910 /* obsolete and trouble full */
#pragma warning(push)
#pragma warning(disable : 4268) /* 'const' static / global data initialized    \
                                   with compiler generated default constructor \
                                   fills the object with zeros */
#endif                          /* _MSC_VER < 1910 */
  static constexpr const std::array<fptu_type, 31> fptu_types{
      {fptu_null,         fptu_uint16,       fptu_int32,
       fptu_uint32,       fptu_fp32,         fptu_int64,
       fptu_uint64,       fptu_fp64,         fptu_datetime,
       fptu_96,           fptu_128,          fptu_160,
       fptu_256,          fptu_cstr,         fptu_opaque,
       fptu_nested,       fptu_array_uint16, fptu_array_int32,
       fptu_array_uint32, fptu_array_fp32,   fptu_array_int64,
       fptu_array_uint64, fptu_array_fp64,   fptu_array_datetime,
       fptu_array_96,     fptu_array_128,    fptu_array_160,
       fptu_array_256,    fptu_array_cstr,   fptu_array_opaque,
       fptu_array_nested}};
#if defined(_MSC_VER) && _MSC_VER < 1910 /* obsolete and trouble full */
#pragma warning(pop)
#endif /* _MSC_VER < 1910 */

protected:
  std::unordered_map<unsigned, std::string> map_tag2name;
  std::unordered_map<fptu::string_view, unsigned> map_name2tag;

  std::unordered_map<std::pair<unsigned, unsigned>, std::string,
                     hash_pair<unsigned, unsigned>>
      map_value2enum;
  std::unordered_map<std::pair<fptu::string_view, unsigned>, unsigned,
                     hash_pair<fptu::string_view, unsigned>>
      map_enum2value;

  enum dict_of_schema_ids {
    dsid_field = 0,
    dsid_name = 0,
    dsid_colnum = 0,
    dsid_type = 0,
    dsid_enum_def = 1,
    dsid_enum_value = 1
  };

public:
  void add_field(const fptu::string_view &name, fptu_type type,
                 unsigned colnum);
  void add_enum_value(unsigned colnum, const fptu::string_view &name,
                      unsigned value);

  std::string schema2json() const;
  static schema_dict dict_of_schema();
  static const char *tag2name(const void *schema_ctx, unsigned tag);
  static const char *value2enum(const void *schema_ctx, unsigned tag,
                                unsigned value);
};

constexpr const std::array<fptu_type, 31> schema_dict::fptu_types;

void schema_dict::add_field(const fptu::string_view &name, fptu_type type,
                            unsigned colnum) {
  const unsigned tag = fptu::make_tag(colnum, type);
  const auto pair = map_tag2name.emplace(tag, name);
  if (unlikely(!pair.second))
    throw std::logic_error(fptu::format(
        "schema_dict::add_field: Duplicate field tag (colnum %u, type %s)",
        colnum, fptu_type_name(type)));

  assert(pair.first->second.data() != name.data());
  if (!pair.first->second.empty()) {
    const bool inserted =
        map_name2tag.emplace(fptu::string_view(pair.first->second), tag).second;
    if (unlikely(!inserted))
      throw std::logic_error(
          fptu::format("schema_dict::add_field: Duplicate field name '%.*s'",
                       (int)name.length(), name.data()));
    assert(map_name2tag.at(name) == tag);
  }
  assert(map_tag2name.at(tag) == name);
}

void schema_dict::add_enum_value(unsigned colnum, const fptu::string_view &name,
                                 unsigned value) {
  const unsigned tag = fptu::make_tag(colnum, fptu_enum);
  const auto pair = map_value2enum.emplace(std::make_pair(tag, value), name);
  if (unlikely(!pair.second))
    throw std::logic_error(fptu::format(
        "schema_dict::add_field: Duplicate enum item (colnum %u, value %u)",
        colnum, value));

  assert(pair.first->second.data() != name.data());
  if (!pair.first->second.empty()) {
    const auto inserted = map_enum2value.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(fptu::string_view(pair.first->second), tag),
        std::forward_as_tuple(value));
    if (unlikely(!inserted.second))
      throw std::logic_error(fptu::format("schema_dict::add_field: Duplicate "
                                          "enum item (colnum %u, name '%.*s')",
                                          colnum, (int)name.length(),
                                          name.data()));
    assert(map_enum2value.at(std::make_pair(name, tag)) == value);
  }

  assert(map_value2enum.at(std::make_pair(tag, value)) == name);
}

const char *schema_dict::tag2name(const void *schema_ctx, unsigned tag) {
  const schema_dict *dist = static_cast<const schema_dict *>(schema_ctx);
  const auto search = dist->map_tag2name.find(tag);
  return (search != dist->map_tag2name.end()) ? search->second.c_str()
                                              : nullptr;
}

const char *schema_dict::value2enum(const void *schema_ctx, unsigned tag,
                                    unsigned value) {
  const schema_dict *dist = static_cast<const schema_dict *>(schema_ctx);
  const auto search = dist->map_value2enum.find(std::make_pair(tag, value));
  return (search != dist->map_value2enum.end()) ? search->second.c_str()
                                                : nullptr;
}

schema_dict schema_dict::dict_of_schema() {
  schema_dict dict;
  dict.add_field("field", fptu_nested, dsid_field);
  dict.add_field("name", fptu_cstr, dsid_name);
  dict.add_field("colnum", fptu_uint32, dsid_colnum);
  dict.add_field("type", fptu_enum, dsid_type);
  dict.add_field("enum", fptu_nested, dsid_enum_def);
  dict.add_field("value", fptu_uint16, dsid_enum_value);
  for (const auto type : fptu_types)
    dict.add_enum_value(dsid_type, fptu_type_name(type), type);
  return dict;
}

std::string schema_dict::schema2json() const {
  std::vector<std::pair<unsigned, fptu::string_view>> fieldlist;
  for (const auto &i : map_tag2name) {
    const unsigned tag = i.first;
    const fptu::string_view name(i.second);
    fieldlist.emplace_back(std::make_pair(tag, name));
  }
  std::sort(fieldlist.begin(), fieldlist.end());

  fptu::tuple_ptr schema(
      fptu_rw::create(1 + map_tag2name.size(), fptu_max_tuple_bytes));
  fptu::tuple_ptr field(fptu_rw::create(fptu_max_fields, fptu_max_tuple_bytes));
  fptu::tuple_ptr item(fptu_rw::create(2, fptu_max_tuple_bytes));

  for (const auto &i : fieldlist) {
    const unsigned tag = i.first;
    const fptu::string_view &name = i.second;
    fptu_error err = fptu_clear(field.get());
    if (unlikely(err))
      fptu::throw_error(err);

    err = fptu_insert_string(field.get(), dsid_name, name);
    if (unlikely(err))
      fptu::throw_error(err);
    err = fptu_insert_uint32(field.get(), dsid_colnum, fptu_get_colnum(tag));
    if (unlikely(err))
      fptu::throw_error(err);
    err = fptu_insert_uint16(field.get(), dsid_type, fptu_get_type(tag));
    if (unlikely(err))
      fptu::throw_error(err);
    if (fptu_get_type(tag) == fptu_enum ||
        fptu_get_type(tag) == fptu_array_enum) {
      for (unsigned value = 0; value <= UINT16_MAX; value++) {
        if (value == FPTU_DENIL_UINT16)
          continue;
        const char *enum_item = value2enum(this, tag, value);
        if (enum_item) {
          err = fptu_upsert_uint16(item.get(), dsid_enum_value, value);
          if (unlikely(err))
            fptu::throw_error(err);
          err = fptu_upsert_string(item.get(), dsid_name,
                                   fptu::format("enum:%s", enum_item));
          if (unlikely(err))
            fptu::throw_error(err);

          err = fptu_insert_nested(field.get(), dsid_enum_def,
                                   fptu_take_noshrink(item.get()));
          if (unlikely(err))
            fptu::throw_error(err);
        }
      }
    }

    err = fptu_insert_nested(schema.get(), dsid_field,
                             fptu_take_noshrink(field.get()));
    if (unlikely(err))
      fptu::throw_error(err);
  }

  const schema_dict dict = dict_of_schema();
  std::string result = fptu::tuple2json(
      fptu_take_noshrink(schema.get()), "  ", 0, &dict, tag2name, value2enum,
      fptu_json_default /*fptu_json_enable_ObjectNameProperty*/);
  return result;
}

//------------------------------------------------------------------------------

static schema_dict create_schemaX() {
  // Cоздается простой словарь схемы, в которой для каждого типа
  // по 10 (римская X) штук полей и массивов.
  // TODO: При этом 10-е поля делаются скрытыми (с пустым именем).
  schema_dict dict;
  for (unsigned n = 1; n < 10; n++) {
    for (const auto type : schema_dict::fptu_types) {
      if (type >= fptu_farray)
        break;

      const std::string field = fptu::format("f%u_%s", n, fptu_type_name(type));
      dict.add_field(fptu::string_view(field), type, n);

      if (type > fptu_null) {
        const std::string array =
            fptu::format("a%u_%s", n, fptu_type_name(type));
        dict.add_field(fptu::string_view(array), fptu_type_array_of(type), n);
      }
    }
  }

  // Для 9-го поля добавляем два пустых имени,
  // чтобы использовать встроенные true/false для bool,
  // а также не-пустое имя для проверки enum
  dict.add_enum_value(9, "", 0);
  dict.add_enum_value(9, "", 1);
  dict.add_enum_value(9, "item42", 42);

  return dict;
}

static const std::string
make_json(const schema_dict &dict, const fptu_ro &ro, bool indentation = false,
          const fptu_json_options options = fptu_json_default) {
  return fptu::tuple2json(ro, indentation ? "  " : nullptr, 0, &dict,
                          schema_dict::tag2name, schema_dict::value2enum,
                          options);
}

static const std::string
make_json(const schema_dict &dict, fptu_rw *pt, bool indentation = false,
          const fptu_json_options options = fptu_json_default) {
  return make_json(dict, fptu_take_noshrink(pt), indentation, options);
}

static const std::string
make_json(const schema_dict &dict, const fptu::tuple_ptr &pt,
          bool indentation = false,
          const fptu_json_options options = fptu_json_default) {
  return make_json(dict, pt.get(), indentation, options);
}

static const char *json(const schema_dict &dict, const fptu::tuple_ptr &pt,
                        bool indentation = false,
                        const fptu_json_options options = fptu_json_default) {
  static std::string json_holder;
  json_holder = make_json(dict, pt, indentation, options);
  return json_holder.c_str();
}

//------------------------------------------------------------------------------

TEST(Emit, Null) {
  schema_dict dict;
  EXPECT_NO_THROW(dict = create_schemaX());

  fptu::tuple_ptr pt(fptu_rw::create(67, 12345));
  ASSERT_NE(nullptr, pt.get());
  ASSERT_STREQ(nullptr, fptu::check(pt.get()));

  // пустой кортеж
  ASSERT_TRUE(fptu::is_empty(pt.get()));
  EXPECT_STREQ("null", json(dict, pt));

  ASSERT_STREQ(nullptr, fptu::check(pt.get()));
}

TEST(Emit, UnsignedInt16) {
  schema_dict dict;
  EXPECT_NO_THROW(dict = create_schemaX());

  fptu::tuple_ptr pt(fptu_rw::create(67, 12345));
  ASSERT_NE(nullptr, pt.get());
  ASSERT_STREQ(nullptr, fptu::check(pt.get()));

  // несколько разных полей uint16 включая DENIL
  ASSERT_EQ(FPTU_OK, fptu_upsert_uint16(pt.get(), 1, 0));
  ASSERT_EQ(FPTU_OK, fptu_upsert_uint16(pt.get(), 2, 35671));
  ASSERT_EQ(FPTU_OK, fptu_upsert_uint16(pt.get(), 3, FPTU_DENIL_UINT16));
  ASSERT_EQ(FPTU_OK, fptu_upsert_uint16(pt.get(), 4, 42));
  EXPECT_STREQ("{f1_uint16:0,f2_uint16:35671,f3_uint16:null,f4_uint16:42}",
               json(dict, pt));

  ASSERT_STREQ(nullptr, fptu::check(pt.get()));
}

TEST(Emit, BoolAndEnum) {
  schema_dict dict;
  EXPECT_NO_THROW(dict = create_schemaX());

  fptu::tuple_ptr pt(fptu_rw::create(67, 12345));
  ASSERT_NE(nullptr, pt.get());
  ASSERT_STREQ(nullptr, fptu::check(pt.get()));

  // коллекция из bool, включая DENIL
  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  ASSERT_EQ(FPTU_OK, fptu_insert_bool(pt.get(), 9, true));
  ASSERT_EQ(FPTU_OK, fptu_insert_uint16(pt.get(), 9, FPTU_DENIL_UINT16));
  ASSERT_EQ(FPTU_OK, fptu_insert_bool(pt.get(), 9, false));
  EXPECT_STREQ("{f9_uint16:[true,null,false]}", json(dict, pt));

  // коллекция из enum, включая DENIL
  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  ASSERT_EQ(FPTU_OK, fptu_insert_uint16(pt.get(), 9, 42));
  ASSERT_EQ(FPTU_OK, fptu_insert_uint16(pt.get(), 9, FPTU_DENIL_UINT16));
  ASSERT_EQ(FPTU_OK, fptu_insert_uint16(pt.get(), 9, 33));
  EXPECT_STREQ("{f9_uint16:[\"item42\",null,33]}", json(dict, pt));
}

//------------------------------------------------------------------------------

TEST(Emit, UnignedInt32) {
  schema_dict dict;
  EXPECT_NO_THROW(dict = create_schemaX());

  fptu::tuple_ptr pt(fptu_rw::create(67, 12345));
  ASSERT_NE(nullptr, pt.get());
  ASSERT_STREQ(nullptr, fptu::check(pt.get()));

  // несколько разных полей uint32 включая DENIL
  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  ASSERT_EQ(FPTU_OK, fptu_upsert_uint32(pt.get(), 1, 0));
  ASSERT_EQ(FPTU_OK, fptu_upsert_uint32(pt.get(), 2, 4242424242));
  ASSERT_EQ(FPTU_OK, fptu_upsert_uint32(pt.get(), 3, 1));
  ASSERT_EQ(FPTU_OK, fptu_upsert_uint32(pt.get(), 4, FPTU_DENIL_UINT32));
  EXPECT_STREQ("{f1_uint32:0,f2_uint32:4242424242,f3_uint32:1,f4_uint32:null}",
               json(dict, pt));
}

TEST(Emit, SignedInt32) {
  schema_dict dict;
  EXPECT_NO_THROW(dict = create_schemaX());

  fptu::tuple_ptr pt(fptu_rw::create(67, 12345));
  ASSERT_NE(nullptr, pt.get());
  ASSERT_STREQ(nullptr, fptu::check(pt.get()));

  // несколько разных полей uint32 включая DENIL
  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  ASSERT_EQ(FPTU_OK, fptu_upsert_int32(pt.get(), 1, FPTU_DENIL_SINT32));
  ASSERT_EQ(FPTU_OK, fptu_upsert_int32(pt.get(), 2, 0));
  ASSERT_EQ(FPTU_OK, fptu_upsert_int32(pt.get(), 3, 2121212121));
  ASSERT_EQ(FPTU_OK, fptu_upsert_int32(pt.get(), 4, -1));
  EXPECT_STREQ("{f1_int32:null,f2_int32:0,f3_int32:2121212121,f4_int32:-1}",
               json(dict, pt));

  ASSERT_STREQ(nullptr, fptu::check(pt.get()));
}

TEST(Emit, UnsignedInt64) {
  schema_dict dict;
  EXPECT_NO_THROW(dict = create_schemaX());

  fptu::tuple_ptr pt(fptu_rw::create(67, 12345));
  ASSERT_NE(nullptr, pt.get());
  ASSERT_STREQ(nullptr, fptu::check(pt.get()));

  // несколько разных полей uint64 включая DENIL
  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  ASSERT_EQ(FPTU_OK, fptu_upsert_uint64(pt.get(), 1, 0));
  ASSERT_EQ(FPTU_OK, fptu_upsert_uint64(pt.get(), 2, 4242424242));
  ASSERT_EQ(FPTU_OK, fptu_upsert_uint64(pt.get(), 3, INT64_MAX));
  ASSERT_EQ(FPTU_OK, fptu_upsert_uint64(pt.get(), 4, FPTU_DENIL_UINT64));
  EXPECT_STREQ(
      "{f1_uint64:0,f2_uint64:4242424242,f3_uint64:9223372036854775807,"
      "f4_uint64:null}",
      json(dict, pt));

  ASSERT_STREQ(nullptr, fptu::check(pt.get()));
}

TEST(Emit, SignedInt64) {
  schema_dict dict;
  EXPECT_NO_THROW(dict = create_schemaX());

  fptu::tuple_ptr pt(fptu_rw::create(67, 12345));
  ASSERT_NE(nullptr, pt.get());
  ASSERT_STREQ(nullptr, fptu::check(pt.get()));

  // несколько разных полей int64 включая DENIL
  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  ASSERT_EQ(FPTU_OK, fptu_upsert_int64(pt.get(), 1, 0));
  ASSERT_EQ(FPTU_OK, fptu_upsert_int64(pt.get(), 2, 4242424242));
  ASSERT_EQ(FPTU_OK, fptu_upsert_int64(pt.get(), 3, -INT64_MAX));
  ASSERT_EQ(FPTU_OK, fptu_upsert_int64(pt.get(), 4, FPTU_DENIL_SINT64));
  EXPECT_STREQ(
      "{f1_int64:0,f2_int64:4242424242,f3_int64:-9223372036854775807,f4_"
      "int64:null}",
      json(dict, pt));
}

//------------------------------------------------------------------------------

TEST(Emit, String) {
  schema_dict dict;
  EXPECT_NO_THROW(dict = create_schemaX());

  fptu::tuple_ptr pt(fptu_rw::create(67, 12345));
  ASSERT_NE(nullptr, pt.get());
  ASSERT_STREQ(nullptr, fptu::check(pt.get()));

  ASSERT_EQ(FPTU_OK, fptu_upsert_cstr(pt.get(), 0, ""));
  ASSERT_EQ(FPTU_OK, fptu_upsert_cstr(pt.get(), 1, "строка"));
  ASSERT_EQ(FPTU_OK, fptu_upsert_cstr(pt.get(), 2, "42"));
  ASSERT_EQ(FPTU_OK, fptu_insert_cstr(pt.get(), 2, "string"));
  ASSERT_EQ(FPTU_OK, fptu_insert_cstr(pt.get(), 2, "null"));
  ASSERT_EQ(FPTU_OK, fptu_insert_cstr(pt.get(), 2, "true"));
  ASSERT_EQ(FPTU_OK, fptu_insert_cstr(pt.get(), 2, "false"));

  EXPECT_STREQ("{\"@13\":\"\",f1_cstr:"
               "\"\xD1\x81\xD1\x82\xD1\x80\xD0\xBE\xD0\xBA\xD0\xB0\",f2_cstr:["
               "\"42\",\"string\",\"null\",\"true\",\"false\"]}",
               json(dict, pt));

  EXPECT_STREQ("{\"@13\":\"\",\"f1_cstr\":"
               "\"\xD1\x81\xD1\x82\xD1\x80\xD0\xBE\xD0\xBA\xD0\xB0\",\"f2_"
               "cstr\":[\"42\",\"string\",\"null\",\"true\",\"false\"]}",
               json(dict, pt, false, fptu_json_disable_JSON5));

  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  ASSERT_EQ(FPTU_OK, fptu_upsert_cstr(pt.get(), 1, "\\"));
  ASSERT_EQ(FPTU_OK, fptu_upsert_cstr(pt.get(), 2, "\""));
  ASSERT_EQ(FPTU_OK, fptu_upsert_cstr(pt.get(), 3, "'"));
  ASSERT_EQ(FPTU_OK, fptu_upsert_cstr(pt.get(), 4, "\n\r\t\b\f"));
  ASSERT_EQ(FPTU_OK, fptu_upsert_cstr(pt.get(), 5, "\1\2\3ddfg\xff\x1f"));
  EXPECT_STREQ(
      "{f1_cstr:\"\\\\\",f2_cstr:\"\\\"\",f3_cstr:\"'\",f4_cstr:"
      "\"\\n\\r\\t\\b\\f\",f5_cstr:\"\\u0001\\u0002\\u0003ddfg\xFF\\u0031\"}",
      json(dict, pt));

  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  ASSERT_EQ(FPTU_OK, fptu_upsert_string(pt.get(), 1, std::string(1111, 'A')));
  EXPECT_STREQ(
      "{f1_cstr:"
      "\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\"}",
      json(dict, pt));

  ASSERT_STREQ(nullptr, fptu::check(pt.get()));
}

//------------------------------------------------------------------------------

TEST(Emit, FloatAndDouble) {
  /* Кратко о текстовом представлении плавающей точки:
   *  - Выводится минимальное кол-во цифр достаточное для однозначного
   *    точного воспроизведения исходного машинного значения.
   *  - Десятичная точка не используется, а экспонента выводится
   *    только при ненулевом значении и всегда со знаком.
   *    Таким образом, в соответствии с традициями JavaScript числа с плавающей
   *    точкой неотличимы от целых если их значения равны.
   *    Кроме этого, это немного облегчает/ускоряет как сериализацию,
   *    так и десериализацию.
   *  - Ноль сохраняет знак, т.е. может быть отрицательны. Это спорный момент,
   *    но в большинстве дискуссий читается правильным/ценным сохранять знак.
   * Кроме этого это соответствует де-факто поведению всех актуальных движков
   * JavaScript.
   *  - Infinity и NaN выводятся в соответствии с JSON5, либо как null
   *    если расширения JSON5 выключены опциями. */

  schema_dict dict;
  EXPECT_NO_THROW(dict = create_schemaX());

  fptu::tuple_ptr pt(fptu_rw::create(67, 12345));
  ASSERT_NE(nullptr, pt.get());
  ASSERT_STREQ(nullptr, fptu::check(pt.get()));

  // несколько разных полей fp32 включая DENIL
  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp32(pt.get(), 0, 1));
  ASSERT_EQ(FPTU_OK,
            fptu_upsert_fp32(pt.get(), 1, static_cast<float>(DBL_MIN)));
  ASSERT_EQ(FPTU_OK,
            fptu_upsert_fp32(pt.get(), 2, static_cast<float>(-DBL_MIN)));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp32(pt.get(), 3, FLT_MAX));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp32(pt.get(), 4, FLT_MIN));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp32(pt.get(), 5, std::nanf("")));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp32(pt.get(), 6, -std::nanf("")));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp32(pt.get(), 7,
                                      std::numeric_limits<float>::infinity()));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp32(pt.get(), 8,
                                      -std::numeric_limits<float>::infinity()));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp32(pt.get(), 9, FPTU_DENIL_FP32));
  EXPECT_STREQ(
      "{\"@4\":1,f1_fp32:0,f2_fp32:-0,f3_fp32:34028234663852886e+22,f4_fp32:"
      "11754943508222875e-54,f5_fp32:NaN,f6_fp32:NaN,f7_fp32:+"
      "Infinity,f8_fp32:-Infinity,f9_fp32:null}",
      json(dict, pt));

  //---------------------------------------------------------------------------

  // несколько разных полей fp64 включая DENIL
  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp64(pt.get(), 0, 42));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp64(pt.get(), 1, DBL_MIN / DBL_MAX));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp64(pt.get(), 2, -DBL_MIN / DBL_MAX));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp64(pt.get(), 3, DBL_MAX));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp64(pt.get(), 4, DBL_MIN));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp64(pt.get(), 5, std::nan("")));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp64(pt.get(), 6, -std::nan("")));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp64(pt.get(), 7,
                                      std::numeric_limits<double>::infinity()));
  ASSERT_EQ(
      FPTU_OK,
      fptu_upsert_fp64(pt.get(), 8, -std::numeric_limits<double>::infinity()));
  ASSERT_EQ(FPTU_OK, fptu_upsert_fp64(pt.get(), 9, FPTU_DENIL_FP64));
  EXPECT_STREQ(
      "{\"@7\":42,f1_fp64:0,f2_fp64:-0,f3_fp64:17976931348623157e+292,f4_fp64:"
      "22250738585072014e-324,f5_fp64:NaN,f6_fp64:NaN,f7_fp64:+"
      "Infinity,f8_fp64:-Infinity,f9_fp64:null}",
      json(dict, pt));

  //---------------------------------------------------------------------------

  // Теперь NaN и Infinity с выключенным JSON5
  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  ASSERT_EQ(FPTU_OK, fptu_insert_fp32(pt.get(), 1, std::nanf("")));
  ASSERT_EQ(FPTU_OK, fptu_insert_fp32(pt.get(), 1, -std::nanf("")));
  ASSERT_EQ(FPTU_OK, fptu_insert_fp32(pt.get(), 1,
                                      std::numeric_limits<float>::infinity()));
  ASSERT_EQ(FPTU_OK, fptu_insert_fp32(pt.get(), 1,
                                      -std::numeric_limits<float>::infinity()));
  ASSERT_EQ(FPTU_OK, fptu_insert_fp32(pt.get(), 1, FPTU_DENIL_FP32));

  ASSERT_EQ(FPTU_OK, fptu_insert_fp64(pt.get(), 1, std::nan("")));
  ASSERT_EQ(FPTU_OK, fptu_insert_fp64(pt.get(), 1, -std::nan("")));
  ASSERT_EQ(FPTU_OK, fptu_insert_fp64(pt.get(), 1,
                                      std::numeric_limits<double>::infinity()));
  ASSERT_EQ(
      FPTU_OK,
      fptu_insert_fp64(pt.get(), 1, -std::numeric_limits<double>::infinity()));
  ASSERT_EQ(FPTU_OK, fptu_insert_fp64(pt.get(), 1, FPTU_DENIL_FP64));
  EXPECT_STREQ("{\"f1_fp32\":[null,null,null,null,null],\"f1_fp64\":[null,null,"
               "null,null,null]}",
               json(dict, pt, false, fptu_json_disable_JSON5));

  ASSERT_STREQ(nullptr, fptu::check(pt.get()));
}

//------------------------------------------------------------------------------

TEST(Emit, Datetime) {
  schema_dict dict;
  EXPECT_NO_THROW(dict = create_schemaX());

  fptu::tuple_ptr pt(fptu_rw::create(67, 12345));
  ASSERT_NE(nullptr, pt.get());
  ASSERT_STREQ(nullptr, fptu::check(pt.get()));

  // несколько разных полей datetime включая DENIL
  fptu_time datetime;
  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  ASSERT_EQ(FPTU_OK, fptu_insert_datetime(pt.get(), 1, FPTU_DENIL_TIME));
  datetime.fixedpoint =
      1 /* 1970-01-01 00:00:00.0000000002328306436538696289 */;
  ASSERT_EQ(FPTU_OK, fptu_insert_datetime(pt.get(), 1, datetime));
  EXPECT_STREQ("{f1_datetime:[null,\"1970-01-01T00:00:00."
               "0000000002328306436538696289\"]}",
               json(dict, pt));

  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  datetime.fixedpoint =
      2 /* 1970-01-01 00:00:00.0000000004656612873077392578 */;
  ASSERT_EQ(FPTU_OK, fptu_insert_datetime(pt.get(), 1, datetime));
  EXPECT_STREQ(
      "{f1_datetime:\"1970-01-01T00:00:00.0000000004656612873077392578\"}",
      json(dict, pt));

  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  datetime.fixedpoint =
      INT64_MAX -
      INT32_MAX /* 2038-01-19 03:14:07.5000000000000000000000000000 */;
  ASSERT_EQ(FPTU_OK, fptu_insert_datetime(pt.get(), 1, datetime));
  EXPECT_STREQ("{f1_datetime:\"2038-01-19T03:14:07.5\"}", json(dict, pt));

  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  datetime.fixedpoint =
      INT64_MAX + UINT32_MAX * UINT64_C(41) + 42 /* 2038-01-19 03:14:49 */;
  ASSERT_EQ(FPTU_OK, fptu_insert_datetime(pt.get(), 1, datetime));
  EXPECT_STREQ("{f1_datetime:\"2038-01-19T03:14:49\"}", json(dict, pt));

  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  datetime.fixedpoint =
      UINT64_MAX - 2 /* 2106-02-07 06:28:15.9999999993015080690383911133 */;
  ASSERT_EQ(FPTU_OK, fptu_insert_datetime(pt.get(), 1, datetime));
  EXPECT_STREQ("{f1_datetime:\"2106-02-07T06:28:15.999999999301508069\"}",
               json(dict, pt));

  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  datetime.fixedpoint =
      UINT64_MAX - 1 /* 2106-02-07 06:28:15.9999999995343387126922607422 */;
  ASSERT_EQ(FPTU_OK, fptu_insert_datetime(pt.get(), 1, datetime));
  EXPECT_STREQ("{f1_datetime:\"2106-02-07T06:28:15.9999999995343387127\"}",
               json(dict, pt));

  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  datetime.fixedpoint = UINT64_C(
      803114901978536803) /* 1975-12-05 05:35:59.5556771389674395322799682617 */
      ;
  ASSERT_EQ(FPTU_OK, fptu_insert_datetime(pt.get(), 1, datetime));
  EXPECT_STREQ("{f1_datetime:\"1975-12-05T05:35:59.5556771389674395323\"}",
               json(dict, pt));

  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  datetime.fixedpoint =
      UINT64_C(6617841065462088288) /* 2018-10-29
                                       18:03:14.8705483898520469665527343750 */
      ;
  ASSERT_EQ(FPTU_OK, fptu_insert_datetime(pt.get(), 1, datetime));
  EXPECT_STREQ("{f1_datetime:\"2018-10-29T18:03:14.8705483898520469666\"}",
               json(dict, pt));

  static const unsigned utc_2037_2105[] = {2143766249 /* 2037-12-07 02:37:29 */,
                                           2175302249 /* 2038-12-07 02:37:29 */,
                                           2206838249 /* 2039-12-07 02:37:29 */,
                                           2238460649 /* 2040-12-07 02:37:29 */,
                                           2269996649 /* 2041-12-07 02:37:29 */,
                                           2301532649 /* 2042-12-07 02:37:29 */,
                                           2333068649 /* 2043-12-07 02:37:29 */,
                                           2364691049 /* 2044-12-07 02:37:29 */,
                                           2396227049 /* 2045-12-07 02:37:29 */,
                                           2427763049 /* 2046-12-07 02:37:29 */,
                                           2459299049 /* 2047-12-07 02:37:29 */,
                                           2490921449 /* 2048-12-07 02:37:29 */,
                                           2522457449 /* 2049-12-07 02:37:29 */,
                                           2553993449 /* 2050-12-07 02:37:29 */,
                                           2585529449 /* 2051-12-07 02:37:29 */,
                                           2617151849 /* 2052-12-07 02:37:29 */,
                                           2648687849 /* 2053-12-07 02:37:29 */,
                                           2680223849 /* 2054-12-07 02:37:29 */,
                                           2711759849 /* 2055-12-07 02:37:29 */,
                                           2743382249 /* 2056-12-07 02:37:29 */,
                                           2774918249 /* 2057-12-07 02:37:29 */,
                                           2806454249 /* 2058-12-07 02:37:29 */,
                                           2837990249 /* 2059-12-07 02:37:29 */,
                                           2869612649 /* 2060-12-07 02:37:29 */,
                                           2901148649 /* 2061-12-07 02:37:29 */,
                                           2932684649 /* 2062-12-07 02:37:29 */,
                                           2964220649 /* 2063-12-07 02:37:29 */,
                                           2995843049 /* 2064-12-07 02:37:29 */,
                                           3027379049 /* 2065-12-07 02:37:29 */,
                                           3058915049 /* 2066-12-07 02:37:29 */,
                                           3090451049 /* 2067-12-07 02:37:29 */,
                                           3122073449 /* 2068-12-07 02:37:29 */,
                                           3153609449 /* 2069-12-07 02:37:29 */,
                                           3185145449 /* 2070-12-07 02:37:29 */,
                                           3216681449 /* 2071-12-07 02:37:29 */,
                                           3248303849 /* 2072-12-07 02:37:29 */,
                                           3279839849 /* 2073-12-07 02:37:29 */,
                                           3311375849 /* 2074-12-07 02:37:29 */,
                                           3342911849 /* 2075-12-07 02:37:29 */,
                                           3374534249 /* 2076-12-07 02:37:29 */,
                                           3406070249 /* 2077-12-07 02:37:29 */,
                                           3437606249 /* 2078-12-07 02:37:29 */,
                                           3469142249 /* 2079-12-07 02:37:29 */,
                                           3500764649 /* 2080-12-07 02:37:29 */,
                                           3532300649 /* 2081-12-07 02:37:29 */,
                                           3563836649 /* 2082-12-07 02:37:29 */,
                                           3595372649 /* 2083-12-07 02:37:29 */,
                                           3626995049 /* 2084-12-07 02:37:29 */,
                                           3658531049 /* 2085-12-07 02:37:29 */,
                                           3690067049 /* 2086-12-07 02:37:29 */,
                                           3721603049 /* 2087-12-07 02:37:29 */,
                                           3753225449 /* 2088-12-07 02:37:29 */,
                                           3784761449 /* 2089-12-07 02:37:29 */,
                                           3816297449 /* 2090-12-07 02:37:29 */,
                                           3847833449 /* 2091-12-07 02:37:29 */,
                                           3879455849 /* 2092-12-07 02:37:29 */,
                                           3910991849 /* 2093-12-07 02:37:29 */,
                                           3942527849 /* 2094-12-07 02:37:29 */,
                                           3974063849 /* 2095-12-07 02:37:29 */,
                                           4005686249 /* 2096-12-07 02:37:29 */,
                                           4037222249 /* 2097-12-07 02:37:29 */,
                                           4068758249 /* 2098-12-07 02:37:29 */,
                                           4100294249 /* 2099-12-07 02:37:29 */,
                                           4131830249 /* 2100-12-07 02:37:29 */,
                                           4163366249 /* 2101-12-07 02:37:29 */,
                                           4194902249 /* 2102-12-07 02:37:29 */,
                                           4226438249 /* 2103-12-07 02:37:29 */,
                                           4258060649 /* 2104-12-07 02:37:29 */,
                                           4289596649 /* 2105-12-07 02:37:29 */,
                                           0};

  for (unsigned i = 0; i < utc_2037_2105[i]; ++i) {
    char expected[64];
    snprintf(expected, sizeof(expected),
             "{f1_datetime:\"%04u-12-07T02:37:29\"}", i + 2037);
    datetime.utc = utc_2037_2105[i];
    datetime.fractional = 0;
    ASSERT_EQ(FPTU_OK, fptu_upsert_datetime(pt.get(), 1, datetime));
    EXPECT_STREQ(expected, json(dict, pt));
  }

  ASSERT_STREQ(nullptr, fptu::check(pt.get()));
}

//------------------------------------------------------------------------------

TEST(Emit, FixbinAndOpacity) {
  schema_dict dict;
  EXPECT_NO_THROW(dict = create_schemaX());

  fptu::tuple_ptr pt(fptu_rw::create(67, 12345));
  ASSERT_NE(nullptr, pt.get());
  ASSERT_STREQ(nullptr, fptu::check(pt.get()));

  static const uint8_t zeros[32] = {};
  static uint8_t sequence[256];
  for (unsigned i = 0; i < sizeof(sequence); i++)
    sequence[i] = (uint8_t)~i;

  ASSERT_EQ(FPTU_OK, fptu_upsert_96(pt.get(), 1, zeros));
  ASSERT_EQ(FPTU_OK, fptu_upsert_96(pt.get(), 2, sequence));
  ASSERT_EQ(FPTU_OK, fptu_upsert_128(pt.get(), 1, zeros));
  ASSERT_EQ(FPTU_OK, fptu_upsert_128(pt.get(), 2, sequence));
  ASSERT_EQ(FPTU_OK, fptu_upsert_160(pt.get(), 1, zeros));
  ASSERT_EQ(FPTU_OK, fptu_upsert_160(pt.get(), 2, sequence));
  ASSERT_EQ(FPTU_OK, fptu_upsert_256(pt.get(), 1, zeros));
  ASSERT_EQ(FPTU_OK, fptu_upsert_256(pt.get(), 2, sequence));
  EXPECT_STREQ(
      "{f1_b96:"
      "\"000000000000000000000000\","
      "f2_b96:"
      "\"fffefdfcfbfaf9f8f7f6f5f4\","
      "f1_b128:"
      "\"00000000000000000000000000000000\","
      "f2_b128:"
      "\"fffefdfcfbfaf9f8f7f6f5f4f3f2f1f0\","
      "f1_b160:"
      "\"0000000000000000000000000000000000000000\","
      "f2_b160:"
      "\"fffefdfcfbfaf9f8f7f6f5f4f3f2f1f0efeeedec\","
      "f1_b256:"
      "\"0000000000000000000000000000000000000000000000000000000000000000\","
      "f2_b256:"
      "\"fffefdfcfbfaf9f8f7f6f5f4f3f2f1f0efeeedecebeae9e8e7e6e5e4e3e2e1e0\"}",
      json(dict, pt));

  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  ASSERT_EQ(FPTU_OK, fptu_upsert_opaque(pt.get(), 0, nullptr, 0));
  ASSERT_EQ(FPTU_OK, fptu_upsert_opaque(pt.get(), 1, sequence, 1));
  ASSERT_EQ(FPTU_OK, fptu_upsert_opaque(pt.get(), 2, sequence + 1, 2));
  ASSERT_EQ(FPTU_OK, fptu_upsert_opaque(pt.get(), 3, sequence + 2, 3));
  ASSERT_EQ(FPTU_OK, fptu_upsert_opaque(pt.get(), 4, sequence + 3, 4));
  ASSERT_EQ(FPTU_OK, fptu_upsert_opaque(pt.get(), 5, sequence + 4, 5));
  ASSERT_EQ(FPTU_OK, fptu_upsert_opaque(pt.get(), 6, sequence + 5, 6));
  ASSERT_EQ(FPTU_OK, fptu_upsert_opaque(pt.get(), 7, sequence + 6, 7));
  ASSERT_EQ(FPTU_OK, fptu_upsert_opaque(pt.get(), 8, sequence + 7, 8));
  EXPECT_STREQ("{\"@14\":\"\",f1_opaque:\"ff\",f2_opaque:\"fefd\",f3_opaque:"
               "\"fdfcfb\",f4_opaque:\"fcfbfaf9\",f5_opaque:\"fbfaf9f8f7\",f6_"
               "opaque:\"faf9f8f7f6f5\",f7_opaque:\"f9f8f7f6f5f4f3\",f8_opaque:"
               "\"f8f7f6f5f4f3f2f1\"}",
               json(dict, pt));

  ASSERT_EQ(FPTU_OK, fptu_clear(pt.get()));
  ASSERT_EQ(FPTU_OK,
            fptu_upsert_opaque(pt.get(), 9, sequence, sizeof(sequence)));
  EXPECT_STREQ(
      "{f9_opaque:"
      "\"fffefdfcfbfaf9f8f7f6f5f4f3f2f1f0efeeedecebeae9e8e7e6e5e4e3e2e1e0dfdedd"
      "dcdbdad9d8d7d6d5d4d3d2d1d0cfcecdcccbcac9c8c7c6c5c4c3c2c1c0bfbebdbcbbbab9"
      "b8b7b6b5b4b3b2b1b0afaeadacabaaa9a8a7a6a5a4a3a2a1a09f9e9d9c9b9a9998979695"
      "94939291908f8e8d8c8b8a898887868584838281807f7e7d7c7b7a797877767574737271"
      "706f6e6d6c6b6a696867666564636261605f5e5d5c5b5a595857565554535251504f4e4d"
      "4c4b4a494847464544434241403f3e3d3c3b3a393837363534333231302f2e2d2c2b2a29"
      "2827262524232221201f1e1d1c1b1a191817161514131211100f0e0d0c0b0a0908070605"
      "0403020100\"}",
      json(dict, pt));

  ASSERT_STREQ(nullptr, fptu::check(pt.get()));
}

//------------------------------------------------------------------------------

TEST(SchemaDict, SchemaOfSchema) {
  // sed -e 's/\\/\\\\/g;s/"/\\"/g;s/^/"/;s/$/\\n"/'
  static const char
      reference[] = /* expected JSON-representation for schema of schema */
      "{\n"
      "  field: [\n"
      "    {\n"
      "      name: \"type\",\n"
      "      colnum: 0,\n"
      "      type: \"uint16\",\n"
      "      \"enum\": [\n"
      "        {\n"
      "          value: 0,\n"
      "          name: \"enum:null\"\n"
      "        },\n"
      "        {\n"
      "          value: 1,\n"
      "          name: \"enum:uint16\"\n"
      "        },\n"
      "        {\n"
      "          value: 2,\n"
      "          name: \"enum:int32\"\n"
      "        },\n"
      "        {\n"
      "          value: 3,\n"
      "          name: \"enum:uint32\"\n"
      "        },\n"
      "        {\n"
      "          value: 4,\n"
      "          name: \"enum:fp32\"\n"
      "        },\n"
      "        {\n"
      "          value: 5,\n"
      "          name: \"enum:int64\"\n"
      "        },\n"
      "        {\n"
      "          value: 6,\n"
      "          name: \"enum:uint64\"\n"
      "        },\n"
      "        {\n"
      "          value: 7,\n"
      "          name: \"enum:fp64\"\n"
      "        },\n"
      "        {\n"
      "          value: 8,\n"
      "          name: \"enum:datetime\"\n"
      "        },\n"
      "        {\n"
      "          value: 9,\n"
      "          name: \"enum:b96\"\n"
      "        },\n"
      "        {\n"
      "          value: 10,\n"
      "          name: \"enum:b128\"\n"
      "        },\n"
      "        {\n"
      "          value: 11,\n"
      "          name: \"enum:b160\"\n"
      "        },\n"
      "        {\n"
      "          value: 12,\n"
      "          name: \"enum:b256\"\n"
      "        },\n"
      "        {\n"
      "          value: 13,\n"
      "          name: \"enum:cstr\"\n"
      "        },\n"
      "        {\n"
      "          value: 14,\n"
      "          name: \"enum:opaque\"\n"
      "        },\n"
      "        {\n"
      "          value: 15,\n"
      "          name: \"enum:nested\"\n"
      "        },\n"
      "        {\n"
      "          value: 17,\n"
      "          name: \"enum:uint16[]\"\n"
      "        },\n"
      "        {\n"
      "          value: 18,\n"
      "          name: \"enum:int32[]\"\n"
      "        },\n"
      "        {\n"
      "          value: 19,\n"
      "          name: \"enum:uint32[]\"\n"
      "        },\n"
      "        {\n"
      "          value: 20,\n"
      "          name: \"enum:fp32[]\"\n"
      "        },\n"
      "        {\n"
      "          value: 21,\n"
      "          name: \"enum:int64[]\"\n"
      "        },\n"
      "        {\n"
      "          value: 22,\n"
      "          name: \"enum:uint64[]\"\n"
      "        },\n"
      "        {\n"
      "          value: 23,\n"
      "          name: \"enum:fp64[]\"\n"
      "        },\n"
      "        {\n"
      "          value: 24,\n"
      "          name: \"enum:datetime[]\"\n"
      "        },\n"
      "        {\n"
      "          value: 25,\n"
      "          name: \"enum:b96[]\"\n"
      "        },\n"
      "        {\n"
      "          value: 26,\n"
      "          name: \"enum:b128[]\"\n"
      "        },\n"
      "        {\n"
      "          value: 27,\n"
      "          name: \"enum:b160[]\"\n"
      "        },\n"
      "        {\n"
      "          value: 28,\n"
      "          name: \"enum:b256[]\"\n"
      "        },\n"
      "        {\n"
      "          value: 29,\n"
      "          name: \"enum:cstr[]\"\n"
      "        },\n"
      "        {\n"
      "          value: 30,\n"
      "          name: \"enum:opaque[]\"\n"
      "        },\n"
      "        {\n"
      "          value: 31,\n"
      "          name: \"enum:nested[]\"\n"
      "        }\n"
      "      ]\n"
      "    },\n"
      "    {\n"
      "      name: \"colnum\",\n"
      "      colnum: 0,\n"
      "      type: \"uint32\"\n"
      "    },\n"
      "    {\n"
      "      name: \"name\",\n"
      "      colnum: 0,\n"
      "      type: \"cstr\"\n"
      "    },\n"
      "    {\n"
      "      name: \"field\",\n"
      "      colnum: 0,\n"
      "      type: \"nested\"\n"
      "    },\n"
      "    {\n"
      "      name: \"value\",\n"
      "      colnum: 1,\n"
      "      type: \"uint16\"\n"
      "    },\n"
      "    {\n"
      "      name: \"enum\",\n"
      "      colnum: 1,\n"
      "      type: \"nested\"\n"
      "    }\n"
      "  ]\n"
      "}";

  schema_dict self;
  EXPECT_NO_THROW(self = schema_dict::dict_of_schema());
  std::string json;
  EXPECT_NO_THROW(json = self.schema2json());
  ASSERT_FALSE(json.empty());
  EXPECT_STREQ(json.c_str(), reference);
  /* fprintf(stderr, "\n---\n%s\n---\n", json.c_str()); */
}

//------------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
