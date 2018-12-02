/*
 * Copyright 2016-2018 libfpta authors: please see AUTHORS file.
 *
 * This file is part of libfpta, aka "Fast Positive Tables".
 *
 * libfpta is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * libfpta is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with libfpta.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "details.h"

#include <unordered_map>
#include <unordered_set>
#include <vector>

template <bool first> static __inline bool is_valid_char4name(char c) {
  if (first ? isalpha(c) : isalnum(c))
    return true;
  if (c == '_')
    return true;
  if (FPTA_ALLOW_DOT4NAMES && c == '.')
    return true;

  return false;
}

__hot fpta_shove_t fpta_name_validate_and_shove(const fptu::string_view &name) {
  const auto length = name.length();
  if (unlikely(length < fpta_name_len_min || length > fpta_name_len_max))
    return 0;

  if (unlikely(!is_valid_char4name<true>(name[0])))
    return 0;

  char uppercase[fpta_name_len_max];
  uppercase[0] = (char)toupper(name[0]);
  for (size_t i = 1; i < length; ++i) {
    if (unlikely(!is_valid_char4name<false>(name[i])))
      return 0;
    uppercase[i] = (char)toupper(name[i]);
  }

  constexpr uint64_t seed = UINT64_C(0x7D7859C1743733) * FPTA_VERSION_MAJOR +
                            UINT64_C(0xC8E6067A913D) * FPTA_VERSION_MINOR +
                            1543675803 /* Сб дек  1 17:50:03 MSK 2018 */;
  return t1ha2_atonce(uppercase, length, seed) << fpta_name_hash_shift;
}

bool fpta_validate_name(const char *name) {
  return fpta_name_validate_and_shove(fptu::string_view(name)) != 0;
}

//----------------------------------------------------------------------------

namespace {

static constexpr fpta_shove_t dict_key = 0;

/* Простейший словарь.
 * Реализован как вектор из пар <хеш, имя>, которые отсортированы по значению
 * хеша. Имена должны храниться снаружи, внутри вектора только ссылки. */
class trivial_dict {
  typedef std::pair<fpta_shove_t, const char *> item;
  static constexpr size_t mask_length = (1 << fpta_name_hash_shift) - 1;
  static constexpr fpta_shove_t mask_hash = ~fpta_shove_t(mask_length);

  static constexpr fpta_shove_t internal(const fpta_shove_t &shove,
                                         const size_t &length) {
    static_assert(mask_length > fpta_name_len_max, "unexpected");
#if __cplusplus >= 201402L
    assert(length >= fpta_name_len_min && length <= fpta_name_len_max);
#endif /* C++14 */
    return (shove & mask_hash) + length;
  }

  static constexpr fpta_shove_t internal(fpta_shove_t shove) {
    return shove | mask_length;
  }

  static fpta_shove_t internal(const fptu::string_view &name) {
    return internal(fpta_name_validate_and_shove(name), name.length());
  }

  static constexpr size_t length(const fpta_shove_t &shove) {
    return size_t(shove & mask_length);
  }

  static constexpr size_t length(const item &word) {
    return length(word.first);
  }

  static constexpr fptu::string_view take(const item &word) {
    return fptu::string_view(word.second, length(word));
  }

  static constexpr bool is_valid(const fpta_shove_t &shove) {
    return shove && length(shove) >= fpta_name_len_min &&
           length(shove) <= fpta_name_len_max;
  }

  static bool is_valid(const item &word) {
    return is_valid(word.first) && internal(take(word)) == word.first;
  }

  struct gt {
    bool constexpr operator()(const item &a, const item &b) const {
      return a.first > b.first;
    }
    bool constexpr operator()(const item &a, const fpta_shove_t &b) const {
      return a.first > b;
    }
    bool constexpr operator()(const fpta_shove_t &a, const item &b) const {
      return a > b.first;
    }
    bool constexpr operator()(const fpta_shove_t &a,
                              const fpta_shove_t &b) const {
      return a > b;
    }
  };

  struct eq {
    bool constexpr operator()(const fpta_shove_t &a,
                              const fpta_shove_t &b) const {
      return fpta_shove_eq(a, b);
    }
  };

  void append(size_t anchor, const fptu::string_view &name) {
    const fpta_shove_t shove = internal(name);
    assert(is_valid(shove));
    assert(anchor <= vector.size());
    if (!std::binary_search(vector.begin(), vector.begin() + anchor, shove,
                            gt())) {
      vector.emplace_back(item(shove, name.begin()));
      assert(is_valid(vector.back()));
    }
  }
  std::vector<item> vector;

  ptrdiff_t search(const fpta_shove_t &shove) const {
    assert(validate());
    const auto i =
        std::lower_bound(vector.begin(), vector.end(), internal(shove), gt());
    return (i != vector.end() && eq()(i->first, internal(shove)))
               ? i - vector.begin()
               : -1;
  }

public:
  static constexpr char delimiter = '\t';

  trivial_dict() : vector() {}

  trivial_dict(const fptu::string_view &str) : vector() {
    merge(str, fptu::string_view());
  }

  bool empty() const { return vector.empty(); }

  bool exists(fpta_shove_t shove) const { return search(shove) >= 0; }

  fptu::string_view lookup(fpta_shove_t shove) const {
    const auto i = search(shove);
    return (i >= 0) ? take(vector[i]) : fptu::string_view();
  }

  bool validate() const {
    for (size_t i = 0; i < vector.size(); ++i) {
      if (!is_valid(vector[i]))
        return false;
      if (i > 0 && (!gt()(vector[i - 1], vector[i]) ||
                    eq()(vector[i - 1].first, vector[i].first)))
        return false;
    }
    return true;
  }

  bool fetch(const MDBX_val &data) {
    vector.clear();
    merge(fptu::string_view((const char *)data.iov_base, data.iov_len),
          fptu::string_view());
    return validate();
  }

  bool merge(const fptu::string_view &columns_chain,
             const fptu::string_view &table_name) {
    assert(validate());
    assert(table_name.end() ==
           std::find(table_name.begin(), table_name.end(), delimiter));

    vector.reserve(
        !table_name.empty() + !columns_chain.empty() +
        std::count(columns_chain.begin(), columns_chain.end(), delimiter));
    const size_t anchor = vector.size();
    if (!table_name.empty())
      append(anchor, table_name);

    for (auto *scan = columns_chain.begin(); scan < columns_chain.end();) {
      const auto next = std::find(scan, columns_chain.end(), delimiter);
      append(anchor, fptu::string_view(scan, next));
      scan = next + 1;
    }

    if (anchor == vector.size())
      return false;

    std::sort(vector.begin() + anchor, vector.end(), gt());
    std::inplace_merge(vector.begin(), vector.begin() + anchor, vector.end(),
                       gt());
    assert(validate());
    return true;
  }

  bool pickup(const trivial_dict &from, const fpta_shove_t &shove) {
    assert(validate() && from.validate());
    const auto dst =
        std::lower_bound(vector.begin(), vector.end(), internal(shove), gt());
    if (dst != vector.end() && eq()(dst->first, internal(shove)))
      return false;

    const auto src = std::lower_bound(from.vector.begin(), from.vector.end(),
                                      internal(shove), gt());
    assert(src != from.vector.end() && eq()(src->first, internal(shove)));

    vector.insert(dst, *src);
    assert(validate() && from.validate());
    return true;
  }

  std::string string() const {
    std::string result;
    if (!empty()) {
      size_t bytes = length(vector.front());
      for (size_t i = 1; i < vector.size(); ++i)
        bytes += 1 + length(vector[i]);
      result.reserve(bytes);

      result.assign(take(vector.front()));
      for (size_t i = 1; i < vector.size(); ++i) {
        result.append(1, delimiter);
        result.append(take(vector[i]));
      }
    }
    return result;
  }
};

} // namespace
//----------------------------------------------------------------------------

static constexpr int index2prio(const fpta_shove_t index) {
  /* primary, secondary, non-indexed non-nullable, non-indexed nullable */
  if (fpta_is_indexed(index))
    return fpta_index_is_primary(index) ? 0 : 1;
  return (index & fpta_index_fnullable) ? 3 : 2;
}

static constexpr bool shove_index_compare(const fpta_shove_t &left,
                                          const fpta_shove_t &right) {
  const auto left_prio = index2prio(left);
  const auto rigth_prio = index2prio(right);
  return left_prio < rigth_prio || (left_prio == rigth_prio && left < right);
}

//----------------------------------------------------------------------------

static int fpta_schema_open(fpta_txn *txn, bool create) {
  assert(fpta_txn_validate(txn, create ? fpta_schema : fpta_read) ==
         FPTA_SUCCESS);
  const fpta_shove_t key_shove =
      fpta_column_shove(0, fptu_uint64, fpta_primary_unique_ordered_obverse);
  const fpta_shove_t data_shove =
      fpta_column_shove(0, fptu_opaque, fpta_primary_unique_ordered_obverse);
  return fpta_dbi_open(txn, 0, txn->db->schema_dbi,
                       create ? MDBX_INTEGERKEY | MDBX_CREATE : MDBX_INTEGERKEY,
                       key_shove, data_shove);
}

static size_t fpta_schema_stored_size(fpta_column_set *column_set,
                                      const void *composites_end) {
  assert(column_set != nullptr);
  assert(column_set->count >= 1 && column_set->count <= fpta_max_cols);
  assert(&column_set->composites[0] <= composites_end &&
         FPT_ARRAY_END(column_set->composites) >= composites_end);

  return fpta_table_schema::header_size() +
         sizeof(fpta_shove_t) * column_set->count + (uintptr_t)composites_end -
         (uintptr_t)&column_set->composites[0];
}

static void fpta_schema_free(fpta_table_schema *def) {
  if (likely(def)) {
    def->_stored.signature = 0;
    def->_stored.checksum = ~def->_stored.checksum;
    def->_stored.count = 0;
    free(def);
  }
}

static int fpta_schema_clone(const fpta_shove_t schema_key,
                             const MDBX_val &schema_data,
                             fpta_table_schema **ptrdef) {
  assert(ptrdef != nullptr);
  const size_t payload_size =
      schema_data.iov_len - fpta_table_schema::header_size();

  const auto stored = (const fpta_table_stored_schema *)schema_data.iov_base;
  const size_t bytes =
      sizeof(fpta_table_schema) - sizeof(fpta_table_stored_schema::columns) +
      payload_size +
      stored->count * sizeof(fpta_table_schema::composite_item_t);

  fpta_table_schema *schema = (fpta_table_schema *)realloc(*ptrdef, bytes);
  if (unlikely(schema == nullptr))
    return FPTA_ENOMEM;

  *ptrdef = schema;
  memset(schema, ~0, bytes);
  memcpy(&schema->_stored, schema_data.iov_base, schema_data.iov_len);
  fpta_table_schema::composite_item_t *const offsets =
      (fpta_table_schema::composite_item_t *)((uint8_t *)schema + bytes) -
      schema->_stored.count;
  schema->_key = schema_key;
  schema->_composite_offsets = offsets;

  const auto composites_begin =
      (const fpta_table_schema::composite_item_t *)&schema->_stored
          .columns[schema->_stored.count];
  const auto composites_end = schema->_composite_offsets;
  auto composites = composites_begin;
  for (size_t i = 0; i < schema->_stored.count; ++i) {
    const fpta_shove_t column_shove = schema->_stored.columns[i];
    if (!fpta_is_indexed(column_shove))
      break;
    if (!fpta_is_composite(column_shove))
      continue;
    if (unlikely(composites >= composites_end || *composites == 0))
      return FPTA_EOOPS;

    const auto first = composites + 1;
    const auto last = first + *composites;
    if (unlikely(last > composites_end))
      return FPTA_EOOPS;

    const ptrdiff_t distance = composites - composites_begin;
    assert(distance >= 0 && distance <= fpta_max_cols);
    offsets[i] = (fpta_table_schema::composite_item_t)distance;
    composites = last;
  }
  return FPTA_SUCCESS;
}

static constexpr bool fpta_check_indextype(const fpta_index_type index_type) {
  switch (index_type) {
  default:
    return false;

  case fpta_primary_withdups_ordered_obverse:
  case fpta_primary_withdups_ordered_obverse_nullable:
  case fpta_primary_withdups_ordered_reverse:
  case fpta_primary_withdups_ordered_reverse_nullable:

  case fpta_primary_unique_ordered_obverse:
  case fpta_primary_unique_ordered_obverse_nullable:
  case fpta_primary_unique_ordered_reverse:
  case fpta_primary_unique_ordered_reverse_nullable:

  case fpta_primary_unique_unordered:
  case fpta_primary_unique_unordered_nullable_obverse:
  case fpta_primary_unique_unordered_nullable_reverse:

  case fpta_primary_withdups_unordered:
  case fpta_primary_withdups_unordered_nullable_obverse:
    /* fpta_primary_withdups_unordered_nullable_reverse = НЕДОСТУПЕН,
     * так как битовая коминация совпадает с fpta_noindex_nullable */

  case fpta_secondary_withdups_ordered_obverse:
  case fpta_secondary_withdups_ordered_obverse_nullable:
  case fpta_secondary_withdups_ordered_reverse:
  case fpta_secondary_withdups_ordered_reverse_nullable:

  case fpta_secondary_unique_ordered_obverse:
  case fpta_secondary_unique_ordered_obverse_nullable:
  case fpta_secondary_unique_ordered_reverse:
  case fpta_secondary_unique_ordered_reverse_nullable:

  case fpta_secondary_unique_unordered:
  case fpta_secondary_unique_unordered_nullable_obverse:
  case fpta_secondary_unique_unordered_nullable_reverse:

  case fpta_secondary_withdups_unordered:
  case fpta_secondary_withdups_unordered_nullable_obverse:
  case fpta_secondary_withdups_unordered_nullable_reverse:
  // fall through
  case fpta_index_none:
  case fpta_noindex_nullable:
    return true;
  }
}

static int fpta_columns_description_validate(
    const fpta_shove_t *shoves, size_t shoves_count,
    const fpta_table_schema::composite_item_t *const composites_begin,
    const fpta_table_schema::composite_item_t *const composites_detent,
    const void **composites_eof = nullptr) {
  if (unlikely(shoves_count < 1))
    return FPTA_EINVAL;
  if (unlikely(shoves_count > fpta_max_cols))
    return FPTA_SCHEMA_CORRUPTED;

  if (unlikely(composites_begin > composites_detent ||
               fpta_is_intersected(shoves, shoves + shoves_count,
                                   composites_begin, composites_detent)))
    return FPTA_SCHEMA_CORRUPTED;

  size_t index_count = 0;
  auto composites = composites_begin;
  for (size_t i = 0; i < shoves_count; ++i) {
    const fpta_shove_t shove = shoves[i];
    const fpta_index_type index_type = fpta_shove2index(shove);
    if (!fpta_check_indextype(index_type))
      return FPTA_EFLAG;

    if ((i == 0) !=
        (fpta_is_indexed(index_type) && fpta_index_is_primary(index_type)))
      /* первичный индекс обязан быть, только один и только в самом начале */
      return FPTA_EFLAG;

    if (fpta_index_is_secondary(index_type) && !fpta_index_is_unique(shoves[0]))
      /* для вторичных индексов первичный ключ должен быть уникальным */
      return FPTA_EFLAG;

    if (fpta_is_indexed(index_type) && ++index_count > fpta_max_indexes)
      return FPTA_TOOMANY;
    assert((index_type & fpta_column_index_mask) == index_type);
    assert(index_type != (fpta_index_type)fpta_flag_table);

    const fptu_type data_type = fpta_shove2type(shove);
    if (data_type > fptu_nested) {
      if (data_type == (fptu_null | fptu_farray))
        return FPTA_ETYPE;
      /* support indexes for arrays */
      if (fpta_is_indexed(index_type))
        return FPTA_EFLAG;
    } else {
      if (data_type == /* composite */ fptu_null) {
        if (unlikely(!fpta_is_indexed(index_type)))
          return FPTA_EFLAG;
        if (unlikely(composites >= composites_detent || *composites == 0))
          return FPTA_SCHEMA_CORRUPTED;

        const auto first = composites + 1;
        const auto last = first + *composites;
        if (unlikely(last > composites_detent))
          return FPTA_SCHEMA_CORRUPTED;

        composites = last;
        int rc = fpta_composite_index_validate(index_type, first, last, shoves,
                                               shoves_count, composites_begin,
                                               composites_detent, shove);
        if (rc != FPTA_SUCCESS)
          return rc;
      } else {
        if (unlikely(data_type < fptu_uint16 || data_type > fptu_nested))
          return FPTA_ETYPE;
        if (fpta_is_indexed(index_type) && fpta_index_is_reverse(index_type) &&
            (fpta_index_is_unordered(index_type) || data_type < fptu_96) &&
            !(fpta_is_indexed_and_nullable(index_type) &&
              fpta_nullable_reverse_sensitive(data_type)))
          return FPTA_EFLAG;
      }
    }

    for (size_t j = 0; j < i; ++j)
      if (fpta_shove_eq(shove, shoves[j]))
        return FPTA_EEXIST;
  }

  if (composites_eof)
    *composites_eof = composites;

  return FPTA_SUCCESS;
}

static int fpta_column_set_sort(fpta_column_set *column_set) {
  assert(column_set != nullptr && column_set->count > 0 &&
         column_set->count <= fpta_max_cols);
  if (std::is_sorted(column_set->shoves, column_set->shoves + column_set->count,
                     [](const fpta_shove_t &left, const fpta_shove_t &right) {
                       return shove_index_compare(left, right);
                     }))
    return FPTA_SUCCESS;

  std::vector<fpta_shove_t> sorted(column_set->shoves,
                                   column_set->shoves + column_set->count);
  /* sort descriptions of columns, so that a non-indexed was at the end */
  std::sort(sorted.begin(), sorted.end(),
            [](const fpta_shove_t &left, const fpta_shove_t &right) {
              return shove_index_compare(left, right);
            });

  /* fixup composites after sort */
  std::vector<fpta_table_schema::composite_item_t> fixup;
  fixup.reserve(column_set->count);
  auto composites = column_set->composites;
  for (size_t i = 0; i < column_set->count; ++i) {
    const fpta_shove_t column_shove = column_set->shoves[i];
    if (!fpta_is_composite(column_shove))
      continue;
    if (unlikely(!fpta_is_indexed(column_shove) ||
                 composites >= FPT_ARRAY_END(column_set->composites) ||
                 *composites == 0))
      return FPTA_SCHEMA_CORRUPTED;

    const auto first = composites + 1;
    const auto last = first + *composites;
    if (unlikely(last > FPT_ARRAY_END(column_set->composites)))
      return FPTA_SCHEMA_CORRUPTED;

    fixup.push_back(*composites);
    composites = last;
    for (auto scan = first; scan < last; ++scan) {
      const size_t column_number = *scan;
      if (unlikely(column_number >= column_set->count))
        return FPTA_SCHEMA_CORRUPTED;
      if (unlikely(std::find(first, scan, column_number) != scan))
        return FPTA_EEXIST;

      const auto renum = std::distance(
          sorted.begin(), std::find(sorted.begin(), sorted.end(),
                                    column_set->shoves[column_number]));
      if (unlikely(renum < 0 || (unsigned)renum >= column_set->count))
        return FPTA_EOOPS;

      fixup.push_back(static_cast<fpta_table_schema::composite_item_t>(renum));
    }
  }

  /* put sorted arrays */
  memset(column_set->shoves, 0, sizeof(column_set->shoves));
  memset(column_set->composites, 0, sizeof(column_set->composites));
  std::copy(sorted.begin(), sorted.end(), column_set->shoves);
  std::copy(fixup.begin(), fixup.end(), column_set->composites);

  /* final checking */
  return fpta_columns_description_validate(
      column_set->shoves, column_set->count, column_set->composites,
      FPT_ARRAY_END(column_set->composites));
}

int fpta_column_set_add(fpta_column_set *column_set, const char *id_name,
                        fptu_type data_type, fpta_index_type index_type) {
  const fpta_shove_t name_shove = fpta_shove_name(id_name, fpta_column);
  if (unlikely(!name_shove))
    return FPTA_ENAME;

  if (!fpta_check_indextype(index_type))
    return FPTA_EFLAG;

  assert((index_type & fpta_column_index_mask) == index_type);
  assert(index_type != (fpta_index_type)fpta_flag_table);

  if (unlikely(column_set == nullptr || column_set->count > fpta_max_cols))
    return FPTA_EINVAL;

  const fpta_shove_t shove =
      fpta_column_shove(name_shove, data_type, index_type);
  assert(fpta_shove2index(shove) != (fpta_index_type)fpta_flag_table);
  for (size_t i = 0; i < column_set->count; ++i) {
    if (fpta_shove_eq(column_set->shoves[i], shove))
      return FPTA_EEXIST;
  }

  if (fpta_is_indexed(index_type) && fpta_index_is_primary(index_type)) {
    if (column_set->shoves[0])
      return FPTA_EEXIST;
    if (column_set->count < 1)
      column_set->count = 1;
    else if (!fpta_index_is_unique(shove))
      return FPTA_EFLAG;
    column_set->shoves[0] = shove;
  } else {
    if (fpta_index_is_secondary(index_type) && column_set->shoves[0] &&
        !fpta_index_is_unique(column_set->shoves[0]))
      return FPTA_EFLAG;
    if (unlikely(column_set->count == fpta_max_cols))
      return FPTA_TOOMANY;
    size_t place = (column_set->count > 0) ? column_set->count : 1;
    column_set->shoves[place] = shove;
    column_set->count = (unsigned)place + 1;
  }

  const size_t dict_length =
      column_set->dict_ptr ? strlen((const char *)column_set->dict_ptr) + 1 : 0;
  const size_t dict_add_length = strlen(id_name) + 1;
  char *const dict_string =
      (char *)realloc(column_set->dict_ptr, dict_length + dict_add_length);
  if (unlikely(!dict_string))
    return FPTA_ENOMEM;

  column_set->dict_ptr = dict_string;
  if (dict_length) {
    assert(dict_string[dict_length - 1] == '\0');
    dict_string[dict_length - 1] = trivial_dict::delimiter;
  }
  memcpy(dict_string + dict_length, id_name, dict_add_length);
  return FPTA_SUCCESS;
}

static const fpta_table_stored_schema *
fpta_schema_image_validate(const fpta_shove_t schema_key,
                           const MDBX_val &schema_data) {
  if (unlikely(schema_data.iov_len < sizeof(fpta_table_stored_schema)))
    return nullptr;

  if (unlikely((schema_data.iov_len - sizeof(fpta_table_stored_schema)) %
               std::min(sizeof(fpta_shove_t),
                        sizeof(fpta_table_schema::composite_item_t))))
    return nullptr;

  const fpta_table_stored_schema *schema =
      (const fpta_table_stored_schema *)schema_data.iov_base;
  if (unlikely(schema->signature != FTPA_SCHEMA_SIGNATURE))
    return nullptr;

  if (unlikely(schema->count < 1 || schema->count > fpta_max_cols))
    return nullptr;

  if (unlikely(schema_data.iov_len < fpta_table_schema::header_size() +
                                         sizeof(fpta_shove_t) * schema->count))
    return nullptr;

  if (unlikely(schema->csn == 0))
    return nullptr;

  if (unlikely(fpta_shove2index(schema_key) !=
               (fpta_index_type)fpta_flag_table))
    return nullptr;

  uint64_t checksum =
      t1ha2_atonce(&schema->signature, schema_data.iov_len - sizeof(checksum),
                   FTPA_SCHEMA_CHECKSEED);
  if (unlikely(checksum != schema->checksum))
    return nullptr;

  const void *const composites_begin = schema->columns + schema->count;
  const void *const composites_end =
      (uint8_t *)schema_data.iov_base + schema_data.iov_len;
  if (FPTA_SUCCESS !=
      fpta_columns_description_validate(
          schema->columns, schema->count,
          (const fpta_table_schema::composite_item_t *)composites_begin,
          (const fpta_table_schema::composite_item_t *)composites_end))
    return nullptr;

  if (!std::is_sorted(schema->columns, schema->columns + schema->count,
                      [](const fpta_shove_t &left, const fpta_shove_t &right) {
                        return shove_index_compare(left, right);
                      }))
    return nullptr;

  return schema;
}

static const fpta_table_stored_schema *
fpta_schema_image_validate(const fpta_shove_t schema_key,
                           const MDBX_val &schema_data,
                           const trivial_dict &schema_dict) {

  const fpta_table_stored_schema *const schema =
      fpta_schema_image_validate(schema_key, schema_data);
  if (likely(schema)) {

    for (size_t i = 0; i < schema->count; ++i)
      if (unlikely(!schema_dict.exists(schema->columns[i])))
        return nullptr;
  }
  return schema;
}

static bool fpta_schema_image_validate(const fpta_shove_t schema_key,
                                       const MDBX_val &schema_data,
                                       const MDBX_val &schema_dict) {
  trivial_dict dict;
  if (!dict.fetch(schema_dict))
    return false;

  return fpta_schema_image_validate(schema_key, schema_data, dict);
}

static int fpta_schema_read(fpta_txn *txn, fpta_shove_t schema_key,
                            fpta_table_schema **def) {
  assert(fpta_txn_validate(txn, fpta_read) == FPTA_SUCCESS && def);

  int rc;
  fpta_db *db = txn->db;
  if (db->schema_dbi < 1) {
    rc = fpta_schema_open(txn, false);
    if (rc != MDBX_SUCCESS)
      return rc;
  }

  MDBX_val schema_data, key;
  key.iov_len = sizeof(schema_key);
  key.iov_base = &schema_key;
  rc = mdbx_get(txn->mdbx_txn, db->schema_dbi, &key, &schema_data);
  if (rc != MDBX_SUCCESS)
    return rc;

  MDBX_val schema_dict;
  key.iov_len = sizeof(dict_key);
  key.iov_base = (void *)&dict_key;
  rc = mdbx_get(txn->mdbx_txn, db->schema_dbi, &key, &schema_dict);
  if (unlikely(rc != MDBX_SUCCESS)) {
    if (rc != MDBX_NOTFOUND)
      return rc;
    schema_dict.iov_base = nullptr;
    schema_dict.iov_len = 0;
  }

  if (unlikely(
          !fpta_schema_image_validate(schema_key, schema_data, schema_dict)))
    return FPTA_SCHEMA_CORRUPTED;

  return fpta_schema_clone(schema_key, schema_data, def);
}

//----------------------------------------------------------------------------

static constexpr unsigned column_set_signature =
    1543140327 /* Вс ноя 25 15:10:11 MSK 2018 */;

void fpta_column_set_init(fpta_column_set *column_set) {
  assert(column_set->signature != column_set_signature ||
         column_set->dict_ptr == nullptr);
  column_set->signature = column_set_signature;
  column_set->count = 0;
  column_set->dict_ptr = nullptr;
  column_set->shoves[0] = 0;
  column_set->composites[0] = 0;
}

int fpta_column_set_destroy(fpta_column_set *column_set) {
  if (likely(column_set != nullptr && column_set->count != FPTA_DEADBEEF &&
             column_set->signature == column_set_signature)) {
    column_set->signature = ~column_set_signature;
    column_set->count = (unsigned)FPTA_DEADBEEF;
    free(column_set->dict_ptr);
    column_set->dict_ptr = (void *)(intptr_t)FPTA_DEADBEEF;
    column_set->shoves[0] = 0;
    column_set->composites[0] = INT16_MAX;
    return FPTA_SUCCESS;
  }

  return FPTA_EINVAL;
}

int fpta_column_set_reset(fpta_column_set *column_set) {
  if (likely(column_set != nullptr && column_set->count != FPTA_DEADBEEF &&
             column_set->signature == column_set_signature)) {
    if (column_set->dict_ptr)
      *(char *)column_set->dict_ptr = '\0';
    column_set->count = 0;
    column_set->shoves[0] = 0;
    column_set->composites[0] = 0;
    return FPTA_SUCCESS;
  }

  return FPTA_EINVAL;
}

int fpta_column_describe(const char *column_name, fptu_type data_type,
                         fpta_index_type index_type,
                         fpta_column_set *column_set) {
  if (unlikely(data_type < fptu_uint16 || data_type > fptu_nested))
    return FPTA_ETYPE;

  if (unlikely(fpta_is_indexed(index_type) &&
               fpta_index_is_reverse(index_type) &&
               (fpta_index_is_unordered(index_type) || data_type < fptu_96) &&
               !(fpta_is_indexed_and_nullable(index_type) &&
                 fpta_nullable_reverse_sensitive(data_type))))
    return FPTA_EFLAG;

  return fpta_column_set_add(column_set, column_name, data_type, index_type);
}

int fpta_column_set_validate(fpta_column_set *column_set) {
  if (unlikely(column_set == nullptr))
    return FPTA_EINVAL;

  return fpta_columns_description_validate(
      column_set->shoves, column_set->count, column_set->composites,
      FPT_ARRAY_END(column_set->composites));
}

//----------------------------------------------------------------------------

static constexpr unsigned schema_info_signature = 1543147811;

int fpta_schema_fetch(fpta_txn *txn, fpta_schema_info *info) {
  if (unlikely(!info))
    return FPTA_EINVAL;
  memset(info, 0, sizeof(fpta_schema_info));
  info->signature = schema_info_signature;

  int rc = fpta_txn_validate(txn, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_db *db = txn->db;
  if (db->schema_dbi < 1) {
    rc = fpta_schema_open(txn, false);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  rc = mdbx_dbi_sequence(txn->mdbx_txn, txn->db->schema_dbi, &info->version, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  MDBX_cursor *mdbx_cursor;
  rc = mdbx_cursor_open(txn->mdbx_txn, db->schema_dbi, &mdbx_cursor);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  MDBX_val data, key;
  rc = mdbx_cursor_get(mdbx_cursor, &key, &data, MDBX_FIRST);
  while (likely(rc == MDBX_SUCCESS)) {
    if (unlikely(info->tables_count >= fpta_tables_max)) {
      rc = FPTA_SCHEMA_CORRUPTED;
      break;
    }

    if (unlikely(key.iov_len != sizeof(fpta_shove_t))) {
      rc = FPTA_SCHEMA_CORRUPTED;
      break;
    }

    fpta_shove_t shove;
    memcpy(&shove, key.iov_base, sizeof(fpta_shove_t));
    if (shove == dict_key) {
      assert(info->tables_count == 0);
      if (unlikely(info->tables_count || info->dict_ptr) /* paranoia */) {
        rc = FPTA_SCHEMA_CORRUPTED;
        break;
      }
      info->dict_ptr = new trivial_dict() /* FIXME: std::bad_alloc */;
      if (unlikely(!info->dict_ptr)) {
        rc = FPTA_ENOMEM;
        break;
      }
      if (unlikely(!static_cast<trivial_dict *>(info->dict_ptr)->fetch(data))) {
        rc = FPTA_SCHEMA_CORRUPTED;
        break;
      }
    } else {
      if (unlikely(!info->dict_ptr)) {
        rc = FPTA_SCHEMA_CORRUPTED;
        break;
      }
      fpta_name *id = &info->tables_names[info->tables_count];
      id->shove = shove;
      // id->table_schema = nullptr; /* done by memset() */
      assert(id->table_schema == nullptr);

      rc = fpta_id_validate(id, fpta_table);
      if (unlikely(rc != FPTA_SUCCESS))
        break;

      if (unlikely(!fpta_schema_image_validate(
              id->shove, data, *static_cast<trivial_dict *>(info->dict_ptr)))) {
        rc = FPTA_SCHEMA_CORRUPTED;
        break;
      }

      info->tables_count += 1;
    }
    rc = mdbx_cursor_get(mdbx_cursor, &key, &data, MDBX_NEXT);
  }

  mdbx_cursor_close(mdbx_cursor);
  return likely(rc == MDBX_NOTFOUND) ? (int)FPTA_SUCCESS : rc;
}

int fpta_schema_destroy(fpta_schema_info *info) {
  if (unlikely(info == nullptr || info->tables_count == FPTA_DEADBEEF ||
               info->signature != schema_info_signature))
    return FPTA_EINVAL;

  info->signature = ~schema_info_signature;
  delete static_cast<trivial_dict *>(info->dict_ptr);
  info->dict_ptr = nullptr;

  for (size_t i = 0; i < info->tables_count; i++)
    fpta_name_destroy(info->tables_names + i);
  info->tables_count = (unsigned)FPTA_DEADBEEF;

  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

static int fpta_name_init(fpta_name *id, const char *name,
                          fpta_schema_item schema_item) {
  if (unlikely(id == nullptr))
    return FPTA_EINVAL;

  memset(id, 0, sizeof(fpta_name));
  switch (schema_item) {
  default:
    return FPTA_EFLAG;
  case fpta_table:
    id->shove = fpta_shove_name(name, fpta_table);
    if (unlikely(!id->shove))
      return FPTA_ENAME;

    // id->table_schema = nullptr; /* done by memset() */
    assert(id->table_schema == nullptr);
    assert(fpta_id_validate(id, fpta_table) == FPTA_SUCCESS);
    break;
  case fpta_column:
    id->shove = fpta_shove_name(name, fpta_column);
    if (unlikely(!id->shove))
      return FPTA_ENAME;
    id->shove = fpta_column_shove(id->shove, fptu_null, fpta_index_none);
    if (unlikely(!id->shove))
      return FPTA_ENAME;
    id->column.num = ~0u;
    id->column.table = id;
    assert(fpta_id_validate(id, fpta_column) == FPTA_SUCCESS);
    break;
  }

  // id->version = 0; /* done by memset() */
  return FPTA_SUCCESS;
}

int fpta_table_init(fpta_name *table_id, const char *name) {
  return fpta_name_init(table_id, name, fpta_table);
}

int fpta_column_init(const fpta_name *table_id, fpta_name *column_id,
                     const char *name) {
  int rc = fpta_id_validate(table_id, fpta_table);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  rc = fpta_name_init(column_id, name, fpta_column);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  column_id->column.table = const_cast<fpta_name *>(table_id);
  return FPTA_SUCCESS;
}

void fpta_name_destroy(fpta_name *id) {
  if (fpta_id_validate(id, fpta_table) == FPTA_SUCCESS)
    fpta_schema_free(id->table_schema);
  memset(id, 0, sizeof(fpta_name));
}

int fpta_name_refresh(fpta_txn *txn, fpta_name *name_id) {
  if (unlikely(name_id == nullptr))
    return FPTA_EINVAL;

  const bool is_table =
      fpta_shove2index(name_id->shove) == (fpta_index_type)fpta_flag_table;

  return fpta_name_refresh_couple(txn,
                                  is_table ? name_id : name_id->column.table,
                                  is_table ? nullptr : name_id);
}

int fpta_name_refresh_couple(fpta_txn *txn, fpta_name *table_id,
                             fpta_name *column_id) {
  int rc = fpta_id_validate(table_id, fpta_table);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;
  if (column_id) {
    rc = fpta_id_validate(column_id, fpta_column);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
  }
  rc = fpta_txn_validate(txn, fpta_read);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (unlikely(table_id->version != txn->schema_csn())) {
    if (table_id->version > txn->schema_csn())
      return FPTA_SCHEMA_CHANGED;

    rc = fpta_schema_read(txn, table_id->shove, &table_id->table_schema);
    if (unlikely(rc != FPTA_SUCCESS)) {
      if (rc != MDBX_NOTFOUND)
        return rc;
      fpta_schema_free(table_id->table_schema);
      table_id->table_schema = nullptr;
    }

    rc = fpta_dbicache_cleanup(txn, table_id->table_schema, false);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;

    assert(table_id->table_schema == nullptr ||
           txn->schema_csn() >= table_id->table_schema->version_csn());
    table_id->version = txn->schema_csn();
  }

  if (unlikely(table_id->table_schema == nullptr))
    return MDBX_NOTFOUND;

  fpta_table_schema *schema = table_id->table_schema;
  if (unlikely(schema->signature() != FTPA_SCHEMA_SIGNATURE))
    return FPTA_SCHEMA_CORRUPTED;

  assert(fpta_shove2index(table_id->shove) == (fpta_index_type)fpta_flag_table);
  if (unlikely(schema->table_shove() != table_id->shove))
    return FPTA_SCHEMA_CORRUPTED;

  assert(table_id->version >= schema->version_csn());
  if (column_id == nullptr)
    return FPTA_SUCCESS;

  assert(fpta_shove2index(column_id->shove) !=
         (fpta_index_type)fpta_flag_table);

  if (unlikely(column_id->column.table != table_id)) {
    if (column_id->column.table != column_id)
      return FPTA_EINVAL;
    column_id->column.table = table_id;
  }

  if (unlikely(column_id->version > table_id->version))
    return FPTA_SCHEMA_CHANGED;

  if (column_id->version != table_id->version) {
    column_id->column.num = ~0u;
    for (size_t i = 0; i < schema->column_count(); ++i) {
      if (fpta_shove_eq(column_id->shove, schema->column_shove(i))) {
        column_id->shove = schema->column_shove(i);
        column_id->column.num = (unsigned)i;
        break;
      }
    }
    column_id->version = table_id->version;
  }

  if (unlikely(column_id->column.num > fpta_max_cols))
    return FPTA_ENOENT;
  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_table_create(fpta_txn *txn, const char *table_name,
                      fpta_column_set *column_set) {
  int rc = fpta_txn_validate(txn, fpta_schema);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;
  const fpta_shove_t table_shove = fpta_shove_name(table_name, fpta_table);
  if (unlikely(!table_shove))
    return FPTA_ENAME;

  const void *composites_eof = nullptr;
  rc = fpta_columns_description_validate(
      column_set->shoves, column_set->count, column_set->composites,
      FPT_ARRAY_END(column_set->composites), &composites_eof);
  if (rc != FPTA_SUCCESS)
    return rc;

  if ((txn->db->regime_flags & fpta_allow_clumsy) == 0) {
    if (!fpta_is_ordinary(column_set->shoves[0])) {
      unsigned clumsy_count = 0;
      for (size_t i = 1; i < column_set->count; ++i) {
        const auto shove = column_set->shoves[i];
        if (!fpta_is_indexed(shove))
          break;
        if (fpta_is_ordinary(shove) && !fpta_column_is_nullable(shove))
          /* primary index costly than secondary */
          return FPTA_CLUMSY_INDEX;

        if (++clumsy_count > 1)
          /* too costly, ordinary PK should be used */
          return FPTA_CLUMSY_INDEX;
      }
    }
  }

  const size_t bytes = fpta_schema_stored_size(column_set, composites_eof);
  rc = fpta_column_set_sort(column_set);
  if (rc != FPTA_SUCCESS)
    return rc;

  fpta_db *db = txn->db;
  if (db->schema_dbi < 1) {
    rc = fpta_schema_open(txn, true);
    if (rc != MDBX_SUCCESS)
      return rc;
  }

  MDBX_dbi dbi[fpta_max_indexes];
  memset(dbi, 0, sizeof(dbi));

  for (size_t i = 0; i < column_set->count; ++i) {
    const auto shove = column_set->shoves[i];
    if (!fpta_is_indexed(shove))
      break;
    assert(i < fpta_max_indexes);

    const unsigned dbi_flags = fpta_dbi_flags(column_set->shoves, i);
    const fpta_shove_t data_shove = fpta_data_shove(column_set->shoves, i);
    int err = fpta_dbi_open(txn, fpta_dbi_shove(table_shove, i), dbi[i],
                            dbi_flags, shove, data_shove);
    if (err != MDBX_NOTFOUND)
      return (err == MDBX_SUCCESS) ? (int)FPTA_EEXIST : err;
  }

#ifndef NDEBUG
  std::string dict_string;
#endif
  trivial_dict dict;
  MDBX_val key, data;
  key.iov_len = sizeof(dict_key);
  key.iov_base = (void *)&dict_key;
  rc = mdbx_get(txn->mdbx_txn, db->schema_dbi, &key, &data);
  if (rc == MDBX_SUCCESS) {
    if (!dict.fetch(data))
      return FPTA_SCHEMA_CORRUPTED;
#ifndef NDEBUG
    dict_string = fptu::string_view((const char *)data.iov_base, data.iov_len);
#endif
  } else if (rc != MDBX_NOTFOUND)
    return rc;

  if (dict.merge(fptu::string_view((const char *)column_set->dict_ptr),
                 fptu::string_view(table_name))) {
#ifdef NDEBUG
    const std::string
#endif
        dict_string = dict.string();
    assert(key.iov_len == sizeof(dict_key) &&
           key.iov_base == (void *)&dict_key);
    data.iov_base = (void *)dict_string.data();
    data.iov_len = dict_string.length();
    rc = mdbx_put(txn->mdbx_txn, db->schema_dbi, &key, &data, MDBX_NODUPDATA);
    if (rc != MDBX_SUCCESS)
      return rc;
  }

  for (size_t i = 0; i < column_set->count; ++i) {
    const auto shove = column_set->shoves[i];
    if (!fpta_is_indexed(shove))
      break;
    assert(i < fpta_max_indexes);

    const unsigned dbi_flags =
        MDBX_CREATE | fpta_dbi_flags(column_set->shoves, i);
    const fpta_shove_t data_shove = fpta_data_shove(column_set->shoves, i);
    rc = fpta_dbi_open(txn, fpta_dbi_shove(table_shove, i), dbi[i], dbi_flags,
                       shove, data_shove);
    if (rc != MDBX_SUCCESS)
      goto bailout;
  }

  key.iov_len = sizeof(table_shove);
  key.iov_base = (void *)&table_shove;
  data.iov_base = nullptr;
  data.iov_len = bytes;
  rc = mdbx_put(txn->mdbx_txn, db->schema_dbi, &key, &data,
                MDBX_NOOVERWRITE | MDBX_RESERVE);
  if (rc == MDBX_SUCCESS) {
    fpta_table_stored_schema *const record =
        (fpta_table_stored_schema *)data.iov_base;
    record->signature = FTPA_SCHEMA_SIGNATURE;
    record->count = column_set->count;
    record->csn = txn->db_version;
    memcpy(record->columns, column_set->shoves,
           sizeof(fpta_shove_t) * record->count);

    fpta_table_schema::composite_item_t *ptr =
        (fpta_table_schema::composite_item_t *)&record->columns[record->count];
    const size_t composites_bytes =
        (uintptr_t)composites_eof - (uintptr_t)&column_set->composites[0];
    memcpy(ptr, column_set->composites, composites_bytes);
    assert((uint8_t *)ptr + composites_bytes == (uint8_t *)record + bytes);

    record->checksum =
        t1ha2_atonce(&record->signature, bytes - sizeof(record->checksum),
                     FTPA_SCHEMA_CHECKSEED);
#ifndef NDEBUG
    MDBX_val dict_data = {(void *)dict_string.data(), dict_string.length()};
    assert(fpta_schema_image_validate(table_shove, data, dict_data));
#endif

    rc = mdbx_dbi_sequence(txn->mdbx_txn, txn->db->schema_dbi, nullptr, 1);
    if (rc == MDBX_SUCCESS) {
      txn->schema_csn() = txn->db_version;
      return FPTA_SUCCESS;
    }
  }

bailout:
  return fpta_internal_abort(txn, rc);
}

int fpta_table_drop(fpta_txn *txn, const char *table_name) {
  int rc = fpta_txn_validate(txn, fpta_schema);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;
  const fpta_shove_t table_shove = fpta_shove_name(table_name, fpta_table);
  if (unlikely(!table_shove))
    return FPTA_ENAME;

  fpta_db *db = txn->db;
  if (db->schema_dbi < 1) {
    rc = fpta_schema_open(txn, false);
    if (rc != MDBX_SUCCESS)
      return rc;
  }

  MDBX_dbi dbi[fpta_max_indexes];
  memset(dbi, 0, sizeof(dbi));

  MDBX_val data, key;
  const fpta_table_stored_schema *table_schema = nullptr;

  MDBX_cursor *mdbx_cursor;
  rc = mdbx_cursor_open(txn->mdbx_txn, db->schema_dbi, &mdbx_cursor);
  if (rc != MDBX_SUCCESS)
    return rc;

  trivial_dict old_dict, new_dict;
  rc = mdbx_cursor_get(mdbx_cursor, &key, &data, MDBX_FIRST);
  while (rc == MDBX_SUCCESS) {
    if (key.iov_len != sizeof(fpta_shove_t)) {
      rc = FPTA_SCHEMA_CORRUPTED;
      break;
    }

    fpta_shove_t shove;
    memcpy(&shove, key.iov_base, sizeof(fpta_shove_t));
    if (shove == dict_key) {
      if (unlikely(!old_dict.fetch(data))) {
        rc = FPTA_SCHEMA_CORRUPTED;
        break;
      }
    } else {
      const fpta_table_stored_schema *schema =
          fpta_schema_image_validate(shove, data, old_dict);
      if (unlikely(!schema)) {
        rc = FPTA_SCHEMA_CORRUPTED;
        break;
      }

      if (shove == table_shove) {
        table_schema = schema;
        rc = mdbx_is_dirty(txn->mdbx_txn, schema);
        if (unlikely(rc == MDBX_RESULT_TRUE)) {
          assert(table_schema == data.iov_base);
          table_schema = (const fpta_table_stored_schema *)memcpy(
              alloca(data.iov_len), table_schema, data.iov_len);
        } else {
          assert(rc == MDBX_RESULT_FALSE);
        }
      } else {
        for (size_t i = 0; i < schema->count; ++i)
          new_dict.pickup(old_dict, schema->columns[i]);
      }
    }
    rc = mdbx_cursor_get(mdbx_cursor, &key, &data, MDBX_NEXT);
  }

  mdbx_cursor_close(mdbx_cursor);
  if (unlikely(rc != MDBX_NOTFOUND || !table_schema))
    return rc;

  for (size_t i = 0; i < table_schema->count; ++i) {
    const auto shove = table_schema->columns[i];
    if (!fpta_is_indexed(shove))
      break;
    assert(i < fpta_max_indexes);

    const unsigned dbi_flags = fpta_dbi_flags(table_schema->columns, i);
    const fpta_shove_t data_shove = fpta_data_shove(table_schema->columns, i);
    rc = fpta_dbi_open(txn, fpta_dbi_shove(table_shove, i), dbi[i], dbi_flags,
                       shove, data_shove);
    if (unlikely(rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND))
      return rc;
  }

  // обновляем словарь схемы
  const std::string new_dict_string = new_dict.string();
  if (new_dict_string != old_dict.string()) {
    key.iov_len = sizeof(dict_key);
    key.iov_base = (void *)&dict_key;
    data.iov_len = new_dict_string.length();
    data.iov_base = (void *)new_dict_string.data();
    rc = mdbx_put(txn->mdbx_txn, db->schema_dbi, &key, &data, MDBX_NODUPDATA);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
  }

  // удаляем из схемы описание таблицы
  key.iov_len = sizeof(table_shove);
  key.iov_base = (void *)&table_shove;
  rc = mdbx_del(txn->mdbx_txn, db->schema_dbi, &key, nullptr);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  // удаляем все связаныне таблицы, включая вторичные индексы
  for (size_t i = 0; i < table_schema->count; ++i) {
    if (dbi[i] > 0) {
      fpta_dbicache_remove(db, fpta_dbi_shove(table_shove, i));
      rc = mdbx_drop(txn->mdbx_txn, dbi[i], true);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }
  }

  // увеличиваем номер ревизии схемы
  rc = mdbx_dbi_sequence(txn->mdbx_txn, txn->db->schema_dbi, nullptr, 1);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;
  txn->schema_csn() = txn->db_version;
  return MDBX_SUCCESS;

bailout:
  return fpta_internal_abort(txn, rc);
}

//----------------------------------------------------------------------------

int fpta_table_column_count_ex(const fpta_name *table_id,
                               unsigned *total_columns,
                               unsigned *composite_count) {
  int rc = fpta_id_validate(table_id, fpta_table_with_schema);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  const fpta_table_schema *schema = table_id->table_schema;
  if (likely(total_columns))
    *total_columns = schema->column_count();
  if (composite_count) {
    unsigned count = 0;
    for (size_t i = 0; i < schema->column_count(); ++i) {
      const auto shove = schema->column_shove(i);
      assert(i < fpta_max_indexes);
      if (fpta_index_is_secondary(shove))
        break;
      if (fpta_is_composite(shove))
        ++count;
    }
    *composite_count = count;
  }

  return FPTA_SUCCESS;
}

int fpta_table_column_get(const fpta_name *table_id, unsigned column,
                          fpta_name *column_id) {
  if (unlikely(column_id == nullptr))
    return FPTA_EINVAL;
  memset(column_id, 0, sizeof(fpta_name));

  int rc = fpta_id_validate(table_id, fpta_table_with_schema);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  const fpta_table_schema *schema = table_id->table_schema;
  if (column >= schema->column_count())
    return FPTA_NODATA;
  column_id->column.table = const_cast<fpta_name *>(table_id);
  column_id->shove = schema->column_shove(column);
  column_id->column.num = column;
  column_id->version = table_id->version;

  assert(fpta_id_validate(column_id, fpta_column_with_schema) == FPTA_SUCCESS);
  return FPTA_SUCCESS;
}

int fpta_name_reset(fpta_name *name_id) {
  if (unlikely(name_id == nullptr))
    return FPTA_EINVAL;

  name_id->version = 0;
  return FPTA_SUCCESS;
}
