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

#include "../externals/libfptu/src/erthink/erthink_clz.h++"

/* Проверяет наличие в строке значений non-nullable колонок, которые
 * не индексированы, либо индексированы без ограничений уникальности.
 * Другими словами, это те колонки, которые должны иметь значения,
 * но не проверяются в fpta_check_secondary_uniqueness(). */
__hot int fpta_check_nonnullable(const fpta_table_schema *table_def,
                                 const fptu_ro &row) {
  assert(table_def->column_count() > 0);
  for (size_t i = 1; i < table_def->column_count(); ++i) {
    const auto shove = table_def->column_shove(i);
    const auto index = fpta_shove2index(shove);

    if (index & fpta_index_fnullable) {
      if (!fpta_is_indexed(index)) {
/* при сортировке колонок по типам/флажкам индексов не-индексируемые
 * nullable колонки идут последними, т.е. дальше проверять нечего */
#ifndef NDEBUG
        while (++i < table_def->column_count()) {
          const auto chk_shove = table_def->column_shove(i);
          const auto chk_index = fpta_shove2index(chk_shove);
          assert(!fpta_is_indexed(chk_index));
          assert(chk_index & fpta_index_fnullable);
        }
#endif
        break;
      }
      continue;
    }

    if (index & fpta_index_funique) {
      /* колонки с контролем уникальности
       * будут проверены в fpta_check_secondary_uniqueness() */
      assert(fpta_is_indexed(index) && fpta_index_is_secondary(index));
      continue;
    }

    const fptu_type type = fpta_shove2type(shove);
    if (type == /* composite */ fptu_null)
      continue;

    const fptu_field *field = fptu::lookup(row, (unsigned)i, type);
    if (unlikely(field == nullptr))
      return FPTA_COLUMN_MISSING;
  }
  return FPTA_SUCCESS;
}

__hot int fpta_check_secondary_uniq(fpta_txn *txn, fpta_table_schema *table_def,
                                    const fptu_ro &old_row,
                                    const fptu_ro &new_row,
                                    const unsigned stepover) {
  MDBX_dbi dbi[fpta_max_indexes + /* поправка на primary */ 1];
  int rc = fpta_open_secondaries(txn, table_def, dbi);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  for (size_t i = 1; i < table_def->column_count(); ++i) {
    const auto shove = table_def->column_shove(i);
    const auto index = fpta_shove2index(shove);
    if (!fpta_index_is_secondary(index))
      break;
    assert(i < fpta_max_indexes + /* поправка на primary */ 1);
    if (i == stepover || !fpta_index_is_unique(index))
      continue;

    fpta_key new_se_key;
    rc = fpta_index_row2key(table_def, i, new_row, new_se_key, false);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    if (old_row.sys.iov_base) {
      fpta_key old_se_key;
      rc = fpta_index_row2key(table_def, i, old_row, old_se_key, false);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      if (fpta_is_same(old_se_key.mdbx, new_se_key.mdbx))
        continue;
    }

    MDBX_val pk_exist;
    rc = mdbx_get(txn->mdbx_txn, dbi[i], &new_se_key.mdbx, &pk_exist);
    if (unlikely(rc != MDBX_NOTFOUND))
      return (rc == MDBX_SUCCESS) ? MDBX_KEYEXIST : rc;
  }

  return FPTA_SUCCESS;
}

int fpta_secondary_upsert(fpta_txn *txn, fpta_table_schema *table_def,
                          MDBX_val old_pk_key, const fptu_ro &old_row,
                          MDBX_val new_pk_key, const fptu_ro &new_row,
                          const unsigned stepover) {
  MDBX_dbi dbi[fpta_max_indexes + /* поправка на primary */ 1];
  int rc = fpta_open_secondaries(txn, table_def, dbi);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  for (size_t i = 1; i < table_def->column_count(); ++i) {
    const auto shove = table_def->column_shove(i);
    const auto index = fpta_shove2index(shove);
    if (!fpta_index_is_secondary(index))
      break;
    assert(i < fpta_max_indexes + /* поправка на primary */ 1);
    if (i == stepover)
      continue;

    fpta_key new_se_key;
    rc = fpta_index_row2key(table_def, i, new_row, new_se_key, false);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    if (old_row.sys.iov_base == nullptr) {
      /* Старой версии нет, выполняется добавление новой строки */
      assert(old_pk_key.iov_base == new_pk_key.iov_base);
      /* Вставляем новую пару в secondary индекс */
      rc = mdbx_put(txn->mdbx_txn, dbi[i], &new_se_key.mdbx, &new_pk_key,
                    fpta_index_is_unique(index)
                        ? MDBX_NODUPDATA | MDBX_NOOVERWRITE
                        : MDBX_NODUPDATA);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;

      continue;
    }
    /* else: Выполняется обновление существующей строки */

    fpta_key old_se_key;
    rc = fpta_index_row2key(table_def, i, old_row, old_se_key, false);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    if (!fpta_is_same(old_se_key.mdbx, new_se_key.mdbx)) {
      /* Изменилось значение индексированного поля, выполняем удаление
       * из индекса пары со старым значением и добавляем пару с новым. */
      rc = mdbx_del(txn->mdbx_txn, dbi[i], &old_se_key.mdbx, &old_pk_key);
      if (unlikely(rc != MDBX_SUCCESS))
        return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
      rc = mdbx_put(txn->mdbx_txn, dbi[i], &new_se_key.mdbx, &new_pk_key,
                    fpta_index_is_unique(index)
                        ? MDBX_NODUPDATA | MDBX_NOOVERWRITE
                        : MDBX_NODUPDATA);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      continue;
    }

    if (old_pk_key.iov_base == new_pk_key.iov_base ||
        fpta_is_same(old_pk_key, new_pk_key))
      continue;

    /* Изменился PK, необходимо обновить пару<SE_value, PK_value> во вторичном
     * индексе. Комбинация MDBX_CURRENT | MDBX_NOOVERWRITE для таблиц с
     * MDBX_DUPSORT включает в mdbx_replace() режим обновления конкретного
     * значения из multivalue. Таким образом, мы меняем ссылку именно со
     * старого значения PK на новое, даже если для индексируемого поля
     * разрешены не уникальные значения. */
    MDBX_val old_pk_key_clone = old_pk_key;
    rc = mdbx_replace(txn->mdbx_txn, dbi[i], &new_se_key.mdbx, &new_pk_key,
                      &old_pk_key_clone,
                      fpta_index_is_unique(index)
                          ? MDBX_CURRENT | MDBX_NODUPDATA
                          : MDBX_CURRENT | MDBX_NODUPDATA | MDBX_NOOVERWRITE);
    if (unlikely(rc != MDBX_SUCCESS))
      return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
  }

  return FPTA_SUCCESS;
}

int fpta_secondary_remove(fpta_txn *txn, fpta_table_schema *table_def,
                          MDBX_val &pk_key, const fptu_ro &row,
                          const unsigned stepover) {
  MDBX_dbi dbi[fpta_max_indexes + /* поправка на primary */ 1];
  int rc = fpta_open_secondaries(txn, table_def, dbi);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  for (size_t i = 1; i < table_def->column_count(); ++i) {
    const auto shove = table_def->column_shove(i);
    const auto index = fpta_shove2index(shove);
    if (!fpta_index_is_secondary(index))
      break;
    assert(i < fpta_max_indexes + /* поправка на primary */ 1);
    if (i == stepover)
      continue;

    fpta_key se_key;
    rc = fpta_index_row2key(table_def, i, row, se_key, false);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    rc = mdbx_del(txn->mdbx_txn, dbi[i], &se_key.mdbx, &pk_key);
    if (unlikely(rc != MDBX_SUCCESS))
      return (rc != MDBX_NOTFOUND) ? rc : (int)FPTA_INDEX_CORRUPTED;
  }

  return FPTA_SUCCESS;
}

//----------------------------------------------------------------------------

int fpta_table_info(fpta_txn *txn, fpta_name *table_id, size_t *row_count,
                    fpta_table_stat *stat) {
  return fpta_table_info_ex(txn, table_id, row_count, stat,
                            offsetof(fpta_table_stat, index_costs));
}

static inline __pure_function unsigned log2_dot8(const size_t value) {
  const unsigned w = 8 * sizeof(value);
  const unsigned z = erthink::clz(value + 1) + 1;
  const unsigned f = (value << z) >> (w - 8);
  const unsigned c = f * (255 - f) * 43;
  return ((w - z) << 8) + f + (c >> 15);
}

static void index_stat2cost(const MDBX_stat &stat,
                            fpta_table_stat::index_cost_info *r) {
  r->btree_depth = stat.ms_depth;
  r->items = size_t(stat.ms_entries);
  r->branch_pages = size_t(stat.ms_branch_pages);
  r->leaf_pages = size_t(stat.ms_leaf_pages);
  r->large_pages = size_t(stat.ms_overflow_pages);
  r->bytes = (r->branch_pages + r->leaf_pages + r->large_pages) * stat.ms_psize;

  if (unlikely(r->leaf_pages < 3)) {
    r->scan_O1N = r->items ? 42 + (log2_dot8(r->items) >> 5) : 0;
    r->search_OlogN = r->scan_O1N * (r->leaf_pages * 2 + 7) / 3;
  } else {
    /* Исходим из того, что для сканирования всех записей потребуется обработать
     * все страницы, а также немного повозиться с каждой записью. */
    r->scan_O1N = 42 + r->bytes / (r->items + 1);

    /* Поиск одной записи по значению ключа потребует бинарного поиска в одной
     * странице на каждом уровне дерева. Исходим из того, что стоимость
     * обработки/сравнения одного ключа при бинарном поиске ПРИМЕРНО
     * соответствует стоимости обработки одной записи при переборе. Тогда
     * затраты можно оценить через высоту дерева и плотность укладки ключей
     * и/или значений в страницы в зависимости от их типов. */
    /* Однако, высоту b-дерева нельзя использовать, кроме как в эвристиках.
     * Иначе возвращаемая оценка становится слишком ступенчатой.
     * Поэтому используем логарифм от количества branch-страниц и сверху
     * подпираем его ограничителем упирающимся в высоту дерева. */

    /* среднее кол-во элементов на одной branch-странице */
    const auto epb =
        (r->branch_pages + r->leaf_pages - 1) / (r->branch_pages + 1);
    const auto l2epb = log2_dot8(epb);
    /* средняя высота дерева branch-страниц */
    auto height = (log2_dot8(r->leaf_pages) << 8) / l2epb;

    /* подпираем сверху реальной высотой деревва (актуально для не-уникальных
     * индексов со значительным количеством дубликатов */
    const auto limit = (r->btree_depth - 1) << 8;
    if (height > limit) {
      r->scan_O1N = (r->scan_O1N + r->scan_O1N * height / limit) / 2;
      height = limit + (height - limit) / 16;
    }

    const auto branch_factor =
        log2_dot8(r->leaf_pages / (r->branch_pages + 1) + r->branch_pages);
    const auto leaf_factor = log2_dot8(r->items / (r->leaf_pages + 1));
    const auto complexity = leaf_factor + ((branch_factor * height) >> 8);
    r->search_OlogN = (complexity * r->scan_O1N) >> 8;
  }

  r->clumsy_factor = r->btree_depth * r->scan_O1N;
}

static void index_add_cost(const fpta_shove_t shove, fpta_table_stat *info,
                           const size_t space4costs, const MDBX_stat &stat) {
  fpta_table_stat::index_cost_info stub, *r = &stub;
  info->index_costs_total += 1;
  if (space4costs >= info->index_costs_total) {
    info->index_costs_provided = info->index_costs_total;
    r = &info->index_costs[info->index_costs_provided - 1];
    r->column_shove = shove;
  }
  index_stat2cost(stat, r);

  info->total_items += r->items;
  info->btree_depth = std::max(info->btree_depth, r->btree_depth);
  info->leaf_pages += r->leaf_pages;
  info->branch_pages += r->branch_pages;
  info->large_pages += r->large_pages;
  info->total_bytes += r->bytes;

  /* При обновлении записи потребуется обновлять дерево отдельно для каждого
   * индекса. Кроме этого, потребуется выделение и копирование страниц,
   * перебалансировка деревьев, а также обслуживание списков страниц. */
  info->cost_alter_MOlogN += r->search_OlogN * 7 + r->scan_O1N * 15;

  /* Аналогично оцениваем амортизационные затраты на поиск при проверке
   * уникальности для всех вторичных индексов с контролем уникальности. */
  if (fpta_index_is_secondary(shove) && fpta_index_is_unique(shove))
    info->cost_uniq_MOlogN += r->search_OlogN;
}

int fpta_table_info_ex(fpta_txn *txn, fpta_name *table_id, size_t *row_count,
                       fpta_table_stat *info, size_t space4stat) {
  if (info != nullptr &&
      unlikely(space4stat < offsetof(fpta_table_stat, index_costs)))
    return FPTA_EINVAL;

  int rc = fpta_name_refresh_couple(txn, table_id, nullptr);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDBX_dbi handle;
  rc = fpta_open_table(txn, table_id->table_schema, handle);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDBX_stat mdbx_stat;
  rc = mdbx_dbi_stat(txn->mdbx_txn, handle, &mdbx_stat, sizeof(mdbx_stat));
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (unlikely(info)) {
    const size_t space4costs =
        (space4stat - offsetof(fpta_table_stat, index_costs)) /
        sizeof(info->index_costs[0]);

    info->mod_txnid = mdbx_stat.ms_mod_txnid;
    info->row_count = size_t(mdbx_stat.ms_entries);
    info->total_items = 0;
    info->total_bytes = 0;
    info->btree_depth = 0;
    info->leaf_pages = 0;
    info->branch_pages = 0;
    info->large_pages = 0;

    info->index_costs_total = 0;
    info->index_costs_provided = 0;
    info->cost_alter_MOlogN = 0;
    info->cost_uniq_MOlogN = 0;
    index_add_cost(table_id->table_schema->column_shove(0), info, space4costs,
                   mdbx_stat);

    if (table_id->table_schema->has_secondary()) {
      MDBX_dbi dbi[fpta_max_indexes + /* поправка на primary */ 1];
      rc = fpta_open_secondaries(txn, table_id->table_schema, dbi);
      if (unlikely(rc != FPTA_SUCCESS))
        return rc;
      for (unsigned i = 1; i < table_id->table_schema->column_count(); ++i) {
        const auto shove = table_id->table_schema->column_shove(i);
        if (!fpta_is_indexed(shove))
          break;

        rc =
            mdbx_dbi_stat(txn->mdbx_txn, dbi[i], &mdbx_stat, sizeof(mdbx_stat));
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;

        index_add_cost(shove, info, space4costs, mdbx_stat);
      }
    }
  }

  if (likely(row_count)) {
    if (unlikely(mdbx_stat.ms_entries > SIZE_MAX)) {
      *row_count = size_t(FPTA_DEADBEEF);
      return FPTA_EVALUE;
    }
    *row_count = size_t(mdbx_stat.ms_entries);
  }

  return FPTA_SUCCESS;
}

int fpta_table_sequence(fpta_txn *txn, fpta_name *table_id, uint64_t *result,
                        uint64_t increment) {
  int rc = fpta_name_refresh_couple(txn, table_id, nullptr);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDBX_dbi handle;
  rc = fpta_open_table(txn, table_id->table_schema, handle);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  rc = mdbx_dbi_sequence(txn->mdbx_txn, handle, result, increment);
  static_assert(FPTA_NODATA == int(MDBX_RESULT_TRUE), "expect equal");
  return rc;
}

int fpta_table_clear(fpta_txn *txn, fpta_name *table_id, bool reset_sequence) {
  int rc = fpta_name_refresh_couple(txn, table_id, nullptr);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  fpta_table_schema *table_def = table_id->table_schema;
  MDBX_dbi handle;
  rc = fpta_open_table(txn, table_def, handle);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  MDBX_dbi dbi[fpta_max_indexes + /* поправка на primary */ 1];
  if (table_def->has_secondary()) {
    rc = fpta_open_secondaries(txn, table_def, dbi);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
  }

  uint64_t sequence = 0;
  if (!reset_sequence) {
    rc = mdbx_dbi_sequence(txn->mdbx_txn, handle, &sequence, 0);
    if (unlikely(rc != FPTA_SUCCESS))
      return rc;
  }

  rc = mdbx_drop(txn->mdbx_txn, handle, false);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (table_def->has_secondary()) {
    for (size_t i = 1; i < table_def->column_count(); ++i) {
      const fpta_shove_t shove = table_def->column_shove(i);
      if (!fpta_is_indexed(shove))
        break;
      rc = mdbx_drop(txn->mdbx_txn, dbi[i], false);
      if (unlikely(rc != MDBX_SUCCESS))
        return fpta_internal_abort(txn, rc);
    }
  }

  if (sequence) {
    rc = mdbx_dbi_sequence(txn->mdbx_txn, handle, nullptr, sequence);
    if (unlikely(rc != FPTA_SUCCESS))
      return fpta_internal_abort(txn, rc);
  }

  return FPTA_SUCCESS;
}
