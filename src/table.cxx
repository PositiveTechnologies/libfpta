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

static inline __pure_function unsigned ilog2(size_t value) {
  return 8 * sizeof(value) - erthink::clz(value + 1);
}

static inline __pure_function size_t index_bytes(size_t branch_pages,
                                                 size_t leaf_pages,
                                                 size_t overflow_pages,
                                                 size_t page_size) {
  return (branch_pages + leaf_pages + overflow_pages) * page_size;
}

static inline __pure_function unsigned leaf_factor(size_t entries,
                                                   size_t leaf_pages) {
  return ilog2((entries + leaf_pages) / (leaf_pages + 1));
}

static inline __pure_function unsigned branch_factor(size_t leaf_pages,
                                                     size_t branch_pages) {
  return ilog2(leaf_pages / (branch_pages + 1) + branch_pages);
}

static inline __pure_function unsigned scan_cost(size_t bytes, size_t items) {
  /* Исходим из того, что для сканирования всех записей потребуется прочитать
   * и обработать все страницы, а также немного повозиться с каждой записью. */
  return unsigned(42 + bytes / (items + 1));
}

static inline __pure_function unsigned search_cost(unsigned leaf_factor,
                                                   unsigned branch_factor,
                                                   unsigned branch_height,
                                                   unsigned scan_cost) {
  /* Поиск одной записи по значению ключа потребует бинарного поиска в одной
   * странице на каждом уровне дерева. Исходим из того, что стоимость
   * обработки/сравнения одного ключа при бинарном поиске ПРИМЕРНО соответствует
   * стоимости обработки одной записи при переборе. Тогда затраты можно оценить
   * через высоту дерева и плотность укладки ключей и/или значений в страницы в
   * зависимости от их типов. */
  return (leaf_factor + branch_factor * branch_height) * scan_cost;
}

static void index_stat2cost(const MDBX_stat &stat,
                            fpta_table_stat::index_cost_info &r) {
  r.btree_depth = stat.ms_depth;
  r.items = size_t(stat.ms_entries);
  r.branch_pages = size_t(stat.ms_branch_pages);
  r.leaf_pages = size_t(stat.ms_leaf_pages);
  r.large_pages = size_t(stat.ms_overflow_pages);

  r.bytes =
      index_bytes(r.branch_pages, r.leaf_pages, r.large_pages, stat.ms_psize);
  r.scan_O1N = scan_cost(r.bytes, r.items);
  r.search_OlogN = search_cost(leaf_factor(r.items, r.leaf_pages),
                               branch_factor(r.leaf_pages, r.branch_pages),
                               r.btree_depth - 1, r.scan_O1N);
  r.clumsy_factor = unsigned(r.btree_depth * r.bytes / (r.items + 1));
}

int fpta_table_info_ex(fpta_txn *txn, fpta_name *table_id, size_t *row_count,
                       fpta_table_stat *stat, size_t space4stat) {
  if (stat != nullptr &&
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

  if (unlikely(stat)) {
    const size_t space4costs =
        (space4stat - offsetof(fpta_table_stat, index_costs)) /
        sizeof(stat->index_costs[0]);

    stat->mod_txnid = mdbx_stat.ms_mod_txnid;
    stat->total_items = stat->row_count = size_t(mdbx_stat.ms_entries);
    stat->btree_depth = mdbx_stat.ms_depth;
    stat->leaf_pages = size_t(mdbx_stat.ms_leaf_pages);
    stat->branch_pages = size_t(mdbx_stat.ms_branch_pages);
    stat->large_pages = size_t(mdbx_stat.ms_overflow_pages);

    stat->index_costs_total = 1;
    stat->index_costs_provided = 0;
    if (space4costs) {
      stat->index_costs_provided = 1;
      index_stat2cost(mdbx_stat, stat->index_costs[0]);
      stat->index_costs[0].column_shove =
          table_id->table_schema->column_shove(0);
    }

    size_t uniq_total_items = 0;
    size_t uniq_leaf_pages = 0;
    size_t uniq_branch_pages = 0;
    size_t uniq_large_pages = 0;
    unsigned uniq_trees = 0;
    unsigned uniq_branch_height = 0;

    unsigned overall_branch_height = stat->btree_depth - 1;
    unsigned overall_trees = 1;
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

        if (fpta_index_is_unique(shove)) {
          uniq_total_items += size_t(mdbx_stat.ms_entries);
          uniq_trees += 1;
          uniq_branch_height += mdbx_stat.ms_depth - 1;
          uniq_leaf_pages += size_t(mdbx_stat.ms_leaf_pages);
          uniq_branch_pages += size_t(mdbx_stat.ms_branch_pages);
          uniq_large_pages += size_t(mdbx_stat.ms_overflow_pages);
        }

        stat->total_items += size_t(mdbx_stat.ms_entries);
        overall_trees += 1;
        overall_branch_height += mdbx_stat.ms_depth - 1;
        stat->btree_depth = (stat->btree_depth > mdbx_stat.ms_depth)
                                ? stat->btree_depth
                                : mdbx_stat.ms_depth;
        stat->leaf_pages += size_t(mdbx_stat.ms_leaf_pages);
        stat->branch_pages += size_t(mdbx_stat.ms_branch_pages);
        stat->large_pages += size_t(mdbx_stat.ms_overflow_pages);

        stat->index_costs_total = i + 1;
        if (space4costs >= stat->index_costs_total) {
          stat->index_costs_provided = i + 1;
          index_stat2cost(mdbx_stat, stat->index_costs[i]);
          stat->index_costs[i].column_shove =
              table_id->table_schema->column_shove(i);
        }
      }
    }

    stat->total_bytes = index_bytes(stat->leaf_pages, stat->branch_pages,
                                    stat->large_pages, mdbx_stat.ms_psize);
    stat->cost_scan_O1N = scan_cost(stat->total_bytes, stat->row_count);

    const auto overall_leaf_factor =
        overall_trees * leaf_factor(stat->row_count, stat->leaf_pages);
    const auto overall_branch_factor =
        branch_factor(stat->leaf_pages, stat->branch_pages);
    stat->cost_search_OlogN =
        search_cost(overall_leaf_factor, overall_branch_factor,
                    stat->btree_depth - 1, stat->cost_scan_O1N);

    /* При обновлении записи потребуется обновлять дерево отдельно для каждого
     * индекса, что проще оценить относительно поиска, через количество деревьев
     * и сумму их высот. Кроме этого, потребуется выделение и копирование
     * страниц, перебалансировка деревьев, а также обслуживание
     * списков страниц (домножаем на 4). */
    stat->cost_alter_MOlogN =
        search_cost(overall_leaf_factor, overall_branch_factor,
                    overall_branch_height, stat->cost_scan_O1N << 2);

    /* LY: Аналогично оцениваем амортизационные затраты на поиск при проверке
     * уникальности для всех вторичных индексов с контролем уникальности. */
    const auto uniq_total_bytes =
        index_bytes(uniq_branch_pages, uniq_leaf_pages, uniq_large_pages,
                    mdbx_stat.ms_psize);
    const auto uniq_leaf_factor =
        uniq_trees * leaf_factor(uniq_total_items, uniq_leaf_pages);
    const auto uniq_branch_factor =
        branch_factor(uniq_leaf_pages, uniq_branch_pages);
    const auto uniq_scan_O1N = scan_cost(uniq_total_bytes, uniq_total_items);
    stat->cost_uniq_MOlogN = search_cost(uniq_leaf_factor, uniq_branch_factor,
                                         uniq_branch_height, uniq_scan_O1N);
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
