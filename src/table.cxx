/*
 * Copyright 2016-2019 libfpta authors: please see AUTHORS file.
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
  MDBX_dbi dbi[fpta_max_indexes];
  int rc = fpta_open_secondaries(txn, table_def, dbi);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  for (size_t i = 1; i < table_def->column_count(); ++i) {
    const auto shove = table_def->column_shove(i);
    const auto index = fpta_shove2index(shove);
    assert(i < fpta_max_indexes);
    if (!fpta_index_is_secondary(index))
      break;
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
  MDBX_dbi dbi[fpta_max_indexes];
  int rc = fpta_open_secondaries(txn, table_def, dbi);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  for (size_t i = 1; i < table_def->column_count(); ++i) {
    const auto shove = table_def->column_shove(i);
    const auto index = fpta_shove2index(shove);
    assert(i < fpta_max_indexes);
    if (!fpta_index_is_secondary(index))
      break;
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
  MDBX_dbi dbi[fpta_max_indexes];
  int rc = fpta_open_secondaries(txn, table_def, dbi);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  for (size_t i = 1; i < table_def->column_count(); ++i) {
    const auto shove = table_def->column_shove(i);
    const auto index = fpta_shove2index(shove);
    assert(i < fpta_max_indexes);
    if (!fpta_index_is_secondary(index))
      break;
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

static void index_stat2cost(MDBX_stat &stat,
                            fpta_table_stat::index_cost_info &info) {
  info.btree_depth = stat.ms_depth;
  info.items = (size_t)stat.ms_entries;
  info.branch_pages = (size_t)stat.ms_branch_pages;
  info.leaf_pages = (size_t)stat.ms_leaf_pages;
  info.large_pages = (size_t)stat.ms_overflow_pages;
  info.bytes =
      (info.branch_pages + info.leaf_pages + info.large_pages) * stat.ms_psize;

  info.scan_O1N = unsigned((8 * info.bytes + info.items) / (info.items + 1));
  const size_t records_per_leaf =
      42 + (info.items + info.leaf_pages) / (info.leaf_pages + 1);
  const size_t records_per_branch =
      (info.items + info.branch_pages) / (info.branch_pages + 1);
  info.search_OlogN = unsigned(
      42 + (records_per_leaf + records_per_branch * info.btree_depth + 1) *
               info.scan_O1N);
  info.clumsy_factor =
      unsigned((info.btree_depth * info.bytes + info.items) / (info.items + 1));
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
    stat->total_items = stat->row_count = (size_t)mdbx_stat.ms_entries;
    stat->btree_depth = mdbx_stat.ms_depth;
    stat->leaf_pages = (size_t)mdbx_stat.ms_leaf_pages;
    stat->branch_pages = (size_t)mdbx_stat.ms_branch_pages;
    stat->large_pages = (size_t)mdbx_stat.ms_overflow_pages;

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
    size_t uniq_trees = 0;
    size_t uniq_summary_depth = 0;

    size_t summary_depth = stat->btree_depth;
    size_t number_of_trees = 1;
    if (table_id->table_schema->has_secondary()) {
      MDBX_dbi dbi[fpta_max_indexes];
      rc = fpta_open_secondaries(txn, table_id->table_schema, dbi);
      if (unlikely(rc != FPTA_SUCCESS))
        return rc;
      for (size_t i = 1; i < table_id->table_schema->column_count(); ++i) {
        const auto shove = table_id->table_schema->column_shove(i);
        if (!fpta_is_indexed(shove))
          break;

        rc =
            mdbx_dbi_stat(txn->mdbx_txn, dbi[i], &mdbx_stat, sizeof(mdbx_stat));
        if (unlikely(rc != MDBX_SUCCESS))
          return rc;

        if (fpta_index_is_unique(shove)) {
          uniq_total_items += (size_t)mdbx_stat.ms_entries;
          uniq_trees += 1;
          uniq_summary_depth += mdbx_stat.ms_depth + 1;
          uniq_leaf_pages += (size_t)mdbx_stat.ms_leaf_pages;
          uniq_branch_pages += (size_t)mdbx_stat.ms_branch_pages;
          uniq_large_pages += (size_t)mdbx_stat.ms_overflow_pages;
        }

        stat->total_items += (size_t)mdbx_stat.ms_entries;
        number_of_trees += 1;
        summary_depth += mdbx_stat.ms_depth + 1;
        stat->btree_depth = (stat->btree_depth > mdbx_stat.ms_depth)
                                ? stat->btree_depth
                                : mdbx_stat.ms_depth;
        stat->leaf_pages += (size_t)mdbx_stat.ms_leaf_pages;
        stat->branch_pages += (size_t)mdbx_stat.ms_branch_pages;
        stat->large_pages += (size_t)mdbx_stat.ms_overflow_pages;

        stat->index_costs_total = unsigned(i + 1);
        if (space4costs >= stat->index_costs_total) {
          stat->index_costs_provided = unsigned(i + 1);
          index_stat2cost(mdbx_stat, stat->index_costs[i]);
          stat->index_costs[i].column_shove =
              table_id->table_schema->column_shove(i);
        }
      }
    }

    stat->total_bytes =
        (stat->leaf_pages + stat->branch_pages + stat->large_pages) *
        mdbx_stat.ms_psize;

    /* LY: Исходим из того, что для сканирования всех записей потребуется
     * прочитать и обработать все страницы */
    stat->cost_scan_O1N = unsigned(
        42 + (8 * stat->total_bytes + stat->row_count) / (stat->row_count + 1));

    /* LY: Поиск одной записи по значению ключа потребует бинарного поиска в
     * одной странице на каждом уровне дерева. Исходим из того, что стоимость
     * обработки/сравнения одного ключа при бинарном поиске, примерно
     * соответствует стоимости обработки одной записи при переборе. Тогда
     * затраты можно оценить через высоту дерева и плотность укладки ключей
     * и/или значений в страницы в зависимости от их типов. */
    const size_t records_per_leaf =
        (stat->row_count + stat->leaf_pages) / (stat->leaf_pages + 1);
    const size_t records_per_branch =
        (stat->row_count + stat->branch_pages) / (stat->branch_pages + 1);
    stat->cost_search_OlogN = unsigned(
        42 + (records_per_leaf + records_per_branch * stat->btree_depth + 1) *
                 stat->cost_scan_O1N);

    /* LY: При обновлении записи потребуется обновлять дерево отдельно для
     * каждого индекса, что проще оценить относительно поиска, через количество
     * деревьев и сумму их высот. Кроме этого, потребуется выделение и
     * копирование страниц, перебалансировка деревьев, а также обслуживание
     * списков страниц. */
    stat->cost_alter_MOlogN =
        unsigned(42 + (records_per_leaf * number_of_trees +
                       records_per_branch * summary_depth + 1) *
                          stat->cost_scan_O1N * 3);

    /* LY: Аналогично оцениваем амортизационные затраты на поиск при проверке
     * уникальности для всех вторичных индексов с контролем уникальности. */
    const size_t uniq_total_bytes =
        (uniq_leaf_pages + uniq_branch_pages + uniq_large_pages) *
        mdbx_stat.ms_psize;
    const size_t uniq_per_leaf =
        (uniq_total_items + uniq_leaf_pages) / (uniq_leaf_pages + 1);
    const size_t uniq_per_branch =
        (uniq_total_items + uniq_branch_pages) / (uniq_branch_pages + 1);

    const size_t uniq_O1N =
        (8 * uniq_total_bytes + uniq_total_items) / (uniq_total_items + 1);
    stat->cost_uniq_MOlogN =
        unsigned((uniq_per_leaf * uniq_trees +
                  uniq_per_branch * uniq_summary_depth + 1) *
                 uniq_O1N);
  }

  if (likely(row_count)) {
    if (unlikely(mdbx_stat.ms_entries > SIZE_MAX)) {
      *row_count = (size_t)FPTA_DEADBEEF;
      return FPTA_EVALUE;
    }
    *row_count = (size_t)mdbx_stat.ms_entries;
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
  static_assert(FPTA_NODATA == MDBX_RESULT_TRUE, "expect equal");
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

  MDBX_dbi dbi[fpta_max_indexes];
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

  rc = mdbx_drop(txn->mdbx_txn, handle, 0);
  if (unlikely(rc != FPTA_SUCCESS))
    return rc;

  if (table_def->has_secondary()) {
    for (size_t i = 1; i < table_def->column_count(); ++i) {
      const fpta_shove_t shove = table_def->column_shove(i);
      if (!fpta_is_indexed(shove))
        break;
      rc = mdbx_drop(txn->mdbx_txn, dbi[i], 0);
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
