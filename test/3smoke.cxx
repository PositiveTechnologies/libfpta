﻿/*
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

#ifdef _MSC_VER
#define _USE_MATH_DEFINES
#endif

#include "fpta_test.h"
#include "tools.hpp"

static const char testdb_name[] = "ut_smoke.fpta";
static const char testdb_name_lck[] = "ut_smoke.fpta" MDBX_LOCK_SUFFIX;

TEST(SmokeIndex, Primary) {
  /* Smoke-проверка жизнеспособности первичных индексов.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей, в которой три колонки
   *     и один (primary) индекс.
   *  2. Добавляем данные:
   *     - добавляем "первую" запись, одновременно пытаясь
   *       добавить в строку-кортеж поля с "плохими" значениями.
   *     - добавляем "вторую" запись, которая отличается от первой
   *       всеми колонками.
   *     - также попутно пытаемся обновить несуществующие записи
   *       и вставить дубликаты.
   *  3. Читаем добавленное:
   *     - открываем курсор по основному индексу, без фильтра,
   *       на всю таблицу (весь диапазон строк),
   *       и проверяем кол-во записей и дубликатов.
   *     - переходим к последней, читаем и проверяем её (должна быть
   *       "вторая").
   *     - переходим к первой, читаем и проверяем её (должна быть "первая").
   *  4. Удаляем данные:
   *     - сначала "вторую" запись, потом "первую".
   *     - проверяем кол-во записей и дубликатов, eof для курсора.
   *  5. Завершаем операции и освобождаем ресурсы.
   */
  if (GTEST_IS_EXECUTION_TIMEOUT())
    return;
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  // открываем/создаем базульку в 1 мегабайт
  fpta_db *db = nullptr;
  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644, 1,
                         true, &db));
  ASSERT_NE(nullptr, db);

  // описываем простейшую таблицу с тремя колонками и одним PK
  fpta_column_set def;
  fpta_column_set_init(&def);

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("pk_str_uniq", fptu_cstr,
                                 fpta_primary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("a_uint", fptu_uint64, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("b_fp", fptu_fp64, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  // запускам транзакцию и создаем таблицу с обозначенным набором колонок
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_1", &def));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // разрушаем описание таблицы
  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

  // инициализируем идентификаторы таблицы и её колонок
  fpta_name table, col_pk, col_a, col_b;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table_1"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_pk, "pk_str_uniq"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_a, "a_uint"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_b, "b_fp"));

  // начинаем транзакцию для вставки данных
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);
  // ради теста делаем привязку вручную
  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_pk));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_a));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_b));

  // проверяем иформацию о таблице (сейчас таблица пуста)
  size_t row_count;
  fpta_table_stat stat;
  memset(&row_count, 42, sizeof(row_count));
  memset(&stat, 42, sizeof(stat));
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table, &row_count, &stat));
  EXPECT_EQ(0u, row_count);
  EXPECT_EQ(row_count, stat.row_count);
  EXPECT_EQ(0u, stat.btree_depth);
  EXPECT_EQ(0u, stat.large_pages);
  EXPECT_EQ(0u, stat.branch_pages);
  EXPECT_EQ(0u, stat.leaf_pages);
  EXPECT_EQ(0u, stat.total_bytes);

  // создаем кортеж, который станет первой записью в таблице
  fptu_rw *pt1 = fptu_alloc(3, 42);
  ASSERT_NE(nullptr, pt1);
  ASSERT_STREQ(nullptr, fptu_check(pt1));

  // ради проверки пытаемся сделать нехорошее (добавить поля с нарушениями)
  EXPECT_EQ(FPTA_ETYPE, fpta_upsert_column(pt1, &col_pk, fpta_value_uint(12)));
  EXPECT_EQ(FPTA_EVALUE, fpta_upsert_column(pt1, &col_a, fpta_value_sint(-34)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt1, &col_b, fpta_value_cstr("string")));

  // добавляем нормальные значения
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt1, &col_pk, fpta_value_cstr("pk-string")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_a, fpta_value_sint(34)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_b, fpta_value_float(56.78)));
  ASSERT_STREQ(nullptr, fptu_check(pt1));

  // создаем еще один кортеж для второй записи
  fptu_rw *pt2 = fptu_alloc(3, 42);
  ASSERT_NE(nullptr, pt2);
  ASSERT_STREQ(nullptr, fptu_check(pt2));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_pk, fpta_value_cstr("zzz")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_a, fpta_value_sint(90)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_b, fpta_value_float(12.34)));
  ASSERT_STREQ(nullptr, fptu_check(pt2));

  // пытаемся обновить несуществующую запись
  EXPECT_EQ(FPTA_NOTFOUND,
            fpta_update_row(txn, &table, fptu_take_noshrink(pt1)));
  // вставляем и обновляем
  EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt1)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_row(txn, &table, fptu_take_noshrink(pt1)));
  EXPECT_EQ(FPTA_OK, fpta_update_row(txn, &table, fptu_take_noshrink(pt1)));
  EXPECT_EQ(FPTA_KEYEXIST,
            fpta_insert_row(txn, &table, fptu_take_noshrink(pt1)));

  // аналогично со второй записью
  EXPECT_EQ(FPTA_NOTFOUND,
            fpta_update_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_OK, fpta_update_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_KEYEXIST,
            fpta_insert_row(txn, &table, fptu_take_noshrink(pt2)));

  // фиксируем изменения
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  // и начинаем следующую транзакцию
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);

  // открываем простейщий курсор: на всю таблицу, без фильтра
  fpta_cursor *cursor;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn, &col_pk, fpta_value_begin(), fpta_value_end(),
                             nullptr, fpta_unsorted_dont_fetch, &cursor));
  ASSERT_NE(nullptr, cursor);

  // узнам сколько записей за курсором (в таблице).
  size_t count;
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(2u, count);

  // снова проверяем иформацию о таблице (сейчас в таблице две строки)
  memset(&row_count, 42, sizeof(row_count));
  memset(&stat, 42, sizeof(stat));
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table, &row_count, &stat));
  EXPECT_EQ(2u, row_count);
  EXPECT_EQ(row_count, stat.row_count);
  EXPECT_EQ(1u, stat.btree_depth);
  EXPECT_EQ(0u, stat.large_pages);
  EXPECT_EQ(0u, stat.branch_pages);
  EXPECT_EQ(1u, stat.leaf_pages);
  EXPECT_LE(512u, stat.total_bytes);

  // переходим к последней записи
  EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
  // ради проверки убеждаемся что за курсором есть данные
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

  // считаем повторы, их не должно быть
  size_t dups;
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ(1u, dups);

  // получаем текущую строку, она должна совпадать со вторым кортежем
  fptu_ro row2;
  EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row2));
  ASSERT_STREQ(nullptr, fptu_check_ro(row2));
  EXPECT_EQ(fptu_eq, fptu_cmp_tuples(fptu_take_noshrink(pt2), row2));

  // позиционируем курсор на конкретное значение ключевого поля
  fpta_value pk = fpta_value_cstr("pk-string");
  EXPECT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, &pk, nullptr));
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

  // ради проверки считаем повторы
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ(1u, dups);

  // получаем текущую строку, она должна совпадать с первым кортежем
  fptu_ro row1;
  EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row1));
  ASSERT_STREQ(nullptr, fptu_check_ro(row1));
  EXPECT_EQ(fptu_eq, fptu_cmp_tuples(fptu_take_noshrink(pt1), row1));

  // разрушаем созданные кортежи
  // на всякий случай предварительно проверяя их
  ASSERT_STREQ(nullptr, fptu_check(pt1));
  free(pt1);
  pt1 = nullptr;
  ASSERT_STREQ(nullptr, fptu_check(pt2));
  free(pt2);
  pt2 = nullptr;

  // удяляем текущую запись через курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_delete(cursor));
  // считаем сколько записей теперь, должа быть одна
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ(1u, dups);
  // ради теста проверям что данные есть
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);

  // переходим к первой записи
  EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  // еще раз удаляем запись
  EXPECT_EQ(FPTA_OK, fpta_cursor_delete(cursor));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  // теперь должно быть пусто
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ(0u, dups);
#else
  // курсор должен стать неустановленным
  EXPECT_EQ(FPTA_ECURSOR, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ((size_t)FPTA_DEADBEEF, dups);
#endif
  // ради теста проверям что данных больше нет
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(0u, count);

  // закрываем курсор и завершаем транзакцию
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // разрушаем привязанные идентификаторы
  fpta_name_destroy(&table);
  fpta_name_destroy(&col_pk);
  fpta_name_destroy(&col_a);
  fpta_name_destroy(&col_b);

  // закрываем базульку
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  // пока не удялем файлы чтобы можно было посмотреть и натравить mdbx_chk
  if (false) {
    ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
    ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
  }
}

TEST(SmokeIndex, Secondary) {
  /* Smoke-проверка жизнеспособности вторичных индексов.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей, в которой три колонки,
   *     и два индекса (primary и secondary).
   *  2. Добавляем данные:
   *      - добавляем "первую" запись, одновременно пытаясь
   *        добавить в строку-кортеж поля с "плохими" значениями.
   *      - добавляем "вторую" запись, которая отличается от первой
   *        всеми колонками.
   *      - также попутно пытаемся обновить несуществующие записи
   *        и вставить дубликаты.
   *  3. Читаем добавленное:
   *     - открываем курсор по вторичному индексу, без фильтра,
   *       на всю таблицу (весь диапазон строк),
   *       и проверяем кол-во записей и дубликатов.
   *     - переходим к последней, читаем и проверяем её (должна быть
   *       "вторая").
   *     - переходим к первой, читаем и проверяем её (должна быть "первая").
   *  4. Удаляем данные:
   *     - сначала "вторую" запись, потом "первую".
   *     - проверяем кол-во записей и дубликатов, eof для курсора.
   *  5. Завершаем операции и освобождаем ресурсы.
   */
  if (GTEST_IS_EXECUTION_TIMEOUT())
    return;
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  // открываем/создаем базульку в 1 мегабайт
  fpta_db *db = nullptr;
  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644, 1,
                         true, &db));
  ASSERT_NE(nullptr, db);

  // описываем простейшую таблицу с тремя колонками,
  // одним Primary и одним Secondary
  fpta_column_set def;
  fpta_column_set_init(&def);

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("pk_str_uniq", fptu_cstr,
                                 fpta_primary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "a_uint", fptu_uint64,
                         fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("b_fp", fptu_fp64, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  // запускам транзакцию и создаем таблицу с обозначенным набором колонок
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_1", &def));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // разрушаем описание таблицы
  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

  // инициализируем идентификаторы таблицы и её колонок
  fpta_name table, col_pk, col_a, col_b;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table_1"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_pk, "pk_str_uniq"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_a, "a_uint"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_b, "b_fp"));

  // начинаем транзакцию для вставки данных
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);
  // ради теста делаем привязку вручную
  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_pk));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_a));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_b));

  // создаем кортеж, который станет первой записью в таблице
  fptu_rw *pt1 = fptu_alloc(3, 42);
  ASSERT_NE(nullptr, pt1);
  ASSERT_STREQ(nullptr, fptu_check(pt1));

  // ради проверки пытаемся сделать нехорошее (добавить поля с нарушениями)
  EXPECT_EQ(FPTA_ETYPE, fpta_upsert_column(pt1, &col_pk, fpta_value_uint(12)));
  EXPECT_EQ(FPTA_EVALUE, fpta_upsert_column(pt1, &col_a, fpta_value_sint(-34)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt1, &col_b, fpta_value_cstr("string")));

  // добавляем нормальные значения
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt1, &col_pk, fpta_value_cstr("pk-string")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_a, fpta_value_sint(34)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_b, fpta_value_float(56.78)));
  ASSERT_STREQ(nullptr, fptu_check(pt1));

  // создаем еще один кортеж для второй записи
  fptu_rw *pt2 = fptu_alloc(3, 42);
  ASSERT_NE(nullptr, pt2);
  ASSERT_STREQ(nullptr, fptu_check(pt2));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_pk, fpta_value_cstr("zzz")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_a, fpta_value_sint(90)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_b, fpta_value_float(12.34)));
  ASSERT_STREQ(nullptr, fptu_check(pt2));

  // пытаемся обновить несуществующую запись
  EXPECT_EQ(FPTA_NOTFOUND,
            fpta_update_row(txn, &table, fptu_take_noshrink(pt1)));
  // вставляем и обновляем
  EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt1)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_row(txn, &table, fptu_take_noshrink(pt1)));
  EXPECT_EQ(FPTA_OK, fpta_update_row(txn, &table, fptu_take_noshrink(pt1)));
  EXPECT_EQ(FPTA_KEYEXIST,
            fpta_insert_row(txn, &table, fptu_take_noshrink(pt1)));

  // аналогично со второй записью
  EXPECT_EQ(FPTA_NOTFOUND,
            fpta_update_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_OK, fpta_update_row(txn, &table, fptu_take_noshrink(pt2)));
  EXPECT_EQ(FPTA_KEYEXIST,
            fpta_insert_row(txn, &table, fptu_take_noshrink(pt2)));

  // фиксируем изменения
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  // и начинаем следующую транзакцию
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);

  // открываем простейщий курсор: на всю таблицу, без фильтра
  fpta_cursor *cursor;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn, &col_a, fpta_value_begin(), fpta_value_end(),
                             nullptr, fpta_unsorted_dont_fetch, &cursor));
  ASSERT_NE(nullptr, cursor);

  // узнам сколько записей за курсором (в таблице).
  size_t count;
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(2u, count);

  // переходим к первой записи
  EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  // ради проверки убеждаемся что за курсором есть данные
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

  // считаем повторы, их не должно быть
  size_t dups;
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  ASSERT_EQ(1u, dups);

  // переходим к последней записи
  EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
  // ради проверки убеждаемся что за курсором есть данные
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

  // получаем текущую строку, она должна совпадать со вторым кортежем
  fptu_ro row2;
  EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row2));
  ASSERT_STREQ(nullptr, fptu_check_ro(row2));
  EXPECT_EQ(fptu_eq, fptu_cmp_tuples(fptu_take_noshrink(pt2), row2));

  // считаем повторы, их не должно быть
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  ASSERT_EQ(1u, dups);

  // позиционируем курсор на конкретное значение ключевого поля
  fpta_value pk = fpta_value_uint(34);
  EXPECT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, &pk, nullptr));
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

  // ради проверки считаем повторы
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ(1u, dups);

  // получаем текущую строку, она должна совпадать с первым кортежем
  fptu_ro row1;
  EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row1));
  ASSERT_STREQ(nullptr, fptu_check_ro(row1));
  EXPECT_EQ(fptu_eq, fptu_cmp_tuples(fptu_take_noshrink(pt1), row1));

  // разрушаем созданные кортежи
  // на всякий случай предварительно проверяя их
  ASSERT_STREQ(nullptr, fptu_check(pt1));
  free(pt1);
  pt1 = nullptr;
  ASSERT_STREQ(nullptr, fptu_check(pt2));
  free(pt2);
  pt2 = nullptr;

  // удяляем текущую запись через курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_delete(cursor));
  // считаем сколько записей теперь, должа быть одна
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ(1u, dups);
  // ради теста проверям что данные есть
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);

  // переходим к первой записи
  EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  // еще раз удаляем запись
  EXPECT_EQ(FPTA_OK, fpta_cursor_delete(cursor));
#if FPTA_ENABLE_RETURN_INTO_RANGE
  // теперь должно быть пусто
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ(0u, dups);
#else
  // курсор должен стать неустановленным
  EXPECT_EQ(FPTA_ECURSOR, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ((size_t)FPTA_DEADBEEF, dups);
#endif
  // ради теста проверям что данных больше нет
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(0u, count);

  // закрываем курсор и завершаем транзакцию
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // разрушаем привязанные идентификаторы
  fpta_name_destroy(&table);
  fpta_name_destroy(&col_pk);
  fpta_name_destroy(&col_a);
  fpta_name_destroy(&col_b);

  // закрываем базульку
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  // пока не удялем файлы чтобы можно было посмотреть и натравить mdbx_chk
  if (false) {
    ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
    ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
  }
}

//----------------------------------------------------------------------------

#include "keygen.hpp"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

static int mapdup_order2key(int order, int NNN) {
  int quart = NNN / 4;
  int offset = 0;
  int shift = 0;

  while (order >= quart) {
    offset += quart >> shift++;
    order -= quart;
  }
  return (order >> shift) + offset;
}

int mapdup_order2count(int order, int NNN) {
  int value = mapdup_order2key(order, NNN);

  int count = 1;
  for (int n = order; n < NNN; ++n)
    if (n != order && value == mapdup_order2key(n, NNN))
      count++;

  return count;
}

TEST(Smoke, mapdup_order2key) {
  std::map<int, int> checker;

  const int NNN = 32;
  for (int order = 0; order < 32; ++order) {
    int dup = mapdup_order2key(order, NNN);
    checker[dup] += 1;
  }
  EXPECT_EQ(1, checker[0]);
  EXPECT_EQ(1, checker[1]);
  EXPECT_EQ(1, checker[2]);
  EXPECT_EQ(1, checker[3]);
  EXPECT_EQ(1, checker[4]);
  EXPECT_EQ(1, checker[5]);
  EXPECT_EQ(1, checker[6]);
  EXPECT_EQ(1, checker[7]);
  EXPECT_EQ(2, checker[8]);
  EXPECT_EQ(2, checker[9]);
  EXPECT_EQ(2, checker[10]);
  EXPECT_EQ(2, checker[11]);
  EXPECT_EQ(4, checker[12]);
  EXPECT_EQ(4, checker[13]);
  EXPECT_EQ(8, checker[14]);
  EXPECT_EQ(15u, checker.size());
}

/* используем для контроля отдельную структуру, чтобы при проблемах/ошибках
 * явно видеть значения в отладчике. */
struct crud_item {
  unsigned pk_uint;
  double se_real;
  fptu_time time;
  std::string se_str;

  crud_item(unsigned pk, const char *str, double real, fptu_time datetime) {
    pk_uint = pk;
    se_real = real;
    time = datetime;
    se_str.assign(str, strlen(str));
  }

  struct less_pk {
    bool operator()(const crud_item *left, const crud_item *right) const {
      return left->pk_uint < right->pk_uint;
    }
  };
  struct less_str {
    bool operator()(const crud_item *left, const crud_item *right) const {
      return left->se_str < right->se_str;
    }
  };
  struct less_real {
    bool operator()(const crud_item *left, const crud_item *right) const {
      return left->se_real < right->se_real;
    }
  };
};

class SmokeCRUD : public ::testing::Test {
public:
  bool skipped;
  scoped_db_guard db_quard;
  scoped_txn_guard txn_guard;
  scoped_cursor_guard cursor_guard;
  fpta_name table, col_uint, col_time, col_str, col_real;

  // для проверки набора строк и их порядка
  std::vector<std::unique_ptr<crud_item>> container;
  std::set<crud_item *, crud_item::less_pk> checker_pk_uint;
  std::set<crud_item *, crud_item::less_str> checker_str;
  std::set<crud_item *, crud_item::less_real> checker_real;
  int ndeleted;

  void Check(fpta_cursor *cursor) {
    int move_result = fpta_cursor_move(cursor, fpta_first);
    if (container.size() - ndeleted == 0)
      EXPECT_EQ(FPTA_NODATA, move_result);
    else {
      EXPECT_EQ(FPTA_OK, move_result);
      unsigned count = 0;
      do {
        ASSERT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
        fptu_ro row;
        ASSERT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row));
        SCOPED_TRACE("row #" + std::to_string(count) + ", " +
                     std::to_string(row));
        unsigned row_present = 0;
        for (const auto &item : container) {
          if (!item)
            /* пропускаем удаленные строки */
            continue;
          fpta_value value;
          ASSERT_EQ(FPTA_OK, fpta_get_column(row, &col_uint, &value));
          if (item->pk_uint == value.uint) {
            row_present++;
            ASSERT_EQ(FPTA_OK, fpta_get_column(row, &col_str, &value));
            EXPECT_STREQ(item->se_str.c_str(), value.str);
            ASSERT_EQ(FPTA_OK, fpta_get_column(row, &col_real, &value));
            EXPECT_EQ(item->se_real, value.fp);
            ASSERT_EQ(FPTA_OK, fpta_get_column(row, &col_time, &value));
            EXPECT_EQ(item->time.fixedpoint, value.datetime.fixedpoint);
          }
        }
        ASSERT_EQ(1u, row_present);
        count++;
        move_result = fpta_cursor_move(cursor, fpta_next);
        ASSERT_TRUE(move_result == FPTA_OK || move_result == FPTA_NODATA);
      } while (move_result == FPTA_OK);
      EXPECT_EQ(container.size() - ndeleted, count);
    }
  }

  void Check() {
    ASSERT_TRUE(txn_guard.operator bool());

    /* проверяем по PK */ {
      SCOPED_TRACE("check: pk/uint");
      // открываем курсор по col_uint: на всю таблицу, без фильтра
      scoped_cursor_guard guard;
      fpta_cursor *cursor;
      EXPECT_EQ(FPTA_OK,
                fpta_cursor_open(txn_guard.get(), &col_uint, fpta_value_begin(),
                                 fpta_value_end(), nullptr,
                                 fpta_unsorted_dont_fetch, &cursor));
      ASSERT_NE(cursor, nullptr);
      guard.reset(cursor);
      ASSERT_NO_FATAL_FAILURE(Check(cursor));
    }

    /* проверяем по вторичному индексу колонки 'str' */ {
      SCOPED_TRACE("check: se/str");
      // открываем курсор по col_str: на всю таблицу, без фильтра
      scoped_cursor_guard guard;
      fpta_cursor *cursor;
      EXPECT_EQ(FPTA_OK,
                fpta_cursor_open(txn_guard.get(), &col_str, fpta_value_begin(),
                                 fpta_value_end(), nullptr,
                                 fpta_unsorted_dont_fetch, &cursor));
      ASSERT_NE(cursor, nullptr);
      guard.reset(cursor);
      ASSERT_NO_FATAL_FAILURE(Check(cursor));
    }

    /* проверяем по вторичному индексу колонки 'real' */ {
      SCOPED_TRACE("check: se/real");
      // открываем курсор по col_real: на всю таблицу, без фильтра
      scoped_cursor_guard guard;
      fpta_cursor *cursor;
      EXPECT_EQ(FPTA_OK,
                fpta_cursor_open(txn_guard.get(), &col_real, fpta_value_begin(),
                                 fpta_value_end(), nullptr,
                                 fpta_unsorted_dont_fetch, &cursor));
      ASSERT_NE(cursor, nullptr);
      guard.reset(cursor);
      ASSERT_NO_FATAL_FAILURE(Check(cursor));
    }
  }

  virtual void SetUp() {
    SCOPED_TRACE("setup");
    skipped = GTEST_IS_EXECUTION_TIMEOUT();
    if (skipped)
      return;

    // инициализируем идентификаторы таблицы и её колонок
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table_crud"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_uint, "uint"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_time, "time"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_str, "str"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_real, "real"));

    // чистим
    if (REMOVE_FILE(testdb_name) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }
    if (REMOVE_FILE(testdb_name_lck) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }
    ndeleted = 0;

    // открываем/создаем базульку в 1 мегабайт
    fpta_db *db = nullptr;
    EXPECT_EQ(FPTA_SUCCESS,
              fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644, 1,
                           true, &db));
    ASSERT_NE(nullptr, db);
    db_quard.reset(db);

    // описываем структуру таблицы
    fpta_column_set def;
    fpta_column_set_init(&def);

    EXPECT_EQ(FPTA_OK, fpta_column_describe("time", fptu_datetime,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("uint", fptu_uint32,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "str", fptu_cstr,
                           fpta_secondary_unique_ordered_reverse, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("real", fptu_fp64,
                                   fpta_secondary_withdups_unordered, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    // запускам транзакцию и создаем таблицу
    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);
    txn_guard.reset(txn);
    ASSERT_EQ(FPTA_OK, fpta_table_create(txn, "table_crud", &def));
    ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
    txn = nullptr;

    // разрушаем описание таблицы
    EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
    EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));
  }

  virtual void TearDown() {
    if (skipped)
      return;
    SCOPED_TRACE("teardown");

    // разрушаем привязанные идентификаторы
    fpta_name_destroy(&table);
    fpta_name_destroy(&col_uint);
    fpta_name_destroy(&col_time);
    fpta_name_destroy(&col_str);
    fpta_name_destroy(&col_real);

    // закрываем курсор и завершаем транзакцию
    if (cursor_guard) {
      EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    }
    if (txn_guard) {
      ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), true));
    }
    if (db_quard) {
      // закрываем и удаляем базу
      ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db_quard.release()));
      ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
      ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
    }
  }

  static unsigned mesh_order4uint(int n, int NNN) {
    return (37 * (unsigned)n) % NNN;
  }

  static int mesh_order4str(int n, int NNN) {
    return (int)((67 * (unsigned)n + 17) % NNN);
  }

  static int mesh_order4real(int n, int NNN) {
    return (int)((97 * (unsigned)n + 43) % NNN);
  }

  static unsigned mesh_order4update(int n, int NNN) {
    return (11 * (unsigned)n + 23) % NNN;
  }

  static unsigned mesh_order4delete(int n, int NNN) {
    return (5 * (unsigned)n + 13) % NNN;
  }
};

TEST_F(SmokeCRUD, none) {
  /* Smoke-проверка CRUD операций с участием индексов.
   *
   * Сценарий:
   *     Заполняем таблицу и затем обновляем и удаляем часть строк,
   *     как без курсора, так и открывая курсор для каждого из
   *     проиндексированных полей.
   *
   *  1. Создаем базу с одной таблицей, в которой:
   *      - четыре колонки и три индекса.
   *      - первичный индекс, для возможности secondary он должен быть
   *        с контролем уникальности.
   *      - два secondary, из которых один с контролем уникальности,
   *        второй неупорядоченный и "с дубликатами".
   *  2. Добавляем данные:
   *     - последующие шаги требуют не менее 32 строк;
   *     - для колонки с дубликатами реализуем карту: 8x1 (8 уникальных),
   *       4x2 (4 парных дубля), 2x4 (два значения по 4 раза),
   *       1x8 (одно значение 8 раз), это делает mapdup_order2key();
   *  3. Обновляем строки:
   *     - без курсора и без изменения PK: перебираем все комбинации
   *       сохранения/изменения каждой колонки = 7 комбинаций из 3 колонок;
   *     - через курсор по каждому индексу: перебираем все комбинации
   *       сохранения/изменения каждой колонки = 7 комбинаций из 3 колонок
   *       для каждого из трех индексов;
   *     - попутно пробуем сделать обновление с нарушением уникальности.
   *     = итого: обновляем 28 строк.
   *  4. Удаляем строки:
   *     - одну без использования курсора;
   *     - по одной через курсор по каждому индексу;
   *     - делаем это как для обновленных строк, так и для нетронутых.
   *     - попутно пробуем удалить несуществующие строки.
   *     - попутно пробуем удалить через fpta_delete() строки
   *       с существующим PK, но различиями в других колонках.
   *     = итого: удаляем 8 строк, из которых 4 не были обновлены.
   *  5. Проверяем содержимое таблицы и состояние индексов:
   *     - читаем без курсора, fpta_get() для каждого индекса с контролем
   *       уникальности = 3 строки;
   *     - через курсор по каждому индексу ходим по трём строкам (первая,
   *       последняя, туда-сюда), при этом читаем и сверяем значения.
   *  6. Завершаем операции и освобождаем ресурсы.
   */

  if (skipped)
    return;

  // начинаем транзакцию для вставки данных
  fpta_txn *txn = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_quard.get(), fpta_write, &txn));
  ASSERT_NE(nullptr, txn);
  txn_guard.reset(txn);

  // связываем идентификаторы с ранее созданной схемой
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_uint));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_time));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_str));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_real));

  // инициализируем генератор значений для строковой колонки
  any_keygen keygen(fptu_cstr, fpta_name_colindex(&col_str));

  // создаем кортеж, который будем использовать для заполнения таблицы
  fptu_rw *row = fptu_alloc(4, fpta_max_keylen * 2);
  ASSERT_NE(nullptr, row);
  ASSERT_STREQ(nullptr, fptu_check(row));

  constexpr int NNN = 42;
  /* создаем достаточно кол-во строк для последующих проверок */ {
    SCOPED_TRACE("fill");
    for (int i = 0; i < NNN; ++i) {
      /* перемешиваем, так чтобы у полей был независимый порядок */
      unsigned pk_uint_value = mesh_order4uint(i, NNN);
      int order_se_str = mesh_order4str(i, NNN);
      int order_se_real = mesh_order4real(i, NNN);
      double se_real_value = mapdup_order2key(order_se_real, NNN) / (double)NNN;

      SCOPED_TRACE(
          "add: row " + std::to_string(i) + " of [0.." + std::to_string(NNN) +
          "), orders: " + std::to_string(pk_uint_value) + " / " +
          std::to_string(order_se_str) + " / " + std::to_string(order_se_real) +
          " (" + std::to_string(se_real_value) + ")");
      ASSERT_EQ(FPTU_OK, fptu_clear(row));

      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_uint,
                                            fpta_value_uint(pk_uint_value)));
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_real,
                                            fpta_value_float(se_real_value)));

      /* пытаемся обновить несуществующую строку */
      EXPECT_EQ(FPTA_NOTFOUND, fpta_probe_and_update_row(
                                   txn, &table, fptu_take_noshrink(row)));

      /* пытаемся вставить неполноценную строку, в которой сейчас
       * не хватает одного из индексируемых полей, поэтому вместо
       * FPTA_NOTFOUND должно быть возвращено FPTA_COLUMN_MISSING */
      EXPECT_EQ(FPTA_COLUMN_MISSING, fpta_probe_and_upsert_row(
                                         txn, &table, fptu_take_noshrink(row)));
      EXPECT_EQ(FPTA_COLUMN_MISSING, fpta_probe_and_insert_row(
                                         txn, &table, fptu_take_noshrink(row)));

      /* добавляем недостающее индексируемое поле */
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_str,
                                            keygen.make(order_se_str, NNN)));

      /* теперь вставляем новую запись, но пока без поля `time`.
       * проверяем как insert, так и upsert. */
      if (i & 1) {
        EXPECT_EQ(FPTA_OK,
                  fpta_insert_row(txn, &table, fptu_take_noshrink(row)));
      } else {
        EXPECT_EQ(FPTA_OK,
                  fpta_upsert_row(txn, &table, fptu_take_noshrink(row)));
      }

      /* пробуем вставить дубликат */
      EXPECT_EQ(FPTA_KEYEXIST, fpta_probe_and_insert_row(
                                   txn, &table, fptu_take_noshrink(row)));

      /* добавляем поле `time` с нулевым значением и обновлем */
      fptu_time datetime;
      datetime.fixedpoint = 0;
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_time,
                                            fpta_value_datetime(datetime)));
      EXPECT_EQ(FPTA_OK, fpta_update_row(txn, &table, fptu_take_noshrink(row)));

      /* обновляем поле `time`, проверяя как update, так и upsert. */
      datetime = NOW_FINE();
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_time,
                                            fpta_value_datetime(datetime)));
      if (i & 2) {
        EXPECT_EQ(FPTA_OK, fpta_probe_and_update_row(txn, &table,
                                                     fptu_take_noshrink(row)));
      } else {
        EXPECT_EQ(FPTA_OK, fpta_probe_and_upsert_row(txn, &table,
                                                     fptu_take_noshrink(row)));
      }

      /* еще раз пробуем вставить дубликат */
      EXPECT_EQ(FPTA_KEYEXIST, fpta_probe_and_insert_row(
                                   txn, &table, fptu_take_noshrink(row)));

      /* обновляем PK и пробуем вставить дубликат по вторичным ключам */
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_column(row, &col_uint, fpta_value_uint(NNN)));
      EXPECT_EQ(FPTA_KEYEXIST, fpta_probe_and_insert_row(
                                   txn, &table, fptu_take_noshrink(row)));

      // добавляем аналог строки в проверочный набор
      fpta_value se_str_value;
      ASSERT_EQ(FPTA_OK, fpta_get_column(fptu_take_noshrink(row), &col_str,
                                         &se_str_value));
      container.emplace_back(new crud_item(pk_uint_value, se_str_value.str,
                                           se_real_value, datetime));

      checker_pk_uint.insert(container.back().get());
      checker_str.insert(container.back().get());
      checker_real.insert(container.back().get());
    }
  }

  // фиксируем транзакцию и добавленные данные
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
  txn = nullptr;

  //--------------------------------------------------------------------------

  /* При добавлении строк значения полей были перемешаны (сгенерированы в
   * нелинейном порядке), поэтому из container их можно брать просто
   * последовательно. Однако, для параметризируемой стохастичности теста
   * порядок будет еще раз перемешан посредством mesh_order4update(). */
  int nn = 0;

  // начинаем транзакцию для проверочных обновлений
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_quard.get(), fpta_write, &txn));
  ASSERT_NE(nullptr, txn);
  txn_guard.reset(txn);

  ASSERT_NO_FATAL_FAILURE(Check());

  /* обновляем строки без курсора и без изменения PK */ {
    SCOPED_TRACE("update.without-cursor");
    for (int m = 0; m < 8; ++m) {
      const auto n = mesh_order4update(nn++, NNN);
      SCOPED_TRACE("item " + std::to_string(n) + " of [0.." +
                   std::to_string(NNN) +
                   "), change-mask: " + std::to_string(m));
      crud_item *item = container[n].get();
      SCOPED_TRACE("row-src: pk " + std::to_string(item->pk_uint) + ", str \"" +
                   item->se_str + "\", real " + std::to_string(item->se_real) +
                   ", time " + std::to_string(item->time));
      ASSERT_EQ(FPTU_OK, fptu_clear(row));
      if (m & 1)
        item->se_str += "42";
      if (m & 2)
        item->se_real += 42;
      if (m & 4)
        item->time.fixedpoint += 42;
      SCOPED_TRACE("row-dst: pk " + std::to_string(item->pk_uint) + ", str \"" +
                   item->se_str + "\", real " + std::to_string(item->se_real) +
                   ", time " + std::to_string(item->time));

      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_str,
                                            fpta_value_str(item->se_str)));
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_real,
                                            fpta_value_float(item->se_real)));
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_time,
                                            fpta_value_datetime(item->time)));
      /* пробуем обновить без одного поля */
      EXPECT_EQ(FPTA_COLUMN_MISSING, fpta_probe_and_upsert_row(
                                         txn, &table, fptu_take_noshrink(row)));

      /* обновляем строку */
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_uint,
                                            fpta_value_uint(item->pk_uint)));
      EXPECT_EQ(FPTA_OK, fpta_probe_and_upsert_row(txn, &table,
                                                   fptu_take_noshrink(row)));
      ASSERT_NO_FATAL_FAILURE(Check());
    }
    ASSERT_NO_FATAL_FAILURE(Check());
  }

  /* обновляем строки через курсор по col_str. */ {
    SCOPED_TRACE("update.cursor-ordered_unique_reverse_str");
    // открываем курсор по col_str: на всю таблицу, без фильтра
    fpta_cursor *cursor;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &col_str, fpta_value_begin(),
                                        fpta_value_end(), nullptr,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);

    for (int m = 0; m < 8; ++m) {
      const auto n = mesh_order4update(nn++, NNN);
      SCOPED_TRACE("item " + std::to_string(n) + " of [0.." +
                   std::to_string(NNN) +
                   "), change-mask: " + std::to_string(m));
      crud_item *item = container[n].get();
      SCOPED_TRACE("row-src: pk " + std::to_string(item->pk_uint) + ", str \"" +
                   item->se_str + "\", real " + std::to_string(item->se_real) +
                   ", time " + std::to_string(item->time));

      fpta_value key = fpta_value_str(item->se_str);
      ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, &key, nullptr));
      EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
      // ради проверки считаем повторы
      size_t dups;
      EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
      EXPECT_EQ(1u, dups);

      ASSERT_EQ(FPTU_OK, fptu_clear(row));
      if (m & 1)
        item->pk_uint += NNN;
      if (m & 2)
        item->se_real += 42;
      if (m & 4)
        item->time.fixedpoint += 42;
      SCOPED_TRACE("row-dst: pk " + std::to_string(item->pk_uint) + ", str \"" +
                   item->se_str + "\", real " + std::to_string(item->se_real) +
                   ", time " + std::to_string(item->time));

      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_str,
                                            fpta_value_str(item->se_str)));
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_real,
                                            fpta_value_float(item->se_real)));
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_time,
                                            fpta_value_datetime(item->time)));
      /* пробуем обновить без одного поля */
      EXPECT_EQ(FPTA_COLUMN_MISSING,
                fpta_cursor_probe_and_update(cursor, fptu_take_noshrink(row)));

      /* обновляем строку */
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_uint,
                                            fpta_value_uint(item->pk_uint)));
      ASSERT_EQ(FPTA_OK,
                fpta_cursor_probe_and_update(cursor, fptu_take_noshrink(row)));

      ASSERT_NO_FATAL_FAILURE(Check());
    }

    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;

    ASSERT_NO_FATAL_FAILURE(Check());
  }

  /* обновляем строки через курсор по col_real. */ {
    SCOPED_TRACE("update.cursor-se-unordered_withdups_real");
    // открываем курсор по col_real: на всю таблицу, без фильтра
    fpta_cursor *cursor;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &col_real, fpta_value_begin(),
                                        fpta_value_end(), nullptr,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);

    for (int m = 0; m < 8; ++m) {
      const auto n = mesh_order4update(nn++, NNN);
      SCOPED_TRACE("item " + std::to_string(n) + " of [0.." +
                   std::to_string(NNN) +
                   "), change-mask: " + std::to_string(m));
      crud_item *item = container[n].get();
      SCOPED_TRACE("row-src: pk " + std::to_string(item->pk_uint) + ", str \"" +
                   item->se_str + "\", real " + std::to_string(item->se_real) +
                   ", time " + std::to_string(item->time));

      // считаем сколько должно быть повторов
      int expected_dups = 0;
      for (auto const &scan : container)
        if (item->se_real == scan->se_real)
          expected_dups++;

      fpta_value key = fpta_value_float(item->se_real);
      if (expected_dups == 1) {
        ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, &key, nullptr));
      } else {
        /* больше одного значения, точное позиционирование возможно
         * только по ключу не возможно, создаем фейковую строку с PK
         * и искомым значением для поиска */
        ASSERT_EQ(FPTU_OK, fptu_clear(row));
        ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_uint,
                                              fpta_value_uint(item->pk_uint)));
        ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_real, key));
        fptu_ro row_value = fptu_take_noshrink(row);
        /* теперь поиск должен быть успешен */
        ASSERT_EQ(FPTA_OK,
                  fpta_cursor_locate(cursor, true, nullptr, &row_value));
      }
      EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

      // проверяем кол-во повторов
      size_t dups;
      EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
      EXPECT_EQ(expected_dups, (int)dups);

      ASSERT_EQ(FPTU_OK, fptu_clear(row));
      if (m & 1)
        item->pk_uint += NNN;
      if (m & 2)
        item->se_str += "42";
      if (m & 4)
        item->time.fixedpoint += 42;
      SCOPED_TRACE("row-dst: pk " + std::to_string(item->pk_uint) + ", str \"" +
                   item->se_str + "\", real " + std::to_string(item->se_real) +
                   ", time " + std::to_string(item->time));

      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_uint,
                                            fpta_value_uint(item->pk_uint)));
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_real,
                                            fpta_value_float(item->se_real)));
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_time,
                                            fpta_value_datetime(item->time)));
      /* пробуем обновить без одного поля */
      EXPECT_EQ(FPTA_COLUMN_MISSING,
                fpta_cursor_probe_and_update(cursor, fptu_take_noshrink(row)));

      /* обновляем строку */
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_str,
                                            fpta_value_str(item->se_str)));
      ASSERT_EQ(FPTA_OK,
                fpta_cursor_probe_and_update(cursor, fptu_take_noshrink(row)));
      ASSERT_NO_FATAL_FAILURE(Check());
    }

    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;

    ASSERT_NO_FATAL_FAILURE(Check());
  }

  /* обновляем строки через курсор по col_uint (PK). */ {
    SCOPED_TRACE("update.cursor-pk_uint");
    // открываем курсор по col_uint: на всю таблицу, без фильтра
    fpta_cursor *cursor;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &col_uint, fpta_value_begin(),
                                        fpta_value_end(), nullptr,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);

    for (int m = 0; m < 8; ++m) {
      const auto n = mesh_order4update(nn++, NNN);
      SCOPED_TRACE("item " + std::to_string(n) + " of [0.." +
                   std::to_string(NNN) +
                   "), change-mask: " + std::to_string(m));
      crud_item *item = container[n].get();
      SCOPED_TRACE("row-src: pk " + std::to_string(item->pk_uint) + ", str \"" +
                   item->se_str + "\", real " + std::to_string(item->se_real) +
                   ", time " + std::to_string(item->time));

      fpta_value key = fpta_value_uint(item->pk_uint);
      ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, &key, nullptr));
      EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
      // ради проверки считаем повторы
      size_t dups;
      EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
      EXPECT_EQ(1u, dups);

      ASSERT_EQ(FPTU_OK, fptu_clear(row));
      if (m & 1)
        item->se_str += "42";
      if (m & 2)
        item->se_real += 42;
      if (m & 4)
        item->time.fixedpoint += 42;
      SCOPED_TRACE("row-dst: pk " + std::to_string(item->pk_uint) + ", str \"" +
                   item->se_str + "\", real " + std::to_string(item->se_real) +
                   ", time " + std::to_string(item->time));

      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_str,
                                            fpta_value_str(item->se_str)));
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_real,
                                            fpta_value_float(item->se_real)));
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_time,
                                            fpta_value_datetime(item->time)));
      /* пробуем обновить без одного поля */
      EXPECT_EQ(FPTA_COLUMN_MISSING,
                fpta_cursor_probe_and_update(cursor, fptu_take_noshrink(row)));

      /* обновляем строку */
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_uint,
                                            fpta_value_uint(item->pk_uint)));
      EXPECT_EQ(FPTA_OK,
                fpta_cursor_probe_and_update(cursor, fptu_take_noshrink(row)));
      ASSERT_NO_FATAL_FAILURE(Check());
    }

    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;

    ASSERT_NO_FATAL_FAILURE(Check());
  }

  // фиксируем транзакцию и измененные данные
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
  txn = nullptr;

  //--------------------------------------------------------------------------

  /* При добавлении строк значения полей были перемешаны (сгенерированы в
   * нелинейном порядке), поэтому из container их можно брать просто
   * последовательно. Однако, для параметризируемой стохастичности теста
   * порядок будет еще раз перемешан посредством mesh_order4delete(). */
  nn = 0;

  /* за четыре подхода удаляем половину от добавленных строк. */
  const int ndel = NNN / 2 / 4;

  // начинаем транзакцию для проверочных удалений
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_quard.get(), fpta_write, &txn));
  ASSERT_NE(nullptr, txn);
  txn_guard.reset(txn);

  /* удаляем строки без курсора */ {
    SCOPED_TRACE("delete.without-cursor");

    for (int i = 0; i < ndel; ++i) {
      const auto n = mesh_order4delete(nn++, NNN);
      SCOPED_TRACE("item " + std::to_string(n) + " of [0.." +
                   std::to_string(NNN) + "), step #" + std::to_string(i));
      crud_item *item = container[n].get();
      ASSERT_NE(nullptr, item);
      SCOPED_TRACE("row: pk " + std::to_string(item->pk_uint) + ", str \"" +
                   item->se_str + "\", real " + std::to_string(item->se_real) +
                   ", time " + std::to_string(item->time));
      ASSERT_EQ(FPTU_OK, fptu_clear(row));

      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_str,
                                            fpta_value_str(item->se_str)));
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_real,
                                            fpta_value_float(item->se_real)));
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_uint,
                                            fpta_value_uint(item->pk_uint)));

      /* пробуем удалить без одного поля */
      EXPECT_EQ(FPTA_NOTFOUND,
                fpta_delete(txn, &table, fptu_take_noshrink(row)));
      /* пробуем удалить с различием в данных (поле time) */
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_time,
                                            fpta_value_datetime(NOW_FINE())));
      EXPECT_EQ(FPTA_NOTFOUND,
                fpta_delete(txn, &table, fptu_take_noshrink(row)));

      /* пробуем удалить с другим различием в данных (поле real) */
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_time,
                                            fpta_value_datetime(item->time)));
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_column(row, &col_real,
                                   fpta_value_float(item->se_real + 42)));
      EXPECT_EQ(FPTA_NOTFOUND,
                fpta_delete(txn, &table, fptu_take_noshrink(row)));

      /* устряняем расхождение и удаляем */
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_real,
                                            fpta_value_float(item->se_real)));
      EXPECT_EQ(FPTA_OK, fpta_delete(txn, &table, fptu_take_noshrink(row)));

      container[n].reset();
      ndeleted++;

      ASSERT_NO_FATAL_FAILURE(Check());
    }

    ASSERT_NO_FATAL_FAILURE(Check());
  }

  /* удаляем строки через курсор по col_str. */ {
    SCOPED_TRACE("delete.cursor-ordered_unique_reverse_str");
    // открываем курсор по col_str: на всю таблицу, без фильтра
    fpta_cursor *cursor;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &col_str, fpta_value_begin(),
                                        fpta_value_end(), nullptr,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);

    for (int i = 0; i < ndel; ++i) {
      const auto n = mesh_order4delete(nn++, NNN);
      SCOPED_TRACE("item " + std::to_string(n) + " of [0.." +
                   std::to_string(NNN) + "), step #" + std::to_string(i));
      crud_item *item = container[n].get();
      ASSERT_NE(nullptr, item);
      SCOPED_TRACE("row: pk " + std::to_string(item->pk_uint) + ", str \"" +
                   item->se_str + "\", real " + std::to_string(item->se_real) +
                   ", time " + std::to_string(item->time));

      fpta_value key = fpta_value_str(item->se_str);
      ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, &key, nullptr));
      EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
      // ради проверки считаем повторы
      size_t dups;
      EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
      EXPECT_EQ(1u, dups);

      ASSERT_EQ(FPTA_OK, fpta_cursor_delete(cursor));
      ASSERT_EQ(FPTA_NODATA, fpta_cursor_locate(cursor, true, &key, nullptr));
      EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
      EXPECT_EQ(FPTA_ECURSOR, fpta_cursor_dups(cursor, &dups));
      EXPECT_EQ((size_t)FPTA_DEADBEEF, dups);

      /* LY: удалять элемент нужно после использования key, так как
       * в key просто указатель на данные std::string, которые будут
       * освобождены при удалении. */
      container[n].reset();
      ndeleted++;
      ASSERT_NO_FATAL_FAILURE(Check());
    }

    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;

    ASSERT_NO_FATAL_FAILURE(Check());
  }

  /* удаляем строки через курсор по col_real. */ {
    SCOPED_TRACE("delete.cursor-se-unordered_withdups_real");
    // открываем курсор по col_real: на всю таблицу, без фильтра
    fpta_cursor *cursor;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &col_real, fpta_value_begin(),
                                        fpta_value_end(), nullptr,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);

    for (int i = 0; i < ndel; ++i) {
      const auto n = mesh_order4delete(nn++, NNN);
      SCOPED_TRACE("item " + std::to_string(n) + " of [0.." +
                   std::to_string(NNN) + "), step #" + std::to_string(i));
      crud_item *item = container[n].get();
      ASSERT_NE(nullptr, item);
      SCOPED_TRACE("row: pk " + std::to_string(item->pk_uint) + ", str \"" +
                   item->se_str + "\", real " + std::to_string(item->se_real) +
                   ", time " + std::to_string(item->time));

      // считаем сколько должно быть повторов
      unsigned expected_dups = 0;
      for (auto const &scan : container)
        if (scan && item->se_real == scan->se_real)
          expected_dups++;

      fptu_ro row_value;
      fpta_value key = fpta_value_float(item->se_real);
      if (expected_dups == 1) {
        ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, &key, nullptr));
      } else {
        /* больше одного значения, точное позиционирование возможно
         * только по ключу не возможно, создаем фейковую строку с PK
         * и искомым значением для поиска */
        ASSERT_EQ(FPTU_OK, fptu_clear(row));
        ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_uint,
                                              fpta_value_uint(item->pk_uint)));
        ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_real, key));
        row_value = fptu_take_noshrink(row);
        /* теперь поиск должен быть успешен */
        ASSERT_EQ(FPTA_OK,
                  fpta_cursor_locate(cursor, true, nullptr, &row_value));
      }
      EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

      // проверяем кол-во повторов
      size_t dups;
      EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
      EXPECT_EQ(expected_dups, dups);

      ASSERT_EQ(FPTA_OK, fpta_cursor_delete(cursor));
      container[n].reset();
      ndeleted++;

      if (--expected_dups == 0) {
        ASSERT_EQ(FPTA_NODATA, fpta_cursor_locate(cursor, true, &key, nullptr));
        EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
        EXPECT_EQ(FPTA_ECURSOR, fpta_cursor_dups(cursor, &dups));
        EXPECT_EQ((size_t)FPTA_DEADBEEF, dups);
      } else {
        ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, &key, nullptr));
        EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
        EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
        EXPECT_EQ(expected_dups, dups);
      }

      ASSERT_NO_FATAL_FAILURE(Check());
    }

    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;

    ASSERT_NO_FATAL_FAILURE(Check());
  }

  /* удаляем строки через курсор по col_uint (PK). */ {
    SCOPED_TRACE("delete.cursor-pk_uint");
    // открываем курсор по col_uint: на всю таблицу, без фильтра
    fpta_cursor *cursor;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &col_uint, fpta_value_begin(),
                                        fpta_value_end(), nullptr,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);

    for (int i = 0; i < ndel; ++i) {
      const auto n = mesh_order4delete(nn++, NNN);
      SCOPED_TRACE("item " + std::to_string(n) + " of [0.." +
                   std::to_string(NNN) + "), step #" + std::to_string(i));
      crud_item *item = container[n].get();
      ASSERT_NE(nullptr, item);
      SCOPED_TRACE("row: pk " + std::to_string(item->pk_uint) + ", str \"" +
                   item->se_str + "\", real " + std::to_string(item->se_real) +
                   ", time " + std::to_string(item->time));

      fpta_value key = fpta_value_uint(item->pk_uint);
      ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, &key, nullptr));
      EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
      // ради проверки считаем повторы
      size_t dups;
      EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
      EXPECT_EQ(1u, dups);

      ASSERT_EQ(FPTA_OK, fpta_cursor_delete(cursor));
      container[n].reset();
      ndeleted++;

      ASSERT_EQ(FPTA_NODATA, fpta_cursor_locate(cursor, true, &key, nullptr));
      EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
      EXPECT_EQ(FPTA_ECURSOR, fpta_cursor_dups(cursor, &dups));
      EXPECT_EQ((size_t)FPTA_DEADBEEF, dups);

      ASSERT_NO_FATAL_FAILURE(Check());
    }

    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;

    ASSERT_NO_FATAL_FAILURE(Check());
  }

  // фиксируем транзакцию и удаление данных
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
  txn = nullptr;

  //--------------------------------------------------------------------------

  // начинаем транзакцию для финальной проверки
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_quard.get(), fpta_read, &txn));
  ASSERT_NE(nullptr, txn);
  txn_guard.reset(txn);

  ASSERT_NO_FATAL_FAILURE(Check());

  // закрываем транзакцию
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
  txn = nullptr;

  free(row);
  row = nullptr;
}

//----------------------------------------------------------------------------

class SmokeSelect
    : public ::testing::TestWithParam<
          GTEST_TUPLE_NAMESPACE_::tuple<fpta_index_type, fpta_cursor_options>> {
public:
  scoped_db_guard db_quard;
  scoped_txn_guard txn_guard;
  scoped_cursor_guard cursor_guard;

  fpta_name table, col_1, col_2;
  fpta_index_type index;
  fpta_cursor_options ordering;
  bool valid_ops, skipped;

  unsigned count_value_3;
  virtual void SetUp() {
    index = GTEST_TUPLE_NAMESPACE_::get<0>(GetParam());
    ordering = GTEST_TUPLE_NAMESPACE_::get<1>(GetParam());
    valid_ops =
        is_valid4primary(fptu_int32, index) && is_valid4cursor(index, ordering);
    ordering = (fpta_cursor_options)(ordering | fpta_dont_fetch);

    SCOPED_TRACE("index " + std::to_string(index) + ", ordering " +
                 std::to_string(ordering) +
                 (valid_ops ? ", (valid case)" : ", (invalid case)"));

    skipped = GTEST_IS_EXECUTION_TIMEOUT();
    if (!valid_ops || skipped)
      return;

    // инициализируем идентификаторы таблицы и её колонок
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_1, "col_1"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_2, "col_2"));

    if (REMOVE_FILE(testdb_name) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }
    if (REMOVE_FILE(testdb_name_lck) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }

    // открываем/создаем базульку в 1 мегабайт
    fpta_db *db = nullptr;
    EXPECT_EQ(FPTA_SUCCESS,
              fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644, 1,
                           true, &db));
    ASSERT_NE(nullptr, db);
    db_quard.reset(db);

    // описываем простейшую таблицу с двумя колонками
    fpta_column_set def;
    fpta_column_set_init(&def);

    EXPECT_EQ(FPTA_OK, fpta_column_describe("col_1", fptu_int32, index, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("col_2", fptu_int32, fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    // запускам транзакцию и создаем таблицу с обозначенным набором колонок
    fpta_txn *txn = (fpta_txn *)&txn;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);
    txn_guard.reset(txn);
    EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
    txn = nullptr;

    // разрушаем описание таблицы
    EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
    EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

    // начинаем транзакцию для вставки данных
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
    ASSERT_NE(nullptr, txn);
    txn_guard.reset(txn);

    // создаем кортеж, который станет первой записью в таблице
    fptu_rw *pt = fptu_alloc(3, 42);
    ASSERT_NE(nullptr, pt);
    ASSERT_STREQ(nullptr, fptu_check(pt));

    // делаем привязку к схеме
    fpta_name_refresh_couple(txn, &table, &col_1);
    fpta_name_refresh(txn, &col_2);

    count_value_3 = 0;
    for (unsigned n = 0; n < 42; ++n) {
      EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_1, fpta_value_sint(n)));
      unsigned value = (n + 3) % 5;
      count_value_3 += (value == 3);
      EXPECT_EQ(FPTA_OK,
                fpta_upsert_column(pt, &col_2, fpta_value_sint(value)));
      ASSERT_STREQ(nullptr, fptu_check(pt));

      ASSERT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt)));
      // EXPECT_EQ(FPTA_OK, fptu_clear(pt));
    }

    free(pt);
    pt = nullptr;

    // фиксируем изменения
    EXPECT_EQ(FPTA_OK, fpta_transaction_commit(txn_guard.release()));
    txn = nullptr;

    // начинаем следующую транзакцию
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
    ASSERT_NE(nullptr, txn);
    txn_guard.reset(txn);
  }

  virtual void TearDown() {
    if (skipped)
      return;
    SCOPED_TRACE("teardown");

    // разрушаем привязанные идентификаторы
    fpta_name_destroy(&table);
    fpta_name_destroy(&col_1);
    fpta_name_destroy(&col_2);

    // закрываем курсор и завершаем транзакцию
    if (cursor_guard) {
      EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    }
    if (txn_guard) {
      ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), true));
    }
    if (db_quard) {
      // закрываем и удаляем базу
      ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db_quard.release()));
      ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
      ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
    }
  }
};

TEST_P(SmokeSelect, Range) {
  /* Smoke-проверка жизнеспособности курсоров с ограничениями диапазона.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей, в которой две колонки
   *     и один (primary) индекс.
   *
   *  2. Вставляем 42 строки, с последовательным увеличением
   *     значения в первой колонке.
   *
   *  3. Несколько раз открываем курсор с разнымм диапазонами
   *     и проверяем кол-во строк попадающее в выборку.
   *
   *  4. Завершаем операции и освобождаем ресурсы.
   */
  SCOPED_TRACE("index " + std::to_string(index) + ", ordering " +
               std::to_string(ordering) +
               (valid_ops ? ", (valid case)" : ", (invalid case)"));

  if (!valid_ops || skipped)
    return;

  // открываем простейщий курсор БЕЗ диапазона
  fpta_cursor *cursor;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                             fpta_value_end(), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  size_t count;
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(42u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем простейщий курсор c диапазоном (полное покрытие)
  if (fpta_index_is_ordered(index)) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(
                           txn_guard.get(), &col_1, fpta_value_sint(-1),
                           fpta_value_sint(43), nullptr, ordering, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    // проверяем кол-во записей и закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(42u, count);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
  } else {
    EXPECT_EQ(FPTA_NO_INDEX,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(-1),
                               fpta_value_sint(43), nullptr, ordering,
                               &cursor));
    ASSERT_EQ(nullptr, cursor);
  }

  // открываем простейщий курсор c диапазоном (полное покрытие, от begin)
  // LY: в случае unordered индексов здесь эксплуатируется недокументированное
  //     свойство unordered_index(integer) == ordered_index(integer)
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                             fpta_value_sint(43), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(42u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем простейщий курсор c диапазоном (полное покрытие, до begin)
  // LY: в случае unordered индексов здесь эксплуатируется недокументированное
  //     свойство unordered_index(integer) == ordered_index(integer)
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(-1),
                             fpta_value_end(), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(42u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  if (!fpta_index_is_ordered(index)) {
    // для unordered индексов тесты ниже вернут FPTA_NO_INDEX
    // и это уже было проверенно выше
    return;
  }

  // открываем c диапазоном (нулевое пересечение, курсор "ниже")
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(-42),
                             fpta_value_sint(0), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(0u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем c диапазоном (нулевое пересечение, курсор "выше")
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(42),
                             fpta_value_sint(100), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(0u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем c диапазоном (единичное пересечение, курсор "снизу")
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(-42),
                             fpta_value_sint(1), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем c диапазоном (единичное пересечение, курсор "сверху")
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(41),
                             fpta_value_sint(100), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем c диапазоном (пересечение 50%, курсор "снизу")
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(-100),
                             fpta_value_sint(21), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(21u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем c диапазоном (пересечение 50%, курсор "сверху")
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(21),
                             fpta_value_sint(100), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(21u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем c диапазоном (пересечение 50%, курсор "внутри")
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(10),
                             fpta_value_sint(31), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(21u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем c диапазоном (без пересечения, пустой диапазон)
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(17),
                             fpta_value_sint(17), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(0u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем c диапазоном (без пересечения, "отрицательный" диапазон)
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(31),
                             fpta_value_sint(10), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(0u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;
}

static bool filter_row_predicate_true(const fptu_ro *, void *, void *) {
  return true;
}

static bool filter_row_predicate_false(const fptu_ro *, void *, void *) {
  return false;
}

static bool filter_col_predicate_odd(const fptu_field *column, void *) {
  return (fptu_field_int32(column) & 1) != 0;
}

TEST_P(SmokeSelect, Filter) {
  /* Smoke-проверка жизнеспособности курсоров с фильтром.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей, в которой две колонки
   *     и один (primary) индекс.
   *
   *  2. Вставляем 42 строки, с последовательным увеличением
   *     значения в первой колонке.
   *
   *  3. Несколько раз открываем курсор с разными фильтрами
   *     и проверяем кол-во строк попадающее в выборку.
   *
   *  4. Завершаем операции и освобождаем ресурсы.
   */
  SCOPED_TRACE("index " + std::to_string(index) + ", ordering " +
               std::to_string(ordering) +
               (valid_ops ? ", (valid case)" : ", (invalid case)"));

  if (!valid_ops || skipped)
    return;

  // открываем простейщий курсор БЕЗ фильтра
  fpta_cursor *cursor;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                             fpta_value_end(), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  size_t count;
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(42u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем простейщий курсор c псевдо-фильтром (полное покрытие)
  fpta_filter filter;
  filter.type = fpta_node_fnrow;
  filter.node_fnrow.context = nullptr /* unused */;
  filter.node_fnrow.arg = nullptr /* unused */;
  filter.node_fnrow.predicate = filter_row_predicate_true;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                             fpta_value_end(), &filter, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(42u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем простейщий курсор c псевдо-фильтром (нулевое покрытие)
  filter.node_fnrow.predicate = filter_row_predicate_false;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                             fpta_value_end(), &filter, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(0u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем курсор c фильтром по нечетности значения колонки (покрытие 50%)
  filter.type = fpta_node_fncol;
  filter.node_fncol.column_id = &col_1;
  filter.node_fncol.arg = nullptr /* unused */;
  filter.node_fncol.predicate = filter_col_predicate_odd;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                             fpta_value_end(), &filter, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(21u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем курсор c фильтром по значению колонки (равенство)
  filter.type = fpta_node_eq;
  filter.node_cmp.left_id = &col_2;
  filter.node_cmp.right_value = fpta_value_uint(3);
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                             fpta_value_end(), &filter, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(count_value_3, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем курсор c фильтром по значению колонки (не равенство)
  filter.type = fpta_node_ne;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                             fpta_value_end(), &filter, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(42u - count_value_3, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем курсор c фильтром по значению колонки (больше)
  filter.type = fpta_node_gt;
  filter.node_cmp.left_id = &col_1;
  filter.node_cmp.right_value = fpta_value_uint(10);
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                             fpta_value_end(), &filter, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(31u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем курсор c фильтром по значению колонки (меньше)
  filter.type = fpta_node_lt;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                             fpta_value_end(), &filter, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(10u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем курсор c тем-же фильтром по значению колонки (меньше)
  // и диапазоном с перекрытием 50% после от фильтра.
  filter.type = fpta_node_lt;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                             fpta_value_uint(5), &filter, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(5u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // меняем фильтр на "больше или равно" и открываем курсор с диапазоном,
  // который имеет только одну "общую" запись с условием фильтра.
  filter.type = fpta_node_ge;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                             fpta_value_uint(11), &filter, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;
}

#if defined(GTEST_HAS_COMBINE) && GTEST_HAS_COMBINE

INSTANTIATE_TEST_CASE_P(
    Combine, SmokeSelect,
    ::testing::Combine(::testing::Values(fpta_primary_unique_ordered_obverse,
                                         fpta_primary_withdups_ordered_obverse,
                                         fpta_primary_unique_unordered,
                                         fpta_primary_withdups_unordered),
                       ::testing::Values(fpta_unsorted, fpta_ascending,
                                         fpta_descending)));
#else

TEST(SmokeSelect, GoogleTestCombine_IS_NOT_Supported_OnThisPlatform) {}

#endif /* GTEST_HAS_COMBINE */

//----------------------------------------------------------------------------

TEST(SmokeCrud, OneRowOneColumn) {
  const bool skipped = GTEST_IS_EXECUTION_TIMEOUT();
  if (skipped)
    return;
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  // открываем/создаем базульку в 1 мегабайт
  fpta_db *db = nullptr;
  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644, 1,
                         true, &db));
  ASSERT_NE(nullptr, db);

  // описываем простейшую таблицу с одним PK
  fpta_column_set def;
  fpta_column_set_init(&def);
  ASSERT_EQ(FPTA_OK,
            fpta_column_describe("StrColumn", fptu_cstr,
                                 fpta_primary_unique_ordered_obverse, &def));
  ASSERT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  // запускам транзакцию и создаем таблицу с обозначенным набором колонок
  fpta_txn *txn = nullptr;
  ASSERT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_EQ(FPTA_OK, fpta_table_create(txn, "Table", &def));
  ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // разрушаем описание таблицы
  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

  // инициализируем идентификаторы таблицы и её колонок
  fpta_name table, col_pk;
  ASSERT_EQ(FPTA_OK, fpta_table_init(&table, "Table"));
  ASSERT_EQ(FPTA_OK, fpta_column_init(&table, &col_pk, "StrColumn"));

  // начинаем транзакцию для вставки данных
  ASSERT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));

  // ради теста делаем привязку вручную
  ASSERT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_pk));

  // создаем кортеж, который станет первой записью в таблице
  fptu_rw *pt1 = fptu_alloc(1, 42);
  ASSERT_NE(nullptr, pt1);
  ASSERT_EQ(nullptr, fptu_check(pt1));

  // добавляем значения колонки
  ASSERT_EQ(FPTA_OK,
            fpta_upsert_column(pt1, &col_pk, fpta_value_cstr("login")));
  ASSERT_EQ(nullptr, fptu_check(pt1));

  // вставляем строку в таблицу
  ASSERT_EQ(FPTA_OK, fpta_upsert_row(txn, &table, fptu_take(pt1)));

  // освобождаем кортеж/строку
  free(pt1);
  pt1 = nullptr;

  // фиксируем изменения
  ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  ASSERT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));

  fpta_cursor *cursor = nullptr;
  ASSERT_EQ(FPTA_OK,
            fpta_cursor_open(txn, &col_pk, fpta_value_begin(), fpta_value_end(),
                             nullptr, fpta_unsorted_dont_fetch, &cursor));

  size_t count = size_t(UINT64_C(0xBADBADBAD) & SIZE_MAX);
  ASSERT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  ASSERT_EQ(1u, count);
  ASSERT_EQ(FPTA_OK, fpta_cursor_close(cursor));

  ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, false));

  // разрушаем привязанные идентификаторы
  fpta_name_destroy(&table);
  fpta_name_destroy(&col_pk);

  // закрываем базу
  ASSERT_EQ(FPTA_OK, fpta_db_close(db));
}

//----------------------------------------------------------------------------

TEST(Smoke, DirectDirtyDeletions) {
  /* Smoke-проверка удаления строки из "грязной" страницы, при наличии
   * вторичных индексов.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей, в которой несколько колонок
   *   и есть хотя-бы один вторичный индекс.
   *
   *  2. Вставляем 11 строки, при этом некоторые значения близкие
   *     и точно попадут в одну страницу БД.
   *
   *  3. Удаляем одну строку, затем в той-же транзакции ищем и удаляем
   *     вторую строку, которая после первого удаления должна располагаться
   *     в измененной "грязной" страницы.
   *
   *  4. Завершаем операции и освобождаем ресурсы.
   */
  const bool skipped = GTEST_IS_EXECUTION_TIMEOUT();
  if (skipped)
    return;
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  // создаем базу
  fpta_db *db = nullptr;
  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_sync, fpta_regime_default, 0644, 1,
                         true, &db));
  ASSERT_NE(nullptr, db);

  // начинаем транзакцию с добавлениями
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  // описываем структуру таблицы и создаем её
  fpta_column_set def;
  fpta_column_set_init(&def);
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("Nnn", fptu_int64,
                                 fpta_primary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "_createdAt", fptu_datetime,
                         fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("_id", fptu_int64,
                                 fpta_secondary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));
  ASSERT_EQ(FPTA_OK, fpta_table_create(txn, "bugged", &def));

  // разрушаем описание таблицы
  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

  // готовим идентификаторы для манипуляций с данными
  fpta_name table, col_num, col_date, col_str;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "bugged"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_num, "Nnn"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_date, "_createdAt"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_str, "_id"));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_num));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_date));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_str));

  // выделяем кортеж и вставляем 11 строк
  fptu_rw *pt = fptu_alloc(3, 8 + 8 + 8);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  // 1
  fptu_time datetime;
  datetime.fixedpoint = 1492170771;
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_num, fpta_value_sint(100)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_date, fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_str,
                                        fpta_value_sint(6408824664381050880)));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  fptu_ro row = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(row));
  EXPECT_EQ(FPTA_OK, fpta_put(txn, &table, row, fpta_insert));

  // 2
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_num, fpta_value_sint(101)));

  datetime.fixedpoint = 1492170775;
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_date, fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_str,
                                        fpta_value_sint(6408824680314742784)));
  row = fptu_take_noshrink(pt);
  EXPECT_EQ(FPTA_OK, fpta_put(txn, &table, row, fpta_insert));

  // 3
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_num, fpta_value_sint(102)));

  datetime.fixedpoint = 1492170777;
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_date, fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_str,
                                        fpta_value_sint(6408824688070591488)));
  row = fptu_take_noshrink(pt);
  EXPECT_EQ(FPTA_OK, fpta_put(txn, &table, row, fpta_insert));

  // 4
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_num, fpta_value_sint(103)));
  datetime.fixedpoint = 1492170778;
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_date, fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_str,
                                        fpta_value_sint(6408824693901869056)));
  row = fptu_take_noshrink(pt);
  EXPECT_EQ(FPTA_OK, fpta_put(txn, &table, row, fpta_insert));

  // 5
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_num, fpta_value_sint(104)));
  datetime.fixedpoint = 1492170779;
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_date, fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_str,
                                        fpta_value_sint(6408824699339551744)));
  row = fptu_take_noshrink(pt);
  EXPECT_EQ(FPTA_OK, fpta_put(txn, &table, row, fpta_insert));

  // 6
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_num, fpta_value_sint(105)));
  datetime.fixedpoint = 1492170781;
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_date, fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_str,
                                        fpta_value_sint(6408824705469209600)));
  row = fptu_take_noshrink(pt);
  EXPECT_EQ(FPTA_OK, fpta_put(txn, &table, row, fpta_insert));

  // 7
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_num, fpta_value_sint(106)));
  datetime.fixedpoint = 1492170782;
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_date, fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_str,
                                        fpta_value_sint(6408824710579991552)));
  row = fptu_take_noshrink(pt);
  EXPECT_EQ(FPTA_OK, fpta_put(txn, &table, row, fpta_insert));

  // 8
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_num, fpta_value_sint(107)));
  datetime.fixedpoint = 1492170784;
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_date, fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_str,
                                        fpta_value_sint(6408824719167151104)));
  row = fptu_take_noshrink(pt);
  EXPECT_EQ(FPTA_OK, fpta_put(txn, &table, row, fpta_insert));

  // 9
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_num, fpta_value_sint(108)));
  datetime.fixedpoint = 1492170786;
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_date, fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_str,
                                        fpta_value_sint(6408824727095985152)));
  row = fptu_take_noshrink(pt);
  EXPECT_EQ(FPTA_OK, fpta_put(txn, &table, row, fpta_insert));

  // 10
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_num, fpta_value_sint(109)));
  datetime.fixedpoint = 1492170788;
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_date, fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_str,
                                        fpta_value_sint(6408824736249964544)));
  row = fptu_take_noshrink(pt);
  EXPECT_EQ(FPTA_OK, fpta_put(txn, &table, row, fpta_insert));

  // 11
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_num, fpta_value_sint(110)));
  datetime.fixedpoint = 1492170790;
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_date, fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_str,
                                        fpta_value_sint(6408824744270998528)));
  row = fptu_take_noshrink(pt);
  EXPECT_EQ(FPTA_OK, fpta_put(txn, &table, row, fpta_insert));

  // завершаем транзакцию с добавлениями
  ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //--------------------------------------------------------------------------
  // начинаем транзакцию с удалениями
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);
  fptu_ro row2;
  fpta_value num2;

  // читаем вторую строку для проверки что сейчас она НЕ в грязной странице.
  num2 = fpta_value_sint(6408824736249964544);
  EXPECT_EQ(FPTA_OK, fpta_get(txn, &col_str, &num2, &row2));
  EXPECT_EQ(MDBX_RESULT_FALSE, mdbx_is_dirty(txn->mdbx_txn, row2.sys.iov_base));

  // читаем и удаляем первую строку
  num2 = fpta_value_sint(6408824727095985152);
  EXPECT_EQ(FPTA_OK, fpta_get(txn, &col_str, &num2, &row2));
  EXPECT_EQ(MDBX_RESULT_FALSE, mdbx_is_dirty(txn->mdbx_txn, row2.sys.iov_base));
  EXPECT_EQ(FPTA_OK, fpta_delete(txn, &table, row2));

  // снова читаем вторую строку (теперь она должна быть в "грязной" странице)
  // и удаляем её
  num2 = fpta_value_sint(6408824736249964544);
  EXPECT_EQ(FPTA_OK, fpta_get(txn, &col_str, &num2, &row2));
  EXPECT_EQ(MDBX_RESULT_TRUE, mdbx_is_dirty(txn->mdbx_txn, row2.sys.iov_base));
  EXPECT_EQ(FPTA_OK, fpta_delete(txn, &table, row2));

  ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //--------------------------------------------------------------------------
  // освобождаем ресурсы
  fpta_name_destroy(&table);
  fpta_name_destroy(&col_num);
  fpta_name_destroy(&col_date);
  fpta_name_destroy(&col_str);
  free(pt);
  pt = nullptr;

  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

//----------------------------------------------------------------------------

TEST(Smoke, UpdateViolateUnique) {
  /* Smoke-проверка обновления строки с нарушением уникальности по
   * вторичному ключу.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей, в которой две колонки и два
   *     индекса с контролем уникальности.
   *
   *  2. Вставляем 2 строки с уникальными значениями всех полей.
   *
   *  3. Пытаемся обновить одну из строк с нарушением уникальности.
   *
   *  4. Завершаем операции и освобождаем ресурсы.
   */
  const bool skipped = GTEST_IS_EXECUTION_TIMEOUT();
  if (skipped)
    return;
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  // создаем базу
  fpta_db *db = nullptr;
  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_sync, fpta_regime_default, 0644, 1,
                         true, &db));
  ASSERT_NE(nullptr, db);

  // начинаем транзакцию с добавлениями
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  // описываем структуру таблицы и создаем её
  fpta_column_set def;
  fpta_column_set_init(&def);
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("key", fptu_int64,
                                 fpta_primary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("value", fptu_int64,
                                 fpta_secondary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));
  ASSERT_EQ(FPTA_OK, fpta_table_create(txn, "map", &def));

  // разрушаем описание таблицы
  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

  // готовим идентификаторы для манипуляций с данными
  fpta_name table, col_key, col_value;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "Map"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_key, "Key"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_value, "Value"));
  // начнём с добавления значений полей, поэтому нужен ручной refresh
  ASSERT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_key));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_value));

  // выделяем кортеж и вставляем 2 строки
  fptu_rw *pt = fptu_alloc(2, 8 * 2);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  // 1
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_key, fpta_value_sint(1)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_value, fpta_value_sint(2)));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  fptu_ro row = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(row));
  EXPECT_EQ(FPTA_OK, fpta_put(txn, &table, row, fpta_insert));

  // 2
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_key, fpta_value_sint(2)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_value, fpta_value_sint(3)));
  row = fptu_take_noshrink(pt);
  EXPECT_EQ(FPTA_OK, fpta_put(txn, &table, row, fpta_insert));

  // завершаем транзакцию вставки
  ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //--------------------------------------------------------------------------
  // начинаем транзакцию обновления
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);

  // формируем строку с нарушением
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_key, fpta_value_sint(1)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_value, fpta_value_sint(3)));
  row = fptu_take_noshrink(pt);

  // пробуем с пред-проверкой
  EXPECT_EQ(FPTA_KEYEXIST, fpta_probe_and_update_row(txn, &table, row));
  EXPECT_EQ(FPTA_KEYEXIST, fpta_probe_and_insert_row(txn, &table, row));

  // пробуем сломать уникальность, транзакция должна быть отменена
  EXPECT_EQ(FPTA_KEYEXIST, fpta_update_row(txn, &table, row));

  // транзакция должна быть уже отменена
  ASSERT_EQ(FPTA_TXN_CANCELLED, fpta_transaction_end(txn, false));
  txn = nullptr;

  //--------------------------------------------------------------------------
  // освобождаем ресурсы
  fpta_name_destroy(&table);
  fpta_name_destroy(&col_key);
  fpta_name_destroy(&col_value);
  free(pt);
  pt = nullptr;

  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

//----------------------------------------------------------------------------

class SmokeNullable : public ::testing::Test {
public:
  scoped_db_guard db_quard;
  scoped_txn_guard txn_guard;
  scoped_cursor_guard cursor_guard;
  scoped_ptrw_guard ptrw_guard;
  bool skipped;

  fpta_name table, c0_uint64, c1_date, c2_str, c3_int64, c4_uint32, c5_ip4,
      c6_sha1, c7_fp32, c8_enum, c9_fp64;

  fptu_ro MakeRow(int stepover) {
    EXPECT_EQ(FPTU_OK, fptu_clear(ptrw_guard.get()));

    if (stepover >= 0) {
      // формируем не пустую строку, со скользящим NIL
      if (stepover != 0) {
        EXPECT_EQ(FPTA_OK,
                  fpta_upsert_column(ptrw_guard.get(), &c0_uint64,
                                     fpta_value_uint((unsigned)stepover)));
      }
      if (stepover != 1) {
        EXPECT_EQ(FPTA_OK, fpta_upsert_column(ptrw_guard.get(), &c1_date,
                                              fpta_value_datetime(NOW_FINE())));
      }
      if (stepover != 2) {
        EXPECT_EQ(FPTA_OK,
                  fpta_upsert_column(ptrw_guard.get(), &c2_str,
                                     fpta_value_str(std::to_string(stepover))));
      }
      if (stepover != 3) {
        EXPECT_EQ(FPTA_OK, fpta_upsert_column(ptrw_guard.get(), &c3_int64,
                                              fpta_value_sint(-stepover)));
      }
      if (stepover != 4) {
        EXPECT_EQ(FPTA_OK,
                  fpta_upsert_column(ptrw_guard.get(), &c4_uint32,
                                     fpta_value_uint((unsigned)stepover)));
      }
      if (stepover != 5) {
        EXPECT_EQ(FPTA_OK,
                  fpta_upsert_column(ptrw_guard.get(), &c5_ip4,
                                     fpta_value_uint((unsigned)stepover + 42)));
      }
      if (stepover != 6) {
        uint8_t sha1[160 / 8];
        memset(sha1, stepover + 1, sizeof(sha1));
        EXPECT_EQ(FPTA_OK,
                  fpta_upsert_column(ptrw_guard.get(), &c6_sha1,
                                     fpta_value_binary(sha1, sizeof(sha1))));
      }
      if (stepover != 7) {
        EXPECT_EQ(FPTA_OK,
                  fpta_upsert_column(ptrw_guard.get(), &c7_fp32,
                                     fpta_value_float(stepover * M_PI)));
      }
      if (stepover != 8) {
        EXPECT_EQ(FPTA_OK, fpta_upsert_column(ptrw_guard.get(), &c8_enum,
                                              fpta_value_sint(11 + stepover)));
      }
      if (stepover != 9) {
        EXPECT_EQ(FPTA_OK,
                  fpta_upsert_column(ptrw_guard.get(), &c9_fp64,
                                     fpta_value_float(M_E * stepover)));
      }
    }

    return fptu_take_noshrink(ptrw_guard.get());
  }

  void OpenCursor(int colnum) {
    if (cursor_guard) {
      EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    }

    // выбираем колонку по номеру
    fpta_name *colptr = nullptr;
    switch (colnum) {
    case 0:
      colptr = &c0_uint64;
      break;
    case 1:
      colptr = &c1_date;
      break;
    case 2:
      colptr = &c2_str;
      break;
    case 3:
      colptr = &c3_int64;
      break;
    case 4:
      colptr = &c4_uint32;
      break;
    case 5:
      colptr = &c5_ip4;
      break;
    case 6:
      colptr = &c6_sha1;
      break;
    case 7:
      colptr = &c7_fp32;
      break;
    case 8:
      colptr = &c8_enum;
      break;
    case 9:
      colptr = &c9_fp64;
      break;
    }

    // открываем простейщий курсор: на всю таблицу, без фильтра
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK,
              fpta_cursor_open(txn_guard.get(), colptr, fpta_value_begin(),
                               fpta_value_end(), nullptr,
                               fpta_unsorted_dont_fetch, &cursor));
    cursor_guard.reset(cursor);
  }

  virtual void SetUp() {
    SCOPED_TRACE("setup");
    skipped = GTEST_IS_EXECUTION_TIMEOUT();
    if (skipped)
      return;

    // инициализируем идентификаторы таблицы и её колонок
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "xyz"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &c0_uint64, "c0_uint64"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &c1_date, "c1_date"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &c2_str, "c2_str"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &c3_int64, "c3_int64"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &c4_uint32, "c4_uint32"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &c5_ip4, "c5_ip4"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &c6_sha1, "c6_sha1"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &c7_fp32, "c7_fp32"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &c8_enum, "c8_enum"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &c9_fp64, "c9_fp64"));

    // чистим
    if (REMOVE_FILE(testdb_name) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }
    if (REMOVE_FILE(testdb_name_lck) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }

    // создаем базу
    fpta_db *db = nullptr;
    EXPECT_EQ(FPTA_SUCCESS,
              fpta_db_open(testdb_name, fpta_sync, fpta_regime_default, 0644, 1,
                           true, &db));
    ASSERT_NE(nullptr, db);
    db_quard.reset(db);

    // начинаем транзакцию с созданием таблицы
    fpta_txn *txn = (fpta_txn *)&txn;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);
    txn_guard.reset(txn);

    // описываем структуру таблицы и создаем её
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "c0_uint64", fptu_uint64,
                           fpta_primary_unique_ordered_obverse_nullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "c1_date", fptu_datetime,
                  fpta_secondary_unique_ordered_obverse_nullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "c2_str", fptu_cstr,
                  fpta_secondary_withdups_ordered_obverse_nullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "c3_int64", fptu_int64,
                  fpta_secondary_withdups_unordered_nullable_obverse, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "c4_uint32", fptu_uint32,
                  fpta_secondary_withdups_ordered_reverse_nullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "c5_ip4", fptu_uint32,
                  fpta_secondary_withdups_ordered_obverse_nullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "c6_sha1", fptu_160,
                  fpta_secondary_unique_unordered_nullable_obverse, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "c7_fp32", fptu_fp32,
                  fpta_secondary_unique_ordered_obverse_nullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "c8_enum", fptu_uint16,
                  fpta_secondary_withdups_unordered_nullable_reverse, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "c9_fp64", fptu_fp64,
                  fpta_secondary_withdups_ordered_obverse_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("_", fptu_opaque,
                                            fpta_noindex_nullable, &def));

    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));
    ASSERT_EQ(FPTA_OK, fpta_table_create(txn, "xyz", &def));

    // завершаем транзакцию
    ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
    txn = nullptr;

    // разрушаем описание таблицы
    EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
    EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

    // начинаем транзакцию изменения данных
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
    ASSERT_NE(nullptr, txn);
    txn_guard.reset(txn);

    //------------------------------------------------------------------------

    // нужен ручной refresh, так как начинать будем с добавления полей в кортеж
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &c0_uint64));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &c1_date));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &c2_str));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &c3_int64));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &c4_uint32));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &c5_ip4));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &c6_sha1));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &c7_fp32));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &c8_enum));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &c9_fp64));

    // выделяем кортеж
    fptu_rw *pt = fptu_alloc(10, 8 * 10 + 42);
    ASSERT_NE(nullptr, pt);
    ASSERT_STREQ(nullptr, fptu_check(pt));
    ptrw_guard.reset(pt);
  }

  virtual void TearDown() {
    if (skipped)
      return;
    SCOPED_TRACE("teardown");

    // разрушаем привязанные идентификаторы
    fpta_name_destroy(&table);
    fpta_name_destroy(&c0_uint64);
    fpta_name_destroy(&c1_date);
    fpta_name_destroy(&c2_str);
    fpta_name_destroy(&c3_int64);
    fpta_name_destroy(&c4_uint32);
    fpta_name_destroy(&c5_ip4);
    fpta_name_destroy(&c6_sha1);
    fpta_name_destroy(&c7_fp32);
    fpta_name_destroy(&c8_enum);
    fpta_name_destroy(&c9_fp64);

    // закрываем курсор и завершаем транзакцию
    if (cursor_guard) {
      EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    }
    if (txn_guard) {
      ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), true));
    }
    if (db_quard) {
      // закрываем и удаляем базу
      ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db_quard.release()));
      ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
      ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
    }
  }
};

TEST_F(SmokeNullable, AllNILs) {
  /* Smoke-проверка обновления строки с нарушением уникальности по
   * вторичному ключу.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей, в которой 10 колонок, все они
   *     индексированы и допускают NIL. При этом 5 колонок с контролем
   *     уникальности, а остальные допускают дубликаты.
   *
   *  2. Вставляем строку, в которой только одни NIL-ы.
   *
   *  3. Удаляем вставленную строку.
   *
   *  4. Снова вставляем строку и удаляем её через через курсор.
   *
   *  5. Повторяем пункт 4 для курсора по каждой колонке.
   */
  if (skipped)
    return;

  // формируем строку без колонок
  fptu_ro allNILs = MakeRow(-1);

  // вставляем строку со всеми NIL
  EXPECT_EQ(FPTA_OK,
            fpta_validate_insert_row(txn_guard.get(), &table, allNILs));
  EXPECT_EQ(FPTA_OK, fpta_insert_row(txn_guard.get(), &table, allNILs));
  EXPECT_EQ(FPTA_KEYEXIST,
            fpta_validate_insert_row(txn_guard.get(), &table, allNILs));

  // обновляем строку без реального изменения данных
  EXPECT_EQ(FPTA_OK,
            fpta_validate_upsert_row(txn_guard.get(), &table, allNILs));
  EXPECT_EQ(FPTA_OK, fpta_upsert_row(txn_guard.get(), &table, allNILs));

  // удяляем строку со всеми нулями
  EXPECT_EQ(FPTA_OK, fpta_delete(txn_guard.get(), &table, allNILs));

  // теперь вставляем строку через upsert
  EXPECT_EQ(FPTA_OK,
            fpta_validate_upsert_row(txn_guard.get(), &table, allNILs));
  EXPECT_EQ(FPTA_OK, fpta_upsert_row(txn_guard.get(), &table, allNILs));

  // повторяем что дубликат не лезет
  EXPECT_EQ(FPTA_KEYEXIST,
            fpta_validate_insert_row(txn_guard.get(), &table, allNILs));

  //--------------------------------------------------------------------------
  /* через курсор */
  for (int colnum = 0; colnum < 10; ++colnum) {
    SCOPED_TRACE("cursor column #" + std::to_string(colnum));
    OpenCursor(colnum);
    ASSERT_TRUE(cursor_guard.operator bool());

    EXPECT_EQ(FPTA_OK, fpta_upsert_row(txn_guard.get(), &table, allNILs));
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor_guard.get(),
                                        (colnum & 1) ? fpta_first : fpta_last));
    EXPECT_EQ(FPTA_OK, fpta_cursor_delete(cursor_guard.get()));
    EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor_guard.get()));
  }
}

TEST_F(SmokeNullable, Base) {
  /* Smoke-проверка обновления строки с нарушением уникальности по
   * вторичному ключу.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей, в которой 10 колонок, все они
   *     индексированы и допускают NIL. При этом 5 колонок с контролем
   *     уникальности, а остальные допускают дубликаты.
   *
   *  2. Вставляем 10 строк со "скользящим" NIL и уникальными
   *     значениям в остальных полях.
   *
   *  3. Удаляем 10 строк через курсор открываемый по каждой из колонок.
   *
   *  4. Добавляем и удаляем полностью заполненную строку.
   */
  if (skipped)
    return;

  //--------------------------------------------------------------------------
  for (int nilcol = 0; nilcol < 10; ++nilcol) {
    SCOPED_TRACE("NIL-column #" + std::to_string(nilcol));
    fptu_ro row = MakeRow(nilcol);
    EXPECT_EQ(FPTA_OK, fpta_upsert_row(txn_guard.get(), &table, row));

    // проверяем обновлени (без какого-либо зименения данных)
    EXPECT_EQ(FPTA_OK, fpta_probe_and_update_row(txn_guard.get(), &table, row));
    EXPECT_EQ(FPTA_OK, fpta_probe_and_upsert_row(txn_guard.get(), &table, row));

    // повторяем что дубликат не лезет
    EXPECT_EQ(FPTA_KEYEXIST,
              fpta_validate_insert_row(txn_guard.get(), &table, row));
  }

  // проверяем что не лезет строка со всеми NIL
  EXPECT_EQ(FPTA_KEYEXIST,
            fpta_validate_insert_row(txn_guard.get(), &table, MakeRow(-1)));

  // удялем по одной строке через курсор открываемый по каждой из колонок
  for (int colnum = 0; colnum < 10; ++colnum) {
    SCOPED_TRACE("cursor column #" + std::to_string(colnum));
    OpenCursor(colnum);
    ASSERT_TRUE(cursor_guard.operator bool());

    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor_guard.get(),
                                        (colnum & 1) ? fpta_first : fpta_last));
    EXPECT_EQ(FPTA_OK, fpta_cursor_delete(cursor_guard.get()));
  }

  // вставляем и удаляем полностью заполненную строку (без NIL)/
  fptu_ro row = MakeRow(11);
  EXPECT_EQ(FPTA_OK, fpta_upsert_row(txn_guard.get(), &table, row));
  EXPECT_EQ(FPTA_OK, fpta_delete(txn_guard.get(), &table, row));
}

//----------------------------------------------------------------------------

TEST(Smoke, ReOpenAfterAbort) {
  const bool skipped = GTEST_IS_EXECUTION_TIMEOUT();
  if (skipped)
    return;

  // чистим
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  // открываем/создаем базу
  fpta_db *db = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  0644, 1, true, &db));
  ASSERT_NE(db, (fpta_db *)nullptr);

  // описываем простейшую таблицу с одним PK (int64) и колонками (_last_changed,
  // fp64, int64, string, datetime)
  fpta_column_set def;
  fpta_column_set_init(&def);

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("host", fptu_cstr,
                                 fpta_primary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "_last_changed", fptu_datetime,
                         fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("_id", fptu_int64,
                                 fpta_secondary_unique_unordered, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe("user_name", fptu_cstr,
                                          fpta_noindex_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe("date", fptu_datetime,
                                          fpta_noindex_nullable, &def));

  ASSERT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  // запускам транзакцию и создаем таблицу с обозначенным набором колонок
  fpta_txn *txn = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE((fpta_txn *)nullptr, txn);
  ASSERT_EQ(FPTA_OK, fpta_table_create(txn, "Table", &def));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // закрываем базу
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  db = nullptr;

  // открываем базу
  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  0644, 1, false, &db));
  ASSERT_NE(db, (fpta_db *)nullptr);

  fpta_name table_id;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table_id, "Table"));

  // открываем транзакцию на запись, позже мы ее абортируем
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE((fpta_txn *)nullptr, txn);
  size_t row_count = 0;
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table_id, &row_count, nullptr));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, true));

  // открываем еще одну транзакцию на запись
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE((fpta_txn *)nullptr, txn);

  // пытаемся сделать поиск
  fpta_name column_id;
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table_id, &column_id, "host"));

  fpta_value value = fpta_value_cstr("administrator");
  fptu_ro record;
  memset(&record, 0, sizeof(record));

  EXPECT_EQ(FPTA_NOTFOUND, fpta_get(txn, &column_id, &value, &record));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, true));

  // закрываем базу
  fpta_name_destroy(&table_id);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
}

//----------------------------------------------------------------------------

TEST(Smoke, Kamerades) {
  /* Smoke-проверка совместных операций.
   *
   * Сценарий:
   *  1. Открываем базу "коррелятором".
   *  2. Открываем базу "коммандером", создаём одну таблицу,
   *     в которой одна колонка и один (primary) индекс.
   *  3. В "корреляторе" добавляем в эту таблицу одну запись.
   *  4. В "коммандере" получаем сведения о таблице.
   *  5. Завершаем операции и освобождаем ресурсы.
   */
  const bool skipped = GTEST_IS_EXECUTION_TIMEOUT();
  if (skipped)
    return;

  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  fpta_db *correlator_db = nullptr;
  fpta_db *commander_db = nullptr;

  {
    // открываем/создаем базульку в 1 мегабайт
    EXPECT_EQ(FPTA_SUCCESS,
              fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644, 1,
                           true,
                           &commander_db)); // таблица создаётся из "коммандера"
    ASSERT_NE(nullptr, commander_db);

    // описываем простейшую таблицу с одной колонкой
    fpta_column_set def;
    fpta_column_set_init(&def);

    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("nnn", fptu_int64,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    // запускам транзакцию и создаем таблицу
    fpta_txn *txn = (fpta_txn *)&txn;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(commander_db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);
    EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_1", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    txn = nullptr;

    // закрываем из коммандера (опционально)
    if (false) {
      EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(commander_db));
      commander_db = nullptr;
    }
  }

  {
    // создаем кортеж, который станет единственной записью в таблице
    fptu_rw *pt1 = fptu_alloc(1, 8);
    ASSERT_NE(nullptr, pt1);
    ASSERT_STREQ(nullptr, fptu_check(pt1));

    // инициализируем идентификаторы таблицы
    fpta_name table, col_pk;
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table_1"));

    // открываем из коррелятора
    EXPECT_EQ(FPTA_SUCCESS,
              fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644, 1,
                           false, &correlator_db));
    ASSERT_NE(nullptr, correlator_db);

    // начинаем транзакцию для вставки данных
    fpta_txn *txn = (fpta_txn *)&txn;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(correlator_db, fpta_write, &txn));

    // вставляем запись из "коррелятора"
    ASSERT_NE(nullptr, txn);
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_pk, "nnn"));
    ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));
    // ради теста делаем привязку вручную
    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_pk));

    // добавляем нормальные значения
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_pk, fpta_value_sint(567)));
    ASSERT_STREQ(nullptr, fptu_check(pt1));
    fptu_ro taken_noshrink;
    taken_noshrink = fptu_take_noshrink(pt1);
    EXPECT_EQ(FPTA_OK,
              fpta_validate_put(txn, &table, taken_noshrink, fpta_insert));
    EXPECT_EQ(FPTA_OK, fpta_put(txn, &table, taken_noshrink, fpta_insert));
    free(pt1);
    pt1 = nullptr;

    // фиксируем изменения из коррелятора
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    txn = nullptr;

    // разрушаем привязанные идентификаторы
    fpta_name_destroy(&col_pk);
    fpta_name_destroy(&table);

    // закрываем из коррелятора (опционально)
    if (false) {
      EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(correlator_db));
      correlator_db = nullptr;
    }
  }

  {
    // инициализируем идентификаторы таблицы со стороны "коммандера"
    fpta_name same_table;

    EXPECT_EQ(FPTA_OK, fpta_table_init(&same_table, "table_1"));

    if (!commander_db) {
      // вновь открываем из коммандера
      EXPECT_EQ(
          FPTA_SUCCESS,
          fpta_db_open(
              testdb_name, fpta_weak, fpta_regime_default, 0644, 1, false,
              &commander_db)); // теперь пытаемся только читать из "коммандера"
    }

    // и начинаем читающую транзакцию из "коммандера"
    fpta_txn *txn = (fpta_txn *)&txn;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(commander_db, fpta_read, &txn));
    ASSERT_NE(nullptr, txn);
    EXPECT_EQ(FPTA_OK,
              fpta_name_refresh(txn, &same_table)); // здесь было MDBX_CORRUPTED

    size_t num;
    EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &same_table, &num, NULL));
    EXPECT_EQ(num, 1u);

    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    txn = nullptr;

    // разрушаем привязанные идентификаторы
    fpta_name_destroy(&same_table);
  }

  // закрываем базульку из коррелятора
  if (correlator_db) {
    EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(correlator_db));
    correlator_db = nullptr;
  }

  // закрываем базульку из коммандера
  if (commander_db) {
    EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(commander_db));
    commander_db = nullptr;
  }

  // пока не удялем файлы чтобы можно было посмотреть и натравить mdbx_chk
  if (false) {
    if (REMOVE_FILE(testdb_name) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }
    if (REMOVE_FILE(testdb_name_lck) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }
  }
}

//----------------------------------------------------------------------------

TEST(Smoke, OverchargeOnCommit) {
  /* Smoke-проверка поведения при переполнении БД во время фиксации транзакции.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей и некоторым кол-вом колонок.
   *
   *  2. Итеративно вставляем по одной строке за транзакцию,
   *     пока не закончится место или не случится еще что-то плохое.
   *
   *  3. Параметры подобраны так, чтобы переполнение случилось при фиксации
   *     транзакции (при добавлении записи в garbage-таблицу  внутри libmdbx).
   */
  const bool skipped = GTEST_IS_EXECUTION_TIMEOUT();
  if (skipped)
    return;
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  // открываем/создаем базу
  fpta_db *db = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  0664, 1, true, &db));
  ASSERT_NE(db, (fpta_db *)nullptr);

  // описываем простейшую таблицу с одним PK
  fpta_column_set def;
  fpta_column_set_init(&def);

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("primary_key", fptu_uint64,
                                 fpta_primary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe("user_name", fptu_cstr,
                                          fpta_noindex_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe("date", fptu_datetime,
                                          fpta_noindex_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe("host", fptu_cstr,
                                          fpta_noindex_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "_last_changed", fptu_datetime,
                         fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("_id", fptu_uint64,
                                 fpta_secondary_unique_ordered_obverse, &def));

  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  // запускам транзакцию и создаем таблицу с обозначенным набором колонок
  fpta_txn *txn = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE((fpta_txn *)nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "Table", &def));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // закрываем базу
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  db = nullptr;

  // открываем базу
  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  0664, 1, false, &db));
  ASSERT_NE(db, (fpta_db *)nullptr);

  fpta_name table_id, primary_key, host, id, last_changed, name, date;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table_id, "Table"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table_id, &primary_key, "primary_key"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table_id, &host, "host"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table_id, &id, "_id"));
  EXPECT_EQ(FPTA_OK,
            fpta_column_init(&table_id, &last_changed, "_last_changed"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table_id, &name, "user_name"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table_id, &date, "date"));

  fptu_rw *tuple = fptu_alloc(6, 1000);
  ASSERT_NE(tuple, (fptu_rw *)nullptr);
  scoped_ptrw_guard ptrw_guard;
  ptrw_guard.reset(tuple);

  int err = FPTA_OK;
  for (uint64_t pk = 0; err == FPTA_OK; ++pk) {
    // открываем транзакцию на запись, записываем данные
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
    ASSERT_NE((fpta_txn *)nullptr, txn);

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table_id, &primary_key));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table_id, &host));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table_id, &id));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table_id, &last_changed));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table_id, &name));
    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table_id, &date));

    const auto now = fpta_value_datetime(fptu_now_coarse());
    fptu_clear(tuple);
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(tuple, &primary_key, fpta_value_uint(pk)));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &date, now));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                           tuple, &name,
                           fpta_value_cstr("qa-kolobok.mpqa.OoCa5Qua.ru")));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &host,
                                          fpta_value_cstr("administrator")));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &id, fpta_value_uint(pk)));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &last_changed, now));

    err = fpta_probe_and_upsert_row(txn, &table_id, fptu_take(tuple));
    EXPECT_EQ(FPTA_OK, err);

    if (err != FPTA_OK) {
      // отменяем если была ошибка
      ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, true));
    } else {
      // коммитим и ожидаем ошибку переполнения здесь
      err = fpta_transaction_end(txn, false);
      if (err != FPTA_OK) {
        ASSERT_EQ(FPTA_DB_FULL, err);
      }
    }
  }

  fpta_name_destroy(&host);
  fpta_name_destroy(&id);
  fpta_name_destroy(&last_changed);
  fpta_name_destroy(&table_id);
  fpta_name_destroy(&name);
  fpta_name_destroy(&date);
  fpta_name_destroy(&primary_key);

  // закрываем базу
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
}

//----------------------------------------------------------------------------

TEST(Smoke, AsyncSchemaChange) {
  /* Smoke-проверка поведения при асинхронном изменении схемы.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей и некоторым кол-вом колонок.
   *
   *  2. Вставляем данные из контекста "коррелятора" для проверки
   *     что с таблицей все хорошо.
   *
   *  3. Параллельно открываем базу в контексте "командера" и изменяем
   *     схему таблицы.
   *
   *  4. Еще раз вставляем данные из контекста "коррелятора".
   */
  const bool skipped = GTEST_IS_EXECUTION_TIMEOUT();
  if (skipped)
    return;

  // создаем исходную базу
  {
    // чистим
    if (REMOVE_FILE(testdb_name) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }
    if (REMOVE_FILE(testdb_name_lck) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }

    fpta_db *db = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                    0644, 1, true, &db));
    ASSERT_NE(db, (fpta_db *)nullptr);

    // описываем простейшую таблицу с одним PK (int64) и колонками
    // (_last_changed, fp64, int64, string, datetime)
    fpta_column_set def1;
    fpta_column_set_init(&def1);

    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("host", fptu_cstr,
                                   fpta_primary_unique_ordered_obverse_nullable,
                                   &def1));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "_last_changed", fptu_datetime,
                  fpta_secondary_withdups_ordered_obverse_nullable, &def1));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "_id", fptu_int64,
                  fpta_secondary_unique_ordered_obverse_nullable, &def1));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("user", fptu_cstr,
                                            fpta_noindex_nullable, &def1));

    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def1));

    // запускам транзакцию и создаем таблицу с обозначенным набором колонок
    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_NE((fpta_txn *)nullptr, txn);

    EXPECT_EQ(FPTA_OK, fpta_table_create(
                           txn, "Success_bruteforce_on_host_table", &def1));

    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    txn = nullptr;

    // закрываем базу
    EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  }

  // открываем базу в "корреляторе"
  fpta_db *db_correlator = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  0644, 1, false, &db_correlator));

  fpta_txn *txn_correlator = nullptr;
  fpta_name table_id_, host, last, id, user;

  fptu_rw *tuple = fptu_alloc(4, 1000);
  ASSERT_NE(tuple, (fptu_rw *)nullptr);
  scoped_ptrw_guard ptrw_guard;
  ptrw_guard.reset(tuple);

  // выполняем пробное обновление в кореляторе
  {
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_correlator, fpta_write,
                                              &txn_correlator));

    EXPECT_EQ(FPTA_OK,
              fpta_table_init(&table_id_, "Success_bruteforce_on_host_table"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table_id_, &host, "host"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table_id_, &last, "_last_changed"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table_id_, &id, "_id"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table_id_, &user, "user"));

    EXPECT_EQ(FPTA_OK,
              fpta_name_refresh_couple(txn_correlator, &table_id_, &host));
    EXPECT_EQ(FPTA_OK,
              fpta_name_refresh_couple(txn_correlator, &table_id_, &last));
    EXPECT_EQ(FPTA_OK,
              fpta_name_refresh_couple(txn_correlator, &table_id_, &id));
    EXPECT_EQ(FPTA_OK,
              fpta_name_refresh_couple(txn_correlator, &table_id_, &user));

    EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                           tuple, &host,
                           fpta_value_cstr("qa-kolobok.mpqa.OoCa5Qua.ru")));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                           tuple, &last, fpta_value_datetime(fptu_now_fine())));
    uint64_t seq = 0;
    EXPECT_EQ(FPTA_OK,
              fpta_table_sequence(txn_correlator, &table_id_, &seq, 1));

    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(tuple, &id, fpta_value_sint((int64_t)seq)));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &user,
                                          fpta_value_cstr("Administrator")));

    fpta_value value = fpta_value_cstr("qa-kolobok.mpqa.OoCa5Qua.ru");
    fptu_ro record;
    EXPECT_EQ(FPTA_NOTFOUND, fpta_get(txn_correlator, &host, &value, &record));
    EXPECT_EQ(FPTA_OK, fpta_probe_and_upsert_row(txn_correlator, &table_id_,
                                                 fptu_take(tuple)));

    fptu_clear(tuple);

#if 0 /* лишние телодвижения */
    fpta_name_destroy(&table_id_);
    fpta_name_destroy(&host);
    fpta_name_destroy(&last);
    fpta_name_destroy(&id);
    fpta_name_destroy(&user);
#endif

    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_correlator, false));
    txn_correlator = nullptr;
  }

  // изменяем схему в "коммандоре"
  {
    // открываем базу в "командоре"
    fpta_db *db_commander = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                    0644, 1, true, &db_commander));

    fpta_txn *txn_commander = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_commander, fpta_schema,
                                              &txn_commander));
    ASSERT_NE((fpta_txn *)nullptr, txn_commander);

    // удаляем существующую таблицу
    EXPECT_EQ(FPTA_OK, fpta_table_drop(txn_commander,
                                       "Success_bruteforce_on_host_table"));

#if 0 /* лишние телодвижения */
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_commander, false));
  txn_commander = nullptr;

  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db_commander));
  db_commander = nullptr;

  EXPECT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644, 1, true,
                                  &db_commander));
  EXPECT_EQ(FPTA_OK,
            fpta_transaction_begin(db_commander, fpta_schema, &txn_commander));
  ASSERT_NE((fpta_txn *)nullptr, txn_commander);
#endif

    // описываем новую структуру таблицы
    fpta_column_set def1;
    fpta_column_set_init(&def1);

    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("host", fptu_cstr,
                                   fpta_primary_unique_ordered_obverse_nullable,
                                   &def1));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "_id", fptu_int64,
                  fpta_secondary_unique_ordered_obverse_nullable, &def1));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "_last_changed", fptu_datetime,
                  fpta_secondary_withdups_ordered_obverse_nullable, &def1));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("user", fptu_cstr,
                                            fpta_noindex_nullable, &def1));

    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def1));

    // создаем новую таблицу
    EXPECT_EQ(FPTA_OK,
              fpta_table_create(txn_commander,
                                "Success_bruteforce_on_host_table", &def1));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_commander, false));
    EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db_commander));
  }

  // выполняем контрольное обновление данных после изменения схемы
  {
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db_correlator, fpta_write,
                                              &txn_correlator));
#if 0 /* лишние телодвижения */
    fpta_name table_id_, host, last, id, user;
    EXPECT_EQ(FPTA_OK,
              fpta_table_init(&table_id_, "Success_bruteforce_on_host_table"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table_id_, &host, "host"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table_id_, &last, "_last_changed"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table_id_, &id, "_id"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table_id_, &user, "user"));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn_correlator, &table_id_));
#endif

    EXPECT_EQ(FPTA_OK,
              fpta_name_refresh_couple(txn_correlator, &table_id_, &host));
    EXPECT_EQ(FPTA_OK,
              fpta_name_refresh_couple(txn_correlator, &table_id_, &last));
    EXPECT_EQ(FPTA_OK,
              fpta_name_refresh_couple(txn_correlator, &table_id_, &id));
    EXPECT_EQ(FPTA_OK,
              fpta_name_refresh_couple(txn_correlator, &table_id_, &user));

    EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                           tuple, &host,
                           fpta_value_cstr("qa-kolobok.mpqa.OoCa5Qua.ru")));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                           tuple, &last, fpta_value_datetime(fptu_now_fine())));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &id, fpta_value_sint(0)));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &user,
                                          fpta_value_cstr("Administrator")));

    fpta_value value = fpta_value_cstr("qa-kolobok.mpqa.OoCa5Qua.ru");
    fptu_ro record;
    EXPECT_EQ(FPTA_NOTFOUND, fpta_get(txn_correlator, &host, &value, &record));
    EXPECT_EQ(FPTA_OK, fpta_probe_and_upsert_row(txn_correlator, &table_id_,
                                                 fptu_take(tuple)));

    fptu_clear(tuple);
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_correlator, false));
  }

  fpta_name_destroy(&host);
  fpta_name_destroy(&last);
  fpta_name_destroy(&id);
  fpta_name_destroy(&user);
  fpta_name_destroy(&table_id_);

  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db_correlator));
}

//----------------------------------------------------------------------------

TEST(Smoke, FilterAndRange) {
  /* Smoke-проверка перемещения курсора с заданием диапазона и фильтра
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей и достаточным набором колонок.
   *
   *  2. Вставляем одну строку.
   *
   *  3. Открываем курсор и перемещаем его к первой подходящей записи.
   *     Проверяем для сортировки по-возрастанию и по-убыванию.
   *
   *  4. Освобождаем ресурсы.
   */
  const bool skipped = GTEST_IS_EXECUTION_TIMEOUT();
  if (skipped)
    return;

  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  // создаем базу
  fpta_db *db = nullptr;
  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_sync, fpta_regime_default, 0644, 1,
                         true, &db));
  ASSERT_NE(nullptr, db);

  // начинаем транзакцию с добавлениями
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  // описываем структуру таблицы и создаем её
  fpta_column_set def;
  fpta_column_set_init(&def);
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("int_column", fptu_int64,
                                 fpta_primary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "datetime_column", fptu_datetime,
                         fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("_id", fptu_int64,
                                 fpta_secondary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));
  ASSERT_EQ(FPTA_OK, fpta_table_create(txn, "bugged", &def));

  // разрушаем описание таблицы
  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

  // готовим идентификаторы для манипуляций с данными
  fpta_name table, col_num, col_date, col_str;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "bugged"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_num, "int_column"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_date, "datetime_column"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_str, "_id"));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_num));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_date));
  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_str));

  // выделяем кортеж и вставляем строку
  fptu_rw *pt = fptu_alloc(3, 8 + 8 + 8);
  ASSERT_NE(nullptr, pt);
  ASSERT_STREQ(nullptr, fptu_check(pt));

  fptu_time datetime;
  datetime.fixedpoint = 1492170771;
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_num, fpta_value_sint(16)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_date, fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_str,
                                        fpta_value_sint(6408824664381050880)));
  ASSERT_STREQ(nullptr, fptu_check(pt));
  fptu_ro row = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu_check_ro(row));
  EXPECT_EQ(FPTA_OK, fpta_put(txn, &table, row, fpta_insert));

  // завершаем транзакцию вставки
  ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //--------------------------------------------------------------------------
  // начинаем транзакцию чтения
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);

  // создаём фильтр
  fpta_filter my_filter;
  my_filter.type = fpta_node_gt;

  my_filter.node_cmp.left_id = &col_num;
  my_filter.node_cmp.right_value = fpta_value_sint(15);

  fptu_time datetime2;
  datetime2.fixedpoint = 1492170700;

  // открываем курсор с диапазоном и фильтром, и сортировкой по-убыванию
  fpta_cursor *cursor;
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn, &col_date, fpta_value_datetime(datetime2),
                             fpta_value_end(), &my_filter,
                             fpta_descending_dont_fetch, &cursor));
  // перемещаем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  // закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));

  // открываем курсор с диапазоном и фильтром, и сортировкой по-возрастанию
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn, &col_date, fpta_value_datetime(datetime2),
                             fpta_value_end(), &my_filter,
                             fpta_ascending_dont_fetch, &cursor));
  // перемещаем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  // закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));

  // завершаем транзакцию с чтением
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //--------------------------------------------------------------------------
  // освобождаем ресурсы

  fpta_name_destroy(&table);
  fpta_name_destroy(&col_num);
  fpta_name_destroy(&col_date);
  fpta_name_destroy(&col_str);
  free(pt);
  pt = nullptr;
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

//----------------------------------------------------------------------------

TEST(SmokeIndex, MissingFieldOfCompositeKey) {
  /* Тривиальный тест вставки NULL значения в nullable колонку, для которой
   * присутствует составная не-nullable
   *
   * Сценарий:
   *  - создаем/инициализируем описание колонок.
   *  - пробуем добавить кортеж без записи
   */
  const bool skipped = GTEST_IS_EXECUTION_TIMEOUT();
  if (skipped)
    return;

  fpta_txn *txn = (fpta_txn *)&txn;
  fpta_db *db = nullptr;

  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  // открываем/создаем базульку в 1 мегабайт

  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644, 1,
                         true, &db));
  ASSERT_NE(nullptr, db);

  // описываем простейшую таблицу с тремя колонками и одним PK
  fpta_column_set def;
  fpta_column_set_init(&def);

  EXPECT_EQ(FPTA_OK, fpta_column_describe("some_field", fptu_cstr,
                                          fpta_noindex_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe("name", fptu_cstr,
                                          fpta_noindex_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe("age", fptu_cstr,
                                          fpta_noindex_nullable, &def));

  const char *const composite_names[2] = {"some_field", "name"};
  EXPECT_EQ(FPTA_OK, fpta_describe_composite_index(
                         "mycomposite", fpta_primary_unique_ordered_obverse,
                         &def, composite_names, 2));

  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "some_table", &def));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // инициализируем идентификаторы таблицы и её колонок
  fpta_name some_field, age, table;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "some_table"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &age, "age"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &some_field, "some_field"));

  // начинаем транзакцию для вставки данных
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);
  // ради теста делаем привязку вручную
  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &some_field));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &some_field));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &age));

  // создаем кортеж, который должен быть вставлен в таблицу
  fptu_rw *pt1 = fptu_alloc(3, 1000);
  ASSERT_NE(nullptr, pt1);
  ASSERT_STREQ(nullptr, fptu_check(pt1));

  // добавляем нормальные значения
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &some_field,
                                        fpta_value_cstr("composite_part_1")));
  // пропускаем вставку значения в одну из входящих в mycomposite колонок
  // EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_a, fpta_value_sint(34)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt1, &age, fpta_value_cstr("some data")));
  ASSERT_STREQ(nullptr, fptu_check(pt1));

  // вставляем
  EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt1)));

  // фиксируем изменения
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));

  // разрушаем привязанные идентификаторы
  fpta_name_destroy(&table);
  fpta_name_destroy(&some_field);
  fpta_name_destroy(&age);
  // закрываем базульку
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  db = nullptr;

  // пока не удялем файлы чтобы можно было посмотреть и натравить mdbx_chk
  if (false) {
    ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
    ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
  }
}

//----------------------------------------------------------------------------

static std::string random_string(unsigned len) {
  static std::string alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
  std::string result;
  for (unsigned i = 0; i < len; ++i)
    result.push_back(alphabet[rand() % alphabet.length()]);
  return result;
}

TEST(Smoke, Migration) {
  /* Smoke-проверка сценария миграции с уменьшением размера БД.
   *
   * Сценарий:
   *  1. Создаем базу "коммандером", в которой одна таблица
   *     с тремя индексированными колонками.
   *  2. Открываем базу "коррелятором" и за 1000 транзакций
   *     добавляем 1000 записей, сразу закрываем базу.
   *  3. В "коммандере" обновляем схему и данные в одной транзакции:
   *      - сначала получаем и сверяем сведения о таблице;
   *      - удаляем таблицу, создаем новую с одной колонкой;
   *      - вставляем 1111 записей;
   *      - до завершения транзакции снова открываем базу "коррелятором",
   *      - коммитим транзакцию;
   *  4. В "корреляторе" стартуем транзакцию и получаем сведения о таблице.
   *  5. Закрываем БД в "коммандере", затем переоткрываем в "корреляторе"
   *     и еще раз получаем сведения о таблице.
   *  6. Завершаем операции и освобождаем ресурсы.
   */
  const bool skipped = GTEST_IS_EXECUTION_TIMEOUT();
  if (skipped)
    return;

  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  fpta_db *correlator_db = nullptr;
  fpta_db *commander_db = nullptr;

  // из "коммандера" создаем базу и таблицу
  {
    // создаем базу в 16 мегабайт
    EXPECT_EQ(FPTA_SUCCESS,
              fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644,
                           16, true, &commander_db));
    ASSERT_NE(nullptr, commander_db);

    // описываем таблицу с тремя колонками
    fpta_column_set def;
    fpta_column_set_init(&def);

    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("x", fptu_int64,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "y", fptu_int64,
                  fpta_secondary_withdups_ordered_obverse_nullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "z", fptu_cstr,
                  fpta_secondary_withdups_ordered_reverse_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    // запускам транзакцию и создаем таблицу с обозначенным набором колонок
    fpta_txn *txn = (fpta_txn *)&txn;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(commander_db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);
    EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    txn = nullptr;

    // закрываем в коммандере
    ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(commander_db));
    commander_db = nullptr;
  }

  // из "коррелятора" вставляем 1000 записей по одной в транзакции
  {
    // создаем кортеж для вставки записей
    fptu_rw *pt1 = fptu_alloc(3, 2048);
    ASSERT_NE(nullptr, pt1);
    ASSERT_STREQ(nullptr, fptu_check(pt1));

    // инициализируем идентификаторы
    fpta_name table, col_x, col_y, col_z;
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_x, "x"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_y, "y"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_z, "z"));

    // открываем из коррелятора
    EXPECT_EQ(FPTA_SUCCESS,
              fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644,
                           16, false, &correlator_db));
    ASSERT_NE(nullptr, correlator_db);

    for (unsigned n = 0; n < 1000; ++n) {
      SCOPED_TRACE("txn/record #" + std::to_string(n));

      // начинаем транзакцию для вставки данных
      fpta_txn *txn = (fpta_txn *)&txn;
      EXPECT_EQ(FPTA_OK,
                fpta_transaction_begin(correlator_db, fpta_write, &txn));

      ASSERT_NE(nullptr, txn);
      ASSERT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_x));
      ASSERT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_y));
      ASSERT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_z));

      // добавляем значения
      EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_x, fpta_value_sint(n)));
      EXPECT_EQ(FPTA_OK,
                fpta_upsert_column(pt1, &col_y, fpta_value_uint(n % 42)));
      std::string str = random_string(257u + n);
      EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_z, fpta_value_str(str)));

      // вставляем запись
      ASSERT_STREQ(nullptr, fptu_check(pt1));
      fptu_ro taken_noshrink;
      taken_noshrink = fptu_take_noshrink(pt1);
      ASSERT_EQ(FPTA_OK, fpta_put(txn, &table, taken_noshrink, fpta_insert));
      ASSERT_EQ(FPTA_OK, fptu_clear(pt1));

      // фиксируем изменения из коррелятора
      EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
      txn = nullptr;
    }

    // освобождаем кортеж
    free(pt1);
    pt1 = nullptr;

    // разрушаем привязанные идентификаторы
    fpta_name_destroy(&col_x);
    fpta_name_destroy(&col_y);
    fpta_name_destroy(&col_z);
    fpta_name_destroy(&table);

    // закрываем в корреляторе
    ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(correlator_db));
    correlator_db = nullptr;
  }

  // из "коммандера" в одной транзакции обновляем схему и данные
  {
    // инициализируем идентификаторы таблицы со стороны "коммандера"
    fpta_name table;

    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));

    // вновь открываем из коммандера
    EXPECT_EQ(FPTA_SUCCESS,
              fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644,
                           16, true, &commander_db));

    // начинаем "толстую" транзакцию из "коммандера"
    fpta_txn *txn = (fpta_txn *)&txn;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(commander_db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));
    // сверяем кол-во записей
    size_t num;
    EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table, &num, NULL));
    EXPECT_EQ(num, 1000u);
    // удаляем таблицу
    EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "table"));

    // создаем таблицу с двумя колонками
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("a", fptu_int64,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "b", fptu_int64,
                  fpta_secondary_withdups_ordered_obverse_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));

    // инициализируем идентификаторы
    fpta_name col_a, col_b;
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_a, "a"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_b, "b"));
    ASSERT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_a));
    ASSERT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_b));

    // создаем кортеж для вставки записей
    fptu_rw *pt1 = fptu_alloc(2, 42);
    ASSERT_NE(nullptr, pt1);
    ASSERT_STREQ(nullptr, fptu_check(pt1));

    for (unsigned n = 0; n < 1111; ++n) {
      SCOPED_TRACE("record #" + std::to_string(n));
      // добавляем значения
      EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_a, fpta_value_sint(n)));
      if (n & 1) {
        EXPECT_EQ(FPTA_OK,
                  fpta_upsert_column(pt1, &col_b, fpta_value_uint(n + 10000)));
      }

      // вставляем запись
      ASSERT_STREQ(nullptr, fptu_check(pt1));
      fptu_ro taken_noshrink = fptu_take_noshrink(pt1);
      ASSERT_EQ(FPTA_OK, fpta_put(txn, &table, taken_noshrink, fpta_insert));
      ASSERT_EQ(FPTA_OK, fptu_clear(pt1));
    }

    // до завершения транзакции снова открываем базу в "корреляторе"
    ASSERT_EQ(FPTA_SUCCESS,
              fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644,
                           16, false, &correlator_db));
    ASSERT_NE(nullptr, correlator_db);

    // фиксируем транзакцию
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    txn = nullptr;

    // освобождаем кортеж
    free(pt1);
    pt1 = nullptr;

    // разрушаем привязанные идентификаторы
    fpta_name_destroy(&col_a);
    fpta_name_destroy(&col_b);
    fpta_name_destroy(&table);
  }

  // В "корреляторе" стартуем транзакцию и получаем сведения о таблице
  {
    fpta_txn *txn = (fpta_txn *)&txn;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(correlator_db, fpta_read, &txn));
    ASSERT_NE(nullptr, txn);

    // инициализируем идентификатор таблицы
    fpta_name table;
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));

    // сверяем кол-во записей
    size_t num;
    EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table, &num, NULL));
    EXPECT_EQ(num, 1111u);

    // завершает транзакцию коррелятора
    ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    txn = nullptr;

    // разрушаем идентификатор
    fpta_name_destroy(&table);
  }

  // закрываем базу в коммандере
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(commander_db));
  commander_db = nullptr;

  // переоткрываем базу в корреляторе
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(correlator_db));
  correlator_db = nullptr;
  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644, 16,
                         false, &correlator_db));
  ASSERT_NE(nullptr, correlator_db);

  // В "корреляторе" снова стартуем транзакцию и получаем сведения о таблице
  {
    fpta_txn *txn = (fpta_txn *)&txn;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(correlator_db, fpta_read, &txn));
    ASSERT_NE(nullptr, txn);

    // инициализируем идентификатор таблицы
    fpta_name table;
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));

    // сверяем кол-во записей
    size_t num;
    EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table, &num, NULL));
    EXPECT_EQ(num, 1111u);

    // завершает транзакцию коррелятора
    ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    txn = nullptr;

    // разрушаем идентификатор
    fpta_name_destroy(&table);
  }

  // закрываем базу в корреляторе
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(correlator_db));
  correlator_db = nullptr;

  // пока не удялем файлы чтобы можно было запустить mdbx_chk
  if (false) {
    if (REMOVE_FILE(testdb_name) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }
    if (REMOVE_FILE(testdb_name_lck) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }
  }
}

//----------------------------------------------------------------------------

TEST(SmokeComposite, SimilarValuesPrimary) {
  const bool skipped = GTEST_IS_EXECUTION_TIMEOUT();
  if (skipped)
    return;

  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  // открываем/создаем базульку в 1 мегабайт
  fpta_db *db = nullptr;
  EXPECT_EQ(FPTA_SUCCESS,
            fpta_db_open(testdb_name, fpta_weak, fpta_regime_default, 0644, 1,
                         true, &db));
  ASSERT_NE(nullptr, db);

  // описываем простейшую таблицу с тремя колонками и одним составным PK
  fpta_column_set def;
  fpta_column_set_init(&def);

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("_id", fptu_int64,
                                 fpta_secondary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "_last_changed", fptu_datetime,
                         fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("cpu", fptu_int64, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("hoster", fptu_cstr, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("id", fptu_cstr, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("name", fptu_cstr, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("type", fptu_cstr, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_describe_composite_index_va(
                "ui_composite_field", fpta_primary_unique_ordered_obverse, &def,
                "hoster", "name", "type", "id", "cpu", nullptr));

  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  // запускам транзакцию и создаем таблицу с обозначенным набором колонок
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "composite_table", &def));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // разрушаем описание таблицы
  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

  // инициализируем идентификаторы таблицы и её колонок
  fpta_name table, col_service_id, col_last_changed, col_cpu, col_hoster,
      col_id, col_name, col_type, col_composite;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "composite_table"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_service_id, "_id"));
  EXPECT_EQ(FPTA_OK,
            fpta_column_init(&table, &col_last_changed, "_last_changed"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_cpu, "cpu"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_hoster, "hoster"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_id, "id"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_name, "name"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_type, "type"));
  EXPECT_EQ(FPTA_OK,
            fpta_column_init(&table, &col_composite, "ui_composite_field"));

  // начинаем транзакцию для вставки данных
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);
  // ради теста делаем привязку вручную
  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_composite));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_service_id));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_last_changed));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_cpu));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_hoster));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_id));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_name));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_type));

  // проверяем иформацию о таблице (сейчас таблица пуста)
  size_t row_count;
  fpta_table_stat stat;
  memset(&row_count, 42, sizeof(row_count));
  memset(&stat, 42, sizeof(stat));
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table, &row_count, &stat));
  EXPECT_EQ(0u, row_count);
  EXPECT_EQ(row_count, stat.row_count);
  EXPECT_EQ(0u, stat.btree_depth);
  EXPECT_EQ(0u, stat.large_pages);
  EXPECT_EQ(0u, stat.branch_pages);
  EXPECT_EQ(0u, stat.leaf_pages);
  EXPECT_EQ(0u, stat.total_bytes);

  // создаем кортеж, который станет первой записью в таблице
  fptu_rw *pt1 = fptu_alloc(7, 1000);
  ASSERT_NE(nullptr, pt1);
  ASSERT_STREQ(nullptr, fptu_check(pt1));
  fptu_time datetime;
  datetime.fixedpoint = 1492170771;

  // ради проверки пытаемся сделать нехорошее (добавить поля с нарушениями)
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt1, &col_service_id, fpta_value_sint(0)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_last_changed,
                                        fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_cpu, fpta_value_sint(1)));
  // All good on 24 A, bad on 25
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt1, &col_hoster,
                               fpta_value_cstr("AAAAAAAAAAAAAAAAAAAAAAAAA")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_id, fpta_value_cstr("A")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_type, fpta_value_cstr("A")));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt1, &col_name,
                               fpta_value_cstr("AAAAAAAAAAAAAAAAAAAAAAAAA")));

  ASSERT_STREQ(nullptr, fptu_check(pt1));

  // создаем еще один кортеж для второй записи
  fptu_rw *pt2 = fptu_alloc(7, 1000);
  ASSERT_NE(nullptr, pt2);
  ASSERT_STREQ(nullptr, fptu_check(pt2));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt2, &col_service_id, fpta_value_sint(1)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_last_changed,
                                        fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_cpu, fpta_value_sint(2)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt2, &col_hoster,
                               fpta_value_cstr("AAAAAAAAAAAAAAAAAAAAAAAAA")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_id, fpta_value_cstr("A")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_type, fpta_value_cstr("A")));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt2, &col_name,
                               fpta_value_cstr("AAAAAAAAAAAAAAAAAAAAAAAAA")));
  ASSERT_STREQ(nullptr, fptu_check(pt2));

  EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt1)));
  EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt2)));

  // фиксируем изменения
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  // и начинаем следующую транзакцию
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table, &row_count, &stat));
  EXPECT_EQ(2u, row_count);
  EXPECT_EQ(row_count, stat.row_count);

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // разрушаем привязанные идентификаторы
  fpta_name_destroy(&table);
  fpta_name_destroy(&col_service_id);
  fpta_name_destroy(&col_last_changed);
  fpta_name_destroy(&col_cpu);
  fpta_name_destroy(&col_id);
  fpta_name_destroy(&col_name);
  fpta_name_destroy(&col_type);
  fpta_name_destroy(&col_hoster);
  fpta_name_destroy(&col_composite);
  // закрываем базульку
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  // пока не удялем файлы чтобы можно было посмотреть и натравить mdbx_chk
  if (false) {
    ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
    ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
  }
}

//----------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
