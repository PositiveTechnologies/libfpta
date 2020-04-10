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

#include "fpta_test.h"
#include "keygen.hpp"

static const char testdb_name[] = TEST_DB_DIR "ut_composite.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_composite.fpta" MDBX_LOCK_SUFFIX;

//----------------------------------------------------------------------------
TEST(SmokeComposite, Primary) {
  /* Smoke-проверка жизнеспособности составных индексов в роли первичных.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей, в которой три колонки
   *     и один составной primary индекс (четвертая псевдо-колонка).
   *  2. Добавляем данные:
   *     - добавляем "первую" запись, одновременно пытаясь
   *       добавить в строку-кортеж поля с "плохими" значениями.
   *     - добавляем "вторую" запись, которая отличается от первой
   *       всеми колонками.
   *     - также попутно пытаемся обновить несуществующие записи
   *       и вставить дубликаты.
   *  3. Читаем добавленное:
   *     - открываем курсор по составному индексу, без фильтра,
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
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  // открываем/создаем базульку в 1 мегабайт
  fpta_db *db = nullptr;
  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime4testing,
                                  1, true, &db));
  ASSERT_NE(nullptr, db);

  // описываем простейшую таблицу с тремя колонками и одним составным PK
  fpta_column_set def;
  fpta_column_set_init(&def);

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("a_str", fptu_cstr, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("b_uint", fptu_uint64, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "c_fp", fptu_fp64,
                         fpta_secondary_withdups_ordered_obverse, &def));
/* у нас всегда код C++, но ниже просто пример использования
 * расширенного интерфейса и его аналог на C. */
#ifdef __cplusplus
  EXPECT_EQ(FPTA_OK, fpta::describe_composite_index(
                         "pk", fpta_primary_unique_ordered_obverse, &def,
                         "b_uint", "a_str", "c_fp"));
#else
  EXPECT_EQ(FPTA_OK, fpta_describe_composite_index_va(
                         "pk", fpta_primary_unique_ordered_obverse, &def,
                         "b_uint", "a_str", "c_fp", nullptr));
#endif
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
  fpta_name table, col_a, col_b, col_c, col_pk;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table_1"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_a, "a_str"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_b, "b_uint"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_c, "c_fp"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_pk, "pk"));

  // начинаем транзакцию для вставки данных
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);
  // ради теста делаем привязку вручную
  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_pk));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_a));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_b));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_c));

  // проверяем информацию о таблице (сейчас таблица пуста)
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
  ASSERT_STREQ(nullptr, fptu::check(pt1));

  // ради проверки пытаемся сделать нехорошее (добавить поля с нарушениями)
  EXPECT_EQ(FPTA_ETYPE, fpta_upsert_column(pt1, &col_a, fpta_value_uint(12)));
  EXPECT_EQ(FPTA_EVALUE, fpta_upsert_column(pt1, &col_b, fpta_value_sint(-34)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt1, &col_c, fpta_value_cstr("x-string")));

  // добавляем нормальные значения
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_c, fpta_value_float(56.78)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt1, &col_a, fpta_value_cstr("string")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_b, fpta_value_sint(34)));
  ASSERT_STREQ(nullptr, fptu::check(pt1));

  // создаем еще один кортеж для второй записи
  fptu_rw *pt2 = fptu_alloc(3, 42);
  ASSERT_NE(nullptr, pt2);
  ASSERT_STREQ(nullptr, fptu::check(pt2));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_a, fpta_value_cstr("zzz")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_b, fpta_value_sint(90)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_c, fpta_value_float(12.34)));
  ASSERT_STREQ(nullptr, fptu::check(pt2));

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

  // снова проверяем информацию о таблице (сейчас в таблице две строки)
  memset(&row_count, 42, sizeof(row_count));
  memset(&stat, 42, sizeof(stat));
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table, &row_count, &stat));
  EXPECT_EQ(2u, row_count);
  EXPECT_EQ(row_count, stat.row_count);
  EXPECT_EQ(1u, stat.btree_depth);
  EXPECT_EQ(0u, stat.large_pages);
  EXPECT_EQ(0u, stat.branch_pages);
  EXPECT_EQ(2u, stat.leaf_pages);
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
  ASSERT_STREQ(nullptr, fptu::check(row2));
  EXPECT_EQ(fptu_eq, fptu_cmp_tuples(fptu_take_noshrink(pt2), row2));

  // создаем третий кортеж для получения составного ключа
  fptu_rw *pt3 = fptu_alloc(3, 21);
  ASSERT_NE(nullptr, pt3);

  // сначала пробуем несуществующую комбинацию,
  // причем из существующих значений, но из разных строк таблицы
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt3, &col_b, fpta_value_sint(90)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt3, &col_a, fpta_value_cstr("string")));
  ASSERT_STREQ(nullptr, fptu::check(pt3));

  // получаем составной ключ
  uint8_t key_buffer[fpta_keybuf_len];
  fpta_value pk_composite_key;
  // пробуем без одной колонки
  EXPECT_EQ(FPTA_COLUMN_MISSING,
            fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_pk,
                                   &pk_composite_key, key_buffer,
                                   sizeof(key_buffer)));
  // добавляем недостающую колонку
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt3, &col_c, fpta_value_float(56.78)));
  ASSERT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_pk,
                                            &pk_composite_key, key_buffer,
                                            sizeof(key_buffer)));

  // получаем составной ключ из оригинального кортежа
  uint8_t key_buffer2[fpta_keybuf_len];
  fpta_value pk_composite_origin;
  ASSERT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt1), &col_pk,
                                            &pk_composite_origin, key_buffer2,
                                            sizeof(key_buffer2)));
  EXPECT_EQ(pk_composite_origin.binary_length, pk_composite_key.binary_length);
  EXPECT_GT(0, memcmp(pk_composite_origin.binary_data,
                      pk_composite_key.binary_data,
                      pk_composite_key.binary_length));

  // позиционируем курсор на конкретное НЕсуществующее составное значение
  EXPECT_EQ(FPTA_NODATA,
            fpta_cursor_locate(cursor, true, &pk_composite_key, nullptr));
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));

  // теперь формируем ключ для существующей комбинации
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt3, &col_b, fpta_value_sint(34)));
  ASSERT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_pk,
                                            &pk_composite_key, key_buffer,
                                            sizeof(key_buffer)));
  EXPECT_EQ(pk_composite_origin.binary_length, pk_composite_key.binary_length);
  EXPECT_EQ(0, memcmp(pk_composite_origin.binary_data,
                      pk_composite_key.binary_data,
                      pk_composite_key.binary_length));

  // позиционируем курсор на конкретное составное значение
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_locate(cursor, true, &pk_composite_key, nullptr));
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

  // ради проверки считаем повторы
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ(1u, dups);

  // получаем текущую строку, она должна совпадать с первым кортежем
  fptu_ro row1;
  EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row1));
  ASSERT_STREQ(nullptr, fptu::check(row1));
  EXPECT_EQ(fptu_eq, fptu_cmp_tuples(fptu_take_noshrink(pt1), row1));

  // разрушаем созданные кортежи
  // на всякий случай предварительно проверяя их
  ASSERT_STREQ(nullptr, fptu::check(pt1));
  free(pt1);
  pt1 = nullptr;
  ASSERT_STREQ(nullptr, fptu::check(pt2));
  free(pt2);
  pt2 = nullptr;
  ASSERT_STREQ(nullptr, fptu::check(pt3));
  free(pt3);
  pt3 = nullptr;

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
  fpta_name_destroy(&col_a);
  fpta_name_destroy(&col_b);
  fpta_name_destroy(&col_c);
  fpta_name_destroy(&col_pk);

  // закрываем базульку
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

//----------------------------------------------------------------------------

TEST(SmokeIndex, Secondary) {
  /* Smoke-проверка жизнеспособности составных индексов в роли вторичных.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей, в которой три колонки, и два индекса:
   *       - primary.
   *       - составной secondary (четвертая псевдо-колонка).
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
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  // открываем/создаем базульку в 1 мегабайт
  fpta_db *db = nullptr;
  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime4testing,
                                  1, true, &db));
  ASSERT_NE(nullptr, db);

  // описываем простейшую таблицу с тремя колонками,
  // одним Primary и одним составным Secondary
  fpta_column_set def;
  fpta_column_set_init(&def);

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("pk_str_uniq", fptu_cstr,
                                 fpta_primary_unique_ordered_reverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "a_sint", fptu_int64,
                         fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "b_fp", fptu_fp64,
                fpta_secondary_withdups_unordered_nullable_obverse, &def));

  // пробуем несколько недопустимых комбинаций:
  // - избыточная уникальность для составного индекса при уникальности
  //   одной из колонок (pk_str_uniq)
  EXPECT_EQ(FPTA_SIMILAR_INDEX,
            fpta_describe_composite_index_va(
                "se", fpta_secondary_unique_ordered_obverse, &def, "a_sint",
                "b_fp", "pk_str_uniq", nullptr));

  // - по колонке a_sint уже есть сортирующий/упорядоченный индекс,
  //   и эта колонка первая в составном индексе (который также упорядоченный),
  //   поэтому индекс по колонке a_sint избыточен, так как вместо него может
  //   быть использован составной.
  EXPECT_EQ(FPTA_SIMILAR_INDEX,
            fpta_describe_composite_index_va(
                "se", fpta_secondary_withdups_ordered_obverse, &def, "a_sint",
                "b_fp", "pk_str_uniq", nullptr));

  // - аналогично по колонке pk_str_uniq уже есть сортирующий/упорядоченный
  //   реверсивный индекс и эта колонка последняя в составном индексе (который
  //   также упорядоченный и реверсивный) поэтому индекс по колонке pk_str_uniq
  //   избыточен, так как вместо него может быть использован составной.
  EXPECT_EQ(FPTA_SIMILAR_INDEX,
            fpta_describe_composite_index_va(
                "se", fpta_secondary_withdups_ordered_reverse, &def, "a_sint",
                "b_fp", "pk_str_uniq", nullptr));

  // допустимый вариант, колонку pk_str_uniq ставим последней, чтобы проще
  // проверить усечение составного ключа.
  EXPECT_EQ(FPTA_OK, fpta_describe_composite_index_va(
                         "se", fpta_secondary_withdups_ordered_obverse, &def,
                         "b_fp", "a_sint", "pk_str_uniq", nullptr));

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
  fpta_name table, col_pk, col_a, col_b, col_se;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table_1"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_pk, "pk_str_uniq"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_a, "a_sint"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_b, "b_fp"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_se, "se"));

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
  ASSERT_STREQ(nullptr, fptu::check(pt1));

  // ради проверки пытаемся сделать нехорошее (добавить поля с нарушениями)
  EXPECT_EQ(FPTA_ETYPE, fpta_upsert_column(pt1, &col_pk, fpta_value_uint(12)));
  EXPECT_EQ(FPTA_ETYPE, fpta_upsert_column(pt1, &col_a, fpta_value_float(1.0)));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_upsert_column(pt1, &col_b, fpta_value_cstr("x-string")));

  // добавляем нормальные значения
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt1, &col_pk, fpta_value_cstr("first_")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_a, fpta_value_sint(90)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_b, fpta_value_float(56.78)));
  ASSERT_STREQ(nullptr, fptu::check(pt1));

  // создаем еще один кортеж для второй записи
  fptu_rw *pt2 = fptu_alloc(3, 42 + fpta_max_keylen);
  ASSERT_NE(nullptr, pt2);
  ASSERT_STREQ(nullptr, fptu::check(pt2));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                         pt2, &col_pk,
                         fpta_value_str(std::string(fpta_max_keylen, 'z'))));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_a, fpta_value_sint(90)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt2, &col_b, fpta_value_float(56.78)));
  ASSERT_STREQ(nullptr, fptu::check(pt2));

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
            fpta_cursor_open(txn, &col_se, fpta_value_begin(), fpta_value_end(),
                             nullptr, fpta_unsorted_dont_fetch, &cursor));
  ASSERT_NE(nullptr, cursor);
  fptu_ro row;

  // узнам сколько записей за курсором (в таблице).
  size_t count;
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(2u, count);

  // переходим к первой записи
  EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  // ради проверки убеждаемся что за курсором есть данные
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
  // получаем текущую строку, она должна совпадать с первым кортежем
  EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row));
  ASSERT_STREQ(nullptr, fptu::check(row));
  EXPECT_EQ(fptu_eq, fptu_cmp_tuples(fptu_take_noshrink(pt1), row));

  // считаем повторы, их не должно быть
  size_t dups;
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  ASSERT_EQ(1u, dups);

  // переходим к последней записи
  EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_last));
  // ради проверки убеждаемся что за курсором есть данные
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

  // получаем текущую строку, она должна совпадать со вторым кортежем
  EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row));
  ASSERT_STREQ(nullptr, fptu::check(row));
  EXPECT_EQ(fptu_eq, fptu_cmp_tuples(fptu_take_noshrink(pt2), row));

  // считаем повторы, их не должно быть
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  ASSERT_EQ(1u, dups);

  // получаем составной ключ из оригинального кортежа
  uint8_t key_buffer_origin[fpta_keybuf_len];
  fpta_value se_composite_origin;
  ASSERT_EQ(FPTA_OK, fpta_get_column2buffer(
                         fptu_take_noshrink(pt1), &col_se, &se_composite_origin,
                         key_buffer_origin, sizeof(key_buffer_origin)));

  // создаем третий кортеж для получения составного ключа
  fptu_rw *pt3 = fptu_alloc(4, 42 + fpta_max_keylen);
  ASSERT_NE(nullptr, pt3);
  // здесь будет составной ключ
  uint8_t key_buffer[fpta_keybuf_len];
  fpta_value se_composite_key;

  // пробуем без двух колонок, из которых одна nullable
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt3, &col_pk, fpta_value_cstr("absent")));
  ASSERT_STREQ(nullptr, fptu::check(pt3));
  EXPECT_EQ(FPTA_COLUMN_MISSING,
            fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_se,
                                   &se_composite_key, key_buffer,
                                   sizeof(key_buffer)));

  // добавляем недостающую обязательную колонку
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt3, &col_a, fpta_value_sint(90)));
  // теперь мы должы получить составной ключ
  EXPECT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_se,
                                            &se_composite_key, key_buffer,
                                            sizeof(key_buffer)));
  // но это будет несуществующая комбинация
  EXPECT_EQ(FPTA_NODATA,
            fpta_cursor_locate(cursor, true, &se_composite_key, nullptr));
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  // ключ от которой должен быть:
  //  - РАВНОЙ ДЛИНЫ, так как все компоненты имеют равную длину:
  //    значения в col_pk имеют равную длину строк, а отсутствующая col_b
  //    будет заменена на DENIL (NaN).
  //  - МЕНЬШЕ при сравнении через memcmp(), так как на первом месте
  //    в составном индексе идет col_b типа fptu64, и DENIL (NaN) для неё
  //    после конвертации в байты составного ключа должен быть меньше 56.87
  EXPECT_EQ(se_composite_origin.binary_length, se_composite_key.binary_length);
  EXPECT_LT(0, memcmp(se_composite_origin.binary_data,
                      se_composite_key.binary_data,
                      se_composite_key.binary_length));

  // добавляем недостающую nullable колонку
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt3, &col_b, fpta_value_float(56.78)));
  EXPECT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_se,
                                            &se_composite_key, key_buffer,
                                            sizeof(key_buffer)));
  // но это также будет несуществующая комбинация
  EXPECT_EQ(FPTA_NODATA,
            fpta_cursor_locate(cursor, true, &se_composite_key, nullptr));
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
  // ключ от которой должен быть:
  //  - РАВНОЙ ДЛИНЫ, так как все компоненты имеют равную длину.
  //  - МЕНЬШЕ при сравнении через memcmp(), так как отличие только
  //    в значениях "absent" < "first_"
  EXPECT_EQ(se_composite_origin.binary_length, se_composite_key.binary_length);
  EXPECT_LT(0, memcmp(se_composite_origin.binary_data,
                      se_composite_key.binary_data,
                      se_composite_key.binary_length));

  // позиционируем курсор на конкретное НЕсуществующее составное значение
  EXPECT_EQ(FPTA_NODATA,
            fpta_cursor_locate(cursor, true, &se_composite_key, nullptr));
  EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));

  // теперь формируем ключ для существующей комбинации
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt3, &col_pk, fpta_value_cstr("first_")));
  EXPECT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_se,
                                            &se_composite_key, key_buffer,
                                            sizeof(key_buffer)));
  EXPECT_EQ(se_composite_origin.binary_length, se_composite_key.binary_length);
  EXPECT_EQ(0, memcmp(se_composite_origin.binary_data,
                      se_composite_key.binary_data,
                      se_composite_key.binary_length));

  // позиционируем курсор на конкретное составное значение
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_locate(cursor, true, &se_composite_key, nullptr));
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

  // ради проверки считаем повторы
  EXPECT_EQ(FPTA_OK, fpta_cursor_dups(cursor, &dups));
  EXPECT_EQ(1u, dups);

  // получаем текущую строку, она должна совпадать с первым кортежем
  EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row));
  ASSERT_STREQ(nullptr, fptu::check(row));
  EXPECT_EQ(fptu_eq, fptu_cmp_tuples(fptu_take_noshrink(pt1), row));

  // теперь формируем ключ для второй существующей комбинации
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                         pt3, &col_pk,
                         fpta_value_str(std::string(fpta_max_keylen, 'z'))));
  EXPECT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_se,
                                            &se_composite_key, key_buffer,
                                            sizeof(key_buffer)));
  // ключ от которой должен быть ДЛИННЕЕ и БОЛЬШЕ:
  EXPECT_LT(se_composite_origin.binary_length, se_composite_key.binary_length);
  EXPECT_GT(0, memcmp(se_composite_origin.binary_data,
                      se_composite_key.binary_data,
                      std::min(se_composite_key.binary_length,
                               se_composite_origin.binary_length)));
  // позиционируем курсор на конкретное составное значение,
  // это вторая и последняя строка таблицы
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_locate(cursor, true, &se_composite_key, nullptr));
  EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));

  // получаем составной ключ, для комбинации с отрицательным значением
  // посередине
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt3, &col_a, fpta_value_sint(INT64_MIN)));
  EXPECT_EQ(FPTA_OK, fpta_get_column2buffer(fptu_take_noshrink(pt3), &col_se,
                                            &se_composite_key, key_buffer,
                                            sizeof(key_buffer)));
  // ключ от которой должен быть ДЛИННЕЕ и МЕНЬШЕ:
  EXPECT_LT(se_composite_origin.binary_length, se_composite_key.binary_length);
  EXPECT_LT(0, memcmp(se_composite_origin.binary_data,
                      se_composite_key.binary_data,
                      std::min(se_composite_key.binary_length,
                               se_composite_origin.binary_length)));

  // разрушаем созданные кортежи
  // на всякий случай предварительно проверяя их
  ASSERT_STREQ(nullptr, fptu::check(pt1));
  free(pt1);
  pt1 = nullptr;
  ASSERT_STREQ(nullptr, fptu::check(pt2));
  free(pt2);
  pt2 = nullptr;
  ASSERT_STREQ(nullptr, fptu::check(pt3));
  free(pt3);
  pt3 = nullptr;

  // удяляем текущую запись через курсор, это вторая и последняя строка таблицы
  EXPECT_EQ(FPTA_OK, fpta_cursor_delete(cursor));
  // считаем сколько записей теперь, должа быть одна
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
  fpta_name_destroy(&col_se);

  // закрываем базульку
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

  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  1, true, &db));
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
  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
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
  ASSERT_STREQ(nullptr, fptu::check(pt1));

  // добавляем нормальные значения
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &some_field,
                                        fpta_value_cstr("composite_part_1")));
  // пропускаем вставку значения в одну из входящих в mycomposite колонок
  // EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_a, fpta_value_sint(34)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt1, &age, fpta_value_cstr("some data")));
  ASSERT_STREQ(nullptr, fptu::check(pt1));

  // вставляем
  EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt1)));

  // фиксируем изменения
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));

  // разрушаем созданный кортеж
  // на всякий случай предварительно проверяя их
  ASSERT_STREQ(nullptr, fptu::check(pt1));
  free(pt1);
  pt1 = nullptr;

  // разрушаем привязанные идентификаторы
  fpta_name_destroy(&table);
  fpta_name_destroy(&some_field);
  fpta_name_destroy(&age);
  // закрываем базульку
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  db = nullptr;
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

//----------------------------------------------------------------------------

TEST(SmokeComposite, SimilarValuesPrimary) {
  /* Тривиальный тест вставки двух строк с составным первичным индексом. Причем
   * среди полей, входящих в составной индекс, отличие только в одном. */
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
  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime4testing,
                                  1, true, &db));
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

  // проверяем информацию о таблице (сейчас таблица пуста)
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
  ASSERT_STREQ(nullptr, fptu::check(pt1));
  fptu_time datetime;
  datetime.fixedpoint = 1492170771;
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt1, &col_service_id, fpta_value_sint(0)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_last_changed,
                                        fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_cpu, fpta_value_sint(1)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt1, &col_hoster,
                               fpta_value_cstr("AAAAAAAAAAAAAAAAAAAAAAAAA")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_id, fpta_value_cstr("A")));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt1, &col_type, fpta_value_cstr("A")));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt1, &col_name,
                               fpta_value_cstr("AAAAAAAAAAAAAAAAAAAAAAAAA")));
  ASSERT_STREQ(nullptr, fptu::check(pt1));

  // создаем еще один кортеж для второй записи, от первой строки отличия:
  // в полях:
  //  col_service_id - со вторичным индексом, но не входит в составной.
  //  col_cpu - не индексируется, но входит в составной индекс.
  fptu_rw *pt2 = fptu_alloc(7, 1000);
  ASSERT_NE(nullptr, pt2);
  ASSERT_STREQ(nullptr, fptu::check(pt2));
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
  ASSERT_STREQ(nullptr, fptu::check(pt2));

  // вставляем первую и вторую запись
  EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt1)));
  EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt2)));

  // разрушаем созданные кортежи
  // на всякий случай предварительно проверяя их
  ASSERT_STREQ(nullptr, fptu::check(pt1));
  free(pt1);
  pt1 = nullptr;
  ASSERT_STREQ(nullptr, fptu::check(pt2));
  free(pt2);
  pt2 = nullptr;

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
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

//----------------------------------------------------------------------------

/* Дополнительная проверка жизнеспособности CRUD-операций на составных
 * индексах, как первичных, так и вторичных.
 *
 * Сценарий:
 *
 *  1. Внутри тестовой БД многократно пересоздается одна таблица
 *     с шаблонной структурой:
 *      - составной первичный индекс из колонок A и B.
 *      - составной вторичный индекс из колонок C и D.
 *      - дополнительные вторичные индексы для колонок B и D.
 *      - неиндексируемая колонка order для контроля порядка записей.
 *      - неиндексируемая колонка checksum для контроля содержания записей.
 *
 *  2. На каждой итерации схема таблицы последовательно перебирает
 *     комбинации типов данных для колонок и создаваемых индексов:
 *      - Для колонок A, B, C, D перебираются все комбинации
 *        типов данных (до 14**4 = 38'416 итераций).
 *      - Для всех индексов перебираются возможные подвиды:
 *         - до 6 комбинаций для составного primary-индекса по A и B:
 *             unique_ordered_obverse +/-tersely_composite,
 *             unique_ordered_reverse +/-tersely_composite,
 *             unique_unordered +/-tersely_composite.
 *         - до 12 комбинаций для составного secondary-индекса по C и D:
 *             unique_ordered_obverse +/-tersely_composite,
 *             unique_ordered_reverse +/-tersely_composite,
 *             unique_unordered +/-tersely_composite,
 *             withdups_ordered_obverse +/-tersely_composite,
 *             withdups_ordered_reverse +/-tersely_composite,
 *             withdups_unordered +/-tersely_composite.
 *         - до 14*14=196 комбинаций для дополнительных secondary-индексов
 *           по колонками B и D:
 *             withdups_ordered_obverse,
 *             withdups_ordered_obverse_nullable,
 *             withdups_ordered_reverse,
 *             withdups_ordered_reverse_nullable,
 *             unique_ordered_obverse,
 *             unique_ordered_obverse_nullable,
 *             unique_ordered_reverse,
 *             unique_ordered_reverse_nullable,
 *             unique_unordered,
 *             unique_unordered_nullable,
 *             withdups_unordered,
 *             withdups_unordered_nullable,
 *      - ПРОБЛЕМА в итоговом количестве комбинаций = 14*14*14*14*6*12*14*14
 *        что даёт 542'126'592, среди которых 72'855'552 являются допустимыми.
 *        GoogleTest предполагает регистрацию каждого варианта, на что требуется
 *        невменяемое кол-во гигабайт ОЗУ. А прогон этих тестов зайдем более
 *        2-х лет, если в среднем каждая итерация уложится в 1 секунду.
 *        ПОЭТОМУ приходиться кардинально снижать кол-во комбинаций:
 *         - каждый тип данных используется в составном индексе
 *           только один раз, за исключением string и opaque, которые нужны
 *           для проверки длинных ключей.
 *         - для дополнительного индекса B-колонки исключаются все виды
 *           индексов с контролем уникальности, так как колонка B входит в PK,
 *           который требует уникальности.
 *         - для дополнительного индекса D-колонки исключаются все виды
 *           индексов, которые перебираются дополнительным индексом B-колонки.
 *         - пропускаются комбинации с одинаковыми видами индексов.
 *        В результате остается 114'432 комбинации.
 *
 *  3. На каждой итерации выполняется некоторое (большое) количество
 *     CRUD-операций "горкой":
 *      - Первый "нарастающий" этап до необходимого кол-ва записей в БД:
 *         - обновляется одна из ранее добавленных записей.
 *         - удаляется одна из ранее добавленных записей.
 *         - добавляются две новые записи.
 *      - Второй "нисходящий" этап до полного опустошения БД:
 *         - обновляется одна из ранее добавленных записей.
 *         - удаляется две из ранее добавленных записей.
 *         - добавляются одна новая запись.
 *      - В каждой транзакции, в самом начале и перед фиксацией проверяется
 *        содержание БД и порядок записей в каждом индексе.
 *
 *  4. Метод генерации значений для всех индексируемых колонок (A, B, C, D)
 *     обеспечивает верифицируемость порядка записей по каждому индексу,
 *     а также генерацию "дубликатов" с заданной плотностью:
 *      - Значения формируются общим "генератором ключей", который
 *        обеспечивает получение упорядоченных ключей для всех типов данных,
 *        как фиксированной, так и переменной длины, в том числе обход
 *        крайних и промежуточных значений за заданное кол-во шагов.
 *      - Генератор ключей проверяется отдельными тестами (см. keygen.hpp)
 *        и позволяет получать ключи в произвольном порядке, для obverse и
 *        reverse индексами, обходя домен допустимых значений за N шагов.
 *      - Порядковые номера ключей для генерации значения каждой колонки
 *        формируются через биективное (для unique индексов) или
 *        не-инъективное (для индексов с дубликатами) преобразование
 *        из промежуточной последовательности целых чисел. В свою очередь,
 *        промежуточная последовательность формируются конгруэнтным
 *        биективным преобразованием из линейной последовательности, значения
 *        которой для каждой строки сохраняются в колонке order.
 *      - Сам генератор ключей проверяется отдельным тестом в других юнитах.
 *
 *  5. Завершаем операции и освобождаем ресурсы.
 */

#ifdef FPTA_INDEX_UT_LONG
static cxx11_constexpr_var int NNN_WITHDUP = 797;
static cxx11_constexpr_var int NNN_UNIQ = 32653;
static cxx11_constexpr_var unsigned megabytes = 1024;
#else
static cxx11_constexpr_var int NNN_WITHDUP = 101;
static cxx11_constexpr_var int NNN_UNIQ = 509;
static cxx11_constexpr_var unsigned megabytes = 32;
#endif
static cxx11_constexpr_var unsigned NBATCH = 7;
static cxx11_constexpr_var int NNN = NNN_UNIQ / 2;

#include <bitset>

template <int N>
static inline unsigned map_linear2stochastic(const unsigned linear,
                                             const bool odd,
                                             const unsigned salt) {
  static_assert(N >= 0 && N < 4, "WTF?");
  assert(linear < NNN);
  cxx11_constexpr_var const unsigned x[] = {4026277019, 2450534059, 968322911,
                                            4001240291};
  cxx11_constexpr_var const unsigned y[] = {3351947, 3172243, 16392923,
                                            12004879};
  cxx11_constexpr_var const unsigned z[] = {3086191, 856351, 11844137, 1815599};
  uint64_t order = linear + linear + odd;
  order = (order * x[N] + salt) % NNN_UNIQ;
  order = (order * y[N] + z[N]) % NNN_UNIQ;
  return unsigned(order);
}

using bitset_NNN_UNIQ = std::bitset<NNN_UNIQ>;

TEST(Self, map_linear2stochastic) {
#if defined(NDEBUG) || defined(__OPTIMIZE__)
  cxx11_constexpr_var unsigned n_iterations = 42000;
#else
  cxx11_constexpr_var unsigned n_iterations = 42;
#endif
  unsigned salt = 3216208939;
  for (unsigned n = 0; n < n_iterations; ++n) {
    SCOPED_TRACE("iteration " + std::to_string(n) + ", salt " +
                 std::to_string(salt));
    bitset_NNN_UNIQ probe[4];
    for (unsigned i = 0; i < NNN; ++i) {
      SCOPED_TRACE("linear " + std::to_string(i));
      const unsigned bit_0_even = map_linear2stochastic<0>(i, false, salt);
      EXPECT_FALSE(probe[0][bit_0_even]);
      probe[0].set(bit_0_even);
      const unsigned bit_0_odd = map_linear2stochastic<0>(i, true, salt);
      EXPECT_FALSE(probe[0][bit_0_odd]);
      probe[0].set(bit_0_odd);

      const unsigned bit_1_even = map_linear2stochastic<1>(i, false, salt);
      EXPECT_FALSE(probe[1][bit_1_even]);
      probe[1].set(bit_1_even);
      const unsigned bit_1_odd = map_linear2stochastic<1>(i, true, salt);
      EXPECT_FALSE(probe[1][bit_1_odd]);
      probe[1].set(bit_1_odd);

      const unsigned bit_2_even = map_linear2stochastic<2>(i, false, salt);
      EXPECT_FALSE(probe[2][bit_2_even]);
      probe[2].set(bit_2_even);
      const unsigned bit_2_odd = map_linear2stochastic<2>(i, true, salt);
      EXPECT_FALSE(probe[2][bit_2_odd]);
      probe[2].set(bit_2_odd);

      const unsigned bit_3_even = map_linear2stochastic<3>(i, false, salt);
      EXPECT_FALSE(probe[3][bit_3_even]);
      probe[3].set(bit_3_even);
      const unsigned bit_3_odd = map_linear2stochastic<3>(i, true, salt);
      EXPECT_FALSE(probe[3][bit_3_odd]);
      probe[3].set(bit_3_odd);
    }
    salt = salt * 1664525 + 1013904223;
  }
}

#define CompositeTest_ColumtA_TypeList                                         \
  fptu_uint16, /* fptu_int32, fptu_uint32, fptu_fp32, */ fptu_int64,           \
      /* fptu_uint64, fptu_fp64, fptu_96, */ fptu_128, /* fptu_160,            \
      fptu_datetime, fptu_256, */                                              \
      fptu_cstr                                        /*, fptu_opaque */

#define CompositeTest_ColumtB_TypeList                                         \
  /* fptu_uint16, */ fptu_int32, /* fptu_uint32, fptu_fp32, fptu_int64, */     \
      fptu_uint64, /* fptu_fp64, fptu_96, fptu_128, */ fptu_160,               \
      /* fptu_datetime,  fptu_256, fptu_cstr, */ fptu_opaque

#define CompositeTest_ColumtC_TypeList                                         \
  /* fptu_uint16, fptu_int32, */ fptu_uint32,                                  \
      /* fptu_fp32, fptu_int64, fptu_uint64, */ fptu_fp64, /* fptu_96,         \
                                                              fptu_128,        \
                                                              fptu_160, */     \
      fptu_datetime, /* fptu_256, fptu_cstr, */ fptu_opaque

#define CompositeTest_ColumtD_TypeList                                         \
  /* fptu_uint16, fptu_int32, fptu_uint32, */ fptu_fp32,                       \
      /* fptu_int64, fptu_uint64, fptu_fp64, */ fptu_96,                       \
      /* fptu_128, fptu_160, fptu_datetime, */ fptu_256,                       \
      fptu_cstr /*, fptu_opaque */

#define CompositeTest_PK_IndexAB_List                                          \
  fpta_primary_unique_ordered_obverse, fpta_primary_unique_ordered_reverse,    \
      fpta_primary_unique_unordered,                                           \
      fpta_index_type(fpta_primary_unique_ordered_obverse +                    \
                      fpta_tersely_composite),                                 \
      fpta_index_type(fpta_primary_unique_ordered_reverse +                    \
                      fpta_tersely_composite),                                 \
      fpta_index_type(fpta_primary_unique_unordered + fpta_tersely_composite)

#define CompositeTest_SE_IndexCD_List                                          \
  /* unique*****************************************************************/  \
  fpta_secondary_unique_ordered_obverse,                                       \
      fpta_secondary_unique_ordered_reverse, fpta_secondary_unique_unordered,  \
      fpta_index_type(fpta_secondary_unique_ordered_obverse +                  \
                      fpta_tersely_composite),                                 \
      fpta_index_type(fpta_secondary_unique_ordered_reverse +                  \
                      fpta_tersely_composite),                                 \
      fpta_index_type(fpta_secondary_unique_unordered +                        \
                      fpta_tersely_composite), /* with-dups*****************/  \
      fpta_secondary_withdups_ordered_obverse,                                 \
      fpta_secondary_withdups_ordered_reverse,                                 \
      fpta_secondary_withdups_unordered,                                       \
      fpta_index_type(fpta_secondary_withdups_ordered_obverse +                \
                      fpta_tersely_composite),                                 \
      fpta_index_type(fpta_secondary_withdups_ordered_reverse +                \
                      fpta_tersely_composite),                                 \
      fpta_index_type(fpta_secondary_withdups_unordered +                      \
                      fpta_tersely_composite)

#define CompositeTest_SE_IndexB_List                                           \
  /* ordered****************************************************************/  \
  fpta_secondary_withdups_ordered_obverse,                                     \
      fpta_secondary_withdups_ordered_obverse_nullable,                        \
      fpta_secondary_withdups_ordered_reverse,                                 \
      fpta_secondary_withdups_ordered_reverse_nullable, /* unordered********/  \
      fpta_secondary_withdups_unordered,                                       \
      fpta_secondary_withdups_unordered_nullable_obverse,                      \
      fpta_secondary_withdups_unordered_nullable_reverse

#define CompositeTest_SE_IndexD_List                                           \
  /* unique*****************************************************************/  \
  fpta_secondary_unique_ordered_obverse,                                       \
      fpta_secondary_unique_ordered_obverse_nullable,                          \
      fpta_secondary_unique_ordered_reverse,                                   \
      fpta_secondary_unique_ordered_reverse_nullable,                          \
      fpta_secondary_unique_unordered,                                         \
      fpta_secondary_unique_unordered_nullable_obverse,                        \
      fpta_secondary_unique_unordered_nullable_reverse
/*  withdups****************************************************************
 *    fpta_secondary_withdups_ordered_obverse,
 *    fpta_secondary_withdups_ordered_obverse_nullable,
 *    fpta_secondary_withdups_ordered_reverse,
 *    fpta_secondary_withdups_ordered_reverse_nullable,
 *    fpta_secondary_withdups_unordered,
 *    fpta_secondary_withdups_unordered_nullable_obverse,
 *    fpta_secondary_withdups_unordered_nullable_reverse                   */

#if !(defined(stpcpy) ||                                                       \
      (defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200809L) ||              \
      __GLIBC_PREREQ(2, 10))
static char *local_stpcpy(char *dest, const char *src) {
  size_t len = strlen(src);
  memcpy(dest, src, len + 1);
  return dest + len;
}
#define stpcpy(dest, src) local_stpcpy(dest, src)
#endif /* stpcpy() stub */

static const char *to_cstr(const fpta_index_type index, const bool composite,
                           char *buffer) {
  char *p = buffer;
  if (fpta_is_indexed(index)) {
    p = stpcpy(p, fpta_index_is_secondary(index) ? "Secondary" : "Primary");
    p = stpcpy(p, fpta_index_is_unique(index) ? "Unique" : "Withdups");
    p = stpcpy(p, fpta_index_is_ordered(index) ? "Ordered" : "Unordered");
    p = stpcpy(p, fpta_index_is_obverse(index) ? "Obverse" : "Reverse");
  } else {
    p = stpcpy(p, "Noindex");
    assert(!composite);
  }

  if (fpta_column_is_nullable(index))
    p = stpcpy(p, composite ? "Tersely" : "Nullable");

  return buffer;
}

using CompositeTestParamsTuple =
    GTEST_TUPLE_NAMESPACE_::tuple<fptu_type, fptu_type, fptu_type, fptu_type,
                                  fpta_index_type, fpta_index_type,
                                  fpta_index_type, fpta_index_type>;

struct CompositeCombineParams {
  const fptu_type a_type, b_type, c_type, d_type;
  const fpta_index_type ab_index, cd_index, b_index, d_index;
  const uint64_t checksum_salt;

  CompositeCombineParams(const CompositeTestParamsTuple &params)
      : a_type(GTEST_TUPLE_NAMESPACE_::get<0>(params)),
        b_type(GTEST_TUPLE_NAMESPACE_::get<1>(params)),
        c_type(GTEST_TUPLE_NAMESPACE_::get<2>(params)),
        d_type(GTEST_TUPLE_NAMESPACE_::get<3>(params)),
        ab_index(GTEST_TUPLE_NAMESPACE_::get<4>(params)),
        cd_index(GTEST_TUPLE_NAMESPACE_::get<5>(params)),
        b_index(GTEST_TUPLE_NAMESPACE_::get<6>(params)),
        d_index(GTEST_TUPLE_NAMESPACE_::get<7>(params)),
        checksum_salt(t1ha2_atonce(&params, sizeof(params),
                                   UINT64_C(2688146592618233))) {}

  std::string params_to_string() const {
    char buf1[128];
    char buf2[128];
    char buf3[128];
    char buf4[128];
    return fptu::format(
        "%s_%s_%s_%s_%s_%s_%s_%s", fptu_type_name(a_type),
        fptu_type_name(b_type), fptu_type_name(c_type), fptu_type_name(d_type),
        to_cstr(ab_index, true, buf1) + 7, to_cstr(cd_index, true, buf2) + 9,
        to_cstr(b_index, false, buf3) + 9, to_cstr(d_index, false, buf4) + 9);
  }

  bool is_valid_params() const {
    if (fpta_index_is_unique(ab_index) && fpta_index_is_unique(b_index))
      return false;

    if (fpta_index_is_unique(cd_index) && fpta_index_is_unique(d_index))
      return false;

    if (fpta_index_is_reverse(b_index)) {
      if (fpta_index_is_unordered(b_index) || b_type < fptu_96)
        return false;
      if (!(fpta_is_indexed_and_nullable(b_index) &&
            fpta_nullable_reverse_sensitive(b_type)))
        return false;
      if (fpta_index_is_ordered(ab_index) && fpta_index_is_ordered(b_index) &&
          fpta_index_is_reverse(ab_index))
        return false;
    }

    if (fpta_index_is_reverse(d_index)) {
      if (fpta_index_is_unordered(d_index) || d_type < fptu_96)
        return false;
      if (!(fpta_is_indexed_and_nullable(d_index) &&
            fpta_nullable_reverse_sensitive(d_type)))
        return false;
      if (fpta_index_is_ordered(cd_index) && fpta_index_is_ordered(d_index) &&
          fpta_index_is_reverse(cd_index))
        return false;
    }

    return true;
  }

  bool is_preferable_to_skip() const {
#if 0
    if (b_index == d_index || b_index == cd_index || d_index == cd_index ||
        ((cd_index ^ ab_index) & ~fpta_index_fsecondary) == 0 ||
        ((b_index ^ ab_index) & ~fpta_index_fsecondary) == 0 ||
        ((d_index ^ ab_index) & ~fpta_index_fsecondary) == 0)
      return true;

    if (a_type == c_type || b_type == d_type)
      return true;
#endif
    return false;
  }
};

#ifdef INSTANTIATE_TEST_SUITE_P /* not available in gtest 1.8.1 */

class CompositeCombineFixture : public ::testing::Test,
                                public CompositeCombineParams {
protected:
  struct SharedResources {
    scoped_db_guard db_quard;
  };
  static SharedResources *shared_resource_;

  fptu::tuple_ptr row_foo, row_bar, row_baz;
  scoped_cursor_guard cursor_guard;
  scoped_txn_guard txn_guard;

  std::string a_col_name, b_col_name, c_col_name, d_col_name;
  std::string ab_col_name, cd_col_name;

  fpta_name table, col_a, col_b, col_c, col_d;
  fpta_name col_ab, col_cd, col_linear, col_checksum;

  bool should_drop_table = false;
  bool should_drop_names = false;
  unsigned nops = 0;

public:
  CompositeCombineFixture(const CompositeTestParamsTuple &params)
      : CompositeCombineParams(params) {}

  // Per-test-suite set-up.
  // Called before the first test in this test suite.
  static void SetUpTestSuite() {
    shared_resource_ = new SharedResources;
    if (REMOVE_FILE(testdb_name) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }
    if (REMOVE_FILE(testdb_name_lck) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }
  }

  // Per-test-suite tear-down.
  // Called after the last test in this test suite.
  static void TearDownTestSuite() {
    delete shared_resource_;
    shared_resource_ = NULL;
    ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
    ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
  }

  static fpta_db *db() { return shared_resource_->db_quard.get(); }

  fpta_txn *txn() {
    if (!txn_guard) {
      // начинаем транзакцию записи
      fpta_txn *txn = nullptr;
      EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db(), fpta_write, &txn));
      ASSERT_NE(nullptr, txn), nullptr;
      txn_guard.reset(txn);

      // связываем идентификаторы с ранее созданной схемой
      EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_a));
      EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_b));
      EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_c));
      EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_d));
      EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_ab));
      EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_cd));
      EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_linear));
      EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_checksum));
    }
    return txn_guard.get();
  }

  void commit() {
    cursor_guard.reset();
    ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
  }

  void abort() {
    cursor_guard.reset();
    txn_guard.reset();
  }

  // You can define per-test set-up logic as usual.
  virtual void SetUp() {
    // нужно простое число, иначе сломается переупорядочивание
    ASSERT_TRUE(isPrime(NNN_UNIQ) && isPrime(NNN_WITHDUP));
    // иначе не сможем проверить fptu_uint16
    ASSERT_GE(65535 / 2, NNN);
    ASSERT_LE(2, NNN_UNIQ / NNN_WITHDUP);

    if (!is_valid_params()) {
      GTEST_SKIP();
      return;
    }

    if (GTEST_IS_EXECUTION_TIMEOUT()) {
      GTEST_SKIP();
      return;
    }

    // создаём или открываем БД
    if (!db()) {
      fpta_db *db = nullptr;
      ASSERT_EQ(FPTA_OK,
                test_db_open(testdb_name, fpta_weak, fpta_regime4testing,
                             megabytes, true, &db));
      ASSERT_NE(nullptr, db);
      shared_resource_->db_quard.reset(db);
    }

    // инициализируем идентификаторы колонок
    char buf[128];
    a_col_name = fptu::format("a_%s", fptu_type_name(a_type));
    b_col_name = fptu::format("b_%s_%s", fptu_type_name(a_type),
                              to_cstr(b_index, false, buf));
    c_col_name = fptu::format("c_%s", fptu_type_name(c_type));
    d_col_name = fptu::format("d_%s_%s", fptu_type_name(d_type),
                              to_cstr(d_index, false, buf));
    ab_col_name = fptu::format("ab_%s", to_cstr(ab_index, true, buf));
    cd_col_name = fptu::format("cd_%s", to_cstr(cd_index, true, buf));

    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_a, a_col_name.c_str()));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_b, b_col_name.c_str()));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_c, c_col_name.c_str()));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_d, d_col_name.c_str()));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_ab, ab_col_name.c_str()));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_cd, cd_col_name.c_str()));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_linear, "linear"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_checksum, "checksum"));
    should_drop_names = true;

    // определяем схему
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK, fpta_column_describe(a_col_name.c_str(), a_type,
                                            fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(b_col_name.c_str(), b_type, b_index, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(c_col_name.c_str(), c_type,
                                            fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(d_col_name.c_str(), d_type, d_index, &def));
    EXPECT_EQ(FPTA_OK, fpta::describe_composite_index(
                           ab_col_name.c_str(), ab_index, &def,
                           a_col_name.c_str(), b_col_name.c_str()));
    EXPECT_EQ(FPTA_OK, fpta::describe_composite_index(
                           cd_col_name.c_str(), cd_index, &def,
                           c_col_name.c_str(), d_col_name.c_str()));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("linear", fptu_int32,
                                            fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("checksum", fptu_uint64,
                                            fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    // создаем таблицу
    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db(), fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);
    txn_guard.reset(txn);
    EXPECT_EQ(FPTA_NOTFOUND, fpta_table_drop(txn, "table"));
    ASSERT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));
    ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
    txn = nullptr;
    should_drop_table = true;

    // разрушаем описание таблицы
    EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
    EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

    row_foo.reset(fptu_alloc(6, fpta_max_keylen * 42));
    ASSERT_TRUE(row_foo);
    row_bar.reset(fptu_alloc(6, fpta_max_keylen * 42));
    ASSERT_TRUE(row_bar);
    row_baz.reset(fptu_alloc(6, fpta_max_keylen * 42));
    ASSERT_TRUE(row_baz);
  }

  // You can define per-test tear-down logic as usual.
  virtual void TearDown() {
    if (should_drop_names) {
      // разрушаем привязанные идентификаторы
      fpta_name_destroy(&table);
      fpta_name_destroy(&col_a);
      fpta_name_destroy(&col_b);
      fpta_name_destroy(&col_c);
      fpta_name_destroy(&col_d);
      fpta_name_destroy(&col_ab);
      fpta_name_destroy(&col_cd);
      fpta_name_destroy(&col_linear);
      fpta_name_destroy(&col_checksum);
      should_drop_names = false;
    }

    cursor_guard.reset();
    txn_guard.reset();
    if (should_drop_table) {
      // удаляем таблицу
      fpta_txn *txn = nullptr;
      EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db(), fpta_schema, &txn));
      ASSERT_NE(nullptr, txn);
      txn_guard.reset(txn);
      EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "table"));
      EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
      should_drop_table = false;
    }
  }

  int a_order(const int linear, const int age) const {
    return map_linear2stochastic<0>(linear, (age && age % 4 == 0),
                                    unsigned(checksum_salt));
  }

  int b_order(const int linear, const int age) const {
    return map_linear2stochastic<1>(linear, (age && age % 4 == 1),
                                    unsigned(checksum_salt >> 10));
  }

  int c_order(const int linear, const int age) const {
    return map_linear2stochastic<2>(linear, (age && age % 4 == 2),
                                    unsigned(checksum_salt >> 21));
  }

  int d_order(const int linear, const int age) const {
    return map_linear2stochastic<3>(linear, (age && age % 4 == 3),
                                    unsigned(checksum_salt >> 32));
  }

  fpta_value col_value(const int order, any_keygen &keygen) const {
    assert(order > -1 && order < NNN_UNIQ);
    const fpta_value value =
        fpta_index_is_unique(keygen.get_index())
            ? keygen.make(order, NNN_UNIQ)
            : keygen.make(order % NNN_WITHDUP, NNN_WITHDUP);
    return value;
  }

  fptu_ro make_row(const int linear, any_keygen &keygen_a, any_keygen &keygen_b,
                   any_keygen &keygen_c, any_keygen &keygen_d,
                   fptu::tuple_ptr &row_holder, const int age = 0) const {
    // формируем кортеж-запись по заданному линейному номеру.
    EXPECT_EQ(FPTU_OK, fptu_clear(row_holder.get()));

    /* Генераторы ключей для не-числовых типов используют общий статический
     * буфер, из-за этого генерация следующего значения может повредить
     * предыдущее. Поэтому следующее значение можно генерировать только после
     * вставки в кортеж предыдущего. */
    fpta_value value;

    value = col_value(a_order(linear, age), keygen_a);
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column_ex(row_holder.get(), &col_a, value, true));

    value = col_value(b_order(linear, age), keygen_b);
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column_ex(row_holder.get(), &col_b, value, true));

    value = col_value(c_order(linear, age), keygen_c);
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column_ex(row_holder.get(), &col_c, value, true));

    value = col_value(d_order(linear, age), keygen_d);
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column_ex(row_holder.get(), &col_d, value, true));

    EXPECT_EQ(FPTA_OK, fpta_upsert_column(row_holder.get(), &col_linear,
                                          fpta_value_sint(linear)));

    value =
        fpta_value_uint(t1ha2_atonce(&linear, sizeof(linear), checksum_salt));
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(row_holder.get(), &col_checksum, value));

    EXPECT_STREQ(nullptr, fptu::check(row_holder.get()));
    return fptu_take_noshrink(row_holder.get());
  }

  static size_t composite_item_keylen(const bool tersely, const fptu_ro &row,
                                      fpta_name *column) {
    const fptu_type type = fpta_name_coltype(column);
    unsigned length = (type == fptu_uint16) ? 2 : fptu_internal_map_t2b[type];

    fpta_value value;
    int err = fpta_get_column(row, column, &value);
    if (err == FPTA_NODATA) {
      // null, i.e no nay value
      EXPECT_TRUE(fpta_column_is_nullable(column->shove));
      if (type >= fptu_cstr)
        return tersely ? 0 : 1;
      if (tersely)
        return 1;
      return length;
    }

    EXPECT_EQ(FPTA_OK, err);
    if (type < fptu_cstr) {
      if (fpta_column_is_nullable(column->shove) && tersely)
        /* present-marker for fixed-length nullable columns if TERSELY is ON */
        length += 1;
    } else {
      length = value.binary_length;
      if (!tersely)
        /* present-marker for variable-length columns if TERSELY is OFF */
        length += 1;
    }
    return length;
  }

  void check_composite_keys(fpta_name *column) {
    scoped_cursor_guard guard;
    fpta_cursor *cursor = nullptr;
    ASSERT_EQ(FPTA_OK, fpta_cursor_open(txn(), column, fpta_value_begin(),
                                        fpta_value_end(), nullptr,
                                        fpta_unsorted, &cursor));
    ASSERT_NE(nullptr, cursor);
    guard.reset(cursor);

    const bool tersely = fpta_column_is_nullable(column->shove);
    while (true) {
      int err = fpta_cursor_move(cursor, fpta_next);
      if (err != FPTA_OK) {
        ASSERT_EQ(FPTA_NODATA, err);
        break;
      }

      fptu_ro row;
      ASSERT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row));

      size_t expected_keylen = sizeof(uint64_t);
      if (fpta_index_is_ordered(column->shove)) {
        if (column == &col_ab) {
          expected_keylen = composite_item_keylen(tersely, row, &col_a) +
                            composite_item_keylen(tersely, row, &col_b);
        } else {
          assert(column == &col_cd);
          expected_keylen = composite_item_keylen(tersely, row, &col_c) +
                            composite_item_keylen(tersely, row, &col_d);
        }
      }

      fpta_value key;
      ASSERT_EQ(FPTA_OK, fpta_cursor_key(cursor, &key));
      fpta_value4key check_key;
      EXPECT_EQ(FPTA_OK, fpta_get_column4key(row, column, &check_key));
      EXPECT_EQ(key.binary_length, check_key.value.binary_length);
      EXPECT_EQ((expected_keylen > fpta_max_keylen)
                    ? fpta_max_keylen + sizeof(uint64_t)
                    : expected_keylen,
                key.binary_length);
    }
  }

  void batch_cond_commit() {
    if (txn_guard && ++nops > NBATCH) {
      commit();
      nops = 0;
    }
  }

  void LOG(const std::string &msg) {
    std::cout << msg << "\n";
    std::cout.flush();
  }

  static unsigned update_via_index(unsigned changed_col, unsigned alter_salt) {
    /* выбор индексов (AB=0, CD=1, B=2, D=3), через которые можно обовлять
       строку, при измененни соответствующей колонки (A=0, B=1, C=2, D=3) */
    switch (changed_col % 4) {
    default:
      assert(false);
      __unreachable();
    case 0:
      /* Изменение в колонке А, можно обновлять через индексы: CD, B, D */
      return "123"[alter_salt % 3] - '0';
    case 1:
      /* Изменение в колонке B, можно обновлять через индексы: CD, D */
      return "13"[alter_salt % 2] - '0';
    case 2:
      /* Изменение в колонке C, можно обновлять через индексы: AB, B, D */
      return "023"[alter_salt % 3] - '0';
    case 3:
      /* Изменение в колонке D, можно обновлять через индексы: AB, B */
      return "02"[alter_salt % 2] - '0';
    }
  }
};

CompositeCombineFixture::SharedResources
    *CompositeCombineFixture::shared_resource_;

class CompositeCombineCRUD : public CompositeCombineFixture {
public:
  CompositeCombineCRUD(const CompositeTestParamsTuple &params_tuple)
      : CompositeCombineFixture(params_tuple) {}
  void TestBody() override {
    any_keygen keygen_a(a_type, ab_index);
    any_keygen keygen_b(b_type, b_index);
    any_keygen keygen_c(c_type, cd_index);
    any_keygen keygen_d(d_type, d_index);

    for (unsigned linear = 0; linear < NNN; linear += 2) {
      const auto foo_linear = linear;
      const auto baz_linear = linear + 1;
      txn();

      // вставляем первую запись из пары
      const fptu_ro foo =
          make_row(foo_linear, keygen_a, keygen_b, keygen_c, keygen_d, row_foo);
      // LOG("uphill-insert-1: linear " + std::to_string(foo_linear) + " " +
      //    std::to_string(foo));
      ASSERT_EQ(FPTA_OK, fpta_insert_row(txn(), &table, foo));
      batch_cond_commit();

      fptu_ro baz = {{0, 0}};
      if (baz_linear < NNN) {
        // вставляем вторую запись из пары
        baz = make_row(baz_linear, keygen_a, keygen_b, keygen_c, keygen_d,
                       row_baz);
        // LOG("uphill-insert-2: linear " + std::to_string(baz_linear) + " " +
        //    std::to_string(baz));
        ASSERT_EQ(FPTA_OK, fpta_insert_row(txn(), &table, baz));
        batch_cond_commit();
      }

      // обновляем данные в первой записи
      const unsigned update_diff_salt =
          unsigned((((foo_linear + 144746611) ^ (foo_linear * 2618173)) ^
                    checksum_salt) %
                   4673) +
          1;
      const unsigned alter_mode_salt =
          (((foo_linear + 607750243) ^ (foo_linear * 16458383)) ^
           (update_diff_salt >> 2)) %
          7151;
      const fptu_ro bar = make_row(foo_linear, keygen_a, keygen_b, keygen_c,
                                   keygen_d, row_bar, update_diff_salt);
      const int update_via =
          update_via_index(update_diff_salt, alter_mode_salt);
      // LOG("uphill-update-1: diff-alter " +
      //     std::to_string(update_diff_salt % 4) + ", update-via " +
      //     std::to_string(update_via) + " " + std::to_string(foo) + " ==>> " +
      //     std::to_string(bar));
      fpta_cursor *cursor = nullptr;
      switch (update_via) {
      default:
        assert(false);
        GTEST_FAIL();
        break;
      case 0:
        // обновляем через первичный составной индекс по колонкам A и B
        if ((update_diff_salt ^ alter_mode_salt) % 11 > 5) {
          // обновляем без курсора через PK
          ASSERT_EQ(FPTA_OK, fpta_update_row(txn(), &table, bar));
        } else {
          ASSERT_EQ(FPTA_OK,
                    fpta_cursor_open(txn(), &col_ab, fpta_value_begin(),
                                     fpta_value_end(), nullptr,
                                     fpta_unsorted_dont_fetch, &cursor));
        }
        break;
      case 1:
        // обновляем через вторичный составной индекс по колонкам C и D
        ASSERT_EQ(FPTA_OK, fpta_cursor_open(txn(), &col_cd, fpta_value_begin(),
                                            fpta_value_end(), nullptr,
                                            fpta_unsorted_dont_fetch, &cursor));
        break;
      case 2:
        // обновляем через дополнительный индекс по колонке B
        ASSERT_EQ(FPTA_OK, fpta_cursor_open(txn(), &col_b, fpta_value_begin(),
                                            fpta_value_end(), nullptr,
                                            fpta_unsorted_dont_fetch, &cursor));
        break;
      case 3:
        // обновляем через дополнительный индекс по колонке D
        ASSERT_EQ(FPTA_OK, fpta_cursor_open(txn(), &col_d, fpta_value_begin(),
                                            fpta_value_end(), nullptr,
                                            fpta_unsorted_dont_fetch, &cursor));
        break;
      }
      cursor_guard.reset(cursor);
      if (cursor) {
        ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, nullptr, &foo));
        ASSERT_EQ(FPTA_OK, fpta_cursor_update(cursor, bar));
      }
      batch_cond_commit();

      if (baz_linear < NNN) {
        // удаляем вторую запись
        // LOG("uphill-delete-2: linear " + std::to_string(baz_linear) + " " +
        //     std::to_string(baz));
        if (cursor_guard) {
          // удаляем через открытый курсор
          ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, nullptr, &baz));
          ASSERT_EQ(FPTA_OK, fpta_cursor_delete(cursor));
        } else {
          // удаляем через PK
          ASSERT_EQ(FPTA_OK, fpta_delete(txn(), &table, baz));
        }
        batch_cond_commit();
      }
    }

    //--------------------------------------------------------------------------

    check_composite_keys(&col_ab);
    check_composite_keys(&col_cd);

    //--------------------------------------------------------------------------

    for (unsigned linear = 0; linear < NNN; linear += 2) {
      const auto foo_linear = linear;
      const auto baz_linear = linear + 1;

      // обновляем первую запись из пары
      fptu_ro foo = {{0, 0}};
      fptu_ro bar = {{0, 0}};
      fpta_cursor *cursor = nullptr;
      const unsigned update_diff_salt =
          unsigned((((foo_linear + 144746611) ^ (foo_linear * 2618173)) ^
                    checksum_salt) %
                   4673) +
          1;
      const unsigned alter_mode_salt =
          (((foo_linear + 607750243) ^ (foo_linear * 16458383)) ^
           (update_diff_salt >> 2)) %
          7151;
      foo =
          make_row(foo_linear, keygen_a, keygen_b, keygen_c, keygen_d, row_foo);
      bar = make_row(foo_linear, keygen_a, keygen_b, keygen_c, keygen_d,
                     row_bar, update_diff_salt);
      const int update_via =
          update_via_index(update_diff_salt, alter_mode_salt);
      // LOG("downhill-update-1: diff-alter " +
      //     std::to_string(update_diff_salt % 4) + ", update-via " +
      //     std::to_string(update_via) + " " + std::to_string(bar) + " ==>> " +
      //     std::to_string(foo));
      switch (update_via) {
      default:
        assert(false);
        GTEST_FAIL();
        break;
      case 0:
        // обновляем через первичный составной индекс по колонкам A и B
        if ((update_diff_salt ^ alter_mode_salt) % 11 > 5) {
          // обновляем без курсора через PK
          ASSERT_EQ(FPTA_OK, fpta_update_row(txn(), &table, foo));
        } else {
          ASSERT_EQ(FPTA_OK,
                    fpta_cursor_open(txn(), &col_ab, fpta_value_begin(),
                                     fpta_value_end(), nullptr,
                                     fpta_unsorted_dont_fetch, &cursor));
        }
        break;
      case 1:
        // обновляем через вторичный составной индекс по колонкам C и D
        ASSERT_EQ(FPTA_OK, fpta_cursor_open(txn(), &col_cd, fpta_value_begin(),
                                            fpta_value_end(), nullptr,
                                            fpta_unsorted_dont_fetch, &cursor));
        break;
      case 2:
        // обновляем через дополнительный индекс по колонке B
        ASSERT_EQ(FPTA_OK, fpta_cursor_open(txn(), &col_b, fpta_value_begin(),
                                            fpta_value_end(), nullptr,
                                            fpta_unsorted_dont_fetch, &cursor));
        break;
      case 3:
        // обновляем через дополнительный индекс по колонке D
        ASSERT_EQ(FPTA_OK, fpta_cursor_open(txn(), &col_d, fpta_value_begin(),
                                            fpta_value_end(), nullptr,
                                            fpta_unsorted_dont_fetch, &cursor));
        break;
      }
      cursor_guard.reset(cursor);
      if (cursor) {
        ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor, true, nullptr, &bar));
        ASSERT_EQ(FPTA_OK, fpta_cursor_update(cursor, foo));
      }
      batch_cond_commit();

      // вставляем вторую запись из пары
      fptu_ro baz = {{0, 0}};
      if (baz_linear < NNN) {
        // вставляем вторую запись из пары
        baz = make_row(baz_linear, keygen_a, keygen_b, keygen_c, keygen_d,
                       row_baz);
        // LOG("downhill-insert-2: linear " + std::to_string(baz_linear) + " " +
        //     std::to_string(baz));
        ASSERT_EQ(FPTA_OK, fpta_insert_row(txn(), &table, baz));
        batch_cond_commit();
      }

      // удаляем первую запись
      // LOG("downhill-delete-1: linear " + std::to_string(foo_linear) + " " +
      //     std::to_string(foo));
      ASSERT_EQ(FPTA_OK, fpta_delete(txn(), &table, foo));
      batch_cond_commit();

      // удаляем вторую запись
      if (baz_linear < NNN) {
        // удаляем вторую запись
        // LOG("downhill-delete-2: linear " + std::to_string(baz_linear) + " " +
        //     std::to_string(baz));
        if (cursor_guard) {
          // удаляем через открытый курсор
          ASSERT_EQ(FPTA_OK, fpta_cursor_locate(cursor_guard.get(), true,
                                                nullptr, &baz));
          ASSERT_EQ(FPTA_OK, fpta_cursor_delete(cursor_guard.get()));
        } else {
          // удаляем через PK
          ASSERT_EQ(FPTA_OK, fpta_delete(txn(), &table, baz));
        }
        batch_cond_commit();
      }
    }
    if (txn_guard)
      commit();
  }
};

#endif /* INSTANTIATE_TEST_SUITE_P */

unsigned CompositeTest_Combine(bool just_count) {
  unsigned count = 0;
  for (fptu_type a_type : {CompositeTest_ColumtA_TypeList}) {
    for (fptu_type b_type : {CompositeTest_ColumtB_TypeList}) {
      for (fptu_type c_type : {CompositeTest_ColumtC_TypeList}) {
        for (fptu_type d_type : {CompositeTest_ColumtD_TypeList}) {
          for (fpta_index_type ab_index : {CompositeTest_PK_IndexAB_List}) {
            for (fpta_index_type cd_index : {CompositeTest_SE_IndexCD_List}) {
              for (fpta_index_type b_index : {CompositeTest_SE_IndexB_List}) {
                for (fpta_index_type d_index : {CompositeTest_SE_IndexD_List}) {
                  const CompositeTestParamsTuple tuple(
                      a_type, b_type, c_type, d_type, ab_index, cd_index,
                      b_index, d_index);
                  const CompositeCombineParams params(tuple);
                  if (!params.is_valid_params())
                    continue;
                  if (params.is_preferable_to_skip())
                    continue;

                  if (!just_count) {
#ifdef INSTANTIATE_TEST_SUITE_P
                    const std::string info = params.params_to_string();
                    const std::string caption = "CRUD_" + info;
                    ::testing::RegisterTest(
                        "CompositeCombine", caption.c_str(), nullptr, nullptr,
                        __FILE__, __LINE__, [=]() -> CompositeCombineFixture * {
                          return new CompositeCombineCRUD(tuple);
                        });
#endif /*   */
                  }
                  ++count;
                }
              }
            }
          }
        }
      }
    }
  }
  return count;
}

//----------------------------------------------------------------------------

int main(int argc, char **argv) {
  printf("Total CompositeTest Combinations %u\n", CompositeTest_Combine(true));
  fflush(nullptr);
#ifdef INSTANTIATE_TEST_SUITE_P
  CompositeTest_Combine(false);
#else
  (void)NNN_WITHDUP;
  (void)NNN_UNIQ;
  (void)megabytes;
  (void)NBATCH;
  (void)NNN_UNIQ;
#endif /* INSTANTIATE_TEST_SUITE_P */
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
