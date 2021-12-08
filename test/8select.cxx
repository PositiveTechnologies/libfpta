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

static const char testdb_name[] = TEST_DB_DIR "ut_select.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_select.fpta" MDBX_LOCK_SUFFIX;

//----------------------------------------------------------------------------

TEST(Select, SmokeFilter) {
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
  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_sync, fpta_regime_default,
                                  1, true, &db));
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
  ASSERT_STREQ(nullptr, fptu::check(pt));

  fptu_time datetime;
  datetime.fixedpoint = 1492170771;
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_num, fpta_value_sint(16)));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(pt, &col_date, fpta_value_datetime(datetime)));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_str,
                                        fpta_value_sint(6408824664381050880)));
  ASSERT_STREQ(nullptr, fptu::check(pt));
  fptu_ro row = fptu_take_noshrink(pt);
  ASSERT_STREQ(nullptr, fptu::check(row));
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

class Select
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

    SCOPED_TRACE("index " + std::to_string(index) + ", ordering " +
                 std::to_string(ordering) +
                 (valid_ops ? ", (valid case)" : ", (invalid case)"));

    // инициализируем идентификаторы таблицы и её колонок
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_1, "col_1"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_2, "col_2"));

    skipped = GTEST_IS_EXECUTION_TIMEOUT();
    if (!valid_ops || skipped)
      return;

    if (REMOVE_FILE(testdb_name) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }
    if (REMOVE_FILE(testdb_name_lck) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }

    // открываем/создаем базульку в 1 мегабайт
    fpta_db *db = nullptr;
    ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                    1, true, &db));
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
    ASSERT_STREQ(nullptr, fptu::check(pt));

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
      ASSERT_STREQ(nullptr, fptu::check(pt));

      ASSERT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take_noshrink(pt)));
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

//----------------------------------------------------------------------------

TEST(Select, ChoppedLookup) {
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

  { // create table
    fpta_column_set def;
    fpta_column_set_init(&def);

    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "_id", fptu_uint64,
                           fpta_secondary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "_last_changed", fptu_datetime,
                           fpta_secondary_withdups_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("id", fptu_cstr,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("description", fptu_cstr,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "score", fptu_uint64,
                           fpta_secondary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "threat_type", fptu_cstr,
                           fpta_secondary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "hash_sha256", fptu_cstr,
                  fpta_secondary_withdups_ordered_obverse_nullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "hash_sha1", fptu_cstr,
                  fpta_secondary_withdups_ordered_obverse_nullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe(
                  "hash_md5", fptu_cstr,
                  fpta_secondary_withdups_ordered_obverse_nullable, &def));

    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);
    EXPECT_EQ(FPTA_OK,
              fpta_table_create(
                  txn, "repListHashes_nokind_CybsiExperts_without_kind", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    txn = nullptr;

    // разрушаем описание таблицы
    EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  }
  EXPECT_EQ(FPTA_OK, fpta_db_close(db));
  db = nullptr;

  ASSERT_EQ(FPTA_OK,
            test_db_open(testdb_name, fpta_weak, fpta_saferam, 1, false, &db));
  ASSERT_NE(nullptr, db);

  fpta_name table, _id, date, id_str, desc, score, threat, sha256, sha1, md5;
  EXPECT_EQ(FPTA_OK,
            fpta_table_init(&table,
                            "repListHashes_nokind_CybsiExperts_without_kind"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &_id, "_id"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &date, "_last_changed"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &id_str, "id"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &desc, "description"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &score, "score"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &threat, "threat_type"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &sha256, "hash_sha256"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &sha1, "hash_sha1"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &md5, "hash_md5"));

  // start write-transaction
  fpta_txn *txn = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);

  const std::string md5_content(
      "DA2A486F74498E403B8F28DA7B0D1BD76930BFAFF840C60CA4591340FBECEAF6");
  {
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));

    fptu_rw *tuple = fptu_alloc(9, 2000);
    ASSERT_NE(nullptr, tuple);

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &_id));
    uint64_t result = 0;
    EXPECT_EQ(FPTA_OK, fpta_table_sequence(txn, &table, &result, 1));
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(tuple, &_id, fpta_value_uint(result)));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &date));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                           tuple, &date, fpta_value_datetime(fptu_now_fine())));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &id_str));
    const std::string id_str_content("Bad_file");
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &id_str,
                                          fpta_value_str(id_str_content)));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &desc));
    const std::string desc_content("bad bad file");
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(tuple, &desc, fpta_value_str(desc_content)));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &score));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &score, fpta_value_uint(91)));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &threat));
    std::string threat_content("oooooh so bad file!");
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &threat,
                                          fpta_value_str(threat_content)));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &sha256));
    const std::string sha256_content(
        "BE148EA7ECA5A37AAB92FE2967AE425B8C7D4BC80DEC8099BE25CA5EC309989D");
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &sha256,
                                          fpta_value_str(sha256_content)));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &sha1));
    std::string sha1_content("BE148EA7ECA5A37");
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(tuple, &sha1, fpta_value_str(sha1_content)));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &md5));
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(tuple, &md5, fpta_value_str(md5_content)));

    EXPECT_EQ(FPTA_OK,
              fpta_probe_and_upsert_row(txn, &table, fptu_take(tuple)));

    EXPECT_EQ(FPTU_OK, fptu_clear(tuple));
    free(tuple);
  }
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // start read transaction
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);
  {
    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &md5));

    fpta_filter filter;
    filter.type = fpta_node_eq;
    filter.node_cmp.left_id = &md5;
    filter.node_cmp.right_value = fpta_value_str(md5_content);

    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK,
              fpta_cursor_open(txn, &md5, fpta_value_begin(), fpta_value_end(),
                               &filter, fpta_unsorted, &cursor));
    ASSERT_NE(nullptr, txn);
    EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
    cursor = nullptr;

    std::string md5_left(md5_content.substr(0, fpta_bits::fpta_max_keylen - 1));
    std::string md5_right = md5_left;
    md5_right.back() += 1;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &md5, fpta_value_str(md5_left),
                                        fpta_value_str(md5_right), &filter,
                                        fpta_unsorted, &cursor));
    ASSERT_NE(nullptr, txn);
    EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
    cursor = nullptr;
  }
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  // разрушаем привязанные идентификаторы
  fpta_name_destroy(&table);
  fpta_name_destroy(&_id);
  fpta_name_destroy(&date);
  fpta_name_destroy(&id_str);
  fpta_name_destroy(&desc);
  fpta_name_destroy(&score);
  fpta_name_destroy(&threat);
  fpta_name_destroy(&sha256);
  fpta_name_destroy(&sha1);
  fpta_name_destroy(&md5);

  EXPECT_EQ(FPTA_OK, fpta_db_close(db));
  db = nullptr;
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

//----------------------------------------------------------------------------

TEST_P(Select, Range) {
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
  // проверяем кол-во записей
  size_t count;
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(42u, count);
  // проверяем статистику операций
  fpta_cursor_stat stat;
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  EXPECT_EQ(0u, stat.index_searches);
  EXPECT_EQ(((ordering & fpta_dont_fetch) ? 0u : 1u /* open-first */) +
                1u /* count-first */ + 42u /* count-next */,
            stat.index_scans);
  EXPECT_EQ(0u, stat.pk_lookups);
  EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                         : 2u /* open-first + count */,
            stat.results);
  // закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем простейщий курсор c диапазоном (полное покрытие)
  if (fpta_index_is_ordered(index)) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(
                           txn_guard.get(), &col_1, fpta_value_sint(-1),
                           fpta_value_sint(43), nullptr, ordering, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    // проверяем кол-во записей
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(42u, count);
    // проверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    if (ordering & fpta_descending) {
      if (ordering & fpta_dont_fetch) {
        EXPECT_EQ(1u /* range-end */, stat.index_searches);
        EXPECT_EQ(42u /* next */, stat.index_scans);
      } else {
        EXPECT_EQ(1u * 2 /* range-end */, stat.index_searches);
        EXPECT_EQ(42u /* next */, stat.index_scans);
      }
    } else {
      if (ordering & fpta_dont_fetch) {
        EXPECT_EQ(1u /* range-begin */, stat.index_searches);
        EXPECT_EQ(42u /* next */, stat.index_scans);
      } else {
        EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
        EXPECT_EQ(42u /* next */, stat.index_scans);
      }
    }
    EXPECT_EQ(0u, stat.pk_lookups);
    EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                           : 2u /* open-first + count */,
              stat.results);
    // закрываем курсор
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
  if (fpta_index_is_ordered(index)) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn_guard.get(), &col_1,
                                        fpta_value_begin(), fpta_value_sint(43),
                                        nullptr, ordering, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    // проверяем кол-во записей
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(42u, count);
    // проверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    if (ordering & fpta_descending) {
      if (ordering & fpta_dont_fetch) {
        EXPECT_EQ(1u /* range-end */, stat.index_searches);
        EXPECT_EQ(42u /* next */, stat.index_scans);
      } else {
        EXPECT_EQ(1u * 2 /* range-end */, stat.index_searches);
        EXPECT_EQ(42u /* next */, stat.index_scans);
      }
    } else {
      if (ordering & fpta_dont_fetch) {
        EXPECT_EQ(0u, stat.index_searches);
        EXPECT_EQ(1u /* first */ + 42u /* next */, stat.index_scans);
      } else {
        EXPECT_EQ(0u * 2, stat.index_searches);
        EXPECT_EQ(1u * 2 /* first */ + 42u /* next */, stat.index_scans);
      }
    }
    EXPECT_EQ(0u, stat.pk_lookups);
    EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                           : 2u /* open-first + count */,
              stat.results);
    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
  } else {
    EXPECT_EQ(FPTA_NO_INDEX,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                               fpta_value_sint(43), nullptr, ordering,
                               &cursor));
    ASSERT_EQ(nullptr, cursor);
  }

  // открываем простейщий курсор c диапазоном (полное покрытие, до begin)
  if (fpta_index_is_ordered(index)) {
    EXPECT_EQ(FPTA_OK,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(-1),
                               fpta_value_end(), nullptr, ordering, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    // проверяем кол-во записей
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(42u, count);
    // проверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    if (ordering & fpta_descending) {
      if (ordering & fpta_dont_fetch) {
        EXPECT_EQ(0u, stat.index_searches);
        EXPECT_EQ(1u /* last */ + 42u /* next */, stat.index_scans);
      } else {
        EXPECT_EQ(0u * 2, stat.index_searches);
        EXPECT_EQ(1u * 2 /* last */ + 42u /* next */, stat.index_scans);
      }
    } else {
      if (ordering & fpta_dont_fetch) {
        EXPECT_EQ(1u /* range-begin */, stat.index_searches);
        EXPECT_EQ(42u /* next */, stat.index_scans);
      } else {
        EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
        EXPECT_EQ(42u /* next */, stat.index_scans);
      }
    }
    EXPECT_EQ(0u, stat.pk_lookups);
    EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                           : 2u /* open-first + count */,
              stat.results);
    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
  } else {
    EXPECT_EQ(FPTA_NO_INDEX,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(-1),
                               fpta_value_end(), nullptr, ordering, &cursor));
    ASSERT_EQ(nullptr, cursor);
  }

  // открываем c диапазоном (без пересечения, нулевой диапазон)
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(
                           txn_guard.get(), &col_1, fpta_value_sint(17),
                           fpta_value_sint(17), nullptr, ordering, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    // проверяем кол-во записей
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(0u, count);
    // проверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    if (ordering & fpta_descending) {
      if (ordering & fpta_dont_fetch) {
        EXPECT_EQ(1u /* range-begin */, stat.index_searches);
        EXPECT_EQ(2u /* next+back */, stat.index_scans);
      } else {
        EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
        EXPECT_EQ(1u * 2 /* next */, stat.index_scans);
      }
    } else {
      if (ordering & fpta_dont_fetch) {
        EXPECT_EQ(1u /* range-begin */, stat.index_searches);
        EXPECT_EQ(0u /* next */, stat.index_scans);
      } else {
        EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
        EXPECT_EQ(0u * 2 /* next */, stat.index_scans);
      }
    }
    EXPECT_EQ(0u, stat.pk_lookups);
    EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                           : 2u /* open-first + count */,
              stat.results);
    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
    // повторяем с fpta_zeroed_range_is_point
    EXPECT_EQ(FPTA_OK,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(17),
                               fpta_value_sint(17), nullptr,
                               ordering | fpta_zeroed_range_is_point, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(1u, count);
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    if (ordering & fpta_descending) {
      if (ordering & fpta_dont_fetch) {
        EXPECT_EQ(1u /* range-begin */, stat.index_searches);
        EXPECT_EQ(2u /* next */, stat.index_scans);
      } else {
        EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
        EXPECT_EQ(2u * 2 /* next */, stat.index_scans);
      }
    } else {
      if (ordering & fpta_dont_fetch) {
        EXPECT_EQ(1u /* range-begin */, stat.index_searches);
        EXPECT_EQ(1u /* next */, stat.index_scans);
      } else {
        EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
        EXPECT_EQ(1u * 2 /* next */, stat.index_scans);
      }
    }
    EXPECT_EQ(0u, stat.pk_lookups);
    EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                           : 2u /* open-first + count */,
              stat.results);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
  } else {
    EXPECT_EQ(FPTA_NODATA,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(17),
                               fpta_value_sint(17), nullptr, ordering,
                               &cursor));
    ASSERT_EQ(nullptr, cursor);
  }

  if (fpta_index_is_unordered(index)) {
    // для unordered индексов тесты ниже вернут FPTA_NO_INDEX
    // и это уже было проверенно выше
    return;
  }

  // открываем c диапазоном (нулевое пересечение, курсор "ниже")
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(-42),
                               fpta_value_sint(0), nullptr, ordering, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    // проверяем кол-во записей
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(0u, count);
    // проверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    if (ordering & fpta_descending) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(2u /* next */, stat.index_scans);
    } else {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(0u /* next */, stat.index_scans);
    }
    EXPECT_EQ(0u, stat.pk_lookups);
    EXPECT_EQ(1u /* count */, stat.results);
    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
    // повторяем с fpta_zeroed_range_is_point
    EXPECT_EQ(FPTA_OK,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(-42),
                               fpta_value_sint(0), nullptr,
                               ordering | fpta_zeroed_range_is_point, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(0u, count);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
  } else {
    EXPECT_EQ(FPTA_NODATA,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(-42),
                               fpta_value_sint(0), nullptr, ordering, &cursor));
    ASSERT_EQ(nullptr, cursor);
  }

  // открываем c диапазоном (нулевое пересечение, курсор "выше")
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(
                           txn_guard.get(), &col_1, fpta_value_sint(42),
                           fpta_value_sint(100), nullptr, ordering, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    // проверяем кол-во записей
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(0u, count);
    // проверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    if (ordering & fpta_descending) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(0u, stat.index_scans);
    } else {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(0u, stat.index_scans);
    }
    EXPECT_EQ(0u, stat.pk_lookups);
    EXPECT_EQ(1u /* count */, stat.results);
    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
    // повторяем с fpta_zeroed_range_is_point
    EXPECT_EQ(FPTA_OK,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(42),
                               fpta_value_sint(100), nullptr,
                               ordering | fpta_zeroed_range_is_point, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(0u, count);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
  } else {
    EXPECT_EQ(FPTA_NODATA,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(42),
                               fpta_value_sint(100), nullptr, ordering,
                               &cursor));
    ASSERT_EQ(nullptr, cursor);
  }

  // открываем c диапазоном (единичное пересечение, курсор "снизу")
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(-42),
                             fpta_value_sint(1), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
  // проверяем статистику операций
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  if (ordering & fpta_descending) {
    if (ordering & fpta_dont_fetch) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(2u /* first+back */ + 1 /* next */, stat.index_scans);
    } else {
      EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
      EXPECT_EQ(2u * 2 /* first+back */ + 1 /* next */, stat.index_scans);
    }
  } else {
    if (ordering & fpta_dont_fetch) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(1u /* next */, stat.index_scans);
    } else {
      EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
      EXPECT_EQ(1u /* next */, stat.index_scans);
    }
  }
  EXPECT_EQ(0u, stat.pk_lookups);
  EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                         : 2u /* open-first + count */,
            stat.results);
  // закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;
  // повторяем с fpta_zeroed_range_is_point
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(-42),
                             fpta_value_sint(1), nullptr,
                             ordering | fpta_zeroed_range_is_point, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
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
  // проверяем кол-во записей
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
  // проверяем статистику операций
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  if (ordering & fpta_descending) {
    if (ordering & fpta_dont_fetch) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(1u /* next */, stat.index_scans);
    } else {
      EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
      EXPECT_EQ(1u /* next */, stat.index_scans);
    }
  } else {
    if (ordering & fpta_dont_fetch) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(1u /* next */, stat.index_scans);
    } else {
      EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
      EXPECT_EQ(1u /* next */, stat.index_scans);
    }
  }
  EXPECT_EQ(0u, stat.pk_lookups);
  EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                         : 2u /* open-first + count */,
            stat.results);
  // закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;
  // повторяем с fpta_zeroed_range_is_point
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(41),
                             fpta_value_sint(100), nullptr,
                             ordering | fpta_zeroed_range_is_point, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
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
  // проверяем кол-во записей
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(21u, count);
  // проверяем статистику операций
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  if (ordering & fpta_descending) {
    if (ordering & fpta_dont_fetch) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(2u /* first+back */ + 21 /* next */, stat.index_scans);
    } else {
      EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
      EXPECT_EQ(2u * 2 /* first+back */ + 21 /* next */, stat.index_scans);
    }
  } else {
    if (ordering & fpta_dont_fetch) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(21u /* next */, stat.index_scans);
    } else {
      EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
      EXPECT_EQ(21u /* next */, stat.index_scans);
    }
  }
  EXPECT_EQ(0u, stat.pk_lookups);
  EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                         : 2u /* open-first + count */,
            stat.results);
  // закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;
  // повторяем с fpta_zeroed_range_is_point
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(-100),
                             fpta_value_sint(21), nullptr,
                             ordering | fpta_zeroed_range_is_point, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
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
  // проверяем кол-во записей
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(21u, count);
  // проверяем статистику операций
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  if (ordering & fpta_descending) {
    if (ordering & fpta_dont_fetch) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(21u /* next */, stat.index_scans);
    } else {
      EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
      EXPECT_EQ(21u /* next */, stat.index_scans);
    }
  } else {
    if (ordering & fpta_dont_fetch) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(21u /* next */, stat.index_scans);
    } else {
      EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
      EXPECT_EQ(21u /* next */, stat.index_scans);
    }
  }
  EXPECT_EQ(0u, stat.pk_lookups);
  EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                         : 2u /* open-first + count */,
            stat.results);
  // закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;
  // повторяем с fpta_zeroed_range_is_point
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(21),
                             fpta_value_sint(100), nullptr,
                             ordering | fpta_zeroed_range_is_point, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
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
  // проверяем кол-во записей
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(21u, count);
  // проверяем статистику операций
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  if (ordering & fpta_descending) {
    if (ordering & fpta_dont_fetch) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(2u /* first+back */ + 21 /* next */, stat.index_scans);
    } else {
      EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
      EXPECT_EQ(2u * 2 /* first+back */ + 21 /* next */, stat.index_scans);
    }
  } else {
    if (ordering & fpta_dont_fetch) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(21u /* next */, stat.index_scans);
    } else {
      EXPECT_EQ(1u * 2 /* range-begin */, stat.index_searches);
      EXPECT_EQ(21u /* next */, stat.index_scans);
    }
  }
  EXPECT_EQ(0u, stat.pk_lookups);
  EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                         : 2u /* open-first + count */,
            stat.results);
  // закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;
  // повторяем с fpta_zeroed_range_is_point
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(10),
                             fpta_value_sint(31), nullptr,
                             ordering | fpta_zeroed_range_is_point, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(21u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // открываем c диапазоном (без пересечения, "отрицательный" диапазон)
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(
                           txn_guard.get(), &col_1, fpta_value_sint(31),
                           fpta_value_sint(10), nullptr, ordering, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    // проверяем кол-во записей
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(0u, count);
    // проверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    if (ordering & fpta_descending) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(1u, stat.index_scans);
    } else {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(0u, stat.index_scans);
    }
    EXPECT_EQ(0u, stat.pk_lookups);
    EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                           : 2u /* open-first + count */,
              stat.results);
    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
    // повторяем с fpta_zeroed_range_is_point
    EXPECT_EQ(FPTA_OK,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(31),
                               fpta_value_sint(10), nullptr,
                               ordering | fpta_zeroed_range_is_point, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(0u, count);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
  } else {
    EXPECT_EQ(FPTA_NODATA,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(31),
                               fpta_value_sint(10), nullptr, ordering,
                               &cursor));
    ASSERT_EQ(nullptr, cursor);
  }
}

//----------------------------------------------------------------------------

TEST_P(Select, RangeEpsilon) {
  /* Smoke-проверка жизнеспособности курсоров с ограничениями диапазона.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей, в которой две колонки
   *     и один (primary) индекс.
   *
   *  2. Вставляем 42 строки, с последовательным увеличением
   *     значения в первой колонке.
   *
   *  3. Несколько раз открываем курсор с разнымм диапазонами c fpta_epsilon
   *     и проверяем кол-во строк попадающее в выборку.
   *
   *  4. Завершаем операции и освобождаем ресурсы.
   */
  SCOPED_TRACE("index " + std::to_string(index) + ", ordering " +
               std::to_string(ordering) +
               (valid_ops ? ", (valid case)" : ", (invalid case)"));
  if (!valid_ops || skipped)
    return;

  fpta_cursor *cursor;
  size_t count;
  fpta_value key_value;
  fpta_cursor_stat stat;

  // begin, epsilon
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                             fpta_value_epsilon(), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем статистику операций
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(0u, stat.index_searches);
    EXPECT_EQ(1u /* first to get epsilon base */, stat.index_scans);
    // явно устанавливаем курсор, если задана опция fpta_dont_fetch
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
    // перепроверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    EXPECT_EQ(1u /* seek to epsilon base */, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 2u : 1u, stat.index_scans);
  } else {
    EXPECT_EQ(1u, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 2u /* first & epsilon base */ : 1,
              stat.index_scans);
  }
  // проверяем значение ключа
  EXPECT_EQ(FPTA_OK, fpta_cursor_key(cursor, &key_value));
  EXPECT_EQ(fpta_signed_int, key_value.type);
  if (ordering & fpta_descending) {
    EXPECT_EQ(41, key_value.sint);
  } else {
    EXPECT_EQ(0, key_value.sint);
  }
  // проверяем статистику операций
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  EXPECT_EQ(0u, stat.pk_lookups);
  EXPECT_EQ(1u /* count */, stat.results);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // epsilon, begin
  EXPECT_EQ(FPTA_EINVAL,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_epsilon(),
                             fpta_value_begin(), nullptr, ordering, &cursor));
  ASSERT_EQ(nullptr, cursor);

  // end, epsilon
  EXPECT_EQ(FPTA_EINVAL,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_end(),
                             fpta_value_epsilon(), nullptr, ordering, &cursor));
  ASSERT_EQ(nullptr, cursor);

  // epsilon, end
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_epsilon(),
                             fpta_value_end(), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем статистику операций
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(0u, stat.index_searches);
    EXPECT_EQ(1u /* first to get epsilon base */, stat.index_scans);
    // явно устанавливаем курсор, если задана опция fpta_dont_fetch
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
    // перепроверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    EXPECT_EQ(1u /* seek to epsilon base */, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 2u : 1u, stat.index_scans);
  } else {
    EXPECT_EQ(1u, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 2u /* first & epsilon base */ : 1,
              stat.index_scans);
  }
  // проверяем значение ключа
  EXPECT_EQ(FPTA_OK, fpta_cursor_key(cursor, &key_value));
  EXPECT_EQ(fpta_signed_int, key_value.type);
  if (ordering & fpta_descending) {
    EXPECT_EQ(0, key_value.sint);
  } else {
    EXPECT_EQ(41, key_value.sint);
  }
  // проверяем статистику операций
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  EXPECT_EQ(0u, stat.pk_lookups);
  EXPECT_EQ(1u /* count */, stat.results);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // epsilon, epsilon
  EXPECT_EQ(FPTA_EINVAL,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_epsilon(),
                             fpta_value_epsilon(), nullptr, ordering, &cursor));
  ASSERT_EQ(nullptr, cursor);

  // middle, epsilon
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(3),
                             fpta_value_epsilon(), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем статистику операций
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(0u, stat.index_searches);
    EXPECT_EQ(0u, stat.index_scans);
    // явно устанавливаем курсор, если задана опция fpta_dont_fetch
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
    // перепроверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    EXPECT_EQ(1u /* seek to epsilon base */, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 1u : 0u, stat.index_scans);
  } else {
    EXPECT_EQ(1u, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 1u : 0u, stat.index_scans);
  }
  // проверяем значение ключа
  EXPECT_EQ(FPTA_OK, fpta_cursor_key(cursor, &key_value));
  EXPECT_EQ(fpta_signed_int, key_value.type);
  EXPECT_EQ(3, key_value.sint);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // epsilon, middle
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_epsilon(),
                             fpta_value_sint(3), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем статистику операций
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(0u, stat.index_searches);
    EXPECT_EQ(0u, stat.index_scans);
    // явно устанавливаем курсор, если задана опция fpta_dont_fetch
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
    // перепроверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    EXPECT_EQ(1u /* seek to epsilon base */, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 1u : 0u, stat.index_scans);
  } else {
    EXPECT_EQ(1u, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 1u : 0u, stat.index_scans);
  }
  // проверяем значение ключа
  EXPECT_EQ(FPTA_OK, fpta_cursor_key(cursor, &key_value));
  EXPECT_EQ(fpta_signed_int, key_value.type);
  EXPECT_EQ(3, key_value.sint);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // first, epsilon
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(0),
                             fpta_value_epsilon(), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем статистику операций
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(0u, stat.index_searches);
    EXPECT_EQ(0u, stat.index_scans);
    // явно устанавливаем курсор, если задана опция fpta_dont_fetch
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
    // перепроверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    EXPECT_EQ(1u /* seek to epsilon base */, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 1u : 0u, stat.index_scans);
  } else {
    EXPECT_EQ(1u, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 1u : 0u, stat.index_scans);
  }
  // проверяем значение ключа
  EXPECT_EQ(FPTA_OK, fpta_cursor_key(cursor, &key_value));
  EXPECT_EQ(fpta_signed_int, key_value.type);
  EXPECT_EQ(0, key_value.sint);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // epsilon, first
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_epsilon(),
                             fpta_value_sint(0), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем статистику операций
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(0u, stat.index_searches);
    EXPECT_EQ(0u, stat.index_scans);
    // явно устанавливаем курсор, если задана опция fpta_dont_fetch
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
    // перепроверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    EXPECT_EQ(1u /* seek to epsilon base */, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 1u : 0u, stat.index_scans);
  } else {
    EXPECT_EQ(1u, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 1u : 0u, stat.index_scans);
  }
  // проверяем значение ключа
  EXPECT_EQ(FPTA_OK, fpta_cursor_key(cursor, &key_value));
  EXPECT_EQ(fpta_signed_int, key_value.type);
  EXPECT_EQ(0, key_value.sint);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // last, epsilon
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(41),
                             fpta_value_epsilon(), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем статистику операций
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(0u, stat.index_searches);
    EXPECT_EQ(0u, stat.index_scans);
    // явно устанавливаем курсор, если задана опция fpta_dont_fetch
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
    // перепроверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    EXPECT_EQ(1u /* seek to epsilon base */, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 1u : 0u, stat.index_scans);
  } else {
    EXPECT_EQ(1u, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 1u : 0u, stat.index_scans);
  }
  // проверяем значение ключа
  EXPECT_EQ(FPTA_OK, fpta_cursor_key(cursor, &key_value));
  EXPECT_EQ(fpta_signed_int, key_value.type);
  EXPECT_EQ(41, key_value.sint);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // epsilon, last
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_epsilon(),
                             fpta_value_sint(41), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем статистику операций
  ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(0u, stat.index_searches);
    EXPECT_EQ(0u, stat.index_scans);
    // явно устанавливаем курсор, если задана опция fpta_dont_fetch
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
    // перепроверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    EXPECT_EQ(1u /* seek to epsilon base */, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 1u : 0u, stat.index_scans);
  } else {
    EXPECT_EQ(1u, stat.index_searches);
    EXPECT_EQ((ordering & fpta_descending) ? 1u : 0u, stat.index_scans);
  }
  // проверяем значение ключа
  EXPECT_EQ(FPTA_OK, fpta_cursor_key(cursor, &key_value));
  EXPECT_EQ(fpta_signed_int, key_value.type);
  EXPECT_EQ(41, key_value.sint);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // before-first, epsilon
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(
                           txn_guard.get(), &col_1, fpta_value_sint(-1),
                           fpta_value_epsilon(), nullptr, ordering, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    // проверяем кол-во записей
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(0u, count);
    // проверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    if (ordering & fpta_descending) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(1u /* prev */, stat.index_scans);
    } else {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(0u, stat.index_scans);
    }
    EXPECT_EQ(0u, stat.pk_lookups);
    EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                           : 2u /* open-first + count */,
              stat.results);
    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
  } else {
    EXPECT_EQ(FPTA_NODATA,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(-1),
                               fpta_value_epsilon(), nullptr, ordering,
                               &cursor));
    ASSERT_EQ(nullptr, cursor);
  }

  // epsilon, before-first
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(
                           txn_guard.get(), &col_1, fpta_value_epsilon(),
                           fpta_value_sint(-1), nullptr, ordering, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    // проверяем кол-во записей
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(0u, count);
    // проверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    if (ordering & fpta_descending) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(1u /* prev */, stat.index_scans);
    } else {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(0u, stat.index_scans);
    }
    EXPECT_EQ(0u, stat.pk_lookups);
    EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                           : 2u /* open-first + count */,
              stat.results);
    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
  } else {
    EXPECT_EQ(FPTA_NODATA,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_epsilon(),
                               fpta_value_sint(-1), nullptr, ordering,
                               &cursor));
    ASSERT_EQ(nullptr, cursor);
  }

  // after-last, epsilon
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(
                           txn_guard.get(), &col_1, fpta_value_sint(42),
                           fpta_value_epsilon(), nullptr, ordering, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    // проверяем кол-во записей
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(0u, count);
    // проверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    if (ordering & fpta_descending) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(0u, stat.index_scans);
    } else {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(0u, stat.index_scans);
    }
    EXPECT_EQ(0u, stat.pk_lookups);
    EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                           : 2u /* open-first + count */,
              stat.results);
    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
  } else {
    EXPECT_EQ(FPTA_NODATA,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(42),
                               fpta_value_epsilon(), nullptr, ordering,
                               &cursor));
    ASSERT_EQ(nullptr, cursor);
  }

  // epsilon, after-last
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(
                           txn_guard.get(), &col_1, fpta_value_epsilon(),
                           fpta_value_sint(42), nullptr, ordering, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    // проверяем кол-во записей
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(0u, count);
    // проверяем статистику операций
    ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
    if (ordering & fpta_descending) {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(0u, stat.index_scans);
    } else {
      EXPECT_EQ(1u /* range-begin */, stat.index_searches);
      EXPECT_EQ(0u, stat.index_scans);
    }
    EXPECT_EQ(0u, stat.pk_lookups);
    EXPECT_EQ((ordering & fpta_dont_fetch) ? 1u /* count */
                                           : 2u /* open-first + count */,
              stat.results);
    // закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
  } else {
    EXPECT_EQ(FPTA_NODATA,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_epsilon(),
                               fpta_value_sint(42), nullptr, ordering,
                               &cursor));
    ASSERT_EQ(nullptr, cursor);
  }
}

//----------------------------------------------------------------------------

static bool filter_row_predicate_true(const fptu_ro *, void *, void *) {
  return true;
}

static bool filter_row_predicate_false(const fptu_ro *, void *, void *) {
  return false;
}

static bool filter_col_predicate_odd(const fptu_field *column, void *) {
  return (fptu_field_int32(column) & 1) != 0;
}

TEST_P(Select, Filter) {
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
  if (ordering & fpta_dont_fetch) {
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
  } else {
    EXPECT_EQ(FPTA_NODATA,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                               fpta_value_end(), &filter, ordering, &cursor));
    ASSERT_EQ(nullptr, cursor);
  }

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
  if (fpta_index_is_ordered(index)) {
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
  } else {
    EXPECT_EQ(FPTA_NO_INDEX,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                               fpta_value_uint(5), &filter, ordering, &cursor));
    ASSERT_EQ(nullptr, cursor);
  }

  // меняем фильтр на "больше или равно" и открываем курсор с диапазоном,
  // который имеет только одну "общую" запись с условием фильтра.
  filter.type = fpta_node_ge;
  if (fpta_index_is_ordered(index)) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn_guard.get(), &col_1,
                                        fpta_value_begin(), fpta_value_uint(11),
                                        &filter, ordering, &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
    // проверяем кол-во записей и закрываем курсор
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
    EXPECT_EQ(1u, count);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
    cursor = nullptr;
  } else {
    EXPECT_EQ(FPTA_NO_INDEX,
              fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                               fpta_value_uint(11), &filter, ordering,
                               &cursor));
    ASSERT_EQ(nullptr, cursor);
  }
}

#ifdef INSTANTIATE_TEST_SUITE_P
INSTANTIATE_TEST_SUITE_P(
    Combine, Select,
    ::testing::Combine(::testing::Values(fpta_primary_unique_ordered_obverse,
                                         fpta_primary_withdups_ordered_obverse,
                                         fpta_primary_unique_unordered,
                                         fpta_primary_withdups_unordered),
                       ::testing::Values(fpta_unsorted, fpta_ascending,
                                         fpta_descending,
                                         fpta_unsorted_dont_fetch,
                                         fpta_ascending_dont_fetch,
                                         fpta_descending_dont_fetch)));
#else
INSTANTIATE_TEST_CASE_P(
    Combine, Select,
    ::testing::Combine(::testing::Values(fpta_primary_unique_ordered_obverse,
                                         fpta_primary_withdups_ordered_obverse,
                                         fpta_primary_unique_unordered,
                                         fpta_primary_withdups_unordered),
                       ::testing::Values(fpta_unsorted, fpta_ascending,
                                         fpta_descending,
                                         fpta_unsorted_dont_fetch,
                                         fpta_ascending_dont_fetch,
                                         fpta_descending_dont_fetch)));
#endif

//----------------------------------------------------------------------------

class Metrics : public ::testing::TestWithParam<GTEST_TUPLE_NAMESPACE_::tuple<
                    fpta_index_type, fpta_cursor_options, unsigned>> {
public:
  scoped_db_guard db_quard;
  scoped_txn_guard txn_guard;
  scoped_cursor_guard cursor_guard;

  fpta_name table, col_1, col_2;
  fpta_index_type index;
  fpta_cursor_options ordering;
  int reps_case, first, last;
  bool valid_ops, skipped;

  unsigned reps(const unsigned i) const {
    return (i * 35059 + reps_case) * 56767 % 5;
  }

  virtual void SetUp() {
    index = GTEST_TUPLE_NAMESPACE_::get<0>(GetParam());
    ordering = GTEST_TUPLE_NAMESPACE_::get<1>(GetParam());
    reps_case = GTEST_TUPLE_NAMESPACE_::get<2>(GetParam());
    valid_ops =
        is_valid4primary(fptu_int32, index) && is_valid4cursor(index, ordering);

    SCOPED_TRACE("index " + std::to_string(index) + ", ordering " +
                 std::to_string(ordering) +
                 (valid_ops ? ", (valid case)" : ", (invalid case)"));

    // инициализируем идентификаторы таблицы и её колонок
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_1, "col_1"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_2, "col_2"));

    skipped = GTEST_IS_EXECUTION_TIMEOUT();
    if (!valid_ops || skipped)
      return;

    if (REMOVE_FILE(testdb_name) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }
    if (REMOVE_FILE(testdb_name_lck) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }

    // открываем/создаем базульку в 1 мегабайт
    fpta_db *db = nullptr;
    ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                    1, true, &db));
    ASSERT_NE(nullptr, db);
    db_quard.reset(db);

    // описываем простейшую таблицу с двумя колонками
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK, fpta_column_describe("col_1", fptu_int32, index, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "col_2", fptu_int32,
                           fpta_index_is_primary(index)
                               ? fpta_index_none
                               : fpta_primary_unique_ordered_reverse_nullable,
                           &def));
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
    fptu_rw *pt = fptu_alloc(2, 8);
    ASSERT_NE(nullptr, pt);
    ASSERT_STREQ(nullptr, fptu::check(pt));

    // делаем привязку к схеме
    fpta_name_refresh_couple(txn, &table, &col_1);
    fpta_name_refresh(txn, &col_2);

    // заполняем таблицу со стохастическим кол-вом дубликатов для каждого ключа
    first = last = -1;
    for (unsigned i = 0; i < 42; ++i) {
      const unsigned n = reps(i);
      if (n) {
        if (first < 0)
          first = int(i);
        last = int(i);
      }
      for (unsigned k = 0; k < n; ++k) {
        EXPECT_EQ(FPTA_OK, fpta_upsert_column(pt, &col_1, fpta_value_sint(i)));
        uint64_t seq;
        EXPECT_EQ(FPTA_OK, fpta_db_sequence(txn, &seq, 1));
        EXPECT_EQ(FPTA_OK,
                  fpta_upsert_column(pt, &col_2, fpta_value_sint(seq)));
        ASSERT_STREQ(nullptr, fptu::check(pt));
        ASSERT_EQ(FPTA_OK,
                  fpta_insert_row(txn, &table, fptu_take_noshrink(pt)));
      }
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

  void Check(const fpta_value &from, const fpta_value &to,
             const bool expect_bsearch, const unsigned n,
             const int expect_value = -1) {
    const fpta_cursor_options options =
        (from.type < fpta_begin && to.type < fpta_begin)
            ? ordering | fpta_zeroed_range_is_point
            : ordering;
    fpta_cursor_stat stat;
    fpta_cursor *cursor;
    // используем курсор БЕЗ фильтра
    if (n == 0 && (ordering & fpta_dont_fetch) == 0) {
      EXPECT_EQ(FPTA_NODATA, fpta_cursor_open(txn_guard.get(), &col_1, from, to,
                                              nullptr, options, &cursor));
      ASSERT_EQ(nullptr, cursor);
      memset(&stat, 0, sizeof(stat));
    } else {
      EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn_guard.get(), &col_1, from, to,
                                          nullptr, options, &cursor));
      ASSERT_NE(nullptr, cursor);
      cursor_guard.reset(cursor);

      int err = (ordering & fpta_dont_fetch)
                    ? fpta_cursor_move(cursor, fpta_first)
                    : int(FPTA_SUCCESS);

      uint64_t count = 0;
      while (err == FPTA_SUCCESS) {
        EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
        if (expect_value >= 0) {
          fptu_ro row;
          fpta_value value;
          EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &row));
          EXPECT_EQ(FPTA_OK, fpta_get_column(row, &col_1, &value));
          EXPECT_EQ(fpta_signed_int, value.type);
          EXPECT_EQ(expect_value, value.sint);
        }
        ++count;
        err = fpta_cursor_move(cursor, fpta_next);
      }
      EXPECT_TRUE(err == FPTA_SUCCESS || err == FPTA_NODATA);
      ASSERT_EQ(n, count);

      // получаем статистику операций и закрываем курсор
      ASSERT_EQ(FPTA_OK, fpta_cursor_info(cursor, &stat));
      EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
      cursor = nullptr;

      EXPECT_EQ(expect_bsearch ? 1u : 0u, stat.index_searches);
      EXPECT_GE(
          n + 1 + ((expect_bsearch && (ordering & fpta_descending)) ? 1u : 0u),
          stat.index_scans);
    }

    EXPECT_EQ(n, stat.results);
    if (expect_value >= 0 && !fpta_index_is_primary(index)) {
      EXPECT_EQ(n, stat.pk_lookups);
    }
    EXPECT_EQ(0u, stat.deletions);
    EXPECT_EQ(0u, stat.uniq_checks);
    EXPECT_EQ(0u, stat.upserts);
  }
};

TEST_P(Metrics, Basic) {
  /* Проверка количества операций в базовых сценариях поиска.
   *
   * Сценарий:
   *  1. Создаем базу с одной таблицей, в которой две колонки
   *     и требуемый индекс (primary, либо primary и целевой secondary).
   *
   *  2. Вставляем несколько строк со стохастическим кол-вом дубликатов
   *     в целевом индексе.
   *
   *  3. Проверяем кол-во строк попадающих в выборку
   *     и выполненное кол-во базовых операций (bsearch, scan, pklookup)
   *     для всех основных случаев:
   *      - begin..end
   *      - begin..epsilon
   *      - epsilon..end
   *    Затем для каждого возможного значения ключа:
   *      - value..epsilon и epsilon..value
   *      - value..value для одинаковых значений
   *
   *  4. Завершаем операции и освобождаем ресурсы.
   *
   *  5. Сценарий повторяется для нескольких типов индексов, курсоров
   *     и сменой смешения при стохастической генерации дубликатов (для разного
   *     количества дубликатов для наименьшего и наибольшего значений ключа в
   *     целевом индексе).
   */
  SCOPED_TRACE("index " + std::to_string(index) + ", ordering " +
               std::to_string(ordering) +
               (valid_ops ? ", (valid case)" : ", (invalid case)"));

  if (!valid_ops || skipped)
    return;

  uint64_t n;
  EXPECT_EQ(FPTA_OK, fpta_db_sequence(txn_guard.get(), &n, 0));
  Check(fpta_value_begin(), fpta_value_end(), false, unsigned(n));

  Check(fpta_value_begin(), fpta_value_epsilon(), true,
        reps((ordering & fpta_descending) ? last : first));
  Check(fpta_value_epsilon(), fpta_value_end(), true,
        reps((ordering & fpta_descending) ? first : last));

  for (unsigned i = 0; i < 42; ++i) {
    Check(fpta_value_sint(i), fpta_value_epsilon(), true, reps(i));
    Check(fpta_value_epsilon(), fpta_value_sint(i), true, reps(i));
    Check(fpta_value_sint(i), fpta_value_sint(i), true, reps(i));
  }
}

#ifdef INSTANTIATE_TEST_SUITE_P
INSTANTIATE_TEST_SUITE_P(
    Combine, Metrics,
    ::testing::Combine(
        ::testing::Values(fpta_primary_withdups_ordered_obverse,
                          fpta_primary_withdups_unordered,
                          fpta_secondary_withdups_ordered_obverse,
                          fpta_secondary_withdups_ordered_obverse_nullable,
                          fpta_secondary_withdups_ordered_reverse,
                          fpta_secondary_withdups_unordered,
                          fpta_secondary_withdups_unordered_nullable_reverse),
        ::testing::Values(fpta_unsorted, fpta_ascending, fpta_descending,
                          fpta_unsorted_dont_fetch, fpta_ascending_dont_fetch,
                          fpta_descending_dont_fetch),
        ::testing::Values(0, 1, 2, 3, 42)));
#else
INSTANTIATE_TEST_CASE_P(
    Combine, Metrics,
    ::testing::Combine(
        ::testing::Values(fpta_primary_withdups_ordered_obverse,
                          fpta_primary_withdups_unordered,
                          fpta_secondary_withdups_ordered_obverse,
                          fpta_secondary_withdups_ordered_obverse_nullable,
                          fpta_secondary_withdups_ordered_reverse,
                          fpta_secondary_withdups_unordered,
                          fpta_secondary_withdups_unordered_nullable_reverse),
        ::testing::Values(fpta_unsorted, fpta_ascending, fpta_descending,
                          fpta_unsorted_dont_fetch, fpta_ascending_dont_fetch,
                          fpta_descending_dont_fetch),
        ::testing::Values(0, 1, 2, 3, 42)));
#endif

//----------------------------------------------------------------------------

class FilterTautologyRewriter : public ::testing::Test {
public:
  bool skipped;
  fpta_db *db = nullptr;
  fpta_txn *txn = nullptr;
  fpta_name table, pk_int, se_str;

  virtual void SetUp() {
    SCOPED_TRACE("setup");
    skipped = GTEST_IS_EXECUTION_TIMEOUT();
    if (skipped)
      return;

    if (REMOVE_FILE(testdb_name) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }
    if (REMOVE_FILE(testdb_name_lck) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }

    {
      // создаем базульку в 1 мегабайт
      ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak,
                                      fpta_regime4testing, 1, true, &db));
      ASSERT_NE(nullptr, db);
      // create table
      fpta_column_set def;
      fpta_column_set_init(&def);
      EXPECT_EQ(FPTA_OK, fpta_column_describe(
                             "pk_int", fptu_int32,
                             fpta_primary_unique_ordered_obverse, &def));
      EXPECT_EQ(FPTA_OK,
                fpta_column_describe(
                    "se_str", fptu_cstr,
                    fpta_secondary_unique_unordered_nullable_obverse, &def));
      EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
      ASSERT_NE(nullptr, txn);
      EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));
      EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
      txn = nullptr;

      // разрушаем описание таблицы
      EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
      EXPECT_EQ(FPTA_OK, fpta_db_close(db));
      db = nullptr;
    }

    ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_saferam, 1,
                                    false, &db));
    ASSERT_NE(nullptr, db);

    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &pk_int, "pk_int"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &se_str, "se_str"));

    {
      // write-transaction
      EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
      ASSERT_NE(nullptr, txn);
      EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));
      EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &pk_int));
      EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &se_str));

      // insert one row
      fptu_rw *tuple = fptu_alloc(9, 2000);
      ASSERT_NE(nullptr, tuple);
      EXPECT_EQ(FPTA_OK,
                fpta_upsert_column(tuple, &pk_int, fpta_value_sint(42)));
      EXPECT_EQ(FPTA_OK,
                fpta_upsert_column(tuple, &se_str, fpta_value_cstr("42")));

      EXPECT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take(tuple)));
      EXPECT_EQ(FPTU_OK, fptu_clear(tuple));
      free(tuple);

      // commit and close
      EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
      txn = nullptr;
    }

    // read-transaction
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
    ASSERT_NE(nullptr, txn);
  }

  virtual void TearDown() {
    if (skipped)
      return;
    SCOPED_TRACE("teardown");
    // end
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    txn = nullptr;

    // разрушаем привязанные идентификаторы
    fpta_name_destroy(&table);
    fpta_name_destroy(&pk_int);
    fpta_name_destroy(&se_str);

    EXPECT_EQ(FPTA_OK, fpta_db_close(db));
    db = nullptr;

    ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
    ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
  }
};

static bool filter_predicate_counter(const fptu_ro *, void *, void *ptr) {
  *static_cast<size_t *>(ptr) += 1;
  return true;
}

// simple invalid comparison => `false` without collapsing
TEST_F(FilterTautologyRewriter, SimpleInvalidComparisonToFalse) {

  // eq-compare non-nullable column with null => false
  {
    fpta_filter invalid_cmp_with_null;
    invalid_cmp_with_null.type = fpta_node_eq;
    invalid_cmp_with_null.node_cmp.left_id = &pk_int;
    invalid_cmp_with_null.node_cmp.right_value = fpta_value_null();

    const auto filter = &invalid_cmp_with_null;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(0u, rows);
    EXPECT_EQ(fpta_node_cond_false, invalid_cmp_with_null.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }

  // gt-compare integer column with string => false
  {
    fpta_filter invalid_cmp_with_str;
    invalid_cmp_with_str.type = fpta_node_gt;
    invalid_cmp_with_str.node_cmp.left_id = &pk_int;
    invalid_cmp_with_str.node_cmp.right_value = fpta_value_str("42");

    const auto filter = &invalid_cmp_with_str;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(0u, rows);
    EXPECT_EQ(fpta_node_cond_false, invalid_cmp_with_str.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }

  // lt-compare string column with integer => false
  {
    fpta_filter invalid_cmp_with_int;
    invalid_cmp_with_int.type = fpta_node_lt;
    invalid_cmp_with_int.node_cmp.left_id = &se_str;
    invalid_cmp_with_int.node_cmp.right_value = fpta_value_uint(42);

    const auto filter = &invalid_cmp_with_int;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(0u, rows);
    EXPECT_EQ(fpta_node_cond_false, invalid_cmp_with_int.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }
}

// simple invalid comparison => `true` without collapsing
TEST_F(FilterTautologyRewriter, SimpleInvalidComparisonToTrue) {

  // ne-compare non-nullable column with null => true
  {
    fpta_filter invalid_cmp_with_null;
    invalid_cmp_with_null.type = fpta_node_ne;
    invalid_cmp_with_null.node_cmp.left_id = &pk_int;
    invalid_cmp_with_null.node_cmp.right_value = fpta_value_null();

    const auto filter = &invalid_cmp_with_null;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(1u, rows);
    EXPECT_EQ(fpta_node_cond_true, invalid_cmp_with_null.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }

  // ne-compare integer column with string => true
  {
    fpta_filter invalid_cmp_with_str;
    invalid_cmp_with_str.type = fpta_node_ne;
    invalid_cmp_with_str.node_cmp.left_id = &pk_int;
    invalid_cmp_with_str.node_cmp.right_value = fpta_value_str("42");

    const auto filter = &invalid_cmp_with_str;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(1u, rows);
    EXPECT_EQ(fpta_node_cond_true, invalid_cmp_with_str.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }

  // ne-compare string column with integer => true
  {
    fpta_filter invalid_cmp_with_int;
    invalid_cmp_with_int.type = fpta_node_ne;
    invalid_cmp_with_int.node_cmp.left_id = &se_str;
    invalid_cmp_with_int.node_cmp.right_value = fpta_value_uint(42);

    const auto filter = &invalid_cmp_with_int;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(1u, rows);
    EXPECT_EQ(fpta_node_cond_true, invalid_cmp_with_int.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }
}

// `not` with prpagation
TEST_F(FilterTautologyRewriter, CompoundNot) {

  // not of nested invalid eq-comparison => `true`
  {
    fpta_filter invalid_cmp;
    invalid_cmp.type = fpta_node_eq;
    invalid_cmp.node_cmp.left_id = &pk_int;
    invalid_cmp.node_cmp.right_value = fpta_value_null();

    fpta_filter compoind;
    compoind.type = fpta_node_not;
    compoind.node_not = &invalid_cmp;

    const auto filter = &compoind;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(1u, rows);
    EXPECT_EQ(fpta_node_cond_false, invalid_cmp.type);
    EXPECT_EQ(fpta_node_collapsed_true, compoind.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }

  // not of nested invalid ne-comparison => `false`
  {
    fpta_filter invalid_cmp;
    invalid_cmp.type = fpta_node_ne;
    invalid_cmp.node_cmp.left_id = &pk_int;
    invalid_cmp.node_cmp.right_value = fpta_value_null();

    fpta_filter compoind;
    compoind.type = fpta_node_not;
    compoind.node_not = &invalid_cmp;

    const auto filter = &compoind;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(0u, rows);
    EXPECT_EQ(fpta_node_cond_true, invalid_cmp.type);
    EXPECT_EQ(fpta_node_collapsed_false, compoind.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }
}

// compoind with nested invalid without propagation
TEST_F(FilterTautologyRewriter, CompoundNestedWithoutPropagation) {

  // or(nested invalid eq-comparison, other) => or(`false`, other)
  {
    fpta_filter invalid_cmp;
    invalid_cmp.type = fpta_node_eq;
    invalid_cmp.node_cmp.left_id = &pk_int;
    invalid_cmp.node_cmp.right_value = fpta_value_null();

    fpta_filter predicate_counter;
    size_t counter = 0;
    predicate_counter.type = fpta_node_fnrow;
    predicate_counter.node_fnrow.predicate = filter_predicate_counter;
    predicate_counter.node_fnrow.context = this;
    predicate_counter.node_fnrow.arg = &counter;

    fpta_filter compoind;
    compoind.type = fpta_node_or;
    compoind.node_or.a = &invalid_cmp;
    compoind.node_or.b = &predicate_counter;

    const auto filter = &compoind;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(1u, rows);
    EXPECT_EQ(1u, counter);
    EXPECT_EQ(fpta_node_cond_false, invalid_cmp.type);
    EXPECT_EQ(fpta_node_or, compoind.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }

  // or(other, nested invalid eq-comparison) => or(other, `false`)
  {
    fpta_filter invalid_cmp;
    invalid_cmp.type = fpta_node_eq;
    invalid_cmp.node_cmp.left_id = &pk_int;
    invalid_cmp.node_cmp.right_value = fpta_value_null();

    fpta_filter predicate_counter;
    size_t counter = 0;
    predicate_counter.type = fpta_node_fnrow;
    predicate_counter.node_fnrow.predicate = filter_predicate_counter;
    predicate_counter.node_fnrow.context = this;
    predicate_counter.node_fnrow.arg = &counter;

    fpta_filter compoind;
    compoind.type = fpta_node_or;
    compoind.node_or.a = &predicate_counter;
    compoind.node_or.b = &invalid_cmp;

    const auto filter = &compoind;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(1u, rows);
    EXPECT_EQ(1u, counter);
    EXPECT_EQ(fpta_node_cond_false, invalid_cmp.type);
    EXPECT_EQ(fpta_node_or, compoind.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }

  // and(nested invalid ne-comparison, other) => and(`true`, other)
  {
    fpta_filter invalid_cmp;
    invalid_cmp.type = fpta_node_ne;
    invalid_cmp.node_cmp.left_id = &pk_int;
    invalid_cmp.node_cmp.right_value = fpta_value_null();

    fpta_filter predicate_counter;
    size_t counter = 0;
    predicate_counter.type = fpta_node_fnrow;
    predicate_counter.node_fnrow.predicate = filter_predicate_counter;
    predicate_counter.node_fnrow.context = this;
    predicate_counter.node_fnrow.arg = &counter;

    fpta_filter compoind;
    compoind.type = fpta_node_and;
    compoind.node_or.a = &invalid_cmp;
    compoind.node_or.b = &predicate_counter;

    const auto filter = &compoind;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(1u, rows);
    EXPECT_EQ(1u, counter);
    EXPECT_EQ(fpta_node_cond_true, invalid_cmp.type);
    EXPECT_EQ(fpta_node_and, compoind.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }

  // and(other, nested invalid ne-comparison) => and(other, `true`)
  {
    fpta_filter invalid_cmp;
    invalid_cmp.type = fpta_node_ne;
    invalid_cmp.node_cmp.left_id = &pk_int;
    invalid_cmp.node_cmp.right_value = fpta_value_null();

    fpta_filter predicate_counter;
    size_t counter = 0;
    predicate_counter.type = fpta_node_fnrow;
    predicate_counter.node_fnrow.predicate = filter_predicate_counter;
    predicate_counter.node_fnrow.context = this;
    predicate_counter.node_fnrow.arg = &counter;

    fpta_filter compoind;
    compoind.type = fpta_node_and;
    compoind.node_or.a = &predicate_counter;
    compoind.node_or.b = &invalid_cmp;

    const auto filter = &compoind;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(1u, rows);
    EXPECT_EQ(1u, counter);
    EXPECT_EQ(fpta_node_cond_true, invalid_cmp.type);
    EXPECT_EQ(fpta_node_and, compoind.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }
}

// compoind with nested invalid with propagation
TEST_F(FilterTautologyRewriter, CompoundNestedWithPropagation) {

  // or(nested invalid ne-comparison, any) => or(`true`, any) => `true`
  {
    fpta_filter invalid_cmp;
    invalid_cmp.type = fpta_node_ne;
    invalid_cmp.node_cmp.left_id = &pk_int;
    invalid_cmp.node_cmp.right_value = fpta_value_null();

    fpta_filter predicate_counter;
    size_t counter = 0;
    predicate_counter.type = fpta_node_fnrow;
    predicate_counter.node_fnrow.predicate = filter_predicate_counter;
    predicate_counter.node_fnrow.context = this;
    predicate_counter.node_fnrow.arg = &counter;

    fpta_filter compoind;
    compoind.type = fpta_node_or;
    compoind.node_or.a = &invalid_cmp;
    compoind.node_or.b = &predicate_counter;

    const auto filter = &compoind;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(1u, rows);
    EXPECT_EQ(0u, counter);
    EXPECT_EQ(fpta_node_cond_true, invalid_cmp.type);
    EXPECT_EQ(fpta_node_collapsed_true, compoind.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }

  // or(any, nested invalid ne-comparison) => or(any, `true`) => `true`
  {
    fpta_filter invalid_cmp;
    invalid_cmp.type = fpta_node_ne;
    invalid_cmp.node_cmp.left_id = &pk_int;
    invalid_cmp.node_cmp.right_value = fpta_value_null();

    fpta_filter predicate_counter;
    size_t counter = 0;
    predicate_counter.type = fpta_node_fnrow;
    predicate_counter.node_fnrow.predicate = filter_predicate_counter;
    predicate_counter.node_fnrow.context = this;
    predicate_counter.node_fnrow.arg = &counter;

    fpta_filter compoind;
    compoind.type = fpta_node_or;
    compoind.node_or.a = &predicate_counter;
    compoind.node_or.b = &invalid_cmp;

    const auto filter = &compoind;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(1u, rows);
    EXPECT_EQ(0u, counter);
    EXPECT_EQ(fpta_node_cond_true, invalid_cmp.type);
    EXPECT_EQ(fpta_node_collapsed_true, compoind.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }

  // and(nested invalid eq-comparison, any) => and(`false`, any) => `false`
  {
    fpta_filter invalid_cmp;
    invalid_cmp.type = fpta_node_eq;
    invalid_cmp.node_cmp.left_id = &pk_int;
    invalid_cmp.node_cmp.right_value = fpta_value_null();

    fpta_filter predicate_counter;
    size_t counter = 0;
    predicate_counter.type = fpta_node_fnrow;
    predicate_counter.node_fnrow.predicate = filter_predicate_counter;
    predicate_counter.node_fnrow.context = this;
    predicate_counter.node_fnrow.arg = &counter;

    fpta_filter compoind;
    compoind.type = fpta_node_and;
    compoind.node_or.a = &invalid_cmp;
    compoind.node_or.b = &predicate_counter;

    const auto filter = &compoind;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(0u, rows);
    EXPECT_EQ(0u, counter);
    EXPECT_EQ(fpta_node_cond_false, invalid_cmp.type);
    EXPECT_EQ(fpta_node_collapsed_false, compoind.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }

  // and(any, nested invalid eq-comparison) => and(any, `false`) => `false`
  {
    fpta_filter invalid_cmp;
    invalid_cmp.type = fpta_node_eq;
    invalid_cmp.node_cmp.left_id = &pk_int;
    invalid_cmp.node_cmp.right_value = fpta_value_null();

    fpta_filter predicate_counter;
    size_t counter = 0;
    predicate_counter.type = fpta_node_fnrow;
    predicate_counter.node_fnrow.predicate = filter_predicate_counter;
    predicate_counter.node_fnrow.context = this;
    predicate_counter.node_fnrow.arg = &counter;

    fpta_filter compoind;
    compoind.type = fpta_node_and;
    compoind.node_or.a = &predicate_counter;
    compoind.node_or.b = &invalid_cmp;

    const auto filter = &compoind;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(0u, rows);
    EXPECT_EQ(0u, counter);
    EXPECT_EQ(fpta_node_cond_false, invalid_cmp.type);
    EXPECT_EQ(fpta_node_collapsed_false, compoind.type);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }
}

TEST_F(FilterTautologyRewriter, CompoundNestedDeepPropagation) {
  //  true --|
  //          or => true ------------------|
  //   any --|                             |
  //                                        and => false
  //   any --|                             |
  //          or => true => not => false --|
  //  true --|
  {
    size_t counter = 0;
    fpta_filter invalid_cmp_1;
    invalid_cmp_1.type = fpta_node_ne;
    invalid_cmp_1.node_cmp.left_id = &se_str;
    invalid_cmp_1.node_cmp.right_value = fpta_value_uint(42);
    fpta_filter any_1;
    any_1.type = fpta_node_fnrow;
    any_1.node_fnrow.predicate = filter_predicate_counter;
    any_1.node_fnrow.context = this;
    any_1.node_fnrow.arg = &counter;
    fpta_filter or_1;
    or_1.type = fpta_node_or;
    or_1.node_or.a = &invalid_cmp_1;
    or_1.node_or.b = &any_1;

    fpta_filter invalid_cmp_2;
    invalid_cmp_2.type = fpta_node_ne;
    invalid_cmp_2.node_cmp.left_id = &pk_int;
    invalid_cmp_2.node_cmp.right_value = fpta_value_null();
    fpta_filter any_2;
    any_2.type = fpta_node_fnrow;
    any_2.node_fnrow.predicate = filter_predicate_counter;
    any_2.node_fnrow.context = this;
    any_2.node_fnrow.arg = &counter;
    fpta_filter or_2;
    or_2.type = fpta_node_or;
    or_2.node_or.a = &any_2;
    or_2.node_or.b = &invalid_cmp_2;
    fpta_filter the_not;
    the_not.type = fpta_node_not;
    the_not.node_not = &or_2;

    fpta_filter root_and;
    root_and.type = fpta_node_and;
    root_and.node_and.a = &or_1;
    root_and.node_and.b = &the_not;

    const auto filter = &root_and;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(fpta_node_cond_true, invalid_cmp_2.type);
    EXPECT_EQ(fpta_node_collapsed_true, or_2.type);
    EXPECT_EQ(fpta_node_collapsed_false, the_not.type);
    EXPECT_EQ(fpta_node_collapsed_false, root_and.type);
    EXPECT_EQ(0u, rows);
    EXPECT_EQ(0u, counter);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }

  //   any --|
  //          and => false => not => true --|
  // false --|                              |
  //                                         or => true
  // false --|                              |
  //          and => false -----------------|
  //   any --|
  //
  {
    size_t counter = 0;
    fpta_filter invalid_cmp_1;
    invalid_cmp_1.type = fpta_node_eq;
    invalid_cmp_1.node_cmp.left_id = &se_str;
    invalid_cmp_1.node_cmp.right_value = fpta_value_uint(42);
    fpta_filter any_1;
    any_1.type = fpta_node_fnrow;
    any_1.node_fnrow.predicate = filter_predicate_counter;
    any_1.node_fnrow.context = this;
    any_1.node_fnrow.arg = &counter;
    fpta_filter and_1;
    and_1.type = fpta_node_and;
    and_1.node_and.a = &any_1;
    and_1.node_and.b = &invalid_cmp_1;
    fpta_filter the_not;
    the_not.type = fpta_node_not;
    the_not.node_not = &and_1;

    fpta_filter invalid_cmp_2;
    invalid_cmp_2.type = fpta_node_eq;
    invalid_cmp_2.node_cmp.left_id = &pk_int;
    invalid_cmp_2.node_cmp.right_value = fpta_value_null();
    fpta_filter any_2;
    any_2.type = fpta_node_fnrow;
    any_2.node_fnrow.predicate = filter_predicate_counter;
    any_2.node_fnrow.context = this;
    any_2.node_fnrow.arg = &counter;
    fpta_filter and_2;
    and_2.type = fpta_node_and;
    and_2.node_and.a = &invalid_cmp_2;
    and_2.node_and.b = &any_2;

    fpta_filter root_or;
    root_or.type = fpta_node_or;
    root_or.node_or.a = &the_not;
    root_or.node_or.b = &and_2;

    const auto filter = &root_or;
    SCOPED_TRACE("before rewrite: " + std::to_string(filter));
    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &pk_int, fpta_value_begin(),
                                        fpta_value_end(), filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    SCOPED_TRACE("after rewrite: " + std::to_string(cursor->filter));
    size_t rows;
    EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &rows, INT_MAX));
    EXPECT_EQ(fpta_node_cond_false, invalid_cmp_1.type);
    EXPECT_EQ(fpta_node_collapsed_false, and_1.type);
    EXPECT_EQ(fpta_node_collapsed_true, the_not.type);
    EXPECT_EQ(fpta_node_collapsed_true, root_or.type);
    EXPECT_EQ(1u, rows);
    EXPECT_EQ(0u, counter);
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));
  }
}

//----------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
