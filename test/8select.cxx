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
    ordering = (fpta_cursor_options)(ordering | fpta_dont_fetch);

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

  // открываем c диапазоном (без пересечения, нулевой диапазон)
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
  // повторяем с fpta_zeroed_range_is_point
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(17),
                             fpta_value_sint(17), nullptr,
                             ordering | fpta_zeroed_range_is_point, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  if (fpta_index_is_unordered(index)) {
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
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
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
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(21u, count);
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
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(21u, count);
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
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(21u, count);
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

  // begin, epsilon
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_begin(),
                             fpta_value_epsilon(), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем значение ключа
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  }
  EXPECT_EQ(FPTA_OK, fpta_cursor_key(cursor, &key_value));
  EXPECT_EQ(fpta_signed_int, key_value.type);
  if (ordering & fpta_descending) {
    EXPECT_EQ(41, key_value.sint);
  } else {
    EXPECT_EQ(0, key_value.sint);
  }
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
  // проверяем значение ключа
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  }
  EXPECT_EQ(FPTA_OK, fpta_cursor_key(cursor, &key_value));
  EXPECT_EQ(fpta_signed_int, key_value.type);
  if (ordering & fpta_descending) {
    EXPECT_EQ(0, key_value.sint);
  } else {
    EXPECT_EQ(41, key_value.sint);
  }
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
  // проверяем значение ключа
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  }
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
  // проверяем значение ключа
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  }
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
  // проверяем значение ключа
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  }
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
  // проверяем значение ключа
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  }
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
  // проверяем значение ключа
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  }
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
  // проверяем значение ключа
  if (ordering & fpta_dont_fetch) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  }
  EXPECT_EQ(FPTA_OK, fpta_cursor_key(cursor, &key_value));
  EXPECT_EQ(fpta_signed_int, key_value.type);
  EXPECT_EQ(41, key_value.sint);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(1u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // before-first, epsilon
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(-1),
                             fpta_value_epsilon(), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(0u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // epsilon, before-first
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_epsilon(),
                             fpta_value_sint(-1), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(0u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // after-last, epsilon
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_sint(42),
                             fpta_value_epsilon(), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(0u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;

  // epsilon, after-last
  EXPECT_EQ(FPTA_OK,
            fpta_cursor_open(txn_guard.get(), &col_1, fpta_value_epsilon(),
                             fpta_value_sint(42), nullptr, ordering, &cursor));
  ASSERT_NE(nullptr, cursor);
  cursor_guard.reset(cursor);
  // проверяем кол-во записей и закрываем курсор
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(0u, count);
  EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor_guard.release()));
  cursor = nullptr;
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

#ifdef INSTANTIATE_TEST_SUITE_P
INSTANTIATE_TEST_SUITE_P(
    Combine, Select,
    ::testing::Combine(::testing::Values(fpta_primary_unique_ordered_obverse,
                                         fpta_primary_withdups_ordered_obverse,
                                         fpta_primary_unique_unordered,
                                         fpta_primary_withdups_unordered),
                       ::testing::Values(fpta_unsorted, fpta_ascending,
                                         fpta_descending)));
#else
INSTANTIATE_TEST_CASE_P(
    Combine, Select,
    ::testing::Combine(::testing::Values(fpta_primary_unique_ordered_obverse,
                                         fpta_primary_withdups_ordered_obverse,
                                         fpta_primary_unique_unordered,
                                         fpta_primary_withdups_unordered),
                       ::testing::Values(fpta_unsorted, fpta_ascending,
                                         fpta_descending)));
#endif

//----------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
