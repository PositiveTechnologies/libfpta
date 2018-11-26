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

#include "fpta_test.h"
#include "keygen.hpp"

/* Кол-во проверочных точек в диапазонах значений индексируемых типов.
 *
 * Значение не может быть больше чем 65536, так как это предел кол-ва
 * уникальных значений для fptu_uint16.
 *
 * Но для парного генератора не может быть больше 32768,
 * так как для не-уникальных вторичных индексов нам требуются дубликаты,
 * что требует больше уникальных значений для первичного ключа.
 */
#ifdef FPTA_INDEX_UT_LONG
static constexpr int NNN = 32749; // около 1-2 минуты в /dev/shm/
#else
static constexpr int NNN = 509; // менее секунды в /dev/shm/
#endif

static const char testdb_name[] = TEST_DB_DIR "ut_index_secondary.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_index_secondary.fpta" MDBX_LOCK_SUFFIX;

//----------------------------------------------------------------------------

class IndexSecondary
    : public ::testing::TestWithParam<GTEST_TUPLE_NAMESPACE_::tuple<
          fpta_index_type, fptu_type, fpta_index_type, fptu_type>> {
public:
  fptu_type pk_type;
  fpta_index_type pk_index;
  fptu_type se_type;
  fpta_index_type se_index;

  bool valid_pk;
  bool valid_se;
  bool skipped;
  scoped_db_guard db_quard;
  scoped_txn_guard txn_guard;
  scoped_cursor_guard cursor_guard;
  std::string pk_col_name;
  std::string se_col_name;
  fpta_name table, col_pk, col_se, col_order, col_dup_id, col_t1ha;
  unsigned n;

  void Fill() {
    fptu_rw *row = fptu_alloc(6, fpta_max_keylen * 42);
    ASSERT_NE(nullptr, row);
    ASSERT_STREQ(nullptr, fptu::check(row));
    fpta_txn *const txn = txn_guard.get();

    coupled_keygen pg(pk_index, pk_type, se_index, se_type);
    n = 0;
    for (int order = 0; order < NNN; ++order) {
      SCOPED_TRACE("order " + std::to_string(order));

      // теперь формируем кортеж
      ASSERT_EQ(FPTU_OK, fptu_clear(row));
      ASSERT_STREQ(nullptr, fptu::check(row));
      fpta_value value_pk = pg.make_primary(order, NNN);
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_column(row, &col_order, fpta_value_sint(order)));
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_pk, value_pk));
      // тут важно помнить, что генераторы ключей для не-числовых типов
      // используют статический буфер, поэтому генерация значения для
      // secondary может повредить значение primary.
      // поэтому primary поле нужно добавить в кортеж до генерации
      // secondary.
      fpta_value value_se = pg.make_secondary(order, NNN);
      ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_se, value_se));
      // t1ha как "checksum" для order
      ASSERT_EQ(FPTA_OK,
                fpta_upsert_column(row, &col_t1ha,
                                   order_checksum(order, se_type, se_index)));

      // пытаемся обновить несуществующую запись
      ASSERT_EQ(FPTA_NOTFOUND,
                fpta_update_row(txn, &table, fptu_take_noshrink(row)));

      if (fpta_index_is_unique(se_index)) {
        // вставляем
        ASSERT_EQ(FPTA_OK,
                  fpta_insert_row(txn, &table, fptu_take_noshrink(row)));
        n++;
        // проверяем что полный дубликат не вставляется
        ASSERT_EQ(FPTA_KEYEXIST,
                  fpta_insert_row(txn, &table, fptu_take_noshrink(row)));

        // обновляем dup_id и проверям что дубликат по ключу не проходит
        ASSERT_EQ(FPTA_OK,
                  fpta_upsert_column(row, &col_dup_id, fpta_value_uint(1)));
        ASSERT_EQ(FPTA_KEYEXIST,
                  fpta_insert_row(txn, &table, fptu_take_noshrink(row)));

        // TODO: обновить PK и попробовать вставить дубликат по secondaty,
        // эта вставка не должна пройти.
        // LY: сейчас это проблематично, так как попытка такой вставки
        // приведет к прерыванию транзакции.

        // проверяем что upsert и update работают,
        // сначала upsert с текущим dup_id = 1
        ASSERT_EQ(FPTA_OK,
                  fpta_upsert_row(txn, &table, fptu_take_noshrink(row)));
        // теперь update c dup_id = 42, должен остаться только этот
        // вариант
        ASSERT_EQ(FPTA_OK,
                  fpta_upsert_column(row, &col_dup_id, fpta_value_uint(42)));
        ASSERT_EQ(FPTA_OK,
                  fpta_update_row(txn, &table, fptu_take_noshrink(row)));
      } else {
        // вставляем c dup_id = 0
        ASSERT_EQ(FPTA_OK,
                  fpta_upsert_column(row, &col_dup_id, fpta_value_uint(0)));
        ASSERT_EQ(FPTA_OK,
                  fpta_insert_row(txn, &table, fptu_take_noshrink(row)));
        n++;
        // проверяем что полный дубликат не вставляется
        ASSERT_EQ(FPTA_KEYEXIST,
                  fpta_insert_row(txn, &table, fptu_take_noshrink(row)));

        // обновляем dup_id и вставляем дубль по ключу
        // без обновления primary, такой дубликат также
        // НЕ должен вставится
        ASSERT_EQ(FPTA_OK,
                  fpta_upsert_column(row, &col_dup_id, fpta_value_uint(1)));
        ASSERT_EQ(FPTA_KEYEXIST, fpta_insert_row(txn, &table, fptu_take(row)));

        // теперь обновляем primary key и вставляем дубль по вторичному
        // ключу
        value_pk = pg.make_primary_4dup(order, NNN);
        ASSERT_EQ(FPTA_OK, fpta_upsert_column(row, &col_pk, value_pk));
        ASSERT_EQ(FPTA_OK, fpta_insert_row(txn, &table, fptu_take(row)));
        n++;
      }
    }

    // разрушаем кортеж
    ASSERT_STREQ(nullptr, fptu::check(row));
    free(row);
  }

  virtual void SetUp() {
    // нужно простое число, иначе сломается переупорядочивание
    ASSERT_TRUE(isPrime(NNN));
    // иначе не сможем проверить fptu_uint16
    ASSERT_GE(65535, NNN * 2);
    pk_index = GTEST_TUPLE_NAMESPACE_::get<0>(GetParam());
    pk_type = GTEST_TUPLE_NAMESPACE_::get<1>(GetParam());
    se_index = GTEST_TUPLE_NAMESPACE_::get<2>(GetParam());
    se_type = GTEST_TUPLE_NAMESPACE_::get<3>(GetParam());

    valid_pk = is_valid4primary(pk_type, pk_index);
    valid_se = is_valid4secondary(pk_type, pk_index, se_type, se_index);

    SCOPED_TRACE(
        "pk_type " + std::to_string(pk_type) + ", pk_index " +
        std::to_string(pk_index) + ", se_type " + std::to_string(se_type) +
        ", se_index " + std::to_string(se_index) +
        (valid_se && valid_pk ? ", (valid case)" : ", (invalid case)"));

    skipped = GTEST_IS_EXECUTION_TIMEOUT();
    if (skipped)
      return;

    // создаем пять колонок: primary_key, secondary_key, order, t1ha и dup_id
    fpta_column_set def;
    fpta_column_set_init(&def);

    pk_col_name = "pk_" + std::to_string(pk_type);
    se_col_name = "se_" + std::to_string(se_type);
    // инициализируем идентификаторы колонок
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_pk, pk_col_name.c_str()));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_se, se_col_name.c_str()));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_order, "order"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_dup_id, "dup_id"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_t1ha, "t1ha"));

    if (!valid_pk) {
      EXPECT_NE(FPTA_OK, fpta_column_describe(pk_col_name.c_str(), pk_type,
                                              pk_index, &def));

      // разрушаем описание таблицы
      EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
      EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));
      return;
    }
    EXPECT_EQ(FPTA_OK, fpta_column_describe(pk_col_name.c_str(), pk_type,
                                            pk_index, &def));
    if (!valid_se) {
      EXPECT_NE(FPTA_OK, fpta_column_describe(se_col_name.c_str(), se_type,
                                              se_index, &def));

      // разрушаем описание таблицы
      EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
      EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));
      return;
    }
    EXPECT_EQ(FPTA_OK, fpta_column_describe(se_col_name.c_str(), se_type,
                                            se_index, &def));

    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("order", fptu_int32, fpta_index_none, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("dup_id", fptu_uint16,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("t1ha", fptu_uint64, fpta_index_none, &def));
    ASSERT_EQ(FPTA_OK, fpta_column_set_validate(&def));

    // чистим
    if (REMOVE_FILE(testdb_name) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }
    if (REMOVE_FILE(testdb_name_lck) != 0) {
      ASSERT_EQ(ENOENT, errno);
    }

#ifdef FPTA_INDEX_UT_LONG
    // пытаемся обойтись меньшей базой, но для строк потребуется больше места
    unsigned megabytes = 32;
    if (type > fptu_128)
      megabytes = 40;
    if (type > fptu_256)
      megabytes = 56;
#else
    const unsigned megabytes = 1;
#endif

    fpta_db *db = nullptr;
    ASSERT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_weak, fpta_regime4testing,
                                    0644, megabytes, true, &db));
    ASSERT_NE(nullptr, db);
    db_quard.reset(db);

    // создаем таблицу
    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);
    txn_guard.reset(txn);
    ASSERT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));
    ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
    txn = nullptr;

    // разрушаем описание таблицы
    EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
    EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

    /* Для полноты тесты переоткрываем базу. В этом нет явной необходимости,
     * но только так можно проверить работу некоторых механизмов.
     *
     * В частности:
     *  - внутри движка создание таблицы одновременно приводит к открытию её
     *    dbi-хендла, с размещением его во внутренних структурах.
     *  - причем этот хендл будет жив до закрытии всей базы, либо до удаления
     *    таблицы.
     *  - поэтому для проверки кода открывающего существующую таблицы
     *    необходимо закрыть и повторно открыть всю базу.
     */
    // закрываем базу
    ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db_quard.release()));
    db = nullptr;
    // открываем заново
    ASSERT_EQ(FPTA_OK, fpta_db_open(testdb_name, fpta_weak, fpta_regime4testing,
                                    0644, megabytes, false, &db));
    ASSERT_NE(nullptr, db);
    db_quard.reset(db);

    // сбрасываем привязку идентификаторов
    EXPECT_EQ(FPTA_SUCCESS, fpta_name_reset(&table));
    EXPECT_EQ(FPTA_SUCCESS, fpta_name_reset(&col_pk));
    EXPECT_EQ(FPTA_SUCCESS, fpta_name_reset(&col_se));
    EXPECT_EQ(FPTA_SUCCESS, fpta_name_reset(&col_order));
    EXPECT_EQ(FPTA_SUCCESS, fpta_name_reset(&col_dup_id));
    EXPECT_EQ(FPTA_SUCCESS, fpta_name_reset(&col_t1ha));

    //------------------------------------------------------------------------

    // начинаем транзакцию записи
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
    ASSERT_NE(nullptr, txn);
    txn_guard.reset(txn);

    // связываем идентификаторы с ранее созданной схемой
    ASSERT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_pk));
    ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_se));
    ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_order));
    ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_dup_id));
    ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_t1ha));

    ASSERT_NO_FATAL_FAILURE(Fill());

    // завершаем транзакцию
    ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn_guard.release(), false));
    txn = nullptr;

    //------------------------------------------------------------------------

    // начинаем транзакцию чтения
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
    ASSERT_NE(nullptr, txn);
    txn_guard.reset(txn);

    fpta_cursor *cursor;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &col_se, fpta_value_begin(),
                                        fpta_value_end(), nullptr,
                                        fpta_index_is_ordered(se_index)
                                            ? fpta_ascending_dont_fetch
                                            : fpta_unsorted_dont_fetch,
                                        &cursor));
    ASSERT_NE(nullptr, cursor);
    cursor_guard.reset(cursor);
  }

  virtual void TearDown() {
    if (skipped)
      return;

    // разрушаем привязанные идентификаторы
    fpta_name_destroy(&table);
    fpta_name_destroy(&col_pk);
    fpta_name_destroy(&col_se);
    fpta_name_destroy(&col_order);
    fpta_name_destroy(&col_dup_id);
    fpta_name_destroy(&col_t1ha);

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

TEST_P(IndexSecondary, basic) {
  /* Тест вторичных (secondary) индексов.
   *
   * Сценарий (общий для всех комбинаций типов полей и всех видов
   * первичных и вторичных индексов):
   *  1. Создается тестовая база с одной таблицей, в которой пять колонок:
   *     - PK (primary key) с типом из комбинации, для которого производится
   *       тестирование кооперативной работы первичного индекса.
   *     - SE (secondary key) с типом из комбинации, для которого производится
   *       тестирование кооперативной работы вторичного индекса.
   *     - Колонка "order", в которую записывается контрольный (ожидаемый)
   *       порядковый номер следования строки, при сортировке по PK и
   *       проверяемому виду индекса.
   *     - Колонка "dup_id", которая используется для идентификации дубликатов
   *       индексов допускающих не-уникальные ключи.
   *     - Колонка "t1ha", в которую записывается "контрольная сумма" от
   *       ожидаемого порядка строки, типа PK и вида индекса. Принципиальной
   *       необходимости в этой колонке нет, она используется как
   *       "утяжелитель", а также для дополнительного контроля.
   *
   *  2. Тест также используется для недопустимых комбинаций индексируемых
   *     типов и видов индексов. В этом случае проверяется что формирование
   *     описания колонок таблицы завершается с соответствующими ошибками.
   *
   *  3. Таблица заполняется строками, значение PK и SE в которых
   *     формируются соответствующим генератором значений:
   *     - Сами генераторы проверяются в одном из тестов 0corny.
   *     - Значения PK всегда уникальны, так как это требуется для возможности
   *       включения вторичных индексов.
   *     - Для вторичных индексов без уникальности для каждого значения
   *       вторичного ключа вставляется две строки с разным dup_id и разными
   *       значениям PK. Соответственно, этом случае PK значений генерируется
   *       вдвое больше чем значений SE.
   *     - При заполнении таблицы производятся попытки обновить
   *       несуществующую запись, вставить дубликат по ключу или полный
   *       дубликат записи, выполнить upsert при не уникальном SE.
   *
   *  4. В отдельной транзакции открывается курсор, через который проверяется:
   *      - Итоговое количество строк.
   *      - Перемещение на последнюю и на первую строку.
   *      - Итерирование строк с контролем их порядок, в том числе порядок
   *        следования дубликатов.
   *
   *  5. Завершаются операции и освобождаются ресурсы.
   */
  if (!valid_pk || !valid_se || skipped)
    return;

  SCOPED_TRACE("pk_type " + std::to_string(pk_type) + ", pk_index " +
               std::to_string(pk_index) + ", se_type " +
               std::to_string(se_type) + ", se_index " +
               std::to_string(se_index) +
               (valid_se && valid_pk ? ", (valid case)" : ", (invalid case)"));

  fpta_cursor *const cursor = cursor_guard.get();

  // проверяем кол-во записей.
  size_t count;
  EXPECT_EQ(FPTA_OK, fpta_cursor_count(cursor, &count, INT_MAX));
  EXPECT_EQ(n, count);

  // переходим к первой записи
  if (n) {
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
  } else {
    EXPECT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_first));
  }

  int order = 0;
  for (unsigned i = 0; i < n;) {
    SCOPED_TRACE(std::to_string(i) + " of " + std::to_string(n) + ", order " +
                 std::to_string(order));
    fptu_ro tuple;
    EXPECT_EQ(FPTA_OK, fpta_cursor_get(cursor, &tuple));
    ASSERT_STREQ(nullptr, fptu::check(tuple));

    int error;
    fpta_value key;
    ASSERT_EQ(FPTU_OK, fpta_cursor_key(cursor, &key));
    SCOPED_TRACE("key: " + std::to_string(key.type) + ", length " +
                 std::to_string(key.binary_length));

    auto tuple_order = (int)fptu_get_sint(tuple, col_order.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    if (fpta_index_is_ordered(se_index)) {
      ASSERT_EQ(order, tuple_order);
    }

    auto tuple_checksum = fptu_get_uint(tuple, col_t1ha.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    auto checksum = order_checksum(tuple_order, se_type, se_index).uint;
    ASSERT_EQ(checksum, tuple_checksum);

    auto tuple_dup_id =
        (unsigned)fptu_get_uint(tuple, col_dup_id.column.num, &error);
    ASSERT_EQ(FPTU_OK, error);
    size_t dups = 100500;
    ASSERT_EQ(FPTA_OK, fpta_cursor_dups(cursor_guard.get(), &dups));
    if (fpta_index_is_unique(se_index)) {
      ASSERT_EQ(42u, tuple_dup_id);
      ASSERT_EQ(1u, dups);
    } else {
      /* Наличие дубликатов означает что для одного значения ключа
       * в базе есть несколько значений. Причем эти значения хранятся
       * в отдельном отсортированном под-дереве.
       *
       * В случае с secondary-индексом, значениями является primary key,
       * а для сравнения используется соответствующий компаратор.
       *
       * Сформированные в этом тесте строки-дубликаты по вторичному
       * ключу всегда отличаются двумя полями: pk и dup_id.
       * Однако, порядок следования строк определяется значением pk,
       * который генерируется с чередованием больше/меньше,
       * с тем чтобы можно было проверить корректность реализации.
       * Соответственно, этот порядок должен соблюдаться, если только
       * первичный индекс не unordered.
       */
      if (!fpta_index_is_ordered(pk_index))
        ASSERT_GT(2u, tuple_dup_id);
      else if (tuple_order % 3)
        ASSERT_EQ(i & 1u, tuple_dup_id);
      else
        ASSERT_EQ((i ^ 1) & 1u, tuple_dup_id);
      ASSERT_EQ(2u, dups);
    }

    if (++i < n) {
      ASSERT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_next));
    } else {
      EXPECT_EQ(FPTA_NODATA, fpta_cursor_move(cursor, fpta_next));
    }

    if (fpta_index_is_unique(se_index) || (i & 1) == 0)
      ++order;
  }

  EXPECT_EQ(FPTA_NODATA, fpta_cursor_eof(cursor));
}

//----------------------------------------------------------------------------

INSTANTIATE_TEST_CASE_P(
    Combine, IndexSecondary,
    ::testing::Combine(
        ::testing::Values(fpta_primary_unique_ordered_obverse,
                          fpta_primary_unique_ordered_reverse,
                          fpta_primary_withdups_ordered_obverse,
                          fpta_primary_withdups_ordered_reverse,
                          fpta_primary_unique_unordered,
                          fpta_primary_withdups_unordered),
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_96, fptu_128, fptu_160, fptu_datetime, fptu_256,
                          fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */),
        ::testing::Values(fpta_secondary_unique_ordered_obverse,
                          fpta_secondary_unique_ordered_reverse,
                          fpta_secondary_withdups_ordered_obverse,
                          fpta_secondary_withdups_ordered_reverse,
                          fpta_secondary_unique_unordered,
                          fpta_secondary_withdups_unordered),
        ::testing::Values(fptu_null, fptu_uint16, fptu_int32, fptu_uint32,
                          fptu_fp32, fptu_int64, fptu_uint64, fptu_fp64,
                          fptu_96, fptu_128, fptu_160, fptu_datetime, fptu_256,
                          fptu_cstr, fptu_opaque
                          /*, fptu_nested, fptu_farray */)));

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
