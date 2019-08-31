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

#include "fpta_test.h"

static const char testdb_name[] = TEST_DB_DIR "ut_schema.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_schema.fpta" MDBX_LOCK_SUFFIX;

TEST(Schema, Trivia) {
  /* Тривиальный тест создания/заполнения описания колонок таблицы.
   *
   * Сценарий:
   *  - создаем/инициализируем описание колонок.
   *  - пробуем добавить несколько некорректных колонок,
   *    с плохими: именем, индексом, типом.
   *  - добавляем несколько корректных описаний колонок.
   *
   * Тест НЕ перебирает все возможные комбинации, а только некоторые.
   * Такой относительно полный перебор происходит автоматически при
   * тестировании индексов и курсоров. */
  fpta_column_set def;
  fpta_column_set_init(&def);
  EXPECT_NE(FPTA_SUCCESS, fpta_column_set_validate(&def));

  EXPECT_EQ(FPTA_ENAME,
            fpta_column_describe("", fptu_cstr,
                                 fpta_primary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_EINVAL,
            fpta_column_describe("column_a", fptu_cstr,
                                 fpta_primary_unique_ordered_obverse, nullptr));

  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("column_a", fptu_uint64,
                                 fpta_primary_unique_ordered_reverse, &def));
  EXPECT_EQ(FPTA_ETYPE,
            fpta_column_describe("column_a", fptu_null,
                                 fpta_primary_unique_ordered_obverse, &def));

  /* Валидны все комбинации, в которых установлен хотя-бы один из флажков
   * fpta_index_fordered или fpta_index_fobverse. Другим словами,
   * не может быть unordered индекса со сравнением ключей
   * в обратном порядке. Однако, также допустим fpta_index_none.
   *
   * Поэтому внутри диапазона остаются только две недопустимые комбинации,
   * которые и проверяем. */
  EXPECT_EQ(FPTA_EFLAG, fpta_column_describe("column_a", fptu_cstr,
                                             fpta_index_funique, &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe(
                "column_a", fptu_cstr,
                (fpta_index_type)(fpta_index_fsecondary | fpta_index_funique),
                &def));
  EXPECT_EQ(FPTA_EFLAG, fpta_column_describe("column_a", fptu_cstr,
                                             (fpta_index_type)-1, &def));
  EXPECT_EQ(
      FPTA_EFLAG,
      fpta_column_describe(
          "column_a", fptu_cstr,
          (fpta_index_type)(fpta_index_funique + fpta_index_fordered +
                            fpta_index_fobverse + fpta_index_fsecondary + 1),
          &def));

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("column_a", fptu_cstr,
                                 fpta_primary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  EXPECT_EQ(FPTA_EEXIST,
            fpta_column_describe("column_b", fptu_cstr,
                                 fpta_primary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_EEXIST, fpta_column_describe(
                             "column_a", fptu_cstr,
                             fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));
  EXPECT_EQ(FPTA_EEXIST, fpta_column_describe(
                             "COLUMN_A", fptu_cstr,
                             fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "column_b", fptu_cstr,
                         fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  EXPECT_EQ(FPTA_EEXIST, fpta_column_describe(
                             "column_b", fptu_fp64,
                             fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_EEXIST, fpta_column_describe(
                             "COLUMN_B", fptu_fp64,
                             fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "column_c", fptu_uint16,
                         fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  EXPECT_EQ(FPTA_EEXIST, fpta_column_describe(
                             "column_A", fptu_int32,
                             fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_EEXIST, fpta_column_describe(
                             "Column_b", fptu_datetime,
                             fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_EEXIST, fpta_column_describe(
                             "coLumn_c", fptu_opaque,
                             fpta_secondary_withdups_ordered_obverse, &def));

  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));
  EXPECT_EQ(FPTA_EINVAL, fpta_column_set_destroy(&def));
}

TEST(Schema, Base) {
  /* Базовый тест создания таблицы.
   *
   * Сценарий:
   *  - открываем базу в режиме неизменяемой схемы и пробуем начать
   *    транзакцию уровня изменения схемы.
   *  - открываем базу в режиме изменяемой схемы.
   *  - создаем и заполняем описание колонок.
   *  - создаем таблицу по сформированному описанию колонок.
   *  - затем в другой транзакции проверяем, что у созданной таблицы
   *    есть соответствующие колонки.
   *  - в очередной транзакции создаем еще одну таблицу
   *    и обновляем описание первой.
   *  - после в другой транзакции удаляем созданную таблицу,
   *    а также пробуем удалить несуществующую.
   *
   * Тест НЕ перебирает комбинации. Некий относительно полный перебор
   * происходит автоматически при тестировании индексов и курсоров. */
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  fpta_db *db = nullptr;
  /* открываем базу в режиме неизменяемой схемы */
  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  1, false, &db));
  ASSERT_NE(nullptr, db);

  /* пробуем начать транзакцию изменения схемы в базе с неизменяемой схемой */
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(EPERM, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_EQ(nullptr, txn);
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  //------------------------------------------------------------------------

  /* повторно открываем базу с возможностью изменять схему */
  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime4testing,
                                  1, true, &db));
  ASSERT_NE(nullptr, db);

  // формируем описание колонок для первой таблицы
  fpta_column_set def;
  fpta_column_set_init(&def);
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("pk_str_uniq", fptu_cstr,
                                 fpta_primary_unique_ordered_obverse, &def));
  /* LY: было упущение, из-за которого нумерация колонок с одинаковыми
   * индексами/опциями могла отличаться от порядка добавления */
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "first_uint", fptu_uint64,
                         fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe("second_fp", fptu_fp64,
                                          fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  // формируем описание колонок для второй таблицы
  fpta_column_set def2;
  fpta_column_set_init(&def2);
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("x", fptu_cstr,
                                 fpta_primary_unique_ordered_obverse, &def2));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "y", fptu_cstr,
                         fpta_secondary_withdups_ordered_obverse, &def2));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def2));

  //------------------------------------------------------------------------
  // создаем первую таблицу в отдельной транзакции
  EXPECT_EQ(FPTA_EINVAL, fpta_transaction_begin(db, fpta_read, nullptr));
  EXPECT_EQ(FPTA_EFLAG, fpta_transaction_begin(db, (fpta_level)0, &txn));
  EXPECT_EQ(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_1", &def));

  fpta_schema_info schema_info;
  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(1u, schema_info.tables_count);
  fptu_rw *tuple = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_schema_render(&schema_info, &tuple));
  EXPECT_NE(nullptr, tuple);
  EXPECT_EQ(nullptr, fptu::check(tuple));
  free(tuple);
  tuple = nullptr;
  EXPECT_EQ(
      "{\n    schema_format: 1,\n    schema_t1ha: "
      "\"86f8fa504adfbcbfa513a0b63b6d3f73\",\n    table: {\n        name: "
      "\"table_1\",\n        column: [\n            {\n                name: "
      "\"pk_str_uniq\",\n                number: 0,\n                datatype: "
      "\"cstr\",\n                nullable: false,\n                index: "
      "\"primary\",\n                unique: true,\n                unordered: "
      "false,\n                reverse: false,\n                mdbx: "
      "\"7onkrwutQ@@\"\n            },\n            {\n                name: "
      "\"first_uint\",\n                number: 1,\n                datatype: "
      "\"uint64\",\n                nullable: false,\n                index: "
      "\"secondary\",\n                unique: false,\n                "
      "unordered: false,\n                reverse: false,\n                "
      "mdbx: \"7onkrwutQ@0\"\n            },\n            {\n                "
      "name: \"second_fp\",\n                number: 2,\n                "
      "datatype: \"fp64\",\n                nullable: false,\n                "
      "index: \"none\"\n            }\n        ]\n    }\n}",
      fpta::schema2json(&schema_info, "    ").second);
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

  //------------------------------------------------------------------------
  // проверяем наличие первой таблицы

  fpta_name table, col_pk, col_a, col_b, probe_get;
  memset(&table, 42, sizeof(table)); // чтобы valrind не ругался
  EXPECT_GT(0, fpta_table_column_count(&table));
  EXPECT_EQ(FPTA_EINVAL, fpta_table_column_get(&table, 0, &probe_get));

  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "tAbLe_1"));
  EXPECT_EQ(UINT64_C(9575621353268990208), table.shove);
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table_1"));
  EXPECT_EQ(UINT64_C(9575621353268990208), table.shove);
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_pk, "pk_str_uniq"));
  EXPECT_EQ(UINT64_C(2421465332071795712), col_pk.shove);
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_a, "First_Uint"));
  EXPECT_EQ(UINT64_C(17241369320330542080), col_a.shove);
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_b, "second_FP"));
  EXPECT_EQ(UINT64_C(12536943114372647936), col_b.shove);

  EXPECT_GT(0, fpta_table_column_count(&table));
  EXPECT_EQ(FPTA_EINVAL, fpta_table_column_get(&table, 0, &probe_get));
  EXPECT_EQ(UINT64_C(9575621353268990208), table.shove);

  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_pk));
  EXPECT_EQ(UINT64_C(9575621353268990208), table.shove);
  EXPECT_EQ(UINT64_C(2421465332071795949), col_pk.shove);
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_a));
  EXPECT_EQ(UINT64_C(17241369320330542534), col_a.shove);
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_b));
  EXPECT_EQ(UINT64_C(12536943114372647943), col_b.shove);

  EXPECT_EQ(3, fpta_table_column_count(&table));
  EXPECT_EQ(FPTA_OK, fpta_table_column_get(&table, 0, &probe_get));
  EXPECT_EQ(0, memcmp(&probe_get, &col_pk, sizeof(fpta_name)));
  EXPECT_EQ(FPTA_OK, fpta_table_column_get(&table, 1, &probe_get));
  EXPECT_EQ(0, memcmp(&probe_get, &col_a, sizeof(fpta_name)));
  EXPECT_EQ(FPTA_OK, fpta_table_column_get(&table, 2, &probe_get));
  EXPECT_EQ(0, memcmp(&probe_get, &col_b, sizeof(fpta_name)));
  EXPECT_EQ(FPTA_NODATA, fpta_table_column_get(&table, 3, &probe_get));

  EXPECT_EQ(fptu_cstr, fpta_shove2type(col_pk.shove));
  EXPECT_EQ(fpta_primary_unique_ordered_obverse, fpta_name_colindex(&col_pk));
  EXPECT_EQ(fptu_cstr, fpta_name_coltype(&col_pk));
  EXPECT_EQ(0u, col_pk.column.num);

  EXPECT_EQ(fptu_uint64, fpta_shove2type(col_a.shove));
  EXPECT_EQ(fpta_secondary_withdups_ordered_obverse,
            fpta_name_colindex(&col_a));
  EXPECT_EQ(fptu_uint64, fpta_name_coltype(&col_a));
  EXPECT_EQ(1u, col_a.column.num);

  EXPECT_EQ(fptu_fp64, fpta_shove2type(col_b.shove));
  EXPECT_EQ(fpta_index_none, fpta_name_colindex(&col_b));
  EXPECT_EQ(fptu_fp64, fpta_name_coltype(&col_b));
  EXPECT_EQ(2u, col_b.column.num);

  // получаем описание схемы, проверяем кол-во таблиц и освобождаем
  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(1u, schema_info.tables_count);
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &schema_info.tables_names[0]));
  int err;
  EXPECT_EQ(
      "table_1",
      fpta::schema_symbol(&schema_info, &table, err).operator std::string());
  EXPECT_EQ(FPTA_OK, err);
  EXPECT_EQ(
      "pk_str_uniq",
      fpta::schema_symbol(&schema_info, &col_pk, err).operator std::string());
  EXPECT_EQ(FPTA_OK, err);
  EXPECT_EQ(
      "first_uint",
      fpta::schema_symbol(&schema_info, &col_a, err).operator std::string());
  EXPECT_EQ(FPTA_OK, err);
  EXPECT_EQ(
      "second_fp",
      fpta::schema_symbol(&schema_info, &col_b, err).operator std::string());
  EXPECT_EQ(FPTA_OK, err);
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //------------------------------------------------------------------------
  // создаем вторую таблицу в отдельной транзакции
  EXPECT_EQ(FPTA_EINVAL, fpta_transaction_begin(db, fpta_read, nullptr));
  EXPECT_EQ(FPTA_EFLAG, fpta_transaction_begin(db, (fpta_level)0, &txn));
  EXPECT_EQ(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_2", &def2));
  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(2u, schema_info.tables_count);
  EXPECT_EQ(FPTA_OK, fpta_schema_render(&schema_info, &tuple));
  EXPECT_NE(nullptr, tuple);
  EXPECT_EQ(nullptr, fptu::check(tuple));
  free(tuple);
  tuple = nullptr;
  EXPECT_EQ(
      "{\n    schema_format: 1,\n    schema_t1ha: "
      "\"1e605e662ac7c6d4949edc9cebc7a3fe\",\n    table: [\n        {\n        "
      "    name: \"table_1\",\n            column: [\n                {\n      "
      "              name: \"pk_str_uniq\",\n                    number: 0,\n  "
      "                  datatype: \"cstr\",\n                    nullable: "
      "false,\n                    index: \"primary\",\n                    "
      "unique: true,\n                    unordered: false,\n                  "
      "  reverse: false,\n                    mdbx: \"7onkrwutQ@@\"\n          "
      "      },\n                {\n                    name: "
      "\"first_uint\",\n                    number: 1,\n                    "
      "datatype: \"uint64\",\n                    nullable: false,\n           "
      "         index: \"secondary\",\n                    unique: false,\n    "
      "                unordered: false,\n                    reverse: "
      "false,\n                    mdbx: \"7onkrwutQ@0\"\n                },\n "
      "               {\n                    name: \"second_fp\",\n            "
      "        number: 2,\n                    datatype: \"fp64\",\n           "
      "         nullable: false,\n                    index: \"none\"\n        "
      "        }\n            ]\n        },\n        {\n            name: "
      "\"table_2\",\n            column: [\n                {\n                "
      "    name: \"x\",\n                    number: 0,\n                    "
      "datatype: \"cstr\",\n                    nullable: false,\n             "
      "       index: \"primary\",\n                    unique: true,\n         "
      "           unordered: false,\n                    reverse: false,\n     "
      "               mdbx: \"tz@R95ATDc@\"\n                },\n              "
      "  {\n                    name: \"y\",\n                    number: 1,\n "
      "                   datatype: \"cstr\",\n                    nullable: "
      "false,\n                    index: \"secondary\",\n                    "
      "unique: false,\n                    unordered: false,\n                 "
      "   reverse: false,\n                    mdbx: \"tz@R95ATDc0\"\n         "
      "       }\n            ]\n        }\n    ]\n}",
      fpta::schema2json(&schema_info, "    ").second);

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def2));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def2));
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));

  //------------------------------------------------------------------------
  // проверяем наличие второй таблицы и обновляем описание первой
  fpta_name table2, col_x, col_y;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table2, "table_2"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table2, &col_x, "x"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table2, &col_y, "y"));
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table2));
  EXPECT_EQ(2, fpta_table_column_count(&table2));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_x));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_y));

  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_pk));
  EXPECT_EQ(3, fpta_table_column_count(&table));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_a));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_b));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //------------------------------------------------------------------------
  // в отдельной транзакции удаляем первую таблицу
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(2u, schema_info.tables_count);
  // повторно проверяем получение схемы до удаления первой таблицы
  EXPECT_EQ(FPTA_OK, fpta_schema_render(&schema_info, &tuple));
  EXPECT_NE(nullptr, tuple);
  EXPECT_EQ(nullptr, fptu::check(tuple));
  free(tuple);
  tuple = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));

  // удаляем первую таблицу
  EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "Table_1"));
  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(1u, schema_info.tables_count);
  // проверяем получение схемы после удаления первой таблицы
  EXPECT_EQ(FPTA_OK, fpta_schema_render(&schema_info, &tuple));
  EXPECT_NE(nullptr, tuple);
  EXPECT_EQ(nullptr, fptu::check(tuple));
  free(tuple);
  tuple = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));

  // пробуем удалить несуществующую таблицу
  EXPECT_EQ(FPTA_NOTFOUND, fpta_table_drop(txn, "table_xyz"));
  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(1u, schema_info.tables_count);
  // повторно проверяем получение схемы после удаления первой таблицы
  EXPECT_EQ(FPTA_OK, fpta_schema_render(&schema_info, &tuple));
  EXPECT_NE(nullptr, tuple);
  EXPECT_EQ(nullptr, fptu::check(tuple));
  free(tuple);
  tuple = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));

  // обновляем описание второй таблицы (внутри транзакции изменения схемы)
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table2));
  EXPECT_EQ(2, fpta_table_column_count(&table2));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_x));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_y));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //------------------------------------------------------------------------
  // в отдельной транзакции удаляем вторую таблицу
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(1u, schema_info.tables_count);
  // еще раз проверяем получение схемы после удаления первой таблицы
  EXPECT_EQ(FPTA_OK, fpta_schema_render(&schema_info, &tuple));
  EXPECT_NE(nullptr, tuple);
  EXPECT_EQ(nullptr, fptu::check(tuple));
  free(tuple);
  tuple = nullptr;
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));

  // еще раз обновляем описание второй таблицы
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table2));
  EXPECT_EQ(2, fpta_table_column_count(&table2));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_x));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_y));

  // удаляем вторую таблицу
  EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "Table_2"));
  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(0u, schema_info.tables_count);
  EXPECT_EQ(FPTA_OK, fpta_schema_render(&schema_info, &tuple));
  EXPECT_NE(nullptr, tuple);
  EXPECT_EQ(nullptr, fptu::check(tuple));
  free(tuple);
  tuple = nullptr;
  EXPECT_EQ(
      "{schema_format:1,schema_t1ha:\"ad06cd0771697748a442f92c7a98808e\"}",
      fpta::schema2json(&schema_info).second);
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //------------------------------------------------------------------------
  // разрушаем привязанные идентификаторы
  fpta_name_destroy(&table);
  fpta_name_destroy(&col_pk);
  fpta_name_destroy(&col_a);
  fpta_name_destroy(&col_b);
  fpta_name_destroy(&probe_get);

  fpta_name_destroy(&table2);
  fpta_name_destroy(&col_x);
  fpta_name_destroy(&col_y);

  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

TEST(Schema, TriviaWithNullable) {
  /* Тривиальный тест создания/заполнения описания колонок таблицы,
   * включая nullable.
   *
   * Сценарий:
   *  - создаем/инициализируем описание колонок.
   *  - пробуем добавить несколько некорректных nullable колонок.
   *  - добавляем несколько корректных описаний nullable колонок.
   *
   * Тест НЕ перебирает все возможные комбинации, а только некоторые.
   * Такой относительно полный перебор происходит автоматически при
   * тестировании индексов и курсоров. */
  fpta_column_set def;
  fpta_column_set_init(&def);
  EXPECT_NE(FPTA_SUCCESS, fpta_column_set_validate(&def));

  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("col", fptu_int32,
                                 fpta_primary_unique_ordered_reverse_nullable,
                                 &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("col", fptu_int64,
                                 fpta_primary_unique_ordered_reverse_nullable,
                                 &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("col", fptu_fp32,
                                 fpta_primary_unique_ordered_reverse_nullable,
                                 &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("col", fptu_fp64,
                                 fpta_primary_unique_ordered_reverse_nullable,
                                 &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("col", fptu_datetime,
                                 fpta_primary_unique_ordered_reverse_nullable,
                                 &def));

  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("col", fptu_int32,
                                 fpta_primary_withdups_ordered_reverse_nullable,
                                 &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("col", fptu_int64,
                                 fpta_primary_withdups_ordered_reverse_nullable,
                                 &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("col", fptu_fp32,
                                 fpta_primary_withdups_ordered_reverse_nullable,
                                 &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("col", fptu_fp64,
                                 fpta_primary_withdups_ordered_reverse_nullable,
                                 &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("col", fptu_datetime,
                                 fpta_primary_withdups_ordered_reverse_nullable,
                                 &def));

  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("col", fptu_int32,
                                 fpta_secondary_unique_ordered_reverse_nullable,
                                 &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("col", fptu_int64,
                                 fpta_secondary_unique_ordered_reverse_nullable,
                                 &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("col", fptu_fp32,
                                 fpta_secondary_unique_ordered_reverse_nullable,
                                 &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("col", fptu_fp64,
                                 fpta_secondary_unique_ordered_reverse_nullable,
                                 &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("col", fptu_datetime,
                                 fpta_secondary_unique_ordered_reverse_nullable,
                                 &def));

  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe(
                "col", fptu_int32,
                fpta_secondary_withdups_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe(
                "col", fptu_int64,
                fpta_secondary_withdups_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe(
                "col", fptu_fp32,
                fpta_secondary_withdups_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe(
                "col", fptu_fp64,
                fpta_secondary_withdups_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe(
                "col", fptu_datetime,
                fpta_secondary_withdups_ordered_reverse_nullable, &def));

  //------------------------------------------------------------------------

  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdo0", fptu_uint16,
                         fpta_primary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdr0", fptu_uint16,
                         fpta_primary_withdups_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdo1", fptu_int32,
                         fpta_primary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdo2", fptu_uint32,
                         fpta_primary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdr2", fptu_uint32,
                         fpta_primary_withdups_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdo3", fptu_int64,
                         fpta_primary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdo4", fptu_uint64,
                         fpta_primary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdr4", fptu_uint64,
                         fpta_primary_withdups_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdo5", fptu_fp32,
                         fpta_primary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdo6", fptu_fp64,
                         fpta_primary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdo7", fptu_cstr,
                         fpta_primary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdr7", fptu_cstr,
                         fpta_primary_withdups_ordered_reverse_nullable, &def));

  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdo8", fptu_opaque,
                         fpta_primary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdr8", fptu_opaque,
                         fpta_primary_withdups_ordered_reverse_nullable, &def));

  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdo9", fptu_128,
                         fpta_primary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pdr9", fptu_128,
                         fpta_primary_withdups_ordered_reverse_nullable, &def));

  //------------------------------------------------------------------------

  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "puo0", fptu_uint16,
                         fpta_primary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pur0", fptu_uint16,
                         fpta_primary_unique_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "puo1", fptu_int32,
                         fpta_primary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "puo2", fptu_uint32,
                         fpta_primary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pur2", fptu_uint32,
                         fpta_primary_unique_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "puo3", fptu_int64,
                         fpta_primary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "puo4", fptu_uint64,
                         fpta_primary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pur4", fptu_uint64,
                         fpta_primary_unique_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "puo5", fptu_fp32,
                         fpta_primary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "puo6", fptu_fp64,
                         fpta_primary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "puo7", fptu_cstr,
                         fpta_primary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pur7", fptu_cstr,
                         fpta_primary_unique_ordered_reverse_nullable, &def));

  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "puo8", fptu_opaque,
                         fpta_primary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pur8", fptu_opaque,
                         fpta_primary_unique_ordered_reverse_nullable, &def));

  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "puo9", fptu_96,
                         fpta_primary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_reset(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "pur9", fptu_96,
                         fpta_primary_unique_ordered_reverse_nullable, &def));

  //------------------------------------------------------------------------

  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "suo0", fptu_uint16,
                         fpta_secondary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "sur0", fptu_uint16,
                         fpta_secondary_unique_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "suo1", fptu_int32,
                         fpta_secondary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "suo2", fptu_uint32,
                         fpta_secondary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "sur2", fptu_uint32,
                         fpta_secondary_unique_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "suo3", fptu_int64,
                         fpta_secondary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "suo4", fptu_uint64,
                         fpta_secondary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "sur4", fptu_uint64,
                         fpta_secondary_unique_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "suo5", fptu_fp32,
                         fpta_secondary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "suo6", fptu_fp64,
                         fpta_secondary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "suo7", fptu_cstr,
                         fpta_secondary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "sur7", fptu_cstr,
                         fpta_secondary_unique_ordered_reverse_nullable, &def));

  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "suo8", fptu_opaque,
                         fpta_secondary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "sur8", fptu_opaque,
                         fpta_secondary_unique_ordered_reverse_nullable, &def));

  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "suo9", fptu_160,
                         fpta_secondary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "sur9", fptu_160,
                         fpta_secondary_unique_ordered_reverse_nullable, &def));

  //------------------------------------------------------------------------

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdo0", fptu_uint16,
                fpta_secondary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdr0", fptu_uint16,
                fpta_secondary_withdups_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdo1", fptu_int32,
                fpta_secondary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdo2", fptu_uint32,
                fpta_secondary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdr2", fptu_uint32,
                fpta_secondary_withdups_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdo3", fptu_int64,
                fpta_secondary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdo4", fptu_uint64,
                fpta_secondary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdr4", fptu_uint64,
                fpta_secondary_withdups_ordered_reverse_nullable, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdo5", fptu_fp32,
                fpta_secondary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdo6", fptu_fp64,
                fpta_secondary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdo7", fptu_cstr,
                fpta_secondary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdr7", fptu_cstr,
                fpta_secondary_withdups_ordered_reverse_nullable, &def));

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdo8", fptu_opaque,
                fpta_secondary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdr8", fptu_opaque,
                fpta_secondary_withdups_ordered_reverse_nullable, &def));

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdo9", fptu_256,
                fpta_secondary_withdups_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "sdr9", fptu_256,
                fpta_secondary_withdups_ordered_reverse_nullable, &def));

  //------------------------------------------------------------------------

  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));
  EXPECT_EQ(FPTA_EINVAL, fpta_column_set_destroy(&def));
}

TEST(Schema, NonUniqPrimary_with_Secondary) {
  fpta_column_set def;
  fpta_column_set_init(&def);
  EXPECT_NE(FPTA_SUCCESS, fpta_column_set_validate(&def));

  //------------------------------------------------------------------------

  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("_id", fptu_uint64,
                                 fpta_secondary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "_last_changed", fptu_datetime,
                         fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "port", fptu_int64,
                         fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_EFLAG,
            fpta_column_describe("host", fptu_cstr,
                                 fpta_primary_withdups_ordered_obverse, &def));
  EXPECT_NE(FPTA_SUCCESS, fpta_column_set_validate(&def));

  //------------------------------------------------------------------------

  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));
  EXPECT_EQ(FPTA_EINVAL, fpta_column_set_destroy(&def));
}

//----------------------------------------------------------------------------

TEST(Schema, FailingDrop) {
  /* Сценарий:
   *  - открываем базу в режиме изменяемой схемы.
   *  - создаем три таблицы, две с одной primary-колонкой,
   *    третью с двумя неиндексируемыми и одной composite-колонкой.
   *  - в новой транзакции проверяем, что в базе есть три таблицы,
   *    и последовательно удаляем их.
   *
   * Результат:
   *  - на удалении table_2 срабатывает ассерт index_id < fpta_max_indexes
   *    (details.h:176)
   *  - если порядок удаления поменять на table_2, table_1, table_3,
   *    ассерт срабатывает на удалении table_1
   *  - если table_3 удалять не последней, тест успешно завершается
   *  - если не удалять одну из table_1, table_2, тест успешно завершается */
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  fpta_db *db = nullptr;
  /* открываем базу с возможностью изменять схему */
  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  1, true, &db));
  ASSERT_NE(nullptr, db);

  // формируем описание колонок для первой таблицы
  fpta_column_set def;
  fpta_column_set_init(&def);
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("field", fptu_cstr,
                                 fpta_primary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  // формируем описание колонок для второй таблицы
  fpta_column_set def2;
  fpta_column_set_init(&def2);
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("field", fptu_cstr,
                                 fpta_primary_unique_ordered_obverse, &def2));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def2));

  // формируем описание колонок для третьей таблицы
  fpta_column_set def3;
  fpta_column_set_init(&def3);
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("part_1", fptu_cstr, fpta_index_none, &def3));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("part_2", fptu_cstr, fpta_index_none, &def3));
  EXPECT_EQ(FPTA_OK, fpta_describe_composite_index_va(
                         "field", fpta_primary_unique_ordered_obverse, &def3,
                         "part_1", "part_2", nullptr));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def3));

  //------------------------------------------------------------------------
  // создаем таблицы в транзакции
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_1", &def));
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_2", &def2));
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_3", &def3));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def2));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def2));
  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def3));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def3));

  //------------------------------------------------------------------------
  // в отдельной транзакции удаляем таблицы
  fpta_schema_info schema_info;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(3u, schema_info.tables_count);
  EXPECT_EQ(
      "{\n    schema_format: 1,\n    schema_t1ha: "
      "\"e3e9688ae2db035e8c794e15d5ec49c6\",\n    table: [\n        {\n        "
      "    name: \"table_3\",\n            column: [\n                {\n      "
      "              name: \"field\",\n                    number: 0,\n        "
      "            datatype: \"composite\",\n                    nullable: "
      "false,\n                    index: \"primary\",\n                    "
      "unique: true,\n                    unordered: false,\n                  "
      "  reverse: false,\n                    tersely: false,\n                "
      "    mdbx: \"643JRAjGFy@\",\n                    composite_items: [\n    "
      "                    \"part_1\",\n                        \"part_2\"\n   "
      "                 ]\n                },\n                {\n             "
      "       name: \"part_1\",\n                    number: 1,\n              "
      "      datatype: \"cstr\",\n                    nullable: false,\n       "
      "             index: \"none\"\n                },\n                {\n   "
      "                 name: \"part_2\",\n                    number: 2,\n    "
      "                datatype: \"cstr\",\n                    nullable: "
      "false,\n                    index: \"none\"\n                }\n        "
      "    ]\n        },\n        {\n            name: \"table_1\",\n          "
      "  column: {\n                name: \"field\",\n                number: "
      "0,\n                datatype: \"cstr\",\n                nullable: "
      "false,\n                index: \"primary\",\n                unique: "
      "true,\n                unordered: false,\n                reverse: "
      "false,\n                mdbx: \"7onkrwutQ@@\"\n            }\n        "
      "},\n        {\n            name: \"table_2\",\n            column: {\n  "
      "              name: \"field\",\n                number: 0,\n            "
      "    datatype: \"cstr\",\n                nullable: false,\n             "
      "   index: \"primary\",\n                unique: true,\n                "
      "unordered: false,\n                reverse: false,\n                "
      "mdbx: \"tz@R95ATDc@\"\n            }\n        }\n    ]\n}",
      fpta::schema2json(&schema_info, "    ").second);
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));

  // удаляем первую таблицу
  EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "table_1"));
  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(2u, schema_info.tables_count);
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));

  // удаляем вторую таблицу
  EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "table_2"));
  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(1u, schema_info.tables_count);
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));

  // удаляем третью таблицу
  EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "table_3"));
  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(0u, schema_info.tables_count);
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

//----------------------------------------------------------------------------

TEST(Schema, FailingClear) {
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  fpta_db *db = nullptr;
  /* открываем базу с возможностью изменять схему */
  ASSERT_EQ(FPTA_SUCCESS, test_db_open(testdb_name, fpta_weak,
                                       fpta_regime_default, 1, true, &db));
  ASSERT_NE(nullptr, db);

  // формируем описание колонок для таблицы
  fpta_column_set def;
  fpta_column_set_init(&def);
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("field_1", fptu_cstr, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("field_2", fptu_int64,
                                 fpta_primary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "field_3", fptu_cstr,
                         fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  //------------------------------------------------------------------------
  // создаем таблицу
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

  //------------------------------------------------------------------------
  // очищаем таблицу
  fpta_name table;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));

  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  ASSERT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));

  EXPECT_EQ(FPTA_OK, fpta_table_clear(txn, &table, true));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  fpta_name_destroy(&table);

  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
}

//----------------------------------------------------------------------------

TEST(Schema, SameNames) {
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  fpta_db *db = nullptr;
  /* открываем базу с возможностью изменять схему */
  ASSERT_EQ(FPTA_SUCCESS, test_db_open(testdb_name, fpta_weak,
                                       fpta_regime_default, 1, true, &db));
  ASSERT_NE(nullptr, db);

  // формируем описание колонок для таблиц
  fpta_column_set def;
  fpta_column_set_init(&def);
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("a", fptu_cstr, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("b", fptu_int64,
                                 fpta_primary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe(
                "c", fptu_cstr, fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  //------------------------------------------------------------------------
  // создаем таблицу
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "a", &def));
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "b", &def));
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "C", &def));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

  //------------------------------------------------------------------------
  // запрашиваем информацию о схеме и о каждой таблице
  fpta_name table_a, table_b, table_c;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table_a, "a"));
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table_b, "B"));
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table_c, "c"));

  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);

  fpta_schema_info schema_info;
  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(3u, schema_info.tables_count);
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));

  size_t row_count;
  fpta_table_stat table_stat;
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table_a, &row_count, &table_stat));
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table_b, &row_count, &table_stat));
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table_c, &row_count, &table_stat));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //------------------------------------------------------------------------
  // по-очередно удаляем таблицы

  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "A"));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(2u, schema_info.tables_count);
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));
  EXPECT_EQ(FPTA_NOTFOUND,
            fpta_table_info(txn, &table_a, &row_count, &table_stat));
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table_b, &row_count, &table_stat));
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table_c, &row_count, &table_stat));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "c"));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(1u, schema_info.tables_count);
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));
  EXPECT_EQ(FPTA_NOTFOUND,
            fpta_table_info(txn, &table_a, &row_count, &table_stat));
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table_b, &row_count, &table_stat));
  EXPECT_EQ(FPTA_NOTFOUND,
            fpta_table_info(txn, &table_c, &row_count, &table_stat));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "B"));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  EXPECT_EQ(0u, schema_info.tables_count);
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));
  EXPECT_EQ(FPTA_NOTFOUND,
            fpta_table_info(txn, &table_a, &row_count, &table_stat));
  EXPECT_EQ(FPTA_NOTFOUND,
            fpta_table_info(txn, &table_b, &row_count, &table_stat));
  EXPECT_EQ(FPTA_NOTFOUND,
            fpta_table_info(txn, &table_c, &row_count, &table_stat));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //------------------------------------------------------------------------

  fpta_name_destroy(&table_a);
  fpta_name_destroy(&table_b);
  fpta_name_destroy(&table_c);

  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
}

//----------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
