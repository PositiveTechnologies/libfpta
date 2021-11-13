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
  EXPECT_EQ(FPTA_EINVAL, fpta_transaction_begin(db, fpta_read, nullptr));
  EXPECT_EQ(FPTA_EFLAG, fpta_transaction_begin(db, (fpta_level)0, &txn));
  EXPECT_EQ(nullptr, txn);

  // создаем первую таблицу в отдельной транзакции
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
      "\"2935e2cdbecc9dc6eea976dd5312aa8a\",\n    table: {\n        name: "
      "\"table_1\",\n        column: [\n            {\n                name: "
      "\"pk_str_uniq\",\n                number: 0,\n                datatype: "
      "\"cstr\",\n                nullable: false,\n                index: "
      "\"primary\",\n                unique: true,\n                unordered: "
      "false,\n                reverse: false,\n                mdbx: "
      "\"q35_zeSCP@@\"\n            },\n            {\n                name: "
      "\"first_uint\",\n                number: 1,\n                datatype: "
      "\"uint64\",\n                nullable: false,\n                index: "
      "\"secondary\",\n                unique: false,\n                "
      "unordered: false,\n                reverse: false,\n                "
      "mdbx: \"q35_zeSCP@0\"\n            },\n            {\n                "
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
  EXPECT_EQ(UINT64_C(12756162147867353344), table.shove);
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table_1"));
  EXPECT_EQ(UINT64_C(12756162147867353344), table.shove);
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_pk, "pk_str_uniq"));
  EXPECT_EQ(UINT64_C(5639804144706044928), col_pk.shove);
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_a, "First_Uint"));
  EXPECT_EQ(UINT64_C(5795317090906267648), col_a.shove);
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &col_b, "second_FP"));
  EXPECT_EQ(UINT64_C(12049727541333069824), col_b.shove);

  EXPECT_GT(0, fpta_table_column_count(&table));
  EXPECT_EQ(FPTA_EINVAL, fpta_table_column_get(&table, 0, &probe_get));
  EXPECT_EQ(UINT64_C(12756162147867353344), table.shove);

  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &col_pk));
  EXPECT_EQ(UINT64_C(12756162147867353344), table.shove);
  EXPECT_EQ(UINT64_C(5639804144706045165), col_pk.shove);
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_a));
  EXPECT_EQ(UINT64_C(5795317090906268102), col_a.shove);
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_b));
  EXPECT_EQ(UINT64_C(12049727541333069831), col_b.shove);

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
      "\"0299bc96d94acbff38bc892f1e23e732\",\n    table: [\n        {\n        "
      "    name: \"table_2\",\n            column: [\n                {\n      "
      "              name: \"x\",\n                    number: 0,\n            "
      "        datatype: \"cstr\",\n                    nullable: false,\n     "
      "               index: \"primary\",\n                    unique: true,\n "
      "                   unordered: false,\n                    reverse: "
      "false,\n                    mdbx: \"9LXd44eN3y@\"\n                },\n "
      "               {\n                    name: \"y\",\n                    "
      "number: 1,\n                    datatype: \"cstr\",\n                   "
      " nullable: false,\n                    index: \"secondary\",\n          "
      "          unique: false,\n                    unordered: false,\n       "
      "             reverse: false,\n                    mdbx: "
      "\"9LXd44eN3y0\"\n                }\n            ]\n        },\n        "
      "{\n            name: \"table_1\",\n            column: [\n              "
      "  {\n                    name: \"pk_str_uniq\",\n                    "
      "number: 0,\n                    datatype: \"cstr\",\n                   "
      " nullable: false,\n                    index: \"primary\",\n            "
      "        unique: true,\n                    unordered: false,\n          "
      "          reverse: false,\n                    mdbx: \"q35_zeSCP@@\"\n  "
      "              },\n                {\n                    name: "
      "\"first_uint\",\n                    number: 1,\n                    "
      "datatype: \"uint64\",\n                    nullable: false,\n           "
      "         index: \"secondary\",\n                    unique: false,\n    "
      "                unordered: false,\n                    reverse: "
      "false,\n                    mdbx: \"q35_zeSCP@0\"\n                },\n "
      "               {\n                    name: \"second_fp\",\n            "
      "        number: 2,\n                    datatype: \"fp64\",\n           "
      "         nullable: false,\n                    index: \"none\"\n        "
      "        }\n            ]\n        }\n    ]\n}",
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
      "{schema_format:1,schema_t1ha:\"56a25e1b430952eaca159a02d9763a90\"}",
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
   *    и последовательно удаляем их. */
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
      "\"12e2a2fc43e0c87f685b8b0e963c86e4\",\n    table: [\n        {\n        "
      "    name: \"table_2\",\n            column: {\n                name: "
      "\"field\",\n                number: 0,\n                datatype: "
      "\"cstr\",\n                nullable: false,\n                index: "
      "\"primary\",\n                unique: true,\n                unordered: "
      "false,\n                reverse: false,\n                mdbx: "
      "\"9LXd44eN3y@\"\n            }\n        },\n        {\n            "
      "name: \"table_1\",\n            column: {\n                name: "
      "\"field\",\n                number: 0,\n                datatype: "
      "\"cstr\",\n                nullable: false,\n                index: "
      "\"primary\",\n                unique: true,\n                unordered: "
      "false,\n                reverse: false,\n                mdbx: "
      "\"q35_zeSCP@@\"\n            }\n        },\n        {\n            "
      "name: \"table_3\",\n            column: [\n                {\n          "
      "          name: \"field\",\n                    number: 0,\n            "
      "        datatype: \"composite\",\n                    nullable: "
      "false,\n                    index: \"primary\",\n                    "
      "unique: true,\n                    unordered: false,\n                  "
      "  reverse: false,\n                    tersely: false,\n                "
      "    mdbx: \"qxQ3c@Gdp@@\",\n                    composite_items: [\n    "
      "                    \"part_1\",\n                        \"part_2\"\n   "
      "                 ]\n                },\n                {\n             "
      "       name: \"part_1\",\n                    number: 1,\n              "
      "      datatype: \"cstr\",\n                    nullable: false,\n       "
      "             index: \"none\"\n                },\n                {\n   "
      "                 name: \"part_2\",\n                    number: 2,\n    "
      "                datatype: \"cstr\",\n                    nullable: "
      "false,\n                    index: \"none\"\n                }\n        "
      "    ]\n        }\n    ]\n}",
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

//--------------------------------------------------------------------------

TEST(Schema, CancelledTableDrop) {
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  fpta_db *db = nullptr;
  ASSERT_EQ(FPTA_SUCCESS, test_db_open(testdb_name, fpta_weak,
                                       fpta_regime_default, 1, true, &db));
  ASSERT_NE(nullptr, db);

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

  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  // создаём 2 таблицы
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_permanent", &def));
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_temporary", &def));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;
  fpta_name table_a;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table_a, "table_permanent"));
  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));

  // опрашиваем table_permanent
  size_t row_count;
  fpta_table_stat table_stat;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table_a, &row_count, &table_stat));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //------------------------------------------------------------------------
  // дропаем 2ю таблицу, очищаем 1ю
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "table_temporary"));
  EXPECT_EQ(FPTA_OK, fpta_table_clear(txn, &table_a, true));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, true));
  txn = nullptr;

  //------------------------------------------------------------------------
  // опрашиваем информацию о не изменившейся таблице
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table_a, &row_count, &table_stat));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //------------------------------------------------------------------------
  fpta_name_destroy(&table_a);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
}

//----------------------------------------------------------------------------

TEST(Schema, PreviousDbiReuse) {
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  fpta_db *db = nullptr;
  ASSERT_EQ(FPTA_SUCCESS, test_db_open(testdb_name, fpta_weak,
                                       fpta_regime_default, 1, true, &db));
  ASSERT_NE(nullptr, db);

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

  fpta_txn *txn = (fpta_txn *)&txn;

  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  // создаём первую таблицу
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_primary", &def));
  fpta_name table_original;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table_original, "table_primary"));
  EXPECT_EQ(FPTA_OK, fpta_table_clear(txn, &table_original, true));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //------------------------------------------------------------------------
  // удаляем первую таблицу, создаём вторую
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "table_primary"));
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_secondary", &def));
  fpta_name table_secondary;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table_secondary, "table_secondary"));
  EXPECT_EQ(FPTA_OK, fpta_table_clear(txn, &table_secondary, true));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));

  //------------------------------------------------------------------------
  // опрашиваем информацию о новой таблице
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);

  size_t row_count;
  fpta_table_stat table_stat;
  EXPECT_EQ(FPTA_OK,
            fpta_table_info(txn, &table_secondary, &row_count, &table_stat));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //------------------------------------------------------------------------

  fpta_name_destroy(&table_secondary);
  fpta_name_destroy(&table_original);

  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
}

//----------------------------------------------------------------------------

TEST(Schema, Overkill) {
  /* Сценарий:
   *  1. Открываем базу в режиме изменяемой схемы.
   *  2. Создаем fpta_tables_max таблиц, в каждой из которых
   *     от 2 до fpta_max_cols.
   *  3. Создавая таблицы также создаем индексы для её колонок,
   *     но не превышая:
   *       - fpta_max_indexes для одной таблице;
   *       - fpta_max_dbi суммарно для всех таблиц и колонок;
   *  4. Создавая таблицы начинаем с минимального числа колонок и индексов,
   *     чтобы при большой схеме (~1000 таблиц от 2 до ~1000 колонок и индексов)
   *     выполнять минимум итераций теста, т.е. уйти от O((T*C)^3).
   *  5. Коммитим транзакцию, для проверки переоткрываем БД и читаем схему. */
  bool skipped = GTEST_IS_EXECUTION_TIMEOUT();
  if (skipped)
    return;

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

  //------------------------------------------------------------------------
  // создаем таблицы в транзакции
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  fpta_column_set def;
  unsigned whole_dbi = 0;
  for (unsigned table_count = 0;
       table_count < fpta_tables_max && whole_dbi < fpta_max_dbi;
       ++table_count) {
    const unsigned left_tbl = fpta_tables_max - table_count;
    const unsigned left_dbi = fpta_max_dbi - whole_dbi;
    const unsigned target_column =
        ((left_tbl - 1) * (std::min(fpta_max_indexes, fpta_max_cols) - 3) >
         left_dbi)
            ? 2u
            : unsigned(fpta_max_cols);

    // std::cout << "left_dbi " << left_dbi << ", left_tbl " << left_tbl
    //           << ", target_column " << target_column << "\n";

    SCOPED_TRACE("table #" + std::to_string(table_count) + ", whole DBI #" +
                 std::to_string(whole_dbi));
    fpta_column_set_init(&def);
    std::string table_name = fptu::format("tbl_%04u", table_count);
    ASSERT_EQ(FPTA_OK,
              fpta_column_describe("pk", fptu_uint32,
                                   fpta_primary_unique_ordered_obverse, &def));
    ++whole_dbi;

    unsigned index_count = 0;
    for (unsigned column_count = 1; column_count < target_column;
         ++column_count) {
      ASSERT_EQ(FPTA_OK, fpta_column_set_validate(&def));
      std::string colunm_name = fptu::format("col_%04u", column_count);
      if (whole_dbi < fpta_max_dbi && index_count < fpta_max_indexes) {
        SCOPED_TRACE("column #" + std::to_string(column_count) + " of " +
                     std::to_string(target_column) + ", whole DBI #" +
                     std::to_string(whole_dbi));
        EXPECT_EQ(FPTA_OK, fpta_column_describe(
                               colunm_name.c_str(), fptu_cstr,
                               fpta_secondary_withdups_ordered_obverse, &def));
        ++whole_dbi;
        ++index_count;
      } else {
        if (index_count >= fpta_max_indexes) {
          // пробуем добавить лишний индекс
          EXPECT_EQ(FPTA_TOOMANY,
                    fpta_column_describe(
                        "overkill", fptu_cstr,
                        fpta_secondary_withdups_ordered_obverse, &def));
        }
        EXPECT_EQ(FPTA_OK, fpta_column_describe(colunm_name.c_str(), fptu_cstr,
                                                fpta_index_none, &def));
      }
    }

    if (target_column >= fpta_max_cols) {
      // пробуем добавить лишнюю колонку
      EXPECT_EQ(FPTA_TOOMANY, fpta_column_describe("overkill", fptu_cstr,
                                                   fpta_index_none, &def));
    }
    ASSERT_EQ(FPTA_OK, fpta_table_create(txn, table_name.c_str(), &def));
    ASSERT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
    skipped = GTEST_IS_EXECUTION_TIMEOUT();
    if (skipped)
      break;
  }

  if (!skipped) {
    ASSERT_EQ(unsigned(fpta_max_dbi), whole_dbi);

    // пробуем создать лишнюю таблицу
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("pk", fptu_uint32,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "se", fptu_cstr,
                           fpta_secondary_withdups_ordered_obverse, &def));
    EXPECT_EQ(FPTA_TOOMANY, fpta_table_create(txn, "overkill", &def));
    EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  }

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;
  //------------------------------------------------------------------------

  fpta_schema_info schema_info;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_schema_fetch(txn, &schema_info));
  if (!skipped) {
    EXPECT_EQ(unsigned(fpta_tables_max), schema_info.tables_count);
    EXPECT_EQ(unsigned(fpta_max_dbi),
              schema_info.indexes_count + schema_info.tables_count);
  }
  EXPECT_EQ(FPTA_OK, fpta_schema_destroy(&schema_info));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;
  //------------------------------------------------------------------------

  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
}

//----------------------------------------------------------------------------

TEST(Schema, PreviousDbiReuseBig) {
  bool skipped = GTEST_IS_EXECUTION_TIMEOUT();
  if (skipped)
    return;

  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  fpta_db *db = nullptr;
  ASSERT_EQ(FPTA_SUCCESS, test_db_open(testdb_name, fpta_weak,
                                       fpta_regime_default, 1, true, &db));
  ASSERT_NE(nullptr, db);

  fpta_column_set def;
  fpta_column_set_init(&def);
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("a", fptu_cstr, fpta_index_none, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("b", fptu_int64,
                                 fpta_primary_unique_ordered_obverse, &def));
  int idx = 0;
  for (idx = 0; idx < 783; idx++) {
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           ("c_" + std::to_string(idx)).c_str(), fptu_cstr,
                           fpta_secondary_withdups_ordered_obverse, &def));
    skipped = GTEST_IS_EXECUTION_TIMEOUT();
    if (skipped)
      break;
  }
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  fpta_txn *txn = (fpta_txn *)&txn;

  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  // создаём первые 3 таблицы
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_first", &def));
  fpta_name table_first;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table_first, "table_first"));
  EXPECT_EQ(FPTA_OK, fpta_table_clear(txn, &table_first, true));

  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_second", &def));
  fpta_name table_second;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table_second, "table_second"));
  EXPECT_EQ(FPTA_OK, fpta_table_clear(txn, &table_second, true));

  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_third", &def));
  fpta_name table_third;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table_third, "table_third"));
  EXPECT_EQ(FPTA_OK, fpta_table_clear(txn, &table_third, true));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //------------------------------------------------------------------------
  // удаляем их, создаём вторые три
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "table_third"));
  EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "table_second"));
  EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "table_first"));
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_first_new", &def));
  fpta_name table_first_new;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table_first_new, "table_first_new"));
  EXPECT_EQ(FPTA_OK, fpta_table_clear(txn, &table_first_new, true));

  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_second_new", &def));
  fpta_name table_second_new;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table_second_new, "table_second_new"));
  EXPECT_EQ(FPTA_OK, fpta_table_clear(txn, &table_second_new, true));

  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_third_new", &def));
  fpta_name table_third_new;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table_third_new, "table_third_new"));
  EXPECT_EQ(FPTA_OK, fpta_table_clear(txn, &table_third_new, true));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));

  size_t row_count;
  fpta_table_stat table_stat;

  //------------------------------------------------------------------------
  // опрашиваем информацию о новых трёх таблицах
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);

  EXPECT_EQ(FPTA_OK,
            fpta_table_info(txn, &table_first_new, &row_count, &table_stat));

  EXPECT_EQ(FPTA_OK,
            fpta_table_info(txn, &table_second_new, &row_count, &table_stat));

  EXPECT_EQ(FPTA_OK,
            fpta_table_info(txn, &table_third_new, &row_count, &table_stat));

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  //------------------------------------------------------------------------

  fpta_name_destroy(&table_first);
  fpta_name_destroy(&table_second);
  fpta_name_destroy(&table_third);
  fpta_name_destroy(&table_first_new);
  fpta_name_destroy(&table_second_new);
  fpta_name_destroy(&table_third_new);

  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
}

//----------------------------------------------------------------------------

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
