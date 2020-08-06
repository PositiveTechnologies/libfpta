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

static const char testdb_name[] = TEST_DB_DIR "ut_open.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_open.fpta" MDBX_LOCK_SUFFIX;

TEST(Open, Trivia) {
  /* Тривиальный тест открытия/создания БД во всех режимах durability.
   * Корректность самих режимов не проверяется. */
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  fpta_db *db = (fpta_db *)&db;
  EXPECT_EQ(ENOENT, test_db_open(testdb_name, fpta_readonly,
                                 fpta_regime_default, 1, false, &db));
  EXPECT_EQ(nullptr, db);
  ASSERT_TRUE(REMOVE_FILE(testdb_name) != 0 && errno == ENOENT);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) != 0 && errno == ENOENT);

  ASSERT_EQ(FPTA_OK,
            test_db_open(testdb_name, fpta_sync, fpta_saferam, 1, false, &db));
  EXPECT_NE(nullptr, db);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);

  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_sync,
                                  fpta_frendly4writeback, 1, true, &db));
  EXPECT_NE(nullptr, db);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);

  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_lazy,
                                  fpta_frendly4compaction, 1, false, &db));
  EXPECT_NE(nullptr, db);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);

  ASSERT_EQ(FPTA_OK,
            test_db_open(testdb_name, fpta_weak,
                         fpta_frendly4writeback | fpta_frendly4compaction, 1,
                         false, &db));
  EXPECT_NE(nullptr, db);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

TEST(Open, SingleProcess_ChangeDbSize) {
  // чистим
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  fpta_db_stat_t stat;
  fpta_db *db = nullptr;
  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  1, false, &db));
  ASSERT_NE(nullptr, db);
  ASSERT_EQ(FPTA_OK, fpta_db_info(db, nullptr, &stat));
  EXPECT_EQ(stat.geo.current, 1u * 1024 * 1024);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  0, false, &db));
  ASSERT_NE(nullptr, db);
  ASSERT_EQ(FPTA_OK, fpta_db_info(db, nullptr, &stat));
  EXPECT_EQ(stat.geo.current, 1u * 1024 * 1024);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  32, false, &db));
  ASSERT_NE(nullptr, db);
  ASSERT_EQ(FPTA_OK, fpta_db_info(db, nullptr, &stat));
  EXPECT_EQ(stat.geo.current, 32u * 1024 * 1024);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  0, false, &db));
  ASSERT_NE(nullptr, db);
  ASSERT_EQ(FPTA_OK, fpta_db_info(db, nullptr, &stat));
  EXPECT_EQ(stat.geo.current, 32u * 1024 * 1024);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  3, false, &db));
  ASSERT_NE(nullptr, db);
  ASSERT_EQ(FPTA_OK, fpta_db_info(db, nullptr, &stat));
  EXPECT_EQ(stat.geo.current, 3u * 1024 * 1024);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  fpta_db_creation_params_t creation_params;
  creation_params.params_size = sizeof(creation_params);
  creation_params.file_mode = 0640;
  creation_params.size_lower = creation_params.size_upper = 8 << 20;
  creation_params.growth_step = 0;
  creation_params.shrink_threshold = 0;
  creation_params.pagesize = -1;
  ASSERT_EQ(FPTA_OK,
            fpta_db_create_or_open(nullptr, testdb_name, fpta_weak,
                                   fpta_saferam, true, &db, &creation_params));
  ASSERT_NE(nullptr, db);
  ASSERT_EQ(FPTA_OK, fpta_db_info(db, nullptr, &stat));
  EXPECT_EQ(stat.geo.current, 8u * 1024 * 1024);
  EXPECT_EQ(stat.geo.lower, 8u * 1024 * 1024);
  EXPECT_EQ(stat.geo.upper, 8u * 1024 * 1024);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));
}

TEST(Open, MultipleProcesses_ChangeGeometry) {
  // чистим
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  fpta_db_creation_params_t creation_params;
  creation_params.params_size = sizeof(creation_params);
  creation_params.file_mode = 0640;
  creation_params.size_lower = 1 << 20;
  creation_params.size_upper = 42 << 20;
  creation_params.pagesize = 65536;
  creation_params.growth_step = -1;
  creation_params.shrink_threshold = -1;

  fpta_db_stat_t stat;
  fpta_db *db_commander = nullptr;
  ASSERT_EQ(FPTA_OK, fpta_db_create_or_open(nullptr, testdb_name, fpta_weak,
                                            fpta_regime_default, true,
                                            &db_commander, &creation_params));
  ASSERT_EQ(FPTA_OK, fpta_db_info(db_commander, nullptr, &stat));
  EXPECT_EQ(stat.geo.lower, 1u * 1024 * 1024);
  EXPECT_EQ(stat.geo.current, 1u * 1024 * 1024);
  EXPECT_EQ(stat.geo.upper, 42u * 1024 * 1024);
  EXPECT_EQ(stat.geo.pagesize, 65536u);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db_commander));

  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  0, false, &db_commander));
  ASSERT_NE(nullptr, db_commander);
  ASSERT_EQ(FPTA_OK, fpta_db_info(db_commander, nullptr, &stat));
  EXPECT_EQ(stat.geo.lower, 1u * 1024 * 1024);
  EXPECT_EQ(stat.geo.current, 1u * 1024 * 1024);
  EXPECT_EQ(stat.geo.upper, 42u * 1024 * 1024);
  EXPECT_EQ(stat.geo.pagesize, 65536u);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db_commander));

  creation_params.pagesize = -1 /* default */;
  ASSERT_EQ(FPTA_OK, fpta_db_create_or_open(nullptr, testdb_name, fpta_weak,
                                            fpta_regime_default, true,
                                            &db_commander, &creation_params));
  ASSERT_NE(nullptr, db_commander);
  ASSERT_EQ(FPTA_OK, fpta_db_info(db_commander, nullptr, &stat));
  EXPECT_EQ(stat.geo.lower, 1u * 1024 * 1024);
  EXPECT_EQ(stat.geo.current, 1u * 1024 * 1024);
  EXPECT_EQ(stat.geo.upper, 42u * 1024 * 1024);
  EXPECT_EQ(stat.geo.pagesize, 65536u);
  EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db_commander));

  // открываем БД в исполнителе
  fpta_db *db_executor = nullptr;
  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  0, false, &db_executor));
  ASSERT_NE(nullptr, db_executor);

  // изменяем размер БД в коммандере
  creation_params.size_lower = creation_params.size_upper = 8 << 20;
  creation_params.growth_step = 0;
  creation_params.shrink_threshold = 0;
  if (FPTA_PRESERVE_GEOMETRY) {
    /* With FPTA_PRESERVE_GEOMETRY libpfta will not apply provided geometry
     * after open database and MDBX (for historical reasons) should preserve
     * existing geometry for database which used/open by another process. */
    creation_params.pagesize = 4096;
    ASSERT_EQ(FPTA_OK, fpta_db_create_or_open(nullptr, testdb_name, fpta_weak,
                                              fpta_regime_default, true,
                                              &db_commander, &creation_params));
    ASSERT_NE(nullptr, db_commander);
    ASSERT_EQ(FPTA_OK, fpta_db_info(db_commander, nullptr, &stat));
    EXPECT_EQ(stat.geo.current, 1u * 1024 * 1024);
    EXPECT_EQ(stat.geo.lower, 1u * 1024 * 1024);
    EXPECT_EQ(stat.geo.upper, 42u * 1024 * 1024);
    EXPECT_EQ(stat.geo.pagesize, 65536u);
    EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db_commander));

    // закрывает БД в "исполнителе"
    EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db_executor));
    // снова открываем в "коммандере", теперь размер должен измениться,
    // но размер страници должен остаться прежним
    ASSERT_EQ(FPTA_OK, fpta_db_create_or_open(nullptr, testdb_name, fpta_weak,
                                              fpta_regime_default, true,
                                              &db_commander, &creation_params));
    ASSERT_NE(nullptr, db_commander);
    ASSERT_EQ(FPTA_OK, fpta_db_info(db_commander, nullptr, &stat));
    EXPECT_EQ(stat.geo.current, 8u * 1024 * 1024);
    EXPECT_EQ(stat.geo.lower, 8u * 1024 * 1024);
    EXPECT_EQ(stat.geo.upper, 8u * 1024 * 1024);
    EXPECT_EQ(stat.geo.pagesize, 65536u);
    EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db_commander));

  } else {
    /* Without FPTA_PRESERVE_GEOMETRY libpfta will re-apply provided geometry
     * after open database, so MDBX apply new geometry for database even it
     * used/open by another process. */
    ASSERT_EQ(FPTA_OK, fpta_db_create_or_open(nullptr, testdb_name, fpta_weak,
                                              fpta_regime_default, true,
                                              &db_commander, &creation_params));
    ASSERT_NE(nullptr, db_commander);
    ASSERT_EQ(FPTA_OK, fpta_db_info(db_commander, nullptr, &stat));
    EXPECT_EQ(stat.geo.current, 8u * 1024 * 1024);
    EXPECT_EQ(stat.geo.lower, 8u * 1024 * 1024);
    EXPECT_EQ(stat.geo.upper, 8u * 1024 * 1024);
    EXPECT_EQ(stat.geo.pagesize, 65536u);
    EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db_commander));

    // проверяем размер БД в исполнителе
    ASSERT_EQ(FPTA_OK, fpta_db_info(db_executor, nullptr, &stat));
    EXPECT_EQ(stat.geo.current, 8u * 1024 * 1024);
    EXPECT_EQ(stat.geo.lower, 8u * 1024 * 1024);
    EXPECT_EQ(stat.geo.upper, 8u * 1024 * 1024);
    EXPECT_EQ(stat.geo.pagesize, 65536u);
    EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db_executor));

    // проверяем невозможность изменить размер страницы
    creation_params.pagesize = 4096;
    ASSERT_EQ(FPTA_DB_INCOMPAT,
              fpta_db_create_or_open(nullptr, testdb_name, fpta_weak,
                                     fpta_regime_default, true, &db_executor,
                                     &creation_params));
    ASSERT_EQ(nullptr, db_executor);
  }
}

TEST(Open, AppContent) {
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  fpta_db_creation_params_t creation_params;
  creation_params.params_size = sizeof(creation_params);
  creation_params.file_mode = 0640;
  creation_params.size_lower = creation_params.size_upper = 8 << 20;
  creation_params.growth_step = 0;
  creation_params.shrink_threshold = 0;
  creation_params.pagesize = -1;

  // создаем тестовую БД
  fpta_db *db = (fpta_db *)&db;
  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_lazy,
                                  fpta_frendly4compaction, 1, false, &db));
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  // открывает тестовую БД от имени несовместимого приложения
  // ошибки быть не должно, так как БД пустая (нет схемы).
  static const fpta_appcontent_info legacy{1, 2, "proba"};
  ASSERT_EQ(FPTA_OK, fpta_db_create_or_open(&legacy, testdb_name, fpta_weak,
                                            fpta_regime_default, true, &db,
                                            &creation_params));
  EXPECT_NE(nullptr, db);

  // создаем таблицу, только чтобы БД не была пустой (со схемой)
  fpta_column_set def;
  fpta_column_set_init(&def);
  EXPECT_NE(FPTA_SUCCESS, fpta_column_set_validate(&def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("column_a", fptu_cstr,
                                 fpta_primary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "column_b", fptu_cstr,
                         fpta_secondary_withdups_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_1", &def));
  EXPECT_EQ(FPTA_OK, fpta_transaction_commit(txn));
  txn = nullptr;
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  // пробует открыть созданную БД как тестовую
  ASSERT_EQ(FPTA_APP_MISMATCH,
            test_db_open(testdb_name, fpta_lazy, fpta_frendly4compaction, 1,
                         false, &db));
  EXPECT_EQ(nullptr, db);
  // EXPECT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  // проверяем открытие БД с совместимым интервалом версий приложения
  ASSERT_EQ(FPTA_OK, fpta_db_open_existing(&legacy, testdb_name, fpta_weak,
                                           fpta_regime_default, true, &db));
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  static const fpta_appcontent_info modern{2, 3, "proba"};
  ASSERT_EQ(FPTA_OK, fpta_db_open_existing(&modern, testdb_name, fpta_weak,
                                           fpta_regime_default, true, &db));
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  static const fpta_appcontent_info common{1, 3, "proba"};
  ASSERT_EQ(FPTA_OK, fpta_db_open_existing(&common, testdb_name, fpta_weak,
                                           fpta_regime_default, true, &db));
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  // проверяем открытие БД с НЕсовместимым интервалом версий приложения
  static const fpta_appcontent_info alien{0, 9, "alien"};
  ASSERT_EQ(FPTA_APP_MISMATCH,
            fpta_db_open_existing(&alien, testdb_name, fpta_weak,
                                  fpta_regime_default, true, &db));
  static const fpta_appcontent_info alien_nullptr{0, 9, nullptr};
  ASSERT_EQ(FPTA_APP_MISMATCH,
            fpta_db_open_existing(&alien_nullptr, testdb_name, fpta_weak,
                                  fpta_regime_default, true, &db));
  static const fpta_appcontent_info very_old{0, 0, "proba"};
  ASSERT_EQ(FPTA_APP_MISMATCH,
            fpta_db_open_existing(&very_old, testdb_name, fpta_weak,
                                  fpta_regime_default, true, &db));
  static const fpta_appcontent_info extra_new{4, 5, "proba"};
  ASSERT_EQ(FPTA_APP_MISMATCH,
            fpta_db_open_existing(&extra_new, testdb_name, fpta_weak,
                                  fpta_regime_default, true, &db));

  // пробуем открыть с неверным интервалм версий
  static const fpta_appcontent_info invalid{4, 1, "invalid"};
  ASSERT_EQ(FPTA_EINVAL,
            fpta_db_open_existing(&invalid, "invalid-non-exists", fpta_weak,
                                  fpta_regime_default, true, &db));

  // апгрейдим базу создавая еще одну таблицу
  ASSERT_EQ(FPTA_OK, fpta_db_open_existing(&modern, testdb_name, fpta_weak,
                                           fpta_regime_default, true, &db));
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table_2", &def));
  EXPECT_EQ(FPTA_OK, fpta_transaction_commit(txn));
  txn = nullptr;
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  // теперь версия внутри БД [1..3]
  // и она по-режнему должна открываться старым приложением
  ASSERT_EQ(FPTA_OK, fpta_db_open_existing(&legacy, testdb_name, fpta_weak,
                                           fpta_regime_default, true, &db));
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_EQ(FPTA_OK, fpta_db_open_existing(&common, testdb_name, fpta_weak,
                                           fpta_regime_default, true, &db));
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  // еще раз апгрейдим базу удаляя первую таблицу
  static const fpta_appcontent_info upgraded{3, 4, "proba"};
  ASSERT_EQ(FPTA_OK, fpta_db_open_existing(&upgraded, testdb_name, fpta_weak,
                                           fpta_regime_default, true, &db));
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "table_1"));
  EXPECT_EQ(FPTA_OK, fpta_transaction_commit(txn));
  txn = nullptr;
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  // теперь версия внутри БД [3..4]
  // и она НЕ должна открываться старым приложением
  ASSERT_EQ(FPTA_APP_MISMATCH,
            fpta_db_open_existing(&legacy, testdb_name, fpta_weak,
                                  fpta_regime_default, true, &db));
  ASSERT_EQ(FPTA_OK, fpta_db_open_existing(&common, testdb_name, fpta_weak,
                                           fpta_regime_default, true, &db));
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  // удаляем вторую таблицу
  ASSERT_EQ(FPTA_OK, fpta_db_open_existing(&modern, testdb_name, fpta_weak,
                                           fpta_regime_default, true, &db));
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "table_2"));
  EXPECT_EQ(FPTA_OK, fpta_transaction_commit(txn));
  txn = nullptr;
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  // теперь БД пустая (без схемы) и должна открываться любым приложением
  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_lazy,
                                  fpta_frendly4compaction, 1, false, &db));
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_EQ(FPTA_OK, fpta_db_open_existing(&alien, testdb_name, fpta_weak,
                                           fpta_regime_default, true, &db));
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_EQ(FPTA_OK,
            fpta_db_open_existing(&alien_nullptr, testdb_name, fpta_weak,
                                  fpta_regime_default, true, &db));
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_EQ(FPTA_OK, fpta_db_open_existing(&very_old, testdb_name, fpta_weak,
                                           fpta_regime_default, true, &db));
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));
  ASSERT_EQ(FPTA_OK, fpta_db_open_existing(&extra_new, testdb_name, fpta_weak,
                                           fpta_regime_default, true, &db));
  ASSERT_EQ(FPTA_SUCCESS, fpta_db_close(db));

  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  mdbx_setup_debug(MDBX_LOG_WARN,
                   MDBX_DBG_ASSERT | MDBX_DBG_AUDIT | MDBX_DBG_DUMP |
                       MDBX_DBG_LEGACY_MULTIOPEN | MDBX_DBG_JITTER,
                   nullptr);
  return RUN_ALL_TESTS();
}
