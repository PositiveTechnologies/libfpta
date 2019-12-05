/*
 *  Fast Positive Tables (libfpta), aka Позитивные Таблицы.
 *  Copyright 2016-2019 Leonid Yuriev <leo@yuriev.ru>
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

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
