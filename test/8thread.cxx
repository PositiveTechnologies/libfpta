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
#include <functional> // for std::ref
#include <string>

#if defined(__cpp_lib_jthread) ||                                              \
    (defined(_GLIBCXX_HAS_GTHREADS) || !defined(__GLIBCXX__))

#include <thread>

static const char testdb_name[] = TEST_DB_DIR "ut_thread.fpta";
static const char testdb_name_lck[] =
    TEST_DB_DIR "ut_thread.fpta" MDBX_LOCK_SUFFIX;

static std::string random_string(int len, int seed) {
  static std::string alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
  std::string result;
  srand((unsigned)seed);
  for (int i = 0; i < len; ++i)
    result.push_back(alphabet[rand() % alphabet.length()]);
  return result;
}

//------------------------------------------------------------------------------

static void write_thread_proc(fpta_db *db, const int thread_num,
                              const int reps) {
  SCOPED_TRACE("Thread " + std::to_string(thread_num) + " started");

  for (int i = 0; i < reps; ++i) {
    static volatile bool skipped;
    skipped = skipped || GTEST_IS_EXECUTION_TIMEOUT();
    if (skipped)
      break;

    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
    ASSERT_NE(nullptr, txn);

    fpta_name table, num, uuid, dst_ip, port, date;
    EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "table"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &num, "num"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &uuid, "uuidfield"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &dst_ip, "dst_ip"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &port, "port"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &date, "date"));

    fptu_rw *tuple = fptu_alloc(5, 1000);
    ASSERT_NE(nullptr, tuple);
    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &num));
    uint64_t result = 0;
    EXPECT_EQ(FPTA_OK, fpta_table_sequence(txn, &table, &result, 1));
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(tuple, &num, fpta_value_uint(result)));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &uuid));
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(
                  tuple, &uuid,
                  fpta_value_cstr(
                      random_string(36, thread_num * 32768 + i).c_str())));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &dst_ip));
    EXPECT_EQ(FPTA_OK,
              fpta_upsert_column(tuple, &dst_ip, fpta_value_cstr("127.0.0.1")));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &port));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &port, fpta_value_sint(100)));

    EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &date));
    EXPECT_EQ(FPTA_OK, fpta_upsert_column(
                           tuple, &date, fpta_value_datetime(fptu_now_fine())));

    EXPECT_EQ(FPTA_OK,
              fpta_probe_and_upsert_row(txn, &table, fptu_take(tuple)));

    fptu_clear(tuple);
    free(tuple);

    fpta_name_destroy(&table);
    fpta_name_destroy(&num);
    fpta_name_destroy(&uuid);
    fpta_name_destroy(&dst_ip);
    fpta_name_destroy(&port);
    fpta_name_destroy(&date);

    SCOPED_TRACE("Thread " + std::to_string(thread_num) + " insertion " +
                 std::to_string(i));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
#if defined(_WIN32) || defined(_WIN64)
    SwitchToThread();
#else
    sched_yield();
#endif
  }
  SCOPED_TRACE("Thread " + std::to_string(thread_num) + " finished");
}

TEST(Threaded, SimpleConcurence) {
  // чистим
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  fpta_db_creation_params_t creation_params;
  creation_params.params_size = sizeof(creation_params);
  creation_params.file_mode = 0644;
  creation_params.size_lower = 0;
  creation_params.size_upper = 8 << 20;
  creation_params.pagesize = -1;
  creation_params.growth_step = -1;
  creation_params.shrink_threshold = -1;

  fpta_db *db = nullptr;
  ASSERT_EQ(FPTA_OK,
            fpta_db_create_or_open(nullptr, testdb_name, fpta_weak,
                                   fpta_saferam, true, &db, &creation_params));
  ASSERT_NE(nullptr, db);
  SCOPED_TRACE("Database opened");

  { // create table
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("num", fptu_uint64,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("uuidfield", fptu_cstr,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("dst_ip", fptu_cstr,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("port", fptu_int64,
                                            fpta_noindex_nullable, &def));
    fpta_column_describe("date", fptu_datetime, fpta_noindex_nullable, &def);

    fpta_txn *txn = nullptr;
    ASSERT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_EQ(FPTA_OK, fpta_table_create(txn, "table", &def));
    ASSERT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
    SCOPED_TRACE("Table created");
  }

  ASSERT_EQ(FPTA_OK, fpta_db_close(db));
  db = nullptr;
  SCOPED_TRACE("Database closed");

  ASSERT_EQ(FPTA_OK, fpta_db_open_existing(nullptr, testdb_name, fpta_weak,
                                           fpta_saferam, false, &db));
  ASSERT_NE(nullptr, db);
  SCOPED_TRACE("Database reopened");

  write_thread_proc(db, 42, 50);

#ifdef CI
  const int reps = 250;
#else
  const int reps = 5000;
#endif

  const int threadNum = 8;
  std::vector<std::thread> threads;
  for (int16_t i = 1; i <= threadNum; ++i)
    threads.push_back(std::thread(write_thread_proc, db, i, reps));

  for (auto &it : threads)
    it.join();

  SCOPED_TRACE("All threads are stopped");
  EXPECT_EQ(FPTA_OK, fpta_db_close(db));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

//------------------------------------------------------------------------------

static void read_thread_proc(fpta_db *db,
                             const int SCOPED_TRACE_ONLY thread_num,
                             const int reps) {
  SCOPED_TRACE("Thread " + std::to_string(thread_num) + " started");

  fpta_name table, ip, port;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "MyTable"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &ip, "Ip"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &port, "port"));

  fpta_filter filter, filter_a, filter_b;
  memset(&filter, 0, sizeof(fpta_filter));
  memset(&filter_a, 0, sizeof(fpta_filter));
  memset(&filter_b, 0, sizeof(fpta_filter));

  filter.type = fpta_node_and;
  filter.node_and.a = &filter_a;
  filter.node_and.b = &filter_b;

  filter_a.type = fpta_node_ne;
  filter_a.node_cmp.left_id = &ip;

  filter_b.type = fpta_node_ne;
  filter_b.node_cmp.left_id = &port;

  for (int i = 0; i < reps; ++i) {
    static volatile bool skipped;
    skipped = skipped || GTEST_IS_EXECUTION_TIMEOUT();
    if (skipped)
      break;

    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
    ASSERT_NE(nullptr, txn);

    EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));
    fpta_name column;
    EXPECT_EQ(FPTA_OK, fpta_table_column_get(&table, 0, &column));

    std::string str = random_string(15, i);
    filter_a.node_cmp.right_value = fpta_value_cstr(str.c_str());
    filter_b.node_cmp.right_value = fpta_value_sint(1000 + (rand() % 1000));

    fpta_cursor *cursor = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_cursor_open(txn, &column, fpta_value_begin(),
                                        fpta_value_end(), &filter,
                                        fpta_unsorted_dont_fetch, &cursor));
    ASSERT_NE(nullptr, cursor);
    EXPECT_EQ(FPTA_OK, fpta_cursor_move(cursor, fpta_first));
    EXPECT_EQ(FPTA_OK, fpta_cursor_eof(cursor));
    EXPECT_EQ(FPTA_OK, fpta_cursor_close(cursor));

    fpta_name_destroy(&column);
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  }

  fpta_name_destroy(&table);
  fpta_name_destroy(&ip);
  fpta_name_destroy(&port);

  SCOPED_TRACE("Thread " + std::to_string(thread_num) + " finished");
}

TEST(Threaded, SimpleSelect) {
  // чистим
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  fpta_db *db = nullptr;
  ASSERT_EQ(FPTA_OK,
            test_db_open(testdb_name, fpta_weak, fpta_saferam, 1, true, &db));
  ASSERT_NE(nullptr, db);
  SCOPED_TRACE("Database opened");

  { // create table
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("Ip", fptu_cstr,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("port", fptu_int64,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe("word", fptu_cstr,
                                            fpta_noindex_nullable, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "_last_changed", fptu_datetime,
                           fpta_secondary_withdups_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "_id", fptu_uint64,
                           fpta_secondary_unique_ordered_obverse, &def));

    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);

    EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "MyTable", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
    SCOPED_TRACE("Table created");
  }
  ASSERT_EQ(FPTA_OK, fpta_db_close(db));
  db = nullptr;
  SCOPED_TRACE("Database closed");

  ASSERT_EQ(FPTA_OK,
            test_db_open(testdb_name, fpta_weak, fpta_saferam, 1, false, &db));
  ASSERT_NE(nullptr, db);
  SCOPED_TRACE("Database reopened");

  fpta_txn *txn = nullptr;
  fpta_transaction_begin(db, fpta_write, &txn);
  ASSERT_NE(nullptr, txn);

  fpta_name table, ip, port, word, date, id;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "MyTable"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &ip, "Ip"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &port, "port"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &word, "word"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &date, "_last_changed"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &id, "_id"));

  fptu_rw *tuple = fptu_alloc(5, 1000);
  ASSERT_NE(nullptr, tuple);
  EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));

  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &id));
  uint64_t result = 0;
  EXPECT_EQ(FPTA_OK, fpta_table_sequence(txn, &table, &result, 1));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &id, fpta_value_uint(result)));

  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &ip));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(tuple, &ip, fpta_value_cstr("1.1.1.1")));

  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &word));
  EXPECT_EQ(FPTA_OK,
            fpta_upsert_column(tuple, &word, fpta_value_cstr("hello")));

  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &port));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &port, fpta_value_sint(111)));

  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &date));
  EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &date,
                                        fpta_value_datetime(fptu_now_fine())));

  EXPECT_EQ(FPTA_OK, fpta_probe_and_upsert_row(txn, &table, fptu_take(tuple)));

  EXPECT_EQ(FPTU_OK, fptu_clear(tuple));
  free(tuple);

  fpta_name_destroy(&table);
  fpta_name_destroy(&id);
  fpta_name_destroy(&word);
  fpta_name_destroy(&ip);
  fpta_name_destroy(&port);
  fpta_name_destroy(&date);

  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  SCOPED_TRACE("Record written");

#ifdef CI
  const int reps = 1000;
#else
  const int reps = 10000;
#endif

  const int threadNum = 8;
  std::vector<std::thread> threads;
  for (int i = 0; i < threadNum; ++i)
    threads.push_back(std::thread(read_thread_proc, db, i, reps));

  for (auto &thread : threads)
    thread.join();

  SCOPED_TRACE("All threads are stopped");
  EXPECT_EQ(FPTA_OK, fpta_db_close(db));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

//------------------------------------------------------------------------------

static int visitor(const fptu_ro *row, void *context, void *arg) {
  fpta_value val;
  fpta_name *name = (fpta_name *)arg;
  int rc = fpta_get_column(*row, name, &val);
  if (rc != FPTA_OK)
    return rc;

  if (val.type != fpta_signed_int)
    return (int)FPTA_DEADBEEF;
  int64_t *max_val = (int64_t *)context;
  if (val.sint > *max_val)
    *max_val = val.sint;

  return FPTA_OK;
}

static void visitor_thread_proc(fpta_db *db,
                                const int SCOPED_TRACE_ONLY thread_num,
                                const int reps, int *counter) {
  SCOPED_TRACE("Thread " + std::to_string(thread_num) + " started");
  *counter = 0;

  fpta_name table, key, host, date, id;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "Counting"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &key, "key"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &host, "host"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &date, "_last_changed"));
  EXPECT_EQ(FPTA_OK, fpta_column_init(&table, &id, "_id"));

  fpta_filter filter;
  memset(&filter, 0, sizeof(fpta_filter));
  filter.type = fpta_node_gt;
  filter.node_cmp.left_id = &key;
  filter.node_cmp.right_value = fpta_value_sint(0);

  for (int i = 0; i < reps; ++i) {
    static volatile bool skipped;
    skipped = skipped || GTEST_IS_EXECUTION_TIMEOUT();
    if (skipped)
      break;

    // start read-transaction and get max_value
    int64_t max_value = 0;
    {
      fpta_txn *txn = nullptr;
      EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
      ASSERT_NE(nullptr, txn);

      EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &key));

      fpta_name column;
      EXPECT_EQ(FPTA_OK, fpta_table_column_get(&table, 0, &column));
      EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &column));
      size_t count = 0;

      int err = fpta_apply_visitor(txn, &column, fpta_value_begin(),
                                   fpta_value_end(), &filter, fpta_unsorted, 0,
                                   10000, nullptr, nullptr, &count, &visitor,
                                   &max_value /* context */, &key /* arg */);
      if (err != FPTA_OK) {
        EXPECT_EQ(FPTA_NODATA, err);
      }

      fpta_name_destroy(&column);
      EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    }

    // start write-transaction and insert max_value + 1
    {
      fpta_txn *txn = nullptr;
      EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
      ASSERT_NE(nullptr, txn);

      EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &table));

      fptu_rw *tuple = fptu_alloc(4, 1000);
      ASSERT_NE(nullptr, tuple);

      EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &id));
      uint64_t result = 0;
      EXPECT_EQ(FPTA_OK, fpta_table_sequence(txn, &table, &result, 1));
      EXPECT_EQ(FPTA_OK,
                fpta_upsert_column(tuple, &id, fpta_value_uint(result)));

      EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &host));
      std::string str = random_string(15, i);
      EXPECT_EQ(FPTA_OK,
                fpta_upsert_column(tuple, &host, fpta_value_cstr(str.c_str())));

      EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &date));
      EXPECT_EQ(FPTA_OK,
                fpta_upsert_column(tuple, &date,
                                   fpta_value_datetime(fptu_now_fine())));

      EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, &key));
      EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &key,
                                            fpta_value_sint(max_value + 1)));

      int err = fpta_probe_and_upsert_row(txn, &table, fptu_take(tuple));
      fptu_clear(tuple);
      free(tuple);

      if (err != FPTA_OK) {
        EXPECT_EQ(FPTA_DB_FULL, err);
        // отменяем если была ошибка
        err = fpta_transaction_end(txn, true);
        if (err != FPTA_OK) {
          ASSERT_EQ(FPTA_TXN_CANCELLED, err);
          break;
        }
      } else {
        // коммитим и ожидаем ошибку переполнения здесь
        err = fpta_transaction_end(txn, false);
        if (err != FPTA_OK) {
          ASSERT_EQ(FPTA_DB_FULL, err);
          break;
        }
      }
    }
  }

  fpta_name_destroy(&table);
  fpta_name_destroy(&id);
  fpta_name_destroy(&host);
  fpta_name_destroy(&key);
  fpta_name_destroy(&date);
  SCOPED_TRACE("Thread " + std::to_string(thread_num) + " finished");
}

TEST(Threaded, SimpleVisitor) {
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  fpta_db *db = nullptr;
  ASSERT_EQ(FPTA_OK,
            test_db_open(testdb_name, fpta_weak, fpta_saferam, 1, true, &db));
  ASSERT_NE(nullptr, db);
  SCOPED_TRACE("Database opened");

  { // create table
    fpta_column_set def;
    fpta_column_set_init(&def);
    EXPECT_EQ(FPTA_OK,
              fpta_column_describe("key", fptu_int64,
                                   fpta_primary_unique_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "host", fptu_cstr,
                           fpta_secondary_withdups_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "_last_changed", fptu_datetime,
                           fpta_secondary_withdups_ordered_obverse, &def));
    EXPECT_EQ(FPTA_OK, fpta_column_describe(
                           "_id", fptu_uint64,
                           fpta_secondary_unique_ordered_obverse, &def));

    fpta_txn *txn = nullptr;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);
    EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "Counting", &def));
    EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
    EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
    SCOPED_TRACE("Table created");
  }
  ASSERT_EQ(FPTA_OK, fpta_db_close(db));
  db = nullptr;
  SCOPED_TRACE("Database closed");

  ASSERT_EQ(FPTA_OK,
            test_db_open(testdb_name, fpta_weak, fpta_saferam, 1, false, &db));
  ASSERT_NE(nullptr, db);
  SCOPED_TRACE("Database reopened");

#ifdef CI
  const int reps = 1000;
#else
  const int reps = 10000;
#endif

  const int threadNum = 8;
  int counters[threadNum] = {0};
  std::vector<std::thread> threads;
  for (int i = 0; i < threadNum; ++i)
    threads.push_back(
        std::thread(visitor_thread_proc, db, i, reps, &counters[i]));

  int SCOPED_TRACE_ONLY i = 0;
  for (auto &thread : threads) {
    SCOPED_TRACE("Thread " + std::to_string(i) +
                 ": counter = " + std::to_string(counters[i]));
    thread.join();
  }

  SCOPED_TRACE("All threads are stopped");
  EXPECT_EQ(FPTA_OK, fpta_db_close(db));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

//------------------------------------------------------------------------------

static void info_thread_proc(fpta_db *db) {
  // начинаем транзакцию fpta_read
  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
  ASSERT_NE(nullptr, txn);

  fpta_name table;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "some_table"));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, nullptr));

  // делаем вызов fpta_table_info
  fpta_table_stat stat;
  memset(&stat, 42, sizeof(stat));
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table, nullptr, &stat));

  // завершаем транзакцию
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));

  fpta_name_destroy(&table);

  SCOPED_TRACE("Thread finished");
}

TEST(Threaded, ParallelOpen) {
  if (REMOVE_FILE(testdb_name) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }
  if (REMOVE_FILE(testdb_name_lck) != 0) {
    ASSERT_EQ(ENOENT, errno);
  }

  fpta_db *db = nullptr;
  EXPECT_EQ(FPTA_OK,
            test_db_open(testdb_name, fpta_weak, fpta_saferam, 1, true, &db));
  ASSERT_NE(db, (fpta_db *)nullptr);

  fpta_column_set def;
  fpta_column_set_init(&def);

  EXPECT_EQ(FPTA_OK, fpta_column_describe(
                         "some_field", fptu_uint16,
                         fpta_primary_unique_ordered_obverse_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  fpta_txn *txn = (fpta_txn *)&txn;
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
  ASSERT_NE(nullptr, txn);
  EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "some_table", &def));
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  txn = nullptr;

  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));
  EXPECT_NE(FPTA_OK, fpta_column_set_validate(&def));

  // переоткрываем базу
  EXPECT_EQ(FPTA_OK, fpta_db_close(db));
  db = nullptr;
  EXPECT_EQ(FPTA_OK,
            test_db_open(testdb_name, fpta_weak, fpta_saferam, 1, false, &db));
  ASSERT_NE(db, (fpta_db *)nullptr);

  // начинаем транзакцию fpta_write
  EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
  ASSERT_NE(nullptr, txn);

  // запускаем поток и дожидаемся его завершения
  std::thread thread(info_thread_proc, db);
  thread.join();

  fpta_name table;
  EXPECT_EQ(FPTA_OK, fpta_table_init(&table, "some_table"));
  EXPECT_EQ(FPTA_OK, fpta_name_refresh_couple(txn, &table, nullptr));

  fpta_table_stat stat;
  memset(&stat, 42, sizeof(stat));
  // задействуем DBI-хендл уже открытый в другом потоке
  EXPECT_EQ(FPTA_OK, fpta_table_info(txn, &table, nullptr, &stat));

  // завершаем транзакцию
  EXPECT_EQ(FPTA_OK, fpta_transaction_end(txn, false));
  fpta_name_destroy(&table);
  EXPECT_EQ(FPTA_OK, fpta_db_close(db));

  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

//------------------------------------------------------------------------------

static void commander_thread(fpta_db *db, const volatile bool &done_flag) {
  SCOPED_TRACE("commander-thread started");

  // описываем простейшую таблицу с двумя колонками
  fpta_column_set def;
  fpta_column_set_init(&def);
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("pk", fptu_uint64,
                                 fpta_primary_unique_ordered_obverse, &def));
  EXPECT_EQ(FPTA_OK,
            fpta_column_describe("x", fptu_cstr, fpta_noindex_nullable, &def));
  EXPECT_EQ(FPTA_OK, fpta_column_set_validate(&def));

  unsigned prev_state = 0;
  int i = 0;
  while (!done_flag) {
    fpta_txn *txn;
    EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_schema, &txn));
    ASSERT_NE(nullptr, txn);
    std::this_thread::yield();

    unsigned new_state = (((i++ + 58511) * 977) >> 3) & 3;
    if ((prev_state ^ new_state) & 1) {
      // создаем или удаляем первую таблицу
      if (new_state & 1) {
        EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table1", &def));
      } else {
        EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "table1"));
      }
    }
    if ((prev_state ^ new_state) & 2) {
      // создаем или удаляем вторую таблицу
      if (new_state & 2) {
        EXPECT_EQ(FPTA_OK, fpta_table_create(txn, "table2", &def));
      } else {
        EXPECT_EQ(FPTA_OK, fpta_table_drop(txn, "table2"));
      }
    }

    ASSERT_EQ(FPTA_OK, fpta_transaction_commit(txn));
    txn = nullptr;
    prev_state = new_state;
    std::this_thread::yield();
  }

  // разрушаем описание таблицы
  EXPECT_EQ(FPTA_OK, fpta_column_set_destroy(&def));

  SCOPED_TRACE("commander-thread finished");
}

static void executor_thread(fpta_db *db, const char *const read,
                            const char *const write, volatile bool &done_flag) {
  struct done_guard {
    volatile bool &done;
    done_guard(volatile bool &done_flag) : done(done_flag) {}
    ~done_guard() { done = true; }
  };

  SCOPED_TRACE(
      fptu::format("executor-thread read(%s) write(%s) started", read, write));
  done_guard guard(done_flag);
  {
    fpta_name r_table, w_table, col_pk, col_x;
    int err;
    fptu_rw *tuple = fptu_alloc(2, 256);
    ASSERT_NE(nullptr, tuple);

    // инициализируем идентификаторы таблицы и её колонок
    EXPECT_EQ(FPTA_OK, fpta_table_init(&r_table, read));
    EXPECT_EQ(FPTA_OK, fpta_table_init(&w_table, write));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&w_table, &col_pk, "pk"));
    EXPECT_EQ(FPTA_OK, fpta_column_init(&w_table, &col_x, "x"));

    for (int counter = 0; counter < 3; ++counter) {
      for (int achieved = 0; achieved != 31;) {
        fpta_txn *txn;
        EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_write, &txn));
        ASSERT_NE(nullptr, txn);

        err = fpta_name_refresh_couple(txn, &w_table, &col_pk);
        if (err == FPTA_SUCCESS) {
          EXPECT_EQ(FPTA_OK, fpta_name_refresh(txn, &col_x));
          fptu_clear(tuple);
          uint64_t seq = 0;
          err = fpta_table_sequence(txn, &w_table, &seq, 1);
          if (err != FPTA_TARDY_DBI && err != FPTA_BAD_DBI) {
            EXPECT_EQ(FPTA_OK, err);
            EXPECT_EQ(FPTA_OK, fpta_upsert_column(tuple, &col_pk,
                                                  fpta_value_uint(seq % 100)));
            EXPECT_EQ(FPTA_OK,
                      fpta_upsert_column(tuple, &col_x, fpta_value_cstr("x")));
            EXPECT_EQ(FPTA_OK,
                      fpta_upsert_row(txn, &w_table, fptu_take(tuple)));
            achieved |= 1 << 0;
          }
        } else {
          ASSERT_EQ(FPTA_NOTFOUND, err);
          achieved |= 1 << 1;
        }
        ASSERT_EQ(FPTA_OK, fpta_transaction_commit(txn));
        txn = nullptr;

        EXPECT_EQ(FPTA_OK, fpta_transaction_begin(db, fpta_read, &txn));
        ASSERT_NE(nullptr, txn);

        if (guard.done)
          achieved |= 1 << 2;
        size_t lag = 42;
        do {
          std::this_thread::yield();
          size_t row_count;
          fpta_table_stat stat;
          err = fpta_table_info(txn, &r_table, &row_count, &stat);
          switch (err) {
          case FPTA_SCHEMA_CHANGED:
            achieved |= 1 << 2;
            break;
          case FPTA_NOTFOUND:
            achieved |= 1 << 3;
            break;
          default:
            ASSERT_EQ(FPTA_SUCCESS, err);
            achieved |= 1 << 4;
            ASSERT_EQ(FPTA_OK,
                      fpta_transaction_lag_ex(txn, &lag, nullptr, nullptr));
            break;
          }
        } while (err == FPTA_SUCCESS && lag < 42 && !guard.done);

        ASSERT_EQ(FPTA_OK, fpta_transaction_commit(txn));
        txn = nullptr;
      }
    }

    // разрушаем привязанные идентификаторы
    fpta_name_destroy(&r_table);
    fpta_name_destroy(&w_table);
    fpta_name_destroy(&col_pk);
    fpta_name_destroy(&col_x);
    free(tuple);
  }
  SCOPED_TRACE(
      fptu::format("executor-thread read(%s) write(%s) finished", read, write));
}

TEST(Threaded, AsyncSchemaChange) {
  /*
   * 1. В "коммандере" создаем пустую БД, а в "корреляторе" и "обогатителе"
   *    открываем её.
   *
   * 2. На стороне "коррелятора" и "обогатителя" запускаем по несколько
   *    потоков, которые пытаются читать и вставлять данные в разные таблицы:
   *      - ошибки отсутвия таблиц и FPTA_SCHEMA_CHANGED считаются
   *        допустимыми.
   *      - при отсутствии таблицы пропускаются соответствующие действия.
   *      - FPTA_SCHEMA_CHANGED транзакция чтения перезапускается.
   *
   * 3. В "командере" несколько раз создаем, изменяем и удаляем используемые
   *    таблицы. После каждого изменения ждем пока потоки в "корреляторе"
   *    и "командере" не сделают несколько итераций.
   *
   * 4. Завершаем операции и освобождаем ресурсы.
   */

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

  fpta_db *db_commander = nullptr;
  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  20, true, &db_commander));
  ASSERT_NE(nullptr, db_commander);

  fpta_db *db_enricher = nullptr;
  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  20, false, &db_enricher));
  ASSERT_NE(nullptr, db_enricher);

  fpta_db *db_correlator = nullptr;
  ASSERT_EQ(FPTA_OK, test_db_open(testdb_name, fpta_weak, fpta_regime_default,
                                  20, false, &db_correlator));
  ASSERT_NE(nullptr, db_correlator);

  volatile bool commander_done = false;
  auto commander =
      std::thread(commander_thread, db_commander, std::cref(commander_done));

  volatile bool enricher_done = false;
  auto enricher1 = std::thread(executor_thread, db_enricher, "table1", "table2",
                               std::ref(enricher_done));
  auto enricher2 = std::thread(executor_thread, db_enricher, "table2", "table1",
                               std::ref(enricher_done));

  volatile bool correlator_done = false;
  auto correlator1 = std::thread(executor_thread, db_correlator, "table1",
                                 "table2", std::ref(correlator_done));
  auto correlator2 = std::thread(executor_thread, db_correlator, "table2",
                                 "table1", std::ref(correlator_done));

  enricher1.join();
  correlator1.join();
  enricher2.join();
  correlator2.join();

  commander_done = true;
  commander.join();

  EXPECT_EQ(FPTA_OK, fpta_db_close(db_commander));
  EXPECT_EQ(FPTA_OK, fpta_db_close(db_enricher));
  EXPECT_EQ(FPTA_OK, fpta_db_close(db_correlator));
  ASSERT_TRUE(REMOVE_FILE(testdb_name) == 0);
  ASSERT_TRUE(REMOVE_FILE(testdb_name_lck) == 0);
}

//------------------------------------------------------------------------------
#else
TEST(ReadMe, CXX_STD_Threads_NotAvailadble) {}
#endif /* std::thread */

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  mdbx_setup_debug(MDBX_LOG_WARN,
                   MDBX_DBG_ASSERT | MDBX_DBG_AUDIT | MDBX_DBG_DUMP |
                       MDBX_DBG_LEGACY_MULTIOPEN | MDBX_DBG_JITTER,
                   nullptr);
  return RUN_ALL_TESTS();
}
