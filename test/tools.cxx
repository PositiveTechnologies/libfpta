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

/* Ограничитель по времени выполнения.
 * Нужен для предотвращения таумаута тестов в CI. Предполагается, что он
 * используется вместе с установкой GTEST_SHUFFLE=1, что в сумме дает
 * выполнение части тестов в случайном порядке, пока не будет превышен лимит
 * заданный через переменную среды окружения GTEST_RUNTIME_LIMIT. */
runtime_limiter ci_runtime_limiter;

//------------------------------------------------------------------------------

#if defined(_WIN32) || defined(_WIN64)

int unlink_crutch(const char *pathname) {
  int retry = 0;
  for (;;) {
    int rc =
#ifdef _MSC_VER
        _unlink(pathname);
#else
        unlink(pathname);
#endif

    if (rc == 0 || errno != EACCES || ++retry > 42)
      return rc;

    /* FIXME: Windows kernel is ugly and mad...
     *
     * Workaround for UnlockFile() bugs, e.g. Windows could return EACCES
     * when such file was unlocked just recently. */
    Sleep(42);
  }
}

fptu_time fptu_now_fine_crutch(void) {
  static fptu_time last;
  fptu_time now;

  do
    now = fptu_now_fine();
  while (last.fixedpoint == now.fixedpoint);

  last.fixedpoint = now.fixedpoint;
  return now;
}

#endif /* windows */

int test_db_open(const char *path, fpta_durability durability,
                 fpta_regime_flags regime_flags, size_t megabytes,
                 bool alterable_schema, fpta_db **pdb) {

  if (megabytes == 0 || durability == fpta_readonly)
    return fpta_db_open_existing(path, durability, regime_flags,
                                 alterable_schema, pdb);
  if (unlikely(pdb == nullptr))
    return FPTA_EINVAL;
  *pdb = nullptr;

  if (megabytes > SIZE_MAX >> 22)
    return FPTA_ETOO_LARGE;

  fpta_db_creation_params_t creation_params;
  creation_params.params_size = sizeof(creation_params);
  creation_params.file_mode = 0640;
  creation_params.size_lower = creation_params.size_upper = megabytes << 20;
  creation_params.pagesize = -1;
  creation_params.growth_step = 0;
  creation_params.shrink_threshold = 0;
  return fpta_db_create_or_open(path, durability, regime_flags,
                                alterable_schema, pdb, &creation_params);
}
