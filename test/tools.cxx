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
