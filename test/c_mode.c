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

/* test conformance for C mode */
#include <fast_positive/tables.h>

#ifdef _MSC_VER
#pragma warning(disable : 4710) /* 'xyz': function not inlined */
#pragma warning(disable : 4711) /* function 'xyz' selected for                 \
                                   automatic inline expansion */
#pragma warning(push, 1)
#endif /* _MSC_VER (warnings) */

#include <math.h>
#include <stdio.h>

#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#define print_value(comment, value)                                            \
  wprintf(L"%-24S = %7ld  // %S\n", #value, (long)value, comment)
#define print_mask(comment, value)                                             \
  wprintf(L"%-24S = 0x%-5lx  // %S\n", #value, (long)value, comment)
#define print(text) wprintf(L"%S", text)
#else
#define print_value(comment, value)                                            \
  printf("%-24s = %7ld  // %s\n", #value, (long)value, comment)
#define print_mask(comment, value)                                             \
  printf("%-24s = 0x%05lx  // %s\n", #value, (long)value, comment)
#define print(text) puts(text)
#endif /* WINDOWS */

#ifdef _MSC_VER
#pragma warning(pop)
#endif

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
  SetConsoleOutputCP(CP_UTF8);
#endif /* WINDOWS */

  print("// основные ограничения и константы:");
  print_value("максимальное кол-во таблиц", fpta_tables_max);
  print_value("максимальное кол-во колонок", fptu_max_cols);
  print_value("максимальное кол-во индексов для одной таблице",
              fpta_max_indexes);
  print_value("максимальное суммарное кол-во таблиц и индексов", fpta_max_dbi);

  print_value("максимальная длина строки/записи в байтах", fpta_max_row_bytes);
  print_value("максимальная длина значения колонки в байтах",
              fpta_max_col_bytes);
  print_value("максимальное кол-во элементов в массиве", fpta_max_array_len);

  print_value("минимальная длина имени", fpta_name_len_min);
  print_value("максимальная длина имени", fpta_name_len_max);
  print_value("максимальная длина ключа (дополняется t1ha при превышении)",
              fpta_max_keylen);

  print("\n// внутренние технические детали:");
  print_value("размер буфера для ключа", fpta_keybuf_len);

  print_value("ширина идентификатора в битах", fpta_id_bits);

  print_value("ширина типа колонки в битах", fpta_column_typeid_bits);
  print_value("сдвиг для получения типа колонки", fpta_column_typeid_shift);
  print_mask("маска для получения типа колонки", fpta_column_typeid_mask);

  print_value("ширина типа индекса в битах", fpta_column_index_bits);
  print_value("сдвиг для получения типа индекса", fpta_column_index_shift);
  print_mask("маска для получения типа индекса", fpta_column_index_mask);

  print_value("ширина хэша имени в битах", fpta_name_hash_bits);
  print_value("сдвиг для получения хэша имени", fpta_name_hash_shift);

  const double fpta_name_clash_probab = pow(2.0, -fpta_name_hash_bits / 2.0);
#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
  wprintf(L"%-24S = %.2g  // %S\n",
#else
  printf("%-24s = %.2g  // %s\n",
#endif /* WINDOWS */
          "fpta_name_clash_prob", fpta_name_clash_probab,
          "вероятность коллизии в именах");

#if HAVE_FPTA_VERSIONINFO
  printf("\n libfpta version %s: %s, %d.%d.%d.%d,\n\tcommit %s, tree %s\n",
         fpta_version.git.describe, fpta_version.git.datetime,
         fpta_version.major, fpta_version.minor, fpta_version.release,
         fpta_version.revision, fpta_version.git.commit, fpta_version.git.tree);
#endif /* HAVE_FPTU_VERSIONINFO */

  printf("\n libfpta build %s: %s, %s,\n\t%s,\n\t%s\n", fpta_build.datetime,
         fpta_build.target, fpta_build.compiler, fpta_build.cmake_options,
         fpta_build.compile_flags);

  print("\n less Windows, no Java, no Problems ;)\n");
  return 0;
}
