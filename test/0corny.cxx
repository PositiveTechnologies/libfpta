﻿/*
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
#include "keygen.hpp"

TEST(Corny, NameValidate) {
  /* Проверка корректности валидатора имен.
   *
   * Сценарий:
   *  - несколько кейсов для допустимых имен.
   *  - несколько кейсов для НЕ допустимых имен.
   */
  EXPECT_TRUE(fpta_validate_name("valid"));
  EXPECT_TRUE(fpta_validate_name("valid_valid"));
  EXPECT_TRUE(fpta_validate_name("valid_42"));
  EXPECT_EQ(0, t1ha_selfcheck__t1ha2());

  EXPECT_FALSE(fpta_validate_name(""));
  EXPECT_FALSE(fpta_validate_name(nullptr));
  EXPECT_FALSE(fpta_validate_name("a_very_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_long_"
                                  "long_long_long_long_long_long_name"));

  EXPECT_FALSE(fpta_validate_name("not valid"));
  EXPECT_FALSE(fpta_validate_name("1nvalid"));
#if FPTA_ALLOW_DOT4NAMES
  EXPECT_TRUE(fpta_validate_name("val.d"));
  EXPECT_TRUE(fpta_validate_name(".val.d"));
#else
  EXPECT_FALSE(fpta_validate_name("inval.d"));
  EXPECT_FALSE(fpta_validate_name(".nval.d"));
#endif
  EXPECT_FALSE(fpta_validate_name("inval$d"));
  EXPECT_TRUE(fpta_validate_name("_1nvalid"));
  EXPECT_FALSE(fpta_validate_name("invalid#"));
  EXPECT_FALSE(fpta_validate_name("invalid/"));
#if /* avoid MSVC toubles */ !defined(_MSC_VER) || defined(NDEBUG)
  EXPECT_FALSE(fpta_validate_name("invalid_ещераз"));
#endif
}

TEST(Corny, DeNIL_NaNs) {
  /* Проверка NAN-значений для designaned NILs. */

  // тест базовых констант
  EXPECT_TRUE(std::isnan(fpta_fp32_denil.__f));
  EXPECT_TRUE(std::isnan(fpta_fp32_qsnan.__f));
  EXPECT_TRUE(std::isnan(fpta_fp64_denil.__d));
  EXPECT_TRUE(std::isnan(fpta_fp32x64_denil.__d));
  EXPECT_TRUE(std::isnan(fpta_fp32x64_qsnan.__d));
  EXPECT_FALSE(binary_eq(fpta_fp32_denil, fpta_fp32_qsnan));
  EXPECT_FALSE(binary_eq(fpta_fp64_denil, fpta_fp32x64_qsnan));
  EXPECT_FALSE(binary_eq(fpta_fp64_denil, fpta_fp32x64_denil));
  EXPECT_EQ(FPTA_DENIL_FP32_BIN, fpta_fp32_denil.__i);
  EXPECT_EQ(FPTA_QSNAN_FP32_BIN, fpta_fp32_qsnan.__i);
  EXPECT_EQ(FPTA_DENIL_FP32_BIN, fpta_fp32_qsnan.__i + 1);
  EXPECT_EQ(FPTA_DENIL_FP64_BIN, fpta_fp64_denil.__i);
  EXPECT_EQ(FPTA_DENIL_FP32x64_BIN, fpta_fp32x64_denil.__i);
  EXPECT_EQ(FPTA_QSNAN_FP32x64_BIN, fpta_fp32x64_qsnan.__i);

  fpta_fp32_t fp32;
  fpta_fp64_t fp64;

#ifdef FPTA_DENIL_FP32 /* LY: undefined for buggy compilers like MSVC */
  // проверка FPTA_DENIL_FP32
  fp32.__f = FPTA_DENIL_FP32;
  EXPECT_TRUE(binary_eq(fpta_fp32_denil, fp32));
  EXPECT_EQ(fpta_fp32_denil.__i, fp32.__i);
#endif /* FPTA_DENIL_FP32 */

#ifdef FPTA_DENIL_FP32_MAS /* LY: undefined for buggy compilers like MSVC */
  // проверка FPTA_DENIL_FP32_MAS
  fp32.__f = -std::nanf(FPTA_DENIL_FP32_MAS);
  EXPECT_TRUE(binary_eq(fpta_fp32_denil, fp32));
  EXPECT_EQ(fpta_fp32_denil.__i, fp32.__i);
#endif /* FPTA_DENIL_FP32_MAS */

#ifdef FPTA_DENIL_FP64 /* LY: undefined for buggy compilers like MSVC */
  // проверка FPTA_DENIL_FP64
  fp64.__d = FPTA_DENIL_FP64;
  EXPECT_TRUE(binary_eq(fpta_fp64_denil, fp64));
  EXPECT_EQ(fpta_fp64_denil.__i, fp64.__i);
#endif /* FPTA_DENIL_FP64 */

#ifdef FPTA_DENIL_FP64_MAS /* LY: undefined for buggy compilers like MSVC */
  // проверка FPTA_DENIL_FP64_MAS
  fp64.__d = -std::nan(FPTA_DENIL_FP64_MAS);
  EXPECT_TRUE(binary_eq(fpta_fp64_denil, fp64));
  EXPECT_EQ(fpta_fp64_denil.__i, fp64.__i);
#endif /* FPTA_DENIL_FP64_MAS */

#ifdef FPTA_DENIL_FP32x64_MAS /* LY: undefined for mad compilers like MSVC */
  // проверка FPTA_DENIL_FP32x64_MAS
  fp64.__d = -std::nan(FPTA_DENIL_FP32x64_MAS);
  EXPECT_TRUE(binary_eq(fpta_fp32x64_denil, fp64));
#endif /* FPTA_DENIL_FP32x64_MAS */

#ifdef FPTA_QSNAN_FP32x64_MAS /* LY: undefined for mad compilers like MSVC */
  // проверка FPTA_QSNAN_FP32x64_MAS
  fp64.__d = -std::nan(FPTA_QSNAN_FP32x64_MAS);
  EXPECT_TRUE(binary_eq(fpta_fp32x64_qsnan, fp64));
#endif /* FPTA_QSNAN_FP32x64_MAS */

  // преобразование DENIL с усечением.
  fp32.__f = (float)fpta_fp64_denil.__d;
  EXPECT_EQ(fpta_fp32_denil.__i, fp32.__i);
  EXPECT_TRUE(binary_eq(fpta_fp32_denil, fp32));
  fp32.__f = (float)FPTA_DENIL_FP64;
  EXPECT_EQ(fpta_fp32_denil.__i, fp32.__i);
  EXPECT_TRUE(binary_eq(fpta_fp32_denil, fp32));

  // преобразование DENIL с расширением.
  fp64.__d = fpta_fp32_denil.__f;
  EXPECT_EQ(fpta_fp32x64_denil.__i, fp64.__i);
  EXPECT_TRUE(binary_eq(fpta_fp32x64_denil, fp64));
  fp64.__d = FPTA_DENIL_FP32;
  EXPECT_EQ(fpta_fp32x64_denil.__i, fp64.__i);
  EXPECT_TRUE(binary_eq(fpta_fp32x64_denil, fp64));
  // а так не должно совпадать, ибо мантисса просто шире.
  EXPECT_NE(fpta_fp64_denil.__i, fp64.__i);
  EXPECT_FALSE(binary_eq(fpta_fp64_denil, fp64));

  // преобразование QSNAN с усечением.
  fp32.__f = (float)fpta_fp32x64_qsnan.__d;
  EXPECT_NE(fpta_fp32_denil.__i, fp32.__i);
  EXPECT_EQ(fpta_fp32_denil.__i, fp32.__i + 1);
  EXPECT_FALSE(binary_eq(fpta_fp32_denil, fp32));
  EXPECT_EQ(fpta_fp32_qsnan.__i, fp32.__i);
  EXPECT_TRUE(binary_eq(fpta_fp32_qsnan, fp32));

  // преобразование QSNAN с расширением.
  fp64.__d = fpta_fp32_qsnan.__f;
  EXPECT_EQ(fpta_fp32x64_qsnan.__i, fp64.__i);
  EXPECT_TRUE(binary_eq(fpta_fp32x64_qsnan, fp64));
}

TEST(Corny, KeyGenerator) {
  /* Проверка корректности генераторов ключей/значений, которые используются
   * в последующих тестах, в частности для проверки индексов и курсоров.
   *
   * Сценарий:
   *  1. Имеется три подвида генераторов, основная задача каждого в
   *     генерации N значений, которые покрывают весь диапазон
   *     соответствующего типа данных и при этом включают специфические
   *     точки. Например, бесконечность для double/float или нулевая длина
   *     для opacity.
   *
   *  2. Проверяем что генератор для каждого типа адекватен, а именно:
   *     - выдает ряд из запрошенного кол-во значений.
   *     - генерируемый ряд включает крайние и специфические точки.
   *     - генерируемые значение следуют по возрастанию.
   *     - перечисленное верно как для четного, так и для нечетного
   *       кол-ва точек.
   *
   *  3. Для строковых генераторов также проверяются:
   *     - генерация бинарных и текстовых значений.
   *     - генерация ключей как фиксированной, так и переменной длины.
   */
  scalar_range_stepper<float>::test(42);
  scalar_range_stepper<float>::test(42 * 5);
  scalar_range_stepper<float>::test(43);
  scalar_range_stepper<float>::test(43 * 4);
  scalar_range_stepper<double>::test(42);
  scalar_range_stepper<double>::test(42 * 5);
  scalar_range_stepper<double>::test(43);
  scalar_range_stepper<double>::test(43 * 4);
  scalar_range_stepper<uint16_t>::test(42);
  scalar_range_stepper<uint16_t>::test(42 * 5);
  scalar_range_stepper<uint16_t>::test(43);
  scalar_range_stepper<uint16_t>::test(43 * 4);
  scalar_range_stepper<uint32_t>::test(42);
  scalar_range_stepper<uint32_t>::test(42 * 5);
  scalar_range_stepper<uint32_t>::test(43);
  scalar_range_stepper<uint32_t>::test(43 * 4);
  scalar_range_stepper<int32_t>::test(42);
  scalar_range_stepper<int32_t>::test(42 * 5);
  scalar_range_stepper<int32_t>::test(43);
  scalar_range_stepper<int32_t>::test(43 * 4);
  scalar_range_stepper<int64_t>::test(42);
  scalar_range_stepper<int64_t>::test(42 * 5);
  scalar_range_stepper<int64_t>::test(43);
  scalar_range_stepper<int64_t>::test(43 * 4);

  string_keygen_test<false>(1, 3);
  string_keygen_test<true>(1, 3);
  string_keygen_test<false>(1, fpta_max_keylen);
  string_keygen_test<true>(1, fpta_max_keylen);
  string_keygen_test<false>(8, 8);
  string_keygen_test<true>(8, 8);

  fixbin_stepper<11>::test(42);
  fixbin_stepper<11>::test(43);
  varbin_stepper<fptu_cstr>::test(41 * 5);
  varbin_stepper<fptu_cstr>::test(421);
  varbin_stepper<fptu_cstr>::test(512);
  varbin_stepper<fptu_opaque>::test(41 * 5);
  varbin_stepper<fptu_opaque>::test(421);
  varbin_stepper<fptu_opaque>::test(512);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
