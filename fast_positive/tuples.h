/*
 *  Fast Positive Tuples (libfptu), aka Позитивные Кортежи
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

/*
 * libfptu = { Fast Positive Tuples, aka Позитивные Кортежи }
 *
 * The kind of lightweight linearized tuples, which are extremely handy
 * to machining, including cases with shared memory.
 * Please see README.md at https://github.com/erthink/libfptu
 *
 * The Future will (be) Positive. Всё будет хорошо.
 *
 * "Позитивные Кортежи" дают легковесное линейное представление небольших
 * JSON-подобных структур в экстремально удобной для машины форме,
 * в том числе при размещении в разделяемой памяти.
 */

#pragma once
#ifndef FAST_POSITIVE_TUPLES_H
#define FAST_POSITIVE_TUPLES_H

#define FPTU_VERSION_MAJOR 0
#define FPTU_VERSION_MINOR 1

#include "fast_positive/config.h"
#include "fast_positive/defs.h"

#if defined(fptu_EXPORTS)
#define FPTU_API __dll_export
#elif defined(fptu_IMPORTS)
#define FPTU_API __dll_import
#else
#define FPTU_API
#endif /* fptu_EXPORTS */

#ifdef _MSC_VER
#pragma warning(push)
#if _MSC_VER < 1900
#pragma warning(disable : 4350) /* behavior change: 'std::_Wrap_alloc... */
#endif
#pragma warning(disable : 4514) /* 'xyz': unreferenced inline function         \
                                   has been removed */
#pragma warning(disable : 4710) /* 'xyz': function not inlined */
#pragma warning(disable : 4711) /* function 'xyz' selected for                 \
                                   automatic inline expansion */
#pragma warning(disable : 4061) /* enumerator 'abc' in switch of enum 'xyz' is \
                                   not explicitly handled by a case label */
#pragma warning(disable : 4201) /* nonstandard extension used :                \
                                   nameless struct / union */
#pragma warning(disable : 4127) /* conditional expression is constant */

#pragma warning(push, 1)
#ifndef _STL_WARNING_LEVEL
#define _STL_WARNING_LEVEL 3 /* Avoid warnings inside nasty MSVC STL code */
#endif
#pragma warning(disable : 4548) /* expression before comma has no effect;      \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                   semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling  \
                                   mode specified; termination on exception    \
                                   is not guaranteed. Specify /EHsc */
#endif                          /* _MSC_VER (warnings) */

#include <errno.h>  // for error codes
#include <float.h>  // for FLT_EVAL_METHOD and float_t
#include <limits.h> // for INT_MAX, etc
#include <math.h>   // for NaNs
#include <stdio.h>  // for FILE
#include <string.h> // for strlen()
#include <time.h>   // for struct timespec, struct timeval

#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h> // for struct iovec
#elif !defined(HAVE_STRUCT_IOVEC)
struct iovec {
  void *iov_base; /* Starting address */
  size_t iov_len; /* Number of bytes to transfer */
};
#define HAVE_STRUCT_IOVEC
#endif

#ifdef __cplusplus
#include <cmath>     // for std::ldexp
#include <limits>    // for numeric_limits<>
#include <memory>    // for std::uniq_ptr
#include <ostream>   // for std::ostream
#include <stdexcept> // for std::invalid_argument
#include <string>    // for std::string
#if __cplusplus >= 201703L && __has_include(<string_view>)
#include <string_view>
#define HAVE_cxx17_std_string_view 1
#else
#define HAVE_cxx17_std_string_view 0
#endif

extern "C" {
#endif /* __cplusplus */

#ifdef _MSC_VER
#pragma warning(pop)
#endif

//----------------------------------------------------------------------------
/* Опции конфигурации управляющие внутренним поведением libfptu, т.е
 * их изменение требует пересборки библиотеки.
 *
 * Чуть позже эти определения передедут в fptu_config.h */

// TBD

//----------------------------------------------------------------------------
/* Общие перечисления и структуры */

/* Коды ошибок.
 * Список будет пополнен, а описания уточнены. */
enum fptu_error {
  FPTU_SUCCESS = 0,
  FPTU_OK = FPTU_SUCCESS,
#if defined(_WIN32) || defined(_WIN64)
  FPTU_ENOFIELD = 0x00000650 /* ERROR_INVALID_FIELD */,
  FPTU_EINVAL = 0x00000057 /* ERROR_INVALID_PARAMETER */,
  FPTU_ENOSPACE = 0x00000540 /* ERROR_ALLOTTED_SPACE_EXCEEDED */,
#else
#ifdef ENOKEY
  FPTU_ENOFIELD = ENOKEY /* Required key not available */,
#else
  FPTU_ENOFIELD = ENOENT /* No such file or directory (POSIX) */,
#endif
  FPTU_EINVAL = EINVAL /* Invalid argument (POSIX) */,
  FPTU_ENOSPACE = ENOBUFS /* No buffer space available (POSIX)  */,
/* OVERFLOW - Value too large to be stored in data type (POSIX) */
#endif
};

#pragma pack(push, 1)

#ifdef __cplusplus
enum fptu_type : uint32_t;
union fptu_payload;
#else
typedef enum fptu_error fptu_error;
typedef union fptu_varlen fptu_varlen;
typedef union fptu_payload fptu_payload;
typedef union fptu_field fptu_field;
typedef union fptu_unit fptu_unit;
typedef union fptu_time fptu_time;
typedef union fptu_ro fptu_ro;
typedef struct fptu_rw fptu_rw;
typedef enum fptu_type fptu_type;
typedef enum fptu_filter fptu_filter;
typedef enum fptu_json_options fptu_json_options;
#endif /* __cplusplus */

/* Внутренний тип для хранения размера полей переменной длины. */
union fptu_varlen {
  struct {
    uint16_t brutto; /* брутто-размер в 4-байтовых юнитах,
                      * всегда больше или равен 1. */
    union {
      uint16_t opaque_bytes;
      uint16_t array_length;
      uint16_t tuple_items;
    };
  };
  uint32_t flat;
};

/* Поле кортежа.
 *
 * Фактически это дескриптор поля, в котором записаны: тип данных,
 * номер колонки и смещение к данным. */
union
#ifdef __cplusplus
    FPTU_API
#endif /* __cplusplus */
        fptu_field {
  struct {
    uint16_t tag;    /* тип и "номер колонки". */
    uint16_t offset; /* смещение к данным относительно заголовка, либо
                        непосредственно данные для uint16_t. */
  };
  uint32_t header;
  uint32_t body[1]; /* в body[0] расположен дескриптор/заголовок,
                     * а начиная с body[offset] данные. */
#ifdef __cplusplus
  inline unsigned colnum() const;
  inline fptu_type type() const;
  inline bool is_dead() const;
  inline bool is_fixedsize() const;

  inline uint_fast16_t get_payload_uint16() const;
  inline const fptu_payload *payload() const;
  inline fptu_payload *payload();
  inline const void *inner_begin() const;
  inline const void *inner_end() const;
  inline size_t array_length() const;
#endif /* __cplusplus */
};

/* Внутренний тип соответствующий 32-битной ячейке с данными. */
union fptu_unit {
  fptu_field field;
  fptu_varlen varlen;
  uint32_t data;
};

/* Представление времени.
 *
 * В формате фиксированной точки 32-dot-32:
 *   - В старшей "целой" части секунды по UTC, буквально как выдает time(),
 *     но без знака. Это отодвигает проблему 2038-го года на 2106,
 *     требуя взамен аккуратности при вычитании.
 *   - В младшей "дробной" части неполные секунды в 1/2**32 долях.
 *
 * Эта форма унифицирована с "Positive Hiper100re" и одновременно достаточно
 * удобна в использовании. Поэтому настоятельно рекомендуется использовать
 * именно её, особенно для хранения и передачи данных. */
union
#ifdef __cplusplus
    FPTU_API
#endif /* __cplusplus */
        fptu_time {
  uint64_t fixedpoint;
  struct {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    uint32_t fractional;
    uint32_t utc;
#else
    uint32_t utc;
    uint32_t fractional;
#endif /* byte order */
  };

#ifdef __cplusplus
  static uint_fast32_t ns2fractional(uint_fast32_t);
  static uint_fast32_t fractional2ns(uint_fast32_t);
  static uint_fast32_t us2fractional(uint_fast32_t);
  static uint_fast32_t fractional2us(uint_fast32_t);
  static uint_fast32_t ms2fractional(uint_fast32_t);
  static uint_fast32_t fractional2ms(uint_fast32_t);

  double fractional2seconds() const { return std::ldexp(fractional, -32); }
  double seconds() const { return fractional2seconds() + utc; }

#ifdef HAVE_TIMESPEC_TV_NSEC
  /* LY: Clang не позволяет возвращать из C-linkage функции структуру,
   * у которой есть какие-либо конструкторы C++. Поэтому необходимо отказаться
   * либо от возможности использовать libfptu из C, либо от Clang,
   * либо от конструкторов (они и пострадали). */
  static fptu_time from_timespec(const struct timespec &ts) {
    fptu_time result = {((uint64_t)ts.tv_sec << 32) |
                        ns2fractional((uint_fast32_t)ts.tv_nsec)};
    return result;
  }
#endif /* HAVE_TIMESPEC_TV_NSEC */

#ifdef HAVE_TIMEVAL_TV_USEC
  static FPTU_API fptu_time from_timeval(const struct timeval &tv) {
    fptu_time result = {((uint64_t)tv.tv_sec << 32) |
                        us2fractional((uint_fast32_t)tv.tv_usec)};
    return result;
  }
#endif /* HAVE_TIMEVAL_TV_USEC */

#ifdef _FILETIME_
  static FPTU_API fptu_time from_filetime(FILETIME *pFileTime) {
    uint64_t ns100 =
        ((uint64_t)pFileTime->dwHighDateTime << 32) + pFileTime->dwLowDateTime;
    return from_100ns(
        ns100 - UINT64_C(/* UTC offset from 1601-01-01 */ 116444736000000000));
  }
#endif /* _FILETIME_ */
#endif /* __cplusplus */
};

union fptu_payload {
  uint32_t u32;
  int32_t i32;
  uint64_t u64;
  int64_t i64;
  fptu_time dt;
  float fp32;
  double fp64;
  char cstr[4];
  uint8_t fixbin[8];
  uint32_t fixbin_by32[2];
  uint64_t fixbin_by64[1];
  struct {
    fptu_varlen varlen;
    uint32_t data[1];
  } other;
#ifdef __cplusplus
  const void *inner_begin() const { return other.data; }
  const void *inner_end() const { return other.data - 1 + other.varlen.brutto; }
  size_t array_length() const { return other.varlen.array_length; }
#endif
};

#pragma pack(pop)

/* Представление сериализованной формы кортежа.
 *
 * Фактические это просто системная структура iovec, т.е. указатель
 * на буфер с данными и размер этих данных в байтах. Системный тип struct
 * iovec выбран для совместимости с функциями readv(), writev() и т.п.
 * Другими словами, это просто "оболочка", а сами данные кортежа должны быть
 * где-то размещены. */
union fptu_ro {
  struct {
    const fptu_unit *units;
    size_t total_bytes;
  };
  struct iovec sys;
};

/* Изменяемая форма кортежа.
 * Является плоским буфером, в начале которого расположены служебные поля.
 *
 * Инициализируется функциями fptu_init(), fptu_alloc() и fptu_fetch(). */
struct
#ifdef __cplusplus
    FPTU_API
#endif /* __cplusplus */
        fptu_rw {
  unsigned head; /* Индекс дозаписи дескрипторов, растет к началу буфера,
                    указывает на первый занятый элемент. */
  unsigned tail; /* Индекс для дозаписи данных, растет к концу буфера,
                    указываент на первый не занятый элемент. */
  unsigned junk; /* Счетчик мусорных 32-битных элементов, которые
                    образовались при удалении/обновлении. */
  unsigned pivot; /* Индекс опорной точки, от которой растут "голова" и
                     "хвоcт", указывает на терминатор заголовка. */
  unsigned end; /* Конец выделенного буфера, т.е. units[end] не наше. */

  /* TODO: Автоматическое расширение буфера.

    fptu_unit  implace[1]; // начало данных если память выделяет одним куском.

    fptu_unit *units;  // указатель на данные, который указывает
                       // либо на inplace, либо на "автоматический" буфер
   */

  fptu_unit units[1];

#ifdef __cplusplus
  /* closed and noncopyable. */
  fptu_rw(const fptu_rw &) = delete;
  fptu_rw &operator=(fptu_rw const &) = delete;

  static void *operator new(size_t bytes) = delete;
  static fptu_rw *create(size_t items_limit, size_t data_bytes);
  static void operator delete(void *ptr) { ::free(ptr); }

  static void *operator new[](size_t bytes) = delete;
  static void operator delete[](void *ptr) = delete;
#endif /* __cplusplus */
};

/* Основные ограничения, константы и их производные. */
enum fptu_bits {
  // базовые лимиты и параметры
  fptu_bits = 16u, // ширина счетчиков
  fptu_typeid_bits = 5u, // ширина типа в идентификаторе поля
  fptu_ct_reserve_bits = 1u, // резерв в идентификаторе поля
  fptu_unit_size = 4u,       // размер одного юнита
  // количество служебных (зарезервированных) бит в заголовке кортежа,
  // для признаков сортированности и отсутствия повторяющихся полей
  fptu_lx_bits = 2u,

  // производные константы и параметры
  // log2(fptu_unit_size)
  fptu_unit_shift = 2u,

  // базовый лимит значений
  fptu_limit = (UINT32_C(1) << fptu_bits) - 1u,
  // максимальный суммарный размер сериализованного представления кортежа,
  fptu_max_tuple_bytes = fptu_limit * fptu_unit_size,

  // ширина тега-номера поля/колонки
  fptu_co_bits = fptu_bits - fptu_typeid_bits - fptu_ct_reserve_bits,
  // маска для получения типа из идентификатора поля/колонки
  fptu_ty_mask = (UINT32_C(1) << fptu_typeid_bits) - 1u,
  // маска резервных битов в идентификаторе поля/колонки
  fptu_fr_mask = ((UINT32_C(1) << fptu_ct_reserve_bits) - 1u)
                 << fptu_typeid_bits,

  // сдвиг для получения тега-номера из идентификатора поля/колонки
  fptu_co_shift = fptu_typeid_bits + fptu_ct_reserve_bits,
  // значение тега-номера для удаленных полей/колонок
  fptu_co_dead = (UINT32_C(1) << fptu_co_bits) - 1u,
  // максимальный тег-номер поля/колонки
  fptu_max_cols = fptu_co_dead - 1u,

  // кол-во бит доступных для хранения размера массива дескрипторов полей
  fptu_lt_bits = fptu_bits - fptu_lx_bits,
  // маска для выделения служебных бит из заголовка кортежа
  fptu_lx_mask = ((UINT32_C(1) << fptu_lx_bits) - 1u) << fptu_lt_bits,
  // маска для получения размера массива дескрипторов из заголовка кортежа
  fptu_lt_mask = (UINT32_C(1) << fptu_lt_bits) - 1u,
  // максимальное кол-во полей/колонок в одном кортеже
  fptu_max_fields = fptu_lt_mask,

  // максимальный размер поля/колонки
  fptu_max_field_bytes = fptu_limit,
  // максимальный размер произвольной последовательности байт
  fptu_max_opaque_bytes = fptu_max_field_bytes - fptu_unit_size,
  // максимальное кол-во элементов в массиве,
  // так чтобы при любом базовом типе не превышались другие лимиты
  fptu_max_array_len = fptu_max_opaque_bytes / 32,
  // буфер достаточного размера для любого кортежа
  fptu_buffer_enough =
      sizeof(fptu_rw) + fptu_max_tuple_bytes + fptu_max_fields * fptu_unit_size,
  // предельный размер, превышение которого считается ошибкой
  fptu_buffer_limit = fptu_max_tuple_bytes * 2
};

/* Типы полей.
 *
 * Следует обратить внимание, что fptu_farray является флагом,
 * а значения начиная с fptu_ffilter используются как маски для
 * поиска/фильтрации полей (и видимо будут выделены в отдельный enum). */
enum fptu_type
#ifdef __cplusplus
    : uint32_t
#endif
{
  // fixed length, without ex-data (descriptor only)
  fptu_null = 0,
  fptu_uint16 = 1, // including boolean and enums

  // fixed length with ex-data (at least 4 byte after the pivot)
  fptu_int32 = 2,
  fptu_uint32 = 3,
  fptu_fp32 = 4, // 32-bit float-point, e.g. float

  fptu_int64 = 5,
  fptu_uint64 = 6,
  fptu_fp64 = 7,     // 64-bit float-point, e.g. double
  fptu_datetime = 8, // date-time as fixed-point, compatible with UTC

  fptu_96 = 9,   // opaque 12-bytes (subject to change)
  fptu_128 = 10, // opaque 16-bytes (uuid, ipv6, etc).
  fptu_160 = 11, // opaque 20-bytes (sha1).
  fptu_256 = 12, // opaque 32-bytes (sha256).

  // variable length, e.g. length and payload inside ex-data
  fptu_cstr = 13, // utf-8 с-string, zero terminated

  // with additional length
  fptu_opaque = 14, // opaque octet string
  fptu_nested = 15, // nested tuple
  fptu_farray = 16, // flag for array-types

  // arrays
  fptu_array_uint16 = fptu_uint16 | fptu_farray,
  fptu_array_int32 = fptu_int32 | fptu_farray,
  fptu_array_uint32 = fptu_uint32 | fptu_farray,
  fptu_array_fp32 = fptu_fp32 | fptu_farray,
  fptu_array_int64 = fptu_int64 | fptu_farray,
  fptu_array_uint64 = fptu_uint64 | fptu_farray,
  fptu_array_fp64 = fptu_fp64 | fptu_farray,
  fptu_array_datetime = fptu_datetime | fptu_farray,
  fptu_array_96 = fptu_96 | fptu_farray,
  fptu_array_128 = fptu_128 | fptu_farray,
  fptu_array_160 = fptu_160 | fptu_farray,
  fptu_array_256 = fptu_256 | fptu_farray,
  fptu_array_cstr = fptu_cstr | fptu_farray,
  fptu_array_opaque = fptu_opaque | fptu_farray,
  fptu_array_nested = fptu_nested | fptu_farray,

  // pseudo types for lookup and filtering
  fptu_typeid_max = (INT32_C(1) << fptu_typeid_bits) - 1,

  // aliases
  fptu_16 = fptu_uint16,
  fptu_32 = fptu_uint32,
  fptu_64 = fptu_uint64,
  fptu_bool = fptu_uint16,
  fptu_array_bool = fptu_array_uint16,
  fptu_enum = fptu_uint16,
  fptu_array_enum = fptu_array_uint16,
  fptu_wchar = fptu_uint16,
  fptu_ipv4 = fptu_uint32,
  fptu_uuid = fptu_128,
  fptu_ipv6 = fptu_128,
  fptu_md5 = fptu_128,
  fptu_sha1 = fptu_160,
  fptu_sha256 = fptu_256,
  fptu_wstring = fptu_opaque
};

static __inline fptu_type fptu_type_array_of(fptu_type type) {
  assert(type > fptu_null && type <= fptu_nested);
  return (fptu_type)((uint32_t)type | fptu_farray);
}

enum fptu_filter
#ifdef __cplusplus
    : uint32_t
#endif
{ fptu_ffilter = UINT32_C(1) << (fptu_null | fptu_farray),
  fptu_any = ~UINT32_C(0), // match any type
  fptu_any_int = fptu_ffilter | (UINT32_C(1) << fptu_int32) |
                 (UINT32_C(1) << fptu_int64), // match int32/int64
  fptu_any_uint = fptu_ffilter | (UINT32_C(1) << fptu_uint16) |
                  (UINT32_C(1) << fptu_uint32) |
                  (UINT32_C(1) << fptu_uint64), // match uint16/uint32/uint64
  fptu_any_fp = fptu_ffilter | (UINT32_C(1) << fptu_fp32) |
                (UINT32_C(1) << fptu_fp64), // match fp32/fp64
  fptu_any_number = fptu_any_int | fptu_any_uint | fptu_any_fp,
};
FPT_ENUM_FLAG_OPERATORS(fptu_filter)

static __inline fptu_filter fptu_filter_mask(fptu_type type) {
  assert(type <= fptu_array_nested);
  return (fptu_filter)(UINT32_C(1) << type);
}

#ifdef __cplusplus
enum class fptu_type_or_filter : uint32_t {};
#else
typedef int32_t fptu_type_or_filter;
#endif /* __cplusplus */

static __inline unsigned fptu_get_colnum(uint_fast16_t tag) {
  return ((unsigned)tag) >> fptu_co_shift;
}

static __inline fptu_type fptu_get_type(uint_fast16_t tag) {
  return (fptu_type)(tag & fptu_ty_mask);
}

static __inline uint_fast16_t fptu_make_tag(unsigned column, fptu_type type) {
  assert((unsigned)type <= fptu_ty_mask);
  assert(column <= fptu_max_cols);
  return (uint_fast16_t)type + (column << fptu_co_shift);
}

static __inline bool fptu_tag_is_fixedsize(uint_fast16_t tag) {
  return fptu_get_type(tag) < fptu_cstr;
}

static __inline bool fptu_tag_is_dead(uint_fast16_t tag) {
  return tag >= (fptu_co_dead << fptu_co_shift);
}

static __inline bool fptu_field_is_dead(const fptu_field *pf) {
  return !pf || fptu_tag_is_dead(pf->tag);
}

static __inline const fptu_payload *fptu_get_payload(const fptu_field *pf) {
  return (const fptu_payload *)&pf->body[pf->offset];
}

static __inline fptu_payload *fptu_field_payload(fptu_field *pf) {
  return (fptu_payload *)&pf->body[pf->offset];
}

/* Возвращает текущее время в правильной форме.
 *
 * Аргумент grain_ns задает желаемую точность в наносекундах, в зависимости от
 * которой будет использован CLOCK_REALTIME, либо CLOCK_REALTIME_COARSE.
 *
 * Положительные значения grain_ns, включая нуль, трактуются как наносекунды.
 *
 * Отрицательные же означают количество младших бит, которые НЕ требуются в
 * результате и будут обнулены. Таким образом, отрицательные значения grain_ns
 * позволяют запросить текущее время, одновременно с "резервированием" младших
 * бит результата под специфические нужды.
 *
 * В конечном счете это позволяет существенно экономить на системных вызовах
 * и/или обращении к аппаратуре. В том числе не выполнять системный вызов,
 * а ограничиться использованием механизма vdso (прямое чтение из открытой
 * страницы данных ядра). В зависимости от запрошенной точности,
 * доступной аппаратуры и актуальном режиме работы ядра, экономия может
 * составить до сотен и даже тысяч раз.
 *
 * Следует понимать, что реальная точность зависит от актуальной конфигурации
 * аппаратуры и ядра ОС. Проще говоря, запрос текущего времени с grain_ns = 1
 * достаточно абсурден и вовсе не гарантирует такой точности результата. */
FPTU_API fptu_time fptu_now(int grain_ns);
FPTU_API fptu_time fptu_now_fine(void);
FPTU_API fptu_time fptu_now_coarse(void);

//----------------------------------------------------------------------------

#define FPTU_DENIL_FP32_BIN UINT32_C(0xFFFFffff)
#ifndef _MSC_VER /* MSVC provides invalid nanf(), leave it undefined */
#define FPTU_DENIL_FP32_MAS "0x007FFFFF"
#endif /* ! _MSC_VER */

#if defined(_MSC_VER) && /* obsolete and trouble full */ _MSC_VER < 1910
static __inline float fptu_fp32_denil(void) {
  union {
    uint32_t u32;
    float fp32;
  } casting;
  casting.u32 = FPTU_DENIL_FP32_BIN;
  return casting.fp32;
#else
static __inline constexpr float fptu_fp32_denil(void) {
#if defined(FPTU_DENIL_FP32_MAS) && (__GNUC_PREREQ(3, 3) || __has_builtin(nanf))
  return -__builtin_nanf(FPTU_DENIL_FP32_MAS);
#else
  union {
    uint32_t u32;
    float fp32;
  } const constexpr casting = {FPTU_DENIL_FP32_BIN};
  return casting.fp32;
#endif /* FPTU_DENIL_FP32_MAS */
#endif /* !_MSVC */
}
#define FPTU_DENIL_FP32 fptu_fp32_denil()

#define FPTU_DENIL_FP64_BIN UINT64_C(0xFFFFffffFFFFffff)
#ifndef _MSC_VER /* MSVC provides invalid nan(), leave it undefined */
#define FPTU_DENIL_FP64_MAS "0x000FffffFFFFffff"
#endif /* ! _MSC_VER */

#if defined(_MSC_VER) && /* obsolete and trouble full */ _MSC_VER < 1910
static __inline double fptu_fp64_denil(void) {
  union {
    uint64_t u64;
    double fp64;
  } casting;
  casting.u64 = FPTU_DENIL_FP64_BIN;
  return casting.fp64;
#else
static __inline constexpr double fptu_fp64_denil(void) {
#if defined(FPTU_DENIL_FP64_MAS) && (__GNUC_PREREQ(3, 3) || __has_builtin(nan))
  return -__builtin_nan(FPTU_DENIL_FP64_MAS);
#else
  union {
    uint64_t u64;
    double fp64;
  } const constexpr casting = {FPTU_DENIL_FP64_BIN};
  return casting.fp64;
#endif /* FPTU_DENIL_FP64_MAS */
#endif /* !_MSVC */
}
#define FPTU_DENIL_FP64 fptu_fp64_denil()

#define FPTU_DENIL_UINT16 UINT16_MAX
#define FPTU_DENIL_SINT32 INT32_MIN
#define FPTU_DENIL_UINT32 UINT32_MAX
#define FPTU_DENIL_SINT64 INT64_MIN
#define FPTU_DENIL_UINT64 UINT64_MAX

#define FPTU_DENIL_TIME_BIN (0)
#ifdef __GNUC__
#define FPTU_DENIL_TIME                                                        \
  ({                                                                           \
    constexpr const fptu_time __fptu_time_denil = {FPTU_DENIL_TIME_BIN};       \
    __fptu_time_denil;                                                         \
  })
#else
static __inline fptu_time fptu_time_denil(void) {
  constexpr const fptu_time denil = {FPTU_DENIL_TIME_BIN};
  return denil;
}
#define FPTU_DENIL_TIME fptu_time_denil()
#endif
#define FPTU_DENIL_CSTR nullptr
#define FPTU_DENIL_FIXBIN nullptr

//----------------------------------------------------------------------------

/* Возвращает минимальный размер буфера, который необходим для размещения
 * кортежа с указанным кол-вом полей и данных. */
FPTU_API size_t fptu_space(size_t items, size_t data_bytes);

/* Инициализирует в буфере кортеж, резервируя (отступая) место достаточное
 * для добавления до items_limit полей. Оставшееся место будет
 * использоваться для размещения данных.
 *
 * Возвращает указатель на созданный в буфере объект, либо nullptr, если
 * заданы неверные параметры или размер буфера недостаточен. */
FPTU_API fptu_rw *fptu_init(void *buffer_space, size_t buffer_bytes,
                            size_t items_limit);

/* Выделяет через malloc() и инициализирует кортеж достаточный для
 * размещения заданного кол-ва полей и данных.
 *
 * Возвращает адрес объекта, который необходимо free() по окончании
 * использования. Либо nullptr при неверных параметрах или нехватке памяти. */
FPTU_API fptu_rw *fptu_alloc(size_t items_limit, size_t data_bytes);

/* Очищает ранее инициализированный кортеж.
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTU_API fptu_error fptu_clear(fptu_rw *pt);

/* Возвращает кол-во свободных слотов для добавления дескрипторов
 * полей в заголовок кортежа. */
FPTU_API size_t fptu_space4items(const fptu_rw *pt);

/* Возвращает остаток места доступного для размещения данных. */
FPTU_API size_t fptu_space4data(const fptu_rw *pt);

/* Возвращает объем мусора в байтах, который станет доступным для
 * добавления полей и данных после fptu_shrink(). */
FPTU_API size_t fptu_junkspace(const fptu_rw *pt);

/* Проверяет сериализованную форму кортежа на корректность.
 *
 * Возвращает nullptr если ошибок не обнаружено, либо указатель на константную
 * строку с краткой информацией о проблеме (нарушенное условие). */
FPTU_API const char *fptu_check_ro(fptu_ro ro);

/* Проверяет модифицируемую форму кортежа на корректность.
 *
 * Возвращает nullptr если ошибок не обнаружено, либо указатель на константную
 * строку с краткой информацией о проблеме (нарушенное условие). */
FPTU_API const char *fptu_check_rw(const fptu_rw *pt);

/* Возвращает сериализованную форму кортежа, которая находится внутри
 * модифицируемой. Дефрагментация не выполняется, поэтому сериализованная
 * форма может содержать лишний мусор, см fptu_junkspace().
 *
 * Возвращаемый результат валиден до изменения или разрушения исходной
 * модифицируемой формы кортежа. */
FPTU_API fptu_ro fptu_take_noshrink(const fptu_rw *pt);

/* Строит в указанном буфере модифицируемую форму кортежа из сериализованной.
 * Проверка корректности данных в сериализованной форме не производится.
 * Сериализованная форма не модифицируется и не требуется после возврата из
 * функции.
 * Аргумент more_items резервирует кол-во полей, которые можно будет добавить
 * в создаваемую модифицируемую форму.
 *
 * Возвращает указатель на созданный в буфере объект, либо nullptr, если
 * заданы неверные параметры или размер буфера недостаточен. */
FPTU_API fptu_rw *fptu_fetch(fptu_ro ro, void *buffer_space,
                             size_t buffer_bytes, unsigned more_items);

/* Проверяет содержимое сериализованной формы на корректность. Одновременно
 * возвращает размер буфера, который потребуется для модифицируемой формы,
 * с учетом добавляемых more_items полей и more_payload данных.
 *
 * При неверных параметрах или некорректных данных возвращает 0 и записывает
 * в error информацию об ошибке (указатель на константную строку
 * с краткой информацией о проблеме). */
FPTU_API size_t fptu_check_and_get_buffer_size(fptu_ro ro, unsigned more_items,
                                               unsigned more_payload,
                                               const char **error);
FPTU_API size_t fptu_get_buffer_size(fptu_ro ro, unsigned more_items,
                                     unsigned more_payload);

/* Производит дефрагментацию модифицируемой формы кортежа.
 * Возвращает true если была произведена дефрагментация, что можно
 * использовать как признак инвалидации итераторов. */
FPTU_API bool fptu_shrink(fptu_rw *pt);

/* Производит дефрагментацию модифицируемой формы кортежа при наличии
 * пустот/мусора после удаления полей.
 * Возвращает true если была произведена дефрагментация, что можно
 * использовать как признак инвалидации итераторов. */
static __inline bool fptu_cond_shrink(fptu_rw *pt) {
  return pt->junk != 0 && fptu_shrink(pt);
}

/* Возвращает сериализованную форму кортежа, которая находится внутри
 * модифицируемой. При необходимости автоматически производится
 * дефрагментация.
 * Возвращаемый результат валиден до изменения или разрушения исходной
 * модифицируемой формы кортежа. */
static __inline fptu_ro fptu_take(fptu_rw *pt) {
  fptu_cond_shrink(pt);
  return fptu_take_noshrink(pt);
}

/* Если в аргументе type_or_filter взведен бит fptu_ffilter,
 * то type_or_filter интерпретируется как битовая маска типов.
 * Соответственно, будут удалены все поля с заданным column и попадающие
 * в маску типов. Например, если type_or_filter равен fptu_any_fp,
 * то удаляются все fptu_fp32 и fptu_fp64.
 *
 * Из C++ в namespace fptu доступно несколько перегруженных вариантов.
 *
 * Возвращается кол-во удаленных полей (больше либо равно нулю),
 * либо отрицательный код ошибки. */
FPTU_API int fptu_erase(fptu_rw *pt, unsigned column,
                        fptu_type_or_filter type_or_filter);
FPTU_API void fptu_erase_field(fptu_rw *pt, fptu_field *pf);

FPTU_API bool fptu_is_empty_ro(fptu_ro ro);

static __inline bool fptu_is_empty_rw(const fptu_rw *pt) {
  return pt->pivot - pt->head == pt->junk;
}

//----------------------------------------------------------------------------

FPTU_API extern const char fptu_empty_cstr[];
FPTU_API extern const uint8_t fptu_internal_map_t2b[];
FPTU_API extern const uint8_t fptu_internal_map_t2u[];

/* Вставка или обновление существующего поля.
 *
 * В случае коллекций, когда в кортеже более одного поля с соответствующим
 * типом и номером), будет обновлен первый найденный экземпляр. Но так как
 * в общем случае физический порядок полей не определен, следует считать что
 * функция обновит произвольный экземпляр поля. Поэтому для манипулирования
 * коллекциями следует использовать fptu_erase() и/или fput_field_set_xyz().
 */
FPTU_API fptu_error fptu_upsert_null(fptu_rw *pt, unsigned column);
FPTU_API fptu_error fptu_upsert_uint16(fptu_rw *pt, unsigned column,
                                       uint_fast16_t value);
static __inline fptu_error fptu_upsert_bool(fptu_rw *pt, unsigned column,
                                            bool value) {
  return fptu_upsert_uint16(pt, column, value);
}

FPTU_API fptu_error fptu_upsert_int32(fptu_rw *pt, unsigned column,
                                      int_fast32_t value);
FPTU_API fptu_error fptu_upsert_uint32(fptu_rw *pt, unsigned column,
                                       uint_fast32_t value);
FPTU_API fptu_error fptu_upsert_int64(fptu_rw *pt, unsigned column,
                                      int_fast64_t value);
FPTU_API fptu_error fptu_upsert_uint64(fptu_rw *pt, unsigned column,
                                       uint_fast64_t value);
FPTU_API fptu_error fptu_upsert_fp64(fptu_rw *pt, unsigned column,
                                     double_t value);
FPTU_API fptu_error fptu_upsert_fp32(fptu_rw *pt, unsigned column,
                                     float_t value);
FPTU_API fptu_error fptu_upsert_datetime(fptu_rw *pt, unsigned column,
                                         const fptu_time);

FPTU_API fptu_error fptu_upsert_96(fptu_rw *pt, unsigned column,
                                   const void *data);
FPTU_API fptu_error fptu_upsert_128(fptu_rw *pt, unsigned column,
                                    const void *data);
FPTU_API fptu_error fptu_upsert_160(fptu_rw *pt, unsigned column,
                                    const void *data);
FPTU_API fptu_error fptu_upsert_256(fptu_rw *pt, unsigned column,
                                    const void *data);

FPTU_API fptu_error fptu_upsert_string(fptu_rw *pt, unsigned column,
                                       const char *text, size_t length);
static __inline fptu_error fptu_upsert_cstr(fptu_rw *pt, unsigned column,
                                            const char *value) {
  if (value == nullptr)
    value = fptu_empty_cstr;

  return fptu_upsert_string(pt, column, value, strlen(value));
}

FPTU_API fptu_error fptu_upsert_opaque(fptu_rw *pt, unsigned column,
                                       const void *value, size_t bytes);
FPTU_API fptu_error fptu_upsert_opaque_iov(fptu_rw *pt, unsigned column,
                                           const struct iovec value);
FPTU_API fptu_error fptu_upsert_nested(fptu_rw *pt, unsigned column,
                                       fptu_ro ro);

// TODO
// FPTU_API fptu_error fptu_upsert_array_uint16(fptu_rw* pt, uint_fast16_t ct,
// size_t array_length, const uint16_t* array_data); FPTU_API fptu_error
// fptu_upsert_array_int32(fptu_rw* pt, uint_fast16_t ct, size_t array_length,
// const int32_t* array_data);
// FPTU_API fptu_error fptu_upsert_array_uint32(fptu_rw* pt, uint_fast16_t ct,
// size_t array_length, const uint32_t* array_data); FPTU_API fptu_error
// fptu_upsert_array_int64(fptu_rw* pt, uint_fast16_t ct, size_t array_length,
// const int64_t* array_data);
// FPTU_API fptu_error fptu_upsert_array_uint64(fptu_rw* pt, uint_fast16_t ct,
// size_t array_length, const uint64_t* array_data); FPTU_API fptu_error
// fptu_upsert_array_cstr(fptu_rw* pt, uint_fast16_t ct, size_t array_length,
// const char* array_data[]);
// FPTU_API fptu_error fptu_upsert_array_nested(fptu_rw* pt, uint_fast16_t ct,
// size_t array_length, const char* array_data[]);

//----------------------------------------------------------------------------

// Добавление ещё одного поля, для поддержки коллекций.
FPTU_API fptu_error fptu_insert_uint16(fptu_rw *pt, unsigned column,
                                       uint_fast16_t value);
static __inline fptu_error fptu_insert_bool(fptu_rw *pt, unsigned column,
                                            bool value) {
  return fptu_insert_uint16(pt, column, value);
}
FPTU_API fptu_error fptu_insert_int32(fptu_rw *pt, unsigned column,
                                      int_fast32_t value);
FPTU_API fptu_error fptu_insert_uint32(fptu_rw *pt, unsigned column,
                                       uint_fast32_t value);
FPTU_API fptu_error fptu_insert_int64(fptu_rw *pt, unsigned column,
                                      int_fast64_t value);
FPTU_API fptu_error fptu_insert_uint64(fptu_rw *pt, unsigned column,
                                       uint_fast64_t value);
FPTU_API fptu_error fptu_insert_fp64(fptu_rw *pt, unsigned column,
                                     double_t value);
FPTU_API fptu_error fptu_insert_fp32(fptu_rw *pt, unsigned column,
                                     float_t value);
FPTU_API fptu_error fptu_insert_datetime(fptu_rw *pt, unsigned column,
                                         const fptu_time);

FPTU_API fptu_error fptu_insert_96(fptu_rw *pt, unsigned column,
                                   const void *data);
FPTU_API fptu_error fptu_insert_128(fptu_rw *pt, unsigned column,
                                    const void *data);
FPTU_API fptu_error fptu_insert_160(fptu_rw *pt, unsigned column,
                                    const void *data);
FPTU_API fptu_error fptu_insert_256(fptu_rw *pt, unsigned column,
                                    const void *data);

FPTU_API fptu_error fptu_insert_string(fptu_rw *pt, unsigned column,
                                       const char *text, size_t length);
static __inline fptu_error fptu_insert_cstr(fptu_rw *pt, unsigned column,
                                            const char *value) {
  if (value == nullptr)
    value = fptu_empty_cstr;

  return fptu_insert_string(pt, column, value, strlen(value));
}

FPTU_API fptu_error fptu_insert_opaque(fptu_rw *pt, unsigned column,
                                       const void *value, size_t bytes);
FPTU_API fptu_error fptu_insert_opaque_iov(fptu_rw *pt, unsigned column,
                                           const struct iovec value);
FPTU_API fptu_error fptu_insert_nested(fptu_rw *pt, unsigned column,
                                       fptu_ro ro);

// TODO
// FPTU_API fptu_error fptu_insert_array_uint16(fptu_rw* pt, uint_fast16_t ct,
// size_t array_length, const uint16_t* array_data); FPTU_API fptu_error
// fptu_insert_array_int32(fptu_rw* pt, uint_fast16_t ct, size_t array_length,
// const int32_t* array_data);
// FPTU_API fptu_error fptu_insert_array_uint32(fptu_rw* pt, uint_fast16_t ct,
// size_t array_length, const uint32_t* array_data); FPTU_API fptu_error
// fptu_insert_array_int64(fptu_rw* pt, uint_fast16_t ct, size_t array_length,
// const int64_t* array_data);
// FPTU_API fptu_error fptu_insert_array_uint64(fptu_rw* pt, uint_fast16_t ct,
// size_t array_length, const uint64_t* array_data); FPTU_API fptu_error
// fptu_insert_array_str(fptu_rw* pt, uint_fast16_t ct, size_t array_length,
// const char* array_data[]);

//----------------------------------------------------------------------------

// Обновление существующего поля (первого найденного для коллекций).
FPTU_API fptu_error fptu_update_uint16(fptu_rw *pt, unsigned column,
                                       uint_fast16_t value);
static __inline fptu_error fptu_update_bool(fptu_rw *pt, unsigned column,
                                            bool value) {
  return fptu_update_uint16(pt, column, value);
}
FPTU_API fptu_error fptu_update_int32(fptu_rw *pt, unsigned column,
                                      int_fast32_t value);
FPTU_API fptu_error fptu_update_uint32(fptu_rw *pt, unsigned column,
                                       uint_fast32_t value);
FPTU_API fptu_error fptu_update_int64(fptu_rw *pt, unsigned column,
                                      int_fast64_t value);
FPTU_API fptu_error fptu_update_uint64(fptu_rw *pt, unsigned column,
                                       uint_fast64_t value);
FPTU_API fptu_error fptu_update_fp64(fptu_rw *pt, unsigned column,
                                     double_t value);
FPTU_API fptu_error fptu_update_fp32(fptu_rw *pt, unsigned column,
                                     float_t value);
FPTU_API fptu_error fptu_update_datetime(fptu_rw *pt, unsigned column,
                                         const fptu_time);

FPTU_API fptu_error fptu_update_96(fptu_rw *pt, unsigned column,
                                   const void *data);
FPTU_API fptu_error fptu_update_128(fptu_rw *pt, unsigned column,
                                    const void *data);
FPTU_API fptu_error fptu_update_160(fptu_rw *pt, unsigned column,
                                    const void *data);
FPTU_API fptu_error fptu_update_256(fptu_rw *pt, unsigned column,
                                    const void *data);

FPTU_API fptu_error fptu_update_string(fptu_rw *pt, unsigned column,
                                       const char *text, size_t length);
static __inline fptu_error fptu_update_cstr(fptu_rw *pt, unsigned column,
                                            const char *value) {
  if (value == nullptr)
    value = fptu_empty_cstr;

  return fptu_update_string(pt, column, value, strlen(value));
}

FPTU_API fptu_error fptu_update_opaque(fptu_rw *pt, unsigned column,
                                       const void *value, size_t bytes);
FPTU_API fptu_error fptu_update_opaque_iov(fptu_rw *pt, unsigned column,
                                           const struct iovec value);
FPTU_API fptu_error fptu_update_nested(fptu_rw *pt, unsigned column,
                                       fptu_ro ro);

// TODO
// FPTU_API fptu_error fptu_update_array_uint16(fptu_rw* pt, uint_fast16_t ct,
// size_t array_length, const uint16_t* array_data); FPTU_API fptu_error
// fptu_update_array_int32(fptu_rw* pt, uint_fast16_t ct, size_t array_length,
// const int32_t* array_data);
// FPTU_API fptu_error fptu_update_array_uint32(fptu_rw* pt, uint_fast16_t ct,
// size_t array_length, const uint32_t* array_data); FPTU_API fptu_error
// fptu_update_array_int64(fptu_rw* pt, uint_fast16_t ct, size_t array_length,
// const int64_t* array_data);
// FPTU_API fptu_error fptu_update_array_uint64(fptu_rw* pt, uint_fast16_t ct,
// size_t array_length, const uint64_t* array_data); FPTU_API fptu_error
// fptu_update_array_cstr(fptu_rw* pt, uint_fast16_t ct, size_t array_length,
// const char* array_data[]);

//----------------------------------------------------------------------------

/* Возвращает первое поле попадающее под критерий выбора, либо nullptr.
 * Семантика type_or_filter указана в описании fptu_erase().
 *
 * Из C++ в namespace fptu доступно несколько перегруженных вариантов. */
FPTU_API const fptu_field *fptu_lookup_ro(fptu_ro ro, unsigned column,
                                          fptu_type_or_filter type_or_filter);
FPTU_API fptu_field *fptu_lookup_rw(fptu_rw *pt, unsigned column,
                                    fptu_type_or_filter type_or_filter);

/* Возвращает "итераторы" по кортежу, в виде указателей.
 * Гарантируется что begin меньше, либо равно end.
 * В возвращаемом диапазоне могут буть удаленные поля,
 * для которых fptu_field_column() возвращает отрицательное значение. */
FPTU_API const fptu_field *fptu_begin_ro(fptu_ro ro);
FPTU_API const fptu_field *fptu_end_ro(fptu_ro ro);
FPTU_API const fptu_field *fptu_begin_rw(const fptu_rw *pt);
FPTU_API const fptu_field *fptu_end_rw(const fptu_rw *pt);

/* Итерация полей кортежа с заданным условие отбора, при этом
 * удаленные поля пропускаются.
 * Семантика type_or_filter указана в описании fptu_erase().
 *
 * Из C++ в namespace fptu доступно несколько перегруженных вариантов. */
FPTU_API const fptu_field *fptu_first(const fptu_field *begin,
                                      const fptu_field *end, unsigned column,
                                      fptu_type_or_filter type_or_filter);
FPTU_API const fptu_field *fptu_next(const fptu_field *from,
                                     const fptu_field *end, unsigned column,
                                     fptu_type_or_filter type_or_filter);

/* Итерация полей кортежа с заданным внешним фильтром, при этом
 * удаленные поля пропускаются. */
typedef bool fptu_field_filter(const fptu_field *, void *context, void *param);
FPTU_API const fptu_field *fptu_first_ex(const fptu_field *begin,
                                         const fptu_field *end,
                                         fptu_field_filter filter,
                                         void *context, void *param);
FPTU_API const fptu_field *fptu_next_ex(const fptu_field *begin,
                                        const fptu_field *end,
                                        fptu_field_filter filter, void *context,
                                        void *param);
/* Подсчет количества полей по заданному номеру колонки и типу,
 * либо маски типов.
 * Семантика type_or_filter указана в описании fptu_erase().
 *
 * Из C++ в namespace fptu доступно несколько перегруженных вариантов. */
FPTU_API size_t fptu_field_count_ro(fptu_ro ro, unsigned column,
                                    fptu_type_or_filter type_or_filter);

FPTU_API size_t fptu_field_count_rw(const fptu_rw *rw, unsigned column,
                                    fptu_type_or_filter type_or_filter);

/* Подсчет количества полей задаваемой функцией-фильтром. */
FPTU_API size_t fptu_field_count_rw_ex(const fptu_rw *rw,
                                       fptu_field_filter filter, void *context,
                                       void *param);
FPTU_API size_t fptu_field_count_ro_ex(fptu_ro ro, fptu_field_filter filter,
                                       void *context, void *param);

FPTU_API fptu_type fptu_field_type(const fptu_field *pf);
FPTU_API int fptu_field_column(const fptu_field *pf);
FPTU_API struct iovec fptu_field_as_iovec(const fptu_field *pf);

FPTU_API uint_fast16_t fptu_field_uint16(const fptu_field *pf);
static __inline bool fptu_field_bool(const fptu_field *pf) {
  const uint_fast16_t value = fptu_field_uint16(pf);
  return value != 0 && value != FPTU_DENIL_UINT16;
}

FPTU_API int_fast32_t fptu_field_int32(const fptu_field *pf);
FPTU_API uint_fast32_t fptu_field_uint32(const fptu_field *pf);
FPTU_API int_fast64_t fptu_field_int64(const fptu_field *pf);
FPTU_API uint_fast64_t fptu_field_uint64(const fptu_field *pf);
FPTU_API double_t fptu_field_fp64(const fptu_field *pf);
FPTU_API float_t fptu_field_fp32(const fptu_field *pf);
FPTU_API fptu_time fptu_field_datetime(const fptu_field *pf);
FPTU_API const uint8_t *fptu_field_96(const fptu_field *pf);
FPTU_API const uint8_t *fptu_field_128(const fptu_field *pf);
FPTU_API const uint8_t *fptu_field_160(const fptu_field *pf);
FPTU_API const uint8_t *fptu_field_256(const fptu_field *pf);
FPTU_API const char *fptu_field_cstr(const fptu_field *pf);
FPTU_API struct iovec fptu_field_opaque(const fptu_field *pf);
FPTU_API fptu_ro fptu_field_nested(const fptu_field *pf);

FPTU_API uint_fast16_t fptu_get_uint16(fptu_ro ro, unsigned column, int *error);
static __inline bool fptu_get_bool(fptu_ro ro, unsigned column, int *error) {
  const uint_fast16_t value = fptu_get_uint16(ro, column, error);
  return value != 0 && value != FPTU_DENIL_UINT16;
}

FPTU_API int_fast32_t fptu_get_int32(fptu_ro ro, unsigned column, int *error);
FPTU_API uint_fast32_t fptu_get_uint32(fptu_ro ro, unsigned column, int *error);
FPTU_API int_fast64_t fptu_get_int64(fptu_ro ro, unsigned column, int *error);
FPTU_API uint_fast64_t fptu_get_uint64(fptu_ro ro, unsigned column, int *error);
FPTU_API double_t fptu_get_fp64(fptu_ro ro, unsigned column, int *error);
FPTU_API float_t fptu_get_fp32(fptu_ro ro, unsigned column, int *error);
FPTU_API fptu_time fptu_get_datetime(fptu_ro ro, unsigned column, int *error);

FPTU_API int_fast64_t fptu_get_sint(fptu_ro ro, unsigned column, int *error);
FPTU_API uint_fast64_t fptu_get_uint(fptu_ro ro, unsigned column, int *error);
FPTU_API double_t fptu_get_fp(fptu_ro ro, unsigned column, int *error);

FPTU_API const uint8_t *fptu_get_96(fptu_ro ro, unsigned column, int *error);
FPTU_API const uint8_t *fptu_get_128(fptu_ro ro, unsigned column, int *error);
FPTU_API const uint8_t *fptu_get_160(fptu_ro ro, unsigned column, int *error);
FPTU_API const uint8_t *fptu_get_256(fptu_ro ro, unsigned column, int *error);
FPTU_API const char *fptu_get_cstr(fptu_ro ro, unsigned column, int *error);
FPTU_API struct iovec fptu_get_opaque(fptu_ro ro, unsigned column, int *error);
FPTU_API fptu_ro fptu_get_nested(fptu_ro ro, unsigned column, int *error);

// TODO: fptu_field_array(), fptu_get_array()
// typedef struct FPTU_API fptu_array {
//  size_t size;
//  union {
//    uint16_t uint16[2];
//    int32_t int32[1];
//    uint32_t uint32[1];
//    int64_t int64[1];
//    uint64_t uint64[1];
//    double fp64[1];
//    float fp32[1];
//    const char *cstr[1];
//    fptu_ro nested[1];
//    struct iovec opaque[1];
//  };
//} fptu_array;

//----------------------------------------------------------------------------
/* Определения и примитивы для сравнения. */

typedef enum fptu_lge {
  fptu_ic = 1,                           // incomparable
  fptu_eq = 2,                           // left == right
  fptu_lt = 4,                           // left < right
  fptu_gt = 8,                           // left > right
  fptu_ne = fptu_lt | fptu_gt | fptu_ic, // left != right
  fptu_le = fptu_lt | fptu_eq,           // left <= right
  fptu_ge = fptu_gt | fptu_eq            // left >= right
} fptu_lge;

FPTU_API fptu_lge fptu_cmp_96(fptu_ro ro, unsigned column,
                              const uint8_t *value);
FPTU_API fptu_lge fptu_cmp_128(fptu_ro ro, unsigned column,
                               const uint8_t *value);
FPTU_API fptu_lge fptu_cmp_160(fptu_ro ro, unsigned column,
                               const uint8_t *value);
FPTU_API fptu_lge fptu_cmp_256(fptu_ro ro, unsigned column,
                               const uint8_t *value);
FPTU_API fptu_lge fptu_cmp_opaque(fptu_ro ro, unsigned column,
                                  const void *value, size_t bytes);
FPTU_API fptu_lge fptu_cmp_opaque_iov(fptu_ro ro, unsigned column,
                                      const struct iovec value);

FPTU_API fptu_lge fptu_cmp_binary(const void *left_data, size_t left_len,
                                  const void *right_data, size_t right_len);

FPTU_API fptu_lge fptu_cmp_fields(const fptu_field *left,
                                  const fptu_field *right);
FPTU_API fptu_lge fptu_cmp_tuples(fptu_ro left, fptu_ro right);

FPTU_API const char *fptu_type_name(const fptu_type);

//----------------------------------------------------------------------------
/* Некоторые внутренние служебные функции.
 * Доступны для специальных случаев, в том числе для тестов. */

FPTU_API bool fptu_is_ordered(const fptu_field *begin, const fptu_field *end);
FPTU_API uint16_t *fptu_tags(uint16_t *const first, const fptu_field *begin,
                             const fptu_field *end);
FPTU_API bool fptu_is_under_valgrind(void);

typedef struct fptu_version_info {
  uint8_t major;
  uint8_t minor;
  uint16_t release;
  uint32_t revision;
  struct {
    const char *datetime;
    const char *tree;
    const char *commit;
    const char *describe;
  } git;
} fptu_version_info;

typedef struct fptu_build_info {
  const char *datetime;
  const char *target;
  const char *cmake_options;
  const char *compiler;
  const char *compile_flags;
} fptu_build_info;

#if HAVE_FPTU_VERSIONINFO
extern FPTU_API const fptu_version_info fptu_version;
#endif /* HAVE_FPTU_VERSIONINFO */
extern FPTU_API const fptu_build_info fptu_build;

//----------------------------------------------------------------------------
/* Сервисные функции (будет пополнятся). */

static __inline const void *fptu_inner_begin(fptu_field *pf) {
  assert((fptu_field_type(pf) & fptu_farray) != 0);
  const fptu_payload *payload = fptu_get_payload(pf);
  return payload->other.data;
}

static __inline const void *fptu_inner_end(fptu_field *pf) {
  assert((fptu_field_type(pf) & fptu_farray) != 0);
  const fptu_payload *payload = fptu_get_payload(pf);
  return payload->other.data - 1 + payload->other.varlen.brutto;
}

static __inline size_t fptu_array_length(fptu_field *pf) {
  assert((fptu_field_type(pf) & fptu_farray) != 0);
  return fptu_get_payload(pf)->other.varlen.array_length;
}

/* Функция обратного вызова, используемая для трансляции идентификаторов/тегов
 * полей в символические имена. Функция будет вызываться многократно, при
 * вывода имени каждого поля кортежа, включая все вложенные кортежи.
 *
 * Функция должна возвращать указатель на C-строку, значение которой будет
 * валидно до возврата из функции сериализации, либо до следующего вызова данной
 * функции.
 *
 * Если функция возвратит NULL, то при выводе соответствующего поля вместо
 * символического имени будет использован числовой идентификатор.
 *
 * Если функция возвратит указатель на пустую строку "", то соответствующее
 * поле будет пропущено. Таким образом, часть полей можно сделать "скрытыми"
 * для сериализации. */
typedef const char *(*fptu_tag2name_func)(const void *schema_ctx, unsigned tag);

/* Функция обратного вызова, используемая для трансляции значений полей типа
 * fptu_uint16 в символические имена enum-констант, в том числе true и false.
 * Функция будет вызываться многократно, при вывода каждого значения типа
 * fptu_uint16, включая все коллекции, массивы и поля вложенных кортежи.
 *
 * Функция должна возвращать указатель на C-строку, значение которой будет
 * валидно до возврата из функции сериализации, либо до следующего вызова данной
 * функции.
 *
 * Если функция возвратит NULL, то при выводе соответствующего значения вместо
 * символического имени будет использован числовой идентификатор.
 *
 * Если функция возвратит указатель на пустую строку "", то enum-значение
 * будет интерпретировано как bool - для нулевых значений будет
 * выведено "false", и "true" для отличных от нуля значений. */
typedef const char *(*fptu_value2enum_func)(const void *schema_ctx,
                                            unsigned tag, unsigned value);

enum fptu_json_options {
  fptu_json_default = 0,
  fptu_json_disable_JSON5 = 1 /* Выключает расширения JSON5 (больше кавычек) */,
  fptu_json_disable_Collections =
      2 /* Выключает поддержку коллекций:
           - При преобразовании в json коллекции НЕ будут выводиться
             как JSON-массивы, а соответствующие поля будут просто повторяться.
           - При преобразовании из json JSON-массивы не будут конвертироваться
             в коллекции, но вместо этого будет генерироваться
             ошибка несовпадения типов. */
  ,
  fptu_json_skip_NULLs = 4 /* TODO: Пропускать DENILs и пустые объекты */,
  fptu_json_sort_Tags = 8 /* TODO: Сортировать по тегам, иначе выводить в
                             порядке следования полей */
};
FPT_ENUM_FLAG_OPERATORS(fptu_json_options)

/* Функция обратного вызова для выталкивания сериализованных данных.
 * В случае успеха функция должна возвратить 0 (FPTU_SUCCESS), иначе код
 * ошибки.
 *
 * При преобразовании в JSON используется для вывода генерируемого текстового
 * представления из fptu_tuple2json(). */
typedef int (*fptu_emit_func)(void *emiter_ctx, const char *text,
                              size_t length);

/* Сериализует JSON-представление кортежа в "толкающей" (aka push) модели,
 * выталкивая генерируемый текст в предоставленную функцию.
 * Для трансляции идентификаторов/тегов полей и enum-значений (fptu_uint16)
 * в символические имена используются передаваемые в параметрах функции.
 *
 * Параметры output и output_ctx используются для вывода результирующих
 * текстовых данных, и поэтому обязательны. При вызове output в качестве
 * первого параметра будет передан output_ctx.
 *
 * Параметры indent и depth задают отступ и начальный уровень вложенности.
 * Оба этих параметра опциональные, т.е. могут быть нулевыми.
 *
 * Параметры schema_ctx, tag2name и value2enum используются для трансляции
 * идентификаторов/тегов и значений в символические имена, т.е. выполняют
 * роль примитивного справочника схемы. Все три параметра опциональны и могу
 * быть нулевыми, но при этом вместо символических имен для сериализации будут
 * использованы числовые идентификаторы.
 *
 * В случае успеха возвращает ноль, иначе код ошибки. */
FPTU_API fptu_error fptu_tuple2json(fptu_ro tuple, fptu_emit_func output,
                                    void *output_ctx, const char *indent,
                                    unsigned depth, const void *schema_ctx,
                                    fptu_tag2name_func tag2name,
                                    fptu_value2enum_func value2enum,
                                    const fptu_json_options options);

/* Сериализует JSON-представление кортежа в FILE.
 *
 * Назначение параметров indent, depth, schema_ctx, tag2name и value2enum
 * см в описании функции fptu_tuple2json().
 *
 * В случае успеха возвращает ноль, иначе код ошибки, в том числе
 * значение errno в случае ошибки записи в FILE */
FPTU_API fptu_error fptu_tuple2json_FILE(fptu_ro tuple, FILE *file,
                                         const char *indent, unsigned depth,
                                         const void *schema_ctx,
                                         fptu_tag2name_func tag2name,
                                         fptu_value2enum_func value2enum,
                                         const fptu_json_options options);
#ifdef __cplusplus
} /* extern "C" */

//----------------------------------------------------------------------------
/* Сервисные функции и классы для C++ (будет пополнятся). */

namespace fptu {

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4275) /* non dll-interface class 'FOO' used as base  \
                                   for dll-interface class */
#endif
class FPTU_API bad_tuple : public std::invalid_argument {
public:
  bad_tuple(const fptu_ro &);
  bad_tuple(const fptu_rw *);
};
#ifdef _MSC_VER
#pragma warning(pop)
#endif

FPTU_API void throw_error(fptu_error err);

typedef std::unique_ptr<fptu_rw> tuple_ptr;

/* Минималистичное подобие std::string_view из C++17, но с отличиями:
 *  - более быстрое СРАВНЕНИЕ С ДРУГОЙ СЕМАНТИКОЙ, сначала учитывается длина!
 *  - отсутствуют сервисные методы: remove_prefix, remove_suffix, substr, copy,
 *    starts_with, ends_with, find, rfind, find_first_of, find_last_of,
 *    find_first_not_of, find_last_not_of, operator<<(std::ostream). */
class string_view {
protected:
  const char *str;
  intptr_t len /* LY: здесь намеренно используется intptr_t:
                   - со знаком для отличия нулевой длины от отсутствия
                     значения (nullptr), используя len == -1 как DENIL.
                   - не ptrdiff_t чтобы упростить оператор сравнения,
                     из которого не стоит возвращать ptrdiff_t. */
      ;

public:
  constexpr string_view() : str(nullptr), len(-1) {}
  constexpr string_view(const string_view &) = default;
  cxx14_constexpr string_view &
  operator=(const string_view &) noexcept = default;

  constexpr string_view(const char *str, size_t count)
      : str(str), len(str ? static_cast<intptr_t>(count) : -1) {
#if __cplusplus >= 201402L
    assert(len >= 0 || (len == -1 && !str));
#endif
  }

  constexpr string_view(const char *begin, const char *end)
      : str(begin), len(begin ? static_cast<intptr_t>(end - begin) : -1) {
#if __cplusplus >= 201402L
    assert(end >= begin);
    assert(len >= 0 || (len == -1 && !begin));
#endif
  }

  constexpr string_view(const char *ptr)
      : str(ptr), len(ptr ? static_cast<intptr_t>(strlen(ptr)) : -1) {
#if __cplusplus >= 201402L
    assert(len >= 0 || (len == -1 && !str));
#endif
  }
  /* Конструктор из std::string ОБЯЗАН быть explicit для предотвращения
   * проблемы reference to temporary object из-за неявного создания string_view
   * из переданной по значению временного экземпляра std::string. */
  explicit string_view(const std::string &s)
      : str(s.data()), len(static_cast<intptr_t>(s.size())) {
    assert(s.size() < npos);
  }
  operator std::string() const { return std::string(data(), length()); }

#if HAVE_cxx17_std_string_view
  /* Конструктор из std::string_view:
   *  - Может быть НЕ-explicit, так как у std::string_view нет неявного
   *    конструктора из std::string. Поэтому не возникает проблемы
   *    reference to temporary object из-за неявного создания string_view
   *    из переданной по значению временного экземпляра std::string.
   *  - НЕ ДОЛЖЕН быть explicit для бесшовной интеграции с std::string_view. */
  constexpr string_view(const std::string_view &v) noexcept
      : str(v.data()), len(static_cast<intptr_t>(v.size())) {
    assert(v.size() < npos);
  }
  constexpr operator std::string_view() const noexcept {
    return std::string_view(data(), length());
  }
  constexpr string_view &operator=(std::string_view &v) noexcept {
    assert(v.size() < npos);
    this->str = v.data();
    this->len = static_cast<intptr_t>(v.size());
    return *this;
  }
  constexpr void swap(std::string_view &v) noexcept {
    const auto temp = *this;
    *this = v;
    v = temp;
  }
#endif /* HAVE_cxx17_std_string_view */

  cxx14_constexpr void swap(string_view &v) noexcept {
    const auto temp = *this;
    *this = v;
    v = temp;
  }

  typedef std::size_t size_type;
  typedef std::ptrdiff_t difference_type;
  typedef char value_type;
  typedef const char *const_pointer;
  typedef const char *pointer;
  typedef const char &const_reference;
  typedef const_reference reference;
  constexpr const_reference operator[](size_type pos) const { return str[pos]; }
  constexpr const_reference front() const {
#if __cplusplus >= 201402L
    assert(len > 0);
#endif
    return str[0];
  }
  constexpr const_reference back() const {
#if __cplusplus >= 201402L
    assert(len > 0);
#endif
    return str[len - 1];
  }
  cxx14_constexpr const_reference at(size_type pos) const;
  static constexpr size_type npos = size_type(INT_MAX);

  typedef const char *const_iterator;
  constexpr const_iterator cbegin() const { return str; }
  constexpr const_iterator cend() const { return str + length(); }
  typedef const_iterator iterator;
  constexpr iterator begin() const { return cbegin(); }
  constexpr iterator end() const { return cend(); }

  typedef std::reverse_iterator<const_iterator> const_reverse_iterator;
  const_reverse_iterator crbegin() const {
    return const_reverse_iterator(cend());
  }
  const_reverse_iterator crend() const {
    return const_reverse_iterator(cbegin());
  }
  typedef const_reverse_iterator reverse_iterator;
  reverse_iterator rbegin() const { return crbegin(); }
  reverse_iterator rend() const { return crend(); }

  constexpr const char *data() const { return str; }
  constexpr size_t length() const { return (len >= 0) ? (size_t)len : 0u; }
  constexpr bool empty() const { return len <= 0; }
  constexpr bool nil() const { return len < 0; }
  constexpr size_t size() const { return length(); }
  constexpr size_type max_size() const { return 32767; }

  cxx14_constexpr size_t hash_value() const {
    size_t h = (size_t)len * 3977471;
    for (intptr_t i = 0; i < len; ++i)
      h = (h ^ str[i]) * 1664525 + 1013904223;
    return h ^ 3863194411 * (h >> 11);
  }

  static cxx14_constexpr intptr_t compare(const string_view &a,
                                          const string_view &b) {
    const intptr_t diff = a.len - b.len;
    return diff ? diff
                : (a.str == b.str) ? 0 : memcmp(a.data(), b.data(), a.length());
  }
  cxx14_constexpr bool operator==(const string_view &v) const {
    return compare(*this, v) == 0;
  }
  cxx14_constexpr bool operator<(const string_view &v) const {
    return compare(*this, v) < 0;
  }
  cxx14_constexpr bool operator>(const string_view &v) const {
    return compare(*this, v) > 0;
  }
  cxx14_constexpr bool operator<=(const string_view &v) const {
    return compare(*this, v) <= 0;
  }
  cxx14_constexpr bool operator>=(const string_view &v) const {
    return compare(*this, v) >= 0;
  }
  cxx14_constexpr bool operator!=(const string_view &v) const {
    return compare(*this, v) != 0;
  }

  static intptr_t compare(const std::string &a, const string_view &b) {
    return compare(string_view(a), b);
  }
  static intptr_t compare(const string_view &a, const std::string &b) {
    return compare(a, string_view(b));
  }
  bool operator==(const std::string &s) const { return compare(*this, s) == 0; }
  bool operator<(const std::string &s) const { return compare(*this, s) < 0; }
  bool operator>(const std::string &s) const { return compare(*this, s) > 0; }
  bool operator<=(const std::string &s) const { return compare(*this, s) <= 0; }
  bool operator>=(const std::string &s) const { return compare(*this, s) >= 0; }
  bool operator!=(const std::string &s) const { return compare(*this, s) != 0; }
};

inline unsigned get_colnum(uint_fast16_t tag) { return fptu_get_colnum(tag); }

inline fptu_type get_type(uint_fast16_t tag) { return fptu_get_type(tag); }

inline bool tag_is_fixedsize(uint_fast16_t tag) {
  return fptu_tag_is_fixedsize(tag);
}
inline bool tag_is_dead(uint_fast16_t tag) { return fptu_tag_is_dead(tag); }

inline uint_fast16_t make_tag(unsigned column, fptu_type type) {
  return fptu_make_tag(column, type);
}

FPTU_API std::string format(const char *fmt, ...) __printf_args(1, 2);
FPTU_API std::ostream &format(std::ostream &out, const char *fmt, ...)
    __printf_args(2, 3);
FPTU_API std::string format_va(const char *fmt, va_list ap);
FPTU_API std::ostream &format_va(std::ostream &out, const char *fmt,
                                 va_list ap);
FPTU_API std::string hexadecimal_string(const void *data, size_t bytes);
FPTU_API std::ostream &hexadecimal_dump(std::ostream &out, const void *data,
                                        size_t bytes);

/* hexadecimal output helper */
struct output_hexadecimal {
  const void *const data;
  const size_t length;
  constexpr output_hexadecimal(const output_hexadecimal &) = default;
  constexpr output_hexadecimal(const void *data, size_t length)
      : data(data), length(length) {}
  constexpr output_hexadecimal() : output_hexadecimal(nullptr, 0) {}
  constexpr output_hexadecimal(const string_view &v)
      : output_hexadecimal(v.data(), v.size()) {}
  output_hexadecimal(const std::string &s)
      : output_hexadecimal(s.data(), s.size()) {}
};

inline int erase(fptu_rw *pt, unsigned column, fptu_type type) {
  assert(type <= fptu_array_nested);
  return fptu_erase(pt, column, (fptu_type_or_filter)type);
}

inline int erase(fptu_rw *pt, unsigned column, fptu_filter filter) {
  assert(filter >= fptu_ffilter);
  return fptu_erase(pt, column, (fptu_type_or_filter)filter);
}

inline void erase(fptu_rw *pt, fptu_field *pf) { fptu_erase_field(pt, pf); }

inline bool is_empty(const fptu_ro &ro) { return fptu_is_empty_ro(ro); }

inline bool is_empty(const fptu_rw *pt) { return fptu_is_empty_rw(pt); }

inline const char *check(const fptu_ro &ro) { return fptu_check_ro(ro); }

inline const char *check(const fptu_rw *pt) { return fptu_check_rw(pt); }

inline const fptu_field *lookup(const fptu_ro &ro, unsigned column,
                                fptu_type type) {
  assert(type <= fptu_array_nested);
  return fptu_lookup_ro(ro, column, (fptu_type_or_filter)type);
}
inline const fptu_field *lookup(const fptu_ro &ro, unsigned column,
                                fptu_filter filter) {
  assert(filter >= fptu_ffilter);
  return fptu_lookup_ro(ro, column, (fptu_type_or_filter)filter);
}

inline fptu_field *lookup(fptu_rw *rw, unsigned column, fptu_type type) {
  assert(type <= fptu_array_nested);
  return fptu_lookup_rw(rw, column, (fptu_type_or_filter)type);
}
inline fptu_field *lookup(fptu_rw *rw, unsigned column, fptu_filter filter) {
  assert(filter >= fptu_ffilter);
  return fptu_lookup_rw(rw, column, (fptu_type_or_filter)filter);
}

inline const fptu_field *begin(const fptu_ro &ro) { return fptu_begin_ro(ro); }

inline const fptu_field *begin(const fptu_ro *ro) { return fptu_begin_ro(*ro); }

inline const fptu_field *begin(const fptu_rw &rw) { return fptu_begin_rw(&rw); }

inline const fptu_field *begin(const fptu_rw *rw) { return fptu_begin_rw(rw); }

inline const fptu_field *end(const fptu_ro &ro) { return fptu_end_ro(ro); }

inline const fptu_field *end(const fptu_ro *ro) { return fptu_end_ro(*ro); }

inline const fptu_field *end(const fptu_rw &rw) { return fptu_end_rw(&rw); }

inline const fptu_field *end(const fptu_rw *rw) { return fptu_end_rw(rw); }

inline const fptu_field *first(const fptu_field *begin, const fptu_field *end,
                               unsigned column, fptu_type type) {
  assert(type <= fptu_array_nested);
  return fptu_first(begin, end, column, (fptu_type_or_filter)type);
}
inline const fptu_field *first(const fptu_field *begin, const fptu_field *end,
                               unsigned column, fptu_filter filter) {
  assert(filter >= fptu_ffilter);
  return fptu_first(begin, end, column, (fptu_type_or_filter)filter);
}

inline const fptu_field *next(const fptu_field *from, const fptu_field *end,
                              unsigned column, fptu_type type) {
  assert(type <= fptu_array_nested);
  return fptu_next(from, end, column, (fptu_type_or_filter)type);
}
inline const fptu_field *next(const fptu_field *from, const fptu_field *end,
                              unsigned column, fptu_filter filter) {
  assert(filter >= fptu_ffilter);
  return fptu_next(from, end, column, (fptu_type_or_filter)filter);
}

inline const fptu_field *first(const fptu_field *begin, const fptu_field *end,
                               fptu_field_filter filter, void *context,
                               void *param) {
  return fptu_first_ex(begin, end, filter, context, param);
}

inline const fptu_field *next(const fptu_field *begin, const fptu_field *end,
                              fptu_field_filter filter, void *context,
                              void *param) {
  return fptu_next_ex(begin, end, filter, context, param);
}

inline size_t field_count(const fptu_ro &ro, unsigned column, fptu_type type) {
  assert(type <= fptu_array_nested);
  return fptu_field_count_ro(ro, column, (fptu_type_or_filter)type);
}
inline size_t field_count(const fptu_ro &ro, unsigned column,
                          fptu_filter filter) {
  assert(filter >= fptu_ffilter);
  return fptu_field_count_ro(ro, column, (fptu_type_or_filter)filter);
}

inline size_t field_count(const fptu_rw *rw, unsigned column, fptu_type type) {
  assert(type <= fptu_array_nested);
  return fptu_field_count_rw(rw, column, (fptu_type_or_filter)type);
}
inline size_t field_count(const fptu_rw *rw, unsigned column,
                          fptu_filter filter) {
  assert(filter >= fptu_ffilter);
  return fptu_field_count_rw(rw, column, (fptu_type_or_filter)filter);
}

inline size_t field_count(const fptu_rw *rw, fptu_field_filter filter,
                          void *context, void *param) {
  return fptu_field_count_rw_ex(rw, filter, context, param);
}

inline size_t field_count(const fptu_ro &ro, fptu_field_filter filter,
                          void *context, void *param) {
  return fptu_field_count_ro_ex(ro, filter, context, param);
}

inline size_t check_and_get_buffer_size(const fptu_ro &ro, unsigned more_items,
                                        unsigned more_payload,
                                        const char **error) {
  return fptu_check_and_get_buffer_size(ro, more_items, more_payload, error);
}

inline size_t get_buffer_size(const fptu_ro &ro, unsigned more_items,
                              unsigned more_payload) {
  return fptu_get_buffer_size(ro, more_items, more_payload);
}

static inline int64_t cast_wide(int8_t value) { return value; }
static inline int64_t cast_wide(int16_t value) { return value; }
static inline int64_t cast_wide(int32_t value) { return value; }
static inline int64_t cast_wide(int64_t value) { return value; }
static inline uint64_t cast_wide(uint8_t value) { return value; }
static inline uint64_t cast_wide(uint16_t value) { return value; }
static inline uint64_t cast_wide(uint32_t value) { return value; }
static inline uint64_t cast_wide(uint64_t value) { return value; }
static inline double_t cast_wide(float value) { return value; }
static inline double_t cast_wide(double value) { return value; }
#if FLT_EVAL_METHOD > 1
static inline double_t cast_wide(double_t /*long double*/ value) {
  return value;
}
#endif

template <typename VALUE_TYPE, typename RANGE_BEGIN_TYPE,
          typename RANGE_END_TYPE>
inline bool is_within(VALUE_TYPE value, RANGE_BEGIN_TYPE begin,
                      RANGE_END_TYPE end) {
  return is_within(cast_wide(value), cast_wide(begin), cast_wide(end));
}

template <>
inline bool is_within<int64_t, int64_t, int64_t>(int64_t value, int64_t begin,
                                                 int64_t end) {
  assert(begin < end);
  return value >= begin && value <= end;
}

template <>
inline bool is_within<uint64_t, uint64_t, uint64_t>(uint64_t value,
                                                    uint64_t begin,
                                                    uint64_t end) {
  assert(begin < end);
  return value >= begin && value <= end;
}

template <>
inline bool is_within<double_t, double_t, double_t>(double_t value,
                                                    double_t begin,
                                                    double_t end) {
  assert(begin < end);
  return value >= begin && value <= end;
}

template <>
inline bool is_within<uint64_t, int64_t, int64_t>(uint64_t value, int64_t begin,
                                                  int64_t end) {
  assert(begin < end);
  if (end < 0 || value > (uint64_t)end)
    return false;
  if (begin > 0 && value < (uint64_t)begin)
    return false;
  return true;
}

template <>
inline bool is_within<uint64_t, double_t, double_t>(uint64_t value,
                                                    double_t begin,
                                                    double_t end) {
  assert(begin < end);
  if (end < 0 || (end < (double_t)UINT64_MAX && value > (uint64_t)end))
    return false;
  if (begin > 0 && (begin > (double_t)UINT64_MAX || value < (uint64_t)begin))
    return false;
  return true;
}

template <>
inline bool is_within<int64_t, double_t, double_t>(int64_t value,
                                                   double_t begin,
                                                   double_t end) {
  assert(begin < end);
  if (end < (double_t)INT64_MAX && value > (int64_t)end)
    return false;
  if (begin > (double_t)INT64_MAX || value < (int64_t)begin)
    return false;
  return true;
}

template <>
inline bool is_within<int64_t, uint64_t, uint64_t>(int64_t value,
                                                   uint64_t begin,
                                                   uint64_t end) {
  assert(begin < end);
  if (value < 0)
    return false;
  return is_within((uint64_t)value, begin, end);
}

template <>
inline bool is_within<double_t, int64_t, int64_t>(double_t value, int64_t begin,
                                                  int64_t end) {
  assert(begin < end);
  return value >= begin && value <= end;
}

template <>
inline bool is_within<double_t, uint64_t, uint64_t>(double_t value,
                                                    uint64_t begin,
                                                    uint64_t end) {
  assert(begin < end);
  return value >= begin && value <= end;
}

template <fptu_type field_type, typename RESULT_TYPE>
static RESULT_TYPE get_number(const fptu_field *field) {
  assert(field != nullptr);
  static_assert(fptu_any_number & (INT32_C(1) << field_type),
                "field_type must be numerical");
  assert(field->tag);
  switch (field_type) {
  default:
    assert(false);
    return 0;
  case fptu_uint16:
    assert(is_within(field->get_payload_uint16(),
                     std::numeric_limits<RESULT_TYPE>::lowest(),
                     std::numeric_limits<RESULT_TYPE>::max()));
    return (RESULT_TYPE)field->get_payload_uint16();
  case fptu_uint32:
    assert(is_within(field->payload()->u32,
                     std::numeric_limits<RESULT_TYPE>::lowest(),
                     std::numeric_limits<RESULT_TYPE>::max()));
    return (RESULT_TYPE)field->payload()->u32;
  case fptu_uint64:
    assert(is_within(field->payload()->u64,
                     std::numeric_limits<RESULT_TYPE>::lowest(),
                     std::numeric_limits<RESULT_TYPE>::max()));
    return (RESULT_TYPE)field->payload()->u64;
  case fptu_int32:
    assert(is_within(field->payload()->i32,
                     std::numeric_limits<RESULT_TYPE>::lowest(),
                     std::numeric_limits<RESULT_TYPE>::max()));
    return (RESULT_TYPE)field->payload()->i32;
  case fptu_int64:
    assert(is_within(field->payload()->i64,
                     std::numeric_limits<RESULT_TYPE>::lowest(),
                     std::numeric_limits<RESULT_TYPE>::max()));
    return (RESULT_TYPE)field->payload()->i64;
  case fptu_fp32:
    assert(is_within(field->payload()->fp32,
                     std::numeric_limits<RESULT_TYPE>::lowest(),
                     std::numeric_limits<RESULT_TYPE>::max()));
    return (RESULT_TYPE)field->payload()->fp32;
  case fptu_fp64:
    assert(is_within(field->payload()->fp32,
                     std::numeric_limits<RESULT_TYPE>::lowest(),
                     std::numeric_limits<RESULT_TYPE>::max()));
    return (RESULT_TYPE)field->payload()->fp64;
  }
}

template <fptu_type field_type, typename VALUE_TYPE>
static void set_number(fptu_field *field, const VALUE_TYPE &value) {
  assert(field != nullptr);
  static_assert(fptu_any_number & (INT32_C(1) << field_type),
                "field_type must be numerical");
  switch (field_type) {
  default:
    assert(false);
    break;
  case fptu_uint16:
    assert(is_within(value, 0, INT16_MAX));
    field->offset = (uint16_t)value;
    break;
  case fptu_uint32:
    assert(is_within(value, 0u, UINT32_MAX));
    field->payload()->u32 = (uint32_t)value;
    break;
  case fptu_uint64:
    assert(is_within(value, 0u, UINT64_MAX));
    field->payload()->u64 = (uint64_t)value;
    break;
  case fptu_int32:
    assert(is_within(value, INT32_MIN, INT32_MAX));
    field->payload()->i32 = (int32_t)value;
    break;
  case fptu_int64:
    assert(is_within(value, INT64_MIN, INT64_MAX));
    field->payload()->i64 = (int64_t)value;
    break;
  case fptu_fp32:
    assert(value >= FLT_MIN && value <= FLT_MAX);
    field->payload()->fp32 = (float)value;
    break;
  case fptu_fp64:
    assert(value >= DBL_MIN && value <= DBL_MAX);
    field->payload()->fp64 = (double)value;
    break;
  }
}

template <fptu_type field_type, typename VALUE_TYPE>
static int upsert_number(fptu_rw *pt, unsigned colnum,
                         const VALUE_TYPE &value) {
  static_assert(fptu_any_number & (INT32_C(1) << field_type),
                "field_type must be numerical");
  switch (field_type) {
  default:
    assert(false);
    return 0;
  case fptu_uint16:
    assert(is_within(value, 0, INT16_MAX));
    return fptu_upsert_uint16(pt, colnum, (uint_fast16_t)value);
  case fptu_uint32:
    assert(is_within(value, 0u, UINT32_MAX));
    return fptu_upsert_uint32(pt, colnum, (uint_fast32_t)value);
  case fptu_uint64:
    assert(is_within(value, 0u, UINT64_MAX));
    return fptu_upsert_uint64(pt, colnum, (uint_fast64_t)value);
  case fptu_int32:
    assert(is_within(value, INT32_MIN, INT32_MAX));
    return fptu_upsert_int32(pt, colnum, (int_fast32_t)value);
  case fptu_int64:
    assert(is_within(value, INT64_MIN, INT64_MAX));
    return fptu_upsert_int64(pt, colnum, (int_fast64_t)value);
  case fptu_fp32:
    assert(value >= FLT_MIN && value <= FLT_MAX);
    return fptu_upsert_fp32(pt, colnum, (float_t)value);
  case fptu_fp64:
    assert(value >= DBL_MIN && value <= DBL_MAX);
    return fptu_upsert_fp64(pt, colnum, (double_t)value);
  }
}

FPTU_API int tuple2json(const fptu_ro &tuple, fptu_emit_func output,
                        void *output_ctx, const string_view &indent,
                        unsigned depth, const void *schema_ctx,
                        fptu_tag2name_func tag2name,
                        fptu_value2enum_func value2enum,
                        const fptu_json_options options = fptu_json_default);

inline int tuple2json(const fptu_ro &tuple, FILE *file, const char *indent,
                      unsigned depth, const void *schema_ctx,
                      fptu_tag2name_func tag2name,
                      fptu_value2enum_func value2enum,
                      const fptu_json_options options = fptu_json_default) {

  return fptu_tuple2json_FILE(tuple, file, indent, depth, schema_ctx, tag2name,
                              value2enum, options);
}

/* Сериализует JSON-представление кортежа в std::ostream.
 *
 * Назначение параметров indent, depth, schema_ctx, tag2name и value2enum
 * см в описании функции fptu_tuple2json().
 *
 * Ошибки std::ostream обрабатываются в соответствии
 * с установками https://en.cppreference.com/w/cpp/io/basic_ios/exceptions,
 * если это не приводит к вбросу исключений, то возвращается -1.
 * При ошибках fptu возвращается соответствующий код ошибки.
 * При успешном завершении возвращается FPTU_SUCCESS (нуль). */
FPTU_API int tuple2json(const fptu_ro &tuple, std::ostream &stream,
                        const string_view &indent, unsigned depth,
                        const void *schema_ctx, fptu_tag2name_func tag2name,
                        fptu_value2enum_func value2enum,
                        const fptu_json_options options = fptu_json_default);

/* Сериализует JSON-представление кортежа в std::string и возвращает результат.
 *
 * Назначение параметров indent, depth, schema_ctx, tag2name и value2enum
 * см в описании функции fptu_tuple2json().
 *
 * При ошибках вбрасывает исключения, включая fptu::bad_tuple */
FPTU_API std::string
tuple2json(const fptu_ro &tuple, const string_view &indent, unsigned depth,
           const void *schema_ctx, fptu_tag2name_func tag2name,
           fptu_value2enum_func value2enum,
           const fptu_json_options options = fptu_json_default);

} /* namespace fptu */

static __inline fptu_error fptu_upsert_string(fptu_rw *pt, unsigned column,
                                              const std::string &value) {
  return fptu_upsert_string(pt, column, value.data(), value.size());
}

static __inline fptu_error fptu_insert_string(fptu_rw *pt, unsigned column,
                                              const std::string &value) {
  return fptu_insert_string(pt, column, value.data(), value.size());
}

static __inline fptu_error fptu_update_string(fptu_rw *pt, unsigned column,
                                              const std::string &value) {
  return fptu_update_string(pt, column, value.data(), value.size());
}

#if HAVE_cxx17_std_string_view
static __inline fptu_error fptu_update_string(fptu_rw *pt, unsigned column,
                                              const std::string_view &value) {
  return fptu_update_string(pt, column, value.data(), value.size());
}
#endif /* HAVE_cxx17_std_string_view */

static __inline fptu_error fptu_update_string(fptu_rw *pt, unsigned column,
                                              const fptu::string_view &value) {
  return fptu_update_string(pt, column, value.data(), value.size());
}

unsigned fptu_field::colnum() const { return fptu_get_colnum(this->tag); }

fptu_type fptu_field::type() const { return fptu_get_type(this->tag); }

bool fptu_field::is_dead() const { return fptu_field_is_dead(this); }

bool fptu_field::is_fixedsize() const {
  return fptu_tag_is_fixedsize(this->tag);
}

uint_fast16_t fptu_field::get_payload_uint16() const {
  assert(type() == fptu_uint16);
  return offset;
}

const fptu_payload *fptu_field::payload() const {
  return fptu_get_payload(this);
}

fptu_payload *fptu_field::payload() { return fptu_field_payload(this); }

const void *fptu_field::inner_begin() const {
  assert((type() & fptu_farray) != 0);
  return payload()->inner_begin();
}

const void *fptu_field::inner_end() const {
  assert((type() & fptu_farray) != 0);
  return payload()->inner_end();
}

size_t fptu_field::array_length() const {
  assert((type() & fptu_farray) != 0);
  return payload()->array_length();
}

namespace std {

template <> struct hash<fptu::string_view> {
  cxx14_constexpr std::size_t operator()(fptu::string_view const &v) const {
    return v.hash_value();
  }
};

inline ostream &operator<<(ostream &out, fptu::string_view &sv) {
  return out.write(sv.data(), sv.size());
}

inline ostream &operator<<(ostream &out, const fptu::output_hexadecimal ones) {
  return fptu::hexadecimal_dump(out, ones.data, ones.length);
}

FPTU_API ostream &operator<<(ostream &out, const fptu_error);
FPTU_API ostream &operator<<(ostream &out, const fptu_type);
FPTU_API ostream &operator<<(ostream &out, const fptu_field &);
FPTU_API ostream &operator<<(ostream &out, const fptu_rw &);
FPTU_API ostream &operator<<(ostream &out, const fptu_ro &);
FPTU_API ostream &operator<<(ostream &out, const fptu_lge);
FPTU_API ostream &operator<<(ostream &out, const fptu_time &);

FPTU_API string to_string(const fptu_error);
FPTU_API string to_string(const fptu_type);
FPTU_API string to_string(const fptu_field &);
FPTU_API string to_string(const fptu_rw &);
FPTU_API string to_string(const fptu_ro &);
FPTU_API string to_string(const fptu_lge);
FPTU_API string to_string(const fptu_time &);
} /* namespace std */

inline bool operator>(const fptu::string_view &a, const std::string &b) {
  return fptu::string_view::compare(a, b) > 0;
}
inline bool operator>=(const fptu::string_view &a, const std::string &b) {
  return fptu::string_view::compare(a, b) >= 0;
}
inline bool operator<(const fptu::string_view &a, const std::string &b) {
  return fptu::string_view::compare(a, b) < 0;
}
inline bool operator<=(const fptu::string_view &a, const std::string &b) {
  return fptu::string_view::compare(a, b) <= 0;
}
inline bool operator==(const fptu::string_view &a, const std::string &b) {
  return fptu::string_view::compare(a, b) == 0;
}
inline bool operator!=(const fptu::string_view &a, const std::string &b) {
  return fptu::string_view::compare(a, b) != 0;
}

inline bool operator>(const std::string &a, const fptu::string_view &b) {
  return fptu::string_view::compare(a, b) > 0;
}
inline bool operator>=(const std::string &a, const fptu::string_view &b) {
  return fptu::string_view::compare(a, b) >= 0;
}
inline bool operator<(const std::string &a, const fptu::string_view &b) {
  return fptu::string_view::compare(a, b) < 0;
}
inline bool operator<=(const std::string &a, const fptu::string_view &b) {
  return fptu::string_view::compare(a, b) <= 0;
}
inline bool operator==(const std::string &a, const fptu::string_view &b) {
  return fptu::string_view::compare(a, b) == 0;
}
inline bool operator!=(const std::string &a, const fptu::string_view &b) {
  return fptu::string_view::compare(a, b) != 0;
}

/* Явно удаляем лишенные смысла операции, в том числе для выявления ошибок */
bool operator>(const fptu_lge &, const fptu_lge &) = delete;
bool operator>=(const fptu_lge &, const fptu_lge &) = delete;
bool operator<(const fptu_lge &, const fptu_lge &) = delete;
bool operator<=(const fptu_lge &, const fptu_lge &) = delete;
#endif /* __cplusplus */

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* FAST_POSITIVE_TUPLES_H */
