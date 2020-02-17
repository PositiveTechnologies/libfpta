﻿/*
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

#pragma once

#ifdef _MSC_VER
#if defined(_MSC_VER)
#define _STL_WARNING_LEVEL 3
#endif
#pragma warning(push, 1)
#pragma warning(disable : 4548) /* expression before comma has no effect;      \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                   semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling  \
                                   mode specified; termination on exception    \
                                   is not guaranteed. Specify /EHsc */
#endif

#if defined(__KERNEL__) || !defined(__cplusplus) || __cplusplus < 201103L
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#else
#include <cassert>
#include <cstddef>
#include <cstdint>
#endif

#if defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__) ||     \
    defined(__BSD__) || defined(__NETBSD__) || defined(__bsdi__) ||            \
    defined(__DragonFly__)
#include </usr/include/sys/cdefs.h>
#endif /* BSD */

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#ifndef __GNUC_PREREQ
#if defined(__GNUC__) && defined(__GNUC_MINOR__)
#define __GNUC_PREREQ(maj, min)                                                \
  ((__GNUC__ << 16) + __GNUC_MINOR__ >= ((maj) << 16) + (min))
#else
#define __GNUC_PREREQ(maj, min) (0)
#endif
#endif /* __GNUC_PREREQ */

#ifndef __CLANG_PREREQ
#ifdef __clang__
#define __CLANG_PREREQ(maj, min)                                               \
  ((__clang_major__ << 16) + __clang_minor__ >= ((maj) << 16) + (min))
#else
#define __CLANG_PREREQ(maj, min) (0)
#endif
#endif /* __CLANG_PREREQ */

#ifndef __GLIBC_PREREQ
#if defined(__GLIBC__) && defined(__GLIBC_MINOR__)
#define __GLIBC_PREREQ(maj, min)                                               \
  ((__GLIBC__ << 16) + __GLIBC_MINOR__ >= ((maj) << 16) + (min))
#else
#define __GLIBC_PREREQ(maj, min) (0)
#endif
#endif /* __GLIBC_PREREQ */

#ifndef __has_attribute
#define __has_attribute(x) (0)
#endif

#ifndef __has_feature
#define __has_feature(x) (0)
#endif

#ifndef __has_extension
#define __has_extension(x) (0)
#endif

#ifndef __has_builtin
#define __has_builtin(x) (0)
#endif

#ifndef __has_warning
#define __has_warning(x) (0)
#endif

#ifndef __has_include
#define __has_include(x) (0)
#endif

#ifndef __has_cpp_attribute
#define __has_cpp_attribute(x) (0)
#endif

#if __has_feature(thread_sanitizer)
#define __SANITIZE_THREAD__ 1
#endif

#if __has_feature(address_sanitizer)
#define __SANITIZE_ADDRESS__ 1
#endif

#if !defined(__cplusplus) && (HAVE_STDALIGN_H || __has_include(<stdalign.h>))
#include <stdalign.h>
#endif

//------------------------------------------------------------------------------

#if !defined(__STDC_VERSION__) || __STDC_VERSION__ < 199901
#if __GNUC_PREREQ(2, 0) || defined(__clang__) || defined(_MSC_VER)
#define __func__ __FUNCTION__
#else
#define __func__ "__func__"
#endif
#endif /* __func__ */

#ifndef __extern_C
#ifdef __cplusplus
#define __extern_C extern "C"
#else
#define __extern_C
#endif
#endif /* __extern_C */

#ifndef __cplusplus
#ifndef bool
#define bool _Bool
#endif
#ifndef true
#define true 1
#endif
#ifndef false
#define false 0
#endif
#endif

#ifndef __fallthrough
#if __has_cpp_attribute(fallthrough)
#define __fallthrough [[fallthrough]]
#elif __GNUC_PREREQ(8, 0) && defined(__cplusplus) && __cplusplus >= 201103L
#define __fallthrough [[fallthrough]]
#elif __GNUC_PREREQ(7, 0)
#define __fallthrough __attribute__((__fallthrough__))
#elif defined(__clang__) && defined(__cplusplus) && __cplusplus >= 201103L &&  \
    __has_feature(cxx_attributes) && __has_warning("-Wimplicit-fallthrough")
#define __fallthrough [[clang::fallthrough]]
#else
#define __fallthrough
#endif
#endif /* __fallthrough */

#if !defined(nullptr) && (!defined(__cplusplus) || __cplusplus < 201103L)
#define nullptr NULL
#endif /* nullptr */

#if !defined(noexcept) && (!defined(__cplusplus) || __cplusplus < 201103L)
#define noexcept
#endif /* noexcept */

#if !defined(constexpr) && (!defined(__cplusplus) || __cplusplus < 201103L)
#define constexpr
#endif /* constexpr */

#if !defined(cxx14_constexpr)
#if defined(__cplusplus) && __cplusplus >= 201402L &&                          \
    (!defined(_MSC_VER) || _MSC_VER >= 1910) &&                                \
    (!defined(__GNUC__) || defined(__clang__) || __GNUC__ >= 6)
#define cxx14_constexpr constexpr
#else
#define cxx14_constexpr
#endif
#endif /* cxx14_constexpr */

#if !defined(cxx17_constexpr)
#if defined(__cplusplus) && __cplusplus >= 201703L &&                          \
    (!defined(_MSC_VER) || _MSC_VER >= 1915) &&                                \
    (!defined(__GNUC__) || defined(__clang__) || __GNUC__ >= 7)
#define cxx17_constexpr constexpr
#define cxx17_noexcept noexcept
#define if_constexpr if constexpr
#else
#define cxx17_constexpr
#define cxx17_noexcept
#define if_constexpr if
#endif
#endif /* cxx17_constexpr */

#if !defined(constexpr_assert) && defined(__cplusplus)
#if defined(HAS_RELAXED_CONSTEXPR) ||                                          \
    (__cplusplus >= 201408L && (!defined(_MSC_VER) || _MSC_VER >= 1915) &&     \
     (!defined(__GNUC__) || defined(__clang__) || __GNUC__ >= 6))
#define constexpr_assert(cond) assert(cond)
#else
#define constexpr_assert(foo)
#endif
#endif /* constexpr_assert */

#ifndef NDEBUG_CONSTEXPR
#ifdef NDEBUG
#define NDEBUG_CONSTEXPR constexpr
#else
#define NDEBUG_CONSTEXPR
#endif
#endif /* NDEBUG_CONSTEXPR */

/* Crutch for case when OLD GLIBC++ (without std::max_align_t)
 * is coupled with MODERN C++ COMPILER (with __cpp_aligned_new) */
#ifndef ERTHINK_PROVIDE_ALIGNED_NEW
#if defined(__cpp_aligned_new) &&                                              \
    (!defined(__GLIBCXX__) || defined(_GLIBCXX_HAVE_ALIGNED_ALLOC))
#define ERTHINK_PROVIDE_ALIGNED_NEW 1
#else
#define ERTHINK_PROVIDE_ALIGNED_NEW 0
#endif
#endif /* ERTHINK_PROVIDE_ALIGNED_NEW */

#ifndef ERTHINK_NAME_PREFIX
#ifdef __cplusplus
#define ERTHINK_NAME_PREFIX(NAME) NAME
#else
#define ERTHINK_NAME_PREFIX(NAME) erthink_##NAME
#endif
#endif /* ERTHINK_NAME_PREFIX */

#ifndef constexpr_intrin
#ifdef __GNUC__
#define constexpr_intrin constexpr
#else
#define constexpr_intrin
#endif
#endif /* constexpr_intrin */

//------------------------------------------------------------------------------

#if defined(__GNUC__) || __has_attribute(__format__)
#define __printf_args(format_index, first_arg)                                 \
  __attribute__((__format__(printf, format_index, first_arg)))
#else
#define __printf_args(format_index, first_arg)
#endif

#if !defined(__thread) && (defined(_MSC_VER) || defined(__DMC__))
#define __thread __declspec(thread)
#endif /* __thread */

#ifndef __always_inline
#if defined(__GNUC__) || __has_attribute(__always_inline__)
#define __always_inline __inline __attribute__((__always_inline__))
#elif defined(_MSC_VER)
#define __always_inline __forceinline
#else
#define __always_inline
#endif
#endif /* __always_inline */

#ifndef __must_check_result
#if defined(__GNUC__) || __has_attribute(__warn_unused_result__)
#define __must_check_result __attribute__((__warn_unused_result__))
#else
#define __must_check_result
#endif
#endif /* __must_check_result */

#ifndef __deprecated
#if defined(__GNUC__) || __has_attribute(__deprecated__)
#define __deprecated __attribute__((__deprecated__))
#elif defined(_MSC_VER)
#define __deprecated __declspec(__deprecated__)
#else
#define __deprecated
#endif
#endif /* __deprecated */

#ifndef __noreturn
#if defined(__GNUC__) || __has_attribute(__noreturn__)
#define __noreturn __attribute__((__noreturn__))
#elif defined(_MSC_VER)
#define __noreturn __declspec(noreturn)
#else
#define __noreturn
#endif
#endif /* __noreturn */

#ifndef __nothrow
#if defined(__cplusplus)
#if __cplusplus < 201703L
#define __nothrow throw()
#else
#define __nothrow noexcept(true)
#endif /* __cplusplus */
#elif defined(__GNUC__) || __has_attribute(__nothrow__)
#define __nothrow __attribute__((__nothrow__))
#elif defined(_MSC_VER) && defined(__cplusplus)
#define __nothrow __declspec(nothrow)
#else
#define __nothrow
#endif
#endif /* __nothrow */

#ifndef __pure_function
/* Many functions have no effects except the return value and their
 * return value depends only on the parameters and/or global variables.
 * Such a function can be subject to common subexpression elimination
 * and loop optimization just as an arithmetic operator would be.
 * These functions should be declared with the attribute pure. */
#if defined(__GNUC__) || __has_attribute(__pure__)
#define __pure_function __attribute__((__pure__))
#else
#define __pure_function
#endif
#endif /* __pure_function */

#ifndef __const_function
/* Many functions do not examine any values except their arguments,
 * and have no effects except the return value. Basically this is just
 * slightly more strict class than the PURE attribute, since function
 * is not allowed to read global memory.
 *
 * Note that a function that has pointer arguments and examines the
 * data pointed to must not be declared const. Likewise, a function
 * that calls a non-const function usually must not be const.
 * It does not make sense for a const function to return void. */
#if defined(__GNUC__) || __has_attribute(__const__)
#define __const_function __attribute__((__const__))
#else
#define __const_function
#endif
#endif /* __const_function */

#ifndef __optimize
#if defined(__OPTIMIZE__)
#if defined(__clang__) && !__has_attribute(__optimize__)
#define __optimize(ops)
#elif defined(__GNUC__) || __has_attribute(__optimize__)
#define __optimize(ops) __attribute__((__optimize__(ops)))
#else
#define __optimize(ops)
#endif
#else
#define __optimize(ops)
#endif
#endif /* __optimize */

#ifndef __hot
#if defined(__OPTIMIZE__)
#if defined(__e2k__)
#define __hot __attribute__((__hot__)) __optimize(3)
#elif defined(__clang__) && !__has_attribute(__hot__) &&                       \
    __has_attribute(__section__) &&                                            \
    (defined(__linux__) || defined(__gnu_linux__))
/* just put frequently used functions in separate section */
#define __hot __attribute__((__section__("text.hot"))) __optimize("O3")
#elif defined(__GNUC__) || __has_attribute(__hot__)
#define __hot __attribute__((__hot__)) __optimize("O3")
#else
#define __hot __optimize("O3")
#endif
#else
#define __hot
#endif
#endif /* __hot */

#ifndef __cold
#if defined(__OPTIMIZE__)
#if defined(__e2k__)
#define __cold __optimize(1) __attribute__((__cold__))
#elif defined(__clang__) && !__has_attribute(__cold__) &&                      \
    __has_attribute(__section__) &&                                            \
    (defined(__linux__) || defined(__gnu_linux__))
/* just put infrequently used functions in separate section */
#define __cold __attribute__((__section__("text.unlikely"))) __optimize("Os")
#elif defined(__GNUC__) || __has_attribute(__cold__)
#define __cold __attribute__((__cold__)) __optimize("Os")
#else
#define __cold __optimize("Os")
#endif
#else
#define __cold
#endif
#endif /* __cold */

#ifndef __flatten
#if defined(__OPTIMIZE__) && (defined(__GNUC__) || __has_attribute(__flatten__))
#define __flatten __attribute__((__flatten__))
#else
#define __flatten
#endif
#endif /* __flatten */

#ifndef __noinline
#if defined(__GNUC__) || __has_attribute(__noinline__)
#define __noinline __attribute__((__noinline__))
#elif defined(_MSC_VER)
#define __noinline __declspec(noinline)
#elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
#define __noinline inline
#elif !defined(__INTEL_COMPILER)
#define __noinline /* FIXME ? */
#endif
#endif /* __noinline */

#ifndef __maybe_unused
#if defined(__GNUC__) || __has_attribute(__unused__)
#define __maybe_unused __attribute__((__unused__))
#else
#define __maybe_unused
#endif
#endif /* __maybe_unused */

//------------------------------------------------------------------------------

#ifndef __hidden
#if defined(__GNUC__) || __has_attribute(__visibility__)
#define __hidden __attribute__((__visibility__("hidden")))
#else
#define __hidden
#endif
#endif /* __hidden */

#ifndef __dll_export
#if defined(_WIN32) || defined(_WIN64) || defined(__CYGWIN__)
#if defined(__GNUC__) || __has_attribute(dllexport)
#define __dll_export __attribute__((dllexport))
#else
#define __dll_export __declspec(dllexport)
#endif
#elif defined(__GNUC__) || __has_attribute(__visibility__)
#define __dll_export __attribute__((__visibility__("default")))
#else
#define __dll_export
#endif
#endif /* __dll_export */

#ifndef __dll_import
#if defined(_WIN32) || defined(_WIN64) || defined(__CYGWIN__)
#if defined(__GNUC__) || __has_attribute(dllimport)
#define __dll_import __attribute__((dllimport))
#else
#define __dll_import __declspec(dllimport)
#endif
#elif defined(__GNUC__) || __has_attribute(__visibility__)
#define __dll_import __attribute__((__visibility__("default")))
#else
#define __dll_import
#endif
#endif /* __dll_import */

//----------------------------------------------------------------------------

#if defined(i386) || defined(__386) || defined(__i386) || defined(__i386__) || \
    defined(i486) || defined(__i486) || defined(__i486__) ||                   \
    defined(i586) | defined(__i586) || defined(__i586__) || defined(i686) ||   \
    defined(__i686) || defined(__i686__) || defined(_M_IX86) ||                \
    defined(_X86_) || defined(__THW_INTEL__) || defined(__I86__) ||            \
    defined(__INTEL__) || defined(__x86_64) || defined(__x86_64__) ||          \
    defined(__amd64__) || defined(__amd64) || defined(_M_X64) ||               \
    defined(_M_AMD64) || defined(__IA32__) || defined(__INTEL__)
#ifndef __ia32__
/* LY: define neutral __ia32__ for x86 and x86-64 archs */
#define __ia32__ 1
#endif /* __ia32__ */
#if !defined(__amd64__) && (defined(__x86_64) || defined(__x86_64__) ||        \
                            defined(__amd64) || defined(_M_X64))
/* LY: define trusty __amd64__ for all AMD64/x86-64 arch */
#define __amd64__ 1
#endif /* __amd64__ */
#endif /* all x86 */

#if !defined(__BYTE_ORDER__) || !defined(__ORDER_LITTLE_ENDIAN__) ||           \
    !defined(__ORDER_BIG_ENDIAN__)

/* *INDENT-OFF* */
/* clang-format off */

#if defined(__GLIBC__) || defined(__GNU_LIBRARY__) || defined(__ANDROID__) ||  \
    __has_include(<endian.h>)
#include <endian.h>
#elif defined(__APPLE__) || defined(__MACH__) || defined(__OpenBSD__) ||       \
    __has_include(<machine/endian.h>)
#include <machine/endian.h>
#elif __has_include(<sys/isa_defs.h>)
#include <sys/isa_defs.h>
#elif __has_include(<sys/types.h>) && __has_include(<sys/endian.h>)
#include <sys/endian.h>
#include <sys/types.h>
#elif defined(__bsdi__) || defined(__DragonFly__) || defined(__FreeBSD__) ||   \
    defined(__NETBSD__) || defined(__NetBSD__) ||                              \
    __has_include(<sys/param.h>)
#include <sys/param.h>
#endif /* OS */

/* *INDENT-ON* */
/* clang-format on */

#if defined(__BYTE_ORDER) && defined(__LITTLE_ENDIAN) && defined(__BIG_ENDIAN)
#define __ORDER_LITTLE_ENDIAN__ __LITTLE_ENDIAN
#define __ORDER_BIG_ENDIAN__ __BIG_ENDIAN
#define __BYTE_ORDER__ __BYTE_ORDER
#elif defined(_BYTE_ORDER) && defined(_LITTLE_ENDIAN) && defined(_BIG_ENDIAN)
#define __ORDER_LITTLE_ENDIAN__ _LITTLE_ENDIAN
#define __ORDER_BIG_ENDIAN__ _BIG_ENDIAN
#define __BYTE_ORDER__ _BYTE_ORDER
#else
#define __ORDER_LITTLE_ENDIAN__ 1234
#define __ORDER_BIG_ENDIAN__ 4321

#if defined(__LITTLE_ENDIAN__) ||                                              \
    (defined(_LITTLE_ENDIAN) && !defined(_BIG_ENDIAN)) ||                      \
    defined(__ARMEL__) || defined(__THUMBEL__) || defined(__AARCH64EL__) ||    \
    defined(__MIPSEL__) || defined(_MIPSEL) || defined(__MIPSEL) ||            \
    defined(_M_ARM) || defined(_M_ARM64) || defined(__e2k__) ||                \
    defined(__elbrus_4c__) || defined(__elbrus_8c__) || defined(__bfin__) ||   \
    defined(__BFIN__) || defined(__ia64__) || defined(_IA64) ||                \
    defined(__IA64__) || defined(__ia64) || defined(_M_IA64) ||                \
    defined(__itanium__) || defined(__ia32__) || defined(__CYGWIN__) ||        \
    defined(_WIN64) || defined(_WIN32) || defined(__TOS_WIN__) ||              \
    defined(__WINDOWS__)
#define __BYTE_ORDER__ __ORDER_LITTLE_ENDIAN__

#elif defined(__BIG_ENDIAN__) ||                                               \
    (defined(_BIG_ENDIAN) && !defined(_LITTLE_ENDIAN)) ||                      \
    defined(__ARMEB__) || defined(__THUMBEB__) || defined(__AARCH64EB__) ||    \
    defined(__MIPSEB__) || defined(_MIPSEB) || defined(__MIPSEB) ||            \
    defined(__m68k__) || defined(M68000) || defined(__hppa__) ||               \
    defined(__hppa) || defined(__HPPA__) || defined(__sparc__) ||              \
    defined(__sparc) || defined(__370__) || defined(__THW_370__) ||            \
    defined(__s390__) || defined(__s390x__) || defined(__SYSC_ZARCH__)
#define __BYTE_ORDER__ __ORDER_BIG_ENDIAN__

#else
#error __BYTE_ORDER__ should be defined.
#endif /* Arch */

#endif
#endif /* __BYTE_ORDER__ || __ORDER_LITTLE_ENDIAN__ || __ORDER_BIG_ENDIAN__ */

//----------------------------------------------------------------------------

#ifdef __cplusplus
// Define operator overloads to enable bit operations on enum values that are
// used to define flags (based on Microsoft's DEFINE_ENUM_FLAG_OPERATORS).
// In FPTU we sure to that uint_fast32_t is enough for casting.
#define FPT_ENUM_FLAG_OPERATORS(ENUMTYPE)                                      \
  extern "C++" {                                                               \
  inline ENUMTYPE operator|(ENUMTYPE a, ENUMTYPE b) {                          \
    return ENUMTYPE(((uint_fast32_t)a) | ((uint_fast32_t)b));                  \
  }                                                                            \
  inline ENUMTYPE &operator|=(ENUMTYPE &a, ENUMTYPE b) {                       \
    return (ENUMTYPE &)(((uint_fast32_t &)a) |= ((uint_fast32_t)b));           \
  }                                                                            \
  inline ENUMTYPE operator&(ENUMTYPE a, ENUMTYPE b) {                          \
    return ENUMTYPE(((uint_fast32_t)a) & ((uint_fast32_t)b));                  \
  }                                                                            \
  inline ENUMTYPE &operator&=(ENUMTYPE &a, ENUMTYPE b) {                       \
    return (ENUMTYPE &)(((uint_fast32_t &)a) &= ((uint_fast32_t)b));           \
  }                                                                            \
  inline ENUMTYPE operator~(ENUMTYPE a) {                                      \
    return ENUMTYPE(~((uint_fast32_t)a));                                      \
  }                                                                            \
  inline ENUMTYPE operator^(ENUMTYPE a, ENUMTYPE b) {                          \
    return ENUMTYPE(((uint_fast32_t)a) ^ ((uint_fast32_t)b));                  \
  }                                                                            \
  inline ENUMTYPE &operator^=(ENUMTYPE &a, ENUMTYPE b) {                       \
    return (ENUMTYPE &)(((uint_fast32_t &)a) ^= ((uint_fast32_t)b));           \
  }                                                                            \
  }
#else                                     /* __cplusplus */
#define FPT_ENUM_FLAG_OPERATORS(ENUMTYPE) /* nope, C allows these operators */
#endif                                    /* !__cplusplus */

/* Workaround for Coverity Scan */
#if defined(__COVERITY__) && __GNUC_PREREQ(7, 0) && !defined(__cplusplus)
typedef float _Float32;
typedef double _Float32x;
typedef double _Float64;
typedef long double _Float64x;
typedef float _Float128 __attribute__((__mode__(__TF__)));
typedef __complex__ float __cfloat128 __attribute__((__mode__(__TC__)));
typedef _Complex float __cfloat128 __attribute__((__mode__(__TC__)));
#endif /* Workaround for Coverity Scan */
