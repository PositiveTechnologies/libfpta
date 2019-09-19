/* mdbx_stat.c - memory-mapped database status tool */

/*
 * Copyright 2015-2019 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>. */

#ifdef _MSC_VER
#if _MSC_VER > 1800
#pragma warning(disable : 4464) /* relative include path contains '..' */
#endif
#pragma warning(disable : 4996) /* The POSIX name is deprecated... */
#endif                          /* _MSC_VER (warnings) */

#define MDBX_TOOLS /* Avoid using internal mdbx_assert() */
/*
 * Copyright 2015-2019 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>. */

#define MDBX_BUILD_SOURCERY f443dd297b5330b154138f0628bb225059ce950d7507fe13c771dc9f2060af5d_v0_3_1_162_gda9dc75f
#ifdef MDBX_CONFIG_H
#include MDBX_CONFIG_H
#endif

/* *INDENT-OFF* */
/* clang-format off */

/* In case the MDBX_DEBUG is undefined set it corresponding to NDEBUG */
#ifndef MDBX_DEBUG
#   ifdef NDEBUG
#       define MDBX_DEBUG 0
#   else
#       define MDBX_DEBUG 1
#   endif
#endif

/* Undefine the NDEBUG if debugging is enforced by MDBX_DEBUG */
#if MDBX_DEBUG
#   undef NDEBUG
#endif

#define MDBX_OSX_WANNA_DURABILITY 0 /* using fcntl(F_FULLFSYNC) with 5-10 times slowdown */
#define MDBX_OSX_WANNA_SPEED 1      /* using fsync() with chance of data lost on power failure */
#ifndef MDBX_OSX_SPEED_INSTEADOF_DURABILITY
#   define MDBX_OSX_SPEED_INSTEADOF_DURABILITY MDBX_OSX_WANNA_DURABILITY
#endif

#ifdef MDBX_ALLOY
/* Amalgamated build */
#   define MDBX_INTERNAL_FUNC static
#   define MDBX_INTERNAL_VAR static
#else
/* Non-amalgamated build */
#   define MDBX_INTERNAL_FUNC
#   define MDBX_INTERNAL_VAR extern
#endif /* MDBX_ALLOY */

/*----------------------------------------------------------------------------*/

/* Should be defined before any includes */
#ifndef _FILE_OFFSET_BITS
#   define _FILE_OFFSET_BITS 64
#endif

#ifdef __APPLE__
#define _DARWIN_C_SOURCE
#endif

#ifdef _MSC_VER
#   if _MSC_VER < 1400
#       error "Microsoft Visual C++ 8.0 (Visual Studio 2005) or later version is required"
#   endif
#   ifndef _CRT_SECURE_NO_WARNINGS
#       define _CRT_SECURE_NO_WARNINGS
#   endif
#if _MSC_VER > 1800
#   pragma warning(disable : 4464) /* relative include path contains '..' */
#endif
#if _MSC_VER > 1913
#   pragma warning(disable : 5045) /* Compiler will insert Spectre mitigation... */
#endif
#pragma warning(disable : 4710) /* 'xyz': function not inlined */
#pragma warning(disable : 4711) /* function 'xyz' selected for automatic inline expansion */
#pragma warning(disable : 4201) /* nonstandard extension used : nameless struct / union */
#pragma warning(disable : 4702) /* unreachable code */
#pragma warning(disable : 4706) /* assignment within conditional expression */
#pragma warning(disable : 4127) /* conditional expression is constant */
#pragma warning(disable : 4324) /* 'xyz': structure was padded due to alignment specifier */
#pragma warning(disable : 4310) /* cast truncates constant value */
#pragma warning(disable : 4820) /* bytes padding added after data member for aligment */
#pragma warning(disable : 4548) /* expression before comma has no effect; expected expression with side - effect */
#pragma warning(disable : 4366) /* the result of the unary '&' operator may be unaligned */
#endif                          /* _MSC_VER (warnings) */

#include "mdbx.h"
/*
 * Copyright 2015-2019 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

/* *INDENT-OFF* */
/* clang-format off */

#ifndef __GNUC_PREREQ
#   if defined(__GNUC__) && defined(__GNUC_MINOR__)
#       define __GNUC_PREREQ(maj, min) \
          ((__GNUC__ << 16) + __GNUC_MINOR__ >= ((maj) << 16) + (min))
#   else
#       define __GNUC_PREREQ(maj, min) (0)
#   endif
#endif /* __GNUC_PREREQ */

#ifndef __CLANG_PREREQ
#   ifdef __clang__
#       define __CLANG_PREREQ(maj,min) \
          ((__clang_major__ << 16) + __clang_minor__ >= ((maj) << 16) + (min))
#   else
#       define __CLANG_PREREQ(maj,min) (0)
#   endif
#endif /* __CLANG_PREREQ */

#ifndef __GLIBC_PREREQ
#   if defined(__GLIBC__) && defined(__GLIBC_MINOR__)
#       define __GLIBC_PREREQ(maj, min) \
          ((__GLIBC__ << 16) + __GLIBC_MINOR__ >= ((maj) << 16) + (min))
#   else
#       define __GLIBC_PREREQ(maj, min) (0)
#   endif
#endif /* __GLIBC_PREREQ */

#ifndef __has_attribute
#   define __has_attribute(x) (0)
#endif

#ifndef __has_feature
#   define __has_feature(x) (0)
#endif

#ifndef __has_extension
#   define __has_extension(x) (0)
#endif

#ifndef __has_builtin
#   define __has_builtin(x) (0)
#endif

#ifndef __has_warning
#   define __has_warning(x) (0)
#endif

#ifndef __has_include
#   define __has_include(x) (0)
#endif

#if __has_feature(thread_sanitizer)
#   define __SANITIZE_THREAD__ 1
#endif

#if __has_feature(address_sanitizer)
#   define __SANITIZE_ADDRESS__ 1
#endif

/*----------------------------------------------------------------------------*/

#ifndef __extern_C
#   ifdef __cplusplus
#       define __extern_C extern "C"
#   else
#       define __extern_C
#   endif
#endif /* __extern_C */

#ifndef __cplusplus
#   ifndef bool
#       define bool _Bool
#   endif
#   ifndef true
#       define true (1)
#   endif
#   ifndef false
#       define false (0)
#   endif
#endif

#if !defined(nullptr) && !defined(__cplusplus) || (__cplusplus < 201103L && !defined(_MSC_VER))
#   define nullptr NULL
#endif

/*----------------------------------------------------------------------------*/

#ifndef __always_inline
#   if defined(__GNUC__) || __has_attribute(__always_inline__)
#       define __always_inline __inline __attribute__((__always_inline__))
#   elif defined(_MSC_VER)
#       define __always_inline __forceinline
#   else
#       define __always_inline
#   endif
#endif /* __always_inline */

#ifndef __noinline
#   if defined(__GNUC__) || __has_attribute(__noinline__)
#       define __noinline __attribute__((__noinline__))
#   elif defined(_MSC_VER)
#       define __noinline __declspec(noinline)
#   elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
#       define __noinline inline
#   elif !defined(__INTEL_COMPILER)
#       define __noinline /* FIXME ? */
#   endif
#endif /* __noinline */

#ifndef __must_check_result
#   if defined(__GNUC__) || __has_attribute(__warn_unused_result__)
#       define __must_check_result __attribute__((__warn_unused_result__))
#   else
#       define __must_check_result
#   endif
#endif /* __must_check_result */

#ifndef __maybe_unused
#   if defined(__GNUC__) || __has_attribute(__unused__)
#       define __maybe_unused __attribute__((__unused__))
#   else
#       define __maybe_unused
#   endif
#endif /* __maybe_unused */

#ifndef __deprecated
#   if defined(__GNUC__) || __has_attribute(__deprecated__)
#       define __deprecated __attribute__((__deprecated__))
#   elif defined(_MSC_VER)
#       define __deprecated __declspec(deprecated)
#   else
#       define __deprecated
#   endif
#endif /* __deprecated */

#if !defined(__noop) && !defined(_MSC_VER)
#	ifdef __cplusplus
		static __maybe_unused inline void __noop_consume_args() {}
		template <typename First, typename... Rest>
		static inline void
		__noop_consume_args(const First &first, const Rest &... rest) {
			(void) first; __noop_consume_args(rest...);
		}
#		define __noop(...) __noop_consume_args(__VA_ARGS__)
#	elif defined(__GNUC__) && (!defined(__STRICT_ANSI__) || !__STRICT_ANSI__)
		static __maybe_unused __inline void __noop_consume_args(void* anchor, ...) {
			(void) anchor;
		}
#		define __noop(...) __noop_consume_args(0, ##__VA_ARGS__)
#	else
#		define __noop(...) do {} while(0)
#	endif
#endif /* __noop */

#ifndef __fallthrough
#	if __GNUC_PREREQ(7, 0) || __has_attribute(__fallthrough__)
#		define __fallthrough __attribute__((__fallthrough__))
#	else
#		define __fallthrough __noop()
#	endif
#endif /* __fallthrough */

#ifndef __unreachable
#	if __GNUC_PREREQ(4,5)
#		define __unreachable() __builtin_unreachable()
#	elif defined(_MSC_VER)
#		define __unreachable() __assume(0)
#	else
#		define __unreachable() __noop()
#	endif
#endif /* __unreachable */

#ifndef __prefetch
#	if defined(__GNUC__) || defined(__clang__)
#		define __prefetch(ptr) __builtin_prefetch(ptr)
#	else
#		define __prefetch(ptr) __noop(ptr)
#	endif
#endif /* __prefetch */

#ifndef __noreturn
#   if defined(__GNUC__) || __has_attribute(__noreturn__)
#       define __noreturn __attribute__((__noreturn__))
#   elif defined(_MSC_VER)
#       define __noreturn __declspec(noreturn)
#   else
#       define __noreturn
#   endif
#endif /* __noreturn */

#ifndef __nothrow
#   if defined(__cplusplus)
#       if __cplusplus < 201703L
#           define __nothrow throw()
#       else
#           define __nothrow noexcept(true)
#       endif /* __cplusplus */
#   elif defined(__GNUC__) || __has_attribute(__nothrow__)
#       define __nothrow __attribute__((__nothrow__))
#   elif defined(_MSC_VER) && defined(__cplusplus)
#       define __nothrow __declspec(nothrow)
#   else
#       define __nothrow
#   endif
#endif /* __nothrow */

#ifndef __pure_function
    /* Many functions have no effects except the return value and their
     * return value depends only on the parameters and/or global variables.
     * Such a function can be subject to common subexpression elimination
     * and loop optimization just as an arithmetic operator would be.
     * These functions should be declared with the attribute pure. */
#   if defined(__GNUC__) || __has_attribute(__pure__)
#       define __pure_function __attribute__((__pure__))
#   else
#       define __pure_function
#   endif
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
#   if defined(__GNUC__) || __has_attribute(__const__)
#       define __const_function __attribute__((__const__))
#   else
#       define __const_function
#   endif
#endif /* __const_function */

#ifndef __hidden
#   if defined(__GNUC__) || __has_attribute(__visibility__)
#       define __hidden __attribute__((__visibility__("hidden")))
#   else
#       define __hidden
#   endif
#endif /* __hidden */

#ifndef __optimize
#   if defined(__OPTIMIZE__)
#     if defined(__clang__) && !__has_attribute(__optimize__)
#           define __optimize(ops)
#       elif defined(__GNUC__) || __has_attribute(__optimize__)
#           define __optimize(ops) __attribute__((__optimize__(ops)))
#       else
#           define __optimize(ops)
#       endif
#   else
#           define __optimize(ops)
#   endif
#endif /* __optimize */

#ifndef __hot
#   if defined(__OPTIMIZE__)
#       if defined(__e2k__)
#           define __hot __attribute__((__hot__)) __optimize(3)
#       elif defined(__clang__) && !__has_attribute(__hot_) \
        && __has_attribute(__section__) && (defined(__linux__) || defined(__gnu_linux__))
            /* just put frequently used functions in separate section */
#           define __hot __attribute__((__section__("text.hot"))) __optimize("O3")
#       elif defined(__GNUC__) || __has_attribute(__hot__)
#           define __hot __attribute__((__hot__)) __optimize("O3")
#       else
#           define __hot  __optimize("O3")
#       endif
#   else
#       define __hot
#   endif
#endif /* __hot */

#ifndef __cold
#   if defined(__OPTIMIZE__)
#       if defined(__e2k__)
#           define __cold __attribute__((__cold__)) __optimize(1)
#       elif defined(__clang__) && !__has_attribute(cold) \
        && __has_attribute(__section__) && (defined(__linux__) || defined(__gnu_linux__))
            /* just put infrequently used functions in separate section */
#           define __cold __attribute__((__section__("text.unlikely"))) __optimize("Os")
#       elif defined(__GNUC__) || __has_attribute(cold)
#           define __cold __attribute__((__cold__)) __optimize("Os")
#       else
#           define __cold __optimize("Os")
#       endif
#   else
#       define __cold
#   endif
#endif /* __cold */

#ifndef __flatten
#   if defined(__OPTIMIZE__) && (defined(__GNUC__) || __has_attribute(__flatten__))
#       define __flatten __attribute__((__flatten__))
#   else
#       define __flatten
#   endif
#endif /* __flatten */

#ifndef likely
#   if (defined(__GNUC__) || defined(__clang__)) && !defined(__COVERITY__)
#       define likely(cond) __builtin_expect(!!(cond), 1)
#   else
#       define likely(x) (x)
#   endif
#endif /* likely */

#ifndef unlikely
#   if (defined(__GNUC__) || defined(__clang__)) && !defined(__COVERITY__)
#       define unlikely(cond) __builtin_expect(!!(cond), 0)
#   else
#       define unlikely(x) (x)
#   endif
#endif /* unlikely */

/* Workaround for Coverity Scan */
#if defined(__COVERITY__) && __GNUC_PREREQ(7, 0) && !defined(__cplusplus)
typedef float _Float32;
typedef double _Float32x;
typedef double _Float64;
typedef long double _Float64x;
typedef float _Float128 __attribute__((__mode__(__TF__)));
typedef __complex__ float __cfloat128 __attribute__ ((__mode__ (__TC__)));
typedef _Complex float __cfloat128 __attribute__ ((__mode__ (__TC__)));
#endif /* Workaround for Coverity Scan */

/* Wrapper around __func__, which is a C99 feature */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L
#   define mdbx_func_ __func__
#elif (defined(__GNUC__) && __GNUC__ >= 2) || defined(__clang__) || defined(_MSC_VER)
#   define mdbx_func_ __FUNCTION__
#else
#   define mdbx_func_ "<mdbx_unknown>"
#endif

#if defined(__GNUC__) || __has_attribute(__format__)
#define __printf_args(format_index, first_arg)                                 \
  __attribute__((__format__(printf, format_index, first_arg)))
#else
#define __printf_args(format_index, first_arg)
#endif

/*----------------------------------------------------------------------------*/

#if defined(USE_VALGRIND)
#   include <valgrind/memcheck.h>
#   ifndef VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE
        /* LY: available since Valgrind 3.10 */
#       define VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#       define VALGRIND_ENABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#   endif
#elif !defined(RUNNING_ON_VALGRIND)
#   define VALGRIND_CREATE_MEMPOOL(h,r,z)
#   define VALGRIND_DESTROY_MEMPOOL(h)
#   define VALGRIND_MEMPOOL_TRIM(h,a,s)
#   define VALGRIND_MEMPOOL_ALLOC(h,a,s)
#   define VALGRIND_MEMPOOL_FREE(h,a)
#   define VALGRIND_MEMPOOL_CHANGE(h,a,b,s)
#   define VALGRIND_MAKE_MEM_NOACCESS(a,s)
#   define VALGRIND_MAKE_MEM_DEFINED(a,s)
#   define VALGRIND_MAKE_MEM_UNDEFINED(a,s)
#   define VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#   define VALGRIND_ENABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#   define VALGRIND_CHECK_MEM_IS_ADDRESSABLE(a,s) (0)
#   define VALGRIND_CHECK_MEM_IS_DEFINED(a,s) (0)
#   define RUNNING_ON_VALGRIND (0)
#endif /* USE_VALGRIND */

#ifdef __SANITIZE_ADDRESS__
#   include <sanitizer/asan_interface.h>
#elif !defined(ASAN_POISON_MEMORY_REGION)
#   define ASAN_POISON_MEMORY_REGION(addr, size) \
        ((void)(addr), (void)(size))
#   define ASAN_UNPOISON_MEMORY_REGION(addr, size) \
        ((void)(addr), (void)(size))
#endif /* __SANITIZE_ADDRESS__ */

/*----------------------------------------------------------------------------*/

#ifndef ARRAY_LENGTH
#   ifdef __cplusplus
        template <typename T, size_t N>
        char (&__ArraySizeHelper(T (&array)[N]))[N];
#       define ARRAY_LENGTH(array) (sizeof(::__ArraySizeHelper(array)))
#   else
#       define ARRAY_LENGTH(array) (sizeof(array) / sizeof(array[0]))
#   endif
#endif /* ARRAY_LENGTH */

#ifndef ARRAY_END
#   define ARRAY_END(array) (&array[ARRAY_LENGTH(array)])
#endif /* ARRAY_END */

#ifndef STRINGIFY
#   define STRINGIFY_HELPER(x) #x
#   define STRINGIFY(x) STRINGIFY_HELPER(x)
#endif /* STRINGIFY */

#define CONCAT(a,b) a##b
#define XCONCAT(a,b) CONCAT(a,b)

#ifndef offsetof
#   define offsetof(type, member)  __builtin_offsetof(type, member)
#endif /* offsetof */

#ifndef container_of
#   define container_of(ptr, type, member) \
        ((type *)((char *)(ptr) - offsetof(type, member)))
#endif /* container_of */

#define MDBX_TETRAD(a, b, c, d)                                                \
  ((uint32_t)(a) << 24 | (uint32_t)(b) << 16 | (uint32_t)(c) << 8 | (d))

#define MDBX_STRING_TETRAD(str) MDBX_TETRAD(str[0], str[1], str[2], str[3])

#define FIXME "FIXME: " __FILE__ ", " STRINGIFY(__LINE__)

#ifndef STATIC_ASSERT_MSG
#   if (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L) \
          || __has_feature(c_static_assert)
#       define STATIC_ASSERT_MSG(expr, msg) _Static_assert(expr, msg)
#   elif defined(static_assert)
#       define STATIC_ASSERT_MSG(expr, msg) static_assert(expr, msg)
#   elif defined(_MSC_VER)
#       include <crtdbg.h>
#       define STATIC_ASSERT_MSG(expr, msg) _STATIC_ASSERT(expr)
#   else
#       define STATIC_ASSERT_MSG(expr, msg) switch (0) {case 0:case (expr):;}
#   endif
#endif /* STATIC_ASSERT */

#ifndef STATIC_ASSERT
#   define STATIC_ASSERT(expr) STATIC_ASSERT_MSG(expr, #expr)
#endif

/* *INDENT-ON* */
/* clang-format on */

#if defined(__GNUC__) && !__GNUC_PREREQ(4,2)
    /* Actualy libmdbx was not tested with compilers older than GCC from RHEL6.
     * But you could remove this #error and try to continue at your own risk.
     * In such case please don't rise up an issues related ONLY to old compilers.
     */
#   warning "libmdbx required GCC >= 4.2"
#endif

#if defined(__clang__) && !__CLANG_PREREQ(3,8)
    /* Actualy libmdbx was not tested with CLANG older than 3.8.
     * But you could remove this #error and try to continue at your own risk.
     * In such case please don't rise up an issues related ONLY to old compilers.
     */
#   warning "libmdbx required CLANG >= 3.8"
#endif

#if defined(__GLIBC__) && !__GLIBC_PREREQ(2,12)
    /* Actualy libmdbx was not tested with something older than glibc 2.12 (from RHEL6).
     * But you could remove this #error and try to continue at your own risk.
     * In such case please don't rise up an issues related ONLY to old systems.
     */
#   warning "libmdbx required at least GLIBC 2.12."
#endif

#ifdef __SANITIZE_THREAD__
#   warning "libmdbx don't compatible with ThreadSanitizer, you will get a lot of false-positive issues."
#endif /* __SANITIZE_THREAD__ */

#if __has_warning("-Wconstant-logical-operand")
#   if defined(__clang__)
#       pragma clang diagnostic ignored "-Wconstant-logical-operand"
#   elif defined(__GNUC__)
#       pragma GCC diagnostic ignored "-Wconstant-logical-operand"
#   else
#      pragma warning disable "constant-logical-operand"
#   endif
#endif /* -Wconstant-logical-operand */

#if defined(__LCC__) && (__LCC__ <= 121)
    /* bug #2798 */
#   pragma diag_suppress alignment_reduction_ignored
#elif defined(__ICC)
#   pragma warning(disable: 3453 1366)
#elif __has_warning("-Walignment-reduction-ignored")
#   if defined(__clang__)
#       pragma clang diagnostic ignored "-Walignment-reduction-ignored"
#   elif defined(__GNUC__)
#       pragma GCC diagnostic ignored "-Walignment-reduction-ignored"
#   else
#       pragma warning disable "alignment-reduction-ignored"
#   endif
#endif /* -Walignment-reduction-ignored */

/* https://en.wikipedia.org/wiki/Operating_system_abstraction_layer */

/*
 * Copyright 2015-2019 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */


/*----------------------------------------------------------------------------*/
/* Microsoft compiler generates a lot of warning for self includes... */

#ifdef _MSC_VER
#pragma warning(push, 1)
#pragma warning(disable : 4548) /* expression before comma has no effect;      \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                 * semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling  \
                                 * mode specified; termination on exception is \
                                 * not guaranteed. Specify /EHsc */
#endif                          /* _MSC_VER (warnings) */

#if defined(_WIN32) || defined(_WIN64)
#if !defined(_CRT_SECURE_NO_WARNINGS)
#define _CRT_SECURE_NO_WARNINGS
#endif
#if !defined(_NO_CRT_STDIO_INLINE) && MDBX_BUILD_SHARED_LIBRARY &&             \
    !defined(MDBX_TOOLS)
#define _NO_CRT_STDIO_INLINE
#endif
#endif /* Windows */

/*----------------------------------------------------------------------------*/
/* C99 includes */
#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <assert.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

/* C11 stdalign.h */
#if __has_include(<stdalign.h>)
#include <stdalign.h>
#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
#define alignas(N) _Alignas(N)
#elif defined(_MSC_VER)
#define alignas(N) __declspec(align(N))
#elif __has_attribute(__aligned__) || defined(__GNUC__)
#define alignas(N) __attribute__((__aligned__(N)))
#else
#error "FIXME: Required _alignas() or equivalent."
#endif

/*----------------------------------------------------------------------------*/
/* Systems includes */

#if defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__) ||     \
    defined(__BSD__) || defined(__NETBSD__) || defined(__bsdi__) ||            \
    defined(__DragonFly__) || defined(__APPLE__) || defined(__MACH__)
#include <sys/cdefs.h>
#else
#include <malloc.h>
#ifndef _POSIX_C_SOURCE
#ifdef _POSIX_SOURCE
#define _POSIX_C_SOURCE 1
#else
#define _POSIX_C_SOURCE 0
#endif
#endif
#endif /* !xBSD */

#if defined(__linux__) || defined(__gnu_linux__)
#include <sys/sendfile.h>
#endif /* Linux */

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 0
#endif

#if defined(_WIN32) || defined(_WIN64)
#define WIN32_LEAN_AND_MEAN
#include <tlhelp32.h>
#include <windows.h>
#include <winnt.h>
#include <winternl.h>
#define HAVE_SYS_STAT_H
#define HAVE_SYS_TYPES_H
typedef HANDLE mdbx_thread_t;
typedef unsigned mdbx_thread_key_t;
#define MDBX_OSAL_SECTION HANDLE
#define MAP_FAILED NULL
#define HIGH_DWORD(v) ((DWORD)((sizeof(v) > 4) ? ((uint64_t)(v) >> 32) : 0))
#define THREAD_CALL WINAPI
#define THREAD_RESULT DWORD
typedef struct {
  HANDLE mutex;
  HANDLE event;
} mdbx_condmutex_t;
typedef CRITICAL_SECTION mdbx_fastmutex_t;

#if MDBX_AVOID_CRT
#ifndef mdbx_malloc
static inline void *mdbx_malloc(size_t bytes) {
  return LocalAlloc(LMEM_FIXED, bytes);
}
#endif /* mdbx_malloc */

#ifndef mdbx_calloc
static inline void *mdbx_calloc(size_t nelem, size_t size) {
  return LocalAlloc(LMEM_FIXED | LMEM_ZEROINIT, nelem * size);
}
#endif /* mdbx_calloc */

#ifndef mdbx_realloc
static inline void *mdbx_realloc(void *ptr, size_t bytes) {
  return LocalReAlloc(ptr, bytes, LMEM_MOVEABLE);
}
#endif /* mdbx_realloc */

#ifndef mdbx_free
#define mdbx_free LocalFree
#endif /* mdbx_free */
#else
#define mdbx_malloc malloc
#define mdbx_calloc calloc
#define mdbx_realloc realloc
#define mdbx_free free
#define mdbx_strdup _strdup
#endif /* MDBX_AVOID_CRT */

#ifndef snprintf
#define snprintf _snprintf /* ntdll */
#endif

#ifndef vsnprintf
#define vsnprintf _vsnprintf /* ntdll */
#endif

#else /*----------------------------------------------------------------------*/

#include <pthread.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>
typedef pthread_t mdbx_thread_t;
typedef pthread_key_t mdbx_thread_key_t;
#define INVALID_HANDLE_VALUE (-1)
#define THREAD_CALL
#define THREAD_RESULT void *
typedef struct {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
} mdbx_condmutex_t;
typedef pthread_mutex_t mdbx_fastmutex_t;
#define mdbx_malloc malloc
#define mdbx_calloc calloc
#define mdbx_realloc realloc
#define mdbx_free free
#define mdbx_strdup strdup
#endif /* Platform */

/* *INDENT-OFF* */
/* clang-format off */
#if defined(HAVE_SYS_STAT_H) || __has_include(<sys/stat.h>)
#include <sys/stat.h>
#endif
#if defined(HAVE_SYS_TYPES_H) || __has_include(<sys/types.h>)
#include <sys/types.h>
#endif
#if defined(HAVE_SYS_FILE_H) || __has_include(<sys/file.h>)
#include <sys/file.h>
#endif
/* *INDENT-ON* */
/* clang-format on */

#ifndef SSIZE_MAX
#define SSIZE_MAX INTPTR_MAX
#endif

#if !defined(MADV_DODUMP) && defined(MADV_CORE)
#define MADV_DODUMP MADV_CORE
#endif /* MADV_CORE -> MADV_DODUMP */

#if !defined(MADV_DONTDUMP) && defined(MADV_NOCORE)
#define MADV_DONTDUMP MADV_NOCORE
#endif /* MADV_NOCORE -> MADV_DONTDUMP */

#ifndef MADV_REMOVE_OR_FREE_OR_DONTNEED
#ifdef MADV_REMOVE
#define MADV_REMOVE_OR_FREE_OR_DONTNEED MADV_REMOVE
#elif defined(MADV_FREE)
#define MADV_REMOVE_OR_FREE_OR_DONTNEED MADV_FREE
#elif defined(MADV_DONTNEED)
#define MADV_REMOVE_OR_FREE_OR_DONTNEED MADV_DONTNEED
#endif
#endif /* MADV_REMOVE_OR_FREE_OR_DONTNEED */

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

#if !defined(UNALIGNED_OK)
#if (defined(__ia32__) || defined(__e2k__) ||                                  \
     defined(__ARM_FEATURE_UNALIGNED)) &&                                      \
    !defined(__ALIGNED__)
#define UNALIGNED_OK 1
#else
#define UNALIGNED_OK 0
#endif
#endif /* UNALIGNED_OK */

#if (-6 & 5) || CHAR_BIT != 8 || UINT_MAX < 0xffffffff || ULONG_MAX % 0xFFFF
#error                                                                         \
    "Sanity checking failed: Two's complement, reasonably sized integer types"
#endif

/*----------------------------------------------------------------------------*/
/* Compiler's includes for builtins/intrinsics */

#if defined(_MSC_VER) || defined(__INTEL_COMPILER)
#include <intrin.h>
#elif __GNUC_PREREQ(4, 4) || defined(__clang__)
#if defined(__ia32__) || defined(__e2k__)
#include <x86intrin.h>
#endif /* __ia32__ */
#if defined(__ia32__)
#include <cpuid.h>
#endif /* __ia32__ */
#elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
#include <mbarrier.h>
#elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC)) &&       \
    (defined(HP_IA64) || defined(__ia64))
#include <machine/sys/inline.h>
#elif defined(__IBMC__) && defined(__powerpc)
#include <atomic.h>
#elif defined(_AIX)
#include <builtins.h>
#include <sys/atomic_op.h>
#elif (defined(__osf__) && defined(__DECC)) || defined(__alpha)
#include <c_asm.h>
#include <machine/builtins.h>
#elif defined(__MWERKS__)
/* CodeWarrior - troubles ? */
#pragma gcc_extensions
#elif defined(__SNC__)
/* Sony PS3 - troubles ? */
#elif defined(__hppa__) || defined(__hppa)
#include <machine/inline.h>
#else
#error Unsupported C compiler, please use GNU C 4.4 or newer
#endif /* Compiler */

/*----------------------------------------------------------------------------*/
/* Byteorder */

#if !defined(__BYTE_ORDER__) || !defined(__ORDER_LITTLE_ENDIAN__) ||           \
    !defined(__ORDER_BIG_ENDIAN__)

/* *INDENT-OFF* */
/* clang-format off */
#if defined(__GLIBC__) || defined(__GNU_LIBRARY__) || defined(__ANDROID__) ||  \
    defined(HAVE_ENDIAN_H) || __has_include(<endian.h>)
#include <endian.h>
#elif defined(__APPLE__) || defined(__MACH__) || defined(__OpenBSD__) ||       \
    defined(HAVE_MACHINE_ENDIAN_H) || __has_include(<machine/endian.h>)
#include <machine/endian.h>
#elif defined(HAVE_SYS_ISA_DEFS_H) || __has_include(<sys/isa_defs.h>)
#include <sys/isa_defs.h>
#elif (defined(HAVE_SYS_TYPES_H) && defined(HAVE_SYS_ENDIAN_H)) ||             \
    (__has_include(<sys/types.h>) && __has_include(<sys/endian.h>))
#include <sys/endian.h>
#include <sys/types.h>
#elif defined(__bsdi__) || defined(__DragonFly__) || defined(__FreeBSD__) ||   \
    defined(__NETBSD__) || defined(__NetBSD__) ||                              \
    defined(HAVE_SYS_PARAM_H) || __has_include(<sys/param.h>)
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

/*----------------------------------------------------------------------------*/
/* Memory/Compiler barriers, cache coherence */

static __maybe_unused __inline void mdbx_compiler_barrier(void) {
#if defined(__clang__) || defined(__GNUC__)
  __asm__ __volatile__("" ::: "memory");
#elif defined(_MSC_VER)
  _ReadWriteBarrier();
#elif defined(__INTEL_COMPILER) /* LY: Intel Compiler may mimic GCC and MSC */
  __memory_barrier();
  if (type > MDBX_BARRIER_COMPILER)
#if defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
    __mf();
#elif defined(__i386__) || defined(__x86_64__)
    _mm_mfence();
#else
#error "Unknown target for Intel Compiler, please report to us."
#endif
#elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
  __compiler_barrier();
#elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC)) &&       \
    (defined(HP_IA64) || defined(__ia64))
  _Asm_sched_fence(/* LY: no-arg meaning 'all expect ALU', e.g. 0x3D3D */);
#elif defined(_AIX) || defined(__ppc__) || defined(__powerpc__) ||             \
    defined(__ppc64__) || defined(__powerpc64__)
  __fence();
#else
#error "Could not guess the kind of compiler, please report to us."
#endif
}

static __maybe_unused __inline void mdbx_memory_barrier(void) {
#if __has_extension(c_atomic) || __has_extension(cxx_atomic)
  __c11_atomic_thread_fence(__ATOMIC_SEQ_CST);
#elif defined(__ATOMIC_SEQ_CST)
  __atomic_thread_fence(__ATOMIC_SEQ_CST);
#elif defined(__clang__) || defined(__GNUC__)
  __sync_synchronize();
#elif defined(_MSC_VER)
  MemoryBarrier();
#elif defined(__INTEL_COMPILER) /* LY: Intel Compiler may mimic GCC and MSC */
#if defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
  __mf();
#elif defined(__i386__) || defined(__x86_64__)
  _mm_mfence();
#else
#error "Unknown target for Intel Compiler, please report to us."
#endif
#elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
  __machine_rw_barrier();
#elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC)) &&       \
    (defined(HP_IA64) || defined(__ia64))
  _Asm_mf();
#elif defined(_AIX) || defined(__ppc__) || defined(__powerpc__) ||             \
    defined(__ppc64__) || defined(__powerpc64__)
  __lwsync();
#else
#error "Could not guess the kind of compiler, please report to us."
#endif
}

/*----------------------------------------------------------------------------*/
/* Cache coherence and invalidation */

#ifndef MDBX_CPU_WRITEBACK_IS_COHERENT
#if defined(__ia32__) || defined(__e2k__) || defined(__hppa) ||                \
    defined(__hppa__)
#define MDBX_CPU_WRITEBACK_IS_COHERENT 1
#else
#define MDBX_CPU_WRITEBACK_IS_COHERENT 0
#endif
#endif /* MDBX_CPU_WRITEBACK_IS_COHERENT */

#ifndef MDBX_CACHELINE_SIZE
#if defined(SYSTEM_CACHE_ALIGNMENT_SIZE)
#define MDBX_CACHELINE_SIZE SYSTEM_CACHE_ALIGNMENT_SIZE
#elif defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
#define MDBX_CACHELINE_SIZE 128
#else
#define MDBX_CACHELINE_SIZE 64
#endif
#endif /* MDBX_CACHELINE_SIZE */

#if MDBX_CPU_WRITEBACK_IS_COHERENT
#define mdbx_flush_noncoherent_cpu_writeback() mdbx_compiler_barrier()
#else
#define mdbx_flush_noncoherent_cpu_writeback() mdbx_memory_barrier()
#endif

#if __has_include(<sys/cachectl.h>)
#include <sys/cachectl.h>
#elif defined(__mips) || defined(__mips__) || defined(__mips64) ||             \
    defined(__mips64__) || defined(_M_MRX000) || defined(_MIPS_) ||            \
    defined(__MWERKS__) || defined(__sgi)
/* MIPS should have explicit cache control */
#include <sys/cachectl.h>
#endif

#ifndef MDBX_CPU_CACHE_MMAP_NONCOHERENT
#if defined(__mips) || defined(__mips__) || defined(__mips64) ||               \
    defined(__mips64__) || defined(_M_MRX000) || defined(_MIPS_) ||            \
    defined(__MWERKS__) || defined(__sgi)
/* MIPS has cache coherency issues. */
#define MDBX_CPU_CACHE_MMAP_NONCOHERENT 1
#else
/* LY: assume no relevant mmap/dcache issues. */
#define MDBX_CPU_CACHE_MMAP_NONCOHERENT 0
#endif
#endif /* ndef MDBX_CPU_CACHE_MMAP_NONCOHERENT */

static __maybe_unused __inline void
mdbx_invalidate_mmap_noncoherent_cache(void *addr, size_t nbytes) {
#if MDBX_CPU_CACHE_MMAP_NONCOHERENT
#ifdef DCACHE
  /* MIPS has cache coherency issues.
   * Note: for any nbytes >= on-chip cache size, entire is flushed. */
  cacheflush(addr, nbytes, DCACHE);
#else
#error "Oops, cacheflush() not available"
#endif /* DCACHE */
#else  /* MDBX_CPU_CACHE_MMAP_NONCOHERENT */
  (void)addr;
  (void)nbytes;
#endif /* MDBX_CPU_CACHE_MMAP_NONCOHERENT */
}

/*----------------------------------------------------------------------------*/
/* libc compatibility stuff */

#if (!defined(__GLIBC__) && __GLIBC_PREREQ(2, 1)) &&                           \
    (defined(_GNU_SOURCE) || defined(_BSD_SOURCE))
#define mdbx_asprintf asprintf
#define mdbx_vasprintf vasprintf
#else
MDBX_INTERNAL_FUNC __printf_args(2, 3) int __maybe_unused
    mdbx_asprintf(char **strp, const char *fmt, ...);
MDBX_INTERNAL_FUNC int mdbx_vasprintf(char **strp, const char *fmt, va_list ap);
#endif

/*----------------------------------------------------------------------------*/
/* OS abstraction layer stuff */

/* max bytes to write in one call */
#if defined(_WIN32) || defined(_WIN64)
#define MAX_WRITE UINT32_C(0x01000000)
#else
#define MAX_WRITE UINT32_C(0x3fff0000)
#endif

#if defined(__linux__) || defined(__gnu_linux__)
MDBX_INTERNAL_VAR uint32_t mdbx_linux_kernel_version;
#endif /* Linux */

/* Get the size of a memory page for the system.
 * This is the basic size that the platform's memory manager uses, and is
 * fundamental to the use of memory-mapped files. */
static __maybe_unused __inline size_t mdbx_syspagesize(void) {
#if defined(_WIN32) || defined(_WIN64)
  SYSTEM_INFO si;
  GetSystemInfo(&si);
  return si.dwPageSize;
#else
  return sysconf(_SC_PAGE_SIZE);
#endif
}

#ifndef mdbx_strdup
LIBMDBX_API char *mdbx_strdup(const char *str);
#endif

static __maybe_unused __inline int mdbx_get_errno(void) {
#if defined(_WIN32) || defined(_WIN64)
  DWORD rc = GetLastError();
#else
  int rc = errno;
#endif
  return rc;
}

#ifndef mdbx_memalign_alloc
MDBX_INTERNAL_FUNC int mdbx_memalign_alloc(size_t alignment, size_t bytes,
                                           void **result);
#endif
#ifndef mdbx_memalign_free
MDBX_INTERNAL_FUNC void mdbx_memalign_free(void *ptr);
#endif

MDBX_INTERNAL_FUNC int mdbx_condmutex_init(mdbx_condmutex_t *condmutex);
MDBX_INTERNAL_FUNC int mdbx_condmutex_lock(mdbx_condmutex_t *condmutex);
MDBX_INTERNAL_FUNC int mdbx_condmutex_unlock(mdbx_condmutex_t *condmutex);
MDBX_INTERNAL_FUNC int mdbx_condmutex_signal(mdbx_condmutex_t *condmutex);
MDBX_INTERNAL_FUNC int mdbx_condmutex_wait(mdbx_condmutex_t *condmutex);
MDBX_INTERNAL_FUNC int mdbx_condmutex_destroy(mdbx_condmutex_t *condmutex);

MDBX_INTERNAL_FUNC int mdbx_fastmutex_init(mdbx_fastmutex_t *fastmutex);
MDBX_INTERNAL_FUNC int mdbx_fastmutex_acquire(mdbx_fastmutex_t *fastmutex);
MDBX_INTERNAL_FUNC int mdbx_fastmutex_release(mdbx_fastmutex_t *fastmutex);
MDBX_INTERNAL_FUNC int mdbx_fastmutex_destroy(mdbx_fastmutex_t *fastmutex);

MDBX_INTERNAL_FUNC int mdbx_pwritev(mdbx_filehandle_t fd, struct iovec *iov,
                                    int iovcnt, uint64_t offset,
                                    size_t expected_written);
MDBX_INTERNAL_FUNC int mdbx_pread(mdbx_filehandle_t fd, void *buf, size_t count,
                                  uint64_t offset);
MDBX_INTERNAL_FUNC int mdbx_pwrite(mdbx_filehandle_t fd, const void *buf,
                                   size_t count, uint64_t offset);
MDBX_INTERNAL_FUNC int mdbx_write(mdbx_filehandle_t fd, const void *buf,
                                  size_t count);

MDBX_INTERNAL_FUNC int
mdbx_thread_create(mdbx_thread_t *thread,
                   THREAD_RESULT(THREAD_CALL *start_routine)(void *),
                   void *arg);
MDBX_INTERNAL_FUNC int mdbx_thread_join(mdbx_thread_t thread);

enum mdbx_syncmode_bits {
  MDBX_SYNC_DATA = 1,
  MDBX_SYNC_SIZE = 2,
  MDBX_SYNC_IODQ = 4
};

MDBX_INTERNAL_FUNC int mdbx_filesync(mdbx_filehandle_t fd,
                                     enum mdbx_syncmode_bits mode_bits);
MDBX_INTERNAL_FUNC int mdbx_ftruncate(mdbx_filehandle_t fd, uint64_t length);
MDBX_INTERNAL_FUNC int mdbx_fseek(mdbx_filehandle_t fd, uint64_t pos);
MDBX_INTERNAL_FUNC int mdbx_filesize(mdbx_filehandle_t fd, uint64_t *length);
MDBX_INTERNAL_FUNC int mdbx_openfile(const char *pathname, int flags,
                                     mode_t mode, mdbx_filehandle_t *fd,
                                     bool exclusive);
MDBX_INTERNAL_FUNC int mdbx_closefile(mdbx_filehandle_t fd);
MDBX_INTERNAL_FUNC int mdbx_removefile(const char *pathname);
MDBX_INTERNAL_FUNC int mdbx_is_pipe(mdbx_filehandle_t fd);

typedef struct mdbx_mmap_param {
  union {
    void *address;
    uint8_t *dxb;
    struct MDBX_lockinfo *lck;
  };
  mdbx_filehandle_t fd;
  size_t length; /* mapping length, but NOT a size of file or DB */
#if defined(_WIN32) || defined(_WIN64)
  size_t current; /* mapped region size, e.g. file and DB */
  uint64_t filesize;
#endif
#ifdef MDBX_OSAL_SECTION
  MDBX_OSAL_SECTION section;
#endif
} mdbx_mmap_t;

MDBX_INTERNAL_FUNC int mdbx_mmap(int flags, mdbx_mmap_t *map, size_t must,
                                 size_t limit);
MDBX_INTERNAL_FUNC int mdbx_munmap(mdbx_mmap_t *map);
MDBX_INTERNAL_FUNC int mdbx_mresize(int flags, mdbx_mmap_t *map, size_t current,
                                    size_t wanna);
#if defined(_WIN32) || defined(_WIN64)
typedef struct {
  unsigned limit, count;
  HANDLE handles[31];
} mdbx_handle_array_t;
MDBX_INTERNAL_FUNC int
mdbx_suspend_threads_before_remap(MDBX_env *env, mdbx_handle_array_t **array);
MDBX_INTERNAL_FUNC int
mdbx_resume_threads_after_remap(mdbx_handle_array_t *array);
#endif /* Windows */
MDBX_INTERNAL_FUNC int mdbx_msync(mdbx_mmap_t *map, size_t offset,
                                  size_t length, int async);
MDBX_INTERNAL_FUNC int mdbx_check4nonlocal(mdbx_filehandle_t handle, int flags);

static __maybe_unused __inline mdbx_pid_t mdbx_getpid(void) {
#if defined(_WIN32) || defined(_WIN64)
  return GetCurrentProcessId();
#else
  return getpid();
#endif
}

static __maybe_unused __inline mdbx_tid_t mdbx_thread_self(void) {
#if defined(_WIN32) || defined(_WIN64)
  return GetCurrentThreadId();
#else
  return pthread_self();
#endif
}

MDBX_INTERNAL_FUNC void __maybe_unused mdbx_osal_jitter(bool tiny);
MDBX_INTERNAL_FUNC uint64_t mdbx_osal_monotime(void);
MDBX_INTERNAL_FUNC uint64_t
mdbx_osal_16dot16_to_monotime(uint32_t seconds_16dot16);

/*----------------------------------------------------------------------------*/
/* lck stuff */

#if defined(_WIN32) || defined(_WIN64)
#undef MDBX_OSAL_LOCK
#define MDBX_OSAL_LOCK_SIGN UINT32_C(0xF10C)
#else
#define MDBX_OSAL_LOCK pthread_mutex_t
#define MDBX_OSAL_LOCK_SIGN UINT32_C(0x8017)
#endif /* MDBX_OSAL_LOCK */

/// \brief Инициализация объектов синхронизации связанных с экземпляром MDBX_env
///   как общик в LCK-файле, так и внутри текущего процесса.
/// \param
///   global_uniqueness_flag = true - означает что сейчас в системе нет других
///     процессов работающих с БД и LCK-файлом. Соответственно функция ДОЛЖНА
///     инициализировать разделяемые объекты синхронизации расположенные
///     в отображенном в память LCK-файле.
///   global_uniqueness_flag = false - означает что в системе есть хотя-бы
///     один другой процесс уже работающий с БД и LCK-файлом, в том числе
///     БД уже может быть открыта текущим процессом. Соответственно функция
///     НЕ должна инициализировать уже используемые разделяемые объекты
///     синхронизации расположенные в отображенном в память LCK-файле.
/// \return Код ошибки или 0 в случае успеха.
MDBX_INTERNAL_FUNC int mdbx_lck_init(MDBX_env *env,
                                     MDBX_env *inprocess_neighbor,
                                     int global_uniqueness_flag);

/// \brief Отключение от общих межпроцесных объектов и разрушение объектов
///   синхронизации внутри текущего процесса связанных с экземпляром MDBX_env.
/// \param
///   inprocess_neighbor = NULL - если в текущем процессе нет других экземпляров
///     MDBX_env связанных с закрываемой БД. Соответственно функция ДОЛЖНА
///     своими средствами проверить, есть ли другие процесса работающие с БД
///     и LCK-файлом, и в зависимости от этого разрушить или сохранить бъекты
///     синхронизации расположенные в отображенном в память LCK-файле.
///   inprocess_neighbor = не-NULL - тогда он указывает на другой (любой
///     в случае нескольких) экземпляр MDBX_env работающей с БД и LCK-файлом
///     внутри текущего процесса. Соответственно, функция НЕ должна пытаться
///     захватывать эксклюзивную блокировки и/или пытаться разрушить общие
///     объекты синхронизации связанные с БД и LCK-файлом. Кроме этого,
///     реализация ДОЛЖНА обеспечить корректность работы других экземпляров
///     MDBX_env внутри процесса - например, восстановить POSIX-fcntl блокировки
///     после закрытия файловых дескрипторов.
/// \return Код ошибки (MDBX_PANIC) или 0 в случае успеха.
MDBX_INTERNAL_FUNC int mdbx_lck_destroy(MDBX_env *env,
                                        MDBX_env *inprocess_neighbor);

/// \brief Подключение к общим межпроцесным объектам блокировки с попыткой
///   захвата блокировки максимального уровня (разделяемой при недоступности
///   эксклюзивной).
///   В зависимости от реализации и/или платформы (Windows) может
///   захватывать блокировку не-операционного супер-уровня (например, для
///   инициализации разделяемых объектов синхронизации), которая затем будет
///   понижена до операционно-эксклюзивной или разделяемой посредством
///   явного вызова mdbx_lck_downgrade().
/// \return
///   MDBX_RESULT_TRUE (-1) - если удалось захватить эксклюзивную блокировку и,
///     следовательно, текущий процесс является первым и единственным
///     после предыдущего использования БД.
///   MDBX_RESULT_FALSE (0) - если удалось захватить только разделяемую
///     блокировку и, следовательно, БД уже открыта и используется другими
///     процессами.
///   Иначе (не 0 и не -1) - код ошибки.
MDBX_INTERNAL_FUNC int mdbx_lck_seize(MDBX_env *env);

/// \brief Снижает уровень первоначальной захваченной блокировки до
///   операционного уровня определяемого аргументом. Смысл функции в возврате
///   в операционный режим:
///    - разблокирование других процессов ожидающих доступа, т.е если
///      (env->me_flags & MDBX_EXCLUSIVE) != 0, то другие процессы должны узнать
///      о невозможности доступа, а не ждать его.
///    - снятия блокировок мешающих работе с файлом (актуально для Windows).
///   (env->me_flags & MDBX_EXCLUSIVE) == 0 - понижение до разделяемой
///   блокировки. (env->me_flags & MDBX_EXCLUSIVE) != 0 - понижение до
///   эксклюзивной операционной блокировки.
/// \return Код ошибки или 0 в случае успеха.
MDBX_INTERNAL_FUNC int mdbx_lck_downgrade(MDBX_env *env);

/// \brief Блокирует lck-файл и/или таблицу читателей для (де)регистрации.
/// \return Код ошибки или 0 в случае успеха.
MDBX_INTERNAL_FUNC int mdbx_rdt_lock(MDBX_env *env);

/// \brief Разблокирует lck-файл и/или таблицу читателей после (де)регистрации.
MDBX_INTERNAL_FUNC void mdbx_rdt_unlock(MDBX_env *env);

/// \brief Захватывает блокировку для изменения БД (при старте пишущей
///   транзакции). Транзакции чтения при этом никак не блокируются.
///   Объявлена LIBMDBX_API так как используется в mdbx_chk.
/// \return Код ошибки или 0 в случае успеха.
LIBMDBX_API int mdbx_txn_lock(MDBX_env *env, bool dontwait);

/// \brief Освобождает блокировку по окончанию изменения БД (после завершения
///   пишущей транзакции).
///   Объявлена LIBMDBX_API так как используется в mdbx_chk.
LIBMDBX_API void mdbx_txn_unlock(MDBX_env *env);

/// \brief Устанавливает alive-флажок присутствия (индицирующую блокировку)
///   читателя для pid текущего процесса. Функции может выполнить не более
///   необходимого минимума для корректной работы mdbx_rpid_check() в других
///   процессах.
/// \return Код ошибки или 0 в случае успеха.
MDBX_INTERNAL_FUNC int mdbx_rpid_set(MDBX_env *env);

/// \brief Снимает alive-флажок присутствия (индицирующую блокировку)
///   читателя для pid текущего процесса. Функции может выполнить не более
///   необходимого минимума для корректной работы mdbx_rpid_check() в других
///   процессах.
/// \return Код ошибки или 0 в случае успеха.
MDBX_INTERNAL_FUNC int mdbx_rpid_clear(MDBX_env *env);

/// \brief Проверяет жив ли процесс-читатель с заданным pid
///   по alive-флажку присутствия (индицирующей блокировку),
///   либо любым другим способом.
/// \return
///   MDBX_RESULT_TRUE (-1) - если процесс-читатель с соответствующим pid жив
///     и работает с БД (индицирующая блокировка присутствует).
///   MDBX_RESULT_FALSE (0) - если процесс-читатель с соответствующим pid
///     отсутствует или не работает с БД (индицирующая блокировка отсутствует).
///   Иначе (не 0 и не -1) - код ошибки.
MDBX_INTERNAL_FUNC int mdbx_rpid_check(MDBX_env *env, mdbx_pid_t pid);

#if defined(_WIN32) || defined(_WIN64)
typedef union MDBX_srwlock {
  struct {
    long volatile readerCount;
    long volatile writerCount;
  };
  RTL_SRWLOCK native;
} MDBX_srwlock;

typedef void(WINAPI *MDBX_srwlock_function)(MDBX_srwlock *);
MDBX_INTERNAL_VAR MDBX_srwlock_function mdbx_srwlock_Init,
    mdbx_srwlock_AcquireShared, mdbx_srwlock_ReleaseShared,
    mdbx_srwlock_AcquireExclusive, mdbx_srwlock_ReleaseExclusive;

typedef BOOL(WINAPI *MDBX_GetFileInformationByHandleEx)(
    _In_ HANDLE hFile, _In_ FILE_INFO_BY_HANDLE_CLASS FileInformationClass,
    _Out_ LPVOID lpFileInformation, _In_ DWORD dwBufferSize);
MDBX_INTERNAL_VAR MDBX_GetFileInformationByHandleEx
    mdbx_GetFileInformationByHandleEx;

typedef BOOL(WINAPI *MDBX_GetVolumeInformationByHandleW)(
    _In_ HANDLE hFile, _Out_opt_ LPWSTR lpVolumeNameBuffer,
    _In_ DWORD nVolumeNameSize, _Out_opt_ LPDWORD lpVolumeSerialNumber,
    _Out_opt_ LPDWORD lpMaximumComponentLength,
    _Out_opt_ LPDWORD lpFileSystemFlags,
    _Out_opt_ LPWSTR lpFileSystemNameBuffer, _In_ DWORD nFileSystemNameSize);
MDBX_INTERNAL_VAR MDBX_GetVolumeInformationByHandleW
    mdbx_GetVolumeInformationByHandleW;

typedef DWORD(WINAPI *MDBX_GetFinalPathNameByHandleW)(_In_ HANDLE hFile,
                                                      _Out_ LPWSTR lpszFilePath,
                                                      _In_ DWORD cchFilePath,
                                                      _In_ DWORD dwFlags);
MDBX_INTERNAL_VAR MDBX_GetFinalPathNameByHandleW mdbx_GetFinalPathNameByHandleW;

typedef BOOL(WINAPI *MDBX_SetFileInformationByHandle)(
    _In_ HANDLE hFile, _In_ FILE_INFO_BY_HANDLE_CLASS FileInformationClass,
    _Out_ LPVOID lpFileInformation, _In_ DWORD dwBufferSize);
MDBX_INTERNAL_VAR MDBX_SetFileInformationByHandle
    mdbx_SetFileInformationByHandle;

typedef NTSTATUS(NTAPI *MDBX_NtFsControlFile)(
    IN HANDLE FileHandle, IN OUT HANDLE Event,
    IN OUT PVOID /* PIO_APC_ROUTINE */ ApcRoutine, IN OUT PVOID ApcContext,
    OUT PIO_STATUS_BLOCK IoStatusBlock, IN ULONG FsControlCode,
    IN OUT PVOID InputBuffer, IN ULONG InputBufferLength,
    OUT OPTIONAL PVOID OutputBuffer, IN ULONG OutputBufferLength);
MDBX_INTERNAL_VAR MDBX_NtFsControlFile mdbx_NtFsControlFile;

#if !defined(_WIN32_WINNT_WIN8) || _WIN32_WINNT < _WIN32_WINNT_WIN8
typedef struct _WIN32_MEMORY_RANGE_ENTRY {
  PVOID VirtualAddress;
  SIZE_T NumberOfBytes;
} WIN32_MEMORY_RANGE_ENTRY, *PWIN32_MEMORY_RANGE_ENTRY;
#endif /* Windows 8.x */

typedef BOOL(WINAPI *MDBX_PrefetchVirtualMemory)(
    HANDLE hProcess, ULONG_PTR NumberOfEntries,
    PWIN32_MEMORY_RANGE_ENTRY VirtualAddresses, ULONG Flags);
MDBX_INTERNAL_VAR MDBX_PrefetchVirtualMemory mdbx_PrefetchVirtualMemory;

typedef DWORD(WINAPI *MDBX_DiscardVirtualMemory)(PVOID VirtualAddress,
                                                 SIZE_T Size);
MDBX_INTERNAL_VAR MDBX_DiscardVirtualMemory mdbx_DiscardVirtualMemory;

#endif /* Windows */

/*----------------------------------------------------------------------------*/
/* Atomics */

#if !defined(__cplusplus) && (__STDC_VERSION__ >= 201112L) &&                  \
    !defined(__STDC_NO_ATOMICS__) &&                                           \
    (__GNUC_PREREQ(4, 9) || __CLANG_PREREQ(3, 8) ||                            \
     !(defined(__GNUC__) || defined(__clang__)))
#include <stdatomic.h>
#elif defined(__GNUC__) || defined(__clang__)
/* LY: nothing required */
#elif defined(_MSC_VER)
#pragma warning(disable : 4163) /* 'xyz': not available as an intrinsic */
#pragma warning(disable : 4133) /* 'function': incompatible types - from       \
                                   'size_t' to 'LONGLONG' */
#pragma warning(disable : 4244) /* 'return': conversion from 'LONGLONG' to     \
                                   'std::size_t', possible loss of data */
#pragma warning(disable : 4267) /* 'function': conversion from 'size_t' to     \
                                   'long', possible loss of data */
#pragma intrinsic(_InterlockedExchangeAdd, _InterlockedCompareExchange)
#pragma intrinsic(_InterlockedExchangeAdd64, _InterlockedCompareExchange64)
#elif defined(__APPLE__)
#include <libkern/OSAtomic.h>
#else
#error FIXME atomic-ops
#endif

static __maybe_unused __inline uint32_t mdbx_atomic_add32(volatile uint32_t *p,
                                                          uint32_t v) {
#if !defined(__cplusplus) && defined(ATOMIC_VAR_INIT)
  assert(atomic_is_lock_free(p));
  return atomic_fetch_add((_Atomic uint32_t *)p, v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_fetch_and_add(p, v);
#else
#ifdef _MSC_VER
  STATIC_ASSERT(sizeof(volatile long) == sizeof(volatile uint32_t));
  return _InterlockedExchangeAdd((volatile long *)p, v);
#endif
#ifdef __APPLE__
  return OSAtomicAdd32(v, (volatile int32_t *)p);
#endif
#endif
}

static __maybe_unused __inline uint64_t mdbx_atomic_add64(volatile uint64_t *p,
                                                          uint64_t v) {
#if !defined(__cplusplus) && defined(ATOMIC_VAR_INIT)
  assert(atomic_is_lock_free(p));
  return atomic_fetch_add((_Atomic uint64_t *)p, v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_fetch_and_add(p, v);
#else
#ifdef _MSC_VER
#ifdef _WIN64
  return _InterlockedExchangeAdd64((volatile int64_t *)p, v);
#else
  return InterlockedExchangeAdd64((volatile int64_t *)p, v);
#endif
#endif /* _MSC_VER */
#ifdef __APPLE__
  return OSAtomicAdd64(v, (volatile int64_t *)p);
#endif
#endif
}

#define mdbx_atomic_sub32(p, v) mdbx_atomic_add32(p, 0 - (v))
#define mdbx_atomic_sub64(p, v) mdbx_atomic_add64(p, 0 - (v))

static __maybe_unused __inline bool
mdbx_atomic_compare_and_swap32(volatile uint32_t *p, uint32_t c, uint32_t v) {
#if !defined(__cplusplus) && defined(ATOMIC_VAR_INIT)
  assert(atomic_is_lock_free(p));
  return atomic_compare_exchange_strong((_Atomic uint32_t *)p, &c, v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_bool_compare_and_swap(p, c, v);
#else
#ifdef _MSC_VER
  STATIC_ASSERT(sizeof(volatile long) == sizeof(volatile uint32_t));
  return c == _InterlockedCompareExchange((volatile long *)p, v, c);
#endif
#ifdef __APPLE__
  return c == OSAtomicCompareAndSwap32Barrier(c, v, (volatile int32_t *)p);
#endif
#endif
}

static __maybe_unused __inline bool
mdbx_atomic_compare_and_swap64(volatile uint64_t *p, uint64_t c, uint64_t v) {
#if !defined(__cplusplus) && defined(ATOMIC_VAR_INIT)
  assert(atomic_is_lock_free(p));
  return atomic_compare_exchange_strong((_Atomic uint64_t *)p, &c, v);
#elif defined(__GNUC__) || defined(__clang__)
  return __sync_bool_compare_and_swap(p, c, v);
#else
#ifdef _MSC_VER
  return c == _InterlockedCompareExchange64((volatile int64_t *)p, v, c);
#endif
#ifdef __APPLE__
  return c == OSAtomicCompareAndSwap64Barrier(c, v, (volatile uint64_t *)p);
#endif
#endif
}

/*----------------------------------------------------------------------------*/

#if defined(_MSC_VER) && _MSC_VER >= 1900
/* LY: MSVC 2015/2017/2019 has buggy/inconsistent PRIuPTR/PRIxPTR macros
 * for internal format-args checker. */
#undef PRIuPTR
#undef PRIiPTR
#undef PRIdPTR
#undef PRIxPTR
#define PRIuPTR "Iu"
#define PRIiPTR "Ii"
#define PRIdPTR "Id"
#define PRIxPTR "Ix"
#define PRIuSIZE "zu"
#define PRIiSIZE "zi"
#define PRIdSIZE "zd"
#define PRIxSIZE "zx"
#endif /* fix PRI*PTR for _MSC_VER */

#ifndef PRIuSIZE
#define PRIuSIZE PRIuPTR
#define PRIiSIZE PRIiPTR
#define PRIdSIZE PRIdPTR
#define PRIxSIZE PRIxPTR
#endif /* PRI*SIZE macros for MSVC */

#ifdef _MSC_VER
#pragma warning(pop)
#endif

/* *INDENT-ON* */
/* clang-format on */

#if UINTPTR_MAX > 0xffffFFFFul || ULONG_MAX > 0xffffFFFFul
#define MDBX_WORDBITS 64
#else
#define MDBX_WORDBITS 32
#endif /* MDBX_WORDBITS */

/* Some platforms define the EOWNERDEAD error code even though they
 *  don't support Robust Mutexes. Compile with -DMDBX_USE_ROBUST=0. */
#ifndef MDBX_USE_ROBUST
#define MDBX_USE_ROBUST_CONFIG AUTO
/* Howard Chu: Android currently lacks Robust Mutex support */
#if defined(EOWNERDEAD) && !defined(__ANDROID__) && !defined(__APPLE__) &&     \
    (!defined(__GLIBC__) ||                                                    \
     __GLIBC_PREREQ(                                                           \
         2,                                                                    \
         10) /* LY: glibc before 2.10 has a troubles with Robust Mutex too. */ \
     || _POSIX_C_SOURCE >= 200809L)
#define MDBX_USE_ROBUST 1
#else
#define MDBX_USE_ROBUST 0
#endif
#else
#define MDBX_USE_ROBUST_CONFIG MDBX_USE_ROBUST
#endif /* MDBX_USE_ROBUST */

#ifndef MDBX_USE_OFDLOCKS
#define MDBX_USE_OFDLOCKS_CONFIG AUTO
#if defined(F_OFD_SETLK) && defined(F_OFD_SETLKW) && defined(F_OFD_GETLK) &&   \
    !defined(MDBX_SAFE4QEMU)
#define MDBX_USE_OFDLOCKS 1
#else
#define MDBX_USE_OFDLOCKS 0
#endif
#else
#define MDBX_USE_OFDLOCKS_CONFIG MDBX_USE_OFDLOCKS
#endif /* MDBX_USE_OFDLOCKS */

/* Controls checking PID against reuse DB environment after the fork() */
#ifndef MDBX_TXN_CHECKPID
#define MDBX_TXN_CHECKPID_CONFIG AUTO
#if defined(MADV_DONTFORK) || defined(_WIN32) || defined(_WIN64)
/* PID check could be ommited:
 *  - on Linux when madvise(MADV_DONTFORK) is available. i.e. after the fork()
 *    mapped pages will not be available for child process.
 *  - in Windows where fork() not available. */
#define MDBX_TXN_CHECKPID 0
#else
#define MDBX_TXN_CHECKPID 1
#endif
#else
#define MDBX_TXN_CHECKPID_CONFIG MDBX_TXN_CHECKPID
#endif /* MDBX_TXN_CHECKPID */

#ifndef MDBX_TXN_CHECKOWNER
#define MDBX_TXN_CHECKOWNER_CONFIG AUTO
#define MDBX_TXN_CHECKOWNER 1
#endif /* MDBX_TXN_CHECKOWNER */

#define mdbx_sourcery_anchor XCONCAT(mdbx_sourcery_, MDBX_BUILD_SOURCERY)
#if defined(MDBX_TOOLS)
extern LIBMDBX_API const char *const mdbx_sourcery_anchor;
#endif

/*----------------------------------------------------------------------------*/
/* Basic constants and types */

/* The minimum number of keys required in a database page.
 * Setting this to a larger value will place a smaller bound on the
 * maximum size of a data item. Data items larger than this size will
 * be pushed into overflow pages instead of being stored directly in
 * the B-tree node. This value used to default to 4. With a page size
 * of 4096 bytes that meant that any item larger than 1024 bytes would
 * go into an overflow page. That also meant that on average 2-3KB of
 * each overflow page was wasted space. The value cannot be lower than
 * 2 because then there would no longer be a tree structure. With this
 * value, items larger than 2KB will go into overflow pages, and on
 * average only 1KB will be wasted. */
#define MDBX_MINKEYS 2

/* A stamp that identifies a file as an MDBX file.
 * There's nothing special about this value other than that it is easily
 * recognizable, and it will reflect any byte order mismatches. */
#define MDBX_MAGIC UINT64_C(/* 56-bit prime */ 0x59659DBDEF4C11)

/* The version number for a database's datafile format. */
#define MDBX_DATA_VERSION 2
/* The version number for a database's lockfile format. */
#define MDBX_LOCK_VERSION 3

/* handle for the DB used to track free pages. */
#define FREE_DBI 0
/* handle for the default DB. */
#define MAIN_DBI 1
/* Number of DBs in metapage (free and main) - also hardcoded elsewhere */
#define CORE_DBS 2
#define MAX_DBI (INT16_MAX - CORE_DBS)
#if MAX_DBI != MDBX_MAX_DBI
#error "Opps, MAX_DBI != MDBX_MAX_DBI"
#endif

/* Number of meta pages - also hardcoded elsewhere */
#define NUM_METAS 3

/* A page number in the database.
 *
 * MDBX uses 32 bit for page numbers. This limits database
 * size up to 2^44 bytes, in case of 4K pages. */
typedef uint32_t pgno_t;
#define PRIaPGNO PRIu32
#define MAX_PAGENO UINT32_C(0x7FFFffff)
#define MIN_PAGENO NUM_METAS

/* A transaction ID. */
typedef uint64_t txnid_t;
#define PRIaTXN PRIi64
#define MIN_TXNID UINT64_C(1)

/* Used for offsets within a single page.
 * Since memory pages are typically 4 or 8KB in size, 12-13 bits,
 * this is plenty. */
typedef uint16_t indx_t;

#define MEGABYTE ((size_t)1 << 20)

/*----------------------------------------------------------------------------*/
/* Core structures for database and shared memory (i.e. format definition) */
#pragma pack(push, 1)

/* Information about a single database in the environment. */
typedef struct MDBX_db {
  uint16_t md_flags;        /* see mdbx_dbi_open */
  uint16_t md_depth;        /* depth of this tree */
  uint32_t md_xsize;        /* also ksize for LEAF2 pages */
  pgno_t md_root;           /* the root page of this tree */
  pgno_t md_branch_pages;   /* number of internal pages */
  pgno_t md_leaf_pages;     /* number of leaf pages */
  pgno_t md_overflow_pages; /* number of overflow pages */
  uint64_t md_seq;          /* table sequence counter */
  uint64_t md_entries;      /* number of data items */
  uint64_t md_merkle;       /* Merkle tree checksum */
} MDBX_db;

/* database size-related parameters */
typedef struct mdbx_geo_t {
  uint16_t grow;   /* datafile growth step in pages */
  uint16_t shrink; /* datafile shrink threshold in pages */
  pgno_t lower;    /* minimal size of datafile in pages */
  pgno_t upper;    /* maximal size of datafile in pages */
  pgno_t now;      /* current size of datafile in pages */
  pgno_t next;     /* first unused page in the datafile,
                    * but actually the file may be shorter. */
} mdbx_geo_t;

/* Meta page content.
 * A meta page is the start point for accessing a database snapshot.
 * Pages 0-1 are meta pages. Transaction N writes meta page (N % 2). */
typedef struct MDBX_meta {
  /* Stamp identifying this as an MDBX file.
   * It must be set to MDBX_MAGIC with MDBX_DATA_VERSION. */
  uint64_t mm_magic_and_version;

  /* txnid that committed this page, the first of a two-phase-update pair */
  volatile txnid_t mm_txnid_a;

  uint16_t mm_extra_flags;  /* extra DB flags, zero (nothing) for now */
  uint8_t mm_validator_id;  /* ID of checksum and page validation method,
                             * zero (nothing) for now */
  uint8_t mm_extra_pagehdr; /* extra bytes in the page header,
                             * zero (nothing) for now */

  mdbx_geo_t mm_geo; /* database size-related parameters */

  MDBX_db mm_dbs[CORE_DBS]; /* first is free space, 2nd is main db */
                            /* The size of pages used in this DB */
#define mm_psize mm_dbs[FREE_DBI].md_xsize
/* Any persistent environment flags, see mdbx_env */
#define mm_flags mm_dbs[FREE_DBI].md_flags
  mdbx_canary mm_canary;

#define MDBX_DATASIGN_NONE 0u
#define MDBX_DATASIGN_WEAK 1u
#define SIGN_IS_WEAK(sign) ((sign) == MDBX_DATASIGN_WEAK)
#define SIGN_IS_STEADY(sign) ((sign) > MDBX_DATASIGN_WEAK)
#define META_IS_WEAK(meta) SIGN_IS_WEAK((meta)->mm_datasync_sign)
#define META_IS_STEADY(meta) SIGN_IS_STEADY((meta)->mm_datasync_sign)
  volatile uint64_t mm_datasync_sign;

  /* txnid that committed this page, the second of a two-phase-update pair */
  volatile txnid_t mm_txnid_b;

  /* Number of non-meta pages which were put in GC after COW. May be 0 in case
   * DB was previously handled by libmdbx without corresponding feature.
   * This value in couple with mr_snapshot_pages_retired allows fast estimation
   * of "how much reader is restraining GC recycling". */
  uint64_t mm_pages_retired;
} MDBX_meta;

/* Common header for all page types. The page type depends on mp_flags.
 *
 * P_BRANCH and P_LEAF pages have unsorted 'MDBX_node's at the end, with
 * sorted mp_ptrs[] entries referring to them. Exception: P_LEAF2 pages
 * omit mp_ptrs and pack sorted MDBX_DUPFIXED values after the page header.
 *
 * P_OVERFLOW records occupy one or more contiguous pages where only the
 * first has a page header. They hold the real data of F_BIGDATA nodes.
 *
 * P_SUBP sub-pages are small leaf "pages" with duplicate data.
 * A node with flag F_DUPDATA but not F_SUBDATA contains a sub-page.
 * (Duplicate data can also go in sub-databases, which use normal pages.)
 *
 * P_META pages contain MDBX_meta, the start point of an MDBX snapshot.
 *
 * Each non-metapage up to MDBX_meta.mm_last_pg is reachable exactly once
 * in the snapshot: Either used by a database or listed in a freeDB record. */
typedef struct MDBX_page {
  union {
    struct MDBX_page *mp_next; /* for in-memory list of freed pages,
                                * must be first field, see NEXT_LOOSE_PAGE */
    uint64_t mp_validator;     /* checksum of page content or a txnid during
                                * which the page has been updated */
  };
  uint16_t mp_leaf2_ksize; /* key size if this is a LEAF2 page */
#define P_BRANCH 0x01      /* branch page */
#define P_LEAF 0x02        /* leaf page */
#define P_OVERFLOW 0x04    /* overflow page */
#define P_META 0x08        /* meta page */
#define P_DIRTY 0x10       /* dirty page, also set for P_SUBP pages */
#define P_LEAF2 0x20       /* for MDBX_DUPFIXED records */
#define P_SUBP 0x40        /* for MDBX_DUPSORT sub-pages */
#define P_LOOSE 0x4000     /* page was dirtied then freed, can be reused */
#define P_KEEP 0x8000      /* leave this page alone during spill */
  uint16_t mp_flags;
  union {
    struct {
      indx_t mp_lower; /* lower bound of free space */
      indx_t mp_upper; /* upper bound of free space */
    };
    uint32_t mp_pages; /* number of overflow pages */
  };
  pgno_t mp_pgno; /* page number */

  /* dynamic size */
  union {
    indx_t mp_ptrs[1];
    MDBX_meta mp_meta;
    uint8_t mp_data[1];
  };
} MDBX_page;

/* Size of the page header, excluding dynamic data at the end */
#define PAGEHDRSZ ((unsigned)offsetof(MDBX_page, mp_data))

#pragma pack(pop)

/* Reader Lock Table
 *
 * Readers don't acquire any locks for their data access. Instead, they
 * simply record their transaction ID in the reader table. The reader
 * mutex is needed just to find an empty slot in the reader table. The
 * slot's address is saved in thread-specific data so that subsequent
 * read transactions started by the same thread need no further locking to
 * proceed.
 *
 * If MDBX_NOTLS is set, the slot address is not saved in thread-specific data.
 * No reader table is used if the database is on a read-only filesystem.
 *
 * Since the database uses multi-version concurrency control, readers don't
 * actually need any locking. This table is used to keep track of which
 * readers are using data from which old transactions, so that we'll know
 * when a particular old transaction is no longer in use. Old transactions
 * that have discarded any data pages can then have those pages reclaimed
 * for use by a later write transaction.
 *
 * The lock table is constructed such that reader slots are aligned with the
 * processor's cache line size. Any slot is only ever used by one thread.
 * This alignment guarantees that there will be no contention or cache
 * thrashing as threads update their own slot info, and also eliminates
 * any need for locking when accessing a slot.
 *
 * A writer thread will scan every slot in the table to determine the oldest
 * outstanding reader transaction. Any freed pages older than this will be
 * reclaimed by the writer. The writer doesn't use any locks when scanning
 * this table. This means that there's no guarantee that the writer will
 * see the most up-to-date reader info, but that's not required for correct
 * operation - all we need is to know the upper bound on the oldest reader,
 * we don't care at all about the newest reader. So the only consequence of
 * reading stale information here is that old pages might hang around a
 * while longer before being reclaimed. That's actually good anyway, because
 * the longer we delay reclaiming old pages, the more likely it is that a
 * string of contiguous pages can be found after coalescing old pages from
 * many old transactions together. */

/* The actual reader record, with cacheline padding. */
typedef struct MDBX_reader {
  /* Current Transaction ID when this transaction began, or (txnid_t)-1.
   * Multiple readers that start at the same time will probably have the
   * same ID here. Again, it's not important to exclude them from
   * anything; all we need to know is which version of the DB they
   * started from so we can avoid overwriting any data used in that
   * particular version. */
  volatile txnid_t mr_txnid;

  /* The information we store in a single slot of the reader table.
   * In addition to a transaction ID, we also record the process and
   * thread ID that owns a slot, so that we can detect stale information,
   * e.g. threads or processes that went away without cleaning up.
   *
   * NOTE: We currently don't check for stale records.
   * We simply re-init the table when we know that we're the only process
   * opening the lock file. */

  /* The thread ID of the thread owning this txn. */
  union {
    volatile mdbx_tid_t mr_tid;
    volatile uint64_t mr_tid_u64;
  };
  /* The process ID of the process owning this reader txn. */
  union {
    volatile mdbx_pid_t mr_pid;
    volatile uint32_t mr_pid_u32;
  };
  /* The number of pages used in the reader's MVCC snapshot,
   * i.e. the value of meta->mm_geo.next and txn->mt_next_pgno */
  volatile pgno_t mr_snapshot_pages_used;
  /* Number of retired pages at the time this reader starts transaction. So,
   * at any time the difference mm_pages_retired - mr_snapshot_pages_retired
   * will give the number of pages which this reader restraining from reuse. */
  volatile uint64_t mr_snapshot_pages_retired;
} MDBX_reader;

/* The header for the reader table (a memory-mapped lock file). */
typedef struct MDBX_lockinfo {
  /* Stamp identifying this as an MDBX file.
   * It must be set to MDBX_MAGIC with with MDBX_LOCK_VERSION. */
  uint64_t mti_magic_and_version;

  /* Format of this lock file. Must be set to MDBX_LOCK_FORMAT. */
  uint32_t mti_os_and_format;

  /* Flags which environment was opened. */
  volatile uint32_t mti_envmode;

  /* Threshold of un-synced-with-disk pages for auto-sync feature,
   * zero means no-threshold, i.e. auto-sync is disabled. */
  volatile pgno_t mti_autosync_threshold;

  uint32_t reserved_pad;

  /* Period for timed auto-sync feature, i.e. at the every steady checkpoint
   * the mti_unsynced_timeout sets to the current_time + mti_autosync_period.
   * The time value is represented in a suitable system-dependent form, for
   * example clock_gettime(CLOCK_BOOTTIME) or clock_gettime(CLOCK_MONOTONIC).
   * Zero means timed auto-sync is disabled. */
  volatile uint64_t mti_autosync_period;

  /* Marker to distinguish uniqueness of DB/CLK.*/
  volatile uint64_t mti_bait_uniqueness;

  /* the hash of /proc/sys/kernel/random/boot_id or analogue */
  volatile uint64_t mti_boot_id;

  alignas(MDBX_CACHELINE_SIZE) /* cacheline ---------------------------------*/
#ifdef MDBX_OSAL_LOCK
      /* Mutex protecting write-txn. */
      MDBX_OSAL_LOCK mti_wmutex;
#endif

  volatile txnid_t mti_oldest_reader;

  /* Timestamp for auto-sync feature, i.e. the steady checkpoint should be
   * created at the first commit that will be not early this timestamp.
   * The time value is represented in a suitable system-dependent form, for
   * example clock_gettime(CLOCK_BOOTTIME) or clock_gettime(CLOCK_MONOTONIC).
   * Zero means timed auto-sync is not pending. */
  volatile uint64_t mti_unsynced_timeout;

  /* Number un-synced-with-disk pages for auto-sync feature. */
  volatile pgno_t mti_unsynced_pages;

  /* Number of page which was discarded last time by madvise(MADV_FREE). */
  volatile pgno_t mti_discarded_tail;

  /* Timestamp of the last readers check. */
  volatile uint64_t mti_reader_check_timestamp;

  alignas(MDBX_CACHELINE_SIZE) /* cacheline ---------------------------------*/

#ifdef MDBX_OSAL_LOCK
      /* Mutex protecting readers registration access to this table. */
      MDBX_OSAL_LOCK mti_rmutex;
#endif

  /* The number of slots that have been used in the reader table.
   * This always records the maximum count, it is not decremented
   * when readers release their slots. */
  volatile unsigned mti_numreaders;
  volatile unsigned mti_readers_refresh_flag;

  alignas(MDBX_CACHELINE_SIZE) /* cacheline ---------------------------------*/
      MDBX_reader mti_readers[1];
} MDBX_lockinfo;

/* Lockfile format signature: version, features and field layout */
#define MDBX_LOCK_FORMAT                                                       \
  (MDBX_OSAL_LOCK_SIGN * 27733 + (unsigned)sizeof(MDBX_reader) * 13 +          \
   (unsigned)offsetof(MDBX_reader, mr_snapshot_pages_used) * 251 +             \
   (unsigned)offsetof(MDBX_lockinfo, mti_oldest_reader) * 83 +                 \
   (unsigned)offsetof(MDBX_lockinfo, mti_numreaders) * 29)

#define MDBX_DATA_MAGIC ((MDBX_MAGIC << 8) + MDBX_DATA_VERSION)
#define MDBX_DATA_MAGIC_DEVEL ((MDBX_MAGIC << 8) + 255)

#define MDBX_LOCK_MAGIC ((MDBX_MAGIC << 8) + MDBX_LOCK_VERSION)

#ifndef MDBX_ASSUME_MALLOC_OVERHEAD
#define MDBX_ASSUME_MALLOC_OVERHEAD (sizeof(void *) * 2u)
#endif /* MDBX_ASSUME_MALLOC_OVERHEAD */

/* The maximum size of a database page.
 *
 * It is 64K, but value-PAGEHDRSZ must fit in MDBX_page.mp_upper.
 *
 * MDBX will use database pages < OS pages if needed.
 * That causes more I/O in write transactions: The OS must
 * know (read) the whole page before writing a partial page.
 *
 * Note that we don't currently support Huge pages. On Linux,
 * regular data files cannot use Huge pages, and in general
 * Huge pages aren't actually pageable. We rely on the OS
 * demand-pager to read our data and page it out when memory
 * pressure from other processes is high. So until OSs have
 * actual paging support for Huge pages, they're not viable. */
#define MAX_PAGESIZE 0x10000u
#define MIN_PAGESIZE 512u

#define MIN_MAPSIZE (MIN_PAGESIZE * MIN_PAGENO)
#if defined(_WIN32) || defined(_WIN64)
#define MAX_MAPSIZE32 UINT32_C(0x38000000)
#else
#define MAX_MAPSIZE32 UINT32_C(0x7ff80000)
#endif
#define MAX_MAPSIZE64 (MAX_PAGENO * (uint64_t)MAX_PAGESIZE)

#if MDBX_WORDBITS >= 64
#define MAX_MAPSIZE MAX_MAPSIZE64
#define MDBX_READERS_LIMIT                                                     \
  ((65536 - sizeof(MDBX_lockinfo)) / sizeof(MDBX_reader) + 1)
#else
#define MDBX_READERS_LIMIT 1024
#define MAX_MAPSIZE MAX_MAPSIZE32
#endif /* MDBX_WORDBITS */

/*----------------------------------------------------------------------------*/
/* Two kind lists of pages (aka PNL) */

/* An PNL is an Page Number List, a sorted array of IDs. The first element of
 * the array is a counter for how many actual page-numbers are in the list.
 * PNLs are sorted in descending order, this allow cut off a page with lowest
 * pgno (at the tail) just truncating the list */
#define MDBX_PNL_ASCENDING 0
typedef pgno_t *MDBX_PNL;

#if MDBX_PNL_ASCENDING
#define MDBX_PNL_ORDERED(first, last) ((first) < (last))
#define MDBX_PNL_DISORDERED(first, last) ((first) >= (last))
#else
#define MDBX_PNL_ORDERED(first, last) ((first) > (last))
#define MDBX_PNL_DISORDERED(first, last) ((first) <= (last))
#endif

/* List of txnid, only for MDBX_env.mt_lifo_reclaimed */
typedef txnid_t *MDBX_TXL;

/* An Dirty-Page list item is an pgno/pointer pair. */
typedef union MDBX_DP {
  struct {
    pgno_t pgno;
    void *ptr;
  };
  struct {
    pgno_t unused;
    unsigned length;
  };
} MDBX_DP;

/* An DPL (dirty-page list) is a sorted array of MDBX_DPs.
 * The first element's length member is a count of how many actual
 * elements are in the array. */
typedef MDBX_DP *MDBX_DPL;

/* PNL sizes - likely should be even bigger */
#define MDBX_PNL_GRANULATE 1024
#define MDBX_PNL_INITIAL                                                       \
  (MDBX_PNL_GRANULATE - 2 - MDBX_ASSUME_MALLOC_OVERHEAD / sizeof(pgno_t))
#define MDBX_PNL_MAX                                                           \
  ((1u << 24) - 2 - MDBX_ASSUME_MALLOC_OVERHEAD / sizeof(pgno_t))
#define MDBX_DPL_TXNFULL (MDBX_PNL_MAX / 4)

#define MDBX_TXL_GRANULATE 32
#define MDBX_TXL_INITIAL                                                       \
  (MDBX_TXL_GRANULATE - 2 - MDBX_ASSUME_MALLOC_OVERHEAD / sizeof(txnid_t))
#define MDBX_TXL_MAX                                                           \
  ((1u << 17) - 2 - MDBX_ASSUME_MALLOC_OVERHEAD / sizeof(txnid_t))

#define MDBX_PNL_ALLOCLEN(pl) ((pl)[-1])
#define MDBX_PNL_SIZE(pl) ((pl)[0])
#define MDBX_PNL_FIRST(pl) ((pl)[1])
#define MDBX_PNL_LAST(pl) ((pl)[MDBX_PNL_SIZE(pl)])
#define MDBX_PNL_BEGIN(pl) (&(pl)[1])
#define MDBX_PNL_END(pl) (&(pl)[MDBX_PNL_SIZE(pl) + 1])

#define MDBX_PNL_SIZEOF(pl) ((MDBX_PNL_SIZE(pl) + 1) * sizeof(pgno_t))
#define MDBX_PNL_IS_EMPTY(pl) (MDBX_PNL_SIZE(pl) == 0)

/*----------------------------------------------------------------------------*/
/* Internal structures */

/* Auxiliary DB info.
 * The information here is mostly static/read-only. There is
 * only a single copy of this record in the environment. */
typedef struct MDBX_dbx {
  MDBX_val md_name;       /* name of the database */
  MDBX_cmp_func *md_cmp;  /* function for comparing keys */
  MDBX_cmp_func *md_dcmp; /* function for comparing data items */
} MDBX_dbx;

/* A database transaction.
 * Every operation requires a transaction handle. */
struct MDBX_txn {
#define MDBX_MT_SIGNATURE UINT32_C(0x93D53A31)
  size_t mt_signature;
  MDBX_txn *mt_parent; /* parent of a nested txn */
  /* Nested txn under this txn, set together with flag MDBX_TXN_HAS_CHILD */
  MDBX_txn *mt_child;
  mdbx_geo_t mt_geo;
  /* next unallocated page */
#define mt_next_pgno mt_geo.next
  /* corresponding to the current size of datafile */
#define mt_end_pgno mt_geo.now

  /* Transaction Flags */
/* mdbx_txn_begin() flags */
#define MDBX_TXN_BEGIN_FLAGS                                                   \
  (MDBX_NOMETASYNC | MDBX_NOSYNC | MDBX_MAPASYNC | MDBX_RDONLY | MDBX_TRYTXN)
  /* internal txn flags */
#define MDBX_TXN_FINISHED 0x01  /* txn is finished or never began */
#define MDBX_TXN_ERROR 0x02     /* txn is unusable after an error */
#define MDBX_TXN_DIRTY 0x04     /* must write, even if dirty list is empty */
#define MDBX_TXN_SPILLS 0x08    /* txn or a parent has spilled pages */
#define MDBX_TXN_HAS_CHILD 0x10 /* txn has an MDBX_txn.mt_child */
/* most operations on the txn are currently illegal */
#define MDBX_TXN_BLOCKED                                                       \
  (MDBX_TXN_FINISHED | MDBX_TXN_ERROR | MDBX_TXN_HAS_CHILD)
  unsigned mt_flags;
  /* The ID of this transaction. IDs are integers incrementing from 1.
   * Only committed write transactions increment the ID. If a transaction
   * aborts, the ID may be re-used by the next writer. */
  txnid_t mt_txnid;
  MDBX_env *mt_env; /* the DB environment */
                    /* The list of reclaimed txns from freeDB */
  MDBX_TXL mt_lifo_reclaimed;
  /* The list of pages that became unused during this transaction. */
  MDBX_PNL mt_befree_pages;
  /* The list of loose pages that became unused and may be reused
   * in this transaction, linked through NEXT_LOOSE_PAGE(page). */
  MDBX_page *mt_loose_pages;
  /* Number of loose pages (mt_loose_pages) */
  unsigned mt_loose_count;
  /* The sorted list of dirty pages we temporarily wrote to disk
   * because the dirty list was full. page numbers in here are
   * shifted left by 1, deleted slots have the LSB set. */
  MDBX_PNL mt_spill_pages;
  union {
    /* For write txns: Modified pages. Sorted when not MDBX_WRITEMAP. */
    MDBX_DPL mt_rw_dirtylist;
    /* For read txns: This thread/txn's reader table slot, or NULL. */
    MDBX_reader *mt_ro_reader;
  };
  /* Array of records for each DB known in the environment. */
  MDBX_dbx *mt_dbxs;
  /* Array of MDBX_db records for each known DB */
  MDBX_db *mt_dbs;
  /* Array of sequence numbers for each DB handle */
  unsigned *mt_dbiseqs;

/* Transaction DB Flags */
#define DB_DIRTY MDBX_TBL_DIRTY /* DB was written in this txn */
#define DB_STALE MDBX_TBL_STALE /* Named-DB record is older than txnID */
#define DB_FRESH MDBX_TBL_FRESH /* Named-DB handle opened in this txn */
#define DB_CREAT MDBX_TBL_CREAT /* Named-DB handle created in this txn */
#define DB_VALID 0x10           /* DB handle is valid, see also MDBX_VALID */
#define DB_USRVALID 0x20        /* As DB_VALID, but not set for FREE_DBI */
#define DB_DUPDATA 0x40         /* DB is MDBX_DUPSORT data */
  /* In write txns, array of cursors for each DB */
  MDBX_cursor **mt_cursors;
  /* Array of flags for each DB */
  uint8_t *mt_dbflags;
  /* Number of DB records in use, or 0 when the txn is finished.
   * This number only ever increments until the txn finishes; we
   * don't decrement it when individual DB handles are closed. */
  MDBX_dbi mt_numdbs;
  /* dirtylist room: Array size - dirty pages visible to this txn.
   * Includes ancestor txns' dirty pages not hidden by other txns'
   * dirty/spilled pages. Thus commit(nested txn) has room to merge
   * dirtylist into mt_parent after freeing hidden mt_parent pages. */
  unsigned mt_dirtyroom;
  mdbx_tid_t mt_owner; /* thread ID that owns this transaction */
  mdbx_canary mt_canary;
};

/* Enough space for 2^32 nodes with minimum of 2 keys per node. I.e., plenty.
 * At 4 keys per node, enough for 2^64 nodes, so there's probably no need to
 * raise this on a 64 bit machine. */
#define CURSOR_STACK 32

struct MDBX_xcursor;

/* Cursors are used for all DB operations.
 * A cursor holds a path of (page pointer, key index) from the DB
 * root to a position in the DB, plus other state. MDBX_DUPSORT
 * cursors include an xcursor to the current data item. Write txns
 * track their cursors and keep them up to date when data moves.
 * Exception: An xcursor's pointer to a P_SUBP page can be stale.
 * (A node with F_DUPDATA but no F_SUBDATA contains a subpage). */
struct MDBX_cursor {
#define MDBX_MC_SIGNATURE UINT32_C(0xFE05D5B1)
#define MDBX_MC_READY4CLOSE UINT32_C(0x2817A047)
#define MDBX_MC_WAIT4EOT UINT32_C(0x90E297A7)
  uint32_t mc_signature;
  /* The database handle this cursor operates on */
  MDBX_dbi mc_dbi;
  /* Next cursor on this DB in this txn */
  MDBX_cursor *mc_next;
  /* Backup of the original cursor if this cursor is a shadow */
  MDBX_cursor *mc_backup;
  /* Context used for databases with MDBX_DUPSORT, otherwise NULL */
  struct MDBX_xcursor *mc_xcursor;
  /* The transaction that owns this cursor */
  MDBX_txn *mc_txn;
  /* The database record for this cursor */
  MDBX_db *mc_db;
  /* The database auxiliary record for this cursor */
  MDBX_dbx *mc_dbx;
  /* The mt_dbflag for this database */
  uint8_t *mc_dbflag;
  uint16_t mc_snum;               /* number of pushed pages */
  uint16_t mc_top;                /* index of top page, normally mc_snum-1 */
                                  /* Cursor state flags. */
#define C_INITIALIZED 0x01        /* cursor has been initialized and is valid */
#define C_EOF 0x02                /* No more data */
#define C_SUB 0x04                /* Cursor is a sub-cursor */
#define C_DEL 0x08                /* last op was a cursor_del */
#define C_UNTRACK 0x10            /* Un-track cursor when closing */
#define C_RECLAIMING 0x20         /* FreeDB lookup is prohibited */
#define C_GCFREEZE 0x40           /* me_reclaimed_pglist must not be updated */
  unsigned mc_flags;              /* see mdbx_cursor */
  MDBX_page *mc_pg[CURSOR_STACK]; /* stack of pushed pages */
  indx_t mc_ki[CURSOR_STACK];     /* stack of page indices */
};

/* Context for sorted-dup records.
 * We could have gone to a fully recursive design, with arbitrarily
 * deep nesting of sub-databases. But for now we only handle these
 * levels - main DB, optional sub-DB, sorted-duplicate DB. */
typedef struct MDBX_xcursor {
  /* A sub-cursor for traversing the Dup DB */
  MDBX_cursor mx_cursor;
  /* The database record for this Dup DB */
  MDBX_db mx_db;
  /* The auxiliary DB record for this Dup DB */
  MDBX_dbx mx_dbx;
  /* The mt_dbflag for this Dup DB */
  uint8_t mx_dbflag;
} MDBX_xcursor;

typedef struct MDBX_cursor_couple {
  MDBX_cursor outer;
  MDBX_xcursor inner;
} MDBX_cursor_couple;

/* Check if there is an inited xcursor, so XCURSOR_REFRESH() is proper */
#define XCURSOR_INITED(mc)                                                     \
  ((mc)->mc_xcursor && ((mc)->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED))

/* Update sub-page pointer, if any, in mc->mc_xcursor.
 * Needed when the node which contains the sub-page may have moved.
 * Called with mp = mc->mc_pg[mc->mc_top], ki = mc->mc_ki[mc->mc_top]. */
#define XCURSOR_REFRESH(mc, mp, ki)                                            \
  do {                                                                         \
    MDBX_page *xr_pg = (mp);                                                   \
    MDBX_node *xr_node = NODEPTR(xr_pg, ki);                                   \
    if ((xr_node->mn_flags & (F_DUPDATA | F_SUBDATA)) == F_DUPDATA)            \
      (mc)->mc_xcursor->mx_cursor.mc_pg[0] = NODEDATA(xr_node);                \
  } while (0)

/* State of FreeDB old pages, stored in the MDBX_env */
typedef struct MDBX_pgstate {
  pgno_t *mf_reclaimed_pglist; /* Reclaimed freeDB pages, or NULL before use */
  txnid_t mf_last_reclaimed;   /* ID of last used record, or 0 if
                                  !mf_reclaimed_pglist */
} MDBX_pgstate;

/* The database environment. */
struct MDBX_env {
#define MDBX_ME_SIGNATURE UINT32_C(0x9A899641)
  size_t me_signature;
  mdbx_mmap_t me_dxb_mmap; /*  The main data file */
#define me_map me_dxb_mmap.dxb
#define me_fd me_dxb_mmap.fd
#define me_mapsize me_dxb_mmap.length
  mdbx_mmap_t me_lck_mmap; /*  The lock file */
#define me_lfd me_lck_mmap.fd
#define me_lck me_lck_mmap.lck

/* Failed to update the meta page. Probably an I/O error. */
#define MDBX_FATAL_ERROR UINT32_C(0x80000000)
/* Additional flag for mdbx_sync_locked() */
#define MDBX_SHRINK_ALLOWED UINT32_C(0x40000000)
/* Some fields are initialized. */
#define MDBX_ENV_ACTIVE UINT32_C(0x20000000)
/* me_txkey is set */
#define MDBX_ENV_TXKEY UINT32_C(0x10000000)
  uint32_t me_flags;      /* see mdbx_env */
  unsigned me_psize;      /* DB page size, inited from me_os_psize */
  unsigned me_psize2log;  /* log2 of DB page size */
  unsigned me_os_psize;   /* OS page size, from mdbx_syspagesize() */
  unsigned me_maxreaders; /* size of the reader table */
  mdbx_fastmutex_t me_dbi_lock;
  MDBX_dbi me_numdbs;         /* number of DBs opened */
  MDBX_dbi me_maxdbs;         /* size of the DB table */
  mdbx_pid_t me_pid;          /* process ID of this env */
  mdbx_thread_key_t me_txkey; /* thread-key for readers */
  char *me_path;              /* path to the DB files */
  void *me_pbuf;              /* scratch area for DUPSORT put() */
  MDBX_txn *me_txn;           /* current write transaction */
  MDBX_txn *me_txn0;          /* prealloc'd write transaction */
#ifdef MDBX_OSAL_LOCK
  MDBX_OSAL_LOCK *me_wmutex; /* write-txn mutex */
#endif
  MDBX_dbx *me_dbxs;           /* array of static DB info */
  uint16_t *me_dbflags;        /* array of flags from MDBX_db.md_flags */
  unsigned *me_dbiseqs;        /* array of dbi sequence numbers */
  volatile txnid_t *me_oldest; /* ID of oldest reader last time we looked */
  MDBX_pgstate me_pgstate;     /* state of old pages from freeDB */
#define me_last_reclaimed me_pgstate.mf_last_reclaimed
#define me_reclaimed_pglist me_pgstate.mf_reclaimed_pglist
  MDBX_page *me_dpages; /* list of malloc'd blocks for re-use */
                        /* PNL of pages that became unused in a write txn */
  MDBX_PNL me_free_pgs;
  /* MDBX_DP of pages written during a write txn. Length MDBX_DPL_TXNFULL. */
  MDBX_DPL me_dirtylist;
  /* Number of freelist items that can fit in a single overflow page */
  unsigned me_maxgc_ov1page;
  /* Max size of a node on a page */
  unsigned me_nodemax;
  unsigned me_maxkey_limit;  /* max size of a key */
  mdbx_pid_t me_live_reader; /* have liveness lock in reader table */
  void *me_userctx;          /* User-settable context */
  volatile uint64_t *me_unsynced_timeout;
  volatile uint64_t *me_autosync_period;
  volatile pgno_t *me_unsynced_pages;
  volatile pgno_t *me_autosync_threshold;
  volatile pgno_t *me_discarded_tail;
  MDBX_oom_func *me_oom_func; /* Callback for kicking laggard readers */
  struct {
#ifdef MDBX_OSAL_LOCK
    MDBX_OSAL_LOCK wmutex;
#endif
    txnid_t oldest;
    uint64_t unsynced_timeout;
    uint64_t autosync_period;
    pgno_t autosync_pending;
    pgno_t autosync_threshold;
    pgno_t discarded_tail;
  } me_lckless_stub;
#if MDBX_DEBUG
  MDBX_assert_func *me_assert_func; /*  Callback for assertion failures */
#endif
#ifdef USE_VALGRIND
  int me_valgrind_handle;
#endif
  MDBX_env *me_lcklist_next;

  struct {
    size_t lower;  /* minimal size of datafile */
    size_t upper;  /* maximal size of datafile */
    size_t now;    /* current size of datafile */
    size_t grow;   /* step to grow datafile */
    size_t shrink; /* threshold to shrink datafile */
  } me_dbgeo;      /* */

#if defined(_WIN32) || defined(_WIN64)
  MDBX_srwlock me_remap_guard;
  /* Workaround for LockFileEx and WriteFile multithread bug */
  CRITICAL_SECTION me_windowsbug_lock;
#else
  mdbx_fastmutex_t me_remap_guard;
#endif
};

/* Nested transaction */
typedef struct MDBX_ntxn {
  MDBX_txn mnt_txn;         /* the transaction */
  MDBX_pgstate mnt_pgstate; /* parent transaction's saved freestate */
} MDBX_ntxn;

/*----------------------------------------------------------------------------*/
/* Debug and Logging stuff */

#define MDBX_RUNTIME_FLAGS_INIT                                                \
  (MDBX_DBG_PRINT | ((MDBX_DEBUG) > 0) * MDBX_DBG_ASSERT |                     \
   ((MDBX_DEBUG) > 1) * MDBX_DBG_AUDIT | ((MDBX_DEBUG) > 2) * MDBX_DBG_TRACE | \
   ((MDBX_DEBUG) > 3) * MDBX_DBG_EXTRA)

#ifndef mdbx_runtime_flags /* avoid override from tools */
MDBX_INTERNAL_VAR int mdbx_runtime_flags;
#endif
MDBX_INTERNAL_VAR MDBX_debug_func *mdbx_debug_logger;

MDBX_INTERNAL_FUNC void mdbx_debug_log(int type, const char *function, int line,
                                       const char *fmt, ...)
    __printf_args(4, 5);

MDBX_INTERNAL_FUNC void mdbx_panic(const char *fmt, ...) __printf_args(1, 2);

#if MDBX_DEBUG

#define mdbx_assert_enabled() unlikely(mdbx_runtime_flags &MDBX_DBG_ASSERT)

#define mdbx_audit_enabled() unlikely(mdbx_runtime_flags &MDBX_DBG_AUDIT)

#define mdbx_debug_enabled(type)                                               \
  unlikely(mdbx_runtime_flags &(type & (MDBX_DBG_TRACE | MDBX_DBG_EXTRA)))

#else
#define mdbx_debug_enabled(type) (0)
#define mdbx_audit_enabled() (0)
#if !defined(NDEBUG) || defined(MDBX_FORCE_ASSERT)
#define mdbx_assert_enabled() (1)
#else
#define mdbx_assert_enabled() (0)
#endif /* NDEBUG */
#endif /* MDBX_DEBUG */

MDBX_INTERNAL_FUNC void mdbx_assert_fail(const MDBX_env *env, const char *msg,
                                         const char *func, int line);

#define mdbx_print(fmt, ...)                                                   \
  mdbx_debug_log(MDBX_DBG_PRINT, NULL, 0, fmt, ##__VA_ARGS__)

#define mdbx_trace(fmt, ...)                                                   \
  do {                                                                         \
    if (mdbx_debug_enabled(MDBX_DBG_TRACE))                                    \
      mdbx_debug_log(MDBX_DBG_TRACE, __FUNCTION__, __LINE__, fmt "\n",         \
                     ##__VA_ARGS__);                                           \
  } while (0)

#define mdbx_verbose(fmt, ...)                                                 \
  do {                                                                         \
    if (mdbx_debug_enabled(MDBX_DBG_TRACE /* FIXME */))                        \
      mdbx_debug_log(MDBX_DBG_TRACE /* FIXME */, __FUNCTION__, __LINE__,       \
                     fmt "\n", ##__VA_ARGS__);                                 \
  } while (0)

#define mdbx_info(fmt, ...)                                                    \
  do {                                                                         \
    if (mdbx_debug_enabled(MDBX_DBG_TRACE /* FIXME */))                        \
      mdbx_debug_log(MDBX_DBG_TRACE /* FIXME */, __FUNCTION__, __LINE__,       \
                     fmt "\n", ##__VA_ARGS__);                                 \
  } while (0)

#define mdbx_notice(fmt, ...)                                                  \
  do {                                                                         \
    if (mdbx_debug_enabled(MDBX_DBG_TRACE /* FIXME */))                        \
      mdbx_debug_log(MDBX_DBG_TRACE /* FIXME */, __FUNCTION__, __LINE__,       \
                     fmt "\n", ##__VA_ARGS__);                                 \
  } while (0)

#define mdbx_warning(fmt, ...)                                                 \
  do {                                                                         \
    if (mdbx_debug_enabled(MDBX_DBG_TRACE /* FIXME */))                        \
      mdbx_debug_log(MDBX_DBG_TRACE /* FIXME */, __FUNCTION__, __LINE__,       \
                     fmt "\n", ##__VA_ARGS__);                                 \
  } while (0)

#define mdbx_error(fmt, ...)                                                   \
  do {                                                                         \
    if (mdbx_debug_enabled(MDBX_DBG_TRACE /* FIXME */))                        \
      mdbx_debug_log(MDBX_DBG_TRACE /* FIXME */, __FUNCTION__, __LINE__,       \
                     fmt "\n", ##__VA_ARGS__);                                 \
  } while (0)

#define mdbx_fatal(fmt, ...)                                                   \
  do {                                                                         \
    if (mdbx_debug_enabled(MDBX_DBG_TRACE /* FIXME */))                        \
      mdbx_debug_log(MDBX_DBG_TRACE /* FIXME */, __FUNCTION__, __LINE__,       \
                     fmt "\n", ##__VA_ARGS__);                                 \
  } while (0)

#define mdbx_debug(fmt, ...)                                                   \
  do {                                                                         \
    if (mdbx_debug_enabled(MDBX_DBG_TRACE))                                    \
      mdbx_debug_log(MDBX_DBG_TRACE, __FUNCTION__, __LINE__, fmt "\n",         \
                     ##__VA_ARGS__);                                           \
  } while (0)

#define mdbx_debug_print(fmt, ...)                                             \
  do {                                                                         \
    if (mdbx_debug_enabled(MDBX_DBG_TRACE))                                    \
      mdbx_debug_log(MDBX_DBG_TRACE, NULL, 0, fmt, ##__VA_ARGS__);             \
  } while (0)

#define mdbx_debug_extra(fmt, ...)                                             \
  do {                                                                         \
    if (mdbx_debug_enabled(MDBX_DBG_EXTRA))                                    \
      mdbx_debug_log(MDBX_DBG_EXTRA, __FUNCTION__, __LINE__, fmt,              \
                     ##__VA_ARGS__);                                           \
  } while (0)

#define mdbx_debug_extra_print(fmt, ...)                                       \
  do {                                                                         \
    if (mdbx_debug_enabled(MDBX_DBG_EXTRA))                                    \
      mdbx_debug_log(MDBX_DBG_EXTRA, NULL, 0, fmt, ##__VA_ARGS__);             \
  } while (0)

#define mdbx_ensure_msg(env, expr, msg)                                        \
  do {                                                                         \
    if (unlikely(!(expr)))                                                     \
      mdbx_assert_fail(env, msg, __FUNCTION__, __LINE__);                      \
  } while (0)

#define mdbx_ensure(env, expr) mdbx_ensure_msg(env, expr, #expr)

/* assert(3) variant in environment context */
#define mdbx_assert(env, expr)                                                 \
  do {                                                                         \
    if (mdbx_assert_enabled())                                                 \
      mdbx_ensure(env, expr);                                                  \
  } while (0)

/* assert(3) variant in cursor context */
#define mdbx_cassert(mc, expr) mdbx_assert((mc)->mc_txn->mt_env, expr)

/* assert(3) variant in transaction context */
#define mdbx_tassert(txn, expr) mdbx_assert((txn)->mt_env, expr)

#ifndef MDBX_TOOLS /* Avoid using internal mdbx_assert() */
#undef assert
#define assert(expr) mdbx_assert(NULL, expr)
#endif

/*----------------------------------------------------------------------------*/
/* Internal prototypes */

MDBX_INTERNAL_FUNC int mdbx_reader_check0(MDBX_env *env, int rlocked,
                                          int *dead);
MDBX_INTERNAL_FUNC int mdbx_rthc_alloc(mdbx_thread_key_t *key,
                                       MDBX_reader *begin, MDBX_reader *end);
MDBX_INTERNAL_FUNC void mdbx_rthc_remove(const mdbx_thread_key_t key);

MDBX_INTERNAL_FUNC void mdbx_rthc_global_init(void);
MDBX_INTERNAL_FUNC void mdbx_rthc_global_dtor(void);
MDBX_INTERNAL_FUNC void mdbx_rthc_thread_dtor(void *ptr);

#define MDBX_IS_ERROR(rc)                                                      \
  ((rc) != MDBX_RESULT_TRUE && (rc) != MDBX_RESULT_FALSE)

/* Internal error codes, not exposed outside libmdbx */
#define MDBX_NO_ROOT (MDBX_LAST_ERRCODE + 10)

/* Debuging output value of a cursor DBI: Negative in a sub-cursor. */
#define DDBI(mc)                                                               \
  (((mc)->mc_flags & C_SUB) ? -(int)(mc)->mc_dbi : (int)(mc)->mc_dbi)

/* Key size which fits in a DKBUF. */
#define DKBUF_MAXKEYSIZE 511 /* FIXME */

#if MDBX_DEBUG
#define DKBUF char _kbuf[DKBUF_MAXKEYSIZE * 4 + 2]
#define DKEY(x) mdbx_dkey(x, _kbuf, DKBUF_MAXKEYSIZE * 2 + 1)
#define DVAL(x)                                                                \
  mdbx_dkey(x, _kbuf + DKBUF_MAXKEYSIZE * 2 + 1, DKBUF_MAXKEYSIZE * 2 + 1)
#else
#define DKBUF ((void)(0))
#define DKEY(x) ("-")
#define DVAL(x) ("-")
#endif

/* An invalid page number.
 * Mainly used to denote an empty tree. */
#define P_INVALID (~(pgno_t)0)

/* Test if the flags f are set in a flag word w. */
#define F_ISSET(w, f) (((w) & (f)) == (f))

/* Round n up to an even number. */
#define EVEN(n) (((n) + 1U) & -2) /* sign-extending -2 to match n+1U */

/* Default size of memory map.
 * This is certainly too small for any actual applications. Apps should
 * always set  the size explicitly using mdbx_env_set_mapsize(). */
#define DEFAULT_MAPSIZE 1048576

/* Number of slots in the reader table.
 * This value was chosen somewhat arbitrarily. The 61 is a prime number,
 * and such readers plus a couple mutexes fit into single 4KB page.
 * Applications should set the table size using mdbx_env_set_maxreaders(). */
#define DEFAULT_READERS 61

/* Address of first usable data byte in a page, after the header */
#define PAGEDATA(p) ((void *)((char *)(p) + PAGEHDRSZ))

/* Number of nodes on a page */
#define NUMKEYS(p) ((unsigned)(p)->mp_lower >> 1)

/* The amount of space remaining in the page */
#define SIZELEFT(p) ((indx_t)((p)->mp_upper - (p)->mp_lower))

/* The percentage of space used in the page, in tenths of a percent. */
#define PAGEFILL(env, p)                                                       \
  (1024UL * ((env)->me_psize - PAGEHDRSZ - SIZELEFT(p)) /                      \
   ((env)->me_psize - PAGEHDRSZ))
/* The minimum page fill factor, in tenths of a percent.
 * Pages emptier than this are candidates for merging. */
#define FILL_THRESHOLD 256

/* Test if a page is a leaf page */
#define IS_LEAF(p) (((p)->mp_flags & P_LEAF) != 0)
/* Test if a page is a LEAF2 page */
#define IS_LEAF2(p) unlikely(((p)->mp_flags & P_LEAF2) != 0)
/* Test if a page is a branch page */
#define IS_BRANCH(p) (((p)->mp_flags & P_BRANCH) != 0)
/* Test if a page is an overflow page */
#define IS_OVERFLOW(p) unlikely(((p)->mp_flags & P_OVERFLOW) != 0)
/* Test if a page is a sub page */
#define IS_SUBP(p) (((p)->mp_flags & P_SUBP) != 0)
/* Test if a page is dirty */
#define IS_DIRTY(p) (((p)->mp_flags & P_DIRTY) != 0)

#define PAGETYPE(p) ((p)->mp_flags & (P_BRANCH | P_LEAF | P_LEAF2 | P_OVERFLOW))

/* The number of overflow pages needed to store the given size. */
#define OVPAGES(env, size) (bytes2pgno(env, PAGEHDRSZ - 1 + (size)) + 1)

/* Link in MDBX_txn.mt_loose_pages list.
 * Kept outside the page header, which is needed when reusing the page. */
#define NEXT_LOOSE_PAGE(p) (*(MDBX_page **)((p) + 2))

/* Header for a single key/data pair within a page.
 * Used in pages of type P_BRANCH and P_LEAF without P_LEAF2.
 * We guarantee 2-byte alignment for 'MDBX_node's.
 *
 * mn_lo and mn_hi are used for data size on leaf nodes, and for child
 * pgno on branch nodes.  On 64 bit platforms, mn_flags is also used
 * for pgno.  (Branch nodes have no flags).  Lo and hi are in host byte
 * order in case some accesses can be optimized to 32-bit word access.
 *
 * Leaf node flags describe node contents.  F_BIGDATA says the node's
 * data part is the page number of an overflow page with actual data.
 * F_DUPDATA and F_SUBDATA can be combined giving duplicate data in
 * a sub-page/sub-database, and named databases (just F_SUBDATA). */
typedef struct MDBX_node {
  union {
    struct {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
      union {
        struct {
          uint16_t mn_lo, mn_hi; /* part of data size or pgno */
        };
        uint32_t mn_dsize;
      };
      uint16_t mn_flags; /* see mdbx_node */
      uint16_t mn_ksize; /* key size */
#else
      uint16_t mn_ksize; /* key size */
      uint16_t mn_flags; /* see mdbx_node */
      union {
        struct {
          uint16_t mn_hi, mn_lo; /* part of data size or pgno */
        };
        uint32_t mn_dsize;
      };
#endif
    };
    pgno_t mn_ksize_and_pgno;
  };

/* mdbx_node Flags */
#define F_BIGDATA 0x01 /* data put on overflow page */
#define F_SUBDATA 0x02 /* data is a sub-database */
#define F_DUPDATA 0x04 /* data has duplicates */

/* valid flags for mdbx_node_add() */
#define NODE_ADD_FLAGS (F_DUPDATA | F_SUBDATA | MDBX_RESERVE | MDBX_APPEND)
  uint8_t mn_data[1]; /* key and data are appended here */
} MDBX_node;

/* Size of the node header, excluding dynamic data at the end */
#define NODESIZE offsetof(MDBX_node, mn_data)

/* Bit position of top word in page number, for shifting mn_flags */
#define PGNO_TOPWORD ((pgno_t)-1 > 0xffffffffu ? 32 : 0)

/* Size of a node in a branch page with a given key.
 * This is just the node header plus the key, there is no data. */
#define INDXSIZE(k) (NODESIZE + ((k) == NULL ? 0 : (k)->iov_len))

/* Size of a node in a leaf page with a given key and data.
 * This is node header plus key plus data size. */
#define LEAFSIZE(k, d) (NODESIZE + (k)->iov_len + (d)->iov_len)

/* Address of the key for the node */
#define NODEKEY(node) (void *)((node)->mn_data)

/* Address of the data for a node */
#define NODEDATA(node) (void *)((char *)(node)->mn_data + (node)->mn_ksize)

/* The size of a key in a node */
#define NODEKSZ(node) ((node)->mn_ksize)

/* The address of a key in a LEAF2 page.
 * LEAF2 pages are used for MDBX_DUPFIXED sorted-duplicate sub-DBs.
 * There are no node headers, keys are stored contiguously. */
#define LEAF2KEY(p, i, ks) ((char *)(p) + PAGEHDRSZ + ((i) * (ks)))

/* Set the node's key into keyptr, if requested. */
#define MDBX_GET_MAYNULL_KEYPTR(node, keyptr)                                  \
  do {                                                                         \
    if ((keyptr) != NULL) {                                                    \
      (keyptr)->iov_len = NODEKSZ(node);                                       \
      (keyptr)->iov_base = NODEKEY(node);                                      \
    }                                                                          \
  } while (0)

/* Set the node's key into key. */
#define MDBX_GET_KEYVALUE(node, key)                                           \
  do {                                                                         \
    key.iov_len = NODEKSZ(node);                                               \
    key.iov_base = NODEKEY(node);                                              \
  } while (0)

#define MDBX_VALID 0x8000 /* DB handle is valid, for me_dbflags */
#define PERSISTENT_FLAGS (0xffff & ~(MDBX_VALID))
/* mdbx_dbi_open() flags */
#define VALID_FLAGS                                                            \
  (MDBX_REVERSEKEY | MDBX_DUPSORT | MDBX_INTEGERKEY | MDBX_DUPFIXED |          \
   MDBX_INTEGERDUP | MDBX_REVERSEDUP | MDBX_CREATE)

/* max number of pages to commit in one writev() call */
#define MDBX_COMMIT_PAGES 64
#if defined(IOV_MAX) && IOV_MAX < MDBX_COMMIT_PAGES /* sysconf(_SC_IOV_MAX) */
#undef MDBX_COMMIT_PAGES
#define MDBX_COMMIT_PAGES IOV_MAX
#endif

/* LY: fast enough on most systems
 *
 *                /
 *                | -1, a < b
 * cmp2int(a,b) = <  0, a == b
 *                |  1, a > b
 *                \
 */
#if 1
#define mdbx_cmp2int(a, b) (((b) > (a)) ? -1 : (a) > (b))
#else
#define mdbx_cmp2int(a, b) (((a) > (b)) - ((b) > (a)))
#endif

/* Do not spill pages to disk if txn is getting full, may fail instead */
#define MDBX_NOSPILL 0x8000

static __maybe_unused __inline pgno_t pgno_add(pgno_t base, pgno_t augend) {
  assert(base <= MAX_PAGENO);
  return (augend < MAX_PAGENO - base) ? base + augend : MAX_PAGENO;
}

static __maybe_unused __inline pgno_t pgno_sub(pgno_t base, pgno_t subtrahend) {
  assert(base >= MIN_PAGENO);
  return (subtrahend < base - MIN_PAGENO) ? base - subtrahend : MIN_PAGENO;
}

static __maybe_unused __inline void mdbx_jitter4testing(bool tiny) {
#if MDBX_DEBUG
  if (MDBX_DBG_JITTER & mdbx_runtime_flags)
    mdbx_osal_jitter(tiny);
#else
  (void)tiny;
#endif
}

#if defined(_WIN32) || defined(_WIN64)
/*
 * POSIX getopt for Windows
 *
 * AT&T Public License
 *
 * Code given out at the 1985 UNIFORUM conference in Dallas.
 */

/*----------------------------------------------------------------------------*/
/* Microsoft compiler generates a lot of warning for self includes... */

#ifdef _MSC_VER
#pragma warning(push, 1)
#pragma warning(disable : 4548) /* expression before comma has no effect;      \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                 * semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling  \
                                 * mode specified; termination on exception is \
                                 * not guaranteed. Specify /EHsc */
#if !defined(_CRT_SECURE_NO_WARNINGS)
#define _CRT_SECURE_NO_WARNINGS
#endif
#endif /* _MSC_VER (warnings) */

#include <stdio.h>
#include <string.h>

#ifdef _MSC_VER
#pragma warning(pop)
#endif
/*----------------------------------------------------------------------------*/

#ifndef NULL
#define NULL 0
#endif

#ifndef EOF
#define EOF (-1)
#endif

#define ERR(s, c)                                                              \
  if (opterr) {                                                                \
    fputs(argv[0], stderr);                                                    \
    fputs(s, stderr);                                                          \
    fputc(c, stderr);                                                          \
  }

int opterr = 1;
int optind = 1;
int optopt;
char *optarg;

int getopt(int argc, char *const argv[], const char *opts) {
  static int sp = 1;
  int c;
  const char *cp;

  if (sp == 1) {
    if (optind >= argc || argv[optind][0] != '-' || argv[optind][1] == '\0')
      return EOF;
    else if (strcmp(argv[optind], "--") == 0) {
      optind++;
      return EOF;
    }
  }
  optopt = c = argv[optind][sp];
  if (c == ':' || (cp = strchr(opts, c)) == NULL) {
    ERR(": illegal option -- ", c);
    if (argv[optind][++sp] == '\0') {
      optind++;
      sp = 1;
    }
    return '?';
  }
  if (*++cp == ':') {
    if (argv[optind][sp + 1] != '\0')
      optarg = &argv[optind++][sp + 1];
    else if (++optind >= argc) {
      ERR(": option requires an argument -- ", c);
      sp = 1;
      return '?';
    } else
      optarg = argv[optind++];
    sp = 1;
  } else {
    if (argv[optind][++sp] == '\0') {
      sp = 1;
      optind++;
    }
    optarg = NULL;
  }
  return c;
}

static volatile BOOL user_break;
static BOOL WINAPI ConsoleBreakHandlerRoutine(DWORD dwCtrlType) {
  (void)dwCtrlType;
  user_break = true;
  return true;
}

#else /* WINDOWS */

static volatile sig_atomic_t user_break;
static void signal_handler(int sig) {
  (void)sig;
  user_break = 1;
}

#endif /* !WINDOWS */

static void prstat(MDBX_stat *ms) {
  printf("  Pagesize: %u\n", ms->ms_psize);
  printf("  Tree depth: %u\n", ms->ms_depth);
  printf("  Branch pages: %" PRIu64 "\n", ms->ms_branch_pages);
  printf("  Leaf pages: %" PRIu64 "\n", ms->ms_leaf_pages);
  printf("  Overflow pages: %" PRIu64 "\n", ms->ms_overflow_pages);
  printf("  Entries: %" PRIu64 "\n", ms->ms_entries);
}

static void usage(char *prog) {
  fprintf(stderr,
          "usage: %s [-V] [-n] [-e] [-r[r]] [-f[f[f]]] [-a|-s subdb] dbpath\n",
          prog);
  exit(EXIT_FAILURE);
}

static int reader_list_func(void *ctx, int num, int slot, mdbx_pid_t pid,
                            mdbx_tid_t thread, uint64_t txnid, uint64_t lag,
                            size_t bytes_used, size_t bytes_retired) {
  (void)ctx;
  if (num == 1)
    printf("Reader Table Status\n"
           "   #\tslot\t%6s %*s %20s %10s %13s %13s\n",
           "pid", (int)sizeof(size_t) * 2, "thread", "txnid", "lag", "used",
           "retained");

  printf(" %3d)\t[%d]\t%6" PRIdSIZE " %*" PRIxSIZE, num, slot, (size_t)pid,
         (int)sizeof(size_t) * 2, (size_t)thread);
  if (txnid)
    printf(" %20" PRIu64 " %10" PRIu64 " %12.1fM %12.1fM\n", txnid, lag,
           bytes_used / 1048576.0, bytes_retired / 1048576.0);
  else
    printf(" %20s %10s %13s %13s\n", "-", "0", "0", "0");

  return user_break ? MDBX_RESULT_TRUE : MDBX_RESULT_FALSE;
}

int main(int argc, char *argv[]) {
  int o, rc;
  MDBX_env *env;
  MDBX_txn *txn;
  MDBX_dbi dbi;
  MDBX_stat mst;
  MDBX_envinfo mei;
  char *prog = argv[0];
  char *envname;
  char *subname = NULL;
  int alldbs = 0, envinfo = 0, envflags = 0, freinfo = 0, rdrinfo = 0;

  if (argc < 2)
    usage(prog);

  /* -a: print stat of main DB and all subDBs
   * -s: print stat of only the named subDB
   * -e: print env info
   * -f: print freelist info
   * -r: print reader info
   * -n: use NOSUBDIR flag on env_open
   * -V: print version and exit
   * (default) print stat of only the main DB
   */
  while ((o = getopt(argc, argv, "Vaefnrs:")) != EOF) {
    switch (o) {
    case 'V':
      printf("mdbx_stat version %d.%d.%d.%d\n"
             " - source: %s %s, commit %s, tree %s\n"
             " - anchor: %s\n"
             " - build: %s for %s by %s\n"
             " - flags: %s\n"
             " - options: %s\n",
             mdbx_version.major, mdbx_version.minor, mdbx_version.release,
             mdbx_version.revision, mdbx_version.git.describe,
             mdbx_version.git.datetime, mdbx_version.git.commit,
             mdbx_version.git.tree, mdbx_sourcery_anchor, mdbx_build.datetime,
             mdbx_build.target, mdbx_build.compiler, mdbx_build.flags,
             mdbx_build.options);
      return EXIT_SUCCESS;
    case 'a':
      if (subname)
        usage(prog);
      alldbs++;
      break;
    case 'e':
      envinfo++;
      break;
    case 'f':
      freinfo++;
      break;
    case 'n':
      envflags |= MDBX_NOSUBDIR;
      break;
    case 'r':
      rdrinfo++;
      break;
    case 's':
      if (alldbs)
        usage(prog);
      subname = optarg;
      break;
    default:
      usage(prog);
    }
  }

  if (optind != argc - 1)
    usage(prog);

#if defined(_WIN32) || defined(_WIN64)
  SetConsoleCtrlHandler(ConsoleBreakHandlerRoutine, true);
#else
#ifdef SIGPIPE
  signal(SIGPIPE, signal_handler);
#endif
#ifdef SIGHUP
  signal(SIGHUP, signal_handler);
#endif
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
#endif /* !WINDOWS */

  envname = argv[optind];
  envname = argv[optind];
  printf("mdbx_stat %s (%s, T-%s)\nRunning for %s...\n",
         mdbx_version.git.describe, mdbx_version.git.datetime,
         mdbx_version.git.tree, envname);
  fflush(NULL);

  rc = mdbx_env_create(&env);
  if (rc) {
    fprintf(stderr, "mdbx_env_create failed, error %d %s\n", rc,
            mdbx_strerror(rc));
    return EXIT_FAILURE;
  }

  if (alldbs || subname)
    mdbx_env_set_maxdbs(env, 4);

  rc = mdbx_env_open(env, envname, envflags | MDBX_RDONLY, 0664);
  if (rc) {
    fprintf(stderr, "mdbx_env_open failed, error %d %s\n", rc,
            mdbx_strerror(rc));
    goto env_close;
  }

  if (envinfo || freinfo) {
    (void)mdbx_env_info(env, &mei, sizeof(mei));
  } else {
    /* LY: zap warnings from gcc */
    memset(&mei, 0, sizeof(mei));
  }

  if (envinfo) {
    (void)mdbx_env_stat(env, &mst, sizeof(mst));
    printf("Environment Info\n");
    printf("  Pagesize: %u\n", mst.ms_psize);
    if (mei.mi_geo.lower != mei.mi_geo.upper) {
      printf("  Dynamic datafile: %" PRIu64 "..%" PRIu64 " bytes (+%" PRIu64
             "/-%" PRIu64 "), %" PRIu64 "..%" PRIu64 " pages (+%" PRIu64
             "/-%" PRIu64 ")\n",
             mei.mi_geo.lower, mei.mi_geo.upper, mei.mi_geo.grow,
             mei.mi_geo.shrink, mei.mi_geo.lower / mst.ms_psize,
             mei.mi_geo.upper / mst.ms_psize, mei.mi_geo.grow / mst.ms_psize,
             mei.mi_geo.shrink / mst.ms_psize);
      printf("  Current datafile: %" PRIu64 " bytes, %" PRIu64 " pages\n",
             mei.mi_geo.current, mei.mi_geo.current / mst.ms_psize);
    } else {
      printf("  Fixed datafile: %" PRIu64 " bytes, %" PRIu64 " pages\n",
             mei.mi_geo.current, mei.mi_geo.current / mst.ms_psize);
    }
    printf("  Current mapsize: %" PRIu64 " bytes, %" PRIu64 " pages \n",
           mei.mi_mapsize, mei.mi_mapsize / mst.ms_psize);
    printf("  Number of pages used: %" PRIu64 "\n", mei.mi_last_pgno + 1);
    printf("  Last transaction ID: %" PRIu64 "\n", mei.mi_recent_txnid);
    printf("  Tail transaction ID: %" PRIu64 " (%" PRIi64 ")\n",
           mei.mi_latter_reader_txnid,
           mei.mi_latter_reader_txnid - mei.mi_recent_txnid);
    printf("  Max readers: %u\n", mei.mi_maxreaders);
    printf("  Number of readers used: %u\n", mei.mi_numreaders);
  } else {
    /* LY: zap warnings from gcc */
    memset(&mst, 0, sizeof(mst));
  }

  if (rdrinfo) {
    rc = mdbx_reader_list(env, reader_list_func, nullptr);
    if (rc == MDBX_RESULT_TRUE)
      printf("Reader Table is empty\n");
    else if (rc == MDBX_SUCCESS && rdrinfo > 1) {
      int dead;
      rc = mdbx_reader_check(env, &dead);
      if (rc == MDBX_RESULT_TRUE) {
        printf("  %d stale readers cleared.\n", dead);
        rc = mdbx_reader_list(env, reader_list_func, nullptr);
        if (rc == MDBX_RESULT_TRUE)
          printf("  Now Reader Table is empty\n");
      } else
        printf("  No stale readers.\n");
    }
    if (MDBX_IS_ERROR(rc)) {
      fprintf(stderr, "mdbx_txn_begin failed, error %d %s\n", rc,
              mdbx_strerror(rc));
      goto env_close;
    }
    if (!(subname || alldbs || freinfo))
      goto env_close;
  }

  rc = mdbx_txn_begin(env, NULL, MDBX_RDONLY, &txn);
  if (rc) {
    fprintf(stderr, "mdbx_txn_begin failed, error %d %s\n", rc,
            mdbx_strerror(rc));
    goto env_close;
  }

  if (freinfo) {
    MDBX_cursor *cursor;
    MDBX_val key, data;
    pgno_t pages = 0, *iptr;
    pgno_t reclaimable = 0;

    printf("Freelist Status\n");
    dbi = 0;
    rc = mdbx_cursor_open(txn, dbi, &cursor);
    if (rc) {
      fprintf(stderr, "mdbx_cursor_open failed, error %d %s\n", rc,
              mdbx_strerror(rc));
      goto txn_abort;
    }
    rc = mdbx_dbi_stat(txn, dbi, &mst, sizeof(mst));
    if (rc) {
      fprintf(stderr, "mdbx_dbi_stat failed, error %d %s\n", rc,
              mdbx_strerror(rc));
      goto txn_abort;
    }
    prstat(&mst);
    while ((rc = mdbx_cursor_get(cursor, &key, &data, MDBX_NEXT)) ==
           MDBX_SUCCESS) {
      if (user_break) {
        rc = MDBX_EINTR;
        break;
      }
      iptr = data.iov_base;
      const pgno_t number = *iptr++;

      pages += number;
      if (envinfo && mei.mi_latter_reader_txnid > *(size_t *)key.iov_base)
        reclaimable += number;

      if (freinfo > 1) {
        char *bad = "";
        pgno_t prev =
            MDBX_PNL_ASCENDING ? NUM_METAS - 1 : (pgno_t)mei.mi_last_pgno + 1;
        pgno_t span = 1;
        for (unsigned i = 0; i < number; ++i) {
          pgno_t pg = iptr[i];
          if (MDBX_PNL_DISORDERED(prev, pg))
            bad = " [bad sequence]";
          prev = pg;
          while (i + span < number &&
                 iptr[i + span] == (MDBX_PNL_ASCENDING ? pgno_add(pg, span)
                                                       : pgno_sub(pg, span)))
            ++span;
        }
        printf("    Transaction %" PRIaTXN ", %" PRIaPGNO
               " pages, maxspan %" PRIaPGNO "%s\n",
               *(txnid_t *)key.iov_base, number, span, bad);
        if (freinfo > 2) {
          for (unsigned i = 0; i < number; i += span) {
            const pgno_t pg = iptr[i];
            for (span = 1;
                 i + span < number &&
                 iptr[i + span] == (MDBX_PNL_ASCENDING ? pgno_add(pg, span)
                                                       : pgno_sub(pg, span));
                 ++span)
              ;
            if (span > 1)
              printf("     %9" PRIaPGNO "[%" PRIaPGNO "]\n", pg, span);
            else
              printf("     %9" PRIaPGNO "\n", pg);
          }
        }
      }
    }
    mdbx_cursor_close(cursor);

    switch (rc) {
    case MDBX_SUCCESS:
    case MDBX_NOTFOUND:
      break;
    case MDBX_EINTR:
      fprintf(stderr, "Interrupted by signal/user\n");
      goto txn_abort;
    default:
      fprintf(stderr, "mdbx_cursor_get failed, error %d %s\n", rc,
              mdbx_strerror(rc));
      goto txn_abort;
    }

    if (envinfo) {
      uint64_t value = mei.mi_mapsize / mst.ms_psize;
      double percent = value / 100.0;
      printf("Page Allocation Info\n");
      printf("  Max pages: %" PRIu64 " 100%%\n", value);

      value = mei.mi_last_pgno + 1;
      printf("  Pages used: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = mei.mi_mapsize / mst.ms_psize - (mei.mi_last_pgno + 1);
      printf("  Remained: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = mei.mi_last_pgno + 1 - pages;
      printf("  Used now: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = pages;
      printf("  Unallocated: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = pages - reclaimable;
      printf("  Detained: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = reclaimable;
      printf("  Reclaimable: %" PRIu64 " %.1f%%\n", value, value / percent);

      value =
          mei.mi_mapsize / mst.ms_psize - (mei.mi_last_pgno + 1) + reclaimable;
      printf("  Available: %" PRIu64 " %.1f%%\n", value, value / percent);
    } else
      printf("  Free pages: %" PRIaPGNO "\n", pages);
  }

  rc = mdbx_dbi_open(txn, subname, 0, &dbi);
  if (rc) {
    fprintf(stderr, "mdbx_open failed, error %d %s\n", rc, mdbx_strerror(rc));
    goto txn_abort;
  }

  rc = mdbx_dbi_stat(txn, dbi, &mst, sizeof(mst));
  if (rc) {
    fprintf(stderr, "mdbx_dbi_stat failed, error %d %s\n", rc,
            mdbx_strerror(rc));
    goto txn_abort;
  }
  printf("Status of %s\n", subname ? subname : "Main DB");
  prstat(&mst);

  if (alldbs) {
    MDBX_cursor *cursor;
    MDBX_val key;

    rc = mdbx_cursor_open(txn, dbi, &cursor);
    if (rc) {
      fprintf(stderr, "mdbx_cursor_open failed, error %d %s\n", rc,
              mdbx_strerror(rc));
      goto txn_abort;
    }
    while ((rc = mdbx_cursor_get(cursor, &key, NULL, MDBX_NEXT_NODUP)) == 0) {
      char *str;
      MDBX_dbi db2;
      if (memchr(key.iov_base, '\0', key.iov_len))
        continue;
      str = mdbx_malloc(key.iov_len + 1);
      memcpy(str, key.iov_base, key.iov_len);
      str[key.iov_len] = '\0';
      rc = mdbx_dbi_open(txn, str, 0, &db2);
      if (rc == MDBX_SUCCESS)
        printf("Status of %s\n", str);
      mdbx_free(str);
      if (rc)
        continue;
      rc = mdbx_dbi_stat(txn, db2, &mst, sizeof(mst));
      if (rc) {
        fprintf(stderr, "mdbx_dbi_stat failed, error %d %s\n", rc,
                mdbx_strerror(rc));
        goto txn_abort;
      }
      prstat(&mst);
      mdbx_dbi_close(env, db2);
    }
    mdbx_cursor_close(cursor);
  }

  if (rc == MDBX_NOTFOUND)
    rc = MDBX_SUCCESS;

  mdbx_dbi_close(env, dbi);
txn_abort:
  mdbx_txn_abort(txn);
env_close:
  mdbx_env_close(env);

  return rc ? EXIT_FAILURE : EXIT_SUCCESS;
}
