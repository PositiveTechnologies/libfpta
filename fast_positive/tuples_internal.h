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

/* *INDENT-OFF* */
/* clang-format off */

#ifndef NOMINMAX
#	define NOMINMAX
#endif

#include "fast_positive/tuples.h"

#ifdef _MSC_VER

#if _MSC_VER < 1900
#pragma warning(disable : 4350) /* behavior change: 'std::_Wrap_alloc... */
#endif

#if _MSC_VER > 1913
#   pragma warning(disable : 5045) /* will insert Spectre mitigation... */
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
#pragma warning(disable : 4702) /* unreachable code */

#pragma warning(push, 1)
#pragma warning(disable : 4548) /* expression before comma has no effect;      \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                   semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling  \
                                   mode specified; termination on exception    \
                                   is not guaranteed. Specify /EHsc */
#endif                          /* _MSC_VER (warnings) */

#if defined(__GNUC__) && !__GNUC_PREREQ(4,2)
	/* Actualy libfptu was not tested with compilers older than GCC from RHEL6.
	 * But you could remove this #error and try to continue at your own risk.
	 * In such case please don't rise up an issues related ONLY to old compilers. */
#	error "libfptu required at least GCC 4.2 compatible C/C++ compiler."
#endif

#if defined(__GLIBC__) && !__GLIBC_PREREQ(2,12)
	/* Actualy libfptu requires just C99 (e.g glibc >= 2.1), but was
	 * not tested with glibc older than 2.12 (from RHEL6). So you could
	 * remove this #error and try to continue at your own risk.
	 * In such case please don't rise up an issues related ONLY to old glibc. */
#	error "libfptu required at least glibc version 2.12 or later."
#endif

#include <limits.h>
#include <string.h>
#if !(defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__) ||   \
      defined(__BSD__) || defined(__NETBSD__) || defined(__bsdi__) ||          \
      defined(__DragonFly__) || defined(__APPLE__) || defined(__MACH__))
#include <malloc.h>
#endif /* xBSD */

#include <cinttypes> // for PRId64, PRIu64
#include <cmath>     // for exp2()
#include <cstdarg>   // for va_list
#include <cstdio>    // for _vscprintf()
#include <cstdlib>   // for snprintf()
#include <ctime>     // for gmtime()

#ifdef HAVE_VALGRIND_MEMCHECK_H
        /* Get debugging help from Valgrind */
#       include <valgrind/memcheck.h>
#       ifndef VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE
                /* LY: available since Valgrind 3.10 */
#               define VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#               define VALGRIND_ENABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#       endif
#else
#       define VALGRIND_CREATE_MEMPOOL(h,r,z)
#       define VALGRIND_DESTROY_MEMPOOL(h)
#       define VALGRIND_MEMPOOL_TRIM(h,a,s)
#       define VALGRIND_MEMPOOL_ALLOC(h,a,s)
#       define VALGRIND_MEMPOOL_FREE(h,a)
#       define VALGRIND_MEMPOOL_CHANGE(h,a,b,s)
#       define VALGRIND_MAKE_MEM_NOACCESS(a,s)
#       define VALGRIND_MAKE_MEM_DEFINED(a,s)
#       define VALGRIND_MAKE_MEM_UNDEFINED(a,s)
#       define VALGRIND_DISABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#       define VALGRIND_ENABLE_ADDR_ERROR_REPORTING_IN_RANGE(a,s)
#       define VALGRIND_CHECK_MEM_IS_ADDRESSABLE(a,s) (0)
#       define VALGRIND_CHECK_MEM_IS_DEFINED(a,s) (0)
#endif /* HAVE_VALGRIND_MEMCHECK_H */

#ifdef _MSC_VER
#pragma warning(pop)
#endif

//----------------------------------------------------------------------------

#ifdef __cplusplus
#	define FPT_NONCOPYABLE(typename) \
		typename(const typename&) = delete; \
		typename& operator=(typename const&) = delete
#endif

#if !defined(__typeof) && defined(_MSC_VER)
#	define __typeof(exp) decltype(exp)
#endif

#if !defined(__noop) && !defined(_MSC_VER)
#	ifdef __cplusplus
		static inline void __noop_consume_args() {}
		template <typename First, typename... Rest>
		static inline void
		__noop_consume_args(const First &first, const Rest &... rest) {
			(void) first; __noop_consume_args(rest...);
		}
#		define __noop(...) __noop_consume_args(__VA_ARGS__)
#	elif defined(__GNUC__) && (!defined(__STRICT_ANSI__) || !__STRICT_ANSI__)
		static __inline void __noop_consume_args(void* anchor, ...) {
			(void) anchor;
		}
#		define __noop(...) __noop_consume_args(0, ##__VA_ARGS__)
#	else
#		define __noop(...) do {} while(0)
#	endif
#endif /* __noop */

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

#ifndef __expect_equal
#	if defined(__GNUC__) || defined(__clang__)
#		define __expect_equal(exp, c) __builtin_expect((exp), (c))
#	else
#		define __expect_equal(exp, c) (exp)
#	endif
#endif /* __expect_equal */

#ifndef likely
#	if defined(__GNUC__) || defined(__clang__)
#		define likely(cond) __builtin_expect(!!(cond), 1)
#	else
#		define likely(x) (x)
#	endif
#endif /* likely */

#ifndef unlikely
#	if defined(__GNUC__) || defined(__clang__)
#		define unlikely(cond) __builtin_expect(!!(cond), 0)
#	else
#		define unlikely(x) (x)
#	endif
#endif /* unlikely */

#ifndef __aligned
#	if defined(__GNUC__) || defined(__clang__)
#		define __aligned(N) __attribute__((__aligned__(N)))
#	elif defined(_MSC_VER)
#		define __aligned(N) __declspec(align(N))
#	else
#		define __aligned(N)
#	endif
#endif /* __align */

#ifndef CACHELINE_SIZE
#	if defined(SYSTEM_CACHE_ALIGNMENT_SIZE)
#		define CACHELINE_SIZE SYSTEM_CACHE_ALIGNMENT_SIZE
#	elif defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
#		define CACHELINE_SIZE 128
#	else
#		define CACHELINE_SIZE 64
#	endif
#endif /* CACHELINE_SIZE */

#ifndef __cache_aligned
#	define __cache_aligned __aligned(CACHELINE_SIZE)
#endif

#if (UINTPTR_MAX > 0xffffFFFFul || ULONG_MAX > 0xffffFFFFul)
#define FPT_ARCH64
#else
#define FPT_ARCH32
#endif /* FPT_ARCH64/32 */

//----------------------------------------------------------------------------

#ifdef __cplusplus
	template <typename T, size_t N> char (&__FPT_ArraySizeHelper(T (&)[N]))[N];
#	define FPT_ARRAY_LENGTH(array) (sizeof(::__FPT_ArraySizeHelper(array)))
#else
#	define FPT_ARRAY_LENGTH(array) (sizeof(array) / sizeof(array[0]))
#endif

#define FPT_ARRAY_END(array) (&array[FPT_ARRAY_LENGTH(array)])

#define FPT_STR(x) #x
#define FPT_STRINGIFY(x) FPT_STR(x)

#ifndef offsetof
#	define offsetof(type, member) __builtin_offsetof(type, member)
#endif

#ifndef container_of
#define container_of(ptr, type, member)                                        \
  ({                                                                           \
    const __typeof(((type *)nullptr)->member) *__ptr = (ptr);                  \
    (type *)((char *)__ptr - offsetof(type, member));                          \
  })
#endif /* container_of */

#define FPT_IS_POWER2(value) (((value) & ((value)-1)) == 0 && (value) > 0)
#define __FPT_FLOOR_MASK(type, value, mask) ((value) & ~(type)(mask))
#define __FPT_CEIL_MASK(type, value, mask)                                     \
  __FPT_FLOOR_MASK(type, (value) + (mask), mask)
#define FPT_ALIGN_FLOOR(value, align)                                          \
  __FPT_FLOOR_MASK(__typeof(value), value, (__typeof(value))(align)-1)
#define FPT_ALIGN_CEIL(value, align)                                           \
  __FPT_CEIL_MASK(__typeof(value), value, (__typeof(value))(align)-1)
#define FPT_IS_ALIGNED(ptr, align)                                             \
  ((((uintptr_t)(align)-1) & (uintptr_t)(ptr)) == 0)

/* *INDENT-ON* */
/* clang-format on */
//----------------------------------------------------------------------------

#if _POSIX_C_SOURCE > 200212 &&                                                \
    /* workaround for avoid musl libc wrong prototype */ (                     \
        defined(__GLIBC__) || defined(__GNU_LIBRARY__))
/* Prototype should match libc runtime. ISO POSIX (2003) & LSB 1.x-3.x */
__extern_C void __assert_fail(const char *assertion, const char *file,
                              unsigned line, const char *function)
#ifdef __THROW
    __THROW
#else
    __nothrow
#endif /* __THROW */
    __noreturn;

#elif defined(__APPLE__) || defined(__MACH__)
__extern_C void __assert_rtn(const char *function, const char *file, int line,
                             const char *assertion) /* __nothrow */
#ifdef __dead2
    __dead2
#else
    __noreturn
#endif /* __dead2 */
#ifdef __disable_tail_calls
    __disable_tail_calls
#endif /* __disable_tail_calls */
    ;

#define __assert_fail(assertion, file, line, function)                         \
  __assert_rtn(function, file, line, assertion)
#elif defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__) ||   \
    defined(__BSD__) || defined(__NETBSD__) || defined(__bsdi__) ||            \
    defined(__DragonFly__)
__extern_C void __assert(const char *function, const char *file, int line,
                         const char *assertion) /* __nothrow */
#ifdef __dead2
    __dead2
#else
    __noreturn
#endif /* __dead2 */
#ifdef __disable_tail_calls
    __disable_tail_calls
#endif /* __disable_tail_calls */
    ;
#define __assert_fail(assertion, file, line, function)                         \
  __assert(function, file, line, assertion)

#endif /* __assert_fail */

static __inline bool is_filter(fptu_type_or_filter type_or_filter) {
  return (((uint32_t)type_or_filter) >= fptu_ffilter) ? true : false;
}

static __inline bool match(const fptu_field *pf, unsigned column,
                           fptu_type_or_filter type_or_filter) {
  if (pf->colnum() != column)
    return false;
  if (is_filter(type_or_filter))
    return ((fptu_filter)type_or_filter & fptu_filter_mask(pf->type())) ? true
                                                                        : false;
  return (fptu_type)type_or_filter == pf->type();
}

static __inline size_t bytes2units(size_t bytes) {
  return (bytes + fptu_unit_size - 1) >> fptu_unit_shift;
}

static __inline size_t units2bytes(size_t units) {
  return units << fptu_unit_shift;
}

static __inline size_t tag_elem_size(uint_fast16_t tag) {
  fptu_type type = fptu_get_type(tag);
  if (likely(fptu_tag_is_fixedsize(type)))
    return fptu_internal_map_t2b[type];

  /* fptu_opaque, fptu_cstr or fptu_farray.
   * at least 4 bytes for length or '\0'. */
  return fptu_unit_size;
}

static __inline bool tag_match_fixedsize(uint_fast16_t tag, size_t units) {
  return fptu_tag_is_fixedsize(tag) &&
         units == fptu_internal_map_t2u[fptu_get_type(tag)];
}

__pure_function size_t fptu_field_units(const fptu_field *pf);

static __inline const void *fptu_ro_detent(fptu_ro ro) {
  return (char *)ro.sys.iov_base + ro.sys.iov_len;
}

static __inline const void *fptu_detent(const fptu_rw *rw) {
  return &rw->units[rw->end];
}

fptu_field *fptu_lookup_tag(fptu_rw *pt, uint_fast16_t tag);

template <typename type>
static __inline fptu_lge fptu_cmp2lge(type left, type right) {
  if (left == right)
    return fptu_eq;
  return (left < right) ? fptu_lt : fptu_gt;
}

template <typename type> static __inline fptu_lge fptu_diff2lge(type diff) {
  return fptu_cmp2lge<type>(diff, 0);
}

fptu_lge fptu_cmp2lge(fptu_lge left, fptu_lge right) = delete;
fptu_lge fptu_diff2lge(fptu_lge diff) = delete;

static __inline fptu_lge fptu_cmp_binary_str(const void *left_data,
                                             size_t left_len,
                                             const char *right_cstr) {
  size_t right_len = right_cstr ? strlen(right_cstr) : 0;
  return fptu_cmp_binary(left_data, left_len, right_cstr, right_len);
}

static __inline fptu_lge fptu_cmp_str_binary(const char *left_cstr,
                                             const void *right_data,
                                             size_t right_len) {
  size_t left_len = left_cstr ? strlen(left_cstr) : 0;
  return fptu_cmp_binary(left_cstr, left_len, right_data, right_len);
}

template <typename type>
static __inline int fptu_cmp2int(type left, type right) {
  return (right > left) ? -1 : left > right;
}

template <typename iterator>
static __inline fptu_lge
fptu_depleted2lge(const iterator &left_pos, const iterator &left_end,
                  const iterator &right_pos, const iterator &right_end) {
  bool left_depleted = (left_pos >= left_end);
  bool right_depleted = (right_pos >= right_end);

  if (left_depleted == right_depleted)
    return fptu_eq;
  return left_depleted ? fptu_lt : fptu_gt;
}

#ifdef _MSC_VER

#ifndef snprintf
#define snprintf(buffer, buffer_size, format, ...)                             \
  _snprintf_s(buffer, buffer_size, _TRUNCATE, format, __VA_ARGS__)
#endif /* snprintf */

#ifndef vsnprintf
#define vsnprintf(buffer, buffer_size, format, args)                           \
  _vsnprintf_s(buffer, buffer_size, _TRUNCATE, format, args)
#endif /* vsnprintf */

#ifdef _ASSERTE
#undef assert
#define assert _ASSERTE
#endif

#if _MSC_VER >= 1900
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
#endif /* fix PRI*PTR for _MSC_VER >= 1900 */

#endif /* _MSC_VER */

#ifndef PRIuSIZE
#define PRIuSIZE PRIuPTR
#define PRIiSIZE PRIiPTR
#define PRIdSIZE PRIdPTR
#define PRIxSIZE PRIxPTR
#endif /* PRI*SIZE macros for MSVC */

/*----------------------------------------------------------------------------*/
/* LY: temporary workaround for Elbrus's memcmp() bug. */
#if defined(__e2k__) && !__GLIBC_PREREQ(2, 24)
extern "C" int mdbx_e2k_memcmp_bug_workaround(const void *s1, const void *s2,
                                              size_t n);
extern "C" int mdbx_e2k_strcmp_bug_workaround(const char *s1, const char *s2);
extern "C" int mdbx_e2k_strncmp_bug_workaround(const char *s1, const char *s2,
                                               size_t n);
extern "C" size_t mdbx_e2k_strlen_bug_workaround(const char *s);
extern "C" size_t mdbx_e2k_strnlen_bug_workaround(const char *s, size_t maxlen);
#include <string.h>
#include <strings.h>
#undef memcmp
#define memcmp mdbx_e2k_memcmp_bug_workaround
#undef bcmp
#define bcmp mdbx_e2k_memcmp_bug_workaround
#undef strcmp
#define strcmp mdbx_e2k_strcmp_bug_workaround
#undef strncmp
#define strncmp mdbx_e2k_strncmp_bug_workaround
#undef strlen
#define strlen mdbx_e2k_strlen_bug_workaround
#undef strnlen
#define strnlen mdbx_e2k_strnlen_bug_workaround
#endif /* Elbrus's memcmp() bug. */
