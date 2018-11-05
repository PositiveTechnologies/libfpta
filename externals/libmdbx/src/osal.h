/* https://en.wikipedia.org/wiki/Operating_system_abstraction_layer */

/*
 * Copyright 2015-2018 Leonid Yuriev <leo@yuriev.ru>
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

#pragma once

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
#if !defined(_NO_CRT_STDIO_INLINE) && defined(MDBX_BUILD_DLL)
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
#include <malloc.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#ifndef _POSIX_C_SOURCE
#ifdef _POSIX_SOURCE
#define _POSIX_C_SOURCE 1
#else
#define _POSIX_C_SOURCE 0
#endif
#endif

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 0
#endif

/*----------------------------------------------------------------------------*/
/* Systems includes */

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

#ifdef MDBX_AVOID_CRT
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

static __inline void mdbx_compiler_barrier(void) {
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

static __inline void mdbx_memory_barrier(void) {
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

#ifndef MDBX_CACHE_IS_COHERENT
#if defined(__ia32__) || defined(__e2k__) || defined(__hppa) ||                \
    defined(__hppa__)
#define MDBX_CACHE_IS_COHERENT 1
#else
#define MDBX_CACHE_IS_COHERENT 0
#endif
#endif /* MDBX_CACHE_IS_COHERENT */

#ifndef MDBX_CACHELINE_SIZE
#if defined(SYSTEM_CACHE_ALIGNMENT_SIZE)
#define MDBX_CACHELINE_SIZE SYSTEM_CACHE_ALIGNMENT_SIZE
#elif defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
#define MDBX_CACHELINE_SIZE 128
#else
#define MDBX_CACHELINE_SIZE 64
#endif
#endif /* MDBX_CACHELINE_SIZE */

#if MDBX_CACHE_IS_COHERENT
#define mdbx_coherent_barrier() mdbx_compiler_barrier()
#else
#define mdbx_coherent_barrier() mdbx_memory_barrier()
#endif

#if defined(__mips) || defined(__mips__) || defined(__mips64) ||               \
    defined(__mips64) || defined(_M_MRX000) || defined(_MIPS_)
/* Only MIPS has explicit cache control */
#include <sys/cachectl.h>
#endif

static __inline void mdbx_invalidate_cache(void *addr, size_t nbytes) {
  mdbx_coherent_barrier();
#if defined(__mips) || defined(__mips__) || defined(__mips64) ||               \
    defined(__mips64) || defined(_M_MRX000) || defined(_MIPS_)
#if defined(DCACHE)
  /* MIPS has cache coherency issues.
   * Note: for any nbytes >= on-chip cache size, entire is flushed. */
  cacheflush(addr, nbytes, DCACHE);
#else
#error "Sorry, cacheflush() for MIPS not implemented"
#endif /* __mips__ */
#else
  /* LY: assume no relevant mmap/dcache issues. */
  (void)addr;
  (void)nbytes;
#endif
}

/*----------------------------------------------------------------------------*/
/* libc compatibility stuff */

#if __GLIBC_PREREQ(2, 1)
#define mdbx_asprintf asprintf
#define mdbx_vasprintf vasprintf
#else
__printf_args(2, 3) int mdbx_asprintf(char **strp, const char *fmt, ...);
int mdbx_vasprintf(char **strp, const char *fmt, va_list ap);
#endif

/*----------------------------------------------------------------------------*/
/* OS abstraction layer stuff */

/* max bytes to write in one call */
#define MAX_WRITE UINT32_C(0x3fff0000)

/* Get the size of a memory page for the system.
 * This is the basic size that the platform's memory manager uses, and is
 * fundamental to the use of memory-mapped files. */
static __inline size_t mdbx_syspagesize(void) {
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

static __inline int mdbx_get_errno(void) {
#if defined(_WIN32) || defined(_WIN64)
  DWORD rc = GetLastError();
#else
  int rc = errno;
#endif
  return rc;
}

#ifndef mdbx_memalign_alloc
int mdbx_memalign_alloc(size_t alignment, size_t bytes, void **result);
#endif
#ifndef mdbx_memalign_free
void mdbx_memalign_free(void *ptr);
#endif

int mdbx_condmutex_init(mdbx_condmutex_t *condmutex);
int mdbx_condmutex_lock(mdbx_condmutex_t *condmutex);
int mdbx_condmutex_unlock(mdbx_condmutex_t *condmutex);
int mdbx_condmutex_signal(mdbx_condmutex_t *condmutex);
int mdbx_condmutex_wait(mdbx_condmutex_t *condmutex);
int mdbx_condmutex_destroy(mdbx_condmutex_t *condmutex);

int mdbx_fastmutex_init(mdbx_fastmutex_t *fastmutex);
int mdbx_fastmutex_acquire(mdbx_fastmutex_t *fastmutex);
int mdbx_fastmutex_release(mdbx_fastmutex_t *fastmutex);
int mdbx_fastmutex_destroy(mdbx_fastmutex_t *fastmutex);

int mdbx_pwritev(mdbx_filehandle_t fd, struct iovec *iov, int iovcnt,
                 uint64_t offset, size_t expected_written);
int mdbx_pread(mdbx_filehandle_t fd, void *buf, size_t count, uint64_t offset);
int mdbx_pwrite(mdbx_filehandle_t fd, const void *buf, size_t count,
                uint64_t offset);
int mdbx_write(mdbx_filehandle_t fd, const void *buf, size_t count);

int mdbx_thread_create(mdbx_thread_t *thread,
                       THREAD_RESULT(THREAD_CALL *start_routine)(void *),
                       void *arg);
int mdbx_thread_join(mdbx_thread_t thread);

int mdbx_filesync(mdbx_filehandle_t fd, bool fullsync);
int mdbx_filesize_sync(mdbx_filehandle_t fd);
int mdbx_ftruncate(mdbx_filehandle_t fd, uint64_t length);
int mdbx_fseek(mdbx_filehandle_t fd, uint64_t pos);
int mdbx_filesize(mdbx_filehandle_t fd, uint64_t *length);
int mdbx_openfile(const char *pathname, int flags, mode_t mode,
                  mdbx_filehandle_t *fd, bool exclusive);
int mdbx_closefile(mdbx_filehandle_t fd);
int mdbx_removefile(const char *pathname);

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

int mdbx_mmap(int flags, mdbx_mmap_t *map, size_t must, size_t limit);
int mdbx_munmap(mdbx_mmap_t *map);
int mdbx_mresize(int flags, mdbx_mmap_t *map, size_t current, size_t wanna);
#if defined(_WIN32) || defined(_WIN64)
typedef struct {
  unsigned limit, count;
  HANDLE handles[31];
} mdbx_handle_array_t;
int mdbx_suspend_threads_before_remap(MDBX_env *env,
                                      mdbx_handle_array_t **array);
int mdbx_resume_threads_after_remap(mdbx_handle_array_t *array);
#endif /* Windows */
int mdbx_msync(mdbx_mmap_t *map, size_t offset, size_t length, int async);
int mdbx_check4nonlocal(mdbx_filehandle_t handle, int flags);

static __inline mdbx_pid_t mdbx_getpid(void) {
#if defined(_WIN32) || defined(_WIN64)
  return GetCurrentProcessId();
#else
  return getpid();
#endif
}

static __inline mdbx_tid_t mdbx_thread_self(void) {
#if defined(_WIN32) || defined(_WIN64)
  return GetCurrentThreadId();
#else
  return pthread_self();
#endif
}

void mdbx_osal_jitter(bool tiny);

/*----------------------------------------------------------------------------*/
/* lck stuff */

#if defined(_WIN32) || defined(_WIN64)
#undef MDBX_OSAL_LOCK
#define MDBX_OSAL_LOCK_SIGN UINT32_C(0xF10C)
#else
#define MDBX_OSAL_LOCK pthread_mutex_t
#define MDBX_OSAL_LOCK_SIGN UINT32_C(0x8017)
#endif /* MDBX_OSAL_LOCK */

#ifdef MDBX_OSAL_LOCK
#define MDBX_OSAL_LOCK_SIZE sizeof(MDBX_OSAL_LOCK)
#else
#define MDBX_OSAL_LOCK_SIZE 0
#endif /* MDBX_OSAL_LOCK_SIZE */

int mdbx_lck_init(MDBX_env *env);

int mdbx_lck_seize(MDBX_env *env);
int mdbx_lck_downgrade(MDBX_env *env, bool complete);
void mdbx_lck_destroy(MDBX_env *env);

int mdbx_rdt_lock(MDBX_env *env);
void mdbx_rdt_unlock(MDBX_env *env);

LIBMDBX_API int mdbx_txn_lock(MDBX_env *env, bool dontwait);
LIBMDBX_API void mdbx_txn_unlock(MDBX_env *env);

int mdbx_rpid_set(MDBX_env *env);
int mdbx_rpid_clear(MDBX_env *env);

#if defined(_WIN32) || defined(_WIN64)
typedef union MDBX_srwlock {
  struct {
    long volatile readerCount;
    long volatile writerCount;
  };
  RTL_SRWLOCK native;
} MDBX_srwlock;

typedef void(WINAPI *MDBX_srwlock_function)(MDBX_srwlock *);
extern MDBX_srwlock_function mdbx_srwlock_Init, mdbx_srwlock_AcquireShared,
    mdbx_srwlock_ReleaseShared, mdbx_srwlock_AcquireExclusive,
    mdbx_srwlock_ReleaseExclusive;

typedef BOOL(WINAPI *MDBX_GetFileInformationByHandleEx)(
    _In_ HANDLE hFile, _In_ FILE_INFO_BY_HANDLE_CLASS FileInformationClass,
    _Out_ LPVOID lpFileInformation, _In_ DWORD dwBufferSize);
extern MDBX_GetFileInformationByHandleEx mdbx_GetFileInformationByHandleEx;

typedef BOOL(WINAPI *MDBX_GetVolumeInformationByHandleW)(
    _In_ HANDLE hFile, _Out_opt_ LPWSTR lpVolumeNameBuffer,
    _In_ DWORD nVolumeNameSize, _Out_opt_ LPDWORD lpVolumeSerialNumber,
    _Out_opt_ LPDWORD lpMaximumComponentLength,
    _Out_opt_ LPDWORD lpFileSystemFlags,
    _Out_opt_ LPWSTR lpFileSystemNameBuffer, _In_ DWORD nFileSystemNameSize);

extern MDBX_GetVolumeInformationByHandleW mdbx_GetVolumeInformationByHandleW;

typedef DWORD(WINAPI *MDBX_GetFinalPathNameByHandleW)(_In_ HANDLE hFile,
                                                      _Out_ LPWSTR lpszFilePath,
                                                      _In_ DWORD cchFilePath,
                                                      _In_ DWORD dwFlags);
extern MDBX_GetFinalPathNameByHandleW mdbx_GetFinalPathNameByHandleW;

typedef NTSTATUS(NTAPI *MDBX_NtFsControlFile)(
    IN HANDLE FileHandle, IN OUT HANDLE Event,
    IN OUT PVOID /* PIO_APC_ROUTINE */ ApcRoutine, IN OUT PVOID ApcContext,
    OUT PIO_STATUS_BLOCK IoStatusBlock, IN ULONG FsControlCode,
    IN OUT PVOID InputBuffer, IN ULONG InputBufferLength,
    OUT OPTIONAL PVOID OutputBuffer, IN ULONG OutputBufferLength);

extern MDBX_NtFsControlFile mdbx_NtFsControlFile;

#endif /* Windows */

/* Checks reader by pid.
 *
 * Returns:
 *   MDBX_RESULT_TRUE, if pid is live (unable to acquire lock)
 *   MDBX_RESULT_FALSE, if pid is dead (lock acquired)
 *   or otherwise the errcode. */
int mdbx_rpid_check(MDBX_env *env, mdbx_pid_t pid);

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

static __inline uint32_t mdbx_atomic_add32(volatile uint32_t *p, uint32_t v) {
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

static __inline uint64_t mdbx_atomic_add64(volatile uint64_t *p, uint64_t v) {
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

static __inline bool mdbx_atomic_compare_and_swap32(volatile uint32_t *p,
                                                    uint32_t c, uint32_t v) {
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

static __inline bool mdbx_atomic_compare_and_swap64(volatile uint64_t *p,
                                                    uint64_t c, uint64_t v) {
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

#if defined(_MSC_VER) && _MSC_VER >= 1900 && _MSC_VER < 1920
/* LY: MSVC 2015/2017 has buggy/inconsistent PRIuPTR/PRIxPTR macros
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
