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

#pragma once
#include "fast_positive/config.h"

#include <assert.h>
#if defined(_MSC_VER) && defined(_ASSERTE)
#undef assert
#define assert _ASSERTE
#endif /* _MSC_VER */

#if !defined(UNALIGNED_OK)
#if defined(__i386) || defined(__x86_64__) || defined(_M_IX86) ||              \
    defined(_M_X64) || defined(i386) || defined(_X86_) || defined(__i386__) || \
    defined(_X86_64_)
#define UNALIGNED_OK 1
#else
#define UNALIGNED_OK 0
#endif
#endif /* UNALIGNED_OK */

/*----------------------------------------------------------------------------*/
/* Threads */

#ifdef CMAKE_HAVE_PTHREAD_H
#include <pthread.h>

typedef struct fpta_rwl {
  pthread_rwlock_t prwl;
} fpta_rwl_t;

static int __inline fpta_rwl_init(fpta_rwl_t *rwl) {
  return pthread_rwlock_init(&rwl->prwl, NULL);
}

static int __inline fpta_rwl_sharedlock(fpta_rwl_t *rwl) {
  return pthread_rwlock_rdlock(&rwl->prwl);
}

static int __inline fpta_rwl_exclusivelock(fpta_rwl_t *rwl) {
  return pthread_rwlock_wrlock(&rwl->prwl);
}

static int __inline fpta_rwl_unlock(fpta_rwl_t *rwl) {
  return pthread_rwlock_unlock(&rwl->prwl);
}

static int __inline fpta_rwl_destroy(fpta_rwl_t *rwl) {
  return pthread_rwlock_destroy(&rwl->prwl);
}

typedef struct fpta_mutex {
  pthread_mutex_t ptmx;
} fpta_mutex_t;

static int __inline fpta_mutex_init(fpta_mutex_t *mutex) {
  return pthread_mutex_init(&mutex->ptmx, NULL);
}

static int __inline fpta_mutex_lock(fpta_mutex_t *mutex) {
  return pthread_mutex_lock(&mutex->ptmx);
}

static int __inline fpta_mutex_trylock(fpta_mutex_t *mutex) {
  return pthread_mutex_trylock(&mutex->ptmx);
}

static int __inline fpta_mutex_unlock(fpta_mutex_t *mutex) {
  return pthread_mutex_unlock(&mutex->ptmx);
}

static int __inline fpta_mutex_destroy(fpta_mutex_t *mutex) {
  return pthread_mutex_destroy(&mutex->ptmx);
}

#else

#ifdef _MSC_VER
#pragma warning(disable : 4514) /* 'xyz': unreferenced inline function         \
                                   has been removed */
#pragma warning(disable : 4127) /* conditional expression is constant          \
                                 */

#pragma warning(push, 1)
#pragma warning(disable : 4530) /* C++ exception handler used, but             \
                                   unwind semantics are not enabled. Specify   \
                                   /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception           \
                                   handling mode specified; termination on     \
                                   exception is not guaranteed. Specify /EHsc  \
                                   */
#endif                          /* _MSC_VER (warnings) */

#include <windows.h>

#ifdef _MSC_VER
#pragma warning(pop)
#endif

enum fpta_rwl_state : ptrdiff_t {
  SRWL_FREE = (ptrdiff_t)0xF08EE00Fl,
  SRWL_RDLC = (ptrdiff_t)0xF0008D1Cl,
  SRWL_POISON = (ptrdiff_t)0xDEADBEEFl,
};

typedef struct fpta_rwl {
  SRWLOCK srwl;
  ptrdiff_t state;
} fpta_rwl_t;

static __inline void __srwl_set_state(fpta_rwl_t *rwl, ptrdiff_t state) {
  assert((ptrdiff_t)GetCurrentThreadId() != SRWL_FREE);
  assert((ptrdiff_t)GetCurrentThreadId() != SRWL_RDLC);
  assert((ptrdiff_t)GetCurrentThreadId() != SRWL_POISON);
  rwl->state = state;
}

static __inline ptrdiff_t __srwl_get_state(fpta_rwl_t *rwl) {
  assert((ptrdiff_t)GetCurrentThreadId() != SRWL_FREE);
  assert((ptrdiff_t)GetCurrentThreadId() != SRWL_RDLC);
  assert((ptrdiff_t)GetCurrentThreadId() != SRWL_POISON);
  return rwl->state;
}

static int __inline fpta_rwl_init(fpta_rwl_t *rwl) {
  if (!rwl)
    return FPTA_EINVAL;
  InitializeSRWLock(&rwl->srwl);
  __srwl_set_state(rwl, SRWL_FREE);
  return FPTA_SUCCESS;
}

static int __inline fpta_rwl_sharedlock(fpta_rwl_t *rwl) {
  if (!rwl || __srwl_get_state(rwl) == SRWL_POISON)
    return FPTA_EINVAL;

  AcquireSRWLockShared(&rwl->srwl);
  __srwl_set_state(rwl, SRWL_RDLC);
  return FPTA_SUCCESS;
}

static int __inline fpta_rwl_exclusivelock(fpta_rwl_t *rwl) {
  if (!rwl || __srwl_get_state(rwl) == SRWL_POISON)
    return FPTA_EINVAL;
  AcquireSRWLockExclusive(&rwl->srwl);
  __srwl_set_state(rwl, (ptrdiff_t)GetCurrentThreadId());
  return FPTA_SUCCESS;
}

static int __inline fpta_rwl_unlock(fpta_rwl_t *rwl) {
  if (!rwl)
    return FPTA_EINVAL;

  ptrdiff_t state = __srwl_get_state(rwl);
  switch (state) {
  default:
    if (state == (ptrdiff_t)GetCurrentThreadId()) {
      __srwl_set_state(rwl, SRWL_FREE);
      ReleaseSRWLockExclusive(&rwl->srwl);
      return FPTA_SUCCESS;
    }
  case SRWL_FREE:
    return FPTA_EPERM;
  case SRWL_POISON:
    return FPTA_EINVAL;
  case SRWL_RDLC:
    ReleaseSRWLockShared(&rwl->srwl);
    return FPTA_SUCCESS;
  }
}

static int __inline fpta_rwl_destroy(fpta_rwl_t *rwl) {
  if (!rwl || __srwl_get_state(rwl) == SRWL_POISON)
    return FPTA_EINVAL;
  AcquireSRWLockExclusive(&rwl->srwl);
  __srwl_set_state(rwl, SRWL_POISON);
  return FPTA_SUCCESS;
}

typedef struct fpta_mutex {
  CRITICAL_SECTION cs;
} fpta_mutex_t;

static int __inline fpta_mutex_init(fpta_mutex_t *mutex) {
  if (!mutex)
    return FPTA_EINVAL;
  InitializeCriticalSection(&mutex->cs);
  return FPTA_SUCCESS;
}

static int __inline fpta_mutex_lock(fpta_mutex_t *mutex) {
  if (!mutex)
    return FPTA_EINVAL;
  EnterCriticalSection(&mutex->cs);
  return FPTA_SUCCESS;
}

static int __inline fpta_mutex_trylock(fpta_mutex_t *mutex) {
  if (!mutex)
    return FPTA_EINVAL;
  return TryEnterCriticalSection(&mutex->cs) ? FPTA_SUCCESS : FPTA_EBUSY;
}

static int __inline fpta_mutex_unlock(fpta_mutex_t *mutex) {
  if (!mutex)
    return FPTA_EINVAL;
  LeaveCriticalSection(&mutex->cs);
  return FPTA_SUCCESS;
}

static int __inline fpta_mutex_destroy(fpta_mutex_t *mutex) {
  if (!mutex)
    return FPTA_EINVAL;
  DeleteCriticalSection(&mutex->cs);
  return FPTA_SUCCESS;
}

#endif /* CMAKE_HAVE_PTHREAD_H */
