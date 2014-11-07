/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:

#ident "$Id$"
/*
COPYING CONDITIONS NOTICE:

  This program is free software; you can redistribute it and/or modify
  it under the terms of version 2 of the GNU General Public License as
  published by the Free Software Foundation, and provided that the
  following conditions are met:

      * Redistributions of source code must retain this COPYING
        CONDITIONS NOTICE, the COPYRIGHT NOTICE (below), the
        DISCLAIMER (below), the UNIVERSITY PATENT NOTICE (below), the
        PATENT MARKING NOTICE (below), and the PATENT RIGHTS
        GRANT (below).

      * Redistributions in binary form must reproduce this COPYING
        CONDITIONS NOTICE, the COPYRIGHT NOTICE (below), the
        DISCLAIMER (below), the UNIVERSITY PATENT NOTICE (below), the
        PATENT MARKING NOTICE (below), and the PATENT RIGHTS
        GRANT (below) in the documentation and/or other materials
        provided with the distribution.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
  02110-1301, USA.

COPYRIGHT NOTICE:

  TokuFT, Tokutek Fractal Tree Indexing Library.
  Copyright (C) 2007-2013 Tokutek, Inc.

DISCLAIMER:

  This program is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

UNIVERSITY PATENT NOTICE:

  The technology is licensed by the Massachusetts Institute of
  Technology, Rutgers State University of New Jersey, and the Research
  Foundation of State University of New York at Stony Brook under
  United States of America Serial No. 11/760379 and to the patents
  and/or patent applications resulting from it.

PATENT MARKING NOTICE:

  This software is covered by US Patent No. 8,185,551.
  This software is covered by US Patent No. 8,489,638.

PATENT RIGHTS GRANT:

  "THIS IMPLEMENTATION" means the copyrightable works distributed by
  Tokutek as part of the Fractal Tree project.

  "PATENT CLAIMS" means the claims of patents that are owned or
  licensable by Tokutek, both currently or in the future; and that in
  the absence of this license would be infringed by THIS
  IMPLEMENTATION or by using or running THIS IMPLEMENTATION.

  "PATENT CHALLENGE" shall mean a challenge to the validity,
  patentability, enforceability and/or non-infringement of any of the
  PATENT CLAIMS or otherwise opposing any of the PATENT CLAIMS.

  Tokutek hereby grants to you, for the term and geographical scope of
  the PATENT CLAIMS, a non-exclusive, no-charge, royalty-free,
  irrevocable (except as stated in this section) patent license to
  make, have made, use, offer to sell, sell, import, transfer, and
  otherwise run, modify, and propagate the contents of THIS
  IMPLEMENTATION, where such license applies only to the PATENT
  CLAIMS.  This grant does not include claims that would be infringed
  only as a consequence of further modifications of THIS
  IMPLEMENTATION.  If you or your agent or licensee institute or order
  or agree to the institution of patent litigation against any entity
  (including a cross-claim or counterclaim in a lawsuit) alleging that
  THIS IMPLEMENTATION constitutes direct or contributory patent
  infringement, or inducement of patent infringement, then any rights
  granted to you under this License shall terminate as of the date
  such litigation is filed.  If you or your agent or exclusive
  licensee institute or order or agree to the institution of a PATENT
  CHALLENGE, then Tokutek may terminate any rights granted to you
  under this License.
*/

#pragma once

#ident "Copyright (c) 2007-2013 Tokutek Inc.  All rights reserved."

#ifdef WIN32
// TODO: Include either WinBase.h (Windows 7) or Synchapi.h (Windows 8)

typedef CRITICAL_SECTION pthread_mutex_t;
typedef DWORD pthread_t;
typedef CONDITION_VARIABLE pthread_cond_t;

typedef int pthread_attr_t;
typedef int pthread_mutexattr_t;
typedef int pthread_condattr_t;
typedef int pthread_rwlockattr_t;

// NOTE: Windows 7 and higher support reader/writer locks of this
// type, but the type does not save reader/writer state.  To
// put it another way, if we were to just return the SRWLOCK,
// there would be no way of knowing from just the type
// which Windows lock release call to make.  POSIX threads
// do not care how the lock was acquired, they only have one
// unlock method.  Hence, the need for a second boolean field, 
// stating how the lock was acquired.
// NOTE: We may be able to get rid of this, since we have explicit
// unlock functions for rwlocks...
struct slim_reader_writer_lock {
  SRWLOCK srwlock;
  BOOL acquired_exclusive_lock;
};
typdef struct slim_reader_writer_lock toku_pthread_rwlock_t;
#else // End of Windows ifdef.
#include <pthread.h>

#endif

typedef pthread_attr_t toku_pthread_attr_t;
typedef pthread_t toku_pthread_t;
typedef pthread_mutexattr_t toku_pthread_mutexattr_t;
typedef pthread_condattr_t toku_pthread_condattr_t;
typedef pthread_cond_t toku_pthread_cond_t;
typedef pthread_rwlock_t toku_pthread_rwlock_t;
typedef pthread_rwlockattr_t  toku_pthread_rwlockattr_t;
typedef pthread_key_t toku_pthread_key_t;
typedef struct timespec toku_timespec_t;

typedef struct toku_mutex {
    pthread_mutex_t pmutex;
#if TOKU_PTHREAD_DEBUG
    pthread_t owner; // = pthread_self(); // for debugging
    bool locked;
    bool valid;
#endif
} toku_mutex_t;

typedef struct toku_mutex_aligned {
    toku_mutex_t aligned_mutex ALIGNED(64);
} toku_mutex_aligned_t;

typedef struct toku_cond {
    pthread_cond_t pcond;
} toku_cond_t;

void toku_mutex_init(toku_mutex_t *mutex, const toku_pthread_mutexattr_t *attr);
void toku_mutexattr_init(toku_pthread_mutexattr_t *attr);
void toku_mutexattr_settype(toku_pthread_mutexattr_t *attr, int type);
void toku_mutexattr_destroy(toku_pthread_mutexattr_t *attr);
void toku_mutex_destroy(toku_mutex_t *mutex);
void toku_mutex_lock(toku_mutex_t *mutex);
int toku_mutex_trylock(toku_mutex_t *mutex);
void toku_mutex_unlock(toku_mutex_t *mutex);
void toku_mutex_assert_locked(const toku_mutex_t mutex);
void toku_mutex_assert_unlocked(toku_mutex_t mutex);

void toku_cond_init(toku_cond_t *cond, const toku_pthread_condattr_t *attr);
void toku_cond_destroy(toku_cond_t *cond);
void toku_cond_wait(toku_cond_t *cond, toku_mutex_t *mutex);
int toku_cond_timedwait(toku_cond_t *cond, toku_mutex_t *mutex, toku_timespec_t *wakeup_at);
void toku_cond_signal(toku_cond_t *cond);
void toku_cond_broadcast(toku_cond_t *cond);
int toku_pthread_yield(void) DEFAULT_VISIBILITY;
toku_pthread_t toku_pthread_self(void);

void toku_pthread_rwlock_init(toku_pthread_rwlock_t *__restrict rwlock, const toku_pthread_rwlockattr_t *__restrict attr);
void toku_pthread_rwlock_destroy(toku_pthread_rwlock_t *rwlock);
void toku_pthread_rwlock_rdlock(toku_pthread_rwlock_t *rwlock);
void toku_pthread_rwlock_rdunlock(toku_pthread_rwlock_t *rwlock);
void toku_pthread_rwlock_wrlock(toku_pthread_rwlock_t *rwlock);
void toku_pthread_rwlock_wrunlock(toku_pthread_rwlock_t *rwlock);

int toku_pthread_create(toku_pthread_t *thread, const toku_pthread_attr_t *attr, void *(*start_function)(void *), void *arg);
int toku_pthread_join(toku_pthread_t thread, void **value_ptr);
int toku_pthread_detach(toku_pthread_t thread);
int toku_pthread_key_create(toku_pthread_key_t *key, void (*destroyf)(void *));
int toku_pthread_key_delete(toku_pthread_key_t key);
void * toku_pthread_getspecific(toku_pthread_key_t key);
int toku_pthread_setspecific(toku_pthread_key_t key, void *data);