/* -*- mode: C; c-basic-offset: 4 -*- */
#ifndef TOKU_RWLOCK_H
#define TOKU_RWLOCK_H
#ident "$Id$"
#ident "Copyright (c) 2007-2010 Tokutek Inc.  All rights reserved."
#ident "The technology is licensed by the Massachusetts Institute of Technology, Rutgers State University of New Jersey, and the Research Foundation of State University of New York at Stony Brook under United States of America Serial No. 11/760379 and to the patents and/or patent applications resulting from it."

#include <toku_assert.h>

#if defined(__cplusplus) || defined(__cilkplusplus)
extern "C" {
#endif

//Use case:
// A read lock is acquired by threads that get and pin an entry in the
// cachetable. A write lock is acquired by the writer thread when an entry
// is evicted from the cachetable and is being written storage.

//Use case:
// General purpose reader writer lock with properties:
// 1. multiple readers, no writers
// 2. one writer at a time
// 3. pending writers have priority over pending readers

// An external mutex must be locked when using these functions.  An alternate
// design would bury a mutex into the rwlock itself.  While this may
// increase parallelism at the expense of single thread performance, we
// are experimenting with a single higher level lock.

typedef struct rwlock *RWLOCK;
struct rwlock {
    int reader;                  // the number of readers
    int want_read;                // the number of blocked readers
    toku_cond_t wait_read;
    int writer;                  // the number of writers
    int want_write;              // the number of blocked writers
    toku_cond_t wait_write;
};

// initialize a read write lock

static __attribute__((__unused__))
void
rwlock_init(RWLOCK rwlock) {
    rwlock->reader = rwlock->want_read = 0;
    toku_cond_init(&rwlock->wait_read, 0);
    rwlock->writer = rwlock->want_write = 0;
    toku_cond_init(&rwlock->wait_write, 0);
}

// destroy a read write lock

static __attribute__((__unused__))
void
rwlock_destroy(RWLOCK rwlock) {
    assert(rwlock->reader == 0 && rwlock->want_read == 0);
    assert(rwlock->writer == 0 && rwlock->want_write == 0);
    toku_cond_destroy(&rwlock->wait_read);
    toku_cond_destroy(&rwlock->wait_write);
}

// obtain a read lock
// expects: mutex is locked

static inline void rwlock_read_lock(RWLOCK rwlock, toku_mutex_t *mutex) {
    if (rwlock->writer || rwlock->want_write) {
        rwlock->want_read++;
        while (rwlock->writer || rwlock->want_write) {
            toku_cond_wait(&rwlock->wait_read, mutex);
        }
        rwlock->want_read--;
    }
    rwlock->reader++;
}

static inline void rwlock_read_lock_and_unlock (RWLOCK rwlock, toku_mutex_t *mutex)
// Effect: Has the effect of obtaining a read lock and then unlocking it.
// Implementation note: This can be done faster than actually doing the lock/unlock
// Usage note:  This is useful when we are waiting on someone who has the write lock, but then we are just going to try again from the top.  (E.g., when releasing the ydb lock).
{
    if (rwlock->writer || rwlock->want_write) {
	rwlock->want_read++;
        while (rwlock->writer || rwlock->want_write) {
            toku_cond_wait(&rwlock->wait_read, mutex);
        }
        rwlock->want_read--;
    }
    // Don't increment reader.
}

// preferentially obtain a read lock (ignore request for write lock)
// expects: mutex is locked

static inline void rwlock_prefer_read_lock(RWLOCK rwlock, toku_mutex_t *mutex) {
    if (rwlock->reader)
	rwlock->reader++;
    else
	rwlock_read_lock(rwlock, mutex);
}

// try to acquire a read lock preferentially (ignore request for write lock).
// If a stall would happen (write lock is held), instead return EBUSY immediately.
// expects: mutex is locked

//Bug in ICL compiler prevents the UU definition from propogating to this header. Redefine UU here.
#define UU(x) x __attribute__((__unused__))
static inline int rwlock_try_prefer_read_lock(RWLOCK rwlock, toku_mutex_t *UU(mutex)) {
    int r = EBUSY;
    if (!rwlock->writer) {
	rwlock->reader++;
        r = 0;
    }
    return r;
}



// release a read lock
// expects: mutex is locked

static inline void rwlock_read_unlock(RWLOCK rwlock) {
    assert(rwlock->reader > 0);
    assert(rwlock->writer == 0);
    rwlock->reader--;
    if (rwlock->reader == 0 && rwlock->want_write) {
        toku_cond_signal(&rwlock->wait_write);
    }
}

// obtain a write lock
// expects: mutex is locked

static inline void rwlock_write_lock(RWLOCK rwlock, toku_mutex_t *mutex) {
    if (rwlock->reader || rwlock->writer) {
        rwlock->want_write++;
        while (rwlock->reader || rwlock->writer) {
            toku_cond_wait(&rwlock->wait_write, mutex);
        }
        rwlock->want_write--;
    }
    rwlock->writer++;
}

// release a write lock
// expects: mutex is locked

static inline void rwlock_write_unlock(RWLOCK rwlock) {
    assert(rwlock->reader == 0);
    assert(rwlock->writer == 1);
    rwlock->writer--;
    if (rwlock->writer == 0) {
        if (rwlock->want_write) {
            toku_cond_signal(&rwlock->wait_write);
        } else if (rwlock->want_read) {
            toku_cond_broadcast(&rwlock->wait_read);
        }
    }
}

// returns: the number of readers

static inline int rwlock_readers(RWLOCK rwlock) {
    return rwlock->reader;
}

// returns: the number of readers who are waiting for the lock

static inline int rwlock_blocked_readers(RWLOCK rwlock) {
    return rwlock->want_read;
}

// returns: the number of writers who are waiting for the lock

static inline int rwlock_blocked_writers(RWLOCK rwlock) {
    return rwlock->want_write;
}

// returns: the number of writers

static inline int rwlock_writers(RWLOCK rwlock) {
    return rwlock->writer;
}

// returns: the sum of the number of readers, pending readers, writers, and
// pending writers

static inline int rwlock_users(RWLOCK rwlock) {
    return rwlock->reader + rwlock->want_read + rwlock->writer + rwlock->want_write;
}

#if defined(__cplusplus) || defined(__cilkplusplus)
};
#endif

#endif
