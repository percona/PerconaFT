/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
#ident "$Id$"
/*======
This file is part of PerconaFT.


Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved.

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License, version 2,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.

----------------------------------------

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License, version 3,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.
======= */

#ident "Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved."

#pragma once

#include <toku_portability.h>
#include <toku_pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <util/context.h>

//TODO: update comment, this is from rwlock.h

namespace toku {

class frwlock {
public:

    // This is a fair readers-writers lock.  It has one extra property
    // beyond a vanilla fair rw lock: it can tell you whether waiting
    // on a lock may be "expensive".  This is done by requiring anyone
    // who obtains a write lock to say whether it's expensive.

    void init(toku_mutex_t *const mutex);
    void deinit(void);

    void write_lock(bool expensive);
    bool try_write_lock(bool expensive);
    void write_unlock(void);
    // returns true if acquiring a write lock will be expensive
    bool write_lock_is_expensive(void);

    void read_lock(void);
    bool try_read_lock(void);
    void read_unlock(void);
    // returns true if acquiring a read lock will be expensive
    bool read_lock_is_expensive(void);

    uint32_t users(void) const;
    uint32_t blocked_users(void) const;
    uint32_t writers(void) const;
    uint32_t blocked_writers(void) const;
    uint32_t readers(void) const;
    uint32_t blocked_readers(void) const;

private:
    struct queue_item {
        toku_cond_t *cond;
        struct queue_item *next;
    };

    bool queue_is_empty(void) const;
    void enq_item(queue_item *const item);
    toku_cond_t *deq_item(void);
    void maybe_signal_or_broadcast_next(void);
    void maybe_signal_next_writer(void);

    // Discussion of the frwlock (especially of the condition
    // variables).  This implementation seems needlessly complex, so
    // it needs some documentation here.  We implements a fair
    // readers-writer lock by keeping a queue.  We employ condition
    // variables to wake up threads when they get to the front of the
    // queue.
    //
    // For a read: If there is a writer (holding the lock, or in the
    // queue), then the reader must wait (that makes it fair).
    //  The way it waits: If there are any other readers in the queue, put this reader into the queue.
    //                    We then wait on the condition variable.
    //      Bradley says: This looks pretty buggy.  A reader could end up in the queue and then get spuriously woken up
    //       I think this code is still not safe against spurious wakeups.

    toku_mutex_t *m_mutex;

    // How many readers currently hold the lock (any nonnegative
    // number).
    uint32_t m_num_readers;

    // How many writers currently hold the lock (must be zero or one).
    uint32_t m_num_writers;

    // Number of writers in the queue.
    uint32_t m_num_want_write;

    // Number of readers in the queue.
    uint32_t m_num_want_read;
   // Number of readers that we have woken up (but they haven't
   // woken up yet and, for example, incremented the m_num_readers.)
    uint32_t m_num_signaled_readers;

    // When we signal a writer to wake up, it must be waiting on this
    // cond variable to avoid spurious wakeups.
    toku_cond_t *m_waking_cond;

    // number of writers waiting that are expensive
    // MUST be < m_num_want_write
    uint32_t m_num_expensive_want_write;
    // bool that states if the current writer is expensive
    // if there is no current writer, then is false
    bool m_current_writer_expensive;
    // bool that states if waiting for a read
    // is expensive
    // if there are currently no waiting readers, then set to false
    bool m_read_wait_expensive;
    // thread-id of the current writer
    int m_current_writer_tid;
    // context id describing the context of the current writer blocking
    // new readers (either because this writer holds the write lock or
    // is the first to want the write lock).
    context_id m_blocking_writer_context_id;
    
    toku_cond_t m_wait_read;
    queue_item m_queue_item_read;
    bool m_wait_read_is_in_queue;

    queue_item *m_wait_head;
    queue_item *m_wait_tail;
};

ENSURE_POD(frwlock);

} // namespace toku

// include the implementation here
// #include "frwlock.cc"
