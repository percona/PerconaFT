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
#include <queue>

//TODO: update comment, this is from rwlock.h

namespace toku {

class frwlock_queueitem {
  public:
    toku_cond_t *m_cond;
    bool         m_is_read;
    int          m_writer_tid;
    context_id   m_writer_context_id;
    frwlock_queueitem(toku_cond_t *cond) 
            : m_cond(cond)
            , m_is_read(true)
            , m_writer_tid(-1)
            , m_writer_context_id(CTX_INVALID)
    {}
    frwlock_queueitem(toku_cond_t *cond, int writer_tid, context_id writer_context_id)
            : m_cond(cond)
            , m_is_read(false)
            , m_writer_tid(writer_tid)
            , m_writer_context_id(writer_context_id)
    {}
};

class frwlock {
public:

    // This is a fair readers-writers lock.  It has two extra properties
    // beyond a vanilla fair rw lock: 
    //
    //  1) It can tell you whether waiting on a lock may be
    //     "expensive".  This is done by requiring anyone who obtains
    //     a write lock to say whether it's expensive, and keeping
    //     track of whether any expensive write request is either
    //     holding the lock or in the queue waiting.
    //
    //  2) It records the context and thread id of the writer who is
    //     currently blocking any other thread from getting the lock.
    //     It does this by recording the context and thread id when a
    //     writer gets the lock, or when a reader gets the lock and
    //     the next item in the queue is a writer.
    //
    // The implementation employs a std::queue of frwlock_queueitems
    // (which contain a condition variable, a bool indicatin whether
    // the request is a reader or a writer, and the threadid and
    // context of the requesting thread.
    //
    // When a reader or writer tries, and cannot get, the lock, it
    // places a condition variable into the queue.
    //
    // When a reader releases a lock, it checks to see if there are
    // any other readers still holding the lock, and if not, it
    // signals the next item in the queue (which is responsible for
    // fremoving itself from the queue, and if it is a reader, for
    // signaling the next item in the queue if it is a reader).
    //
    // When a writer releases a lock, it signals the next item in the
    // queue.

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

    // How many threads are holding or waiting on the lock ?  (You must hold the lock to call this.)
    uint32_t users(void) const;

    // How many writer therads are holding the lock (0 or 1)?  (You must hold the lock to hold this).
    uint32_t writers(void) const;

    // How many readers currently hold the lock?  (You must hold the lock to call this.)
    uint32_t readers(void) const;

private:
    // the pair is the condition variable and true for read, false for write
    std::queue<frwlock_queueitem> *m_queue;

    toku_mutex_t *m_mutex;

    // How many readers hold the lock?
    uint32_t m_num_readers;
    
    // How many writers hold the lock?
    uint32_t m_num_writers;

    // Number of writers that are expensive (not including the writer that holds the lock, if any)
    // MUST be < the number in the queue that want to write.
    uint32_t m_num_expensive_want_write;

    // Is the current writer expensive (we must store this separately
    // from m_num_expensive_want_write)
    bool     m_current_writer_expensive;

    // thread-id of the current writer
    int m_current_writer_tid;

    // context id describing the context of the current writer blocking
    // new readers (either because this writer holds the write lock or
    // is one of the ones that wants the write lock).
    context_id m_blocking_writer_context_id;
};

ENSURE_POD(frwlock);

} // namespace toku

// include the implementation here
// #include "frwlock.cc"
