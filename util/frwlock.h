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

    // How many threads are holding or waiting on the lock ?
    uint32_t users(void) const;

    // How many are waiting on the lock?
    uint32_t blocked_users(void) const;

    // How many writer therads are holding the lock (0 or 1)?
    uint32_t writers(void) const;

    // How many writers are waiting on the lock?
    uint32_t blocked_writers(void) const;

    // How many readers currently hold the lock?
    uint32_t readers(void) const;

    // How many readers currently wait on the lock?
    uint32_t blocked_readers(void) const;

private:
    // the pair is the condition variable and true for read, false for write
    std::queue<std::pair<toku_cond_t *, bool>> *m_queue;

    toku_mutex_t *m_mutex;

    // How many readers hold the lock?
    uint32_t m_num_readers;
    
    // How many writers hold the lock?
    uint32_t m_num_writers;

    // How many readers in the queue?
    uint32_t m_num_want_read;

    // How many writers in the queue?
    uint32_t m_num_want_write;

    // Number of writers that are expensive (not including the writer that holds the lock, if any)
    // MUST be < m_num_want_write
    uint32_t m_num_expensive_want_write;

    // Is the current writer expensive (we must store this separately
    // from m_num_expensive_want_write)
    bool     m_current_writer_expensive;

    // thread-id of the current writer
    int m_current_writer_tid;
    // context id describing the context of the current writer blocking
    // new readers (either because this writer holds the write lock or
    // is the first to want the write lock).
    context_id m_blocking_writer_context_id;
};

ENSURE_POD(frwlock);

} // namespace toku

// include the implementation here
// #include "frwlock.cc"
