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

#include <toku_assert.h>

#include <util/context.h>
#include <util/frwlock.h>

namespace toku {

static __thread int thread_local_tid = -1;
static int get_local_tid() {
    if (thread_local_tid == -1) {
        thread_local_tid = toku_os_gettid();
    }
    return thread_local_tid;
}

void frwlock::init(toku_mutex_t *const mutex) {
    m_queue = new std::queue<std::pair<toku_cond_t*,bool>>();
    m_mutex = mutex;
    m_num_readers = 0;
    m_num_writers = 0;
    m_num_want_read = 0;
    m_num_want_write = 0;
    m_num_expensive_want_write = 0;

    // Do we really want these?
    m_current_writer_tid = -1;
    m_blocking_writer_context_id = CTX_INVALID;
}

void frwlock::deinit(void) {
    assert(m_queue->empty());
    delete m_queue;
}

// Prerequisite: Holds m_mutex.
void frwlock::write_lock(bool expensive) {
    toku_mutex_assert_locked(m_mutex);
    if (this->try_write_lock(expensive)) {
        return;
    }

    toku_cond_t cond = TOKU_COND_INITIALIZER;
    m_queue->push(std::pair<toku_cond_t *,bool>(&cond, false));
    ++m_num_want_write;
    if (expensive) {
        ++m_num_expensive_want_write;
    }

    if (m_num_writers == 0 && m_num_want_write == 1) {
        // We are the first to want a write lock. No new readers can get the lock.
        // Set our thread id and context for proper instrumentation.
        // see: toku_context_note_frwlock_contention()
        m_current_writer_tid = get_local_tid();
        m_blocking_writer_context_id = toku_thread_get_context()->get_id();
    }
    while (m_num_writers || m_num_readers || m_queue->front().first != &cond) {
        // Wait until this cond variable is woken up.  We could get a spurious wakeup.
        toku_cond_wait(&cond, m_mutex);
    }
    m_queue->pop();
    toku_cond_destroy(&cond);

    // Now it's our turn.
    paranoid_invariant(m_num_want_write > 0);
    paranoid_invariant_zero(m_num_readers);
    paranoid_invariant_zero(m_num_writers);

    // Not waiting anymore; grab the lock.
    --m_num_want_write;
    if (expensive) {
        --m_num_expensive_want_write;
    }
    m_num_writers = 1;
    m_current_writer_expensive = expensive;
    m_current_writer_tid = get_local_tid();
    m_blocking_writer_context_id = toku_thread_get_context()->get_id();
}

bool frwlock::try_write_lock(bool expensive) {
    toku_mutex_assert_locked(m_mutex);
    if (m_num_readers > 0 || m_num_writers > 0 || !m_queue->empty()) {
        return false;
    }
    // No one holds the lock.  Grant the write lock.
    paranoid_invariant_zero(m_num_want_write);
    paranoid_invariant_zero(m_num_want_read);
    m_num_writers = 1;
    m_current_writer_expensive = expensive;
    m_current_writer_tid = get_local_tid();
    m_blocking_writer_context_id = toku_thread_get_context()->get_id();
    return true;
}

void frwlock::read_lock(void) {
    toku_mutex_assert_locked(m_mutex);
    if (this->try_read_lock()) return;
    
    toku_cond_t cond = TOKU_COND_INITIALIZER;
    m_queue->push(std::pair<toku_cond_t *, bool>(&cond, true));
    ++m_num_want_read;
    while (m_num_writers || m_queue->front().first != &cond) {
        toku_context_note_frwlock_contention(
            toku_thread_get_context()->get_id(),
            m_blocking_writer_context_id);
        toku_cond_wait(&cond, m_mutex);
    }
    m_queue->pop();
    toku_cond_destroy(&cond);
    paranoid_invariant_zero(m_num_writers);
    paranoid_invariant(m_num_want_read > 0);
    --m_num_want_read;
    ++m_num_readers;
    if (!m_queue->empty() && m_queue->front().second) {
        // The next guy is a reader, so wake him up too.
        toku_cond_signal(m_queue->front().first);
    }
}

bool frwlock::try_read_lock(void) {
    toku_mutex_assert_locked(m_mutex);
    if (m_num_writers > 0 || !m_queue->empty()) {
        return false;
    }
    // No writer holds the lock.
    // No writers are waiting.
    // Grant the read lock.
    ++m_num_readers;
    return true;
}

void frwlock::read_unlock(void) {
    toku_mutex_assert_locked(m_mutex);
    paranoid_invariant(m_num_writers == 0);
    paranoid_invariant(m_num_readers > 0);
    --m_num_readers;
    if (m_num_readers == 0 && !m_queue->empty()) {
        toku_cond_signal(m_queue->front().first);
    }
}

bool frwlock::read_lock_is_expensive(void) {
    toku_mutex_assert_locked(m_mutex);
    return m_num_expensive_want_write > 0 || m_current_writer_expensive;
}

void frwlock::write_unlock(void) {
    toku_mutex_assert_locked(m_mutex);
    paranoid_invariant(m_num_writers == 1);
    m_num_writers = 0;
    m_current_writer_expensive = false;
    m_current_writer_tid = -1;
    m_blocking_writer_context_id = CTX_INVALID;
    if (!m_queue->empty()) {
        toku_cond_signal(m_queue->front().first);
    }
}
bool frwlock::write_lock_is_expensive(void) {
    toku_mutex_assert_locked(m_mutex);
    return (m_num_expensive_want_write > 0) || m_current_writer_expensive;
}


uint32_t frwlock::users(void) const {
    toku_mutex_assert_locked(m_mutex);
    return m_num_readers + m_num_writers + m_num_want_read + m_num_want_write;
}
uint32_t frwlock::blocked_users(void) const {
    toku_mutex_assert_locked(m_mutex);
    return m_num_want_read + m_num_want_write;
}
uint32_t frwlock::writers(void) const {
    // this is sometimes called as "assert(lock->writers())" when we
    // assume we have the write lock.  if that's the assumption, we may
    // not own the mutex, so we don't assert_locked here
    return m_num_writers;
}
uint32_t frwlock::blocked_writers(void) const {
    toku_mutex_assert_locked(m_mutex);
    return m_num_want_write;
}
uint32_t frwlock::readers(void) const {
    toku_mutex_assert_locked(m_mutex);
    return m_num_readers;
}
uint32_t frwlock::blocked_readers(void) const {
    toku_mutex_assert_locked(m_mutex);
    return m_num_want_read;
}

} // namespace toku
