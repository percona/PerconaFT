/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:

// Verify that lock request retry returns TOKUDB_OUT_OF_LOCKS when
// all of the locktree memory is used.

#include "lock_request_unit_test.h"

namespace toku {

    static void locktree_release_lock(locktree *lt,
                                      TXNID txn_id,
                                      const DBT *left,
                                      const DBT *right) {
        range_buffer buffer;
        buffer.create();
        buffer.append(left, right);
        lt->release_locks(txn_id, &buffer);
        buffer.destroy();
    }

    void lock_request_unit_test::run(void) {
        int r;

        locktree_manager mgr;
        mgr.create(nullptr, nullptr, nullptr, nullptr);

        DICTIONARY_ID dict_id = {1};
        locktree *lt = mgr.get_lt(dict_id, dbt_comparator, nullptr);

        // set max lock memory small so that we can test the limit
        // with just 2 locks
        mgr.set_max_lock_memory(300);

        // create a small key
        DBT small_dbt;
        int64_t small_key = 1;
        toku_fill_dbt(&small_dbt, &small_key, sizeof small_key);
        small_dbt.flags = DB_DBT_USERMEM;
        const DBT *small_ptr = &small_dbt;

        // create a large key
        DBT large_dbt;
        union { int64_t n; char c[64]; } large_key;
        memset(&large_key, 0, sizeof large_key);
        large_key.n = 2;
        toku_fill_dbt(&large_dbt, &large_key, sizeof large_key);
        large_dbt.flags = DB_DBT_USERMEM;
        const DBT *large_dbt_ptr = &large_dbt;

        TXNID txn_a = { 1 };
        TXNID txn_b = { 2 };

        // a locks small key
        lock_request a;
        a.create();
        a.set(lt, txn_a, small_ptr, small_ptr, lock_request::type::WRITE, false);
        r = a.start();
        assert(r == 0);
        assert(a.m_state == lock_request::state::COMPLETE);

        // b tries to lock small key, fails since it is already locked
        lock_request b;
        b.create();
        b.set(lt, txn_b, small_ptr, small_ptr, lock_request::type::WRITE, false);
        r = b.start();
        assert(r == DB_LOCK_NOTGRANTED);
        assert(b.m_state == lock_request::state::PENDING);

        // a locks large key. lock memory is over the limit
        lock_request c;
        c.create();
        c.set(lt, txn_a, large_dbt_ptr, large_dbt_ptr, lock_request::type::WRITE, false);
        r = c.start();
        assert(r == 0);
        assert(c.m_state == lock_request::state::COMPLETE);

        // a releases small key, lock memory is still over the limit
        locktree_release_lock(lt, txn_a, small_ptr, small_ptr);

        // retry all lock requests, should complete lock request
        // b with a TOKUDB_OUT_OF_LOCKS result
        lock_request::retry_all_lock_requests(lt);

        assert(b.m_state == lock_request::state::COMPLETE);
        assert(b.m_complete_r == TOKUDB_OUT_OF_LOCKS);

        // b waits for small key, gets out of locks
        r = b.wait(0);
        assert(r == TOKUDB_OUT_OF_LOCKS);
        assert(b.m_state == lock_request::state::COMPLETE);
        assert(b.m_complete_r == TOKUDB_OUT_OF_LOCKS);

        // a releases large key
        locktree_release_lock(lt, txn_a, large_dbt_ptr, large_dbt_ptr);

        // b locks small key, gets its
        r = b.start();
        assert(r == 0);

        // b releases lock so we can exit cleanly
        locktree_release_lock(lt, txn_b, small_ptr, small_ptr);

        a.destroy();
        b.destroy();

        mgr.release_lt(lt);
        mgr.destroy();
    }

} /* namespace toku */

int main(void) {
    toku::lock_request_unit_test test;
    test.run();
    return 0;
}
