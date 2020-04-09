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

// Verify that a DB put that is waiting on a previously locked key get TOKUDB_OUT_OF_LOCKS
// result when another key is released AND the lock memory used is still over the limit.

#include "test.h"
#include "toku_pthread.h"

static int put_small_key(DB *db, DB_TXN *txn) {
    char k[8] = {};
    DBT key = { .data = &k, .size = sizeof k};
    DBT val = {};
    int r = db->put(db, txn, &key, &val, 0);
    return r;
}

static int put_large_key(DB *db, DB_TXN *txn) {
    char k[200] = {};
    DBT key = { .data = &k, .size = sizeof k};
    DBT val = {};
    int r = db->put(db, txn, &key, &val, 0);
    return r;
}

struct test_c_args {
    DB *db;
    DB_TXN *txn;
};

static void *test_c(void *arg) {
    struct test_c_args *a = (struct test_c_args *) arg;
    int r = put_small_key(a->db, a->txn);
    assert(r == TOKUDB_OUT_OF_LOCKS);
    return arg;
}

int test_main(int argc, char * const argv[]) {
    const char *db_env_dir = TOKU_TEST_FILENAME;
    const char *db_filename = "test.db";
    int db_env_open_flags = DB_CREATE | DB_PRIVATE | DB_INIT_MPOOL | DB_INIT_TXN | DB_INIT_LOCK | DB_INIT_LOG | DB_THREAD;

    // parse_args(argc, argv);
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0) {
            verbose++;
            continue;
        }
        if (strcmp(argv[i], "-q") == 0 || strcmp(argv[i], "--quiet") == 0) {
            if (verbose > 0)
                verbose--;
            continue;
        }
        assert(0);
    }

    // setup the test environment
    int r;
    char rm_cmd[strlen(db_env_dir) + strlen("rm -rf ") + 1];
    snprintf(rm_cmd, sizeof(rm_cmd), "rm -rf %s", db_env_dir);
    r = system(rm_cmd); assert(r == 0);

    r = toku_os_mkdir(db_env_dir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH); assert(r == 0);

    DB_ENV *db_env = nullptr;
    r = db_env_create(&db_env, 0); assert(r == 0);

    // Set a small lock memory limit
    const uint64_t lock_memory_wanted = 300;
    r = db_env->set_lk_max_memory(db_env, lock_memory_wanted); assert(r == 0);
    uint64_t lock_memory_limit;
    r = db_env->get_lk_max_memory(db_env, &lock_memory_limit); assert(r == 0 && lock_memory_limit == lock_memory_wanted);

    r = db_env->open(db_env, db_env_dir, db_env_open_flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH); assert(r == 0);
    r = db_env->set_lock_timeout(db_env, 30 * 1000, nullptr); assert(r == 0);

    // create the db
    DB *db = nullptr;
    r = db_create(&db, db_env, 0); assert(r == 0);
    r = db->open(db, nullptr, db_filename, nullptr, DB_BTREE, DB_CREATE|DB_AUTO_COMMIT|DB_THREAD, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH); assert(r == 0);

    // create the txn's
    DB_TXN *txn_a = nullptr;
    r = db_env->txn_begin(db_env, nullptr, &txn_a, 0); assert(r == 0);

    DB_TXN *txn_b = nullptr;
    r = db_env->txn_begin(db_env, nullptr, &txn_b, 0); assert(r == 0);

    DB_TXN *txn_c = nullptr;
    r = db_env->txn_begin(db_env, nullptr, &txn_c, 0); assert(r == 0);

    // Put a small key into the DB.
    // Before: lock memory used is 0.
    // After: lock memory used is under the limit.
    r = put_small_key(db, txn_a);
    assert(r == 0);

    // Create a thread that will attempt to lock the same key as txn_a.
    // Effect: this thread will be blocking on the lock request for this
    // key
    toku_pthread_t tid_c;
    test_c_args a = { db, txn_c };
    r = toku_pthread_create(toku_uninstrumented, &tid_c, nullptr, test_c, &a);
    assert(r == 0);

    // give thread c some time to get blocked
    sleep(1);

    // Put a large key into the DB, which should succeed.
    // Before: lock memory used is under the limit
    // After: lock memory used is over the limit due to the addition of the large key
    r = put_large_key(db, txn_b);
    assert(r == 0);

    // abort txn a, should release lock on the small key but lock memory
    // is still over the limit, so test c put lock retry should get
    // TOKUDB_OUT_OF_LOCKS
    r = txn_a->abort(txn_a); assert(r == 0);

    // cleanup
    void *ret;
    r = toku_pthread_join(tid_c, &ret); assert(r == 0);
    r = txn_b->abort(txn_b); assert(r == 0);
    r = txn_c->abort(txn_c); assert(r == 0);
    r = db->close(db, 0); assert(r == 0); db = nullptr;
    r = db_env->close(db_env, 0); assert(r == 0); db_env = nullptr;

    return 0;
}
