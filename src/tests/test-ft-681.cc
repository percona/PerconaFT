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

#include "test.h"

// insert enough rows with a child txn and then force an eviction to verify the rollback
// log node is in valid state.
// the test fails without ft-681 fix (and of course passes with it).
// The test basically simulates the test script of George's. 

static void
populate(DB_ENV *env, DB *db) {
    int r;
    DB_TXN *parent = NULL;
    r = env->txn_begin(env, NULL, &parent, 0); assert_zero(r);

    DB_TXN *txn = NULL;
    
    r = env->txn_begin(env, parent, &txn, 0); assert_zero(r);

    // populate
    for (int i = 0; i < 128; i++) {
        int k = htonl(i);
        char kk[4]; // 4 KB key
        char str[220];
        memset(kk, 0, sizeof kk);
        memcpy(kk, &k, sizeof k);
        memset(str,'a', sizeof str-1);
        DBT key = { .data = kk, .size = sizeof kk };
        DBT val = { .data = str, .size = 220 };
        r = db->put(db, txn, &key, &val, 0);
        assert_zero(r);
    }

    r = txn->commit(txn, 0); assert_zero(r);
    
//we know the eviction is going to happen here
    
    r = env->txn_begin(env, parent, &txn, 0); assert_zero(r);

    // populate--- those rows will be lost in rollback nodes(ft-681)
    for (int i = 128; i < 256; i++) {
        int k = htonl(i);
        char kk[4]; // 4 KB key
        char str[220];
        memset(kk, 0, sizeof kk);
        memcpy(kk, &k, sizeof k);
        memset(str,'a', sizeof str-1);
        DBT key = { .data = kk, .size = sizeof kk };
        DBT val = { .data = str, .size = 220 };
        r = db->put(db, txn, &key, &val, 0);
        assert_zero(r);
    }

    r = txn->commit(txn, 0); assert_zero(r);
    
//again eviction due to the memory pressure
    r = env->txn_begin(env, parent, &txn, 0); assert_zero(r);

    // populate
    for (int i = 256; i < 512; i++) {
        int k = htonl(i);
        char kk[4]; // 4 KB key
        char str[220];
        memset(kk, 0, sizeof kk);
        memcpy(kk, &k, sizeof k);
        memset(str,'a', sizeof str-1);
        DBT key = { .data = kk, .size = sizeof kk };
        DBT val = { .data = str, .size = 220 };
        r = db->put(db, txn, &key, &val, 0);
        assert_zero(r);
    }

    r = txn->commit(txn, 0); assert_zero(r);
    
    r = parent->abort(parent); assert_zero(r);
//try to search anything in the lost range

        int k = htonl(200);
        char kk[4]; // 4 KB key
        memset(kk, 0, sizeof kk);
        memcpy(kk, &k, sizeof k);
        DBT key = { .data = kk, .size = sizeof kk };
        DBT val;
        r = db->get(db, NULL, &key, &val, 0);
        assert(r==DB_NOTFOUND);

}

static void
run_test(void) {
    int r;
    DB_ENV *env = NULL;
    r = db_env_create(&env, 0); assert_zero(r);
    env->set_errfile(env, stderr);
  
    //setting up the cachetable size 128k
    uint32_t cachesize = 64*1024;
    r = env->set_cachesize(env, 0, cachesize, 1); CKERR(r);
    //setting up the log write block size to 4k so the rollback log nodes spill in accordance with
    //the node size
    r = env->set_lg_bsize(env, 4096);                                                   CKERR(r);
    
    r = env->open(env, TOKU_TEST_FILENAME, DB_INIT_MPOOL|DB_CREATE|DB_THREAD |DB_INIT_LOCK|DB_INIT_LOG|DB_INIT_TXN|DB_PRIVATE, S_IRWXU+S_IRWXG+S_IRWXO); 
    assert_zero(r);

    DB *db = NULL;
    r = db_create(&db, env, 0); 
    assert_zero(r);
    
    r = db->set_pagesize(db, 4096);
    assert_zero(r);
    
    r = db->set_readpagesize(db, 1024);
    assert_zero(r);
 
    r = db->open(db, NULL, "test.tdb", NULL, DB_BTREE, DB_AUTO_COMMIT+DB_CREATE, S_IRWXU+S_IRWXG+S_IRWXO); 
    assert_zero(r);

    populate(env, db);

    r = db->close(db, 0); assert_zero(r);

    r = env->close(env, 0); assert_zero(r);
}

int
test_main(int argc, char * const argv[]) {
    int r;

    // parse_args(argc, argv);
    for (int i = 1; i < argc; i++) {
        char * const arg = argv[i];
        if (strcmp(arg, "-v") == 0) {
            verbose++;
            continue;
        }
        if (strcmp(arg, "-q") == 0) {
            verbose = 0;
            continue;
        }
    }

    toku_os_recursive_delete(TOKU_TEST_FILENAME);
    r = toku_os_mkdir(TOKU_TEST_FILENAME, S_IRWXU+S_IRWXG+S_IRWXO); assert_zero(r);

    run_test();

    return 0;
}

