/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <utility>

#include <db.h>
#include "test.h"  // hax

#include "cursor.hpp"

const uint32_t N = 100000;

static void fill(DB_ENV *env, DB *db) {
    int r;

    DB_TXN *txn;
    r = env->txn_begin(env, NULL, &txn, 0);
    assert_zero(r);

    {
        uint32_t k;
        char vbuf[1<<10];
        memset(vbuf, 'x', sizeof vbuf);
        DBT key, val;
        dbt_init(&key, &k, sizeof k);
        dbt_init(&val, vbuf, sizeof vbuf);
        for (uint32_t i = 0; i < N; ++i) {
            k = i;
            db->put(db, txn, &key, &val, 0);
            assert_zero(r);
        }
    }

    r = txn->commit(txn, 0);
    assert_zero(r);
}

struct UIntComparator {
    int operator()(const DBT *a, const DBT *b) {
        return uint_dbt_cmp((DB *) this /*lol*/, a, b);
    }
};

static void run_test(DB_ENV *env, DB *db) {
    fill(env, db);

    DB_TXN *txn;
    int r = env->txn_begin(env, NULL, &txn, 0);
    assert_zero(r);

    ftcxx::Cursor cur(db, txn);

    uint32_t lk;
    uint32_t rk;

    DBT left;
    DBT right;
    dbt_init(&left, &lk, sizeof lk);
    dbt_init(&right, &rk, sizeof rk);
    for (uint32_t i = 0; i < N; i += 1000) {
        lk = i;
        rk = i + 499;

        DBT key;
        DBT val;
        uint32_t expect = i;
        uint32_t last = 0;
        for (auto it(cur.iterator(&left, &right, UIntComparator(), ftcxx::Cursor::NoFilter())); it.next(&key, &val); ) {
            assert(sizeof(uint32_t) == key.size);
            last = *reinterpret_cast<const uint32_t *>(key.data);
            assert(expect == last);
            expect++;
        }
        assert(last == (i + 499));
    }

    r = txn->commit(txn, 0);
    assert_zero(r);
}

int test_main(int argc, char *const argv[]) {
    int r;
    const char *env_dir = TOKU_TEST_FILENAME;
    const char *db_filename = "ftcxx_cursor_test";
    parse_args(argc, argv);

    char rm_cmd[strlen(env_dir) + strlen("rm -rf ") + 1];
    snprintf(rm_cmd, sizeof(rm_cmd), "rm -rf %s", env_dir);
    r = system(rm_cmd);
    assert_zero(r);

    r = toku_os_mkdir(env_dir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
    assert_zero(r);

    DB_ENV *env = NULL;
    r = db_env_create(&env, 0);
    assert_zero(r);
    r = env->set_default_bt_compare(env, uint_dbt_cmp);
    assert_zero(r);
    int env_open_flags = DB_CREATE | DB_PRIVATE | DB_INIT_MPOOL | DB_INIT_TXN | DB_INIT_LOCK | DB_INIT_LOG;
    r = env->open(env, env_dir, env_open_flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    assert_zero(r);

    // create the db
    DB *db = NULL;
    r = db_create(&db, env, 0);
    assert_zero(r);
    DB_TXN *create_txn = NULL;
    r = env->txn_begin(env, NULL, &create_txn, 0);
    assert_zero(r);
    r = db->open(db, create_txn, db_filename, NULL, DB_BTREE, DB_CREATE, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    assert_zero(r);
    r = create_txn->commit(create_txn, 0);
    assert_zero(r);

    run_test(env, db);

    r = db->close(db, 0);
    assert_zero(r);

    r = env->close(env, 0);
    assert_zero(r);

    return 0;
}
