/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <utility>

#include <db.h>
#include "test.h"  // hax

#include "cursor.hpp"
#include "db.hpp"
#include "db_env.hpp"
#include "db_txn.hpp"
#include "slice.hpp"

const uint32_t N = 100000;

static void fill(const ftcxx::DBEnv &env, const ftcxx::DB &db) {
    ftcxx::DBTxn txn(env);

    ftcxx::Slice val(1<<10);
    memset(val.mutable_data(), 'x', val.size());
    for (uint32_t i = 0; i < N; ++i) {
        int r = db.put(txn, ftcxx::Slice::slice_of(i), val);
        assert_zero(r);
    }

    txn.commit();
}

struct UIntComparator {
    int operator()(const ftcxx::Slice &a, const ftcxx::Slice &b) {
        DBT adbt = a.dbt();
        DBT bdbt = b.dbt();
        return uint_dbt_cmp((DB *) this /*lol*/, &adbt, &bdbt);
    }
};

static void run_test(const ftcxx::DBEnv &env, const ftcxx::DB &db) {
    fill(env, db);

    ftcxx::DBTxn txn(env);

    {
        uint32_t lk;
        uint32_t rk;

        for (uint32_t i = 0; i < N; i += 1000) {
            lk = i;
            rk = i + 499;

            ftcxx::Slice key;
            ftcxx::Slice val;
            uint32_t expect = i;
            uint32_t last = 0;
            for (auto cur(db.buffered_cursor(txn, ftcxx::Slice::slice_of(lk), ftcxx::Slice::slice_of(rk),
                                             UIntComparator(), ftcxx::DB::NullFilter()));
                 cur.next(key, val);
                 ) {
                last = key.as<uint32_t>();
                assert(expect == last);
                expect++;
            }
            assert(last == (i + 499));
        }
    }

    txn.commit();

    ftcxx::DBTxn extxn(env);

    {
        ftcxx::Slice key;
        ftcxx::Slice val;
        uint32_t expect = 0;
        uint32_t last = 0;
        for (auto cur(db.buffered_cursor(extxn, UIntComparator(), ftcxx::DB::NullFilter())); cur.next(key, val); ) {
            last = key.as<uint32_t>();
            assert(expect == last);
            expect++;
        }
        assert(last == N - 1);
    }

    {
        ftcxx::Slice key;
        ftcxx::Slice val;
        uint32_t expect = 0;
        uint32_t last = 0;
        for (auto cur(db.simple_cursor(extxn, UIntComparator(), key, val)); ; ) {
            std::cout << key.as<uint32_t>() << std::endl;
            last = key.as<uint32_t>();
            assert(expect == last);
            expect++;
            if (!cur.next()) {
                break;
            }
        }
        assert(last == N - 1);
    }

    extxn.commit();
}

int test_main(int argc, char *const argv[]) {
    int r;
    const char *old_env_dir = TOKU_TEST_FILENAME;
    char env_dir[strlen(old_env_dir)+32]; // use unique env directories for parallel tests
    snprintf(env_dir, sizeof env_dir, "%s.%d", old_env_dir, getpid());
    const char *db_filename = "ftcxx_cursor_test";
    parse_args(argc, argv);

    char rm_cmd[strlen(env_dir) + strlen("rm -rf ") + 1];
    snprintf(rm_cmd, sizeof(rm_cmd), "rm -rf %s", env_dir);
    r = system(rm_cmd);
    assert_zero(r);

    r = toku_os_mkdir(env_dir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
    assert_zero(r);

    int env_open_flags = DB_CREATE | DB_PRIVATE | DB_INIT_MPOOL | DB_INIT_TXN | DB_INIT_LOCK | DB_INIT_LOG;
    ftcxx::DBEnv env = ftcxx::DBEnvBuilder()
        .set_default_bt_compare(uint_dbt_cmp)
        .open(env_dir, env_open_flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

    ftcxx::DBTxn create_txn(env);
    ftcxx::DB db = ftcxx::DBBuilder()
        .open(env, create_txn, db_filename, NULL, DB_BTREE, DB_CREATE, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    create_txn.commit();

    run_test(env, db);

    db.close();

    env.close();

    r = system(rm_cmd);
    assert_zero(r);

    return 0;
}
