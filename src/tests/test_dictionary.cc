/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
#ident "$Id$"
/*
COPYING CONDITIONS NOTICE:

  This program is free software; you can redistribute it and/or modify
  it under the terms of version 2 of the GNU General Public License as
  published by the Free Software Foundation, and provided that the
  following conditions are met:

      * Redistributions of source code must retain this COPYING
        CONDITIONS NOTICE, the COPYRIGHT NOTICE (below), the
        DISCLAIMER (below), the UNIVERSITY PATENT NOTICE (below), the
        PATENT MARKING NOTICE (below), and the PATENT RIGHTS
        GRANT (below).

      * Redistributions in binary form must reproduce this COPYING
        CONDITIONS NOTICE, the COPYRIGHT NOTICE (below), the
        DISCLAIMER (below), the UNIVERSITY PATENT NOTICE (below), the
        PATENT MARKING NOTICE (below), and the PATENT RIGHTS
        GRANT (below) in the documentation and/or other materials
        provided with the distribution.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
  02110-1301, USA.

COPYRIGHT NOTICE:

  TokuFT, Tokutek Fractal Tree Indexing Library.
  Copyright (C) 2007-2013 Tokutek, Inc.

DISCLAIMER:

  This program is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

UNIVERSITY PATENT NOTICE:

  The technology is licensed by the Massachusetts Institute of
  Technology, Rutgers State University of New Jersey, and the Research
  Foundation of State University of New York at Stony Brook under
  United States of America Serial No. 11/760379 and to the patents
  and/or patent applications resulting from it.

PATENT MARKING NOTICE:

  This software is covered by US Patent No. 8,185,551.
  This software is covered by US Patent No. 8,489,638.

PATENT RIGHTS GRANT:

  "THIS IMPLEMENTATION" means the copyrightable works distributed by
  Tokutek as part of the Fractal Tree project.

  "PATENT CLAIMS" means the claims of patents that are owned or
  licensable by Tokutek, both currently or in the future; and that in
  the absence of this license would be infringed by THIS
  IMPLEMENTATION or by using or running THIS IMPLEMENTATION.

  "PATENT CHALLENGE" shall mean a challenge to the validity,
  patentability, enforceability and/or non-infringement of any of the
  PATENT CLAIMS or otherwise opposing any of the PATENT CLAIMS.

  Tokutek hereby grants to you, for the term and geographical scope of
  the PATENT CLAIMS, a non-exclusive, no-charge, royalty-free,
  irrevocable (except as stated in this section) patent license to
  make, have made, use, offer to sell, sell, import, transfer, and
  otherwise run, modify, and propagate the contents of THIS
  IMPLEMENTATION, where such license applies only to the PATENT
  CLAIMS.  This grant does not include claims that would be infringed
  only as a consequence of further modifications of THIS
  IMPLEMENTATION.  If you or your agent or licensee institute or order
  or agree to the institution of patent litigation against any entity
  (including a cross-claim or counterclaim in a lawsuit) alleging that
  THIS IMPLEMENTATION constitutes direct or contributory patent
  infringement, or inducement of patent infringement, then any rights
  granted to you under this License shall terminate as of the date
  such litigation is filed.  If you or your agent or exclusive
  licensee institute or order or agree to the institution of a PATENT
  CHALLENGE, then Tokutek may terminate any rights granted to you
  under this License.
*/

#ident "Copyright (c) 2007-2013 Tokutek Inc.  All rights reserved."
#include "test.h"
#include <fcntl.h>
#include <src/ydb-internal.h>

DB_TXN * const null_txn = 0;

const char * const fname = "dict_test";

// return non-zero if file exists
static bool
file_exists(DB_ENV* env, char * filename) {
    char * dirname = env->i->real_data_dir;
    int n = 0;
    DIR * dir = opendir(dirname);
    
    struct dirent *ent;
    while ((ent=readdir(dir))) {
        if ((ent->d_type==DT_REG || ent->d_type==DT_UNKNOWN) && strcmp(ent->d_name, filename)==0) {
            n++;
        }
    }
    closedir(dir);
    return n > 0;
}

class dictionary_test {
public:

    static void verify_db_solo(dictionary_info* dinfo, DB* db) {
        assert(db->i->dict->m_id >= persistent_dictionary_manager::m_min_user_id);
        assert(db->i->dict->m_num_prepend_bytes == 0);
        assert(db->i->dict->m_prepend_id == PREPEND_ID_INVALID);

        assert(strcmp(dinfo->dname, db->i->dict->m_dname) == 0);
        assert(dinfo->id == db->i->dict->m_id);
        assert(dinfo->groupname == NULL);
        assert(dinfo->num_prepend_bytes == 0);
        assert(dinfo->prepend_id == PREPEND_ID_INVALID);
    }

    static void verify_db_part_of_big_group(dictionary_info* dinfo, DB* db, const char* expected_groupname) {
        assert(strcmp(dinfo->dname, db->i->dict->m_dname) == 0);
        assert(db->i->dict->m_id >= persistent_dictionary_manager::m_min_user_id);
        assert(dinfo->id == db->i->dict->m_id);
        assert(dinfo->groupname != NULL);
        assert(strcmp(dinfo->groupname, expected_groupname) == 0);
        assert(dinfo->num_prepend_bytes == sizeof(uint64_t));
        assert(dinfo->prepend_id == dinfo->id);
        assert(dinfo->prepend_id != PREPEND_ID_INVALID);
    }

    static void verify_iname_refcount(DB_ENV* env, DB_TXN* txn, dictionary_info* dinfo, uint64_t expected_refcount) {
        uint64_t found_refcount = 0;
        int r = env->i->dict_manager.pdm.get_iname_refcount(dinfo->iname, txn, &found_refcount); CKERR(r);
        assert(found_refcount == expected_refcount);
    }

    static bool iname_exists(DB_ENV* env, dictionary_info* dinfo) {
        return file_exists(env, dinfo->iname);
    }

    static DB_ENV* startup() {
        int r;
        toku_os_recursive_delete(TOKU_TEST_FILENAME);
        r=toku_os_mkdir(TOKU_TEST_FILENAME, S_IRWXU+S_IRWXG+S_IRWXO); assert(r==0);

        // create the DB
        DB_ENV *env;
        r = db_env_create(&env, 0); assert(r == 0);
        int db_env_open_flags = DB_CREATE | DB_PRIVATE | DB_INIT_MPOOL | DB_INIT_TXN | DB_INIT_LOCK | DB_INIT_LOG | DB_THREAD;
        r = env->open(env, TOKU_TEST_FILENAME, db_env_open_flags, 0); assert(r == 0);
        return env;
    }
    static void shutdown(DB_ENV* env) {
        int r = env->close(env, 0); CKERR(r);
    }

    // test that two handles to the same dictionary
    // have the same dictionary object
    static void run_single_db_test() {
        // run test twice, once without a groupname and once with
        for (int i = 0; i < 2; i++) {
            const char* groupname = (i == 0) ? nullptr : "group";
            DB_ENV* env = startup();
            DB *db1;
            int r = db_create(&db1, env, 0); assert(r == 0);
            r = db1->create_new_db(db1, null_txn, fname, groupname, 0);
            CKERR(r);

            assert(env->i->dict_manager.num_open_dictionaries() == 1);
            assert(db1->i->dict->m_refcount == 1);
            assert(db1->i->dict->m_id >= persistent_dictionary_manager::m_min_user_id);
            if (groupname == nullptr) {
                assert(db1->i->dict->m_num_prepend_bytes == 0);
                assert(db1->i->dict->m_prepend_id == PREPEND_ID_INVALID);
            }
            else {
                assert(db1->i->dict->m_num_prepend_bytes > 0);
                assert(db1->i->dict->m_prepend_id != PREPEND_ID_INVALID);
                assert(db1->i->dict->m_prepend_id == db1->i->dict->m_id);
            }

            DB *db2;
            r = db_create(&db2, env, 0); assert(r == 0);
            r = db2->open(db2, null_txn, fname, NULL, DB_BTREE, 0, 0666);
            CKERR(r);
            // verify that they have the same dictionary
            assert(db1->i->dict == db2->i->dict);
            assert(db1->i->dict->m_refcount == 2);

            r = db1->close(db1, 0); assert(r == 0);
            assert(env->i->dict_manager.num_open_dictionaries() == 1);
            assert(db2->i->dict->m_refcount == 1);
            r = db2->close(db2, 0); assert(r == 0);
            assert(env->i->dict_manager.num_open_dictionaries() == 0);

            shutdown(env);
        }
    }

    // we create 10 different dictionaries, each with multiple handles
    // we then verify that as we close all the handles, the ref counts
    // are correct and they get destroyed at the appropriate time
    static void run_multiple_db_test() {
        DB_ENV* env = startup();
        uint32_t num_dnames = 10;
        char* dnames[num_dnames];
        uint32_t expected_refs[num_dnames];
        uint32_t expected_num_open_dict = 0;
        // create dictionaries
        for (uint32_t i = 0; i < num_dnames; i++) {
            expected_refs[i] = 0;
            char curr_dname[32];
            sprintf(curr_dname, "%d-dict", i);
            dnames[i] = toku_strdup(curr_dname);
            DB* db;
            int r = db_create(&db, env, 0); CKERR(r);
            r = db->create_new_db(db, null_txn, dnames[i], NULL, 0); CKERR(r);
            r = db->close(db, 0); CKERR(r);
        }
        assert(env->i->dict_manager.num_open_dictionaries() == 0);
        // now let's create a bunch of handles
        DB* dbs[num_dnames*10];
        uint32_t num_handles = sizeof(dbs)/sizeof(dbs[0]);
        for (uint32_t i = 0; i < num_handles; i++) {
            dbs[i] = NULL;
        }

        // this is the test 
        for (uint32_t i = 0; i < 1000; i++) {
            uint32_t curr = rand() % num_handles;
            if (dbs[curr] == NULL) {
                if (expected_refs[curr % num_dnames] == 0) {
                    expected_num_open_dict++;
                }
                expected_refs[curr % num_dnames]++;
                int r = db_create(&dbs[curr], env, 0); CKERR(r);
                r = dbs[curr]->open(dbs[curr], null_txn, dnames[curr % num_dnames], NULL, DB_BTREE, 0, 0666);
                CKERR(r);
            }
            else {
                int r = dbs[curr]->close(dbs[curr], 0);
                dbs[curr] = NULL;
                CKERR(r);
                assert(expected_refs[curr % num_dnames] > 0);
                expected_refs[curr % num_dnames]--;
                if (expected_refs[curr % num_dnames] == 0) {
                    assert(expected_num_open_dict > 0);
                    expected_num_open_dict--;
                }
            }
            assert(expected_num_open_dict == env->i->dict_manager.num_open_dictionaries());
        }

        // cleanup, close dictionaries
        for (uint32_t i = 0; i < num_handles; i++) {
            if (dbs[i] != NULL) {
                int r = dbs[i]->close(dbs[i], 0); CKERR(r);
                dbs[i] = NULL;
            }
        }
        assert(env->i->dict_manager.num_open_dictionaries() == 0);

        for (uint32_t i = 0; i < num_dnames; i++) {
            toku_free(dnames[i]);
        }
        shutdown(env);
    }

    // test that dictionary ids are generated in a predictable manner
    // that we also get the right ids after a shutdown
    static void run_dictionary_id_generation_test() {
        DB_ENV* env = startup();
        DB *db1;
        int r = db_create(&db1, env, 0); assert(r == 0);
        r = db1->create_new_db(db1, null_txn, "foo", NULL, 0); CKERR(r);
        assert(db1->i->dict->m_id >= persistent_dictionary_manager::m_min_user_id);
        uint64_t db1_id = db1->i->dict->m_id;

        DB *db2;
        r = db_create(&db2, env, 0); assert(r == 0);
        r = db2->create_new_db(db2, null_txn, "foo", NULL, 0); CKERR2(r, EEXIST);
        r = db2->create_new_db(db2, null_txn, "bar", NULL, 0); CKERR(r);
        uint64_t db2_id = db2->i->dict->m_id;
        assert(db2_id == db1_id + 1);

        // create a dictionary with a transaction and then abort it
        // verify that the next dictionary created afterwards has a higher id
        // than the one used here
        DB *db3;
        r = db_create(&db3, env, 0); assert(r == 0);
        DB_TXN* txn_abort = NULL;
        r = env->txn_begin(env, 0, &txn_abort, 0); CKERR(r);
        r = db3->create_new_db(db3, txn_abort, "fooff", NULL, 0); CKERR(r);
        uint64_t db3_id = db3->i->dict->m_id;
        assert(db3_id == db2_id + 1);
        r = db3->close(db3, 0); CKERR(r);
        r = txn_abort->abort(txn_abort); CKERR(r);

        // now recreate the dictionary with the same iname, make sure there is a different id
        DB *db4;
        r = db_create(&db4, env, 0); assert(r == 0);
        r = db4->create_new_db(db4, null_txn, "fooff", NULL, 0); CKERR(r);
        uint64_t db4_id = db4->i->dict->m_id;
        assert(db4_id == db3_id + 1);


        r = db1->close(db1, 0); CKERR(r);
        r = db2->close(db2, 0); CKERR(r);
        r = db4->close(db4, 0); CKERR(r);
        shutdown(env);

        // now let's restart the environment
        r = db_env_create(&env, 0); assert(r == 0);
        int db_env_open_flags = DB_PRIVATE | DB_INIT_MPOOL | DB_INIT_TXN | DB_INIT_LOCK | DB_INIT_LOG | DB_THREAD;
        r = env->open(env, TOKU_TEST_FILENAME, db_env_open_flags, 0); assert(r == 0);

        // create a new dictionary and verify it has an id greater than the biggest used pre-shutdown
        DB *db5;
        r = db_create(&db5, env, 0); assert(r == 0);
        r = db5->create_new_db(db5, null_txn, "barff", NULL, 0); CKERR(r);
        uint64_t db5_id = db5->i->dict->m_id;
        assert(db5_id == db4_id + 1);

        // make sure retrieving ids from older dictionaries is right
        r = db_create(&db1, env, 0); assert(r == 0);
        r = db1->open(db1, null_txn, "foo", NULL, DB_BTREE, 0, 0666); CKERR(r);
        assert(db1->i->dict->m_id == db1_id);

        r = db_create(&db2, env, 0); assert(r == 0);
        r = db2->open(db2, null_txn, "bar", NULL, DB_BTREE, 0, 0666); CKERR(r);
        assert(db2->i->dict->m_id == db2_id);

        r = db_create(&db4, env, 0); assert(r == 0);
        r = db4->open(db4, null_txn, "fooff", NULL, DB_BTREE, 0, 0666); CKERR(r);
        assert(db4->i->dict->m_id == db4_id);

        r = db1->close(db1, 0); CKERR(r);
        r = db2->close(db2, 0); CKERR(r);
        r = db4->close(db4, 0); CKERR(r);
        r = db5->close(db5, 0); CKERR(r);
        shutdown(env);
    }

    // this tests that creating/renaming/removing dbs with no groupname
    // work properly, that they have no prepend bytes, and that when
    // we remove the dictionary, their inames dissappear
    static void test_no_groupname() {
        DB_ENV* env = startup();
        const char* foo = "foo";
        DB *db1;
        // just creating some dummy dictionaries so that the metadata dictionaries
        // we deal with have more than one entry
        int r = db_create(&db1, env, 0); assert(r == 0);
        r = db1->create_new_db(db1, null_txn, "aaa", NULL, 0); CKERR(r);
        r = db1->close(db1, 0); CKERR(r);
        r = db_create(&db1, env, 0); assert(r == 0);
        r = db1->create_new_db(db1, null_txn, "ddd", NULL, 0); CKERR(r);
        r = db1->close(db1, 0); CKERR(r);
        r = db_create(&db1, env, 0); assert(r == 0);
        r = db1->create_new_db(db1, null_txn, "mmm", NULL, 0); CKERR(r);
        r = db1->close(db1, 0); CKERR(r);
        r = db_create(&db1, env, 0); assert(r == 0);
        r = db1->create_new_db(db1, null_txn, "zzz", NULL, 0); CKERR(r);
        r = db1->close(db1, 0); CKERR(r);

        // now the real test begins
        r = db_create(&db1, env, 0); assert(r == 0);
        r = db1->create_new_db(db1, null_txn, foo, NULL, 0);
        CKERR(r);

        DB_TXN* txn = NULL;
        r = env->txn_begin(env, 0, &txn, 0); CKERR(r);
        dictionary_info dinfo;
        r = env->i->dict_manager.pdm.get_dinfo(foo, txn, &dinfo); CKERR(r);
        // verify that the dinfo information is correct
        verify_db_solo(&dinfo, db1);
        assert(iname_exists(env, &dinfo));
        verify_iname_refcount(env, txn, &dinfo, 1);
        r = env->dbremove(env, txn, foo, NULL, 0); CKERR2(r, EINVAL);
        r = txn->commit(txn, 0); CKERR(r);
        r = db1->close(db1, 0); CKERR(r);

        r = env->txn_begin(env, 0, &txn, 0); CKERR(r);
        r = env->dbremove(env, txn, foo, NULL, 0); CKERR(r);
        assert(iname_exists(env, &dinfo));
        r = env->i->dict_manager.pdm.get_dinfo(foo, txn, &dinfo); CKERR2(r, DB_NOTFOUND);
        r = txn->commit(txn, 0); CKERR(r);
        assert(!iname_exists(env, &dinfo));
        r = db_create(&db1, env, 0); CKERR(r);
        r = db1->open(db1, null_txn, foo, NULL, DB_BTREE, 0, 0666); CKERR2(r, ENOENT);
        r = db1->close(db1, 0); CKERR(r);

        shutdown(env);
        dinfo.destroy();
    }

    // does a rename, verifies that all data is the same
    // except for the dname
    static void test_simple_rename() {
        DB_ENV* env = startup();
        const char* foo = "foo";
        const char* bar = "bar";
        DB *db1;

        // now the real test begins
        int r = db_create(&db1, env, 0); assert(r == 0);
        r = db1->create_new_db(db1, null_txn, foo, NULL, 0);
        CKERR(r);

        DB_TXN* txn = NULL;
        r = env->txn_begin(env, 0, &txn, 0); CKERR(r);
        dictionary_info dinfo;
        r = env->i->dict_manager.pdm.get_dinfo(foo, txn, &dinfo); CKERR(r);
        // verify that the dinfo information is correct
        verify_db_solo(&dinfo, db1);
        r = txn->commit(txn, 0); CKERR(r);
        r = db1->close(db1, 0); CKERR(r);

        r = env->txn_begin(env, 0, &txn, 0); CKERR(r);
        r = env->dbrename(env, txn, foo, NULL, bar, 0); CKERR(r);
        assert(iname_exists(env, &dinfo));
        r = env->i->dict_manager.pdm.get_dinfo(foo, txn, &dinfo); CKERR2(r, DB_NOTFOUND);
        r = txn->commit(txn, 0); CKERR(r);
        r = db_create(&db1, env, 0); CKERR(r);
        r = env->txn_begin(env, 0, &txn, 0); CKERR(r);
        r = db1->open(db1, null_txn, foo, NULL, DB_BTREE, 0, 0666); CKERR2(r, ENOENT);
        r = db1->open(db1, null_txn, bar, NULL, DB_BTREE, 0, 0666); CKERR(r);
        dictionary_info dinfo2;
        r = env->i->dict_manager.pdm.get_dinfo(bar, txn, &dinfo2); CKERR(r);
        verify_db_solo(&dinfo2, db1);
        r = txn->commit(txn, 0); CKERR(r);
        r = db1->close(db1, 0); CKERR(r);

        // check that the dinfo's are the same outside of dname
        assert(dinfo.id == dinfo2.id);
        assert(strcmp(dinfo.iname, dinfo2.iname) == 0);
        assert(iname_exists(env, &dinfo2));

        shutdown(env);
        dinfo.destroy();
        dinfo2.destroy();
    }
};


int
test_main(int argc, char *const argv[]) {
    parse_args(argc, argv);

    /*
    dictionary_test::run_single_db_test();
    dictionary_test::run_multiple_db_test();
    dictionary_test::run_dictionary_id_generation_test();
*/
    dictionary_test::test_no_groupname();
    dictionary_test::test_simple_rename();
    return 0;
}
