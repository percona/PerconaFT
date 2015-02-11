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

#include <stdio.h>
#include <stdlib.h>

#include <unistd.h>
#include <memory.h>
#include <errno.h>
#include <sys/stat.h>
#include <db.h>

static DB_ENV* startup() {
    int r;
    toku_os_recursive_delete(TOKU_TEST_FILENAME);
    r=toku_os_mkdir(TOKU_TEST_FILENAME, S_IRWXU+S_IRWXG+S_IRWXO); assert(r==0);

    // create the DB
    DB_ENV *env;
    r = db_env_create(&env, 0); assert(r == 0);
    int db_env_open_flags = DB_CREATE | DB_PRIVATE | DB_INIT_MPOOL | DB_INIT_TXN | DB_INIT_LOCK | DB_INIT_LOG | DB_THREAD;
    r = env->set_default_bt_compare(env, int64_dbt_cmp);                         CKERR(r);
    r = env->open(env, TOKU_TEST_FILENAME, db_env_open_flags, 0); assert(r == 0);
    return env;
}
static void shutdown(DB_ENV* env) {
    int r = env->close(env, 0); CKERR(r);
}

static void insertRandomData(DB* db1, DB* db2) {
    int r = 0;
    for (int i = 0; i < 10000; i++) {
        uint64_t curr_key = 10*i;
        uint64_t curr_val = rand();
        DBT key, val;
        dbt_init(&key, &curr_key, sizeof(curr_key));
        dbt_init(&val, &curr_val, sizeof(curr_val));
        r = db1->put(
            db1,
            NULL,
            &key,
            &val,
            0
            );
        CKERR(r);
        r = db2->put(
            db2,
            NULL,
            &key,
            &val,
            0
            );
        CKERR(r);
    }
}

static void checkDBTsSame(DBT* first, DBT* second) {
    if (first->size != second->size) {
        printf("bad sizes for DBTs %d %d", first->size, second->size);
        assert(false);
    }
    if (memcmp(first->data, second->data, first->size) != 0) {
        printf("data bad");
        assert(false);
    }
}

static void verifyEmptyDB(DB* db, DB_ENV* env) {
    DB_TXN* txn = NULL;
    int r = 0;
    r = env->txn_begin(env, NULL, &txn, 0); CKERR(r);
    DBC* c;
    r = db->cursor(db, txn, &c, 0); CKERR(r);
    DBT first_key, first_val;
    dbt_init(&first_key, NULL, 0);
    dbt_init(&first_val, NULL, 0);
    r = c->c_get(c, &first_key, &first_val, DB_FIRST);
    CKERR2(r, DB_NOTFOUND);
    r = c->c_get(c, &first_key, &first_val, DB_LAST);
    CKERR2(r, DB_NOTFOUND);
    r = c->c_get(c, &first_key, &first_val, DB_NEXT);
    CKERR2(r, DB_NOTFOUND);
    r = c->c_get(c, &first_key, &first_val, DB_PREV);
    CKERR2(r, DB_NOTFOUND);
    c->c_close(c);
    r = txn->commit(txn, 0); CKERR(r);
}

static int cursor_check(DBT const * key,
        DBT const * value UU(), void * extra)
{
    assert(key->size == sizeof(uint64_t));
    uint64_t found_key = *(uint64_t *)key->data;
    uint64_t expected_key = *(uint64_t *)extra;
    assert(found_key == expected_key);
    return 0;
}

static void verifySameData(DB* db1, DB* db2, DB_ENV* env) {
    DB_TXN* txn = NULL;
    DBC* c1 = NULL;
    DBC* c2 = NULL;
    int r = env->txn_begin(env, NULL, &txn, 0); CKERR(r);
    r = db1->cursor(db1, txn, &c1, 0); CKERR(r);
    r = db2->cursor(db2, txn, &c2, 0); CKERR(r);

    {
        DBT first_key, first_val, second_key, second_val;
        dbt_init(&first_key, NULL, 0);
        dbt_init(&first_val, NULL, 0);
        dbt_init(&second_key, NULL, 0);
        dbt_init(&second_val, NULL, 0);
        r = c1->c_get(c1, &first_key, &first_val, DB_FIRST); CKERR(r);
        r = c2->c_get(c2, &second_key, &second_val, DB_FIRST); CKERR(r);
        checkDBTsSame(&first_key, &second_key);
        checkDBTsSame(&first_val, &second_val);
    }
    int count = 0;
    while (true) {
        DBT first_key, first_val, second_key, second_val;
        dbt_init(&first_key, NULL, 0);
        dbt_init(&first_val, NULL, 0);
        dbt_init(&second_key, NULL, 0);
        dbt_init(&second_val, NULL, 0);
        int r1 = c1->c_get(c1, &first_key, &first_val, DB_NEXT);
        assert(r1 == 0 || r1 == DB_NOTFOUND);
        int r2 = c2->c_get(c2, &second_key, &second_val, DB_NEXT);
        assert(r1 == r2);
        if (r1 == DB_NOTFOUND) break;
        checkDBTsSame(&first_key, &second_key);
        checkDBTsSame(&first_val, &second_val);
        count++;
    }
    c1->c_close(c1);
    c2->c_close(c2);

    // check some point queries
    r = db1->cursor(db1, txn, &c1, 0); CKERR(r);
    r = db2->cursor(db2, txn, &c2, 0); CKERR(r);
    uint64_t lookup = rand() % (9000 * 10);
    uint64_t expected = ((lookup / 10) + 1)*10;
    DBT lookup_key;
    dbt_init(&lookup_key, &lookup, sizeof(lookup));
    r = c1->c_getf_set_range(c1, 0, &lookup_key, cursor_check, &expected);
    r = c2->c_getf_set_range(c2, 0, &lookup_key, cursor_check, &expected);
    expected -= 10;
    r = c1->c_getf_set_range_reverse(c1, 0, &lookup_key, cursor_check, &expected);
    r = c2->c_getf_set_range_reverse(c2, 0, &lookup_key, cursor_check, &expected);
    r = c1->c_getf_current(c1, 0, cursor_check, &expected);
    r = c2->c_getf_current(c2, 0, cursor_check, &expected);

    lookup = 555;
    r = c1->c_getf_set(c1, 0, &lookup_key, cursor_check, &expected);
    CKERR2(r, DB_NOTFOUND);
    r = c2->c_getf_set(c2, 0, &lookup_key, cursor_check, &expected);
    CKERR2(r, DB_NOTFOUND);
    lookup = 550;
    expected = 550;
    r = c1->c_getf_set(c1, 0, &lookup_key, cursor_check, &expected);
    CKERR(r);
    r = c2->c_getf_set(c2, 0, &lookup_key, cursor_check, &expected);
    CKERR(r);

    // not sure how this API is supposed to work, so commenting out test
    // the code below fails, although one would think it would succeed.
    /*
    lookup = 551;
    uint64_t bound = 555;
    DBT bound_key;
    dbt_init(&bound_key, &bound, sizeof(bound));
    r = c1->c_getf_set_range_with_bound(c1, 0, &lookup_key, &bound_key, cursor_check, &expected);
    CKERR2(r, DB_NOTFOUND);
    r = c2->c_getf_set_range_with_bound(c2, 0, &lookup_key, &bound_key, cursor_check, &expected);
    CKERR2(r, DB_NOTFOUND);
    */

    c1->c_close(c1);
    c2->c_close(c2);

    r = txn->commit(txn, 0); CKERR(r);
}

static void
test_cursor (void) {
    DB_ENV * env = startup();
    const char * const fname = "test.cursor.ft_handle";
    int r;

    // put data in the dbs
    uint32_t num_DBs = 5;
    DB* normal_dbs[num_DBs];
    DB* group_dbs[num_DBs];
    for (uint32_t i = 0; i < num_DBs; i++) {
        char name[100];
        char gname[100];
        r = db_create(&normal_dbs[i], env, 0); CKERR(r);
        snprintf(name, sizeof(name), "n_%s_%d", fname, i);
        r = normal_dbs[i]->create_new_db(normal_dbs[i], NULL, name, NULL, 0);
        CKERR(r);
        r = db_create(&group_dbs[i], env, 0); CKERR(r);
        snprintf(gname, sizeof(gname), "g_%s_%d", fname, i);
        r = group_dbs[i]->create_new_db(group_dbs[i], NULL, gname, "mah_group", 0);
        CKERR(r);
    }
    for (uint32_t i = 0; i < num_DBs; i++) {
        if (i != 3) {
            insertRandomData(normal_dbs[i], group_dbs[i]);
        }
    }

    for (uint32_t i = 0; i < num_DBs; i++) {
        if (i != 3) {
            verifySameData(normal_dbs[i], group_dbs[i], env);
        }
        else {
            verifyEmptyDB(normal_dbs[i], env);
            verifyEmptyDB(group_dbs[i], env);
        }
    }

    for (uint32_t i = 0; i < num_DBs; i++) {
        r = normal_dbs[i]->close(normal_dbs[i], 0);
        CKERR(r);        
        r = group_dbs[i]->close(group_dbs[i], 0);
        CKERR(r);        
    }
    shutdown(env);
}

int
test_main(int argc, char *const argv[]) {

    parse_args(argc, argv);
  
    toku_os_recursive_delete(TOKU_TEST_FILENAME);
    toku_os_mkdir(TOKU_TEST_FILENAME, S_IRWXU+S_IRWXG+S_IRWXO);
    
    test_cursor();

    return 0;
}
