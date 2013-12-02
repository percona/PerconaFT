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

  TokuDB, Tokutek Fractal Tree Indexing Library.
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
#include <errno.h>
#include <sys/stat.h>
#include <memory.h>
#include <db.h>

static void
db_put_int32 (DB *db, uint32_t k, uint32_t v) {
    DB_TXN * const null_txn = 0;
    DBT key, val;
    int r = db->put(db, null_txn, dbt_init(&key, &k, sizeof k), dbt_init(&val, &v, sizeof v), 0);
    CKERR(r);
}

static void
db_put_int64 (DB *db, uint64_t k, uint64_t v) {
    DB_TXN * const null_txn = 0;
    DBT key, val;
    int r = db->put(db, null_txn, dbt_init(&key, &k, sizeof k), dbt_init(&val, &v, sizeof v), 0);
    CKERR(r);
}


static void
test_cursor_recent_key_read (void) {
    if (verbose) printf("test_cursor_current\n");

    DB_TXN * const null_txn = 0;
    const char * const fname = "test.cursor.recent_key_read.ft_handle";
    int r;

    DB_ENV *env;
    r = db_env_create(&env, 0); assert(r == 0);
    r = env->open(env, TOKU_TEST_FILENAME, DB_INIT_MPOOL|DB_CREATE|DB_THREAD |DB_INIT_LOCK|DB_INIT_LOG|DB_INIT_TXN|DB_PRIVATE, 0); assert(r == 0);

    DB *db;
    r = db_create(&db, env, 0); CKERR(r);
    db->set_errfile(db,0); // Turn off those annoying errors
    r = db->open(db, null_txn, fname, "main", DB_BTREE, DB_CREATE, 0666); CKERR(r);

    uint32_t k = 42, v = 42000;
    uint32_t kk = 43, vv = 42001;
    db_put_int32(db, k, v);
    db_put_int64(db, kk, vv);
 
    DBC *cursor;

    DB_TXN* txn = NULL;
    r = env->txn_begin(env, 0, &txn, DB_TXN_SNAPSHOT);
    CKERR(r);

    r = db->cursor(db, txn, &cursor, 0); CKERR(r);
    DBT foo;
    memset(&foo, 0, sizeof(foo));

    r = cursor->c_get_recent_key_read(cursor, &foo);
    CKERR2(r, EINVAL);
    foo->flags = DB_DBT_REALLOC;
    r = cursor->c_get_recent_key_read(cursor, &foo);
    CKERR(r);
    assert(foo.ulen == 0);
    assert(foo.data == NULL);
    assert(foo.size == 0);

    DBT key, data;
    uint32_t first_key;
    uint64_t second_key;
    r = cursor->c_get(cursor, dbt_init_malloc(&key), dbt_init_malloc(&data), DB_NEXT);
    CKERR(r);
    // sanity check that the data we got is correct
    assert(key.size == sizeof first_key);
    memcpy(&first_key, key.data, sizeof first_key);
    assert(first_key == k);
    toku_free(key.data); toku_free(data.data);
    // check that API returns correct value
    r = cursor->c_get_recent_key_read(cursor, &foo);
    CKERR(r);
    assert(foo.size == sizeof first_key);
    memcpy(&first_key, foo.data, sizeof first_key);
    assert(first_key == k);

    r = cursor->c_get(cursor, dbt_init_malloc(&key), dbt_init_malloc(&data), DB_NEXT);
    CKERR(r);
    assert(key.size == sizeof second_key);
    memcpy(&second_key, key.data, sizeof second_key);
    assert(second_key == kk);

    // check that API returns correct value
    r = cursor->c_get_recent_key_read(cursor, &foo);
    CKERR(r);
    assert(foo.size == sizeof second_key);
    memcpy(&second_key, foo.data, sizeof second_key);
    assert(second_key == k);
    // now delete the key, and check that the API still works
    r = db->del(db, null_txn, &key, DB_DELETE_ANY);

    // check that API returns correct value
    r = cursor->c_get_recent_key_read(cursor, &foo);
    CKERR(r);
    assert(foo.size == sizeof second_key);
    memcpy(&second_key, foo.data, sizeof second_key);
    assert(second_key == k);
    toku_free(key.data); toku_free(data.data);
    
    r = cursor->c_close(cursor); CKERR(r);

    r = txn->commit(txn, 0);
    CKERR(r);    

    r = db->close(db, 0); CKERR(r);
    r = env->close(env, 0); CKERR(r);
}

int
test_main(int argc, char *const argv[]) {
    parse_args(argc, argv);
  
    toku_os_recursive_delete(TOKU_TEST_FILENAME);
    toku_os_mkdir(TOKU_TEST_FILENAME, S_IRWXU+S_IRWXG+S_IRWXO);

    test_cursor_recent_key_read();

    return 0;
}
