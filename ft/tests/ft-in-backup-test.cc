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
#include "cachetable/checkpoint.h"

static TOKUTXN const null_txn = 0;
static const char *fname = TOKU_TEST_FILENAME;

/* test for_backup in ft_close */
static void test_in_backup() {
    int r;
    CACHETABLE ct;
    FT_HANDLE ft;
    unlink(fname);

    toku_cachetable_create(&ct, 0, ZERO_LSN, nullptr);
    CHECKPOINTER cp = toku_cachetable_get_checkpointer(ct);

    // TEST1 : for normal
    r = toku_open_ft_handle(fname, 1, &ft, 1<<12, 1<<9, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun);
    assert_zero(r);
    r = toku_close_ft_handle_nolsn(ft, 0);
    assert_zero(r);

    r = toku_open_ft_handle(fname, 0, &ft, 1<<12, 1<<9, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun);
    assert_zero(r);
    {
        DBT k,v;
        toku_ft_insert(ft, toku_fill_dbt(&k, "hello", 6), toku_fill_dbt(&v, "there", 6), null_txn);
    }
    r = toku_close_ft_handle_nolsn(ft, 0);
    assert_zero(r);

    r = toku_open_ft_handle(fname, 0, &ft, 1<<12, 1<<9, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun);
    assert_zero(r);
    ft_lookup_and_check_nodup(ft, "hello", "there");
    r = toku_close_ft_handle_nolsn(ft, 0);
    assert_zero(r);
    toku_cachetable_close(&ct);

    // TEST2: in fly without checkpoint test
    toku_cachetable_create(&ct, 0, ZERO_LSN, nullptr);
    cp = toku_cachetable_get_checkpointer(ct);
    r = toku_open_ft_handle(fname, 0, &ft, 1<<12, 1<<9, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun);
    assert_zero(r);

    toku_cachetable_begin_backup(ct);
    // this key/value just in fly since we are in backing up
    {
        DBT k,v;
        toku_ft_insert(ft, toku_fill_dbt(&k, "halou", 6), toku_fill_dbt(&v, "not there", 10), null_txn);
    }
    r = toku_close_ft_handle_nolsn(ft, 0);
    assert_zero(r);

    r = toku_open_ft_handle(fname, 0, &ft, 1<<12, 1<<9, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun);
    assert_zero(r);
    ft_lookup_and_check_nodup(ft, "halou", "not there");

    // because we are in backup, so the FT header is stale after cachefile&cachetable closed
    // here has a leak for this ft evicts from memroy, but that makes sense
    r = toku_close_ft_handle_nolsn(ft, 0);
    assert_zero(r);
    toku_cachetable_end_backup(ct);
    toku_cachetable_close(&ct);

    // check the in fly key/value, it shouldn't exist
    toku_cachetable_create(&ct, 0, ZERO_LSN, nullptr);
    r = toku_open_ft_handle(fname, 0, &ft, 1<<12, 1<<9, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun);
    assert_zero(r);
    ft_lookup_and_fail_nodup(ft, (char*)"halou");
    r = toku_close_ft_handle_nolsn(ft, 0);
    assert_zero(r);
    toku_cachetable_close(&ct);

    // TEST3: in fly with checkpoint test
    toku_cachetable_create(&ct, 0, ZERO_LSN, nullptr);
    cp = toku_cachetable_get_checkpointer(ct);
    r = toku_open_ft_handle(fname, 0, &ft, 1<<12, 1<<9, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun);
    assert_zero(r);

    toku_cachetable_begin_backup(ct);
    // this key/value just in fly since we are in backup
    {
        DBT k,v;
        toku_ft_insert(ft, toku_fill_dbt(&k, "halou1", 7), toku_fill_dbt(&v, "not there", 10), null_txn);
    }
    r = toku_close_ft_handle_nolsn(ft, 0);
    assert_zero(r);

    r = toku_open_ft_handle(fname, 0, &ft, 1<<12, 1<<9, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun);
    assert_zero(r);
    ft_lookup_and_check_nodup(ft, "halou1", "not there");

    // because we are in backup, so the FT header is stale after cachefile&cachetable closed
    r = toku_close_ft_handle_nolsn(ft, 0);
    assert_zero(r);
    toku_cachetable_end_backup(ct);
    r = toku_checkpoint(cp, NULL, NULL, NULL, NULL, NULL, CLIENT_CHECKPOINT);
    assert_zero(r);
    toku_cachetable_close(&ct);

    // check after checkpoint
    toku_cachetable_create(&ct, 0, ZERO_LSN, nullptr);
    r = toku_open_ft_handle(fname, 0, &ft, 1<<12, 1<<9, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun);
    assert_zero(r);
    ft_lookup_and_check_nodup(ft, "halou1", "not there");
    r = toku_close_ft_handle_nolsn(ft, 0);
    assert_zero(r);
    toku_cachetable_close(&ct);
}

int
test_main (int argc , const char *argv[]) {
    default_parse_args(argc, argv);
    test_in_backup();
    if (verbose) printf("test ok\n");
    return 0;
}
