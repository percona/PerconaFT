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

#include <unistd.h>
#include <stdlib.h>
#include <libgen.h>
#include <sys/time.h>
#include <portability/toku_path.h>
#include "test.h"

#include "ft-flusher.h"

#include "cachetable/checkpoint.h"

static TOKUTXN const null_txn = NULL;

static int
noop_getf(uint32_t UU(keylen), const void *UU(key), uint32_t UU(vallen), const void *UU(val), void *extra, bool UU(lock_only))
{
    int *CAST_FROM_VOIDP(calledp, extra);
    (*calledp)++;
    return 0;
}

static int
get_one_value(FT_HANDLE t, CACHETABLE UU(ct), void *UU(extra))
{
    int r;
    int called;
    FT_CURSOR cursor;

    r = toku_ft_cursor(t, &cursor, null_txn, false, false);
    CKERR(r);
    called = 0;
    r = toku_ft_cursor_first(cursor, noop_getf, &called);
    CKERR(r);
    assert(called == 1);
    toku_ft_cursor_close(cursor);
    CKERR(r);

    return 0;
}

static int
progress(void *extra, float fraction)
{
    float *CAST_FROM_VOIDP(stop_at, extra);
    if (fraction > *stop_at) {
        return 1;
    } else {
        return 0;
    }
}

static int
do_hot_optimize(FT_HANDLE t, CACHETABLE UU(ct), void *extra)
{
    float *CAST_FROM_VOIDP(fraction, extra);
    uint64_t loops_run = 0;
    int r = toku_ft_hot_optimize(t, NULL, NULL, progress, extra, &loops_run);
    if (*fraction < 1.0) {
        CKERR2(r, 1);
    } else {
        CKERR(r);
    }
    return 0;
}

static int
insert_something(FT_HANDLE t, CACHETABLE UU(ct), void *UU(extra))
{
    assert(t);
    unsigned int dummy_value = 1U << 31;
    DBT key;
    DBT val;
    toku_fill_dbt(&key, &dummy_value, sizeof(unsigned int));
    toku_fill_dbt(&val, &dummy_value, sizeof(unsigned int));
    toku_ft_insert (t, &key, &val, 0);
    return 0;
}

typedef int (*tree_cb)(FT_HANDLE t, CACHETABLE ct, void *extra);

static int
with_open_tree(const char *fname, tree_cb cb, void *cb_extra)
{
    int r, r2;
    FT_HANDLE t;
    CACHETABLE ct;

    toku_cachetable_create(&ct, 16*(1<<20), ZERO_LSN, nullptr);
    r = toku_open_ft_handle(fname, 
                      0, 
                      &t, 
                      4*(1<<20), 
                      128*(1<<10), 
                      TOKU_DEFAULT_COMPRESSION_METHOD,
                      ct, 
                      null_txn, 
                      toku_builtin_compare_fun
                      );
    CKERR(r);

    r2 = cb(t, ct, cb_extra);
    r = toku_verify_ft(t);
    CKERR(r);
    CHECKPOINTER cp = toku_cachetable_get_checkpointer(ct);
    r = toku_checkpoint(cp, NULL, CLIENT_CHECKPOINT);
    CKERR(r);
    r = toku_close_ft_handle_nolsn(t, 0);
    CKERR(r);
    toku_cachetable_close(&ct);

    return r2;
}

#define TMPFTFMT "%s-tmpdata.ft"
static const char *origft_6_0 = "upgrade_test_data.ft.6.0.gz";
static const char *origft_5_0 = "upgrade_test_data.ft.5.0.gz";
static const char *origft_4_2 = "upgrade_test_data.ft.4.2.gz";
static const char *not_flat_4_2 = "upgrade_test_data.ft.4.2.not.flat.gz";

static int
run_test(const char *prog, const char *origft) {
    int r;

    char *fullprog = toku_strdup(__FILE__);
    char *progdir = dirname(fullprog);

    size_t templen = strlen(progdir) + strlen(prog) + strlen(TMPFTFMT) - 1;
    char tempft[templen + 1];
    snprintf(tempft, templen + 1, TMPFTFMT, prog);
    toku_free(fullprog);
    {
        char origbuf[TOKU_PATH_MAX + 1];
        char *datadir = getenv("TOKUDB_DATA");
        toku_path_join(origbuf, 2, datadir, origft);
        size_t len = 13 + strlen(origbuf) + strlen(tempft);
        char buf[len + 1];
        snprintf(buf, len + 1, "gunzip -c %s > %s", origbuf, tempft);
        r = system(buf);
        CKERR(r);
    }

    r = with_open_tree(tempft, get_one_value, NULL);
    CKERR(r);
    r = with_open_tree(tempft, insert_something, NULL);
    CKERR(r);
    float fraction = 0.5;
    r = with_open_tree(tempft, do_hot_optimize, &fraction);
    CKERR(r);
    fraction = 1.0;
    r = with_open_tree(tempft, do_hot_optimize, &fraction);
    CKERR(r);
    r = unlink(tempft);
    CKERR(r);

    return r;
}

int
test_main(int argc __attribute__((__unused__)), const char *argv[])
{
    int r;

    r = run_test(argv[0], origft_6_0);
    CKERR(r);
    r = run_test(argv[0], origft_5_0);
    CKERR(r);
    r = run_test(argv[0], origft_4_2);
    CKERR(r);

    r = run_test(argv[0], not_flat_4_2);
    CKERR(r);

    return r;
}
