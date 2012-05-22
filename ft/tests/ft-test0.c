/* -*- mode: C; c-basic-offset: 4 -*- */
#ident "$Id$"
#ident "Copyright (c) 2007, 2008 Tokutek Inc.  All rights reserved."

#include "includes.h"
#include "test.h"

static TOKUTXN const null_txn = 0;
static DB * const null_db = 0;

static void test0 (void) {
    FT_HANDLE t;
    int r;
    CACHETABLE ct;
    char fname[]= __SRCFILE__ ".ft_handle";
    if (verbose) printf("%s:%d test0\n", __SRCFILE__, __LINE__);
    
    r = toku_create_cachetable(&ct, 0, ZERO_LSN, NULL_LOGGER);
    assert(r==0);
    if (verbose) printf("%s:%d test0\n", __SRCFILE__, __LINE__);
    unlink(fname);
    r = toku_open_ft_handle(fname, 1, &t, 1024, 256, TOKU_DEFAULT_COMPRESSION_METHOD, ct, null_txn, toku_builtin_compare_fun);
    assert(r==0);
    //printf("%s:%d test0\n", __SRCFILE__, __LINE__);
    //printf("%s:%d n_items_malloced=%lld\n", __SRCFILE__, __LINE__, n_items_malloced);
    r = toku_close_ft_handle_nolsn(t, 0);     assert(r==0);
    //printf("%s:%d n_items_malloced=%lld\n", __SRCFILE__, __LINE__, n_items_malloced);
    r = toku_cachetable_close(&ct);
    assert(r==0);
    
}

int
test_main (int argc , const char *argv[]) {
    default_parse_args(argc, argv);
    if (verbose) printf("test0 A\n");
    test0();
    if (verbose) printf("test0 B\n");
    test0(); /* Make sure it works twice. */
    
    if (verbose) printf("test0 ok\n");
    return 0;
}