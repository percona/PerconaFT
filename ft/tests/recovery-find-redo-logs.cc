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

// Test that find redo-log files works correctly.

#include "test.h"
#include <libgen.h>

static int 
run_test(void) {
    int r;

    // setup the test dir
    toku_os_recursive_delete(TOKU_TEST_FILENAME);
    r = toku_os_mkdir(TOKU_TEST_FILENAME, S_IRWXU);
    assert(r == 0);

    char testdir[TOKU_PATH_MAX+1];
    char testfile[TOKU_PATH_MAX+1];

    toku_path_join(testdir, 2, TOKU_TEST_FILENAME, "log002015");
    r = toku_os_mkdir(testdir, S_IRWXU);
    assert(r == 0);

    toku_path_join(testfile, 2, TOKU_TEST_FILENAME, "log002015.tokulog27");
    int fd = open(testfile, O_CREAT+O_WRONLY+O_TRUNC+O_EXCL+O_BINARY, S_IRWXU);
    assert(fd != -1);
    close(fd);

    char **logfiles = nullptr;
    int n_logfiles = 0;
    r = toku_logger_find_logfiles(TOKU_TEST_FILENAME, &logfiles, &n_logfiles);
    CKERR(r);
    assert(n_logfiles == 1);
    fprintf(stderr, "redo log nums: %d \n", n_logfiles);
    for (int i; i < n_logfiles; i++) {
        fprintf(stderr, "redo log %s \n", logfiles[i]);
    }

    toku_logger_free_logfiles(logfiles, n_logfiles);

    toku_os_recursive_delete(TOKU_TEST_FILENAME);

    return 0;
}

int
test_main(int UU(argc), const char *UU(argv[])) {
    int r;
    r = run_test();
    return r;
}
