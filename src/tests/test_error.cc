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

#include <sys/stat.h>
#include <errno.h>
#include <stdio.h>
#include <unistd.h>

char const* expect_errpfx;
int n_handle_error=0;

static void
handle_error (const DB_ENV *UU(dbenv), const char *errpfx, const char *UU(msg), bool) {
    assert(errpfx==expect_errpfx);
    n_handle_error++;
}
int
test_main (int argc, char *const argv[]) {
    parse_args(argc, argv);

    int r;
    toku_os_recursive_delete(TOKU_TEST_FILENAME);
    r=toku_os_mkdir(TOKU_TEST_FILENAME, S_IRWXU+S_IRWXG+S_IRWXO); assert(r==0);

    {
	DB_ENV *env;
	r = db_env_create(&env, 0); assert(r==0);
	env->set_errfile(env,0); // Turn off those annoying errors
	r = env->open(env, TOKU_TEST_FILENAME, (uint32_t) -1, 0644);
	CKERR2(r, EINVAL);
	assert(n_handle_error==0);
	r = env->close(env, 0); assert(r==0);
    }

    int do_errfile, do_errcall,do_errpfx;
    for (do_errpfx=0; do_errpfx<2; do_errpfx++) {
	for (do_errfile=0; do_errfile<2; do_errfile++) {
	    for (do_errcall=0; do_errcall<2; do_errcall++) {
		char errfname[TOKU_PATH_MAX+1];
                toku_path_join(errfname, 2, TOKU_TEST_FILENAME, "errfile");
		unlink(errfname);
		{
		    DB_ENV *env;
		    FILE *write_here = fopen(errfname, "w");
		    assert(write_here);
		    n_handle_error=0;
		    r = db_env_create(&env, 0); assert(r==0);
		    if (do_errpfx) {
			expect_errpfx="whoopi";
			env->set_errpfx(env, expect_errpfx);
		    } else {
			expect_errpfx=0;
		    }
		    env->set_errfile(env,0); // Turn off those annoying errors
		    if (do_errfile)
			env->set_errfile(env, write_here);
		    if (do_errcall) 
			env->set_errcall(env, handle_error);
		    r = env->open(env, TOKU_TEST_FILENAME, (uint32_t) -1, 0644);
		    assert(r==EINVAL);
		    r = env->close(env, 0); assert(r==0);
		    fclose(write_here);
		}
		{
		    FILE *read_here = fopen(errfname, "r");
		    assert(read_here);
		    char buf[10000];
		    int buflen = fread(buf, 1, sizeof(buf)-1, read_here);
		    assert(buflen>=0);
		    buf[buflen]=0;
		    if (do_errfile) {
			if (do_errpfx) {
			    assert(strncmp(buf,"whoopi:",6)==0);
			} else {
			    assert(buf[0]!=0); 
			    assert(buf[0]!=':');
			}
			assert(buf[strlen(buf)-1]=='\n');
		    } else {
			assert(buf[0]==0);
		    }
		    if (do_errcall) {
			assert(n_handle_error==1);
		    } else {
			assert(n_handle_error==0);
		    }
		    fclose(read_here);
		}
		unlink(errfname);
	    }
	}
    }
    return 0;
}
