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

/* Dump the log from stdin to stdout. */

#include "ft/log_header.h"
#include "ft/logger/logger.h"

static void newmain (int count) {
    int i;
    uint32_t version;
    int r = toku_read_and_print_logmagic(stdin, &version);
    for (i=0; i!=count; i++) {
	r = toku_logprint_one_record(stdout, stdin);
	if (r==EOF) break;
	if (r!=0) {
	    fflush(stdout);
	    fprintf(stderr, "Problem in log err=%d\n", r);
	    exit(1);
	}
    }
}

int main (int argc, char *const argv[]) {
    int count=-1;
    while (argc>1) {
	if (strcmp(argv[1], "--oldcode")==0) {
	    fprintf(stderr,"Old code no longer works.\n");
	    exit(1);
	} else {
	    count = atoi(argv[1]);
	}
	argc--; argv++;
    }
    newmain(count);
    return 0;
}

