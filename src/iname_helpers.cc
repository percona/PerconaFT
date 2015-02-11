/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
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
#ident "The technology is licensed by the Massachusetts Institute of Technology, Rutgers State University of New Jersey, and the Research Foundation of State University of New York at Stony Brook under United States of America Serial No. 11/760379 and to the patents and/or patent applications resulting from it."
#ident "$Id$"

#include <ctype.h>

#include <db.h>

#include "ydb-internal.h"
#include "iname_helpers.h"

static uint64_t nontransactional_open_id = 0;

// functions used to construct inames from dnames

static void
create_iname_hint(const char *dname, char *hint) {
    //Requires: size of hint array must be > strlen(dname)
    //Copy alphanumeric characters only.
    //Replace strings of non-alphanumeric characters with a single underscore.
    bool underscored = false;
    while (*dname) {
        if (isalnum(*dname)) {
            char c = *dname++;
            *hint++ = c;
            underscored = false;
        }
        else {
            if (!underscored)
                *hint++ = '_';
            dname++;
            underscored = true;
        }
    }
    *hint = '\0';
}

// n < 0  means to ignore mark and ignore n
// n >= 0 means to include mark ("_B_" or "_P_") with hex value of n in iname
// (intended for use by loader, which will create many inames using one txnid).
static char *
construct_iname(DB_ENV *env, uint64_t id1, uint64_t id2, char *hint, const char *mark, int n) {
    int bytes;
    char inamebase[strlen(hint) +
                   8 +  // hex file format version
                   24 + // hex id (normally the txnid's parent and child)
                   8  + // hex value of n if non-neg
                   sizeof("_B___.") + // extra pieces
                   strlen(toku_product_name)];
    if (n < 0)
        bytes = snprintf(inamebase, sizeof(inamebase),
                         "%s_%" PRIx64 "_%" PRIx64 "_%" PRIx32            ".%s",
                         hint, id1, id2, FT_LAYOUT_VERSION, toku_product_name);
    else {
        invariant(strlen(mark) == 1);
        bytes = snprintf(inamebase, sizeof(inamebase),
                         "%s_%" PRIx64 "_%" PRIx64 "_%" PRIx32 "_%s_%" PRIx32 ".%s",
                         hint, id1, id2, FT_LAYOUT_VERSION, mark, n, toku_product_name);
    }
    assert(bytes>0);
    assert(bytes<=(int)sizeof(inamebase)-1);
    char *rval;
    if (env->i->data_dir)
        rval = toku_construct_full_name(2, env->i->data_dir, inamebase);
    else
        rval = toku_construct_full_name(1, inamebase);
    assert(rval);
    return rval;
}

char* create_new_iname(const char* dname, DB_ENV* env, DB_TXN* txn, const char* mark) {
    char hint[strlen(dname) + 1];
    
    // create iname and make entry in directory
    uint64_t id1 = 0;
    uint64_t id2 = 0;
    
    if (txn) {
        id1 = toku_txn_get_txnid(db_txn_struct_i(txn)->tokutxn).parent_id64;
        id2 = toku_txn_get_txnid(db_txn_struct_i(txn)->tokutxn).child_id64;
    } else {
        id1 = toku_sync_fetch_and_add(&nontransactional_open_id, 1);
    }
    create_iname_hint(dname, hint);
    return construct_iname(env, id1, id2, hint, mark, -1);  // allocated memory for iname
}


