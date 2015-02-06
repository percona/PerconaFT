/* -*- mode: C; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#pragma once

#ident "Copyright (c) 2007-2013 Tokutek Inc.  All rights reserved."
#ident "The technology is licensed by the Massachusetts Institute of Technology, Rutgers State University of New Jersey, and the Research Foundation of State University of New York at Stony Brook under United States of America Serial No. 11/760379 and to the patents and/or patent applications resulting from it."

#include <db.h>
#include <string.h>

#include "portability/memory.h"

#include "util/dbt.h"

enum ft_flags {
    //TOKU_DB_DUP             = (1<<0),  //Obsolete #2862
    //TOKU_DB_DUPSORT         = (1<<1),  //Obsolete #2862
    TOKU_DB_KEYCMP_BUILTIN  = (1<<2),
    //TOKU_DB_VALCMP_BUILTIN_13  = (1<<3),
    TOKU_DB_CMP_NO_DESC = (1<<4), // comparison function uses no descriptor, meaning it is NOT legacy
    TOKU_DB_HAS_PREPEND_BYTES = (1<<5) // meaning FT has prepend bytes
};

typedef int (*ft_compare_func)(const DBT *a, const DBT *b);

int toku_keycompare(const void *key1, uint32_t key1len, const void *key2, uint32_t key2len);

int toku_builtin_compare_fun (const DBT *, const DBT*) __attribute__((__visibility__("default")));

namespace toku {

    // a comparator object encapsulates the data necessary for 
    // comparing two keys in a fractal tree. it further understands
    // that points may be positive or negative infinity.

    class comparator {
        void init(ft_compare_func cmp, const DESCRIPTOR_S* desc, uint32_t ft_flags, uint8_t memcmp_magic) {
            _cmp = cmp;
            _num_prepend_bytes = (ft_flags & TOKU_DB_HAS_PREPEND_BYTES) ? 8 : 0;
            toku_init_dbt(&_desc.dbt);
            if (desc && desc->dbt.data) {
                toku_clone_dbt(&_desc.dbt, desc->dbt);
            }
            _memcmp_magic = memcmp_magic;
        }

        inline bool dbt_has_memcmp_magic(const DBT *dbt) const {
            return *reinterpret_cast<const char *>(dbt->data) == _memcmp_magic;
        }


    public:
        comparator() : _cmp(nullptr), _memcmp_magic(MEMCMP_MAGIC_NONE), _num_prepend_bytes(0) {
            toku_init_dbt(&_desc.dbt);
        }
        // This magic value is reserved to mean that the magic has not been set.
        static const uint8_t MEMCMP_MAGIC_NONE = 0;

        void create(ft_compare_func cmp, DESCRIPTOR desc, uint32_t ft_flags, uint8_t memcmp_magic = MEMCMP_MAGIC_NONE) {
            init(cmp, desc, ft_flags, memcmp_magic);
        }

        // like inherit, but doesn't require that the this comparator
        // was already created
        // this will strip out the ft flags
        // Basically, this function is used in the locktree, which
        // needs to disregard the prepend bytes. Having a stripped
        // out ft flags is one way to do it, but it's subtle. Ought to
        // think of a more understandable way of doing this
        void create_from(const comparator &cmp) {
            destroy();
            invariant_notnull(cmp._cmp);
            init(cmp._cmp, &cmp._desc, 0, cmp._memcmp_magic);
        }

        void destroy() {
            if (_desc.dbt.data) {
                toku_free(_desc.dbt.data);
            }
        }

        ft_compare_func get_compare_func() const {
            return _cmp;
        }

        uint8_t get_memcmp_magic() const {
            return _memcmp_magic;
        }

        bool valid() const {
            return _cmp != nullptr;
        }

        int operator()(const DBT *a, const DBT *b) const {
            paranoid_invariant(!_num_prepend_bytes || (a->size >= _num_prepend_bytes && b->size >= _num_prepend_bytes));
            int c = (_num_prepend_bytes) ? memcmp(a->data, b->data, _num_prepend_bytes): 0; 
            if (__builtin_expect(c == 0, 1)) {
                DBT aa;
                DBT bb;                
                toku_fill_dbt(&aa, (char *)(((char *)a->data) + _num_prepend_bytes), a->size - _num_prepend_bytes);
                toku_fill_dbt(&bb, (char *)(((char *)b->data) + _num_prepend_bytes), b->size - _num_prepend_bytes);
                if (__builtin_expect(toku_dbt_is_infinite(a) || toku_dbt_is_infinite(b), 0)) {
                    return toku_dbt_infinite_compare(a, b);
                } else if (__builtin_expect(aa.size == 0 || bb.size == 0, 0)) {
                    if (aa.size > bb.size) return 1;
                    else if (aa.size < bb.size) return -1;
                    else return 0;
                } else if (false && _memcmp_magic != MEMCMP_MAGIC_NONE
                           // If `a' has the memcmp magic..
                           && dbt_has_memcmp_magic(&aa)
                           // ..then we expect `b' to also have the memcmp magic
                           && __builtin_expect(dbt_has_memcmp_magic(&bb), 1)) {
                    return toku_builtin_compare_fun(&aa, &bb);
                } else {
                    return _cmp(&aa, &bb);
                }
            }
            else {
                return c;
            }
        }

    private:
        DESCRIPTOR_S _desc;
        ft_compare_func _cmp;
        uint8_t _memcmp_magic;
        uint8_t _num_prepend_bytes;
    };

} /* namespace toku */
