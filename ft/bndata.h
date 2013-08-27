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


#pragma once

#include <util/omt.h>
#include "leafentry.h"
#include <util/mempool.h>

#if 0 //for implementation
static int
UU() verify_in_mempool(OMTVALUE lev, uint32_t UU(idx), void *mpv)
{
    LEAFENTRY CAST_FROM_VOIDP(le, lev);
    struct mempool *CAST_FROM_VOIDP(mp, mpv);
    int r = toku_mempool_inrange(mp, le, leafentry_memsize(le));
    lazy_assert(r);
    return 0;
}
            toku_omt_iterate(bn->buffer, verify_in_mempool, &bn->buffer_mempool);

#endif

//TODO: #warning make sure destroy destroys the mempool (OR IS THIS BAD?) Allow manual killing of it for now
//TODO: #warning can't necessarily do this because of destroying basement nodes not doing it.. they might pass things along?
typedef toku::omt<LEAFENTRY> le_omt_t;
// This class stores the data associated with a basement node
class bn_data {
public:
    // globals
    uint64_t get_disk_size();
    void verify_mempool();

    // Info about "omt"
    uint32_t omt_size();

    // get info about a single leafentry by index
    size_t fetch_le_disksize(uint32_t idx);
    void* fetch_le_key_and_len(uint32_t idx, uint32_t *len);

    // Interact with another bn_data
    void move_leafentries_to(BN_DATA dest_bd,
                                      uint32_t lbi, //lower bound inclusive
                                      uint32_t ube, //upper bound exclusive
                                      uint32_t* num_bytes_moved);

    void destroy_mempool();
private:
    le_omt_t m_buffer;                     // pointers to individual leaf entries
    struct mempool m_buffer_mempool;  // storage for all leaf entries
    uint64_t m_n_bytes_in_buffer; // How many bytes to represent the OMT (including the per-key overheads, ...
                                    // ... but not including the overheads for the node.

#if 0 //disabled but may use
public:
    uint64_t get_memory_size();
    uint32_t get_num_entries();
    int fetch_leafentry(uint32_t idx, LEAFENTRY le);
    void delete_leafentry (uint32_t idx, LEAFENTRY le);
    void get_space_for_overwrite(uint32_t idx, uint32_t old_size, void* old_le_space, uint32_t new_size, void** new_le_space);
    void get_space_for_insert(uint32_t idx, uint32_t size, void** new_le_space);
    int find_zero(int (*h)(OMTVALUE, void*extra), void* extra, LEAFENTRY* value, uint32_t* index);
    int find(int (*h)(OMTVALUE, void*extra), void*extra, int direction, LEAFENTRY *value, uint32_t *index);
    int iterate_on_range(uint32_t left, uint32_t right, int (*f)(LEAFENTRY, uint32_t, void*), void* v);

#endif
};

