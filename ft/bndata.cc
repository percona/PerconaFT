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

#if 0
uint64_t bn_data::get_memory_size() {
    uint64_t retval = 0;
    // include fragmentation overhead but do not include space in the
    // mempool that has not yet been allocated for leaf entries
    size_t poolsize = toku_mempool_footprint(&m_buffer_mempool);
    invariant (poolsize >= m_n_bytes_in_buffer);
    retval += poolsize;
    retval += (toku_omt_memory_size(m_buffer));
    return retval;
}


uint32_t bn_data::get_num_entries() {
    return toku_omt_size(m_buffer);
}

int bn_data::fetch_leafentry(uint32_t idx, LEAFENTRY* le) {
    OMTVALUE lev;
    int r = toku_omt_fetch(m_buffer, idx, &lev);
    if (r == 0) {
        CAST_FROM_VOIDP(*le, lev);
    }
    return r;
}

void bn_data::delete_leafentry (uint32_t idx, LEAFENTRY le) {
    {
        int r = toku_omt_delete_at(m_buffer, idx);
        assert_zero(r);
    }

    m_n_bytes_in_buffer -= leafentry_disksize(le);
    toku_mempool_mfree(&m_buffer_mempool, 0, leafentry_memsize(le)); // Must pass 0, since le is no good any more.
}

void bn_data::get_space_for_overwrite(
    uint32_t idx,
    uint32_t old_size,
    LEAFENTRY old_le_space,
    uint32_t new_size,
    LEAFENTRY* new_le_space
    )
{
    if (old_size >= new_size) {
        // simple little optimization, reuse space allocated in mempool if possible,
        //
        //
        // This may not be wise, as we need to check for fragmentation.
        //
        //
        *new_le_space = old_le_space;
        toku_mempool_mfree(m_buffer_mempool, NULL, old_size - new_size);
    }
    else {
        void* maybe_free;
        *new_le_space = mempool_malloc_from_omt(
            m_buffer,
            m_buffer_mempool,
            new_size,
            &maybe_free
            );
        if (maybe_free) {
            toku_free(maybe_free);
        }
        toku_omt_set_at(m_buffer, new_le_space, idx);
    }
    m_n_bytes_in_buffer += new_size;
    m_n_bytes_in_buffer -= old_size;
}

void bn_data::get_space_for_insert(uint32_t idx, size_t size, LEAFENTRY* new_le_space) {
    void* maybe_free;
    *new_le_space = mempool_malloc_from_omt(
        m_buffer,
        m_buffer_mempool,
        size,
        &maybe_free
        );
    if (maybe_free) {
        toku_free(maybe_free);
    }
    toku_omt_insert_at(m_buffer, new_le_space, idx);
    m_n_bytes_in_buffer += size;
}

int bn_data::find_zero(
    int (*h)(OMTVALUE, void*extra),
    void* extra,
    LEAFENTRY* value,
    uint32_t* index
    )
{
    OMTVALUE storeddatav = NULL;
    int r = toku_omt_find_zero(m_buffer, h, extra, &storeddatav, index);
    if (r == DB_NOTFOUND) {
        *value = NULL;
    }
    else {
        assert_zero(r);
        CAST_FROM_VOIDP(*value, storeddatav);
    }
    return r;
}

int bn_data::find(
    int (*h)(OMTVALUE, void*extra),
    void*extra,
    int direction,
    LEAFENTRY *value,
    uint32_t *index
    )
{
    OMTVALUE storeddatav = NULL;
    int r = toku_omt_find(m_buffer, h, extra, direction, &storeddatav, index);
    if (r == DB_NOTFOUND) {
        *value = NULL;
    }
    else {
        assert_zero(r);
        CAST_FROM_VOIDP(*value, storeddatav);
    }
    return r;
}

struct iterate_on_range_extra{
    int (*f)(LEAFENTRY, uint32_t, void*);
    void* v;
};

static int bndata_iterate_callback(OMTVALUE lev, uint32_t idx, void *extra) {
    struct iterate_on_range_extra *CAST_FROM_VOIDP(e, extra);
    LEAFENRTY le = NULL;
    CAST_FROM_VOIDP(le, lev);
    return e.f(le, idx, e.v);
}

int bn_data::iterate_on_range(
    uint32_t left,
    uint32_t right,
    int (*f)(LEAFENTRY, uint32_t, void*),
    void* v
    )
{
    struct iterate_on_range_extra extra;
    extra.f = f;
    extra.v = v;
    int r = toku_omt_iterate_on_range(
            m_buffer,
            left,
            right,
            bndata_iterate_callback,
            &extra
            );
    return r;
}
#endif

#warning Yoni did only below

void bn_data::move_leafentries_to(
     BN_DATA dest_bd,
     uint32_t lbi, //lower bound inclusive
     uint32_t ube, //upper bound exclusive
     uint32_t* num_bytes_moved
     )
//Effect: move leafentries in the range [lbi, ube) from this to src_omt to newly created dest_omt
{
    paranoid_invariant(lbi < ube);
    paranoid_invariant(ube <= dest_bd->omt_size());
    LEAFENTRY *XMALLOC_N(ube-lbi, newleafpointers);    // create new omt

    size_t mpsize = toku_mempool_get_used_space(&m_buffer_mempool);   // overkill, but safe
    struct mempool *dest_mp = &dest_bd->buffer_mempool;
    struct mempool *src_mp  = &m_buffer_mempool;
    toku_mempool_construct(dest_mp, mpsize);

    uint32_t i = 0;
    *num_bytes_moved = 0;
    for (i = lbi; i < ube; i++) {
        LEAFENTRY curr_le;
        m_buffer.fetch(i, &curr_le);

        size_t le_size = leafentry_memsize(curr_le);
        *num_bytes_moved += leafentry_disksize(curr_le);
        LEAFENTRY CAST_FROM_VOIDP(new_le, toku_mempool_malloc(dest_mp, le_size, 1));
        memcpy(new_le, curr_le, le_size);
        newleafpointers[i-lbi] = new_le;
        toku_mempool_mfree(src_mp, curr_le, le_size);
    }

    dest_bd->m_buffer.create_steal_sorted_array(&newleafpointers, ube-lbi, ube-lbi);
    // now remove the elements from src_omt
    for (i=ube-1; i >= lbi; i--) {
        m_buffer.delete_at(i);
    }
}

uint64_t bn_data::get_disk_size() {
    return m_n_bytes_in_buffer;
}

void bn_data::destroy_mempool() {
    toku_mempool_destroy(&m_buffer_mempool);
}


