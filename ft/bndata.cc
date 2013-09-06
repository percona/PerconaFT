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

#include <bndata.h>

void bn_data::init_zero() {
    m_buffer = NULL;
    toku_mempool_zero(&m_buffer_mempool);
}

void bn_data::initialize_empty() {
    toku_mempool_zero(m_buffer_mempool);
    int r = toku_omt_create(&m_buffer);
    lazy_assert_zero(r);
}

void bn_data::initialize_from_data(uint32_t num_entries, unsigned char *buf, uint32_t data_size) {
    if (data_size == 0) {
        invariant_zero(num_entries);
    }
    LEAFENTRY *XMALLOC_N(num_entries, array); // create array of pointers to leafentries
    uint32_t curr_offset = 0;
    toku_mempool_copy_construct(&m_buffer_mempool, buf, data_size);
    uint8_t *CAST_FROM_VOIDP(le_base, toku_mempool_get_base(&m_buffer_mempool));   // point to first le in mempool
    for (uint32_t i = 0; i < num_entries; i++) {                     // now set up the pointers in the omt
        LEAFENTRY le = reinterpret_cast<LEAFENTRY>(&le_base[curr_offset]); // point to durable mempool, not to transient rbuf
        uint32_t disksize = leafentry_disksize(le);
        curr_offset += disksize;
        invariant(curr_offset <= data_size);
        array[i] = le;
    }
    
    // destroy old omt that was created by toku_create_empty_bn(), so we can create a new one
    toku_omt_destroy(&m_buffer);
    m_buffer.create_steal_sorted_array(&array, num_les, num_les);
}

uint64_t bn_data::get_memory_size() {
    uint64_t retval = 0;
    // include fragmentation overhead but do not include space in the
    // mempool that has not yet been allocated for leaf entries
    size_t poolsize = toku_mempool_footprint(&m_buffer_mempool);
    //TODO: (yoni) Make sure m_n_bytes_in_buffer is consistent.. with.. what?
//    invariant (poolsize >= m_n_bytes_in_buffer);
    invariant(poolsize >= get_disk_size());
    retval += poolsize;
    retval += m_buffer.memory_size();
    return retval;
}

#if 0


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

//TODO: implement the rest (e.g. above)
//#warning Yoni did only below

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
    struct mempool *dest_mp = &dest_bd->m_buffer_mempool;
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
    //TODO: any reason why we can't just do this?  As opposed to keeping track of a number manually.
    return toku_mempool_get_used_space(&m_buffer_mempool);
}

void bn_data::destroy(void) {
    // The buffer may have been freed already, in some cases.
    if (m_buffer) {
        toku_omt_destroy(&m_buffer);
    }
    toku_mempool_destroy(&m_buffer_mempool);
}

// note, bn_data is essentially unusable after this call
void* bn_data::mempool_detach_base(void) {
    void* base = toku_mempool_get_base(&m_buffer_mempool);
    toku_mempool_zero(&m_buffer_mempool);
    return base;
}


//TODO: Splitting key/val requires changing this
void* bn_data::replace_contents_with_clone_of_sorted_array(uint32_t num_les, LEAFENTRY* old_les, size_t *le_sizes, size_t mempool_size) {
    void * retval = toku_mempool_get_base(&m_buffer_mempool);
    toku_mempool_construct(&m_buffer_mempool, mempool_size);
    LEAFENTRY *XMALLOC_N(num_les, le_array);
    for (uint32_t idx = 0; idx < num_les; idx++) {
        void *new_le = toku_mempool_malloc(&m_buffer_mempool, le_sizes[idx], 1); // point to new location
        //TODO: Splitting key/val requires changing this; memcpy may not be sufficient
        memcpy(new_le, old_les[idx], le_sizes[idx]);
        CAST_FROM_VOIDP(le_array[idx], new_le);
    }
    //TODO: Splitting key/val requires changing this; keys are stored in old omt.. cannot delete it yet?
    m_buffer.destroy();
    m_buffer.create_steal_sorted_array(&le_array, num_les, num_les);
    return retval;
}


template<typename iterate_extra_t,
         int (*f)(const LEAFENTRY &, const uint32_t, iterate_extra_t *const)>
int bn_data::omt_iterate(iterate_extra_t *const iterate_extra) const {
    return m_buffer.iterate<iterate_extra_t, f>(iterate_extra);
}

template<typename iterate_extra_t,
         int (*f)(const LEAFENTRY &, const uint32_t, iterate_extra_t *const)>
int bn_data::omt_iterate_on_range(const uint32_t left, const uint32_t right, iterate_extra_t *const iterate_extra) const {
    return m_buffer.iterate<iterate_extra_t, f>(left, right, iterate_extra);
}

template<typename omtcmp_t,
         int (*h)(const LEAFENTRY &, const omtcmp_t &)>
int bn_data::find_zero(const omtcmp_t &extra, LEAFENTRY *const value, uint32_t *const idxp) const {
    return m_buffer.find_zero<omtcmp_t, h>(extra, value, idxp);
}

template<typename omtcmp_t,
         int (*h)(const LEAFENTRY &, const omtcmp_t &)>
int bn_data::find(const omtcmp_t &extra, int direction, LEAFENTRY *const value, uint32_t *const idxp) const {
    return m_buffer.find<omtcmp_t, h>(extra, direction, value, idxp);
}

struct mp_pair {
    void* orig_base;
    void* new_base;
    OMT omt;
};

static int fix_mp_offset(OMTVALUE v, uint32_t i, void* extra) {
    struct mp_pair *CAST_FROM_VOIDP(p, extra);
    char* old_value = (char *) v;
    char *new_value = old_value - (char *)p->orig_base + (char *)p->new_base;
    toku_omt_set_at(p->omt, (OMTVALUE) new_value, i);
    return 0;
}

void bn_data::clone(bn_data* orig_bn_data) {
    toku_mempool_clone(&orig_bn_data->m_buffer_mempool, &m_buffer_mempool);
    toku_omt_clone_noptr(&m_buffer, orig_bn_data->m_buffer);
    struct mp_pair p;
    p.orig_base = toku_mempool_get_base(&orig_bn_data->m_buffer_mempool);
    p.new_base = toku_mempool_get_base(&m_buffer_mempool);
    p.omt = m_buffer;
    // TODO: fix this to use the right API
    toku_omt_iterate(
        m_buffer,
        fix_mp_offset,
        &p
        );
}


