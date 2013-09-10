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
    toku_mempool_zero(&m_buffer_mempool);
}

void bn_data::initialize_empty() {
    toku_mempool_zero(&m_buffer_mempool);
    m_buffer.create();
}

void bn_data::initialize_from_data(uint32_t num_entries, unsigned char *buf, uint32_t data_size) {
    if (data_size == 0) {
        invariant_zero(num_entries);
    }
    LEAFENTRY *XMALLOC_N(num_entries, array); // create array of pointers to leafentries
    uint32_t curr_offset = 0;
    toku_mempool_copy_construct(&m_buffer_mempool, buf, data_size);
    uint8_t *CAST_FROM_VOIDP(le_base, toku_mempool_get_base(&m_buffer_mempool)); // point to first le in mempool
    for (uint32_t i = 0; i < num_entries; i++) { // now set up the pointers in the omt
        LEAFENTRY le = reinterpret_cast<LEAFENTRY>(&le_base[curr_offset]); // point to durable mempool, not to transient rbuf
        uint32_t disksize = leafentry_disksize(le);
        curr_offset += disksize;
        invariant(curr_offset <= data_size);
        array[i] = le;
    }
    //TODO: Verify we used EXACTLY all memory provided
    
    // destroy old omt that was created by toku_create_empty_bn(), so we can create a new one
    m_buffer.destroy();
    m_buffer.create_steal_sorted_array(&array, num_entries, num_entries);
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

void bn_data::delete_leafentry (uint32_t idx, LEAFENTRY le) {
    m_buffer.delete_at(idx);
    toku_mempool_mfree(&m_buffer_mempool, 0, leafentry_memsize(le)); // Must pass 0, since le is no good any more.
}

/* mempool support */

struct omt_compressor_state {
    struct mempool *new_kvspace;
    LEAFENTRY *newvals;
};

static int move_it (const void* key UU(), const uint32_t keylen UU(), const LEAFENTRY& le, const uint32_t idx, struct omt_compressor_state * const oc) {
    uint32_t size = leafentry_memsize(le);
    LEAFENTRY CAST_FROM_VOIDP(newdata, toku_mempool_malloc(oc->new_kvspace, size, 1));
    paranoid_invariant_notnull(newdata); // we do this on a fresh mempool, so nothing bad should happen
    memcpy(newdata, le, size);
    oc->newvals[idx] = newdata;
    return 0;
}

// Compress things, and grow the mempool if needed.
void bn_data::omt_compress_kvspace(size_t added_size, void **maybe_free) {
    uint32_t total_size_needed = toku_mempool_get_used_space(&m_buffer_mempool) + added_size;
    if (total_size_needed+total_size_needed >= m_buffer_mempool.size) {
        m_buffer_mempool.size = total_size_needed+total_size_needed;
    }
    void *newmem = toku_xmalloc(m_buffer_mempool.size);
    struct mempool new_kvspace;
    toku_mempool_init(&new_kvspace, newmem, m_buffer_mempool.size);
    uint32_t numvals = omt_size();
    LEAFENTRY *XMALLOC_N(numvals, newvals);
    struct omt_compressor_state oc = { &new_kvspace, newvals };
    omt_iterate<decltype(oc), move_it>(&oc);
    m_buffer.destroy();
    m_buffer.create_steal_sorted_array(&newvals, numvals, numvals);

    if (maybe_free) {
        *maybe_free = m_buffer_mempool.base;
    } else {
        toku_free(m_buffer_mempool.base);
    }
    m_buffer_mempool = new_kvspace;
}

// Effect: Allocate a new object of size SIZE in MP.  If MP runs out of space, allocate new a new mempool space, and copy all the items
//  from the OMT (which items refer to items in the old mempool) into the new mempool.
//  If MAYBE_FREE is NULL then free the old mempool's space.
//  Otherwise, store the old mempool's space in maybe_free.
LEAFENTRY bn_data::mempool_malloc_from_omt(size_t size, void **maybe_free) {
    void *v = toku_mempool_malloc(&m_buffer_mempool, size, 1);
    if (v == NULL) {
        omt_compress_kvspace(size, maybe_free);
        v = toku_mempool_malloc(&m_buffer_mempool, size, 1);
        paranoid_invariant_notnull(v);
    }
    return (LEAFENTRY)v;
}

//TODO: probably not free the "maybe_free" right away?
void bn_data::get_space_for_overwrite(
    uint32_t idx,
    uint32_t old_le_size,
    LEAFENTRY old_le_space UU(),
    uint32_t new_size,
    LEAFENTRY* new_le_space
    )
{
    void* maybe_free = nullptr;
    *new_le_space = mempool_malloc_from_omt(
        new_size,
        &maybe_free
        );
    //TODO: See if we can improve perf? by freeing first?  But then compress kvspace recopies it!
    toku_mempool_mfree(&m_buffer_mempool, nullptr, old_le_size);  // Must pass nullptr, since le is no good any more.
    if (maybe_free) {
        toku_free(maybe_free);
    }
    m_buffer.set_at(*new_le_space, idx);
}

//TODO: probably not free the "maybe_free" right away?
void bn_data::get_space_for_insert(uint32_t idx, size_t size, LEAFENTRY* new_le_space) {
    void* maybe_free = nullptr;
    *new_le_space = mempool_malloc_from_omt(
        size,
        &maybe_free
        );
    if (maybe_free) {
        toku_free(maybe_free);
    }
    m_buffer.insert_at(*new_le_space, idx);
}

//TODO: implement the rest (e.g. above)
//#warning Yoni did only below

void bn_data::move_leafentries_to(
     BN_DATA dest_bd,
     uint32_t lbi, //lower bound inclusive
     uint32_t ube //upper bound exclusive
     )
//Effect: move leafentries in the range [lbi, ube) from this to src_omt to newly created dest_omt
{
    paranoid_invariant(lbi < ube);
    paranoid_invariant(ube <= omt_size());
    LEAFENTRY *XMALLOC_N(ube-lbi, newleafpointers);    // create new omt

    size_t mpsize = toku_mempool_get_used_space(&m_buffer_mempool);   // overkill, but safe
    struct mempool *dest_mp = &dest_bd->m_buffer_mempool;
    struct mempool *src_mp  = &m_buffer_mempool;
    toku_mempool_construct(dest_mp, mpsize);

    uint32_t i = 0;
    for (i = lbi; i < ube; i++) {
        LEAFENTRY curr_le;
        m_buffer.fetch(i, &curr_le);

        size_t le_size = leafentry_memsize(curr_le);
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

void bn_data::verify_mempool(void) {
    // TODO: implement something
}

uint32_t bn_data::omt_size(void) const {
    return m_buffer.size();
}

void bn_data::destroy(void) {
    // The buffer may have been freed already, in some cases.
    m_buffer.destroy();
    //TODO: Can we just always call mempool_destroy?
    toku_mempool_destroy(&m_buffer_mempool);
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


// get info about a single leafentry by index
int bn_data::fetch_le(uint32_t idx, LEAFENTRY *le) {
    return m_buffer.fetch(idx, le);
}

//TODO: reimplement if/when we split keys and vals
int bn_data::fetch_le_disksize(uint32_t idx, size_t *size) {
    LEAFENTRY le;
    int r = fetch_le(idx, &le);
    if (r == 0) {
        *size = leafentry_disksize(le);
    }
    return r;
}

//TODO: reimplement if/when we split keys and vals
int bn_data::fetch_le_key_and_len(uint32_t idx, uint32_t *len, void** key) {
    LEAFENTRY le;
    int r = fetch_le(idx, &le);
    if (r == 0) {
        *key = le_key_and_len(le, len);
    }
    return r;
}


struct mp_pair {
    void* orig_base;
    void* new_base;
    le_omt_t* omt;
};

static int fix_mp_offset(const void* key UU(), const uint32_t keylen UU(), const LEAFENTRY &le, const uint32_t i, struct mp_pair * const p) {
    char* old_value = (char *) le;
    char *new_value = old_value - (char *)p->orig_base + (char *)p->new_base;
    p->omt->set_at((LEAFENTRY)new_value, i);
    return 0;
}

void bn_data::clone(bn_data* orig_bn_data) {
    toku_mempool_clone(&orig_bn_data->m_buffer_mempool, &m_buffer_mempool);
    m_buffer.clone(orig_bn_data->m_buffer);
    struct mp_pair p;
    p.orig_base = toku_mempool_get_base(&orig_bn_data->m_buffer_mempool);
    p.new_base = toku_mempool_get_base(&m_buffer_mempool);
    p.omt = &m_buffer;
    // TODO: fix this to use the right API

    int r = omt_iterate<decltype(p), fix_mp_offset>(&p);
    invariant_zero(r);
}


