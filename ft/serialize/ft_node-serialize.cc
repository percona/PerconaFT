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

#include "portability/toku_atomic.h"

#include "ft/cachetable/cachetable.h"
#include "ft/ft.h"
#include "ft/ft-internal.h"
#include "ft/node.h"
#include "ft/logger/log-internal.h"
#include "ft/txn/rollback.h"
#include "ft/serialize/block_allocator.h"
#include "ft/serialize/block_table.h"
#include "ft/serialize/compress.h"
#include "ft/serialize/ft_node-serialize.h"
#include "ft/serialize/sub_block.h"
#include "util/sort.h"
#include "util/threadpool.h"
#include "util/status.h"
#include "util/scoped_malloc.h"

static FT_UPGRADE_STATUS_S ft_upgrade_status;

#define STATUS_INIT(k,c,t,l,inc) TOKUFT_STATUS_INIT(ft_upgrade_status, k, c, t, "ft upgrade: " l, inc)

static void
status_init(void)
{
    // Note, this function initializes the keyname, type, and legend fields.
    // Value fields are initialized to zero by compiler.
    STATUS_INIT(FT_UPGRADE_FOOTPRINT,             nullptr, UINT64, "footprint", TOKU_ENGINE_STATUS);
    ft_upgrade_status.initialized = true;
}
#undef STATUS_INIT

#define UPGRADE_STATUS_VALUE(x) ft_upgrade_status.status[x].value.num

void
toku_ft_upgrade_get_status(FT_UPGRADE_STATUS s) {
    if (!ft_upgrade_status.initialized) {
        status_init();
    }
    UPGRADE_STATUS_VALUE(FT_UPGRADE_FOOTPRINT) = toku_log_upgrade_get_footprint();
    *s = ft_upgrade_status;
}

static int num_cores = 0; // cache the number of cores for the parallelization
static struct toku_thread_pool *ft_pool = NULL;
bool toku_serialize_in_parallel;

int get_num_cores(void) {
    return num_cores;
}

struct toku_thread_pool *get_ft_pool(void) {
    return ft_pool;
}

void toku_serialize_set_parallel(bool in_parallel) {
    toku_drd_unsafe_set(&toku_serialize_in_parallel, in_parallel);
}

void toku_ft_serialize_layer_init(void) {
    num_cores = toku_os_get_number_active_processors();
    int r = toku_thread_pool_create(&ft_pool, num_cores);
    lazy_assert_zero(r);
    block_allocator::maybe_initialize_trace();
    toku_serialize_in_parallel = false;
}

void toku_ft_serialize_layer_destroy(void) {
    toku_thread_pool_destroy(&ft_pool);
    block_allocator::maybe_close_trace();
}

enum { FILE_CHANGE_INCREMENT = (16 << 20) };

static inline uint64_t 
alignup64(uint64_t a, uint64_t b) {
    return ((a+b-1)/b)*b;
}

// safe_file_size_lock must be held.
void
toku_maybe_truncate_file (int fd, uint64_t size_used, uint64_t expected_size, uint64_t *new_sizep)
// Effect: If file size >= SIZE+32MiB, reduce file size.
// (32 instead of 16.. hysteresis).
// Return 0 on success, otherwise an error number.
{
    int64_t file_size;
    {
        int r = toku_os_get_file_size(fd, &file_size);
        lazy_assert_zero(r);
        invariant(file_size >= 0);
    }
    invariant(expected_size == (uint64_t)file_size);
    // If file space is overallocated by at least 32M
    if ((uint64_t)file_size >= size_used + (2*FILE_CHANGE_INCREMENT)) {
        toku_off_t new_size = alignup64(size_used, (2*FILE_CHANGE_INCREMENT)); //Truncate to new size_used.
        invariant(new_size < file_size);
        invariant(new_size >= 0);
        int r = ftruncate(fd, new_size);
        lazy_assert_zero(r);
        *new_sizep = new_size;
    }
    else {
        *new_sizep = file_size;
    }
    return;
}

static int64_t 
min64(int64_t a, int64_t b) {
    if (a<b) return a;
    return b;
}

void
toku_maybe_preallocate_in_file (int fd, int64_t size, int64_t expected_size, int64_t *new_size)
// Effect: make the file bigger by either doubling it or growing by 16MiB whichever is less, until it is at least size
// Return 0 on success, otherwise an error number.
{
    int64_t file_size = 0;
    //TODO(yoni): Allow variable stripe_width (perhaps from ft) for larger raids
    const uint64_t stripe_width = 4096;
    {
        int r = toku_os_get_file_size(fd, &file_size);
        if (r != 0) { // debug #2463
            int the_errno = get_maybe_error_errno();
            fprintf(stderr, "%s:%d fd=%d size=%" PRIu64 " r=%d errno=%d\n", __FUNCTION__, __LINE__, fd, size, r, the_errno); fflush(stderr);
        }
        lazy_assert_zero(r);
    }
    invariant(file_size >= 0);
    invariant(expected_size == file_size);
    // We want to double the size of the file, or add 16MiB, whichever is less.
    // We emulate calling this function repeatedly until it satisfies the request.
    int64_t to_write = 0;
    if (file_size == 0) {
        // Prevent infinite loop by starting with stripe_width as a base case.
        to_write = stripe_width;
    }
    while (file_size + to_write < size) {
        to_write += alignup64(min64(file_size + to_write, FILE_CHANGE_INCREMENT), stripe_width);
    }
    if (to_write > 0) {
        assert(to_write%512==0);
        toku::scoped_malloc_aligned wbuf_aligned(to_write, 512);
        char *wbuf = reinterpret_cast<char *>(wbuf_aligned.get());
        memset(wbuf, 0, to_write);
        toku_off_t start_write = alignup64(file_size, stripe_width);
        invariant(start_write >= file_size);
        toku_os_full_pwrite(fd, wbuf, to_write, start_write);
        *new_size = start_write + to_write;
    }
    else {
        *new_size = file_size;
    }
}

// Don't include the sub_block header
// Overhead calculated in same order fields are written to wbuf
enum {
    node_header_overhead = (8+   // magic "tokunode" or "tokuleaf" or "tokuroll"
                            4+   // layout_version
                            4+   // layout_version_original
                            4),  // build_id
};

// uncompressed header offsets
enum {
    uncompressed_magic_offset = 0,
    uncompressed_version_offset = 8,
};

static uint32_t
serialize_node_header_size(FTNODE node) {
    uint32_t retval = 0;
    retval += 8; // magic
    retval += sizeof(node->layout_version);
    retval += sizeof(node->layout_version_original);
    retval += 4; // BUILD_ID
    retval += 4; // n_children
    retval += node->n_children*8; // encode start offset and length of each partition
    retval += 4; // checksum
    return retval;
}

static void
serialize_node_header(FTNODE node, FTNODE_DISK_DATA ndd, struct wbuf *wbuf) {
    if (node->height == 0) 
        wbuf_nocrc_literal_bytes(wbuf, "tokuleaf", 8);
    else 
        wbuf_nocrc_literal_bytes(wbuf, "tokunode", 8);
    paranoid_invariant(node->layout_version == FT_LAYOUT_VERSION);
    wbuf_nocrc_int(wbuf, node->layout_version);
    wbuf_nocrc_int(wbuf, node->layout_version_original);
    wbuf_nocrc_uint(wbuf, BUILD_ID);
    wbuf_nocrc_int (wbuf, node->n_children);
    for (int i=0; i<node->n_children; i++) {
        assert(BP_SIZE(ndd,i)>0);
        wbuf_nocrc_int(wbuf, BP_START(ndd, i)); // save the beginning of the partition
        wbuf_nocrc_int(wbuf, BP_SIZE (ndd, i));         // and the size
    }
    // checksum the header
    uint32_t end_to_end_checksum = toku_x1764_memory(wbuf->buf, wbuf_get_woffset(wbuf));
    wbuf_nocrc_int(wbuf, end_to_end_checksum);
    invariant(wbuf->ndone == wbuf->size);
}

static uint32_t
serialize_ftnode_partition_size (FTNODE node, int i)
{
    uint32_t result = 0;
    paranoid_invariant(node->bp[i].state == PT_AVAIL);
    result++; // Byte that states what the partition is
    if (node->height > 0) {
        NONLEAF_CHILDINFO bnc = BNC(node, i);
        // number of messages (4 bytes) plus size of the buffer
        result += (4 + toku_bnc_nbytesinbuf(bnc));
        // number of offsets (4 bytes) plus an array of 4 byte offsets, for each message tree
        result += (4 + (4 * bnc->fresh_message_tree.size()));
        result += (4 + (4 * bnc->stale_message_tree.size()));
        result += (4 + (4 * bnc->broadcast_list.size()));
    }
    else {
        result += 4 + bn_data::HEADER_LENGTH; // n_entries in buffer table + basement header
        result += BLB_NBYTESINDATA(node, i);
    }
    result += 4; // checksum
    return result;
}

#define FTNODE_PARTITION_DMT_LEAVES 0xaa
#define FTNODE_PARTITION_MSG_BUFFER 0xbb

UU() static int
assert_fresh(const int32_t &offset, const uint32_t UU(idx), message_buffer *const msg_buffer) {
    bool is_fresh = msg_buffer->get_freshness(offset);
    assert(is_fresh);
    return 0;
}

UU() static int
assert_stale(const int32_t &offset, const uint32_t UU(idx), message_buffer *const msg_buffer) {
    bool is_fresh = msg_buffer->get_freshness(offset);
    assert(!is_fresh);
    return 0;
}

static void bnc_verify_message_trees(NONLEAF_CHILDINFO UU(bnc)) {
#ifdef TOKU_DEBUG_PARANOID
    bnc->fresh_message_tree.iterate<message_buffer, assert_fresh>(&bnc->msg_buffer);
    bnc->stale_message_tree.iterate<message_buffer, assert_stale>(&bnc->msg_buffer);
#endif
}

static int
wbuf_write_offset(const int32_t &offset, const uint32_t UU(idx), struct wbuf *const wb) {
    wbuf_nocrc_int(wb, offset);
    return 0;
}

static void serialize_child_buffer(NONLEAF_CHILDINFO bnc, struct wbuf *wb) {
    unsigned char ch = FTNODE_PARTITION_MSG_BUFFER;
    wbuf_nocrc_char(wb, ch);

    // serialize the message buffer
    bnc->msg_buffer.serialize_to_wbuf(wb);

    // serialize the message trees (num entries, offsets array):
    // first, verify their contents are consistent with the message buffer
    bnc_verify_message_trees(bnc);

    // fresh
    wbuf_nocrc_int(wb, bnc->fresh_message_tree.size());
    bnc->fresh_message_tree.iterate<struct wbuf, wbuf_write_offset>(wb);

    // stale
    wbuf_nocrc_int(wb, bnc->stale_message_tree.size());
    bnc->stale_message_tree.iterate<struct wbuf, wbuf_write_offset>(wb);

    // broadcast
    wbuf_nocrc_int(wb, bnc->broadcast_list.size());
    bnc->broadcast_list.iterate<struct wbuf, wbuf_write_offset>(wb);
}

//
// Serialize the i'th partition of node into sb
// For leaf nodes, this would be the i'th basement node
// For internal nodes, this would be the i'th internal node
//
static void
serialize_ftnode_partition(FTNODE node, int i, struct sub_block *sb) {
    // Caller should have allocated memory.
    invariant_notnull(sb->uncompressed_ptr);
    invariant(sb->uncompressed_size > 0);
    paranoid_invariant(sb->uncompressed_size == serialize_ftnode_partition_size(node, i));

    //
    // Now put the data into sb->uncompressed_ptr
    //
    struct wbuf wb;
    wbuf_init(&wb, sb->uncompressed_ptr, sb->uncompressed_size);
    if (node->height > 0) {
        // TODO: (Zardosht) possibly exit early if there are no messages
        serialize_child_buffer(BNC(node, i), &wb);
    }
    else {
        unsigned char ch = FTNODE_PARTITION_DMT_LEAVES;
        bn_data* bd = BLB_DATA(node, i);

        wbuf_nocrc_char(&wb, ch);
        wbuf_nocrc_uint(&wb, bd->num_klpairs());

        bd->serialize_to_wbuf(&wb);
    }
    uint32_t end_to_end_checksum = toku_x1764_memory(sb->uncompressed_ptr, wbuf_get_woffset(&wb));
    wbuf_nocrc_int(&wb, end_to_end_checksum);
    invariant(wb.ndone == wb.size);
    invariant(sb->uncompressed_size==wb.ndone);
}

//
// Takes the data in sb->uncompressed_ptr, and compresses it 
// into a newly allocated buffer sb->compressed_ptr
// 
static void
compress_ftnode_sub_block(struct sub_block *sb, enum toku_compression_method method) {
    invariant(sb->compressed_ptr != nullptr);
    invariant(sb->compressed_size_bound > 0);
    paranoid_invariant(sb->compressed_size_bound == toku_compress_bound(method, sb->uncompressed_size));
    
    //
    // This probably seems a bit complicated. Here is what is going on.
    // In TokuFT 5.0, sub_blocks were compressed and the compressed data
    // was checksummed. The checksum did NOT include the size of the compressed data
    // and the size of the uncompressed data. The fields of sub_block only reference the
    // compressed data, and it is the responsibility of the user of the sub_block
    // to write the length
    //
    // For Dr. No, we want the checksum to also include the size of the compressed data, and the 
    // size of the decompressed data, because this data
    // may be read off of disk alone, so it must be verifiable alone.
    //
    // So, we pass in a buffer to compress_nocrc_sub_block that starts 8 bytes after the beginning
    // of sb->compressed_ptr, so we have space to put in the sizes, and then run the checksum.
    //
    sb->compressed_size = compress_nocrc_sub_block(
        sb,
        (char *)sb->compressed_ptr + 8,
        sb->compressed_size_bound,
        method
        );

    uint32_t* extra = (uint32_t *)(sb->compressed_ptr);
    // store the compressed and uncompressed size at the beginning
    extra[0] = toku_htod32(sb->compressed_size);
    extra[1] = toku_htod32(sb->uncompressed_size);
    // now checksum the entire thing
    sb->compressed_size += 8; // now add the eight bytes that we saved for the sizes
    sb->xsum = toku_x1764_memory(sb->compressed_ptr,sb->compressed_size);

    //
    // This is the end result for Dr. No and forward. For ftnodes, sb->compressed_ptr contains
    // two integers at the beginning, the size and uncompressed size, and then the compressed
    // data. sb->xsum contains the checksum of this entire thing.
    // 
    // In TokuFT 5.0, sb->compressed_ptr only contained the compressed data, sb->xsum
    // checksummed only the compressed data, and the checksumming of the sizes were not
    // done here.
    //
}

//
// Returns the size needed to serialize the ftnode info
// Does not include header information that is common with rollback logs
// such as the magic, layout_version, and build_id
// Includes only node specific info such as pivot information, n_children, and so on
//
static uint32_t
serialize_ftnode_info_size(FTNODE node)
{
    uint32_t retval = 0;
    retval += 8; // max_msn_applied_to_node_on_disk
    retval += 4; // nodesize
    retval += 4; // flags
    retval += 4; // height;
    retval += 8; // oldest_referenced_xid_known
    retval += node->pivotkeys.serialized_size();
    retval += (node->n_children-1)*4; // encode length of each pivot
    if (node->height > 0) {
        retval += node->n_children*8; // child blocknum's
    }
    retval += 4; // checksum
    return retval;
}

static void serialize_ftnode_info(FTNODE node, SUB_BLOCK sb) {
    // Memory must have been allocated by our caller.
    invariant(sb->uncompressed_size > 0);
    invariant_notnull(sb->uncompressed_ptr);
    paranoid_invariant(sb->uncompressed_size == serialize_ftnode_info_size(node));

    struct wbuf wb;
    wbuf_init(&wb, sb->uncompressed_ptr, sb->uncompressed_size);

    wbuf_MSN(&wb, node->max_msn_applied_to_node_on_disk);
    wbuf_nocrc_uint(&wb, 0); // write a dummy value for where node->nodesize used to be
    wbuf_nocrc_uint(&wb, node->flags);
    wbuf_nocrc_int (&wb, node->height);    
    wbuf_TXNID(&wb, node->oldest_referenced_xid_known);
    node->pivotkeys.serialize_to_wbuf(&wb);

    // child blocks, only for internal nodes
    if (node->height > 0) {
        for (int i = 0; i < node->n_children; i++) {
            wbuf_nocrc_BLOCKNUM(&wb, BP_BLOCKNUM(node,i));
        }
    }

    uint32_t end_to_end_checksum = toku_x1764_memory(sb->uncompressed_ptr, wbuf_get_woffset(&wb));
    wbuf_nocrc_int(&wb, end_to_end_checksum);
    invariant(wb.ndone == wb.size);
    invariant(sb->uncompressed_size==wb.ndone);
}

// This is the size of the uncompressed data, not including the compression headers
unsigned int
toku_serialize_ftnode_size (FTNODE node) {
    unsigned int result = 0;
    //
    // As of now, this seems to be called if and only if the entire node is supposed
    // to be in memory, so we will assert it.
    //
    toku_ftnode_assert_fully_in_memory(node);
    result += serialize_node_header_size(node);
    result += serialize_ftnode_info_size(node);
    for (int i = 0; i < node->n_children; i++) {
        result += serialize_ftnode_partition_size(node,i);
    }
    return result;
}

struct serialize_times {
    tokutime_t serialize_time;
    tokutime_t compress_time;
};

static void
serialize_and_compress_partition(FTNODE node,
                                 int childnum,
                                 enum toku_compression_method compression_method,
                                 SUB_BLOCK sb,
                                 struct serialize_times *st)
{
    // serialize, compress, update status
    tokutime_t t0 = toku_time_now();
    serialize_ftnode_partition(node, childnum, sb);
    tokutime_t t1 = toku_time_now();
    compress_ftnode_sub_block(sb, compression_method);
    tokutime_t t2 = toku_time_now();

    st->serialize_time += t1 - t0;
    st->compress_time += t2 - t1;
}

void
toku_create_compressed_partition_from_available(
    FTNODE node,
    int childnum,
    enum toku_compression_method compression_method,
    SUB_BLOCK sb
    )
{
    tokutime_t t0 = toku_time_now();

    // serialize
    sb->uncompressed_size = serialize_ftnode_partition_size(node, childnum);
    toku::scoped_malloc uncompressed_buf(sb->uncompressed_size);
    sb->uncompressed_ptr = uncompressed_buf.get();
    serialize_ftnode_partition(node, childnum, sb);

    tokutime_t t1 = toku_time_now();

    // compress. no need to pad with extra bytes for sizes/xsum - we're not storing them
    set_compressed_size_bound(sb, compression_method);
    sb->compressed_ptr = toku_xmalloc(sb->compressed_size_bound);
    sb->compressed_size = compress_nocrc_sub_block(
        sb,
        sb->compressed_ptr,
        sb->compressed_size_bound,
        compression_method
        );
    sb->uncompressed_ptr = NULL;

    tokutime_t t2 = toku_time_now();

    toku_ft_status_update_serialize_times(node, t1 - t0, t2 - t1);
}

static void
serialize_and_compress_serially(FTNODE node,
                                int npartitions,
                                enum toku_compression_method compression_method,
                                struct sub_block sb[],
                                struct serialize_times *st) {
    for (int i = 0; i < npartitions; i++) {
        serialize_and_compress_partition(node, i, compression_method, &sb[i], st);
    }
}

struct serialize_compress_work {
    struct work base;
    FTNODE node;
    int i;
    enum toku_compression_method compression_method;
    struct sub_block *sb;
    struct serialize_times st;
};

static void *
serialize_and_compress_worker(void *arg) {
    struct workset *ws = (struct workset *) arg;
    while (1) {
        struct serialize_compress_work *w = (struct serialize_compress_work *) workset_get(ws);
        if (w == NULL)
            break;
        int i = w->i;
        serialize_and_compress_partition(w->node, i, w->compression_method, &w->sb[i], &w->st);
    }
    workset_release_ref(ws);
    return arg;
}

static void
serialize_and_compress_in_parallel(FTNODE node,
                                   int npartitions,
                                   enum toku_compression_method compression_method,
                                   struct sub_block sb[],
                                   struct serialize_times *st) {
    if (npartitions == 1) {
        serialize_and_compress_partition(node, 0, compression_method, &sb[0], st);
    } else {
        int T = num_cores;
        if (T > npartitions)
            T = npartitions;
        if (T > 0)
            T = T - 1;
        struct workset ws;
        ZERO_STRUCT(ws);
        workset_init(&ws);
        struct serialize_compress_work work[npartitions];
        workset_lock(&ws);
        for (int i = 0; i < npartitions; i++) {
            work[i] = (struct serialize_compress_work) { .base = {{NULL}},
                                                         .node = node,
                                                         .i = i,
                                                         .compression_method = compression_method,
                                                         .sb = sb,
                                                         .st = { .serialize_time = 0, .compress_time = 0} };
            workset_put_locked(&ws, &work[i].base);
        }
        workset_unlock(&ws);
        toku_thread_pool_run(ft_pool, 0, &T, serialize_and_compress_worker, &ws);
        workset_add_ref(&ws, T);
        serialize_and_compress_worker(&ws);
        workset_join(&ws);
        workset_destroy(&ws);

        // gather up the statistics from each thread's work item
        for (int i = 0; i < npartitions; i++) {
            st->serialize_time += work[i].st.serialize_time;
            st->compress_time += work[i].st.compress_time;
        }
    }
}

static void
serialize_and_compress_sb_node_info(FTNODE node, struct sub_block *sb,
        enum toku_compression_method compression_method, struct serialize_times *st) {
    // serialize, compress, update serialize times.
    tokutime_t t0 = toku_time_now();
    serialize_ftnode_info(node, sb);
    tokutime_t t1 = toku_time_now();
    compress_ftnode_sub_block(sb, compression_method);
    tokutime_t t2 = toku_time_now();

    st->serialize_time += t1 - t0;
    st->compress_time += t2 - t1;
}

int toku_serialize_ftnode_to_memory(FTNODE node,
                                    FTNODE_DISK_DATA* ndd,
                                    unsigned int basementnodesize,
                                    enum toku_compression_method compression_method,
                                    bool do_rebalancing,
                                    bool in_parallel, // for loader is true, for toku_ftnode_flush_callback, is false
                            /*out*/ size_t *n_bytes_to_write,
                            /*out*/ size_t *n_uncompressed_bytes,
                            /*out*/ char  **bytes_to_write)
// Effect: Writes out each child to a separate malloc'd buffer, then compresses
//   all of them, and writes the uncompressed header, to bytes_to_write,
//   which is malloc'd.
//
//   The resulting buffer is guaranteed to be 512-byte aligned and the total length is a multiple of 512 (so we pad with zeros at the end if needed).
//   512-byte padding is for O_DIRECT to work.
{
    toku_ftnode_assert_fully_in_memory(node);

    if (do_rebalancing && node->height == 0) {
        toku_ftnode_leaf_rebalance(node, basementnodesize);
    }
    const int npartitions = node->n_children;

    // Each partition represents a compressed sub block
    // For internal nodes, a sub block is a message buffer
    // For leaf nodes, a sub block is a basement node
    toku::scoped_calloc sb_buf(sizeof(struct sub_block) * npartitions);
    struct sub_block *sb = reinterpret_cast<struct sub_block *>(sb_buf.get());
    XREALLOC_N(npartitions, *ndd);

    //
    // First, let's serialize and compress the individual sub blocks
    //

    // determine how large our serialization and compression buffers need to be.
    size_t serialize_buf_size = 0, compression_buf_size = 0;
    for (int i = 0; i < node->n_children; i++) {
        sb[i].uncompressed_size = serialize_ftnode_partition_size(node, i);
        sb[i].compressed_size_bound = toku_compress_bound(compression_method, sb[i].uncompressed_size);
        serialize_buf_size += sb[i].uncompressed_size;
        compression_buf_size += sb[i].compressed_size_bound + 8; // add 8 extra bytes, 4 for compressed size, 4 for decompressed size
    }

    // give each sub block a base pointer to enough buffer space for serialization and compression
    toku::scoped_malloc serialize_buf(serialize_buf_size);
    toku::scoped_malloc compression_buf(compression_buf_size);
    for (size_t i = 0, uncompressed_offset = 0, compressed_offset = 0; i < (size_t) node->n_children; i++) {
        sb[i].uncompressed_ptr = reinterpret_cast<char *>(serialize_buf.get()) + uncompressed_offset;
        sb[i].compressed_ptr = reinterpret_cast<char *>(compression_buf.get()) + compressed_offset;
        uncompressed_offset += sb[i].uncompressed_size;
        compressed_offset += sb[i].compressed_size_bound + 8; // add 8 extra bytes, 4 for compressed size, 4 for decompressed size
        invariant(uncompressed_offset <= serialize_buf_size);
        invariant(compressed_offset <= compression_buf_size);
    }

    // do the actual serialization now that we have buffer space
    struct serialize_times st = { 0, 0 };
    if (in_parallel) {
        serialize_and_compress_in_parallel(node, npartitions, compression_method, sb, &st);
    } else {
        serialize_and_compress_serially(node, npartitions, compression_method, sb, &st);
    }

    //
    // Now lets create a sub-block that has the common node information,
    // This does NOT include the header
    //

    // determine how large our serialization and copmression buffers need to be
    struct sub_block sb_node_info;
    sub_block_init(&sb_node_info);
    size_t sb_node_info_uncompressed_size = serialize_ftnode_info_size(node);
    size_t sb_node_info_compressed_size_bound = toku_compress_bound(compression_method, sb_node_info_uncompressed_size);
    toku::scoped_malloc sb_node_info_uncompressed_buf(sb_node_info_uncompressed_size);
    toku::scoped_malloc sb_node_info_compressed_buf(sb_node_info_compressed_size_bound + 8); // add 8 extra bytes, 4 for compressed size, 4 for decompressed size
    sb_node_info.uncompressed_size = sb_node_info_uncompressed_size;
    sb_node_info.uncompressed_ptr = sb_node_info_uncompressed_buf.get();
    sb_node_info.compressed_size_bound = sb_node_info_compressed_size_bound;
    sb_node_info.compressed_ptr = sb_node_info_compressed_buf.get();

    // do the actual serialization now that we have buffer space
    serialize_and_compress_sb_node_info(node, &sb_node_info, compression_method, &st);

    //
    // At this point, we have compressed each of our pieces into individual sub_blocks,
    // we can put the header and all the subblocks into a single buffer and return it.
    //

    // update the serialize times, ignore the header for simplicity. we captured all
    // of the partitions' serialize times so that's probably good enough.
    toku_ft_status_update_serialize_times(node, st.serialize_time, st.compress_time);

    // The total size of the node is:
    // size of header + disk size of the n+1 sub_block's created above
    uint32_t total_node_size = (serialize_node_header_size(node) // uncompressed header
                                 + sb_node_info.compressed_size   // compressed nodeinfo (without its checksum)
                                 + 4);                            // nodeinfo's checksum
    uint32_t total_uncompressed_size = (serialize_node_header_size(node) // uncompressed header
                                 + sb_node_info.uncompressed_size   // uncompressed nodeinfo (without its checksum)
                                 + 4);                            // nodeinfo's checksum
    // store the BP_SIZESs
    for (int i = 0; i < node->n_children; i++) {
        uint32_t len         = sb[i].compressed_size + 4; // data and checksum
        BP_SIZE (*ndd,i) = len;
        BP_START(*ndd,i) = total_node_size;
        total_node_size += sb[i].compressed_size + 4;
        total_uncompressed_size += sb[i].uncompressed_size + 4;
    }

    // now create the final serialized node
    uint32_t total_buffer_size = roundup_to_multiple(512, total_node_size); // make the buffer be 512 bytes.
    char *XMALLOC_N_ALIGNED(512, total_buffer_size, data);
    char *curr_ptr = data;

    // write the header
    struct wbuf wb;
    wbuf_init(&wb, curr_ptr, serialize_node_header_size(node));
    serialize_node_header(node, *ndd, &wb);
    assert(wb.ndone == wb.size);
    curr_ptr += serialize_node_header_size(node);

    // now write sb_node_info
    memcpy(curr_ptr, sb_node_info.compressed_ptr, sb_node_info.compressed_size);
    curr_ptr += sb_node_info.compressed_size;
    // write the checksum
    *(uint32_t *)curr_ptr = toku_htod32(sb_node_info.xsum);
    curr_ptr += sizeof(sb_node_info.xsum);

    for (int i = 0; i < npartitions; i++) {
        memcpy(curr_ptr, sb[i].compressed_ptr, sb[i].compressed_size);
        curr_ptr += sb[i].compressed_size;
        // write the checksum
        *(uint32_t *)curr_ptr = toku_htod32(sb[i].xsum);
        curr_ptr += sizeof(sb[i].xsum);
    }
    // Zero the rest of the buffer
    memset(data + total_node_size, 0, total_buffer_size - total_node_size);
            
    assert(curr_ptr - data == total_node_size);
    *bytes_to_write = data;
    *n_bytes_to_write = total_buffer_size;
    *n_uncompressed_bytes = total_uncompressed_size;

    invariant(*n_bytes_to_write % 512 == 0);
    invariant(reinterpret_cast<unsigned long long>(*bytes_to_write) % 512 == 0);
    return 0;
}

int
toku_serialize_ftnode_to (int fd, BLOCKNUM blocknum, FTNODE node, FTNODE_DISK_DATA* ndd, bool do_rebalancing, FT ft, bool for_checkpoint) {

    size_t n_to_write;
    size_t n_uncompressed_bytes;
    char *compressed_buf = nullptr;

    // because toku_serialize_ftnode_to is only called for 
    // in toku_ftnode_flush_callback, we pass false
    // for in_parallel. The reasoning is that when we write
    // nodes to disk via toku_ftnode_flush_callback, we 
    // assume that it is being done on a non-critical
    // background thread (probably for checkpointing), and therefore 
    // should not hog CPU,
    //
    // Should the above facts change, we may want to revisit
    // passing false for in_parallel here
    //
    // alternatively, we could have made in_parallel a parameter
    // for toku_serialize_ftnode_to, but instead we did this.
    int r = toku_serialize_ftnode_to_memory(
        node,
        ndd,
        ft->h->basementnodesize,
        ft->h->compression_method,
        do_rebalancing,
        toku_drd_unsafe_fetch(&toku_serialize_in_parallel),
        &n_to_write,
        &n_uncompressed_bytes,
        &compressed_buf
        );
    if (r != 0) {
        return r;
    }

    // If the node has never been written, then write the whole buffer, including the zeros
    invariant(blocknum.b>=0);
    DISKOFF offset;

    // Dirties the ft
    ft->blocktable.realloc_on_disk(blocknum, n_to_write, &offset,
                                   ft, fd, for_checkpoint,
                                   // Allocations for nodes high in the tree are considered 'hot',
                                   // as they are likely to move again in the next checkpoint.
                                   node->height);

    tokutime_t t0 = toku_time_now();
    toku_os_full_pwrite(fd, compressed_buf, n_to_write, offset);
    tokutime_t t1 = toku_time_now();

    tokutime_t io_time = t1 - t0;
    toku_ft_status_update_flush_reason(node, n_uncompressed_bytes, n_to_write, io_time, for_checkpoint);

    toku_free(compressed_buf);
    node->dirty = 0;  // See #1957.   Must set the node to be clean after serializing it so that it doesn't get written again on the next checkpoint or eviction.
    return 0;
}

static void
deserialize_child_buffer(NONLEAF_CHILDINFO bnc, struct rbuf *rb) {
    // read in the message buffer
    bnc->msg_buffer.deserialize_from_rbuf(rb,
                                          nullptr, nullptr,  // fresh_offsets, nfresh,
                                          nullptr, nullptr,  // stale_offsets, nstale,
                                          nullptr, nullptr); // broadcast_offsets, nbroadcast

    // read in each message tree (fresh, stale, broadcast)
    int32_t nfresh = rbuf_int(rb);
    int32_t *XMALLOC_N(nfresh, fresh_offsets);
    for (int i = 0; i < nfresh; i++) {
        fresh_offsets[i] = rbuf_int(rb);
    }

    int32_t nstale = rbuf_int(rb);
    int32_t *XMALLOC_N(nstale, stale_offsets);
    for (int i = 0; i < nstale; i++) {
        stale_offsets[i] = rbuf_int(rb);
    }

    int32_t nbroadcast = rbuf_int(rb);
    int32_t *XMALLOC_N(nbroadcast, broadcast_offsets);
    for (int i = 0; i < nbroadcast; i++) {
        broadcast_offsets[i] = rbuf_int(rb);
    }

    // build OMTs out of each offset array
    bnc->fresh_message_tree.destroy();
    bnc->fresh_message_tree.create_steal_sorted_array(&fresh_offsets, nfresh, nfresh);
    bnc->stale_message_tree.destroy();
    bnc->stale_message_tree.create_steal_sorted_array(&stale_offsets, nstale, nstale);
    bnc->broadcast_list.destroy();
    bnc->broadcast_list.create_steal_sorted_array(&broadcast_offsets, nbroadcast, nbroadcast);
}

// dump a buffer to stderr
// no locking around this for now
void
dump_bad_block(unsigned char *vp, uint64_t size) {
    const uint64_t linesize = 64;
    uint64_t n = size / linesize;
    for (uint64_t i = 0; i < n; i++) {
        fprintf(stderr, "%p: ", vp);
        for (uint64_t j = 0; j < linesize; j++) {
            unsigned char c = vp[j];
            fprintf(stderr, "%2.2X", c);
        }
        fprintf(stderr, "\n");
        vp += linesize;
    }
    size = size % linesize;
    for (uint64_t i=0; i<size; i++) {
        if ((i % linesize) == 0)
            fprintf(stderr, "%p: ", vp+i);
        fprintf(stderr, "%2.2X", vp[i]);
        if (((i+1) % linesize) == 0)
            fprintf(stderr, "\n");
    }
    fprintf(stderr, "\n");
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

BASEMENTNODE toku_create_empty_bn(void) {
    BASEMENTNODE bn = toku_create_empty_bn_no_buffer();
    bn->data_buffer.initialize_empty();
    return bn;
}

BASEMENTNODE toku_clone_bn(BASEMENTNODE orig_bn) {
    BASEMENTNODE bn = toku_create_empty_bn_no_buffer();
    bn->max_msn_applied = orig_bn->max_msn_applied;
    bn->seqinsert = orig_bn->seqinsert;
    bn->stale_ancestor_messages_applied = orig_bn->stale_ancestor_messages_applied;
    bn->stat64_delta = orig_bn->stat64_delta;
    bn->data_buffer.clone(&orig_bn->data_buffer);
    return bn;
}

BASEMENTNODE toku_create_empty_bn_no_buffer(void) {
    BASEMENTNODE XMALLOC(bn);
    bn->max_msn_applied.msn = 0;
    bn->seqinsert = 0;
    bn->stale_ancestor_messages_applied = false;
    bn->stat64_delta = ZEROSTATS;
    bn->data_buffer.init_zero();
    return bn;
}

NONLEAF_CHILDINFO toku_create_empty_nl(void) {
    NONLEAF_CHILDINFO XMALLOC(cn);
    cn->msg_buffer.create();
    cn->fresh_message_tree.create_no_array();
    cn->stale_message_tree.create_no_array();
    cn->broadcast_list.create_no_array();
    memset(cn->flow, 0, sizeof cn->flow);
    return cn;
}

// must clone the OMTs, since we serialize them along with the message buffer
NONLEAF_CHILDINFO toku_clone_nl(NONLEAF_CHILDINFO orig_childinfo) {
    NONLEAF_CHILDINFO XMALLOC(cn);
    cn->msg_buffer.clone(&orig_childinfo->msg_buffer);
    cn->fresh_message_tree.create_no_array();
    cn->fresh_message_tree.clone(orig_childinfo->fresh_message_tree);
    cn->stale_message_tree.create_no_array();
    cn->stale_message_tree.clone(orig_childinfo->stale_message_tree);
    cn->broadcast_list.create_no_array();
    cn->broadcast_list.clone(orig_childinfo->broadcast_list);
    memset(cn->flow, 0, sizeof cn->flow);
    return cn;
}

void destroy_basement_node (BASEMENTNODE bn)
{
    bn->data_buffer.destroy();
    toku_free(bn);
}

void destroy_nonleaf_childinfo (NONLEAF_CHILDINFO nl)
{
    nl->msg_buffer.destroy();
    nl->fresh_message_tree.destroy();
    nl->stale_message_tree.destroy();
    nl->broadcast_list.destroy();
    toku_free(nl);
}

void read_block_from_fd_into_rbuf(
    int fd, 
    BLOCKNUM blocknum,
    FT ft,
    struct rbuf *rb
    ) 
{
    // get the file offset and block size for the block
    DISKOFF offset, size;
    ft->blocktable.translate_blocknum_to_offset_size(blocknum, &offset, &size);
    DISKOFF size_aligned = roundup_to_multiple(512, size);
    uint8_t *XMALLOC_N_ALIGNED(512, size_aligned, raw_block);
    rbuf_init(rb, raw_block, size);
    // read the block
    ssize_t rlen = toku_os_pread(fd, raw_block, size_aligned, offset);
    assert((DISKOFF)rlen >= size);
    assert((DISKOFF)rlen <= size_aligned);
}

static const int read_header_heuristic_max = 32*1024;

#ifndef MIN
#define MIN(a,b) (((a)>(b)) ? (b) : (a))
#endif

// Effect: If the header part of the node is small enough, then read it into the rbuf.  The rbuf will be allocated to be big enough in any case.
static void read_ftnode_header_from_fd_into_rbuf_if_small_enough(int fd, BLOCKNUM blocknum,
                                                                 FT ft, struct rbuf *rb,
                                                                 ftnode_fetch_extra *bfe) {
    DISKOFF offset, size;
    ft->blocktable.translate_blocknum_to_offset_size(blocknum, &offset, &size);
    DISKOFF read_size = roundup_to_multiple(512, MIN(read_header_heuristic_max, size));
    uint8_t *XMALLOC_N_ALIGNED(512, roundup_to_multiple(512, size), raw_block);
    rbuf_init(rb, raw_block, read_size);

    // read the block
    tokutime_t t0 = toku_time_now();
    ssize_t rlen = toku_os_pread(fd, raw_block, read_size, offset);
    tokutime_t t1 = toku_time_now();

    assert(rlen >= 0);
    rbuf_init(rb, raw_block, rlen);

    bfe->bytes_read = rlen;
    bfe->io_time = t1 - t0;
    toku_ft_status_update_pivot_fetch_reason(bfe);
}

//
// read the compressed partition into the sub_block,
// validate the checksum of the compressed data
//
int
read_compressed_sub_block(struct rbuf *rb, struct sub_block *sb)
{
    int r = 0;
    sb->compressed_size = rbuf_int(rb);
    sb->uncompressed_size = rbuf_int(rb);
    const void **cp = (const void **) &sb->compressed_ptr;
    rbuf_literal_bytes(rb, cp, sb->compressed_size);
    sb->xsum = rbuf_int(rb);
    // let's check the checksum
    uint32_t actual_xsum = toku_x1764_memory((char *)sb->compressed_ptr-8, 8+sb->compressed_size);
    if (sb->xsum != actual_xsum) {
        r = TOKUDB_BAD_CHECKSUM;
    }
    return r;
}

static int
read_and_decompress_sub_block(struct rbuf *rb, struct sub_block *sb)
{
    int r = 0;
    r = read_compressed_sub_block(rb, sb);
    if (r != 0) {
        goto exit;
    }

    just_decompress_sub_block(sb);
exit:
    return r;
}

// Allocates space for the sub-block and de-compresses the data from
// the supplied compressed pointer..
void
just_decompress_sub_block(struct sub_block *sb)
{
    // <CER> TODO: Add assert that the subblock was read in.
    sb->uncompressed_ptr = toku_xmalloc(sb->uncompressed_size);

    toku_decompress(
        (Bytef *) sb->uncompressed_ptr,
        sb->uncompressed_size,
        (Bytef *) sb->compressed_ptr,
        sb->compressed_size
        );
}

// verify the checksum
int
verify_ftnode_sub_block (struct sub_block *sb)
{
    int r = 0;
    // first verify the checksum
    uint32_t data_size = sb->uncompressed_size - 4; // checksum is 4 bytes at end
    uint32_t stored_xsum = toku_dtoh32(*((uint32_t *)((char *)sb->uncompressed_ptr + data_size)));
    uint32_t actual_xsum = toku_x1764_memory(sb->uncompressed_ptr, data_size);
    if (stored_xsum != actual_xsum) {
        dump_bad_block((Bytef *) sb->uncompressed_ptr, sb->uncompressed_size);
        r = TOKUDB_BAD_CHECKSUM;
    }
    return r;
}

// This function deserializes the data stored by serialize_ftnode_info
static int
deserialize_ftnode_info(
    struct sub_block *sb, 
    FTNODE node
    )
{
    // sb_node_info->uncompressed_ptr stores the serialized node information
    // this function puts that information into node

    // first verify the checksum
    int r = 0;
    r = verify_ftnode_sub_block(sb);
    if (r != 0) {
        goto exit;
    }

    uint32_t data_size;
    data_size = sb->uncompressed_size - 4; // checksum is 4 bytes at end

    // now with the data verified, we can read the information into the node
    struct rbuf rb;
    rbuf_init(&rb, (unsigned char *) sb->uncompressed_ptr, data_size);

    node->max_msn_applied_to_node_on_disk = rbuf_MSN(&rb);
    (void)rbuf_int(&rb);
    node->flags = rbuf_int(&rb);
    node->height = rbuf_int(&rb);
    rbuf_TXNID(&rb, &node->oldest_referenced_xid_known);

    // now create the basement nodes or childinfos, depending on whether this is a
    // leaf node or internal node
    // now the subtree_estimates

    // n_children is now in the header, nd the allocatio of the node->bp is in deserialize_ftnode_from_rbuf.

    // now the pivots
    if (node->n_children > 1) {
        node->pivotkeys.deserialize_from_rbuf(&rb, node->n_children - 1);
    } else {
        node->pivotkeys.create_empty();
    }

    // if this is an internal node, unpack the block nums, and fill in necessary fields
    // of childinfo
    if (node->height > 0) {
        for (int i = 0; i < node->n_children; i++) {
            BP_BLOCKNUM(node,i) = rbuf_blocknum(&rb);
            BP_WORKDONE(node, i) = 0;
        }
    }

    // make sure that all the data was read
    if (data_size != rb.ndone) {
        dump_bad_block(rb.buf, rb.size);
        abort();
    }
exit:
    return r;
}

static void
setup_available_ftnode_partition(FTNODE node, int i) {
    if (node->height == 0) {
        set_BLB(node, i, toku_create_empty_bn());
        BLB_MAX_MSN_APPLIED(node,i) = node->max_msn_applied_to_node_on_disk;
    }
    else {
        set_BNC(node, i, toku_create_empty_nl());
    }
}

// Assign the child_to_read member of the bfe from the given ftnode
// that has been brought into memory.
static void
update_bfe_using_ftnode(FTNODE node, ftnode_fetch_extra *bfe)
{
    if (bfe->type == ftnode_fetch_subset && bfe->search != NULL) {
        // we do not take into account prefetching yet
        // as of now, if we need a subset, the only thing
        // we can possibly require is a single basement node
        // we find out what basement node the query cares about
        // and check if it is available
        bfe->child_to_read = toku_ft_search_which_child(
            bfe->ft->cmp,
            node,
            bfe->search
            );
    } else if (bfe->type == ftnode_fetch_keymatch) {
        // we do not take into account prefetching yet
        // as of now, if we need a subset, the only thing
        // we can possibly require is a single basement node
        // we find out what basement node the query cares about
        // and check if it is available
        if (node->height == 0) {
            int left_child = bfe->leftmost_child_wanted(node);
            int right_child = bfe->rightmost_child_wanted(node);
            if (left_child == right_child) {
                bfe->child_to_read = left_child;
            }
        }
    }
}

// Using the search parameters in the bfe, this function will
// initialize all of the given ftnode's partitions.
static void
setup_partitions_using_bfe(FTNODE node,
                           ftnode_fetch_extra *bfe,
                           bool data_in_memory)
{
    // Leftmost and Rightmost Child bounds.
    int lc, rc;
    if (bfe->type == ftnode_fetch_subset || bfe->type == ftnode_fetch_prefetch) {
        lc = bfe->leftmost_child_wanted(node);
        rc = bfe->rightmost_child_wanted(node);
    } else {
        lc = -1;
        rc = -1;
    }

    //
    // setup memory needed for the node
    //
    //printf("node height %d, blocknum %" PRId64 ", type %d lc %d rc %d\n", node->height, node->blocknum.b, bfe->type, lc, rc);
    for (int i = 0; i < node->n_children; i++) {
        BP_INIT_UNTOUCHED_CLOCK(node,i);
        if (data_in_memory) {
            BP_STATE(node, i) = ((bfe->wants_child_available(i) || (lc <= i && i <= rc))
                                 ? PT_AVAIL : PT_COMPRESSED);
        } else {
            BP_STATE(node, i) = PT_ON_DISK;
        }
        BP_WORKDONE(node,i) = 0;

        switch (BP_STATE(node,i)) {
        case PT_AVAIL:
            setup_available_ftnode_partition(node, i);
            BP_TOUCH_CLOCK(node,i);
            break;
        case PT_COMPRESSED:
            set_BSB(node, i, sub_block_creat());
            break;
        case PT_ON_DISK:
            set_BNULL(node, i);
            break;
        case PT_INVALID:
            abort();
        }
    }
}

static void setup_ftnode_partitions(FTNODE node, ftnode_fetch_extra *bfe, bool data_in_memory)
// Effect: Used when reading a ftnode into main memory, this sets up the partitions.
//   We set bfe->child_to_read as well as the BP_STATE and the data pointers (e.g., with set_BSB or set_BNULL or other set_ operations).
// Arguments:  Node: the node to set up.
//             bfe:  Describes the key range needed.
//             data_in_memory: true if we have all the data (in which case we set the BP_STATE to be either PT_AVAIL or PT_COMPRESSED depending on the bfe.
//                             false if we don't have the partitions in main memory (in which case we set the state to PT_ON_DISK.
{
    // Set bfe->child_to_read.
    update_bfe_using_ftnode(node, bfe);

    // Setup the partitions.
    setup_partitions_using_bfe(node, bfe, data_in_memory);
}

/* deserialize the partition from the sub-block's uncompressed buffer
 * and destroy the uncompressed buffer
 */
static int
deserialize_ftnode_partition(
    struct sub_block *sb,
    FTNODE node,
    int childnum // which partition to deserialize
    )
{
    int r = 0;
    r = verify_ftnode_sub_block(sb);
    if (r != 0) {
        goto exit;
    }
    uint32_t data_size;
    data_size = sb->uncompressed_size - 4; // checksum is 4 bytes at end

    // now with the data verified, we can read the information into the node
    struct rbuf rb;
    rbuf_init(&rb, (unsigned char *) sb->uncompressed_ptr, data_size);
    unsigned char ch;
    ch = rbuf_char(&rb);

    if (node->height > 0) {
        assert(ch == FTNODE_PARTITION_MSG_BUFFER);
        NONLEAF_CHILDINFO bnc = BNC(node, childnum);
        deserialize_child_buffer(bnc, &rb);
        BP_WORKDONE(node, childnum) = 0;
    }
    else {
        assert(ch == FTNODE_PARTITION_DMT_LEAVES);
        BLB_SEQINSERT(node, childnum) = 0;
        uint32_t num_entries = rbuf_int(&rb);
        // we are now at the first byte of first leafentry
        data_size -= rb.ndone; // remaining bytes of leafentry data

        BASEMENTNODE bn = BLB(node, childnum);
        bn->data_buffer.deserialize_from_rbuf(num_entries, &rb, data_size, node->layout_version_read_from_disk);
    }
    assert(rb.ndone == rb.size);
exit:
    return r;
}

static int
decompress_and_deserialize_worker(struct rbuf curr_rbuf, struct sub_block curr_sb, FTNODE node, int child,
                                 tokutime_t *decompress_time)
{
    int r = 0;
    tokutime_t t0 = toku_time_now();
    r = read_and_decompress_sub_block(&curr_rbuf, &curr_sb);
    tokutime_t t1 = toku_time_now();
    if (r == 0) {
        // at this point, sb->uncompressed_ptr stores the serialized node partition
        r = deserialize_ftnode_partition(&curr_sb, node, child);
    }
    *decompress_time = t1 - t0;

    toku_free(curr_sb.uncompressed_ptr);
    return r;
}

static int
check_and_copy_compressed_sub_block_worker(struct rbuf curr_rbuf, struct sub_block curr_sb, FTNODE node, int child)
{
    int r = 0;
    r = read_compressed_sub_block(&curr_rbuf, &curr_sb);
    if (r != 0) {
        goto exit;
    }

    SUB_BLOCK bp_sb;
    bp_sb = BSB(node, child);
    bp_sb->compressed_size = curr_sb.compressed_size;
    bp_sb->uncompressed_size = curr_sb.uncompressed_size;
    bp_sb->compressed_ptr = toku_xmalloc(bp_sb->compressed_size);
    memcpy(bp_sb->compressed_ptr, curr_sb.compressed_ptr, bp_sb->compressed_size);
exit:
    return r;
}

static FTNODE alloc_ftnode_for_deserialize(uint32_t fullhash, BLOCKNUM blocknum) {
// Effect: Allocate an FTNODE and fill in the values that are not read from
    FTNODE XMALLOC(node);
    node->fullhash = fullhash;
    node->blocknum = blocknum;
    node->dirty = 0;
    node->bp = nullptr;
    node->oldest_referenced_xid_known = TXNID_NONE;
    return node; 
}

static int
deserialize_ftnode_header_from_rbuf_if_small_enough (FTNODE *ftnode,
                                                      FTNODE_DISK_DATA* ndd, 
                                                      BLOCKNUM blocknum,
                                                      uint32_t fullhash,
                                                      ftnode_fetch_extra *bfe,
                                                      struct rbuf *rb,
                                                      int fd)
// If we have enough information in the rbuf to construct a header, then do so.
// Also fetch in the basement node if needed.
// Return 0 if it worked.  If something goes wrong (including that we are looking at some old data format that doesn't have partitions) then return nonzero.
{
    int r = 0;

    tokutime_t t0, t1;
    tokutime_t decompress_time = 0;
    tokutime_t deserialize_time = 0;
    
    t0 = toku_time_now();

    FTNODE node = alloc_ftnode_for_deserialize(fullhash, blocknum);

    if (rb->size < 24) {
        // TODO: What error do we return here?
        // Does it even matter?
        r = toku_db_badformat();
        goto cleanup;
    }

    const void *magic;
    rbuf_literal_bytes(rb, &magic, 8);
    if (memcmp(magic, "tokuleaf", 8)!=0 &&
        memcmp(magic, "tokunode", 8)!=0) {
        r = toku_db_badformat();        
        goto cleanup;
    }

    node->layout_version_read_from_disk = rbuf_int(rb);

    // If we get here, we know the node is at least
    // FT_FIRST_LAYOUT_VERSION_WITH_BASEMENT_NODES.  We haven't changed
    // the serialization format since then (this comment is correct as of
    // version 20, which is Deadshot) so we can go ahead and say the
    // layout version is current (it will be as soon as we finish
    // deserializing).
    // TODO(leif): remove node->layout_version (#5174)
    node->layout_version = FT_LAYOUT_VERSION;

    node->layout_version_original = rbuf_int(rb);
    node->build_id = rbuf_int(rb);
    node->n_children = rbuf_int(rb);
    // Guaranteed to be have been able to read up to here.  If n_children
    // is too big, we may have a problem, so check that we won't overflow
    // while reading the partition locations.
    unsigned int nhsize;
    nhsize =  serialize_node_header_size(node); // we can do this because n_children is filled in.
    unsigned int needed_size;
    needed_size = nhsize + 12; // we need 12 more so that we can read the compressed block size information that follows for the nodeinfo.
    if (needed_size > rb->size) {
        r = toku_db_badformat();
        goto cleanup;
    }

    XMALLOC_N(node->n_children, node->bp);
    XMALLOC_N(node->n_children, *ndd);
    // read the partition locations
    for (int i=0; i<node->n_children; i++) {
        BP_START(*ndd,i) = rbuf_int(rb);
        BP_SIZE (*ndd,i) = rbuf_int(rb);
    }

    uint32_t checksum;
    checksum = toku_x1764_memory(rb->buf, rb->ndone);
    uint32_t stored_checksum;
    stored_checksum = rbuf_int(rb);
    if (stored_checksum != checksum) {
        dump_bad_block(rb->buf, rb->size);
        r = TOKUDB_BAD_CHECKSUM;
        goto cleanup;
    }

    // Now we want to read the pivot information.
    struct sub_block sb_node_info;
    sub_block_init(&sb_node_info);
    sb_node_info.compressed_size = rbuf_int(rb); // we'll be able to read these because we checked the size earlier.
    sb_node_info.uncompressed_size = rbuf_int(rb);
    if (rb->size-rb->ndone < sb_node_info.compressed_size + 8) {
        r = toku_db_badformat();
        goto cleanup;
    }

    // Finish reading compressed the sub_block
    const void **cp;
    cp = (const void **) &sb_node_info.compressed_ptr;
    rbuf_literal_bytes(rb, cp, sb_node_info.compressed_size);
    sb_node_info.xsum = rbuf_int(rb);
    // let's check the checksum
    uint32_t actual_xsum;
    actual_xsum = toku_x1764_memory((char *)sb_node_info.compressed_ptr-8, 8+sb_node_info.compressed_size);
    if (sb_node_info.xsum != actual_xsum) {
        r = TOKUDB_BAD_CHECKSUM;
        goto cleanup;
    }

    // Now decompress the subblock
    {
        toku::scoped_malloc sb_node_info_buf(sb_node_info.uncompressed_size);
        sb_node_info.uncompressed_ptr = sb_node_info_buf.get();
        tokutime_t decompress_t0 = toku_time_now();
        toku_decompress(
            (Bytef *) sb_node_info.uncompressed_ptr,
            sb_node_info.uncompressed_size,
            (Bytef *) sb_node_info.compressed_ptr,
            sb_node_info.compressed_size
            );
        tokutime_t decompress_t1 = toku_time_now();
        decompress_time = decompress_t1 - decompress_t0;

        // at this point sb->uncompressed_ptr stores the serialized node info.
        r = deserialize_ftnode_info(&sb_node_info, node);
        if (r != 0) {
            goto cleanup;
        }
    }

    // Now we have the ftnode_info.  We have a bunch more stuff in the
    // rbuf, so we might be able to store the compressed data for some
    // objects.
    // We can proceed to deserialize the individual subblocks.

    // setup the memory of the partitions
    // for partitions being decompressed, create either message buffer or basement node
    // for partitions staying compressed, create sub_block
    setup_ftnode_partitions(node, bfe, false);

    // We must capture deserialize and decompression time before
    // the pf_callback, otherwise we would double-count.
    t1 = toku_time_now();
    deserialize_time = (t1 - t0) - decompress_time;

    // do partial fetch if necessary
    if (bfe->type != ftnode_fetch_none) {
        PAIR_ATTR attr;
        r = toku_ftnode_pf_callback(node, *ndd, bfe, fd, &attr);
        if (r != 0) {
            goto cleanup;
        }
    }

    // handle clock
    for (int i = 0; i < node->n_children; i++) {
        if (bfe->wants_child_available(i)) {
            paranoid_invariant(BP_STATE(node,i) == PT_AVAIL);
            BP_TOUCH_CLOCK(node,i);
        }
    }
    *ftnode = node;
    r = 0;

cleanup:
    if (r == 0) {
        bfe->deserialize_time += deserialize_time;
        bfe->decompress_time += decompress_time;
        toku_ft_status_update_deserialize_times(node, deserialize_time, decompress_time);
    }
    if (r != 0) {
        if (node) {
            toku_free(*ndd);
            toku_free(node->bp);
            toku_free(node);
        }
    }
    return r;
}


static int
read_and_decompress_block_from_fd_into_rbuf(int fd, BLOCKNUM blocknum,
                                            DISKOFF offset, DISKOFF size,
                                            FT ft,
                                            struct rbuf *rb,
                                            /* out */ int *layout_version_p);


static int
deserialize_ftnode_from_rbuf(
    FTNODE *ftnode,
    FTNODE_DISK_DATA* ndd,
    BLOCKNUM blocknum,
    uint32_t fullhash,
    ftnode_fetch_extra *bfe,
    struct rbuf *rb
    )
// Effect: deserializes a ftnode that is in rb (with pointer of rb just past the magic) into a FTNODE.
{
    int r = 0;
    struct sub_block sb_node_info;

    tokutime_t t0, t1;
    tokutime_t decompress_time = 0;
    tokutime_t deserialize_time = 0;

    t0 = toku_time_now();

    FTNODE node = alloc_ftnode_for_deserialize(fullhash, blocknum);

    // now start reading from rbuf
    // first thing we do is read the header information
    const void *magic;
    rbuf_literal_bytes(rb, &magic, 8);
    if (memcmp(magic, "tokuleaf", 8)!=0 &&
        memcmp(magic, "tokunode", 8)!=0) {
        r = toku_db_badformat();
        goto cleanup;
    }

    node->layout_version_read_from_disk = rbuf_int(rb);
    lazy_assert(node->layout_version_read_from_disk >= FT_LAYOUT_MIN_SUPPORTED_VERSION);

    // Upgrade versions after 14 to current.  This upgrade is trivial, it
    // removes the optimized for upgrade field, which has already been
    // removed in the deserialization code (see
    // deserialize_ftnode_info()).
    node->layout_version = FT_LAYOUT_VERSION;
    node->layout_version_original = rbuf_int(rb);
    node->build_id = rbuf_int(rb);
    node->n_children = rbuf_int(rb);
    XMALLOC_N(node->n_children, node->bp);
    XMALLOC_N(node->n_children, *ndd);
    // read the partition locations
    for (int i=0; i<node->n_children; i++) {
        BP_START(*ndd,i) = rbuf_int(rb);
        BP_SIZE (*ndd,i) = rbuf_int(rb);
    }
    // verify checksum of header stored
    uint32_t checksum;
    checksum = toku_x1764_memory(rb->buf, rb->ndone);
    uint32_t stored_checksum;
    stored_checksum = rbuf_int(rb);
    if (stored_checksum != checksum) {
        dump_bad_block(rb->buf, rb->size);
        invariant(stored_checksum == checksum);
    }

    // now we read and decompress the pivot and child information
    sub_block_init(&sb_node_info);
    {
        tokutime_t sb_decompress_t0 = toku_time_now();
        r = read_and_decompress_sub_block(rb, &sb_node_info);
        tokutime_t sb_decompress_t1 = toku_time_now();
        decompress_time += sb_decompress_t1 - sb_decompress_t0;
    }
    if (r != 0) {
        goto cleanup;
    }

    // at this point, sb->uncompressed_ptr stores the serialized node info
    r = deserialize_ftnode_info(&sb_node_info, node);
    if (r != 0) {
        goto cleanup;
    }
    toku_free(sb_node_info.uncompressed_ptr);

    // now that the node info has been deserialized, we can proceed to deserialize
    // the individual sub blocks

    // setup the memory of the partitions
    // for partitions being decompressed, create either message buffer or basement node
    // for partitions staying compressed, create sub_block
    setup_ftnode_partitions(node, bfe, true);

    // This loop is parallelizeable, since we don't have a dependency on the work done so far.
    for (int i = 0; i < node->n_children; i++) {
        uint32_t curr_offset = BP_START(*ndd,i);
        uint32_t curr_size   = BP_SIZE(*ndd,i);
        // the compressed, serialized partitions start at where rb is currently pointing,
        // which would be rb->buf + rb->ndone
        // we need to intialize curr_rbuf to point to this place
        struct rbuf curr_rbuf  = {.buf = NULL, .size = 0, .ndone = 0};
        rbuf_init(&curr_rbuf, rb->buf + curr_offset, curr_size);

        //
        // now we are at the point where we have:
        //  - read the entire compressed node off of disk,
        //  - decompressed the pivot and offset information,
        //  - have arrived at the individual partitions.
        //
        // Based on the information in bfe, we want to decompress a subset of
        // of the compressed partitions (also possibly none or possibly all)
        // The partitions that we want to decompress and make available
        // to the node, we do, the rest we simply copy in compressed
        // form into the node, and set the state of the partition to PT_COMPRESSED
        //

        struct sub_block curr_sb;
        sub_block_init(&curr_sb);

        // curr_rbuf is passed by value to decompress_and_deserialize_worker, so there's no ugly race condition.
        // This would be more obvious if curr_rbuf were an array.

        // deserialize_ftnode_info figures out what the state
        // should be and sets up the memory so that we are ready to use it

        switch (BP_STATE(node,i)) {
        case PT_AVAIL: {
                //  case where we read and decompress the partition
                tokutime_t partition_decompress_time;
                r = decompress_and_deserialize_worker(curr_rbuf, curr_sb, node, i,
                                                      &partition_decompress_time);
                decompress_time += partition_decompress_time;
                if (r != 0) {
                    goto cleanup;
                }
                break;
            }
        case PT_COMPRESSED:
            // case where we leave the partition in the compressed state
            r = check_and_copy_compressed_sub_block_worker(curr_rbuf, curr_sb, node, i);
            if (r != 0) {
                goto cleanup;
            }
            break;
        case PT_INVALID: // this is really bad
        case PT_ON_DISK: // it's supposed to be in memory.
            abort();
        }
    }
    *ftnode = node;
    r = 0;

cleanup:
    if (r == 0) {
        t1 = toku_time_now();
        deserialize_time = (t1 - t0) - decompress_time;
        bfe->deserialize_time += deserialize_time;
        bfe->decompress_time += decompress_time; 
        toku_ft_status_update_deserialize_times(node, deserialize_time, decompress_time);
    }
    if (r != 0) {
        // NOTE: Right now, callers higher in the stack will assert on
        // failure, so this is OK for production.  However, if we
        // create tools that use this function to search for errors in
        // the FT, then we will leak memory.
        if (node) {
            toku_free(node);
        }
    }
    return r;
}

int
toku_deserialize_bp_from_disk(FTNODE node, FTNODE_DISK_DATA ndd, int childnum, int fd, ftnode_fetch_extra *bfe) {
    int r = 0;
    assert(BP_STATE(node,childnum) == PT_ON_DISK);
    assert(node->bp[childnum].ptr.tag == BCT_NULL);
    
    //
    // setup the partition
    //
    setup_available_ftnode_partition(node, childnum);
    BP_STATE(node,childnum) = PT_AVAIL;
    
    //
    // read off disk and make available in memory
    // 
    // get the file offset and block size for the block
    DISKOFF node_offset, total_node_disk_size;
    bfe->ft->blocktable.translate_blocknum_to_offset_size(node->blocknum, &node_offset, &total_node_disk_size);

    uint32_t curr_offset = BP_START(ndd, childnum);
    uint32_t curr_size = BP_SIZE (ndd, childnum);

    struct rbuf rb;
    rbuf_init(&rb, nullptr, 0);

    uint32_t pad_at_beginning = (node_offset+curr_offset)%512;
    uint32_t padded_size = roundup_to_multiple(512, pad_at_beginning + curr_size);

    toku::scoped_malloc_aligned raw_block_buf(padded_size, 512);
    uint8_t *raw_block = reinterpret_cast<uint8_t *>(raw_block_buf.get());
    rbuf_init(&rb, pad_at_beginning+raw_block, curr_size);
    tokutime_t t0 = toku_time_now();

    // read the block
    assert(0==((unsigned long long)raw_block)%512); // for O_DIRECT
    assert(0==(padded_size)%512);
    assert(0==(node_offset+curr_offset-pad_at_beginning)%512);
    ssize_t rlen = toku_os_pread(fd, raw_block, padded_size, node_offset+curr_offset-pad_at_beginning);
    assert((DISKOFF)rlen >= pad_at_beginning + curr_size); // we read in at least enough to get what we wanted
    assert((DISKOFF)rlen <= padded_size);                  // we didn't read in too much.

    tokutime_t t1 = toku_time_now();

    // read sub block
    struct sub_block curr_sb;
    sub_block_init(&curr_sb);
    r = read_compressed_sub_block(&rb, &curr_sb);
    if (r != 0) {
        return r;
    }
    invariant(curr_sb.compressed_ptr != NULL);

    // decompress
    toku::scoped_malloc uncompressed_buf(curr_sb.uncompressed_size);
    curr_sb.uncompressed_ptr = uncompressed_buf.get();
    toku_decompress((Bytef *) curr_sb.uncompressed_ptr, curr_sb.uncompressed_size,
                    (Bytef *) curr_sb.compressed_ptr, curr_sb.compressed_size);

    // deserialize
    tokutime_t t2 = toku_time_now();

    r = deserialize_ftnode_partition(&curr_sb, node, childnum);

    tokutime_t t3 = toku_time_now();

    // capture stats
    tokutime_t io_time = t1 - t0;
    tokutime_t decompress_time = t2 - t1;
    tokutime_t deserialize_time = t3 - t2;
    bfe->deserialize_time += deserialize_time;
    bfe->decompress_time += decompress_time;
    toku_ft_status_update_deserialize_times(node, deserialize_time, decompress_time);

    bfe->bytes_read = rlen;
    bfe->io_time = io_time;

    return r;
}

// Take a ftnode partition that is in the compressed state, and make it avail
int
toku_deserialize_bp_from_compressed(FTNODE node, int childnum, ftnode_fetch_extra *bfe) {
    int r = 0;
    assert(BP_STATE(node, childnum) == PT_COMPRESSED);
    SUB_BLOCK curr_sb = BSB(node, childnum);

    toku::scoped_malloc uncompressed_buf(curr_sb->uncompressed_size);
    assert(curr_sb->uncompressed_ptr == NULL);
    curr_sb->uncompressed_ptr = uncompressed_buf.get();

    setup_available_ftnode_partition(node, childnum);
    BP_STATE(node,childnum) = PT_AVAIL;

    // decompress the sub_block
    tokutime_t t0 = toku_time_now();

    toku_decompress(
        (Bytef *) curr_sb->uncompressed_ptr,
        curr_sb->uncompressed_size,
        (Bytef *) curr_sb->compressed_ptr,
        curr_sb->compressed_size
        );

    tokutime_t t1 = toku_time_now();

    r = deserialize_ftnode_partition(curr_sb, node, childnum);

    tokutime_t t2 = toku_time_now();

    tokutime_t decompress_time = t1 - t0;
    tokutime_t deserialize_time = t2 - t1;
    bfe->deserialize_time += deserialize_time;
    bfe->decompress_time += decompress_time;
    toku_ft_status_update_deserialize_times(node, deserialize_time, decompress_time);

    toku_free(curr_sb->compressed_ptr);
    toku_free(curr_sb);
    return r;
}

static int
deserialize_ftnode_from_fd(int fd,
                            BLOCKNUM blocknum,
                            uint32_t fullhash,
                            FTNODE *ftnode,
                            FTNODE_DISK_DATA *ndd,
                            ftnode_fetch_extra *bfe)
{
    struct rbuf rb = RBUF_INITIALIZER;

    tokutime_t t0 = toku_time_now();
    read_block_from_fd_into_rbuf(fd, blocknum, bfe->ft, &rb); 
    tokutime_t t1 = toku_time_now();

    // Decompress and deserialize the ftnode. Time statistics
    // are taken inside this function.
    int r = deserialize_ftnode_from_rbuf(ftnode, ndd, blocknum, fullhash, bfe, &rb);
    if (r != 0) {
        dump_bad_block(rb.buf,rb.size);
    }

    bfe->bytes_read = rb.size;
    bfe->io_time = t1 - t0;
    toku_free(rb.buf);
    return r;
}

// Read ftnode from file into struct.  Perform version upgrade if necessary.
int
toku_deserialize_ftnode_from (int fd,
                               BLOCKNUM blocknum,
                               uint32_t fullhash,
                               FTNODE *ftnode,
                               FTNODE_DISK_DATA* ndd,
                               ftnode_fetch_extra *bfe
    )
// Effect: Read a node in.  If possible, read just the header.
{
    int r = 0;
    struct rbuf rb = RBUF_INITIALIZER;

    // each function below takes the appropriate io/decompression/deserialize statistics

    if (!bfe->read_all_partitions) {
        read_ftnode_header_from_fd_into_rbuf_if_small_enough(fd, blocknum, bfe->ft, &rb, bfe);
        r = deserialize_ftnode_header_from_rbuf_if_small_enough(ftnode, ndd, blocknum, fullhash, bfe, &rb, fd);
    } else {
        // force us to do it the old way
        r = -1;
    }
    if (r != 0) {
        // Something went wrong, go back to doing it the old way.
        r = deserialize_ftnode_from_fd(fd, blocknum, fullhash, ftnode, ndd, bfe);
    }

    toku_free(rb.buf);
    return r;
}

void
toku_verify_or_set_counts(FTNODE UU(node)) {
}

int 
toku_db_badformat(void) {
    return DB_BADFORMAT;
}

static size_t
serialize_rollback_log_size(ROLLBACK_LOG_NODE log) {
    size_t size = node_header_overhead //8 "tokuroll", 4 version, 4 version_original, 4 build_id
                 +16 //TXNID_PAIR
                 +8 //sequence
                 +8 //blocknum
                 +8 //previous (blocknum)
                 +8 //resident_bytecount
                 +8 //memarena size
                 +log->rollentry_resident_bytecount;
    return size;
}

static void
serialize_rollback_log_node_to_buf(ROLLBACK_LOG_NODE log, char *buf, size_t calculated_size, int UU(n_sub_blocks), struct sub_block UU(sub_block[])) {
    struct wbuf wb;
    wbuf_init(&wb, buf, calculated_size);
    {   //Serialize rollback log to local wbuf
        wbuf_nocrc_literal_bytes(&wb, "tokuroll", 8);
        lazy_assert(log->layout_version == FT_LAYOUT_VERSION);
        wbuf_nocrc_int(&wb, log->layout_version);
        wbuf_nocrc_int(&wb, log->layout_version_original);
        wbuf_nocrc_uint(&wb, BUILD_ID);
        wbuf_nocrc_TXNID_PAIR(&wb, log->txnid);
        wbuf_nocrc_ulonglong(&wb, log->sequence);
        wbuf_nocrc_BLOCKNUM(&wb, log->blocknum);
        wbuf_nocrc_BLOCKNUM(&wb, log->previous);
        wbuf_nocrc_ulonglong(&wb, log->rollentry_resident_bytecount);
        //Write down memarena size needed to restore
        wbuf_nocrc_ulonglong(&wb, log->rollentry_arena.total_size_in_use());

        {
            //Store rollback logs
            struct roll_entry *item;
            size_t done_before = wb.ndone;
            for (item = log->newest_logentry; item; item = item->prev) {
                toku_logger_rollback_wbuf_nocrc_write(&wb, item);
            }
            lazy_assert(done_before + log->rollentry_resident_bytecount == wb.ndone);
        }
    }
    lazy_assert(wb.ndone == wb.size);
    lazy_assert(calculated_size==wb.ndone);
}

static void
serialize_uncompressed_block_to_memory(char * uncompressed_buf,
                                       int n_sub_blocks,
                                       struct sub_block sub_block[/*n_sub_blocks*/],
                                       enum toku_compression_method method,
                               /*out*/ size_t *n_bytes_to_write,
                               /*out*/ char  **bytes_to_write)
// Guarantees that the malloc'd BYTES_TO_WRITE is 512-byte aligned (so that O_DIRECT will work)
{
    // allocate space for the compressed uncompressed_buf
    size_t compressed_len = get_sum_compressed_size_bound(n_sub_blocks, sub_block, method);
    size_t sub_block_header_len = sub_block_header_size(n_sub_blocks);
    size_t header_len = node_header_overhead + sub_block_header_len + sizeof (uint32_t); // node + sub_block + checksum
    char *XMALLOC_N_ALIGNED(512, roundup_to_multiple(512, header_len + compressed_len), compressed_buf);

    // copy the header
    memcpy(compressed_buf, uncompressed_buf, node_header_overhead);
    if (0) printf("First 4 bytes before compressing data are %02x%02x%02x%02x\n",
                  uncompressed_buf[node_header_overhead],   uncompressed_buf[node_header_overhead+1],
                  uncompressed_buf[node_header_overhead+2], uncompressed_buf[node_header_overhead+3]);

    // compress all of the sub blocks
    char *uncompressed_ptr = uncompressed_buf + node_header_overhead;
    char *compressed_ptr = compressed_buf + header_len;
    compressed_len = compress_all_sub_blocks(n_sub_blocks, sub_block, uncompressed_ptr, compressed_ptr, num_cores, ft_pool, method);

    //if (0) printf("Block %" PRId64 " Size before compressing %u, after compression %" PRIu64 "\n", blocknum.b, calculated_size-node_header_overhead, (uint64_t) compressed_len);

    // serialize the sub block header
    uint32_t *ptr = (uint32_t *)(compressed_buf + node_header_overhead);
    *ptr++ = toku_htod32(n_sub_blocks);
    for (int i=0; i<n_sub_blocks; i++) {
        ptr[0] = toku_htod32(sub_block[i].compressed_size);
        ptr[1] = toku_htod32(sub_block[i].uncompressed_size);
        ptr[2] = toku_htod32(sub_block[i].xsum);
        ptr += 3;
    }

    // compute the header checksum and serialize it
    uint32_t header_length = (char *)ptr - (char *)compressed_buf;
    uint32_t xsum = toku_x1764_memory(compressed_buf, header_length);
    *ptr = toku_htod32(xsum);

    uint32_t padded_len = roundup_to_multiple(512, header_len + compressed_len);
    // Zero out padding.
    for (uint32_t i = header_len+compressed_len; i < padded_len; i++) {
        compressed_buf[i] = 0;
    }
    *n_bytes_to_write = padded_len;
    *bytes_to_write   = compressed_buf;
}

void
toku_serialize_rollback_log_to_memory_uncompressed(ROLLBACK_LOG_NODE log, SERIALIZED_ROLLBACK_LOG_NODE serialized) {
    // get the size of the serialized node
    size_t calculated_size = serialize_rollback_log_size(log);

    serialized->len = calculated_size;
    serialized->n_sub_blocks = 0;
    // choose sub block parameters
    int sub_block_size = 0;
    size_t data_size = calculated_size - node_header_overhead;
    choose_sub_block_size(data_size, max_sub_blocks, &sub_block_size, &serialized->n_sub_blocks);
    lazy_assert(0 < serialized->n_sub_blocks && serialized->n_sub_blocks <= max_sub_blocks);
    lazy_assert(sub_block_size > 0);

    // set the initial sub block size for all of the sub blocks
    for (int i = 0; i < serialized->n_sub_blocks; i++) 
        sub_block_init(&serialized->sub_block[i]);
    set_all_sub_block_sizes(data_size, sub_block_size, serialized->n_sub_blocks, serialized->sub_block);

    // allocate space for the serialized node
    XMALLOC_N(calculated_size, serialized->data);
    // serialize the node into buf
    serialize_rollback_log_node_to_buf(log, serialized->data, calculated_size, serialized->n_sub_blocks, serialized->sub_block);
    serialized->blocknum = log->blocknum;
}

int
toku_serialize_rollback_log_to (int fd, ROLLBACK_LOG_NODE log, SERIALIZED_ROLLBACK_LOG_NODE serialized_log, bool is_serialized,
                                FT ft, bool for_checkpoint) {
    size_t n_to_write;
    char *compressed_buf;
    struct serialized_rollback_log_node serialized_local;

    if (is_serialized) {
        invariant_null(log);
    } else {
        invariant_null(serialized_log);
        serialized_log = &serialized_local;
        toku_serialize_rollback_log_to_memory_uncompressed(log, serialized_log);
    }

    BLOCKNUM blocknum = serialized_log->blocknum;
    invariant(blocknum.b >= 0);

    // Compress and malloc buffer to write
    serialize_uncompressed_block_to_memory(serialized_log->data,
                                           serialized_log->n_sub_blocks,
                                           serialized_log->sub_block,
                                           ft->h->compression_method,
                                           &n_to_write, &compressed_buf);

    // Dirties the ft
    DISKOFF offset;
    ft->blocktable.realloc_on_disk(blocknum, n_to_write, &offset,
                                   ft, fd, for_checkpoint,
                                   // We consider rollback log flushing the hottest possible allocation,
                                   // since rollback logs are short-lived compared to FT nodes.
                                   INT_MAX);

    toku_os_full_pwrite(fd, compressed_buf, n_to_write, offset);
    toku_free(compressed_buf);
    if (!is_serialized) {
        toku_static_serialized_rollback_log_destroy(&serialized_local);
        log->dirty = 0;  // See #1957.   Must set the node to be clean after serializing it so that it doesn't get written again on the next checkpoint or eviction.
    }
    return 0;
}

static int
deserialize_rollback_log_from_rbuf (BLOCKNUM blocknum, ROLLBACK_LOG_NODE *log_p, struct rbuf *rb) {
    ROLLBACK_LOG_NODE MALLOC(result);
    int r;
    if (result==NULL) {
	r=get_error_errno();
	if (0) { died0: toku_free(result); }
	return r;
    }

    const void *magic;
    rbuf_literal_bytes(rb, &magic, 8);
    lazy_assert(!memcmp(magic, "tokuroll", 8));

    result->layout_version    = rbuf_int(rb);
    lazy_assert((result->layout_version <= FT_LAYOUT_VERSION_27) ||
                (result->layout_version == FT_LAYOUT_VERSION));
    result->layout_version_original = rbuf_int(rb);
    result->layout_version_read_from_disk = result->layout_version;
    result->build_id = rbuf_int(rb);
    result->dirty = false;
    //TODO: Maybe add descriptor (or just descriptor version) here eventually?
    //TODO: This is hard.. everything is shared in a single dictionary.
    rbuf_TXNID_PAIR(rb, &result->txnid);
    result->sequence = rbuf_ulonglong(rb);
    result->blocknum = rbuf_blocknum(rb);
    if (result->blocknum.b != blocknum.b) {
        r = toku_db_badformat();
        goto died0;
    }
    result->previous       = rbuf_blocknum(rb);
    result->rollentry_resident_bytecount = rbuf_ulonglong(rb);

    size_t arena_initial_size = rbuf_ulonglong(rb);
    result->rollentry_arena.create(arena_initial_size);
    if (0) { died1: result->rollentry_arena.destroy(); goto died0; }

    //Load rollback entries
    lazy_assert(rb->size > 4);
    //Start with empty list
    result->oldest_logentry = result->newest_logentry = NULL;
    while (rb->ndone < rb->size) {
        struct roll_entry *item;
        uint32_t rollback_fsize = rbuf_int(rb); //Already read 4.  Rest is 4 smaller
        const void *item_vec;
        rbuf_literal_bytes(rb, &item_vec, rollback_fsize-4);
        unsigned char* item_buf = (unsigned char*)item_vec;
        r = toku_parse_rollback(item_buf, rollback_fsize-4, &item, &result->rollentry_arena);
        if (r!=0) {
            r = toku_db_badformat();
            goto died1;
        }
        //Add to head of list
        if (result->oldest_logentry) {
            result->oldest_logentry->prev = item;
            result->oldest_logentry       = item;
            item->prev = NULL;
        }
        else {
            result->oldest_logentry = result->newest_logentry = item;
            item->prev = NULL;
        }
    }

    toku_free(rb->buf);
    rb->buf = NULL;
    *log_p = result;
    return 0;
}

static int
deserialize_rollback_log_from_rbuf_versioned (uint32_t version, BLOCKNUM blocknum,
                                              ROLLBACK_LOG_NODE *log,
                                              struct rbuf *rb) {
    int r = 0;
    ROLLBACK_LOG_NODE rollback_log_node = NULL;
    invariant((version <= FT_LAYOUT_VERSION_27) || version == FT_LAYOUT_VERSION);
    r = deserialize_rollback_log_from_rbuf(blocknum, &rollback_log_node, rb);
    if (r==0) {
        *log = rollback_log_node;
    }
    return r;
}

int
decompress_from_raw_block_into_rbuf(uint8_t *raw_block, size_t raw_block_size, struct rbuf *rb, BLOCKNUM blocknum) {
    int r = 0;
    // get the number of compressed sub blocks
    int n_sub_blocks;
    n_sub_blocks = toku_dtoh32(*(uint32_t*)(&raw_block[node_header_overhead]));

    // verify the number of sub blocks
    invariant(0 <= n_sub_blocks);
    invariant(n_sub_blocks <= max_sub_blocks);

    { // verify the header checksum
        uint32_t header_length = node_header_overhead + sub_block_header_size(n_sub_blocks);
        invariant(header_length <= raw_block_size);
        uint32_t xsum = toku_x1764_memory(raw_block, header_length);
        uint32_t stored_xsum = toku_dtoh32(*(uint32_t *)(raw_block + header_length));
        if (xsum != stored_xsum) {
            r = TOKUDB_BAD_CHECKSUM;
        }
    }

    // deserialize the sub block header
    struct sub_block sub_block[n_sub_blocks];
    uint32_t *sub_block_header = (uint32_t *) &raw_block[node_header_overhead+4];
    for (int i = 0; i < n_sub_blocks; i++) {
        sub_block_init(&sub_block[i]);
        sub_block[i].compressed_size = toku_dtoh32(sub_block_header[0]);
        sub_block[i].uncompressed_size = toku_dtoh32(sub_block_header[1]);
        sub_block[i].xsum = toku_dtoh32(sub_block_header[2]);
        sub_block_header += 3;
    }

    // This predicate needs to be here and instead of where it is set
    // for the compiler.
    if (r == TOKUDB_BAD_CHECKSUM) {
        goto exit;
    }

    // verify sub block sizes
    for (int i = 0; i < n_sub_blocks; i++) {
        uint32_t compressed_size = sub_block[i].compressed_size;
        if (compressed_size<=0   || compressed_size>(1<<30)) { 
            r = toku_db_badformat(); 
            goto exit;
        }

        uint32_t uncompressed_size = sub_block[i].uncompressed_size;
        if (0) printf("Block %" PRId64 " Compressed size = %u, uncompressed size=%u\n", blocknum.b, compressed_size, uncompressed_size);
        if (uncompressed_size<=0 || uncompressed_size>(1<<30)) { 
            r = toku_db_badformat();
            goto exit;
        }
    }

    // sum up the uncompressed size of the sub blocks
    size_t uncompressed_size;
    uncompressed_size = get_sum_uncompressed_size(n_sub_blocks, sub_block);

    // allocate the uncompressed buffer
    size_t size;
    size = node_header_overhead + uncompressed_size;
    unsigned char *buf;
    XMALLOC_N(size, buf);
    rbuf_init(rb, buf, size);

    // copy the uncompressed node header to the uncompressed buffer
    memcpy(rb->buf, raw_block, node_header_overhead);

    // point at the start of the compressed data (past the node header, the sub block header, and the header checksum)
    unsigned char *compressed_data;
    compressed_data = raw_block + node_header_overhead + sub_block_header_size(n_sub_blocks) + sizeof (uint32_t);

    // point at the start of the uncompressed data
    unsigned char *uncompressed_data;
    uncompressed_data = rb->buf + node_header_overhead;    

    // decompress all the compressed sub blocks into the uncompressed buffer
    r = decompress_all_sub_blocks(n_sub_blocks, sub_block, compressed_data, uncompressed_data, num_cores, ft_pool);
    if (r != 0) {
        fprintf(stderr, "%s:%d block %" PRId64 " failed %d at %p size %lu\n", __FUNCTION__, __LINE__, blocknum.b, r, raw_block, raw_block_size);
        dump_bad_block(raw_block, raw_block_size);
        goto exit;
    }

    rb->ndone=0;
exit:
    return r;
}

static int decompress_from_raw_block_into_rbuf_versioned(uint8_t *raw_block, size_t raw_block_size, struct rbuf *rb, BLOCKNUM blocknum) {
    // This function exists solely to accomodate future changes in compression.
    int r = 0;
    r = decompress_from_raw_block_into_rbuf(raw_block, raw_block_size, rb, blocknum);
    return r;
}

static int
read_and_decompress_block_from_fd_into_rbuf(int fd, BLOCKNUM blocknum,
                                            DISKOFF offset, DISKOFF size,
                                            FT ft,
                                            struct rbuf *rb,
                                  /* out */ int *layout_version_p) {
    int r = 0;
    if (0) printf("Deserializing Block %" PRId64 "\n", blocknum.b);

    DISKOFF size_aligned = roundup_to_multiple(512, size);
    uint8_t *XMALLOC_N_ALIGNED(512, size_aligned, raw_block);
    {
        // read the (partially compressed) block
        ssize_t rlen = toku_os_pread(fd, raw_block, size_aligned, offset);
        lazy_assert((DISKOFF)rlen >= size);
        lazy_assert((DISKOFF)rlen <= size_aligned);
    }
    // get the layout_version
    int layout_version;
    {
        uint8_t *magic = raw_block + uncompressed_magic_offset;
        if (memcmp(magic, "tokuleaf", 8)!=0 &&
            memcmp(magic, "tokunode", 8)!=0 &&
            memcmp(magic, "tokuroll", 8)!=0) {
            r = toku_db_badformat();
            goto cleanup;
        }
        uint8_t *version = raw_block + uncompressed_version_offset;
        layout_version = toku_dtoh32(*(uint32_t*)version);
        if (layout_version < FT_LAYOUT_MIN_SUPPORTED_VERSION || layout_version > FT_LAYOUT_VERSION) {
            r = toku_db_badformat();
            goto cleanup;
        }
    }

    r = decompress_from_raw_block_into_rbuf_versioned(raw_block, size, rb, blocknum);
    if (r != 0) {
        // We either failed the checksome, or there is a bad format in
        // the buffer.
        if (r == TOKUDB_BAD_CHECKSUM) {
            fprintf(stderr,
                    "Checksum failure while reading raw block in file %s.\n",
                    toku_cachefile_fname_in_env(ft->cf));
            abort();
        } else {
            r = toku_db_badformat();
            goto cleanup;
        }
    }

    *layout_version_p = layout_version;
cleanup:
    if (r!=0) {
        if (rb->buf) toku_free(rb->buf);
        rb->buf = NULL;
    }
    if (raw_block) {
        toku_free(raw_block);
    }
    return r;
}

// Read rollback log node from file into struct.
// Perform version upgrade if necessary.
int toku_deserialize_rollback_log_from(int fd, BLOCKNUM blocknum, ROLLBACK_LOG_NODE *logp, FT ft) {
    int layout_version = 0;
    int r;

    struct rbuf rb;
    rbuf_init(&rb, nullptr, 0);

    // get the file offset and block size for the block
    DISKOFF offset, size;
    ft->blocktable.translate_blocknum_to_offset_size(blocknum, &offset, &size);

    // if the size is 0, then the blocknum is unused
    if (size == 0) {
        // blocknum is unused, just create an empty one and get out
        ROLLBACK_LOG_NODE XMALLOC(log);
        rollback_empty_log_init(log);
        log->blocknum.b = blocknum.b;
        r = 0;
        *logp = log;
        goto cleanup;
    }

    r = read_and_decompress_block_from_fd_into_rbuf(fd, blocknum, offset, size, ft, &rb, &layout_version);
    if (r!=0) goto cleanup;

    {
        uint8_t *magic = rb.buf + uncompressed_magic_offset;
        if (memcmp(magic, "tokuroll", 8)!=0) {
            r = toku_db_badformat();
            goto cleanup;
        }
    }

    r = deserialize_rollback_log_from_rbuf_versioned(layout_version, blocknum, logp, &rb);

cleanup:
    if (rb.buf) {
        toku_free(rb.buf);
    }
    return r;
}

#undef UPGRADE_STATUS_VALUE
