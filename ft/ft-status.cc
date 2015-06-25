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

#include "ft/ft.h"
#include "ft/ft-status.h"

#include <toku_race_tools.h>

LE_STATUS_S le_status;
void LE_STATUS_S::init() {
    if (m_initialized) return;
#define LE_STATUS_INIT(k,c,t,l,inc) TOKUFT_STATUS_INIT((*this), k, c, t, "le: " l, inc)
    LE_STATUS_INIT(LE_MAX_COMMITTED_XR,   nullptr, UINT64, "max committed xr", TOKU_ENGINE_STATUS);
    LE_STATUS_INIT(LE_MAX_PROVISIONAL_XR, nullptr, UINT64, "max provisional xr", TOKU_ENGINE_STATUS);
    LE_STATUS_INIT(LE_EXPANDED,           nullptr, UINT64, "expanded", TOKU_ENGINE_STATUS);
    LE_STATUS_INIT(LE_MAX_MEMSIZE,        nullptr, UINT64, "max memsize", TOKU_ENGINE_STATUS);
    LE_STATUS_INIT(LE_APPLY_GC_BYTES_IN,  nullptr, PARCOUNT, "size of leafentries before garbage collection (during message application)", TOKU_ENGINE_STATUS);
    LE_STATUS_INIT(LE_APPLY_GC_BYTES_OUT, nullptr, PARCOUNT, "size of leafentries after garbage collection (during message application)", TOKU_ENGINE_STATUS);
    LE_STATUS_INIT(LE_NORMAL_GC_BYTES_IN, nullptr, PARCOUNT, "size of leafentries before garbage collection (outside message application)", TOKU_ENGINE_STATUS);
    LE_STATUS_INIT(LE_NORMAL_GC_BYTES_OUT,nullptr, PARCOUNT, "size of leafentries after garbage collection (outside message application)", TOKU_ENGINE_STATUS);
    m_initialized = true;
#undef LE_STATUS_INIT
}
void LE_STATUS_S::destroy() {
    if (!m_initialized) return;
    for (int i = 0; i < LE_STATUS_NUM_ROWS; ++i) {
        if (status[i].type == PARCOUNT) {
            destroy_partitioned_counter(status[i].value.parcount);
        }
    }
}



CHECKPOINT_STATUS_S cp_status;
void CHECKPOINT_STATUS_S::init(void) {
    if (m_initialized) return;
#define CP_STATUS_INIT(k,c,t,l,inc) TOKUFT_STATUS_INIT((*this), k, c, t, "checkpoint: " l, inc)
    CP_STATUS_INIT(CP_PERIOD,                              CHECKPOINT_PERIOD, UINT64,   "period", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CP_STATUS_INIT(CP_FOOTPRINT,                           nullptr, UINT64,   "footprint", TOKU_ENGINE_STATUS);
    CP_STATUS_INIT(CP_TIME_LAST_CHECKPOINT_BEGIN,          CHECKPOINT_LAST_BEGAN, UNIXTIME, "last checkpoint began ", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CP_STATUS_INIT(CP_TIME_LAST_CHECKPOINT_BEGIN_COMPLETE, CHECKPOINT_LAST_COMPLETE_BEGAN, UNIXTIME, "last complete checkpoint began ", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CP_STATUS_INIT(CP_TIME_LAST_CHECKPOINT_END,            CHECKPOINT_LAST_COMPLETE_ENDED, UNIXTIME, "last complete checkpoint ended", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CP_STATUS_INIT(CP_TIME_CHECKPOINT_DURATION,            CHECKPOINT_DURATION, UINT64, "time spent during checkpoint (begin and end phases)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CP_STATUS_INIT(CP_TIME_CHECKPOINT_DURATION_LAST,       CHECKPOINT_DURATION_LAST, UINT64, "time spent during last checkpoint (begin and end phases)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CP_STATUS_INIT(CP_LAST_LSN,                            nullptr, UINT64,   "last complete checkpoint LSN", TOKU_ENGINE_STATUS);
    CP_STATUS_INIT(CP_CHECKPOINT_COUNT,                    CHECKPOINT_TAKEN, UINT64,   "checkpoints taken ", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CP_STATUS_INIT(CP_CHECKPOINT_COUNT_FAIL,               CHECKPOINT_FAILED, UINT64,   "checkpoints failed", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CP_STATUS_INIT(CP_WAITERS_NOW,                         nullptr, UINT64,   "waiters now", TOKU_ENGINE_STATUS);
    CP_STATUS_INIT(CP_WAITERS_MAX,                         nullptr, UINT64,   "waiters max", TOKU_ENGINE_STATUS);
    CP_STATUS_INIT(CP_CLIENT_WAIT_ON_MO,                   nullptr, UINT64,   "non-checkpoint client wait on mo lock", TOKU_ENGINE_STATUS);
    CP_STATUS_INIT(CP_CLIENT_WAIT_ON_CS,                   nullptr, UINT64,   "non-checkpoint client wait on cs lock", TOKU_ENGINE_STATUS);

    CP_STATUS_INIT(CP_BEGIN_TIME,                          CHECKPOINT_BEGIN_TIME, UINT64,   "checkpoint begin time", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CP_STATUS_INIT(CP_LONG_BEGIN_COUNT,                    CHECKPOINT_LONG_BEGIN_COUNT, UINT64,   "long checkpoint begin count", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CP_STATUS_INIT(CP_LONG_BEGIN_TIME,                     CHECKPOINT_LONG_BEGIN_TIME, UINT64,   "long checkpoint begin time", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    m_initialized = true;
#undef CP_STATUS_INIT
}
void CHECKPOINT_STATUS_S::destroy() {
    if (!m_initialized) return;
    for (int i = 0; i < CP_STATUS_NUM_ROWS; ++i) {
        if (status[i].type == PARCOUNT) {
            destroy_partitioned_counter(status[i].value.parcount);
        }
    }
}

CACHETABLE_STATUS_S ct_status;
void CACHETABLE_STATUS_S::init() {
    if (m_initialized) return;
#define CT_STATUS_INIT(k,c,t,l,inc) TOKUFT_STATUS_INIT((*this), k, c, t, "cachetable: " l, inc)
    CT_STATUS_INIT(CT_MISS,                   CACHETABLE_MISS, UINT64, "miss", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_MISSTIME,               CACHETABLE_MISS_TIME, UINT64, "miss time", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_PREFETCHES,             CACHETABLE_PREFETCHES, UINT64, "prefetches", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_SIZE_CURRENT,           CACHETABLE_SIZE_CURRENT, UINT64, "size current", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_SIZE_LIMIT,             CACHETABLE_SIZE_LIMIT, UINT64, "size limit", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_SIZE_WRITING,           CACHETABLE_SIZE_WRITING, UINT64, "size writing", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_SIZE_NONLEAF,           CACHETABLE_SIZE_NONLEAF, UINT64, "size nonleaf", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_SIZE_LEAF,              CACHETABLE_SIZE_LEAF, UINT64, "size leaf", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_SIZE_ROLLBACK,          CACHETABLE_SIZE_ROLLBACK, UINT64, "size rollback", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_SIZE_CACHEPRESSURE,     CACHETABLE_SIZE_CACHEPRESSURE, UINT64, "size cachepressure", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_SIZE_CLONED,            CACHETABLE_SIZE_CLONED, UINT64, "size currently cloned data for checkpoint", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_EVICTIONS,              CACHETABLE_EVICTIONS, UINT64, "evictions", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_CLEANER_EXECUTIONS,     CACHETABLE_CLEANER_EXECUTIONS, UINT64, "cleaner executions", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_CLEANER_PERIOD,         CACHETABLE_CLEANER_PERIOD, UINT64, "cleaner period", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_CLEANER_ITERATIONS,     CACHETABLE_CLEANER_ITERATIONS, UINT64, "cleaner iterations", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    
    CT_STATUS_INIT(CT_WAIT_PRESSURE_COUNT,    CACHETABLE_WAIT_PRESSURE_COUNT, UINT64, "number of waits on cache pressure", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_WAIT_PRESSURE_TIME,    CACHETABLE_WAIT_PRESSURE_TIME, UINT64, "time waiting on cache pressure", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_LONG_WAIT_PRESSURE_COUNT,    CACHETABLE_LONG_WAIT_PRESSURE_COUNT, UINT64, "number of long waits on cache pressure", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    CT_STATUS_INIT(CT_LONG_WAIT_PRESSURE_TIME,    CACHETABLE_LONG_WAIT_PRESSURE_TIME, UINT64, "long time waiting on cache pressure", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    m_initialized = true;

#undef CT_STATUS_INIT
}
void CACHETABLE_STATUS_S::destroy() {
    if (!m_initialized) return;
    for (int i = 0; i < CT_STATUS_NUM_ROWS; ++i) {
        if (status[i].type == PARCOUNT) {
            destroy_partitioned_counter(status[i].value.parcount);
        }
    }
}



LTM_STATUS_S ltm_status;
void LTM_STATUS_S::init() {
    if (m_initialized) return;
#define LTM_STATUS_INIT(k,c,t,l,inc) TOKUFT_STATUS_INIT((*this), k, c, t, "locktree: " l, inc)
    LTM_STATUS_INIT(LTM_SIZE_CURRENT,             LOCKTREE_MEMORY_SIZE, UINT64,   "memory size", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_SIZE_LIMIT,               LOCKTREE_MEMORY_SIZE_LIMIT, UINT64,   "memory size limit", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_ESCALATION_COUNT,         LOCKTREE_ESCALATION_NUM, UINT64, "number of times lock escalation ran", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_ESCALATION_TIME,          LOCKTREE_ESCALATION_SECONDS, TOKUTIME, "time spent running escalation (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_ESCALATION_LATEST_RESULT, LOCKTREE_LATEST_POST_ESCALATION_MEMORY_SIZE, UINT64,   "latest post-escalation memory size", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_NUM_LOCKTREES,            LOCKTREE_OPEN_CURRENT, UINT64,   "number of locktrees open now", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_LOCK_REQUESTS_PENDING,    LOCKTREE_PENDING_LOCK_REQUESTS, UINT64,   "number of pending lock requests", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_STO_NUM_ELIGIBLE,         LOCKTREE_STO_ELIGIBLE_NUM, UINT64,   "number of locktrees eligible for the STO", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_STO_END_EARLY_COUNT,      LOCKTREE_STO_ENDED_NUM, UINT64,   "number of times a locktree ended the STO early", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_STO_END_EARLY_TIME,       LOCKTREE_STO_ENDED_SECONDS, TOKUTIME, "time spent ending the STO early (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    LTM_STATUS_INIT(LTM_WAIT_COUNT,               LOCKTREE_WAIT_COUNT, UINT64, "number of wait locks", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_WAIT_TIME,                LOCKTREE_WAIT_TIME, UINT64, "time waiting for locks", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_LONG_WAIT_COUNT,          LOCKTREE_LONG_WAIT_COUNT, UINT64, "number of long wait locks", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_LONG_WAIT_TIME,           LOCKTREE_LONG_WAIT_TIME, UINT64, "long time waiting for locks", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_TIMEOUT_COUNT,            LOCKTREE_TIMEOUT_COUNT, UINT64, "number of lock timeouts", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    LTM_STATUS_INIT(LTM_WAIT_ESCALATION_COUNT,    LOCKTREE_WAIT_ESCALATION_COUNT, UINT64, "number of waits on lock escalation", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_WAIT_ESCALATION_TIME,     LOCKTREE_WAIT_ESCALATION_TIME, UINT64, "time waiting on lock escalation", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_LONG_WAIT_ESCALATION_COUNT,    LOCKTREE_LONG_WAIT_ESCALATION_COUNT, UINT64, "number of long waits on lock escalation", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LTM_STATUS_INIT(LTM_LONG_WAIT_ESCALATION_TIME,     LOCKTREE_LONG_WAIT_ESCALATION_TIME, UINT64, "long time waiting on lock escalation", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    m_initialized = true;
#undef LTM_STATUS_INIT
}
void LTM_STATUS_S::destroy() {
    if (!m_initialized) return;
    for (int i = 0; i < LTM_STATUS_NUM_ROWS; ++i) {
        if (status[i].type == PARCOUNT) {
            destroy_partitioned_counter(status[i].value.parcount);
        }
    }
}



FT_STATUS_S ft_status;
void FT_STATUS_S::init() {
    if (m_initialized) return;
#define FT_STATUS_INIT(k,c,t,l,inc) TOKUFT_STATUS_INIT((*this), k, c, t, "ft: " l, inc)
    FT_STATUS_INIT(FT_UPDATES,                                DICTIONARY_UPDATES, PARCOUNT, "dictionary updates", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_UPDATES_BROADCAST,                      DICTIONARY_BROADCAST_UPDATES, PARCOUNT, "dictionary broadcast updates", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DESCRIPTOR_SET,                         DESCRIPTOR_SET, PARCOUNT, "descriptor set", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_MSN_DISCARDS,                           MESSAGES_IGNORED_BY_LEAF_DUE_TO_MSN, PARCOUNT, "messages ignored by leaf due to msn", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_TOTAL_RETRIES,                          nullptr, PARCOUNT, "total search retries due to TRY_AGAIN", TOKU_ENGINE_STATUS);
    FT_STATUS_INIT(FT_SEARCH_TRIES_GT_HEIGHT,                 nullptr, PARCOUNT, "searches requiring more tries than the height of the tree", TOKU_ENGINE_STATUS);
    FT_STATUS_INIT(FT_SEARCH_TRIES_GT_HEIGHTPLUS3,            nullptr, PARCOUNT, "searches requiring more tries than the height of the tree plus three", TOKU_ENGINE_STATUS);
    FT_STATUS_INIT(FT_CREATE_LEAF,                            LEAF_NODES_CREATED, PARCOUNT, "leaf nodes created", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_CREATE_NONLEAF,                         NONLEAF_NODES_CREATED, PARCOUNT, "nonleaf nodes created", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DESTROY_LEAF,                           LEAF_NODES_DESTROYED, PARCOUNT, "leaf nodes destroyed", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DESTROY_NONLEAF,                        NONLEAF_NODES_DESTROYED, PARCOUNT, "nonleaf nodes destroyed", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_MSG_BYTES_IN,                           MESSAGES_INJECTED_AT_ROOT_BYTES, PARCOUNT, "bytes of messages injected at root (all trees)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_MSG_BYTES_OUT,                          MESSAGES_FLUSHED_FROM_H1_TO_LEAVES_BYTES, PARCOUNT, "bytes of messages flushed from h1 nodes to leaves", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_MSG_BYTES_CURR,                         MESSAGES_IN_TREES_ESTIMATE_BYTES, PARCOUNT, "bytes of messages currently in trees (estimate)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_MSG_NUM,                                MESSAGES_INJECTED_AT_ROOT, PARCOUNT, "messages injected at root", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_MSG_NUM_BROADCAST,                      BROADCASE_MESSAGES_INJECTED_AT_ROOT, PARCOUNT, "broadcast messages injected at root", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    FT_STATUS_INIT(FT_NUM_BASEMENTS_DECOMPRESSED_NORMAL,      BASEMENTS_DECOMPRESSED_TARGET_QUERY, PARCOUNT, "basements decompressed as a target of a query", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NUM_BASEMENTS_DECOMPRESSED_AGGRESSIVE,  BASEMENTS_DECOMPRESSED_PRELOCKED_RANGE, PARCOUNT, "basements decompressed for prelocked range", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NUM_BASEMENTS_DECOMPRESSED_PREFETCH,    BASEMENTS_DECOMPRESSED_PREFETCH, PARCOUNT, "basements decompressed for prefetch", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NUM_BASEMENTS_DECOMPRESSED_WRITE,       BASEMENTS_DECOMPRESSED_FOR_WRITE, PARCOUNT, "basements decompressed for write", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NUM_MSG_BUFFER_DECOMPRESSED_NORMAL,     BUFFERS_DECOMPRESSED_TARGET_QUERY, PARCOUNT, "buffers decompressed as a target of a query", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NUM_MSG_BUFFER_DECOMPRESSED_AGGRESSIVE, BUFFERS_DECOMPRESSED_PRELOCKED_RANGE, PARCOUNT, "buffers decompressed for prelocked range", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NUM_MSG_BUFFER_DECOMPRESSED_PREFETCH,   BUFFERS_DECOMPRESSED_PREFETCH, PARCOUNT, "buffers decompressed for prefetch", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NUM_MSG_BUFFER_DECOMPRESSED_WRITE,      BUFFERS_DECOMPRESSED_FOR_WRITE, PARCOUNT, "buffers decompressed for write", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    // Eviction statistics:
    FT_STATUS_INIT(FT_FULL_EVICTIONS_LEAF,                    LEAF_NODE_FULL_EVICTIONS, PARCOUNT, "leaf node full evictions", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_FULL_EVICTIONS_LEAF_BYTES,              LEAF_NODE_FULL_EVICTIONS_BYTES, PARCOUNT, "leaf node full evictions (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_FULL_EVICTIONS_NONLEAF,                 NONLEAF_NODE_FULL_EVICTIONS, PARCOUNT, "nonleaf node full evictions", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_FULL_EVICTIONS_NONLEAF_BYTES,           NONLEAF_NODE_FULL_EVICTIONS_BYTES, PARCOUNT, "nonleaf node full evictions (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PARTIAL_EVICTIONS_LEAF,                 LEAF_NODE_PARTIAL_EVICTIONS, PARCOUNT, "leaf node partial evictions", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PARTIAL_EVICTIONS_LEAF_BYTES,           LEAF_NODE_PARTIAL_EVICTIONS_BYTES, PARCOUNT, "leaf node partial evictions (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PARTIAL_EVICTIONS_NONLEAF,              NONLEAF_NODE_PARTIAL_EVICTIONS, PARCOUNT, "nonleaf node partial evictions", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PARTIAL_EVICTIONS_NONLEAF_BYTES,        NONLEAF_NODE_PARTIAL_EVICTIONS_BYTES, PARCOUNT, "nonleaf node partial evictions (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    // Disk read statistics: 
    //
    // Pivots: For queries, prefetching, or writing.
    FT_STATUS_INIT(FT_NUM_PIVOTS_FETCHED_QUERY,               PIVOTS_FETCHED_FOR_QUERY, PARCOUNT, "pivots fetched for query", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_BYTES_PIVOTS_FETCHED_QUERY,             PIVOTS_FETCHED_FOR_QUERY_BYTES, PARCOUNT, "pivots fetched for query (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_TOKUTIME_PIVOTS_FETCHED_QUERY,          PIVOTS_FETCHED_FOR_QUERY_SECONDS, TOKUTIME, "pivots fetched for query (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NUM_PIVOTS_FETCHED_PREFETCH,            PIVOTS_FETCHED_FOR_PREFETCH, PARCOUNT, "pivots fetched for prefetch", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_BYTES_PIVOTS_FETCHED_PREFETCH,          PIVOTS_FETCHED_FOR_PREFETCH_BYTES, PARCOUNT, "pivots fetched for prefetch (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_TOKUTIME_PIVOTS_FETCHED_PREFETCH,       PIVOTS_FETCHED_FOR_PREFETCH_SECONDS, TOKUTIME, "pivots fetched for prefetch (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NUM_PIVOTS_FETCHED_WRITE,               PIVOTS_FETCHED_FOR_WRITE, PARCOUNT, "pivots fetched for write", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_BYTES_PIVOTS_FETCHED_WRITE,             PIVOTS_FETCHED_FOR_WRITE_BYTES, PARCOUNT, "pivots fetched for write (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_TOKUTIME_PIVOTS_FETCHED_WRITE,          PIVOTS_FETCHED_FOR_WRITE_SECONDS, TOKUTIME, "pivots fetched for write (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    // Basements: For queries, aggressive fetching in prelocked range, prefetching, or writing.
    FT_STATUS_INIT(FT_NUM_BASEMENTS_FETCHED_NORMAL,           BASEMENTS_FETCHED_TARGET_QUERY, PARCOUNT, "basements fetched as a target of a query", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_BYTES_BASEMENTS_FETCHED_NORMAL,         BASEMENTS_FETCHED_TARGET_QUERY_BYTES, PARCOUNT, "basements fetched as a target of a query (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_TOKUTIME_BASEMENTS_FETCHED_NORMAL,      BASEMENTS_FETCHED_TARGET_QUERY_SECONDS, TOKUTIME, "basements fetched as a target of a query (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NUM_BASEMENTS_FETCHED_AGGRESSIVE,       BASEMENTS_FETCHED_PRELOCKED_RANGE, PARCOUNT, "basements fetched for prelocked range", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_BYTES_BASEMENTS_FETCHED_AGGRESSIVE,     BASEMENTS_FETCHED_PRELOCKED_RANGE_BYTES, PARCOUNT, "basements fetched for prelocked range (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_TOKUTIME_BASEMENTS_FETCHED_AGGRESSIVE,  BASEMENTS_FETCHED_PRELOCKED_RANGE_SECONDS, TOKUTIME, "basements fetched for prelocked range (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NUM_BASEMENTS_FETCHED_PREFETCH,         BASEMENTS_FETCHED_PREFETCH, PARCOUNT, "basements fetched for prefetch", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_BYTES_BASEMENTS_FETCHED_PREFETCH,       BASEMENTS_FETCHED_PREFETCH_BYTES, PARCOUNT, "basements fetched for prefetch (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_TOKUTIME_BASEMENTS_FETCHED_PREFETCH,    BASEMENTS_FETCHED_PREFETCH_SECONDS, TOKUTIME, "basements fetched for prefetch (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NUM_BASEMENTS_FETCHED_WRITE,            BASEMENTS_FETCHED_FOR_WRITE, PARCOUNT, "basements fetched for write", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_BYTES_BASEMENTS_FETCHED_WRITE,          BASEMENTS_FETCHED_FOR_WRITE_BYTES, PARCOUNT, "basements fetched for write (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_TOKUTIME_BASEMENTS_FETCHED_WRITE,       BASEMENTS_FETCHED_FOR_WRITE_SECONDS, TOKUTIME, "basements fetched for write (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    // Buffers: For queries, aggressive fetching in prelocked range, prefetching, or writing.
    FT_STATUS_INIT(FT_NUM_MSG_BUFFER_FETCHED_NORMAL,          BUFFERS_FETCHED_TARGET_QUERY, PARCOUNT, "buffers fetched as a target of a query", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_BYTES_MSG_BUFFER_FETCHED_NORMAL,        BUFFERS_FETCHED_TARGET_QUERY_BYTES, PARCOUNT, "buffers fetched as a target of a query (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_TOKUTIME_MSG_BUFFER_FETCHED_NORMAL,     BUFFERS_FETCHED_TARGET_QUERY_SECONDS, TOKUTIME, "buffers fetched as a target of a query (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NUM_MSG_BUFFER_FETCHED_AGGRESSIVE,      BUFFERS_FETCHED_PRELOCKED_RANGE, PARCOUNT, "buffers fetched for prelocked range", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_BYTES_MSG_BUFFER_FETCHED_AGGRESSIVE,    BUFFERS_FETCHED_PRELOCKED_RANGE_BYTES, PARCOUNT, "buffers fetched for prelocked range (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_TOKUTIME_MSG_BUFFER_FETCHED_AGGRESSIVE, BUFFERS_FETCHED_PRELOCKED_RANGE_SECONDS, TOKUTIME, "buffers fetched for prelocked range (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NUM_MSG_BUFFER_FETCHED_PREFETCH,        BUFFERS_FETCHED_PREFETCH, PARCOUNT, "buffers fetched for prefetch", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_BYTES_MSG_BUFFER_FETCHED_PREFETCH,      BUFFERS_FETCHED_PREFETCH_BYTES, PARCOUNT, "buffers fetched for prefetch (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_TOKUTIME_MSG_BUFFER_FETCHED_PREFETCH,   BUFFERS_FETCHED_PREFETCH_SECONDS, TOKUTIME, "buffers fetched for prefetch (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NUM_MSG_BUFFER_FETCHED_WRITE,           BUFFERS_FETCHED_FOR_WRITE, PARCOUNT, "buffers fetched for write", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_BYTES_MSG_BUFFER_FETCHED_WRITE,         BUFFERS_FETCHED_FOR_WRITE_BYTES, PARCOUNT, "buffers fetched for write (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_TOKUTIME_MSG_BUFFER_FETCHED_WRITE,      BUFFERS_FETCHED_FOR_WRITE_SECONDS, TOKUTIME, "buffers fetched for write (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    // Disk write statistics.
    //
    // Leaf/Nonleaf: Not for checkpoint
    FT_STATUS_INIT(FT_DISK_FLUSH_LEAF,                                         LEAF_NODES_FLUSHED_NOT_CHECKPOINT, PARCOUNT, "leaf nodes flushed to disk (not for checkpoint)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_LEAF_BYTES,                                   LEAF_NODES_FLUSHED_NOT_CHECKPOINT_BYTES, PARCOUNT, "leaf nodes flushed to disk (not for checkpoint) (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_LEAF_UNCOMPRESSED_BYTES,                      LEAF_NODES_FLUSHED_NOT_CHECKPOINT_UNCOMPRESSED_BYTES, PARCOUNT, "leaf nodes flushed to disk (not for checkpoint) (uncompressed bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_LEAF_TOKUTIME,                                LEAF_NODES_FLUSHED_NOT_CHECKPOINT_SECONDS, TOKUTIME, "leaf nodes flushed to disk (not for checkpoint) (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_NONLEAF,                                      NONLEAF_NODES_FLUSHED_TO_DISK_NOT_CHECKPOINT, PARCOUNT, "nonleaf nodes flushed to disk (not for checkpoint)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_NONLEAF_BYTES,                                NONLEAF_NODES_FLUSHED_TO_DISK_NOT_CHECKPOINT_BYTES, PARCOUNT, "nonleaf nodes flushed to disk (not for checkpoint) (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_NONLEAF_UNCOMPRESSED_BYTES,                   NONLEAF_NODES_FLUSHED_TO_DISK_NOT_CHECKPOINT_UNCOMPRESSED_BYTES, PARCOUNT, "nonleaf nodes flushed to disk (not for checkpoint) (uncompressed bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_NONLEAF_TOKUTIME,                             NONLEAF_NODES_FLUSHED_TO_DISK_NOT_CHECKPOINT_SECONDS, TOKUTIME, "nonleaf nodes flushed to disk (not for checkpoint) (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    // Leaf/Nonleaf: For checkpoint
    FT_STATUS_INIT(FT_DISK_FLUSH_LEAF_FOR_CHECKPOINT,                          LEAF_NODES_FLUSHED_CHECKPOINT, PARCOUNT, "leaf nodes flushed to disk (for checkpoint)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_LEAF_BYTES_FOR_CHECKPOINT,                    LEAF_NODES_FLUSHED_CHECKPOINT_BYTES, PARCOUNT, "leaf nodes flushed to disk (for checkpoint) (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_LEAF_UNCOMPRESSED_BYTES_FOR_CHECKPOINT,       LEAF_NODES_FLUSHED_CHECKPOINT_UNCOMPRESSED_BYTES, PARCOUNT, "leaf nodes flushed to disk (for checkpoint) (uncompressed bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_LEAF_TOKUTIME_FOR_CHECKPOINT,                 LEAF_NODES_FLUSHED_CHECKPOINT_SECONDS, TOKUTIME, "leaf nodes flushed to disk (for checkpoint) (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_NONLEAF_FOR_CHECKPOINT,                       NONLEAF_NODES_FLUSHED_TO_DISK_CHECKPOINT, PARCOUNT, "nonleaf nodes flushed to disk (for checkpoint)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_NONLEAF_BYTES_FOR_CHECKPOINT,                 NONLEAF_NODES_FLUSHED_TO_DISK_CHECKPOINT_BYTES, PARCOUNT, "nonleaf nodes flushed to disk (for checkpoint) (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_NONLEAF_UNCOMPRESSED_BYTES_FOR_CHECKPOINT,    NONLEAF_NODES_FLUSHED_TO_DISK_CHECKPOINT_UNCOMPRESSED_BYTES, PARCOUNT, "nonleaf nodes flushed to disk (for checkpoint) (uncompressed bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_NONLEAF_TOKUTIME_FOR_CHECKPOINT,              NONLEAF_NODES_FLUSHED_TO_DISK_CHECKPOINT_SECONDS, TOKUTIME, "nonleaf nodes flushed to disk (for checkpoint) (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_LEAF_COMPRESSION_RATIO,                       LEAF_NODE_COMPRESSION_RATIO, DOUBLE, "uncompressed / compressed bytes written (leaf)", TOKU_GLOBAL_STATUS|TOKU_ENGINE_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_NONLEAF_COMPRESSION_RATIO,                    NONLEAF_NODE_COMPRESSION_RATIO, DOUBLE, "uncompressed / compressed bytes written (nonleaf)", TOKU_GLOBAL_STATUS|TOKU_ENGINE_STATUS);
    FT_STATUS_INIT(FT_DISK_FLUSH_OVERALL_COMPRESSION_RATIO,                    OVERALL_NODE_COMPRESSION_RATIO, DOUBLE, "uncompressed / compressed bytes written (overall)", TOKU_GLOBAL_STATUS|TOKU_ENGINE_STATUS);

    // CPU time statistics for [de]serialization and [de]compression.
    FT_STATUS_INIT(FT_LEAF_COMPRESS_TOKUTIME,                                  LEAF_COMPRESSION_TO_MEMORY_SECONDS, TOKUTIME, "leaf compression to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_LEAF_SERIALIZE_TOKUTIME,                                 LEAF_SERIALIZATION_TO_MEMORY_SECONDS, TOKUTIME, "leaf serialization to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_LEAF_DECOMPRESS_TOKUTIME,                                LEAF_DECOMPRESSION_TO_MEMORY_SECONDS, TOKUTIME, "leaf decompression to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_LEAF_DESERIALIZE_TOKUTIME,                               LEAF_DESERIALIZATION_TO_MEMORY_SECONDS, TOKUTIME, "leaf deserialization to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NONLEAF_COMPRESS_TOKUTIME,                               NONLEAF_COMPRESSION_TO_MEMORY_SECONDS, TOKUTIME, "nonleaf compression to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NONLEAF_SERIALIZE_TOKUTIME,                              NONLEAF_SERIALIZATION_TO_MEMORY_SECONDS, TOKUTIME, "nonleaf serialization to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NONLEAF_DECOMPRESS_TOKUTIME,                             NONLEAF_DECOMPRESSION_TO_MEMORY_SECONDS, TOKUTIME, "nonleaf decompression to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_NONLEAF_DESERIALIZE_TOKUTIME,                            NONLEAF_DESERIALIZATION_TO_MEMORY_SECONDS, TOKUTIME, "nonleaf deserialization to memory (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    // Promotion statistics.
    FT_STATUS_INIT(FT_PRO_NUM_ROOT_SPLIT,                     PROMOTION_ROOTS_SPLIT, PARCOUNT, "promotion: roots split", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PRO_NUM_ROOT_H0_INJECT,                 PROMOTION_LEAF_ROOTS_INJECTED_INTO, PARCOUNT, "promotion: leaf roots injected into", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PRO_NUM_ROOT_H1_INJECT,                 PROMOTION_H1_ROOTS_INJECTED_INTO, PARCOUNT, "promotion: h1 roots injected into", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PRO_NUM_INJECT_DEPTH_0,                 PROMOTION_INJECTIONS_AT_DEPTH_0, PARCOUNT, "promotion: injections at depth 0", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PRO_NUM_INJECT_DEPTH_1,                 PROMOTION_INJECTIONS_AT_DEPTH_1, PARCOUNT, "promotion: injections at depth 1", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PRO_NUM_INJECT_DEPTH_2,                 PROMOTION_INJECTIONS_AT_DEPTH_2, PARCOUNT, "promotion: injections at depth 2", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PRO_NUM_INJECT_DEPTH_3,                 PROMOTION_INJECTIONS_AT_DEPTH_3, PARCOUNT, "promotion: injections at depth 3", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PRO_NUM_INJECT_DEPTH_GT3,               PROMOTION_INJECTIONS_LOWER_THAN_DEPTH_3, PARCOUNT, "promotion: injections lower than depth 3", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PRO_NUM_STOP_NONEMPTY_BUF,              PROMOTION_STOPPED_NONEMPTY_BUFFER, PARCOUNT, "promotion: stopped because of a nonempty buffer", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PRO_NUM_STOP_H1,                        PROMOTION_STOPPED_AT_HEIGHT_1, PARCOUNT, "promotion: stopped at height 1", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PRO_NUM_STOP_LOCK_CHILD,                PROMOTION_STOPPED_CHILD_LOCKED_OR_NOT_IN_MEMORY, PARCOUNT, "promotion: stopped because the child was locked or not at all in memory", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PRO_NUM_STOP_CHILD_INMEM,               PROMOTION_STOPPED_CHILD_NOT_FULLY_IN_MEMORY, PARCOUNT, "promotion: stopped because the child was not fully in memory", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PRO_NUM_DIDNT_WANT_PROMOTE,             PROMOTION_STOPPED_AFTER_LOCKING_CHILD, PARCOUNT, "promotion: stopped anyway, after locking the child", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_BASEMENT_DESERIALIZE_FIXED_KEYSIZE,     BASEMENT_DESERIALIZATION_FIXED_KEY, PARCOUNT, "basement nodes deserialized with fixed-keysize", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_BASEMENT_DESERIALIZE_VARIABLE_KEYSIZE,  BASEMENT_DESERIALIZATION_VARIABLE_KEY, PARCOUNT, "basement nodes deserialized with variable-keysize", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FT_STATUS_INIT(FT_PRO_RIGHTMOST_LEAF_SHORTCUT_SUCCESS,    nullptr, PARCOUNT, "promotion: succeeded in using the rightmost leaf shortcut", TOKU_ENGINE_STATUS);
    FT_STATUS_INIT(FT_PRO_RIGHTMOST_LEAF_SHORTCUT_FAIL_POS,   nullptr, PARCOUNT, "promotion: tried the rightmost leaf shorcut but failed (out-of-bounds)", TOKU_ENGINE_STATUS);
    FT_STATUS_INIT(FT_PRO_RIGHTMOST_LEAF_SHORTCUT_FAIL_REACTIVE,nullptr, PARCOUNT, "promotion: tried the rightmost leaf shorcut but failed (child reactive)", TOKU_ENGINE_STATUS);

    FT_STATUS_INIT(FT_CURSOR_SKIP_DELETED_LEAF_ENTRY,         CURSOR_SKIP_DELETED_LEAF_ENTRY, PARCOUNT, "cursor skipped deleted leaf entries", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);

    m_initialized = true;
#undef FT_STATUS_INIT
}
void FT_STATUS_S::destroy() {
    if (!m_initialized) return;
    for (int i = 0; i < FT_STATUS_NUM_ROWS; ++i) {
        if (status[i].type == PARCOUNT) {
            destroy_partitioned_counter(status[i].value.parcount);
        }
    }
}



FT_FLUSHER_STATUS_S fl_status;
void FT_FLUSHER_STATUS_S::init() {
    if (m_initialized) return;
#define FL_STATUS_INIT(k,c,t,l,inc) TOKUFT_STATUS_INIT((*this), k, c, t, "ft flusher: " l, inc)
    FL_STATUS_INIT(FT_FLUSHER_CLEANER_TOTAL_NODES,                nullptr, UINT64, "total nodes potentially flushed by cleaner thread", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_CLEANER_H1_NODES,                   nullptr, UINT64, "height-one nodes flushed by cleaner thread", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_CLEANER_HGT1_NODES,                 nullptr, UINT64, "height-greater-than-one nodes flushed by cleaner thread", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_CLEANER_EMPTY_NODES,                nullptr, UINT64, "nodes cleaned which had empty buffers", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_CLEANER_NODES_DIRTIED,              nullptr, UINT64, "nodes dirtied by cleaner thread", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_CLEANER_MAX_BUFFER_SIZE,            nullptr, UINT64, "max bytes in a buffer flushed by cleaner thread", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_CLEANER_MIN_BUFFER_SIZE,            nullptr, UINT64, "min bytes in a buffer flushed by cleaner thread", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_CLEANER_TOTAL_BUFFER_SIZE,          nullptr, UINT64, "total bytes in buffers flushed by cleaner thread", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_CLEANER_MAX_BUFFER_WORKDONE,        nullptr, UINT64, "max workdone in a buffer flushed by cleaner thread", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_CLEANER_MIN_BUFFER_WORKDONE,        nullptr, UINT64, "min workdone in a buffer flushed by cleaner thread", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_CLEANER_TOTAL_BUFFER_WORKDONE,      nullptr, UINT64, "total workdone in buffers flushed by cleaner thread", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_CLEANER_NUM_LEAF_MERGES_STARTED,    nullptr, UINT64, "times cleaner thread tries to merge a leaf", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_CLEANER_NUM_LEAF_MERGES_RUNNING,    nullptr, UINT64, "cleaner thread leaf merges in progress", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_CLEANER_NUM_LEAF_MERGES_COMPLETED,  nullptr, UINT64, "cleaner thread leaf merges successful", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_CLEANER_NUM_DIRTIED_FOR_LEAF_MERGE, nullptr, UINT64, "nodes dirtied by cleaner thread leaf merges", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_FLUSH_TOTAL,                        nullptr, UINT64, "total number of flushes done by flusher threads or cleaner threads", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_FLUSH_IN_MEMORY,                    nullptr, UINT64, "number of in memory flushes", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_FLUSH_NEEDED_IO,                    nullptr, UINT64, "number of flushes that read something off disk", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_FLUSH_CASCADES,                     nullptr, UINT64, "number of flushes that triggered another flush in child", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_FLUSH_CASCADES_1,                   nullptr, UINT64, "number of flushes that triggered 1 cascading flush", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_FLUSH_CASCADES_2,                   nullptr, UINT64, "number of flushes that triggered 2 cascading flushes", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_FLUSH_CASCADES_3,                   nullptr, UINT64, "number of flushes that triggered 3 cascading flushes", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_FLUSH_CASCADES_4,                   nullptr, UINT64, "number of flushes that triggered 4 cascading flushes", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_FLUSH_CASCADES_5,                   nullptr, UINT64, "number of flushes that triggered 5 cascading flushes", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_FLUSH_CASCADES_GT_5,                nullptr, UINT64, "number of flushes that triggered over 5 cascading flushes", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_SPLIT_LEAF,                         nullptr, UINT64, "leaf node splits", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_SPLIT_NONLEAF,                      nullptr, UINT64, "nonleaf node splits", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_MERGE_LEAF,                         nullptr, UINT64, "leaf node merges", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_MERGE_NONLEAF,                      nullptr, UINT64, "nonleaf node merges", TOKU_ENGINE_STATUS);
    FL_STATUS_INIT(FT_FLUSHER_BALANCE_LEAF,                       nullptr, UINT64, "leaf node balances", TOKU_ENGINE_STATUS);

    FL_STATUS_VAL(FT_FLUSHER_CLEANER_MIN_BUFFER_SIZE) = UINT64_MAX;
    FL_STATUS_VAL(FT_FLUSHER_CLEANER_MIN_BUFFER_WORKDONE) = UINT64_MAX;

    m_initialized = true;
#undef FL_STATUS_INIT
}
void FT_FLUSHER_STATUS_S::destroy() {
    if (!m_initialized) return;
    for (int i = 0; i < FT_FLUSHER_STATUS_NUM_ROWS; ++i) {
        if (status[i].type == PARCOUNT) {
            destroy_partitioned_counter(status[i].value.parcount);
        }
    }
}



FT_HOT_STATUS_S hot_status;
void FT_HOT_STATUS_S::init() {
    if (m_initialized) return;
#define HOT_STATUS_INIT(k,c,t,l,inc) TOKUFT_STATUS_INIT((*this), k, c, t, "hot: " l, inc)
    HOT_STATUS_INIT(FT_HOT_NUM_STARTED,          nullptr, UINT64, "operations ever started", TOKU_ENGINE_STATUS);
    HOT_STATUS_INIT(FT_HOT_NUM_COMPLETED,        nullptr, UINT64, "operations successfully completed", TOKU_ENGINE_STATUS);
    HOT_STATUS_INIT(FT_HOT_NUM_ABORTED,          nullptr, UINT64, "operations aborted", TOKU_ENGINE_STATUS);
    HOT_STATUS_INIT(FT_HOT_MAX_ROOT_FLUSH_COUNT, nullptr, UINT64, "max number of flushes from root ever required to optimize a tree", TOKU_ENGINE_STATUS);

    m_initialized = true;
#undef HOT_STATUS_INIT
}
void FT_HOT_STATUS_S::destroy() {
    if (!m_initialized) return;
    for (int i = 0; i < FT_HOT_STATUS_NUM_ROWS; ++i) {
        if (status[i].type == PARCOUNT) {
            destroy_partitioned_counter(status[i].value.parcount);
        }
    }
}



TXN_STATUS_S txn_status;
void TXN_STATUS_S::init() {
    if (m_initialized) return;
#define TXN_STATUS_INIT(k,c,t,l,inc) TOKUFT_STATUS_INIT((*this), k, c, t, "txn: " l, inc)
    TXN_STATUS_INIT(TXN_BEGIN,            TXN_BEGIN, PARCOUNT,   "begin", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    TXN_STATUS_INIT(TXN_READ_BEGIN,       TXN_BEGIN_READ_ONLY, PARCOUNT,   "begin read only", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    TXN_STATUS_INIT(TXN_COMMIT,           TXN_COMMITS, PARCOUNT,   "successful commits", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    TXN_STATUS_INIT(TXN_ABORT,            TXN_ABORTS, PARCOUNT,   "aborts", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    m_initialized = true;
#undef TXN_STATUS_INIT
}
void TXN_STATUS_S::destroy() {
    if (!m_initialized) return;
    for (int i = 0; i < TXN_STATUS_NUM_ROWS; ++i) {
        if (status[i].type == PARCOUNT) {
            destroy_partitioned_counter(status[i].value.parcount);
        }
    }
}


LOGGER_STATUS_S log_status;
void LOGGER_STATUS_S::init() {
    if (m_initialized) return;
#define LOG_STATUS_INIT(k,c,t,l,inc) TOKUFT_STATUS_INIT((*this), k, c, t, "logger: " l, inc)
    LOG_STATUS_INIT(LOGGER_NEXT_LSN,     nullptr, UINT64,  "next LSN", TOKU_ENGINE_STATUS);
    LOG_STATUS_INIT(LOGGER_NUM_WRITES,                  LOGGER_WRITES, UINT64, "writes", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LOG_STATUS_INIT(LOGGER_BYTES_WRITTEN,               LOGGER_WRITES_BYTES, UINT64, "writes (bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LOG_STATUS_INIT(LOGGER_UNCOMPRESSED_BYTES_WRITTEN,  LOGGER_WRITES_UNCOMPRESSED_BYTES, UINT64, "writes (uncompressed bytes)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LOG_STATUS_INIT(LOGGER_TOKUTIME_WRITES,             LOGGER_WRITES_SECONDS, TOKUTIME, "writes (seconds)", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    LOG_STATUS_INIT(LOGGER_WAIT_BUF_LONG,               LOGGER_WAIT_LONG, UINT64, "number of long logger write operations", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    m_initialized = true;
#undef LOG_STATUS_INIT
}
void LOGGER_STATUS_S::destroy() {
    if (!m_initialized) return;
    for (int i = 0; i < LOGGER_STATUS_NUM_ROWS; ++i) {
        if (status[i].type == PARCOUNT) {
            destroy_partitioned_counter(status[i].value.parcount);
        }
    }
}

void toku_status_init(void) {
    le_status.init();
    cp_status.init();
    ltm_status.init();
    ft_status.init();
    fl_status.init();
    hot_status.init();
    txn_status.init();
    log_status.init();
}
void toku_status_destroy(void) {
    log_status.destroy();
    txn_status.destroy();
    hot_status.destroy();
    fl_status.destroy();
    ft_status.destroy();
    ltm_status.destroy();
    cp_status.destroy();
    le_status.destroy();
}
