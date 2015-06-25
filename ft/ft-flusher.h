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

#pragma once

#ident "Copyright (c) 2007-2013 Tokutek Inc.  All rights reserved."
#ident "The technology is licensed by the Massachusetts Institute of Technology, Rutgers State University of New Jersey, and the Research Foundation of State University of New York at Stony Brook under United States of America Serial No. 11/760379 and to the patents and/or patent applications resulting from it."

#include "ft/ft-internal.h"

void toku_ft_flusher_get_status(FT_FLUSHER_STATUS);

/**
 * Only for testing, not for production.
 *
 * Set a callback the flusher thread will use to signal various points
 * during its execution.
 */
void
toku_flusher_thread_set_callback(
    void (*callback_f)(int, void*),
    void* extra
    );

/**
 * Puts a workitem on the flusher thread queue, scheduling the node to be
 * flushed by toku_ft_flush_some_child.
 */
void toku_ft_flush_node_on_background_thread(FT ft, FTNODE parent);

enum split_mode {
    SPLIT_EVENLY,
    SPLIT_LEFT_HEAVY,
    SPLIT_RIGHT_HEAVY
};


// Given pinned node and pinned child, split child into two
// and update node with information about its new child.
void toku_ft_split_child(
    FT ft,
    FTNODE node,
    int childnum,
    FTNODE child,
    enum split_mode split_mode
    );

// Given pinned node, merge childnum with a neighbor and update node with
// information about the change
void toku_ft_merge_child(
    FT ft,
    FTNODE node,
    int childnum
    );

/**
 * Effect: Split a leaf node.
 * Argument "node" is node to be split.
 * Upon return:
 *   nodea and nodeb point to new nodes that result from split of "node"
 *   nodea is the left node that results from the split
 *   splitk is the right-most key of nodea
 */
// TODO: Rename toku_ft_leaf_split
void
ftleaf_split(
    FT ft,
    FTNODE node,
    FTNODE *nodea,
    FTNODE *nodeb,
    DBT *splitk,
    bool create_new_node,
    enum split_mode split_mode,
    uint32_t num_dependent_nodes,
    FTNODE* dependent_nodes
    );

/**
 * Effect: node must be a node-leaf node.  It is split into two nodes, and
 *         the fanout is split between them.
 *    Sets splitk->data pointer to a malloc'd value
 *    Sets nodea, and nodeb to the two new nodes.
 *    The caller must replace the old node with the two new nodes.
 *    This function will definitely reduce the number of children for the node,
 *    but it does not guarantee that the resulting nodes are smaller than nodesize.
 */
void
// TODO: Rename toku_ft_nonleaf_split
ft_nonleaf_split(
    FT ft,
    FTNODE node,
    FTNODE *nodea,
    FTNODE *nodeb,
    DBT *splitk,
    uint32_t num_dependent_nodes,
    FTNODE* dependent_nodes
    );

/************************************************************************
 * HOT optimize, should perhaps be factored out to its own header file  *
 ************************************************************************
 */
void toku_ft_hot_get_status(FT_HOT_STATUS);

/**
 * Takes given FT and pushes all pending messages between left and right to the leaf nodes.
 * All messages between left and right (inclusive) will be pushed, as will some others
 * that happen to share buffers with messages near the boundary.
 * If left is NULL, messages from beginning of FT are pushed. If right is NULL, that means
 * we go until the end of the FT.
 */
int
toku_ft_hot_optimize(FT_HANDLE ft_h, DBT* left, DBT* right,
                     int (*progress_callback)(void *extra, float progress),
                     void *progress_extra, uint64_t* loops_run);
