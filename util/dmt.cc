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

#include <string.h>
#include <db.h>

#include <toku_include/memory.h>

namespace toku {

    //TODO: only start with this create.  Others can be delayed till after prototype
template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::create(void) {
    this->create_internal_no_array(supports_marks);
    //TODO: maybe allocate enough space for something by default?
    //      We may be relying on not needing to allocate space the first time (due to limited time spent while a lock is held)
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::create_no_array(void) {
    this->create_internal_no_array(supports_marks);
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::create_internal_no_array(bool as_tree) {
    toku_mempool_zero(&this->mp);
    if (as_tree) {
        this->is_array = false;
        this->value_length = 0;
        this->d.t.root.set_to_null();
    } else {
        paranoid_invariant(!supports_marks);
        this->is_array = true;
        this->d.a.start_idx = 0;
        this->d.a.num_values = 0;
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::create_from_sorted_array(const dmtdata_t *const values, const uint32_t numvalues) {
    static_assert(!false, "1st pass not done.  May need to change API. Not needed for prototype.");
    this->create_internal(numvalues);
    memcpy(this->d.a.values, values, numvalues * (sizeof values[0]));
    this->d.a.num_values = numvalues;
    if (supports_marks) {
        this->convert_to_tree();
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::create_internal(const uint32_t mempool_size) {
    static_assert(!false, "1st pass not done.  May need to change API. Not needed for prototype.");
#if 0
    this->create_internal_no_array(new_capacity, dynamic);
    if (!dynamic) {
        XMALLOC_N(this->capacity, this->d.a.values);
    }
#endif
}


template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::create_steal_sorted_array(dmtdata_t **const values, const uint32_t numvalues, const uint32_t new_capacity) {
    static_assert(!false, "1st pass not done.  May need to change API. Not needed for prototype.");
    paranoid_invariant_notnull(values);
    this->create_internal_no_array(new_capacity);
    this->d.a.num_values = numvalues;
    this->d.a.values = *values;
    *values = nullptr;
    if (supports_marks) {
        this->convert_to_tree();
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::split_at(dmt *const newdmt, const uint32_t idx) {
    static_assert(!false, "1st pass not done.  May need to change API. Not needed for prototype.");
    barf_if_marked(*this);
    paranoid_invariant_notnull(newdmt);
    if (idx > this->size()) { return EINVAL; }
    this->convert_to_array();
    const uint32_t newsize = this->size() - idx;
    newdmt->create_from_sorted_array(&this->d.a.values[this->d.a.start_idx + idx], newsize);
    this->d.a.num_values = idx;
    this->maybe_resize_array(idx);
    if (supports_marks) {
        this->convert_to_tree();
    }
    return 0;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::merge(dmt *const leftdmt, dmt *const rightdmt) {
    static_assert(!false, "1st pass not done.  May need to change API. Not needed for prototype.");
    barf_if_marked(*this);
    paranoid_invariant_notnull(leftdmt);
    paranoid_invariant_notnull(rightdmt);
    const uint32_t leftsize = leftdmt->size();
    const uint32_t rightsize = rightdmt->size();
    const uint32_t newsize = leftsize + rightsize;

    if (leftdmt->is_array) {
        if (leftdmt->capacity - (leftdmt->d.a.start_idx + leftdmt->d.a.num_values) >= rightsize) {
            this->create_steal_sorted_array(&leftdmt->d.a.values, leftdmt->d.a.num_values, leftdmt->capacity);
            this->d.a.start_idx = leftdmt->d.a.start_idx;
        } else {
            this->create_internal(newsize);
            memcpy(&this->d.a.values[0],
                   &leftdmt->d.a.values[leftdmt->d.a.start_idx],
                   leftdmt->d.a.num_values * (sizeof this->d.a.values[0]));
        }
    } else {
        this->create_internal(newsize);
        leftdmt->fill_array_with_subtree_values(&this->d.a.values[0], leftdmt->d.t.root);
    }
    leftdmt->destroy();
    this->d.a.num_values = leftsize;

    if (rightdmt->is_array) {
        memcpy(&this->d.a.values[this->d.a.start_idx + this->d.a.num_values],
               &rightdmt->d.a.values[rightdmt->d.a.start_idx],
               rightdmt->d.a.num_values * (sizeof this->d.a.values[0]));
    } else {
        rightdmt->fill_array_with_subtree_values(&this->d.a.values[this->d.a.start_idx + this->d.a.num_values],
                                                 rightdmt->d.t.root);
    }
    rightdmt->destroy();
    this->d.a.num_values += rightsize;
    paranoid_invariant(this->size() == newsize);
    if (supports_marks) {
        this->convert_to_tree();
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::clone(const dmt &src) {
    *this = src;
    toku_mempool_clone(&src.mp, &this->mp);
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::clear(void) {
    this->is_array = true;
    this->d.a.start_idx = 0;
    this->d.a.num_values = 0;
    this->values_same_size = true;  // Reset state  //TODO: determine if this is useful.
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::destroy(void) {
    this->clear();
    toku_mempool_destroy(&this->mp);
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
uint32_t dmt<dmtdata_t, dmtdataout_t, supports_marks>::size(void) const {
    if (this->is_array) {
        return this->d.a.num_values;
    } else {
        return this->nweight(this->d.t.root);
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
uint32_t dmt<dmtdata_t, dmtdataout_t, supports_marks>::nweight(const subtree &subtree) const {
    if (subtree.is_null()) {
        return 0;
    } else {
        const dmt_base_node & node = get_base_node(subtree);
        return node.weight;
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename dmtcmp_t, int (*h)(const dmtdata_t &, const dmtcmp_t &)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::insert(const dmtdatain_t &value, const dmtcmp_t &v, uint32_t *const idx) {
    int r;
    uint32_t insert_idx;

    r = this->find_zero<dmtcmp_t, h>(v, nullptr, &insert_idx);
    if (r==0) {
        if (idx) *idx = insert_idx;
        return DB_KEYEXIST;
    }
    if (r != DB_NOTFOUND) return r;

    if ((r = this->insert_at(value, insert_idx))) return r;
    if (idx) *idx = insert_idx;

    return 0;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::insert_at(const dmtdatain_t &value, const uint32_t idx) {
    barf_if_marked(*this);
    if (idx > this->size()) { return EINVAL; }

    bool same_size = this->values_same_size && value.get_dmtdatain_t_size() == this->value_length;
    if (same_size || this->size() == 0) {
        if (this->is_array) {
            if (idx == this->d.a.num_values) {
                return this->insert_at_array_end(value);
            }
            if (idx == 0 && this->d.a.start_idx > 0) {
                return this->insert_at_array_beginning(value);
            }
            this->convert_to_ctree();
        }
        // Is a c-tree.
        return this->insert_at_ctree(value, idx);
    }
    if (this->values_same_size) {
        this->convert_to_dtree();
        paranoid_invariant(!this->values_same_size);
    }
    paranoid_invariant(!is_array);
    // Is a d-tree.

    //d tree insert (TODO LOOK OVER)
    this->maybe_resize_or_convert(&value);
    subtree *rebalance_subtree = nullptr;
    this->insert_internal(&this->d.t.root, value, idx, &rebalance_subtree);
    if (rebalance_subtree != nullptr) {
        this->rebalance(rebalance_subtree);
    }
    return 0;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::insert_at_array_end(const dmtdatain_t& value_in) {
    paranoid_invariant(this->is_array);
    paranoid_invariant(this->values_same_size);
    if (this->d.a.num_values == 0) {
        this->value_length = value_in.get_dmtdatain_t_size();
    }

    this->maybe_resize_array(+1);
    dmtdata_t *dest = this->alloc_array_value_end();
    value_in.write_dmtdata_t_to(dest);
    return 0;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::insert_at_array_beginning(const dmtdatain_t& value_in) {
    paranoid_invariant(this->is_array);
    paranoid_invariant(this->values_same_size);
    paranoid_invariant(this->d.a.num_values > 0);
    //TODO: when deleting last element, should set start_idx to 0

    this->maybe_resize_array(-1);
    dmtdata_t *dest = this->alloc_array_value_beginning();
    value_in.write_dmtdata_t_to(dest);
    return 0;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
dmtdata_t * dmt<dmtdata_t, dmtdataout_t, supports_marks>::alloc_array_value_end(void) {
    const uint32_t real_idx = this->d.a.num_values + this->d.a.start_idx;
    this->d.a.num_values++;

    return get_array_value_internal(real_idx);
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
dmtdata_t * dmt<dmtdata_t, dmtdataout_t, supports_marks>::alloc_array_value_beginning(void) {
    paranoid_invariant(this->d.a.start_idx > 0);
    const uint32_t real_idx = --this->d.a.start_idx;
    this->d.a.num_values++;

    return get_array_value_internal(real_idx);
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
dmtdata_t * dmt<dmtdata_t, dmtdataout_t, supports_marks>::get_array_value(const uint32_t idx) const {
    //TODO: verify initial create always set is_array and values_same_size
    paranoid_invariant(idx < this->d.a.num_values);
    const uint32_t real_idx = idx + this->d.a.start_idx;
    return get_array_value(real_idx);
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
dmtdata_t * dmt<dmtdata_t, dmtdataout_t, supports_marks>::get_array_value_internal(const uint32_t real_idx) const {
    paranoid_invariant(this->is_array);
    paranoid_invariant(this->values_same_size);

    void* ptr = toku_mempool_get_pointer_from_base_and_offset(&this->mp, real_idx * align(this->value_length));
    dmtdata_t *CAST_FROM_VOIDP(value, ptr);
    return value;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::maybe_resize_array(const int change) {
    paranoid_invariant(change == -1 || change == 1);

    bool space_available = change < 0 || toku_mempool_get_free_space(&this->mp) >= align(this->value_length);

    const uint32_t n = this->d.a.num_values + change;
    const uint32_t new_n = n<=2 ? 4 : 2*n;
    const uint32_t new_space = align(this->value_length) * new_n;
    bool too_much_space = new_space <= toku_mempool_get_size(&this->mp) / 2;

    if (!space_available || too_much_space) {
        struct mempool new_kvspace;
        toku_mempool_construct(&new_kvspace, new_space);
        // Copy over to new mempool
        memcpy(toku_mempool_get_base(&new_kvspace), get_array_value(0), this->d.a.num_values * align(this->value_length));
        toku_mempool_destroy(&this->mp);
        this->mp = new_kvspace;
        this->d.a.start_idx = 0;
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
uint32_t dmt<dmtdata_t, dmtdataout_t, supports_marks>::align(const uint32_t x) const {
    return roundup_to_multiple(ALIGNMENT, x);
}


//TODO: above has at least one pass done
// The following 3 functions implement a static if for us.
template<typename dmtdata_t, typename dmtdataout_t>
static void barf_if_marked(const dmt<dmtdata_t, dmtdataout_t, false> &UU(dmt)) {
}

template<typename dmtdata_t, typename dmtdataout_t>
static void barf_if_marked(const dmt<dmtdata_t, dmtdataout_t, true> &dmt) {
    invariant(!dmt.has_marks());
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
bool dmt<dmtdata_t, dmtdataout_t, supports_marks>::has_marks(void) const {
    static_assert(supports_marks, "Does not support marks");
    if (this->d.t.root.is_null()) {
        return false;
    }
    const dmt_node &node = get_node(this->d.t.root);
    return node.get_marks_below() || node.get_marked();
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::set_at(const dmtdata_t &value, const uint32_t idx) {
    static_assert(!false, "1st pass not done.  May need to change API. Not needed for prototype.");
    barf_if_marked(*this);
    if (idx >= this->size()) { return EINVAL; }

    if (this->is_array) {
        this->set_at_internal_array(value, idx);
    } else {
        this->set_at_internal(this->d.t.root, value, idx);
    }
    return 0;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::delete_at(const uint32_t idx) {
    barf_if_marked(*this);
    if (idx >= this->size()) { return EINVAL; }

#if 0
    if (this->is_array && idx != 0 && idx != this->d.a.num_values - 1) {
        paranoid_invariant(!dynamic && !supports_marks);
        this->convert_to_tree();
    }
    if (this->is_array) {
        paranoid_invariant(!dynamic && !supports_marks);
        //Testing for 0 does not rule out it being the last entry.
        //Test explicitly for num_values-1
        if (idx != this->d.a.num_values - 1) {
            this->d.a.start_idx++;
        }
        this->d.a.num_values--;
    } else {
        subtree *rebalance_subtree = nullptr;
        this->delete_internal(&this->d.t.root, idx, nullptr, &rebalance_subtree);
        if (rebalance_subtree != nullptr) {
            this->rebalance(rebalance_subtree);
        }
    }
    this->maybe_resize_or_convert(std::is_same<dmtdatain_t, dmtdata_t>(), nullptr, this->size() -1);
#endif
    return 0;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename iterate_extra_t,
         int (*f)(const uint32_t, const dmtdata_t &, const uint32_t, iterate_extra_t *const)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::iterate(iterate_extra_t *const iterate_extra) const {
    return this->iterate_on_range<iterate_extra_t, f>(0, this->size(), iterate_extra);
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename iterate_extra_t,
         int (*f)(const uint32_t, const dmtdata_t &, const uint32_t, iterate_extra_t *const)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::iterate_on_range(const uint32_t left, const uint32_t right, iterate_extra_t *const iterate_extra) const {
    if (right > this->size()) { return EINVAL; }
    if (left == right) { return 0; }
    if (this->is_array) {
        return this->iterate_internal_array<iterate_extra_t, f>(left, right, iterate_extra);
    }
    return this->iterate_internal<iterate_extra_t, f>(left, right, this->d.t.root, 0, iterate_extra);
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename iterate_extra_t,
         int (*f)(const uint32_t, const dmtdata_t &, const uint32_t, iterate_extra_t *const)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::iterate_and_mark_range(const uint32_t left, const uint32_t right, iterate_extra_t *const iterate_extra) {
    static_assert(supports_marks, "does not support marks");
    if (right > this->size()) { return EINVAL; }
    if (left == right) { return 0; }
    paranoid_invariant(!this->is_array);
    return this->iterate_and_mark_range_internal<iterate_extra_t, f>(left, right, this->d.t.root, 0, iterate_extra);
}

//TODO: We can optimize this if we steal 3 bits.  1 bit: this node is marked.  1 bit: left subtree has marks. 1 bit: right subtree has marks.
template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename iterate_extra_t,
         int (*f)(const uint32_t, const dmtdata_t &, const uint32_t, iterate_extra_t *const)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::iterate_over_marked(iterate_extra_t *const iterate_extra) const {
    static_assert(supports_marks, "does not support marks");
    paranoid_invariant(!this->is_array);
    return this->iterate_over_marked_internal<iterate_extra_t, f>(this->d.t.root, 0, iterate_extra);
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::unmark(const subtree &subtree, const uint32_t index, GrowableArray<node_idx> *const indexes) {
    if (subtree.is_null()) { return; }
    dmt_node &n = get_node(subtree);
    const uint32_t index_root = index + this->nweight(n.b.left);

    const bool below = n.get_marks_below();
    if (below) {
        this->unmark(n.b.left, index, indexes);
    }
    if (n.get_marked()) {
        indexes->push(index_root);
    }
    n.clear_stolen_bits();
    if (below) {
        this->unmark(n.b.right, index_root + 1, indexes);
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::delete_all_marked(void) {
    static_assert(supports_marks, "does not support marks");
    if (!this->has_marks()) {
        return;
    }
    paranoid_invariant(!this->is_array);
    GrowableArray<node_idx> marked_indexes;
    marked_indexes.init();

    // Remove all marks.
    // We need to delete all the stolen bits before calling delete_at to prevent barfing.
    this->unmark(this->d.t.root, 0, &marked_indexes);

    for (uint32_t i = 0; i < marked_indexes.get_size(); i++) {
        // Delete from left to right, shift by number already deleted.
        // Alternative is delete from right to left.
        int r = this->delete_at(marked_indexes.fetch_unchecked(i) - i);
        lazy_assert_zero(r);
    }
    marked_indexes.deinit();
    barf_if_marked(*this);
}


template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::verify(void) const {
    if (!is_array) {
        verify_internal(this->d.t.root);
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::verify_internal(const subtree &subtree) const {
    if (subtree.is_null()) {
        return;
    }
    const dmt_node &node = get_node(subtree);

    const uint32_t leftweight = this->nweight(node.left);
    const uint32_t rightweight = this->nweight(node.right);

    invariant(leftweight + rightweight + 1 == this->nweight(subtree));
    verify_internal(node.left);
    verify_internal(node.right);
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
uint32_t dmt<dmtdata_t, dmtdataout_t, supports_marks>::verify_marks_consistent_internal(const subtree &subtree, const bool UU(allow_marks)) const {
    if (subtree.is_null()) {
        return 0;
    }
    const dmt_node &node = get_node(subtree);
    uint32_t num_marks = verify_marks_consistent_internal(node.left, node.get_marks_below());
    num_marks += verify_marks_consistent_internal(node.right, node.get_marks_below());
    if (node.get_marks_below()) {
        paranoid_invariant(allow_marks);
        paranoid_invariant(num_marks > 0);
    } else {
        // redundant with invariant below, but nice to have explicitly
        paranoid_invariant(num_marks == 0);
    }
    if (node.get_marked()) {
        paranoid_invariant(allow_marks);
        ++num_marks;
    }
    return num_marks;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::verify_marks_consistent(void) const {
    static_assert(supports_marks, "does not support marks");
    paranoid_invariant(!this->is_array);
    this->verify_marks_consistent_internal(this->d.t.root, true);
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename iterate_extra_t,
         int (*f)(const uint32_t, dmtdata_t *, const uint32_t, iterate_extra_t *const)>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::iterate_ptr(iterate_extra_t *const iterate_extra) {
    if (this->is_array) {
        this->iterate_ptr_internal_array<iterate_extra_t, f>(0, this->size(), iterate_extra);
    } else {
        this->iterate_ptr_internal<iterate_extra_t, f>(0, this->size(), this->d.t.root, 0, iterate_extra);
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::fetch(const uint32_t idx, uint32_t *const value_len, dmtdataout_t *const value) const {
    if (idx >= this->size()) { return EINVAL; }
    if (this->is_array) {
        this->fetch_internal_array(idx, value_len, value);
    } else {
        this->fetch_internal(this->d.t.root, idx, value_len, value);
    }
    return 0;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename dmtcmp_t,
         int (*h)(const uint32_t, const dmtdata_t &, const dmtcmp_t &)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::find_zero(const dmtcmp_t &extra, uint32_t *const value_len, dmtdataout_t *const value, uint32_t *const idxp) const {
    uint32_t tmp_index;
    uint32_t *const child_idxp = (idxp != nullptr) ? idxp : &tmp_index;
    int r;
    if (this->is_array) {
        r = this->find_internal_zero_array<dmtcmp_t, h>(extra, value_len, value, child_idxp);
    }
    else {
        r = this->find_internal_zero<dmtcmp_t, h>(this->d.t.root, extra, value_len, value, child_idxp);
    }
    return r;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename dmtcmp_t,
         int (*h)(const uint32_t, const dmtdata_t &, const dmtcmp_t &)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::find(const dmtcmp_t &extra, int direction, uint32_t *const value_len, dmtdataout_t *const value, uint32_t *const idxp) const {
    uint32_t tmp_index;
    uint32_t *const child_idxp = (idxp != nullptr) ? idxp : &tmp_index;
    paranoid_invariant(direction != 0);
    if (direction < 0) {
        if (this->is_array) {
            return this->find_internal_minus_array<dmtcmp_t, h>(extra, value_len,  value, child_idxp);
        } else {
            return this->find_internal_minus<dmtcmp_t, h>(this->d.t.root, extra, value_len,  value, child_idxp);
        }
    } else {
        if (this->is_array) {
            return this->find_internal_plus_array<dmtcmp_t, h>(extra, value_len,  value, child_idxp);
        } else {
            return this->find_internal_plus<dmtcmp_t, h>(this->d.t.root, extra, value_len, value, child_idxp);
        }
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
size_t dmt<dmtdata_t, dmtdataout_t, supports_marks>::memory_size(void) {
    return (sizeof *this) + toku_mempool_get_size(&this->mp);
}


template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
typename dmt<dmtdata_t, dmtdataout_t, supports_marks>::dmt_node & dmt<dmtdata_t, dmtdataout_t, supports_marks>::get_node(const subtree &subtree) const {
    paranoid_invariant(!subtree.is_null());
    return get_node(subtree.get_index());
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
typename dmt<dmtdata_t, dmtdataout_t, supports_marks>::dmt_node & dmt<dmtdata_t, dmtdataout_t, supports_marks>::get_node(const node_idx offset) const {
    //TODO: implement
    //Need to decide what to do with regards to cnode/dnode
    void* ptr = toku_mempool_get_pointer_from_base_and_offset(&this->mp, offset);
    dmt_node *CAST_FROM_VOIDP(node, ptr);
    return *node;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
node_idx dmt<dmtdata_t, dmtdataout_t, supports_marks>::node_malloc_and_set_value(const dmt_functor<dmtdata_t> &value) {
    size_t val_size = value.get_dmtdatain_t_size();
    size_t size_to_alloc = __builtin_offsetof(dmt_node, value) + val_size;
    size_to_alloc = roundup_to_multiple(ALIGNMENT, size_to_alloc);
    void* np = toku_mempool_malloc(&this->mp, size_to_alloc, 1);
    paranoid_invariant(np != nullptr);
    dmt_node *CAST_FROM_VOIDP(n, np);
    n->value_length = val_size;
    value.write_dmtdata_t_to(&n->value);

    n->b.clear_stolen_bits();
    return toku_mempool_get_offset_from_pointer_and_base(&this->mp, np);
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
node_idx dmt<dmtdata_t, dmtdataout_t, supports_marks>::node_malloc_and_set_value(const dmtdata_t &value) {
    paranoid_invariant(this->d.t.free_idx < this->capacity);
    dmt_node &n = get_node(this->d.t.free_idx);
    n.clear_stolen_bits();
    //TODO: dynamic len
    n.value_length = sizeof(dmtdata_t);
    n.value = value;
    return this->d.t.free_idx++;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::node_free(const type_is_dynamic&, const subtree &st) {
    dmt_node &n = get_node(st);
    size_t size_to_free = __builtin_offsetof(dmt_node, value) + n.value_length;
    size_to_free = roundup_to_multiple(ALIGNMENT, size_to_free);
    toku_mempool_mfree(&this->mp, &n, size_to_free);
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::node_free(const type_is_static&, const subtree &st) {
    paranoid_invariant(st.get_index() < this->capacity);
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::fill_array_with_subtree_values(dmtdata_t *const array, const subtree &subtree) const {
    if (subtree.is_null()) return;
    const dmt_node &tree = get_node(subtree);
    this->fill_array_with_subtree_values(&array[0], tree.left);
    array[this->nweight(tree.left)] = tree.value;
    this->fill_array_with_subtree_values(&array[this->nweight(tree.left) + 1], tree.right);
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::convert_to_array(void) {
    if (!this->is_array) {
        const uint32_t num_values = this->size();
        uint32_t new_size = 2*num_values;
        new_size = new_size < 4 ? 4 : new_size;

        dmtdata_t *XMALLOC_N(new_size, tmp_values);
        this->fill_array_with_subtree_values(tmp_values, this->d.t.root);
        toku_free(this->d.t.nodes);
        this->is_array       = true;
        this->capacity       = new_size;
        this->d.a.num_values = num_values;
        this->d.a.values     = tmp_values;
        this->d.a.start_idx  = 0;
    }
}

// Rebuilds a subtree in place
template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::rebuild_inplace_from_sorted_array(subtree *const subtree_p, node_idx * const subtrees, const uint32_t numvalues) {
    // subtrees is an array of offsets which hold the values in sorted order.  We leave the values alone
    // and reconstruct the subtree based on that.
    if (numvalues==0) {
        subtree_p->set_to_null();
    } else {
        const uint32_t halfway = numvalues/2;
        const dmt_node &n = get_node(subtrees[halfway]);
        subtree_p->set_index(subtrees[halfway]);
        n.b.weight = numvalues;
        //value is left alone (is already set)
        // update everything before the recursive calls so the second call can be a tail call.
        this->rebuild_inplace_from_sorted_array(&n->left,  &subtrees[0], halfway);
        this->rebuild_inplace_from_sorted_array(&n->right, &subtrees[halfway+1], numvalues - (halfway+1));
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::rebuild_from_sorted_array(subtree *const subtree, const dmtdata_t *const values, const uint32_t numvalues) {
    if (numvalues==0) {
        subtree->set_to_null();
    } else {
        const uint32_t halfway = numvalues/2;
        const node_idx newidx = this->node_malloc_and_set_value(values[halfway]);
        dmt_node & newnode = get_node(newidx);
        newnode.weight = numvalues;
        subtree->set_index(newidx);
        // update everything before the recursive calls so the second call can be a tail call.
        this->rebuild_from_sorted_array(&newnode.left,  &values[0], halfway);
        this->rebuild_from_sorted_array(&newnode.right, &values[halfway+1], numvalues - (halfway+1));
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::convert_to_tree(void) {
    if (this->is_array) {
        const uint32_t num_nodes = this->size();
        uint32_t new_size  = num_nodes*2;
        new_size = new_size < 4 ? 4 : new_size;

        dmt_node *XMALLOC_N(new_size, new_nodes);
        dmtdata_t *const values = this->d.a.values;
        dmtdata_t *const tmp_values = &values[this->d.a.start_idx];
        this->is_array = false;
        this->d.t.nodes = new_nodes;
        this->capacity = new_size;
        this->d.t.free_idx = 0;
        this->d.t.root.set_to_null();
        this->rebuild_from_sorted_array(&this->d.t.root, tmp_values, num_nodes);
        toku_free(values);
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::maybe_resize_or_convert(const dmtdatain_t * value) {
    (void)value;
#if 0
    static_assert(std::is_same<dmtdatain_t, dmt_functor<dmtdata_t>>::value, "functor wrong type");
    const ssize_t curr_capacity = toku_mempool_get_size(&this->mp);
    const ssize_t curr_free = toku_mempool_get_free_space(&this->mp);
    const ssize_t curr_used = toku_mempool_get_used_space(&this->mp);
    ssize_t add_size = 0;
    if (value) {
        add_size = __builtin_offsetof(dmt_node, value) + value->get_dmtdatain_t_size();
        add_size = roundup_to_multiple(ALIGNMENT, add_size);
    }

    const ssize_t need_size = curr_used + add_size;
    paranoid_invariant(need_size <= UINT32_MAX);
    const ssize_t new_size = 2*need_size;
    paranoid_invariant(new_size <= UINT32_MAX);

    //const uint32_t num_nodes = this->nweight(this->d.t.root);

    if ((curr_capacity / 2 >= new_size) || // Way too much allocated
        (curr_free < add_size)) {  // No room in mempool
        // Copy all memory and reconstruct dmt in new mempool.
        struct mempool new_kvspace;
        struct mempool old_kvspace;
        toku_mempool_construct(&new_kvspace, new_size);

        if (!this->d.t.root.is_null()) {
            const dmt_node &n = get_node(this->d.t.root);
            node_idx *tmp_array;
            bool malloced = false;
            tmp_array = alloc_temp_node_idxs(n.b.weight);
            if (!tmp_array) {
                malloced = true;
                XMALLOC_N(n.b.weight, tmp_array);
            }
            this->fill_array_with_subtree_idxs(tmp_array, this->d.t.root);
            for (node_idx i = 0; i < n.b.weight; i++) {
                dmt_node &node = get_node(tmp_array[i]);
                const size_t bytes_to_copy = __builtin_offsetof(dmt_node, value) + node.value_length;
                const size_t bytes_to_alloc = roundup_to_multiple(ALIGNMENT, bytes_to_copy);
                void* newdata = toku_mempool_malloc(&new_kvspace, bytes_to_alloc, 1);
                memcpy(newdata, &node, bytes_to_copy);
                tmp_array[i] = toku_mempool_get_offset_from_pointer_and_base(&new_kvspace, newdata);
            }

            old_kvspace = this->mp;
            this->mp = new_kvspace;
            this->rebuild_subtree_from_idxs(&this->d.t.root, tmp_array, n.b.weight);
            if (malloced) toku_free(tmp_array);
            toku_mempool_destroy(&old_kvspace);
        }
        else {
            toku_mempool_destroy(&this->mp);
            this->mp = new_kvspace;
        }
    }
#endif
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
bool dmt<dmtdata_t, dmtdataout_t, supports_marks>::will_need_rebalance(const subtree &subtree, const int leftmod, const int rightmod) const {
    if (subtree.is_null()) { return false; }
    const dmt_node &n = get_node(subtree);
    // one of the 1's is for the root.
    // the other is to take ceil(n/2)
    const uint32_t weight_left  = this->nweight(n.b.left)  + leftmod;
    const uint32_t weight_right = this->nweight(n.b.right) + rightmod;
    return ((1+weight_left < (1+1+weight_right)/2)
            ||
            (1+weight_right < (1+1+weight_left)/2));
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::insert_internal(subtree *const subtreep, const dmtdatain_t &value, const uint32_t idx, subtree **const rebalance_subtree) {
    if (subtreep->is_null()) {
        paranoid_invariant_zero(idx);
        const node_idx newidx = this->node_malloc_and_set_value(value);
        dmt_node &newnode = get_node(newidx);
        newnode.b.weight = 1;
        newnode.b.left.set_to_null();
        newnode.b.right.set_to_null();
        subtreep->set_index(newidx);
    } else {
        dmt_node &n = get_node(*subtreep);
        n.b.weight++;
        if (idx <= this->nweight(n.b.left)) {
            if (*rebalance_subtree == nullptr && this->will_need_rebalance(*subtreep, 1, 0)) {
                *rebalance_subtree = subtreep;
            }
            this->insert_internal(&n.b.left, value, idx, rebalance_subtree);
        } else {
            if (*rebalance_subtree == nullptr && this->will_need_rebalance(*subtreep, 0, 1)) {
                *rebalance_subtree = subtreep;
            }
            const uint32_t sub_index = idx - this->nweight(n.b.left) - 1;
            this->insert_internal(&n.b.right, value, sub_index, rebalance_subtree);
        }
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::set_at_internal_array(const dmtdata_t &value, const uint32_t idx) {
    this->d.a.values[this->d.a.start_idx + idx] = value;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::set_at_internal(const subtree &subtree, const dmtdata_t &value, const uint32_t idx) {
    paranoid_invariant(!subtree.is_null());
    dmt_node &n = get_node(subtree);
    const uint32_t leftweight = this->nweight(n.b.left);
    if (idx < leftweight) {
        this->set_at_internal(n.b.left, value, idx);
    } else if (idx == leftweight) {
        n.value = value;
    } else {
        this->set_at_internal(n.b.right, value, idx - leftweight - 1);
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::delete_internal(subtree *const subtreep, const uint32_t idx, subtree *const subtree_replace, subtree **const rebalance_subtree) {
    paranoid_invariant_notnull(subtreep);
    paranoid_invariant_notnull(rebalance_subtree);
    paranoid_invariant(!subtreep->is_null());
    dmt_node &n = get_node(*subtreep);
    const uint32_t leftweight = this->nweight(n.b.left);
    if (idx < leftweight) {
        n.b.weight--;
        if (*rebalance_subtree == nullptr && this->will_need_rebalance(*subtreep, -1, 0)) {
            *rebalance_subtree = subtreep;
        }
        this->delete_internal(&n.b.left, idx, subtree_replace, rebalance_subtree);
    } else if (idx == leftweight) {
        // Found the correct index.
        if (n.b.left.is_null()) {
            // Delete n and let parent point to n.b.right
            subtree ptr_this = *subtreep;
            *subtreep = n.b.right;
            subtree to_free;
            if (subtree_replace != nullptr) {
                // Swap self with the other node.
                to_free = *subtree_replace;
                dmt_node &ancestor = get_node(*subtree_replace);
                if (*rebalance_subtree == &ancestor.right) {
                    // Take over rebalance responsibility.
                    *rebalance_subtree = &n.b.right;
                }
                n.b.weight = ancestor.weight;
                n.b.left = ancestor.left;
                n.b.right = ancestor.right;
                *subtree_replace = ptr_this;
            } else {
                to_free = ptr_this;
            }
            this->node_free(to_free);
        } else if (n.b.right.is_null()) {
            // Delete n and let parent point to n.b.left
            subtree to_free = *subtreep;
            *subtreep = n.b.left;
            paranoid_invariant_null(subtree_replace);  // To be recursive, we're looking for index 0.  n is index > 0 here.
            this->node_free(to_free);
        } else {
            if (*rebalance_subtree == nullptr && this->will_need_rebalance(*subtreep, 0, -1)) {
                *rebalance_subtree = subtreep;
            }
            // don't need to copy up value, it's only used by this
            // next call, and when that gets to the bottom there
            // won't be any more recursion
            n.b.weight--;
            this->delete_internal(&n.b.right, 0, subtreep, rebalance_subtree);
        }
    } else {
        n.b.weight--;
        if (*rebalance_subtree == nullptr && this->will_need_rebalance(*subtreep, 0, -1)) {
            *rebalance_subtree = subtreep;
        }
        this->delete_internal(&n.b.right, idx - leftweight - 1, subtree_replace, rebalance_subtree);
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename iterate_extra_t,
         int (*f)(const uint32_t, const dmtdata_t &, const uint32_t, iterate_extra_t *const)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::iterate_internal_array(const uint32_t left, const uint32_t right,
                                                         iterate_extra_t *const iterate_extra) const {
#if 1
    (void)left, (void)right, (void)iterate_extra;
#else
    int r;
    for (uint32_t i = left; i < right; ++i) {
        //TODO: dynamic len
        r = f(sizeof(dmtdata_t), this->d.a.values[this->d.a.start_idx + i], i, iterate_extra);
        if (r != 0) {
            return r;
        }
    }
#endif
    return 0;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename iterate_extra_t,
         int (*f)(const uint32_t, dmtdata_t *, const uint32_t, iterate_extra_t *const)>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::iterate_ptr_internal(const uint32_t left, const uint32_t right,
                                                        const subtree &subtree, const uint32_t idx,
                                                        iterate_extra_t *const iterate_extra) {
    if (!subtree.is_null()) { 
        dmt_node &n = get_node(subtree);
        const uint32_t idx_root = idx + this->nweight(n.b.left);
        if (left < idx_root) {
            this->iterate_ptr_internal<iterate_extra_t, f>(left, right, n.b.left, idx, iterate_extra);
        }
        if (left <= idx_root && idx_root < right) {
            int r = f(n.value_length, &n.value, idx_root, iterate_extra);
            lazy_assert_zero(r);
        }
        if (idx_root + 1 < right) {
            this->iterate_ptr_internal<iterate_extra_t, f>(left, right, n.b.right, idx_root + 1, iterate_extra);
        }
    }
}

#if 0
template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename iterate_extra_t,
         int (*f)(const uint32_t, dmtdata_t *, const uint32_t, iterate_extra_t *const)>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::iterate_ptr_internal_array(const uint32_t left, const uint32_t right,
                                                              iterate_extra_t *const iterate_extra) {
    for (uint32_t i = left; i < right; ++i) {
        //TODO: dynamic len
        int r = f(sizeof(dmtdata_t), &this->d.a.values[this->d.a.start_idx + i], i, iterate_extra);
        lazy_assert_zero(r);
    }
}
#endif

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename iterate_extra_t,
         int (*f)(const uint32_t, const dmtdata_t &, const uint32_t, iterate_extra_t *const)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::iterate_internal(const uint32_t left, const uint32_t right,
                                                   const subtree &subtree, const uint32_t idx,
                                                   iterate_extra_t *const iterate_extra) const {
    if (subtree.is_null()) { return 0; }
    int r;
    const dmt_node &n = get_node(subtree);
    const uint32_t idx_root = idx + this->nweight(n.b.left);
    if (left < idx_root) {
        r = this->iterate_internal<iterate_extra_t, f>(left, right, n.b.left, idx, iterate_extra);
        if (r != 0) { return r; }
    }
    if (left <= idx_root && idx_root < right) {
        r = f(n.value_length, n.value, idx_root, iterate_extra);
        if (r != 0) { return r; }
    }
    if (idx_root + 1 < right) {
        return this->iterate_internal<iterate_extra_t, f>(left, right, n.b.right, idx_root + 1, iterate_extra);
    }
    return 0;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename iterate_extra_t,
         int (*f)(const uint32_t, const dmtdata_t &, const uint32_t, iterate_extra_t *const)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::iterate_and_mark_range_internal(const uint32_t left, const uint32_t right,
                                                                                  const subtree &subtree, const uint32_t idx,
                                                                                  iterate_extra_t *const iterate_extra) {
    paranoid_invariant(!subtree.is_null());
    int r;
    dmt_node &n = get_node(subtree);
    const uint32_t idx_root = idx + this->nweight(n.b.left);
    if (left < idx_root && !n.b.left.is_null()) {
        n.set_marks_below_bit();
        r = this->iterate_and_mark_range_internal<iterate_extra_t, f>(left, right, n.b.left, idx, iterate_extra);
        if (r != 0) { return r; }
    }
    if (left <= idx_root && idx_root < right) {
        n.set_marked_bit();
        r = f(n.value, idx_root, iterate_extra);
        if (r != 0) { return r; }
    }
    if (idx_root + 1 < right && !n.b.right.is_null()) {
        n.set_marks_below_bit();
        return this->iterate_and_mark_range_internal<iterate_extra_t, f>(left, right, n.b.right, idx_root + 1, iterate_extra);
    }
    return 0;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename iterate_extra_t,
         int (*f)(const uint32_t, const dmtdata_t &, const uint32_t, iterate_extra_t *const)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::iterate_over_marked_internal(const subtree &subtree, const uint32_t idx,
                                                                               iterate_extra_t *const iterate_extra) const {
    if (subtree.is_null()) { return 0; }
    int r;
    const dmt_node &n = get_node(subtree);
    const uint32_t idx_root = idx + this->nweight(n.b.left);
    if (n.get_marks_below()) {
        r = this->iterate_over_marked_internal<iterate_extra_t, f>(n.b.left, idx, iterate_extra);
        if (r != 0) { return r; }
    }
    if (n.get_marked()) {
        r = f(n.value, idx_root, iterate_extra);
        if (r != 0) { return r; }
    }
    if (n.get_marks_below()) {
        return this->iterate_over_marked_internal<iterate_extra_t, f>(n.b.right, idx_root + 1, iterate_extra);
    }
    return 0;
}

#if 0
template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::fetch_internal_array(const uint32_t i, uint32_t *const value_len, dmtdataout_t *const value) const {
    copyout(value_len, value, &this->d.a.values[this->d.a.start_idx + i]);
}
#endif

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::fetch_internal(const subtree &subtree, const uint32_t i, uint32_t *const value_len, dmtdataout_t *const value) const {
    dmt_node &n = get_node(subtree);
    const uint32_t leftweight = this->nweight(n.b.left);
    if (i < leftweight) {
        this->fetch_internal(n.b.left, i, value_len, value);
    } else if (i == leftweight) {
        copyout(value_len, value, &n);
    } else {
        this->fetch_internal(n.b.right, i - leftweight - 1, value_len, value);
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::fill_array_with_subtree_idxs(node_idx *const array, const subtree &subtree) const {
    if (!subtree.is_null()) {
        const dmt_node &tree = get_node(subtree);
        this->fill_array_with_subtree_idxs(&array[0], tree.left);
        array[this->nweight(tree.left)] = subtree.get_index();
        this->fill_array_with_subtree_idxs(&array[this->nweight(tree.left) + 1], tree.right);
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::rebuild_subtree_from_idxs(subtree *const subtree, const node_idx *const idxs, const uint32_t numvalues) {
    if (numvalues==0) {
        subtree->set_to_null();
    } else {
        uint32_t halfway = numvalues/2;
        subtree->set_index(idxs[halfway]);
        dmt_node &newnode = get_node(idxs[halfway]);
        newnode.weight = numvalues;
        // value is already in there.
        this->rebuild_subtree_from_idxs(&newnode.left,  &idxs[0], halfway);
        this->rebuild_subtree_from_idxs(&newnode.right, &idxs[halfway+1], numvalues-(halfway+1));
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
node_idx* dmt<dmtdata_t, dmtdataout_t, supports_marks>::alloc_temp_node_idxs(uint32_t num_idxs) {
#if 0
    size_t mem_needed = num_idxs * sizeof(node_idx);
    size_t mem_free;
    node_idx* tmp = nullptr;
    paranoid_invariant(!is_array);
    if (dynamic) {
        mem_free = toku_mempool_get_free_space(&this->mp);
        CAST_FROM_VOIDP(tmp, toku_mempool_get_next_free_ptr(&this->mp));
    } else {
        mem_free = (this->capacity - this->d.t.free_idx) * (sizeof this->d.t.nodes[0]);
        tmp = reinterpret_cast<node_idx *>(&this->d.t.nodes[this->d.t.free_idx]);
    }
    if (mem_free >= mem_needed) {
        return tmp;
    }
#endif
    return nullptr;
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::rebalance(subtree *const subtree) {
    (void) subtree;
#if 0
    paranoid_invariant(!subtree->is_null());
    node_idx idx = subtree->get_index();
    if (!dynamic && idx==this->d.t.root.get_index()) {
        //Try to convert to an array.
        //If this fails, (malloc) nothing will have changed.
        //In the failure case we continue on to the standard rebalance
        //algorithm.
        this->convert_to_array();
        if (supports_marks) {
            this->convert_to_tree();
        }
    } else {
        const dmt_node &n = get_node(idx);
        node_idx *tmp_array;
        bool malloced = false;
        tmp_array = alloc_temp_node_idxs(n.b.weight);
        if (!tmp_array) {
            malloced = true;
            XMALLOC_N(n.b.weight, tmp_array);
        }
        this->fill_array_with_subtree_idxs(tmp_array, *subtree);
        this->rebuild_subtree_from_idxs(subtree, tmp_array, n.b.weight);
        if (malloced) toku_free(tmp_array);
    }
#endif
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::copyout(uint32_t *const outlen, dmtdata_t *const out, const dmt_node *const n) {
    if (out) {
        *out = n->value;
    }
    if (outlen) {
        *outlen = n->value_length;
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::copyout(uint32_t *const outlen, dmtdata_t **const out, dmt_node *const n) {
    if (out) {
        *out = &n->value;
    }
    if (outlen) {
        *outlen = n->value_length;
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::copyout(uint32_t *const outlen, dmtdata_t *const out, const dmtdata_t *const stored_value_ptr) {
    if (out) {
        *out = *stored_value_ptr;
    }
    if (outlen) {
        *outlen = sizeof(dmtdata_t);
    }
}

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
void dmt<dmtdata_t, dmtdataout_t, supports_marks>::copyout(uint32_t *const outlen, dmtdata_t **const out, dmtdata_t *const stored_value_ptr) {
    if (out) {
        *out = stored_value_ptr;
    }
    if (outlen) {
        *outlen = sizeof(dmtdata_t);
    }
}

#if 0
template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename dmtcmp_t,
         int (*h)(const uint32_t, const dmtdata_t &, const dmtcmp_t &)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::find_internal_zero_array(const dmtcmp_t &extra, uint32_t *const value_len, dmtdataout_t *const value, uint32_t *const idxp) const {
    paranoid_invariant_notnull(idxp);
    uint32_t min = this->d.a.start_idx;
    uint32_t limit = this->d.a.start_idx + this->d.a.num_values;
    uint32_t best_pos = subtree::NODE_NULL;
    uint32_t best_zero = subtree::NODE_NULL;

    while (min!=limit) {
        uint32_t mid = (min + limit) / 2;
        //TODO: dynamic len
        int hv = h(sizeof(dmtdata_t), this->d.a.values[mid], extra);
        if (hv<0) {
            min = mid+1;
        }
        else if (hv>0) {
            best_pos  = mid;
            limit     = mid;
        }
        else {
            best_zero = mid;
            limit     = mid;
        }
    }
    if (best_zero!=subtree::NODE_NULL) {
        //Found a zero
        copyout(value_len, value, &this->d.a.values[best_zero]);
        *idxp = best_zero - this->d.a.start_idx;
        return 0;
    }
    if (best_pos!=subtree::NODE_NULL) *idxp = best_pos - this->d.a.start_idx;
    else                     *idxp = this->d.a.num_values;
    return DB_NOTFOUND;
}
#endif

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename dmtcmp_t,
         int (*h)(const uint32_t, const dmtdata_t &, const dmtcmp_t &)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::find_internal_zero(const subtree &subtree, const dmtcmp_t &extra, uint32_t *const value_len, dmtdataout_t *const value, uint32_t *const idxp) const {
    paranoid_invariant_notnull(idxp);
    if (subtree.is_null()) {
        *idxp = 0;
        return DB_NOTFOUND;
    }
    dmt_node &n = get_node(subtree);
    int hv = h(n.value_length, n.value, extra);
    if (hv<0) {
        int r = this->find_internal_zero<dmtcmp_t, h>(n.b.right, extra, value_len, value, idxp);
        *idxp += this->nweight(n.b.left)+1;
        return r;
    } else if (hv>0) {
        return this->find_internal_zero<dmtcmp_t, h>(n.b.left, extra, value_len, value, idxp);
    } else {
        int r = this->find_internal_zero<dmtcmp_t, h>(n.b.left, extra, value_len, value, idxp);
        if (r==DB_NOTFOUND) {
            *idxp = this->nweight(n.b.left);
            copyout(value_len, value, &n);
            r = 0;
        }
        return r;
    }
}

#if 0
template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename dmtcmp_t,
         int (*h)(const uint32_t, const dmtdata_t &, const dmtcmp_t &)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::find_internal_plus_array(const dmtcmp_t &extra, uint32_t *const value_len, dmtdataout_t *const value, uint32_t *const idxp) const {
    paranoid_invariant_notnull(idxp);
    uint32_t min = this->d.a.start_idx;
    uint32_t limit = this->d.a.start_idx + this->d.a.num_values;
    uint32_t best = subtree::NODE_NULL;

    while (min != limit) {
        const uint32_t mid = (min + limit) / 2;
        //TODO: dynamic len
        const int hv = h(sizeof(dmtdata_t), this->d.a.values[mid], extra);
        if (hv > 0) {
            best = mid;
            limit = mid;
        } else {
            min = mid + 1;
        }
    }
    if (best == subtree::NODE_NULL) { return DB_NOTFOUND; }
    copyout(value_len, value, &this->d.a.values[best]);
    *idxp = best - this->d.a.start_idx;
    return 0;
}
#endif

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename dmtcmp_t,
         int (*h)(const uint32_t, const dmtdata_t &, const dmtcmp_t &)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::find_internal_plus(const subtree &subtree, const dmtcmp_t &extra, uint32_t *const value_len, dmtdataout_t *const value, uint32_t *const idxp) const {
    paranoid_invariant_notnull(idxp);
    if (subtree.is_null()) {
        return DB_NOTFOUND;
    }
    dmt_node & n = get_node(subtree);
    int hv = h(n.value_length, n.value, extra);
    int r;
    if (hv > 0) {
        r = this->find_internal_plus<dmtcmp_t, h>(n.b.left, extra, value_len, value, idxp);
        if (r == DB_NOTFOUND) {
            *idxp = this->nweight(n.b.left);
            copyout(value_len, value, &n);
            r = 0;
        }
    } else {
        r = this->find_internal_plus<dmtcmp_t, h>(n.b.right, extra, value_len, value, idxp);
        if (r == 0) {
            *idxp += this->nweight(n.b.left) + 1;
        }
    }
    return r;
}

#if 0
template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename dmtcmp_t,
         int (*h)(const uint32_t, const dmtdata_t &, const dmtcmp_t &)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::find_internal_minus_array(const dmtcmp_t &extra, uint32_t *const value_len, dmtdataout_t *const value, uint32_t *const idxp) const {
    paranoid_invariant_notnull(idxp);
    uint32_t min = this->d.a.start_idx;
    uint32_t limit = this->d.a.start_idx + this->d.a.num_values;
    uint32_t best = subtree::NODE_NULL;

    while (min != limit) {
        const uint32_t mid = (min + limit) / 2;
        //TODO: dynamic len
        const int hv = h(sizeof(dmtdata_t), this->d.a.values[mid], extra);
        if (hv < 0) {
            best = mid;
            min = mid + 1;
        } else {
            limit = mid;
        }
    }
    if (best == subtree::NODE_NULL) { return DB_NOTFOUND; }
    copyout(value_len, value, &this->d.a.values[best]);
    *idxp = best - this->d.a.start_idx;
    return 0;
}
#endif

template<typename dmtdata_t, typename dmtdataout_t, bool supports_marks>
template<typename dmtcmp_t,
         int (*h)(const uint32_t, const dmtdata_t &, const dmtcmp_t &)>
int dmt<dmtdata_t, dmtdataout_t, supports_marks>::find_internal_minus(const subtree &subtree, const dmtcmp_t &extra, uint32_t *const value_len, dmtdataout_t *const value, uint32_t *const idxp) const {
    paranoid_invariant_notnull(idxp);
    if (subtree.is_null()) {
        return DB_NOTFOUND;
    }
    dmt_node & n = get_node(subtree);
    int hv = h(n.value_length, n.value, extra);
    if (hv < 0) {
        int r = this->find_internal_minus<dmtcmp_t, h>(n.b.right, extra, value_len, value, idxp);
        if (r == 0) {
            *idxp += this->nweight(n.b.left) + 1;
        } else if (r == DB_NOTFOUND) {
            *idxp = this->nweight(n.b.left);
            copyout(value_len, value, &n);
            r = 0;
        }
        return r;
    } else {
        return this->find_internal_minus<dmtcmp_t, h>(n.b.left, extra, value_len, value, idxp);
    }
}
} // namespace toku
