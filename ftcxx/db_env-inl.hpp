/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <db.h>

#include "db.hpp"
#include "slice.hpp"

namespace ftcxx {

    typedef int (*slice_compare_func)(const Slice &desc, const Slice &key, const Slice &val);

    template<slice_compare_func slice_cmp>
    int wrapped_comparator(::DB *db, const DBT *a, const DBT *b) {
        return slice_cmp(DB(db).descriptor(), Slice(*a), Slice(*b));
    }

} // namespace ftcxx
