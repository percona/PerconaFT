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

    class SetvalFunc {
        void (*_setval)(const DBT *, void *);
        void *_extra;
    public:
        SetvalFunc(void (*setval)(const DBT *, void *), void *extra)
            : _setval(setval),
              _extra(extra)
        {}
        void operator()(const Slice &new_val) {
            DBT vdbt = new_val.dbt();
            _setval(&vdbt, _extra);
        }
    };

    typedef int (*slice_update_func)(const Slice &desc, const Slice &key, const Slice &old_val, const Slice &extra, SetvalFunc callback);

    template<slice_update_func slice_update>
    int wrapped_updater(::DB *db, const DBT *key, const DBT *old_val, const DBT *extra, void (*setval)(const DBT *, void *), void *setval_extra) {
        return slice_update(DB(db).descriptor(), Slice(*key), Slice(*old_val), Slice(*extra), SetvalFunc(setval, setval_extra));
    }

} // namespace ftcxx
