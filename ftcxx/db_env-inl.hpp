/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <db.h>

#include "db.hpp"
#include "slice.hpp"

namespace ftcxx {

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

    typedef int (*slice_update_func)(const Slice &key, const Slice &old_val, const Slice &extra, SetvalFunc callback);

    template<slice_update_func slice_update>
    int wrapped_updater(const DBT *key, const DBT *old_val, const DBT *extra, void (*setval)(const DBT *, void *), void *setval_extra) {
        return slice_update(Slice(*key), Slice(*old_val), Slice(*extra), SetvalFunc(setval, setval_extra));
    }

} // namespace ftcxx
