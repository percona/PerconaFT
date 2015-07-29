/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
#ident "$Id$"
/*======
This file is part of PerconaFT.


Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved.

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License, version 2,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.

----------------------------------------

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License, version 3,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.
======= */

#ident "Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved."

#pragma once

#include <portability/toku_config.h>

#if defined(__linux__) && USE_VALGRIND

# include <valgrind/helgrind.h>
# include <valgrind/drd.h>

# define TOKU_ANNOTATE_NEW_MEMORY(p, size) ANNOTATE_NEW_MEMORY(p, size)
# define TOKU_VALGRIND_HG_ENABLE_CHECKING(p, size) VALGRIND_HG_ENABLE_CHECKING(p, size)
# define TOKU_VALGRIND_HG_DISABLE_CHECKING(p, size) VALGRIND_HG_DISABLE_CHECKING(p, size)
# define TOKU_DRD_IGNORE_VAR(v) DRD_IGNORE_VAR(v)
# define TOKU_DRD_STOP_IGNORING_VAR(v) DRD_STOP_IGNORING_VAR(v)
# define TOKU_ANNOTATE_IGNORE_READS_BEGIN() ANNOTATE_IGNORE_READS_BEGIN()
# define TOKU_ANNOTATE_IGNORE_READS_END() ANNOTATE_IGNORE_READS_END()
# define TOKU_ANNOTATE_IGNORE_WRITES_BEGIN() ANNOTATE_IGNORE_WRITES_BEGIN()
# define TOKU_ANNOTATE_IGNORE_WRITES_END() ANNOTATE_IGNORE_WRITES_END()

/*
 * How to make helgrind happy about tree rotations and new mutex orderings:
 *
 * // Tell helgrind that we unlocked it so that the next call doesn't get a "destroyed a locked mutex" error.
 * // Tell helgrind that we destroyed the mutex.
 * VALGRIND_HG_MUTEX_UNLOCK_PRE(&locka);
 * VALGRIND_HG_MUTEX_DESTROY_PRE(&locka);
 *
 * // And recreate it.  It would be better to simply be able to say that the order on these two can now be reversed, because this code forgets all the ordering information for this mutex.
 * // Then tell helgrind that we have locked it again.
 * VALGRIND_HG_MUTEX_INIT_POST(&locka, 0);
 * VALGRIND_HG_MUTEX_LOCK_POST(&locka);
 *
 * When the ordering of two locks changes, we don't need tell Helgrind about do both locks.  Just one is good enough.
 */

# define TOKU_VALGRIND_RESET_MUTEX_ORDERING_INFO(mutex)  \
    VALGRIND_HG_MUTEX_UNLOCK_PRE(mutex); \
    VALGRIND_HG_MUTEX_DESTROY_PRE(mutex); \
    VALGRIND_HG_MUTEX_INIT_POST(mutex, 0); \
    VALGRIND_HG_MUTEX_LOCK_POST(mutex);

#else // !defined(__linux__) || !USE_VALGRIND

# define NVALGRIND 1
# define TOKU_ANNOTATE_NEW_MEMORY(p, size) ((void) 0)
# define TOKU_VALGRIND_HG_ENABLE_CHECKING(p, size) ((void) 0)
# define TOKU_VALGRIND_HG_DISABLE_CHECKING(p, size) ((void) 0)
# define TOKU_DRD_IGNORE_VAR(v)
# define TOKU_DRD_STOP_IGNORING_VAR(v)
# define TOKU_ANNOTATE_IGNORE_READS_BEGIN() ((void) 0)
# define TOKU_ANNOTATE_IGNORE_READS_END() ((void) 0)
# define TOKU_ANNOTATE_IGNORE_WRITES_BEGIN() ((void) 0)
# define TOKU_ANNOTATE_IGNORE_WRITES_END() ((void) 0)
# define TOKU_VALGRIND_RESET_MUTEX_ORDERING_INFO(mutex)
# define RUNNING_ON_VALGRIND (0U)

#endif

namespace data_race {

    template<typename T>
    class unsafe_read {
        const T &_val;
    public:
        unsafe_read(const T &val)
            : _val(val) {
            TOKU_VALGRIND_HG_DISABLE_CHECKING(&_val, sizeof _val);
            TOKU_ANNOTATE_IGNORE_READS_BEGIN();
        }
        ~unsafe_read() {
            TOKU_ANNOTATE_IGNORE_READS_END();
            TOKU_VALGRIND_HG_ENABLE_CHECKING(&_val, sizeof _val);
        }
        operator T() const {
            return _val;
        }
    };

} // namespace data_race

// Unsafely fetch and return a `T' from src, telling drd to ignore 
// racey access to src for the next sizeof(*src) bytes
template <typename T>
T toku_drd_unsafe_fetch(T *src) {
    return data_race::unsafe_read<T>(*src);
}

// Unsafely set a `T' value into *dest from src, telling drd to ignore 
// racey access to dest for the next sizeof(*dest) bytes
template <typename T>
void toku_drd_unsafe_set(T *dest, const T src) {
    TOKU_VALGRIND_HG_DISABLE_CHECKING(dest, sizeof *dest);
    TOKU_ANNOTATE_IGNORE_WRITES_BEGIN();
    *dest = src;
    TOKU_ANNOTATE_IGNORE_WRITES_END();
    TOKU_VALGRIND_HG_ENABLE_CHECKING(dest, sizeof *dest);
}
