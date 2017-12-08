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

#include <util/partitioned_counter.h>
#include <util/constexpr.h>
#include <portability/toku_race_tools.h>

#define TOKUFT_STATUS_INIT(array,k,c,t,l,inc) do {   \
    array.status[k].keyname = #k;                    \
    array.status[k].columnname = #c;                 \
    array.status[k].type    = t;                     \
    array.status[k].legend  = l;                     \
    static_assert((inc) != 0, "Var must be included in at least one place"); \
    constexpr_static_assert(strcmp(#c, "NULL") && strcmp(#c, "0"),           \
            "Use nullptr for no column name instead of NULL, 0, etc...");    \
    constexpr_static_assert((inc) == TOKU_ENGINE_STATUS                      \
            || strcmp(#c, "nullptr"), "Missing column name.");               \
    constexpr_static_assert(static_strncasecmp(#c, "TOKU", strlen("TOKU")),  \
                  "Do not start column names with toku."); \
    array.status[k].include = static_cast<toku_engine_status_include_type>(inc);  \
    if (t == PARCOUNT) {                                               \
        array.status[k].value.parcount = create_partitioned_counter(); \
    }                                                                  \
} while (0)

#define TOKUFT_STATUS_ADD(v,n) (v) += (n)
#define TOKUFT_STATUS_INC(v) (v)++
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#undef TOKUFT_STATUS_ADD
#define TOKUFT_STATUS_ADD(v,n) toku_unsafe_add(&(v),(n))
#undef TOKUFT_STATUS_INC
#define TOKUFT_STATUS_INC(v) toku_unsafe_inc(&(v))
#endif
#endif
