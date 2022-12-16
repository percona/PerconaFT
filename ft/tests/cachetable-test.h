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

#include "cachetable/cachetable-internal.h"

//
// Dummy callbacks for checkpointing
//
static void dummy_log_fassociate(CACHEFILE UU(cf), void* UU(p)) { }
static void dummy_close_usr(CACHEFILE UU(cf), int UU(i), void* UU(p), bool UU(b), LSN UU(lsn))  { }
static void dummy_free_usr(CACHEFILE UU(cf), void* UU(p))  { }
static void dummy_chckpnt_usr(CACHEFILE UU(cf), int UU(i), void* UU(p)) { }
static void dummy_begin(LSN UU(lsn), void* UU(p)) { }
static void dummy_end(CACHEFILE UU(cf), int UU(i), void* UU(p)) { }
static void dummy_note_pin(CACHEFILE UU(cf), void* UU(p)) { }
static void dummy_note_unpin(CACHEFILE UU(cf), void* UU(p)) { }

//
// Helper function to set dummy functions in given cachefile.
//
static UU() void
create_dummy_functions(CACHEFILE cf)
{
  void *ud = NULL;
  toku_cachefile_set_userdata(cf, ud, &dummy_log_fassociate, &dummy_close_usr,
                              &dummy_free_usr, &dummy_chckpnt_usr, &dummy_begin,
                              &dummy_end, &dummy_note_pin, &dummy_note_unpin,
                              &dummy_note_pin, &dummy_note_unpin);
};
