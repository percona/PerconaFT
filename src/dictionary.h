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

#pragma once

#include <db.h>
#include <toku_pthread.h>

#include <util/omt.h>
#include "ft/txn/txn.h"

class dictionary_manager;

class dictionary {
    char* m_dname;
    uint32_t m_refcount; // access protected by the mutex of dictionary_manager that is managing this dictionary
    dictionary_manager* m_mgr;
public:
    void create(const char* dname, dictionary_manager* manager);
    void destroy();
    void release();
    char* get_dname() const;

    friend class dictionary_manager;
};

class persistent_dictionary_manager {
private:
    DB* m_directory; // maps dname to dictionary id
    DB* m_inamedb; // maps dictionary id to iname
    int open_internal_db(DB* db, DB_TXN* txn, const char* dname, const char* iname, uint32_t flags);
    int setup_internal_db(DB** db, DB_ENV* env, DB_TXN* txn, const char* iname);
    
public:
    persistent_dictionary_manager() : 
        m_directory(nullptr),
        m_inamedb(nullptr)
    {
    }
    int initialize(DB_ENV* env, DB_TXN* txn);
    int get_directory_cursor(DB_TXN* txn, DBC** c);
    int get_iname(const char* dname, DB_TXN* txn, char** iname);
    int change_iname(DB_TXN* txn, const char* dname, const char* new_iname, uint32_t put_flags);
    int pre_acquire_fileops_lock(DB_TXN* txn, char* dname);
    int create_new_db(DB_TXN* txn, const char* dname, DB_ENV* env, bool is_db_hot_index);
    int remove(const char * dname, DB_TXN* txn);
    int rename(DB_TXN* txn, const char *old_dname, const char *new_dname);
    void destroy();
};

class dictionary_manager {
    // persistent environment stuff, should be own class
private:
    DB* m_persistent_environment;
    int maybe_upgrade_persistent_environment_dictionary(
        DB_TXN * txn,
        LSN last_lsn_of_clean_shutdown_read_from_log
        );
    int setup_persistent_environment(DB_ENV* env, bool newenv, DB_TXN* txn, LSN last_lsn_of_clean_shutdown_read_from_log);
public:
    int get_persistent_environment_cursor(DB_TXN* txn, DBC** c);



private:
    persistent_dictionary_manager pdm;
    
    // used to open DBs that will be used internally
    // in the dictionary_manager
    bool can_acquire_table_lock(DB_ENV *env, DB_TXN *txn, const char *iname_in_env);
    int open_internal_db(DB* db, DB_TXN* txn, const char* dname, const char* iname, uint32_t flags);
    int setup_internal_db(DB** db, DB_ENV* env, DB_TXN* txn, const char* iname);
    int validate_metadata_db(DB_ENV* env, const char* iname, bool expect_newenv);

public:
    dictionary_manager() : 
        m_persistent_environment(nullptr)
    {
    }
    int validate_environment(DB_ENV* env, bool* valid_newenv);
    int setup_metadata(
        DB_ENV* env,
        bool newenv,
        DB_TXN* txn,
        LSN last_lsn_of_clean_shutdown_read_from_log
        );
    int get_directory_cursor(DB_TXN* txn, DBC** c);
    int get_iname(const char* dname, DB_TXN* txn, char** iname);
    int get_iname_in_dbt(DB_ENV* env, DBT* dname_dbt, DBT* iname_dbt);
    // used in a part of bulk loading
    int change_iname(DB_TXN* txn, const char* dname, const char* new_iname, uint32_t put_flags);
    int pre_acquire_fileops_lock(DB_TXN* txn, char* dname);
    int rename(DB_ENV* env, DB_TXN *txn, const char *old_dname, const char *new_dname);
    int remove(const char * dname, DB_ENV* env, DB_TXN* txn);
    void create();
    void destroy();

    int open_db(DB* db, const char * dname, DB_TXN * txn, uint32_t flags);

    
private:
    // protects access the map
    toku_mutex_t m_mutex;
    toku::omt<dictionary *> m_dictionary_map;
    static int find_by_dname(dictionary *const &dbi, const char* const &dname);
    dictionary* find(const char* dname);
    void add_db(dictionary* dbi);
    void remove_dictionary(dictionary* dbi);
    dictionary* get_dictionary(const char * dname);
public:
    bool release_dictionary(dictionary* dbi);
    uint32_t num_open_dictionaries();
};
