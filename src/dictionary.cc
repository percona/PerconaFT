/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
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
#ident "$Id$"

#include <ctype.h>

#include <db.h>
#include "dictionary.h"
#include "ft/ft.h"
#include "ydb-internal.h"
#include "ydb_db.h"
#include "ydb_write.h"
#include "ydb_cursor.h"

void dictionary::create(uint64_t id, const char* dname) {
    m_id = id;
    m_dname = toku_strdup(dname);
}

void dictionary::destroy(){
    toku_free(m_dname);
}

const char* dictionary::get_dname() const {
    return m_dname;
}

uint64_t dictionary::get_id() const {
    return m_id;
}

// verifies that either all of the metadata files we are expecting exist
// or none do.
int dictionary_manager::validate_environment(DB_ENV* env, bool* valid_newenv) {
    int r;
    bool expect_newenv = false;        // set true if we expect to create a new env
    toku_struct_stat buf;
    char* path = NULL;

    // Test for persistent environment
    path = toku_construct_full_name(2, env->i->dir, toku_product_name_strings.environmentdictionary);
    assert(path);
    r = toku_stat(path, &buf);
    if (r == 0) {
        expect_newenv = false;  // persistent info exists
    }
    else {
        int stat_errno = get_error_errno();
        if (stat_errno == ENOENT) {
            expect_newenv = true;
            r = 0;
        }
        else {
            r = toku_ydb_do_error(env, stat_errno, "Unable to access persistent environment\n");
            assert(r);
        }
    }
    toku_free(path);

    // Test for fileops directory
    if (r == 0) {
        path = toku_construct_full_name(2, env->i->dir, toku_product_name_strings.fileopsdirectory);
        assert(path);
        r = toku_stat(path, &buf);
        if (r == 0) {  
            if (expect_newenv)  // fileops directory exists, but persistent env is missing
                r = toku_ydb_do_error(env, ENOENT, "Persistent environment is missing\n");
        }
        else {
            int stat_errno = get_error_errno();
            if (stat_errno == ENOENT) {
                if (!expect_newenv)  // fileops directory is missing but persistent env exists
                    r = toku_ydb_do_error(env, ENOENT, "Fileops directory is missing\n");
                else 
                    r = 0;           // both fileops directory and persistent env are missing
            }
            else {
                r = toku_ydb_do_error(env, stat_errno, "Unable to access fileops directory\n");
                assert(r);
            }
        }
        toku_free(path);
    }

    if (r == 0)
        *valid_newenv = expect_newenv;
    else 
        *valid_newenv = false;
    return r;
}

// Keys used in persistent environment dictionary:
// Following keys added in version 12
static const char * orig_env_ver_key = "original_version";
static const char * curr_env_ver_key = "current_version";  
// Following keys added in version 14, add more keys for future versions
static const char * creation_time_key         = "creation_time";

static char * get_upgrade_time_key(int version) {
    static char upgrade_time_key[sizeof("upgrade_v_time") + 12];
    {
        int n;
        n = snprintf(upgrade_time_key, sizeof(upgrade_time_key), "upgrade_v%d_time", version);
        assert(n >= 0 && n < (int)sizeof(upgrade_time_key));
    }
    return &upgrade_time_key[0];
}

static char * get_upgrade_footprint_key(int version) {
    static char upgrade_footprint_key[sizeof("upgrade_v_footprint") + 12];
    {
        int n;
        n = snprintf(upgrade_footprint_key, sizeof(upgrade_footprint_key), "upgrade_v%d_footprint", version);
        assert(n >= 0 && n < (int)sizeof(upgrade_footprint_key));
    }
    return &upgrade_footprint_key[0];
}

static char * get_upgrade_last_lsn_key(int version) {
    static char upgrade_last_lsn_key[sizeof("upgrade_v_last_lsn") + 12];
    {
        int n;
        n = snprintf(upgrade_last_lsn_key, sizeof(upgrade_last_lsn_key), "upgrade_v%d_last_lsn", version);
        assert(n >= 0 && n < (int)sizeof(upgrade_last_lsn_key));
    }
    return &upgrade_last_lsn_key[0];
}

// Requires: persistent environment dictionary is already open.
// Input arg is lsn of clean shutdown of previous version,
// or ZERO_LSN if no upgrade or if crash between log upgrade and here.
// NOTE: To maintain compatibility with previous versions, do not change the 
//       format of any information stored in the persistent environment dictionary.
//       For example, some values are stored as 32 bits, even though they are immediately
//       converted to 64 bits when read.  Do not change them to be stored as 64 bits.
//
int dictionary_manager::maybe_upgrade_persistent_environment_dictionary(
    DB_TXN * txn,
    LSN last_lsn_of_clean_shutdown_read_from_log
    )
{
    int r;
    DBT key, val;

    toku_fill_dbt(&key, curr_env_ver_key, strlen(curr_env_ver_key));
    toku_init_dbt(&val);
    r = toku_db_get(m_persistent_environment, txn, &key, &val, 0);
    assert(r == 0);
    uint32_t stored_env_version = toku_dtoh32(*(uint32_t*)val.data);
    if (stored_env_version > FT_LAYOUT_VERSION)
        r = TOKUDB_DICTIONARY_TOO_NEW;
    else if (stored_env_version < FT_LAYOUT_MIN_SUPPORTED_VERSION)
        r = TOKUDB_DICTIONARY_TOO_OLD;
    else if (stored_env_version < FT_LAYOUT_VERSION) {
        const uint32_t curr_env_ver_d = toku_htod32(FT_LAYOUT_VERSION);
        toku_fill_dbt(&key, curr_env_ver_key, strlen(curr_env_ver_key));
        toku_fill_dbt(&val, &curr_env_ver_d, sizeof(curr_env_ver_d));
        r = toku_db_put(m_persistent_environment, txn, &key, &val, 0, false);
        assert_zero(r);

        time_t upgrade_time_d = toku_htod64(time(NULL));
        uint64_t upgrade_footprint_d = toku_htod64(toku_log_upgrade_get_footprint());
        uint64_t upgrade_last_lsn_d = toku_htod64(last_lsn_of_clean_shutdown_read_from_log.lsn);
        for (int version = stored_env_version+1; version <= FT_LAYOUT_VERSION; version++) {
            uint32_t put_flag = DB_NOOVERWRITE;
            if (version <= FT_LAYOUT_VERSION_19) {
                // See #5902.
                // To prevent a crash (and any higher complexity code) we'll simply
                // silently not overwrite anything if it exists.
                // The keys existing for version <= 19 is not necessarily an error.
                // If this happens for versions > 19 it IS an error and we'll use DB_NOOVERWRITE.
                put_flag = DB_NOOVERWRITE_NO_ERROR;
            }


            char* upgrade_time_key = get_upgrade_time_key(version);
            toku_fill_dbt(&key, upgrade_time_key, strlen(upgrade_time_key));
            toku_fill_dbt(&val, &upgrade_time_d, sizeof(upgrade_time_d));
            r = toku_db_put(m_persistent_environment, txn, &key, &val, put_flag, false);
            assert_zero(r);

            char* upgrade_footprint_key = get_upgrade_footprint_key(version);
            toku_fill_dbt(&key, upgrade_footprint_key, strlen(upgrade_footprint_key));
            toku_fill_dbt(&val, &upgrade_footprint_d, sizeof(upgrade_footprint_d));
            r = toku_db_put(m_persistent_environment, txn, &key, &val, put_flag, false);
            assert_zero(r);

            char* upgrade_last_lsn_key = get_upgrade_last_lsn_key(version);
            toku_fill_dbt(&key, upgrade_last_lsn_key, strlen(upgrade_last_lsn_key));
            toku_fill_dbt(&val, &upgrade_last_lsn_d, sizeof(upgrade_last_lsn_d));
            r = toku_db_put(m_persistent_environment, txn, &key, &val, put_flag, false);
            assert_zero(r);
        }

    }
    return r;
}

int dictionary_manager::setup_persistent_environment(
    DB_ENV* env,
    bool newenv,
    DB_TXN* txn,
    int mode,
    LSN last_lsn_of_clean_shutdown_read_from_log
    ) 
{
    int r = 0;
    r = toku_db_create(&m_persistent_environment, env, 0);
    assert_zero(r);
    r = toku_db_use_builtin_key_cmp(m_persistent_environment);
    assert_zero(r);
    r = toku_db_open_iname(m_persistent_environment, txn, toku_product_name_strings.environmentdictionary, DB_CREATE, mode);
    if (r != 0) {
        r = toku_ydb_do_error(env, r, "Cant open persistent env\n");
        goto cleanup;
    }
    if (newenv) {
        // create new persistent_environment
        DBT key, val;
        uint32_t persistent_original_env_version = FT_LAYOUT_VERSION;
        const uint32_t environment_version = toku_htod32(persistent_original_env_version);

        toku_fill_dbt(&key, orig_env_ver_key, strlen(orig_env_ver_key));
        toku_fill_dbt(&val, &environment_version, sizeof(environment_version));
        r = toku_db_put(m_persistent_environment, txn, &key, &val, 0, false);
        assert_zero(r);

        toku_fill_dbt(&key, curr_env_ver_key, strlen(curr_env_ver_key));
        toku_fill_dbt(&val, &environment_version, sizeof(environment_version));
        r = toku_db_put(m_persistent_environment, txn, &key, &val, 0, false);
        assert_zero(r);

        time_t creation_time_d = toku_htod64(time(NULL));
        toku_fill_dbt(&key, creation_time_key, strlen(creation_time_key));
        toku_fill_dbt(&val, &creation_time_d, sizeof(creation_time_d));
        r = toku_db_put(m_persistent_environment, txn, &key, &val, 0, false);
        assert_zero(r);
    }
    else {
        r = maybe_upgrade_persistent_environment_dictionary(txn, last_lsn_of_clean_shutdown_read_from_log);
        assert_zero(r);
    }
cleanup:
    return r;
}

int dictionary_manager::get_persistent_environtment_cursor(DB_TXN* txn, DBC** c) {
    return toku_db_cursor(m_persistent_environment, txn, c, 0);
}

void dictionary_manager::create() {
    ZERO_STRUCT(m_mutex);
    toku_mutex_init(&m_mutex, nullptr);
    m_dictionary_map.create();
}

void dictionary_manager::destroy() {
    if (m_persistent_environment) {
        toku_db_close(m_persistent_environment);
    }
    m_dictionary_map.destroy();
    toku_mutex_destroy(&m_mutex);
}

int dictionary_manager::find_by_id(dictionary *const &dbi, const uint64_t &id) {
    return dbi->get_id() < id ? -1 : dbi->get_id() > id;
}

dictionary* dictionary_manager::find(const uint64_t id) {
    dictionary *dbi;
    int r = m_dictionary_map.find_zero<const uint64_t, find_by_id>(id, &dbi, nullptr);
    return r == 0 ? dbi : nullptr;
}

void dictionary_manager::add_db(dictionary* dbi) {
    int r = m_dictionary_map.insert<const uint64_t, find_by_id>(dbi, dbi->get_id(), nullptr);
    invariant_zero(r);
}

void dictionary_manager::remove_dictionary(dictionary* dbi) {
    toku_mutex_lock(&m_mutex);
    uint32_t idx;
    dictionary *found_dbi;
    const uint64_t id = dbi->get_id();
    int r = m_dictionary_map.find_zero<const uint64_t, find_by_id>(
        id,
        &found_dbi,
        &idx
        );
    invariant_zero(r);
    invariant(found_dbi == dbi);
    r = m_dictionary_map.delete_at(idx);
    invariant_zero(r);
    toku_mutex_unlock(&m_mutex);
}

dictionary* dictionary_manager::get_dictionary(const uint64_t id, const char * dname) {
    toku_mutex_lock(&m_mutex);
    dictionary *dbi = find(id);
    if (dbi == nullptr) {
        XCALLOC(dbi);
        dbi->create(id, dname);
    }
    toku_mutex_unlock(&m_mutex);
    return dbi;
}

