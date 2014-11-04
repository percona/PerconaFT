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
#include <locktree/locktree.h>
#include "ydb_row_lock.h"
#include "iname_helpers.h"


// set the descriptor and cmp_descriptor to the
// descriptors from the given ft, updating the
// locktree's descriptor pointer if necessary
static void
db_set_descriptors(DB *db, FT_HANDLE ft_handle) {
    const toku::comparator &cmp = toku_ft_get_comparator(ft_handle);
    db->descriptor = toku_ft_get_descriptor(ft_handle);
    db->cmp_descriptor = toku_ft_get_cmp_descriptor(ft_handle);
    invariant(db->cmp_descriptor == cmp.get_descriptor());
}

static int toku_db_open_iname(DB * db, DB_TXN * txn, const char *iname_in_env, uint32_t flags) {
    //Set comparison functions if not yet set.
    HANDLE_READ_ONLY_TXN(txn);
    // we should always have SOME environment comparison function
    // set, even if it is the default one set in toku_env_create
    invariant(db->dbenv->i->bt_compare);
    bool need_locktree = (bool)((db->dbenv->i->open_flags & DB_INIT_LOCK) &&
                                (db->dbenv->i->open_flags & DB_INIT_TXN));

    int is_db_excl    = flags & DB_EXCL;    flags&=~DB_EXCL;
    int is_db_create  = flags & DB_CREATE;  flags&=~DB_CREATE;
     // unknown or conflicting flags are bad
     if (is_db_excl && !is_db_create) {
        return EINVAL;
    }

    if (db_opened(db)) {
        return EINVAL;              /* It was already open. */
    }
     
    FT_HANDLE ft_handle = db->i->ft_handle;
    int r = toku_ft_handle_open(ft_handle, iname_in_env,
                      is_db_create, is_db_excl,
                      db->dbenv->i->cachetable,
                      txn ? db_txn_struct_i(txn)->tokutxn : nullptr);
    if (r != 0) {
        goto out;
    }

    // if the dictionary was opened as a blackhole, mark the
    // fractal tree as blackhole too.
    if (flags & DB_BLACKHOLE) {
        toku_ft_set_blackhole(ft_handle);
    }

    db->i->opened = 1;

    // now that the handle has successfully opened, a valid descriptor
    // is in the ft. we need to set the db's descriptor pointers
    db_set_descriptors(db, ft_handle);

    if (need_locktree) {
        db->i->dict_id = toku_ft_get_dictionary_id(db->i->ft_handle);
        struct lt_on_create_callback_extra on_create_extra = {
            .txn = txn,
            .ft_handle = db->i->ft_handle,
        };
        db->i->lt = db->dbenv->i->ltm.get_lt(db->i->dict_id,
                                             toku_ft_get_comparator(db->i->ft_handle),
                                             &on_create_extra);
        if (db->i->lt == nullptr) {
            r = errno;
            if (r == 0) {
                r = EINVAL;
            }
            goto out;
        }
    }
    r = 0;
 
out:
    if (r != 0) {
        db->i->dict_id = DICTIONARY_ID_NONE;
        db->i->opened = 0;
        if (db->i->lt) {
            db->dbenv->i->ltm.release_lt(db->i->lt);
            db->i->lt = nullptr;
        }
    }
    return r;
}

static int open_internal_db(DB* db, DB_TXN* txn, const dictionary_info* dinfo, uint32_t flags) {    
    int r = toku_db_open_iname(db, txn, dinfo->iname, flags);
    if (r == 0) {
        dictionary *dbi = NULL;
        XCALLOC(dbi);
        dbi->create(dinfo, NULL);
        db->i->dict = dbi;
    }
    return r;
}

void dictionary::create(const dictionary_info* dinfo, inmemory_dictionary_manager* manager) {
    if (dinfo->dname) {
        m_dname = toku_strdup(dinfo->dname);
    }
    else {
        m_dname = nullptr;
    }
    m_id = dinfo->id;
    m_refcount = 0;
    m_mgr = manager;
}

void dictionary::destroy(){
    invariant(m_refcount == 0);
    if (m_dname) {
        toku_free(m_dname);
    }
}

void dictionary::release(){
    bool do_destroy = false;
    if (m_mgr) {
        do_destroy = m_mgr->release_dictionary(this);
    }
    else {
        invariant(m_refcount == 0);
        do_destroy = true;
    }
    if (do_destroy) {
        destroy();
    }
}

char* dictionary::get_dname() const {
    return m_dname;
}

uint64_t dictionary::get_id() const {
    return m_id;
}


//////////////////////////////////////////
// persistent_dictionary_manager methods
//

int persistent_dictionary_manager::setup_internal_db(DB** db, DB_ENV* env, DB_TXN* txn, const char* iname, uint64_t id) {
    int r = toku_db_create(db, env, 0);
    assert_zero(r);
    toku_db_use_builtin_key_cmp(*db);
    dictionary_info dinfo;
    dinfo.iname = toku_strdup(iname);
    dinfo.id = id;
    r = open_internal_db(*db, txn, &dinfo, DB_CREATE);
    if (r != 0) {
        r = toku_ydb_do_error(env, r, "Cant open %s\n", iname);
    }
    dinfo.destroy();
    return r;
}

int persistent_dictionary_manager::initialize(DB_ENV* env, DB_TXN* txn) {
    toku_mutex_init(&m_mutex, nullptr);
    DBC* c = NULL;
    int r = setup_internal_db(&m_directory, env, txn, toku_product_name_strings.fileopsdirectory, DIRECTORY_ID);
    if (r != 0) goto cleanup;
    r = setup_internal_db(&m_inamedb, env, txn, toku_product_name_strings.fileopsinames, INAME_ID);
    if (r != 0) goto cleanup;
    // get the last entry in m_inamesdb, that has the current max used id
    // set m_next_id to that value plus one
    r = m_inamedb->cursor(m_inamedb, txn, &c, DB_SERIALIZABLE);
    if (r != 0) goto cleanup;
    DBT key,val;
    toku_init_dbt(&key);
    toku_init_dbt(&val);
    r = c->c_get(c, &key, &val, DB_LAST);
    if (r == DB_NOTFOUND) {
        // we have nothing in the directory,
        // which is a valid case
        m_next_id = m_min_user_id;
        r = 0;
        goto cleanup;
    }
    if (r != 0) goto cleanup;
    if (key.size != sizeof(uint64_t)) {
        printf("Unexpected size found for last entry in m_inamedb %d\n", key.size);
        r = EINVAL;
        goto cleanup;
    }
    m_next_id = (*(uint64_t *)key.data) + 1;
    if (m_next_id < m_min_user_id) {
        printf("Unexpected low id found in last entry in m_inamedb %" PRIu64 "\n", m_next_id - 1);
        r = EINVAL;
        goto cleanup;
    }

cleanup:
    if (c) {
        int chk = c->c_close(c);
        assert_zero(chk);
    }
    return r;
}

int persistent_dictionary_manager::get_dinfo(const char* dname, DB_TXN* txn, dictionary_info* dinfo) {
    DBT dname_dbt;
    DBT iname_dbt;
    DBT id_dbt;
    uint64_t id;
    toku_fill_dbt(&dname_dbt, dname, strlen(dname)+1);
    toku_init_dbt_flags(&iname_dbt, DB_DBT_MALLOC);
    toku_init_dbt(&id_dbt);
    id_dbt.data = &id;
    id_dbt.ulen = sizeof(id);
    id_dbt.flags = DB_DBT_USERMEM;

    // get id
    int r = toku_db_get(m_directory, txn, &dname_dbt, &id_dbt, DB_SERIALIZABLE);
    if (r != 0) goto cleanup;
    dinfo->id = id;

    // get iname
    r = toku_db_get(m_inamedb, txn, &id_dbt, &iname_dbt, DB_SERIALIZABLE);  // allocates memory for iname
    if (r == DB_NOTFOUND) {
        // we have an inconsistent state. This is a bad bug
        printf("We found an id for the dname, but not the iname\n");
        printf("dname %s\n", dname);
        printf("id: %" PRIu64 "\n", id);
        goto cleanup;
    }
    if (r != 0) goto cleanup;

    dinfo->iname = (char *) iname_dbt.data;
    dinfo->dname = toku_strdup(dname);
cleanup:
    return r;
}

int persistent_dictionary_manager::pre_acquire_fileops_lock(DB_TXN* txn, char* dname) {
    DBT key_in_directory = { .data = dname, .size = (uint32_t) strlen(dname)+1 };
    //Left end of range == right end of range (point lock)
    return toku_db_get_range_lock(m_directory, txn,
            &key_in_directory, &key_in_directory,
            toku::lock_request::type::WRITE);
}

int persistent_dictionary_manager::change_iname(DB_TXN* txn, uint64_t id, const char* new_iname, uint32_t put_flags) {
    DBT id_dbt;  // holds id
    toku_fill_dbt(&id_dbt, &id, sizeof(id));
    DBT iname_dbt;  // holds new iname
    toku_fill_dbt(&iname_dbt, new_iname, strlen(new_iname) + 1);      // iname_in_env goes in directory
    return toku_db_put(m_inamedb, txn, &id_dbt, &iname_dbt, put_flags, true);
}

int persistent_dictionary_manager::rename(DB_TXN* txn, const char *old_dname, const char *new_dname) {
    dictionary_info dinfo;
    dictionary_info dummy; // used to verify an iname does not already exist for new_dname
    int r = get_dinfo(old_dname, txn, &dinfo);
    if (r != 0) {
        if (r == DB_NOTFOUND) {
            r = ENOENT;
        }
        goto exit;
    }
    // verify that newname does not already exist
    r = get_dinfo(new_dname, txn, &dummy);
    if (r == 0) {
        r = EEXIST;
        goto exit;
    }
    if (r != DB_NOTFOUND) {
        goto exit;
    }
    // remove old (dname,iname) and insert (newname,iname) in directory
    DBT old_dname_dbt;
    toku_fill_dbt(&old_dname_dbt, old_dname, strlen(old_dname)+1);
    DBT new_dname_dbt;
    toku_fill_dbt(&new_dname_dbt, new_dname, strlen(new_dname)+1);
    DBT id_dbt;
    toku_fill_dbt(&id_dbt, &dinfo.id, sizeof(dinfo.id));
    r = toku_db_del(m_directory, txn, &old_dname_dbt, DB_DELETE_ANY, true);
    if (r != 0) { goto exit; }
    r = toku_db_put(m_directory, txn, &new_dname_dbt, &id_dbt, 0, true);
    if (r != 0) { goto exit; }

exit:
    dinfo.destroy();
    dummy.destroy();
    return r;
}

int persistent_dictionary_manager::remove(const char * dname, DB_TXN* txn) {
    dictionary_info dinfo;
    int r = get_dinfo(dname, txn, &dinfo);
    if (r != 0) {
        if (r == DB_NOTFOUND) {
            r = ENOENT;
        }
        goto exit;
    }
    // verify that dname is what we expect it to be
    if (strcmp(dname, dinfo.dname) != 0) {
        printf("Error in remove, different dname found expected: %s, actual: %s", dname, dinfo.dname);
    }
    DBT dname_dbt;
    toku_fill_dbt(&dname_dbt, dinfo.dname, strlen(dinfo.dname)+1);
    DBT id_dbt;
    toku_fill_dbt(&id_dbt, &dinfo.id, sizeof(dinfo.id));
    // remove (dname,id) from directory
    r = toku_db_del(m_directory, txn, &dname_dbt, DB_DELETE_ANY, true);
    if (r != 0) { goto exit; }

    r = toku_db_del(m_inamedb, txn, &id_dbt, DB_DELETE_ANY, true);
    if (r != 0) { goto exit; }

exit:
    dinfo.destroy();
    return r;
}

int  persistent_dictionary_manager::create_new_db(DB_TXN* txn, const char* dname, DB_ENV* env, bool is_db_hot_index, dictionary_info* dinfo) {
    dinfo->dname = (dname) ? toku_strdup(dname) : nullptr;
    dinfo->iname = create_new_iname(dname, env, txn, NULL);
    {
        toku_mutex_lock(&m_mutex);
        dinfo->id = m_next_id;
        m_next_id++;
        toku_mutex_unlock(&m_mutex);
    }
    uint32_t put_flags = 0 | ((is_db_hot_index) ? DB_PRELOCKED_WRITE : 0);

    // set entry in directory
    DBT dname_dbt;  // holds new iname
    toku_fill_dbt(&dname_dbt, dname, strlen(dname) + 1);
    DBT id_dbt;  // holds id
    toku_fill_dbt(&id_dbt, &dinfo->id, sizeof(dinfo->id));
    int r = toku_db_put(m_directory, txn, &dname_dbt, &id_dbt, put_flags, true);
    if (r != 0) goto exit;

    // set the iname
    r = change_iname(txn, dinfo->id, dinfo->iname, put_flags);
    if (r != 0) goto exit;
    
exit:
    return r;
}

void persistent_dictionary_manager::destroy() {
    if (m_directory) {
        toku_db_close(m_directory);
    }
    if (m_inamedb) {
        toku_db_close(m_inamedb);
    }
    toku_mutex_destroy(&m_mutex);
}

int persistent_dictionary_manager::get_directory_cursor(DB_TXN* txn, DBC** c) {
    assert(false); // TODO: this will not work, need to rethink this API
    return toku_db_cursor(m_directory, txn, c, 0);
}

// get the iname for the given dname and set it in the variable iname
// responsibility of caller to free iname
int persistent_dictionary_manager::get_iname(const char* dname, DB_TXN* txn, char** iname) {
    dictionary_info dinfo;
    int r = get_dinfo(dname, txn, &dinfo);
    if (r == 0) {
        *iname = toku_strdup(dinfo.iname);
    }
    dinfo.destroy();
    return r;
}

int dictionary_manager::validate_metadata_db(DB_ENV* env, const char* iname, bool expect_newenv) {
    toku_struct_stat buf;
    char* path = NULL;
    path = toku_construct_full_name(2, env->i->dir, iname);
    assert(path);
    int r = toku_stat(path, &buf);
    if (r == 0) {  
        if (expect_newenv)  // directory exists, but persistent env is missing
            r = toku_ydb_do_error(env, ENOENT, "Persistent environment is missing\n");
    }
    else {
        int stat_errno = get_error_errno();
        if (stat_errno == ENOENT) {
            if (!expect_newenv)  // fileops directory is missing but persistent env exists
                r = toku_ydb_do_error(env, ENOENT, "Missing: %s\n", iname);
            else 
                r = 0;           // both fileops directory and persistent env are missing
        }
        else {
            r = toku_ydb_do_error(env, stat_errno, "Unable to access %s\n", iname);
            assert(r);
        }
    }
    toku_free(path);
    return 0;
}

// verifies that either all of the metadata files we are expecting exist
// or none do.
int dictionary_manager::validate_environment(DB_ENV* env, bool* valid_newenv) {
    int r;
    *valid_newenv = false;
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
    r = validate_metadata_db(env, toku_product_name_strings.fileopsdirectory, expect_newenv);
    if (r != 0) goto cleanup;
    r = validate_metadata_db(env, toku_product_name_strings.fileopsinames, expect_newenv);
    if (r != 0) goto cleanup;

    *valid_newenv = expect_newenv;
cleanup:
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
    DBT key, val, put_val;

    toku_fill_dbt(&key, curr_env_ver_key, strlen(curr_env_ver_key));
    toku_init_dbt(&val);
    toku_init_dbt(&put_val);
    toku_init_dbt_flags(&val, DB_DBT_MALLOC);
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
        toku_fill_dbt(&put_val, &curr_env_ver_d, sizeof(curr_env_ver_d));
        r = toku_db_put(m_persistent_environment, txn, &key, &put_val, 0, false);
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
            toku_fill_dbt(&put_val, &upgrade_time_d, sizeof(upgrade_time_d));
            r = toku_db_put(m_persistent_environment, txn, &key, &put_val, put_flag, false);
            assert_zero(r);

            char* upgrade_footprint_key = get_upgrade_footprint_key(version);
            toku_fill_dbt(&key, upgrade_footprint_key, strlen(upgrade_footprint_key));
            toku_fill_dbt(&put_val, &upgrade_footprint_d, sizeof(upgrade_footprint_d));
            r = toku_db_put(m_persistent_environment, txn, &key, &put_val, put_flag, false);
            assert_zero(r);

            char* upgrade_last_lsn_key = get_upgrade_last_lsn_key(version);
            toku_fill_dbt(&key, upgrade_last_lsn_key, strlen(upgrade_last_lsn_key));
            toku_fill_dbt(&put_val, &upgrade_last_lsn_d, sizeof(upgrade_last_lsn_d));
            r = toku_db_put(m_persistent_environment, txn, &key, &put_val, put_flag, false);
            assert_zero(r);
        }

    }
    if (val.data) toku_free(val.data);
    return r;
}

int dictionary_manager::setup_persistent_environment(
    DB_ENV* env,
    bool newenv,
    DB_TXN* txn,
    LSN last_lsn_of_clean_shutdown_read_from_log
    ) 
{
    int r = 0;
    r = toku_db_create(&m_persistent_environment, env, 0);
    assert_zero(r);
    toku_db_use_builtin_key_cmp(m_persistent_environment);
    // don't need to destroy it because we are not copying data into it
    dictionary_info dinfo;
    dinfo.iname = toku_product_name_strings.environmentdictionary;
    dinfo.id = ENV_ID;
    r = open_internal_db(m_persistent_environment, txn, &dinfo, DB_CREATE);
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

int dictionary_manager::setup_metadata(
    DB_ENV* env,
    bool newenv,
    DB_TXN* txn,
    LSN last_lsn_of_clean_shutdown_read_from_log
    )
{
    int r = 0;
    r = setup_persistent_environment(
        env,
        newenv,
        txn,
        last_lsn_of_clean_shutdown_read_from_log
        );
    if (r != 0) goto cleanup;
    r = pdm.initialize(env, txn);
    if (r != 0) goto cleanup;
    
cleanup:
    return r;
}


int dictionary_manager::get_persistent_environment_cursor(DB_TXN* txn, DBC** c) {
    return toku_db_cursor(m_persistent_environment, txn, c, 0);
}

// this is a test function, ONLY. Should not be used in production
int dictionary_manager::get_iname_in_dbt(DB_ENV* env UU(), DBT* dname_dbt UU(), DBT* iname_dbt UU()) {
    if (!iname_dbt->flags & DB_DBT_MALLOC) { // assuming all tests call with this
        return EINVAL;
    }
    DB_TXN* txn = NULL;
    int r = toku_txn_begin(env, NULL, &txn, 0);
    assert_zero(r);
    r = pdm.get_iname((char *)dname_dbt->data, txn, (char **)(&iname_dbt->data));
    if (r == 0) {
        iname_dbt->size = strlen((char*)iname_dbt->data) + 1;
    }
    int ret = locked_txn_commit(txn, 0);
    assert_zero(ret);
    return r;
}

// see if we can acquire a table lock for the given dname.
// requires: write lock on dname in the directory. dictionary
//          open, close, and begin checkpoint cannot occur.
// returns: true if we could open, lock, and close a dictionary
//          with the given dname, false otherwise.
bool
dictionary_manager::can_acquire_table_lock(DB_ENV *env, DB_TXN *txn, const dictionary_info *dinfo) {
    int r;
    bool got_lock = false;
    DB *db;

    r = toku_db_create(&db, env, 0);
    assert_zero(r);
    r = open_internal_db(db, txn, dinfo, 0);
    assert_zero(r);
    r = toku_db_pre_acquire_table_lock(db, txn);
    if (r == 0) {
        got_lock = true;
    } else {
        got_lock = false;
    }
    toku_db_close(db);

    return got_lock;
}

int dictionary_manager::rename(DB_ENV* env, DB_TXN *txn, const char *old_dname, const char *new_dname) {
    // TODO: possibly do an early check to see if handles open
    dictionary_info dinfo;
    dictionary* old_dict = NULL;
    dictionary* new_dict = NULL;
    int r = pdm.get_dinfo(old_dname, txn, &dinfo);
    if (r != 0) {
        if (r == DB_NOTFOUND) {
            r = ENOENT;
        }
        goto exit;
    }
    // perform the rename in metadata dictionaries
    r = pdm.rename(txn, old_dname, new_dname);
    if (r != 0) goto exit;

    // do some checks to make sure that
    // we can do perform the operation, namely,
    // make sure no open handles exist and make sure
    // we can grab a table lock on the dictionary
    old_dict = idm.find(old_dname);
    new_dict = idm.find(new_dname);
    
    if (old_dict) {
        printf("Cannot rename dictionary with an open handle.\n");
        r = EINVAL;
        goto exit;
    }
    if (new_dict) {
        printf("Cannot rename dictionary; Dictionary with target name has an open handle.\n");
        r = EINVAL;
        goto exit;
    }
    // the dinfo below holds the old dname, even though the rename has happened
    // that should be ok, because the locktree does not depend on the dname
    if (txn && !can_acquire_table_lock(env, txn, &dinfo)) {
        r = DB_LOCK_NOTGRANTED;
    }

exit:
    dinfo.destroy();
    return r;
}

int dictionary_manager::remove(const char * dname, DB_ENV* env, DB_TXN* txn) {
    // TODO: perhaps add a fast path of bailing if open handles exist
    DB *db = NULL;
    dictionary_info dinfo;
    int r = pdm.get_dinfo(dname, txn, &dinfo);
    if (r != 0) goto exit;
    r = pdm.remove(dname, txn);
    if (r != 0) goto exit;

    r = toku_db_create(&db, env, 0);
    lazy_assert_zero(r);
    r = open_internal_db(db, txn, &dinfo, 0);
    if (txn && r) {
        if (r == EMFILE || r == ENFILE)
            r = toku_ydb_do_error(env, r, "toku dbremove failed because open file limit reached\n");
        else
            r = toku_ydb_do_error(env, r, "toku dbremove failed\n");
        goto exit;
    }
    if (txn) {
        dictionary* old_dict = NULL;
        old_dict = idm.find(dname);
        // Now that we have a writelock on dname, verify that there are still no handles open. (to prevent race conditions)
        if (old_dict) {
            r = toku_ydb_do_error(env, EINVAL, "Cannot remove dictionary with an open handle.\n");
            goto exit;
        }
        // we know a live db handle does not exist.
        //
        // use the internally opened db to try and get a table lock
        //
        // if we can't get it, then some txn needs the ft and we
        // should return lock not granted.
        //
        // otherwise, we're okay in marking this ft as remove on
        // commit. no new handles can open for this dictionary
        // because the txn has directory write locks on the dname
        r = toku_db_pre_acquire_table_lock(db, txn);
        if (r != 0) {
            r = DB_LOCK_NOTGRANTED;
            goto exit;
        }
        // The ft will be unlinked when the txn commits
        toku_ft_unlink_on_commit(db->i->ft_handle, db_txn_struct_i(txn)->tokutxn);
    }
    else {
        // unlink the ft without a txn
        toku_ft_unlink(db->i->ft_handle);
    }

exit:
    dinfo.destroy();
    if (db) {
        toku_db_close(db);
    }
    return r;
}

int dictionary_manager::open_db(
    DB* db,
    const char * dname,
    DB_TXN * txn,
    uint32_t flags
    )
{
    int r = 0;
    int is_db_excl = flags & DB_EXCL;
    int is_db_create = flags & DB_CREATE;
    int is_db_hot_index  = flags & DB_IS_HOT_INDEX;
    
    assert(!db_opened(db));
    dictionary_info dinfo;
    r = pdm.get_dinfo(dname, txn, &dinfo);
    if (r == DB_NOTFOUND && !is_db_create) {
        r = ENOENT;
    } else if (r==0 && is_db_excl) {
        r = EEXIST;
    } else if (r == DB_NOTFOUND) {
        r = pdm.create_new_db(txn, dname, db->dbenv, is_db_hot_index, &dinfo);
    }
    
    // we now have an iname
    if (r == 0) {
        r = toku_db_open_iname(db, txn, dinfo.iname, flags);
        if (r == 0) {
            // now that the directory has been updated, create the dictionary
            db->i->dict = idm.get_dictionary(&dinfo);
        }
    }
    
    dinfo.destroy();
    return r;
}

void dictionary_manager::create() {
    idm.create();
}

void dictionary_manager::destroy() {
    if (m_persistent_environment) {
        toku_db_close(m_persistent_environment);
    }
    pdm.destroy();
    idm.destroy();
}

///////////////////////////////////////////////
//
// inmemory_dictionary_manager methods
//

void inmemory_dictionary_manager::create() {
    ZERO_STRUCT(m_mutex);
    toku_mutex_init(&m_mutex, nullptr);
    m_dictionary_map.create();
}

void inmemory_dictionary_manager::destroy() {
    m_dictionary_map.destroy();
    toku_mutex_destroy(&m_mutex);
}

int inmemory_dictionary_manager::find_by_dname(dictionary *const &dbi, const char* const &dname) {
    return strcmp(dbi->get_dname(), dname);
}

dictionary* inmemory_dictionary_manager::find_locked(const char* dname) {
    dictionary *dbi;
    int r = m_dictionary_map.find_zero<const char *, find_by_dname>(dname, &dbi, nullptr);
    return r == 0 ? dbi : nullptr;
}

void inmemory_dictionary_manager::add_db(dictionary* dbi) {
    int r = m_dictionary_map.insert<const char *, find_by_dname>(dbi, dbi->get_dname(), nullptr);
    invariant_zero(r);
}

void inmemory_dictionary_manager::remove_dictionary(dictionary* dbi) {
    uint32_t idx;
    dictionary *found_dbi;
    const char* dname = dbi->get_dname();
    int r = m_dictionary_map.find_zero<const char *, find_by_dname>(
        dname,
        &found_dbi,
        &idx
        );
    invariant_zero(r);
    invariant(found_dbi == dbi);
    r = m_dictionary_map.delete_at(idx);
    invariant_zero(r);
}

bool inmemory_dictionary_manager::release_dictionary(dictionary* dbi) {
    bool do_destroy = false;
    toku_mutex_lock(&m_mutex);
    dbi->m_refcount--;
    if (dbi->m_refcount == 0) {
        remove_dictionary(dbi);
        do_destroy = true;
    }
    toku_mutex_unlock(&m_mutex);
    return do_destroy;
}

uint32_t inmemory_dictionary_manager::num_open_dictionaries() {
    toku_mutex_lock(&m_mutex);
    uint32_t retval =  m_dictionary_map.size();
    toku_mutex_unlock(&m_mutex);
    return retval;    
}

dictionary* inmemory_dictionary_manager::get_dictionary(const dictionary_info* dinfo) {
    toku_mutex_lock(&m_mutex);
    dictionary *dbi = find_locked(dinfo->dname);
    if (dbi == nullptr) {
        XCALLOC(dbi);
        dbi->create(dinfo, this);
        add_db(dbi);
    }
    dbi->m_refcount++;
    toku_mutex_unlock(&m_mutex);
    return dbi;
}

