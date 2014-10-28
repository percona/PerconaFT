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

extern const char *toku_patent_string;

#include <db.h>
#include <errno.h>
#include <string.h>

#include "portability/memory.h"
#include "portability/toku_assert.h"
#include "portability/toku_portability.h"
#include "portability/toku_pthread.h"
#include "portability/toku_stdlib.h"

#include "ft/cachetable/cachetable.h"
#include "ft/cachetable/checkpoint.h"
#include "ft/logger/log.h"
#include "ft/loader/loader.h"
#include "ft/log_header.h"
#include "ft/ft.h"
#include "ft/txn/txn_manager.h"
#include "src/ydb.h"
#include "src/ydb-internal.h"
#include "src/ydb_cursor.h"
#include "src/ydb_row_lock.h"
#include "src/ydb_env_func.h"
#include "src/ydb_db.h"
#include "src/ydb_write.h"
#include "src/ydb_txn.h"
#include "src/loader.h"
#include "src/indexer.h"
#include "util/status.h"
#include "util/context.h"

#include "ydb_env_open_close.h"

static void
env_setup_real_dir(DB_ENV *env, char **real_dir, const char *nominal_dir) {
    toku_free(*real_dir);
    *real_dir = NULL;

    assert(env->i->dir);
    if (nominal_dir) 
        *real_dir = toku_construct_full_name(2, env->i->dir, nominal_dir);
    else
        *real_dir = toku_strdup(env->i->dir);
}

static void
env_setup_real_data_dir(DB_ENV *env) {
    env_setup_real_dir(env, &env->i->real_data_dir, env->i->data_dir);
}

static void
env_setup_real_log_dir(DB_ENV *env) {
    env_setup_real_dir(env, &env->i->real_log_dir, env->i->lg_dir);
}

static void
env_setup_real_tmp_dir(DB_ENV *env) {
    env_setup_real_dir(env, &env->i->real_tmp_dir, env->i->tmp_dir);
}

static void keep_cachetable_callback (DB_ENV *env, CACHETABLE cachetable)
{
    env->i->cachetable = cachetable;
}

static int 
ydb_do_recovery (DB_ENV *env) {
    assert(env->i->real_log_dir);
    int r = tokuft_recover(env,
                           toku_keep_prepared_txn_callback,
                           keep_cachetable_callback,
                           env->i->logger,
                           env->i->dir, env->i->real_log_dir, env->i->bt_compare,
                           env->i->update_function,
                           env->i->generate_row_for_put, env->i->generate_row_for_del,
                           env->i->cachetable_size);
    return r;
}

static int 
needs_recovery (DB_ENV *env) {
    assert(env->i->real_log_dir);
    int recovery_needed = tokuft_needs_recovery(env->i->real_log_dir, true);
    return recovery_needed ? DB_RUNRECOVERY : 0;
}

// The version of the environment (on disk) is the version of the recovery log.  
// If the recovery log is of the current version, then there is no upgrade to be done.  
// If the recovery log is of an old version, then replacing it with a new recovery log
// of the current version is how the upgrade is done.  
// Note, the upgrade procedure takes a checkpoint, so we must release the ydb lock.
static int
ydb_maybe_upgrade_env (DB_ENV *env, LSN * last_lsn_of_clean_shutdown_read_from_log, bool * upgrade_in_progress) {
    int r = 0;
    if (env->i->open_flags & DB_INIT_TXN && env->i->open_flags & DB_INIT_LOG) {
        r = toku_maybe_upgrade_log(env->i->dir, env->i->real_log_dir, last_lsn_of_clean_shutdown_read_from_log, upgrade_in_progress);
    }
    return r;
}

// return 0 if log exists or ENOENT if log does not exist
static int
ydb_recover_log_exists(DB_ENV *env) {
    int r = tokuft_recover_log_exists(env->i->real_log_dir);
    return r;
}

// Validate that all required files are present, no side effects.
// Return 0 if all is well, ENOENT if some files are present but at least one is missing, 
// other non-zero value if some other error occurs.
// Set *valid_newenv if creating a new environment (all files missing).
// (Note, if special dictionaries exist, then they were created transactionally and log should exist.)
static int 
validate_env(DB_ENV * env, bool * valid_newenv, bool need_rollback_cachefile) {
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

    // Test for existence of rollback cachefile if it is expected to exist
    if (r == 0 && need_rollback_cachefile) {
        path = toku_construct_full_name(2, env->i->dir, toku_product_name_strings.rollback_cachefile);
        assert(path);
        r = toku_stat(path, &buf);
        if (r == 0) {  
            if (expect_newenv)  // rollback cachefile exists, but persistent env is missing
                r = toku_ydb_do_error(env, ENOENT, "Persistent environment is missing\n");
        }
        else {
            int stat_errno = get_error_errno();
            if (stat_errno == ENOENT) {
                if (!expect_newenv)  // rollback cachefile is missing but persistent env exists
                    r = toku_ydb_do_error(env, ENOENT, "rollback cachefile directory is missing\n");
                else 
                    r = 0;           // both rollback cachefile and persistent env are missing
            }
            else {
                r = toku_ydb_do_error(env, stat_errno, "Unable to access rollback cachefile\n");
                assert(r);
            }
        }
        toku_free(path);
    }

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

    // Test for recovery log
    if ((r == 0) && (env->i->open_flags & DB_INIT_LOG)) {
        // if using transactions, test for existence of log
        r = ydb_recover_log_exists(env);  // return 0 or ENOENT
        if (expect_newenv && (r != ENOENT))
            r = toku_ydb_do_error(env, ENOENT, "Persistent environment information is missing (but log exists)\n");
        else if (!expect_newenv && r == ENOENT)
            r = toku_ydb_do_error(env, ENOENT, "Recovery log is missing (persistent environment information is present)\n");
        else
            r = 0;
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

// Values read from (or written into) persistent environment,
// kept here for read-only access from engine status.
// Note, persistent_upgrade_status info is separate in part to simplify its exclusion from engine status until relevant.
typedef enum {
    PERSISTENT_UPGRADE_ORIGINAL_ENV_VERSION = 0,
    PERSISTENT_UPGRADE_STORED_ENV_VERSION_AT_STARTUP,    // read from curr_env_ver_key, prev version as of this startup
    PERSISTENT_UPGRADE_LAST_LSN_OF_V13,
    PERSISTENT_UPGRADE_V14_TIME,
    PERSISTENT_UPGRADE_V14_FOOTPRINT,
    PERSISTENT_UPGRADE_STATUS_NUM_ROWS
} persistent_upgrade_status_entry;

typedef struct {
    bool initialized;
    TOKU_ENGINE_STATUS_ROW_S status[PERSISTENT_UPGRADE_STATUS_NUM_ROWS];
} PERSISTENT_UPGRADE_STATUS_S, *PERSISTENT_UPGRADE_STATUS;

static PERSISTENT_UPGRADE_STATUS_S persistent_upgrade_status;

#define PERSISTENT_UPGRADE_STATUS_INIT(k,c,t,l,inc) TOKUFT_STATUS_INIT(persistent_upgrade_status, k, c, t, "upgrade: " l, inc)

static void
persistent_upgrade_status_init (void) {
    // Note, this function initializes the keyname, type, and legend fields.
    // Value fields are initialized to zero by compiler.

    PERSISTENT_UPGRADE_STATUS_INIT(PERSISTENT_UPGRADE_ORIGINAL_ENV_VERSION,           nullptr, UINT64,   "original version (at time of environment creation)", TOKU_ENGINE_STATUS);
    PERSISTENT_UPGRADE_STATUS_INIT(PERSISTENT_UPGRADE_STORED_ENV_VERSION_AT_STARTUP,  nullptr, UINT64,   "version at time of startup", TOKU_ENGINE_STATUS);
    PERSISTENT_UPGRADE_STATUS_INIT(PERSISTENT_UPGRADE_LAST_LSN_OF_V13,                nullptr, UINT64,   "last LSN of version 13", TOKU_ENGINE_STATUS);
    PERSISTENT_UPGRADE_STATUS_INIT(PERSISTENT_UPGRADE_V14_TIME,                       nullptr, UNIXTIME, "time of upgrade to version 14", TOKU_ENGINE_STATUS);
    PERSISTENT_UPGRADE_STATUS_INIT(PERSISTENT_UPGRADE_V14_FOOTPRINT,                  nullptr, UINT64,   "footprint from version 13 to 14", TOKU_ENGINE_STATUS);
    persistent_upgrade_status.initialized = true;
}

#define PERSISTENT_UPGRADE_STATUS_VALUE(x) persistent_upgrade_status.status[x].value.num

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
static int
maybe_upgrade_persistent_environment_dictionary(DB_ENV * env, DB_TXN * txn, LSN last_lsn_of_clean_shutdown_read_from_log) {
    int r;
    DBT key, val;
    DB *persistent_environment = env->i->persistent_environment;

    if (!persistent_upgrade_status.initialized)
        persistent_upgrade_status_init();

    toku_fill_dbt(&key, curr_env_ver_key, strlen(curr_env_ver_key));
    toku_init_dbt(&val);
    r = toku_db_get(persistent_environment, txn, &key, &val, 0);
    assert(r == 0);
    uint32_t stored_env_version = toku_dtoh32(*(uint32_t*)val.data);
    PERSISTENT_UPGRADE_STATUS_VALUE(PERSISTENT_UPGRADE_STORED_ENV_VERSION_AT_STARTUP) = stored_env_version;
    if (stored_env_version > FT_LAYOUT_VERSION)
        r = TOKUDB_DICTIONARY_TOO_NEW;
    else if (stored_env_version < FT_LAYOUT_MIN_SUPPORTED_VERSION)
        r = TOKUDB_DICTIONARY_TOO_OLD;
    else if (stored_env_version < FT_LAYOUT_VERSION) {
        const uint32_t curr_env_ver_d = toku_htod32(FT_LAYOUT_VERSION);
        toku_fill_dbt(&key, curr_env_ver_key, strlen(curr_env_ver_key));
        toku_fill_dbt(&val, &curr_env_ver_d, sizeof(curr_env_ver_d));
        r = toku_db_put(persistent_environment, txn, &key, &val, 0, false);
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
            r = toku_db_put(persistent_environment, txn, &key, &val, put_flag, false);
            assert_zero(r);

            char* upgrade_footprint_key = get_upgrade_footprint_key(version);
            toku_fill_dbt(&key, upgrade_footprint_key, strlen(upgrade_footprint_key));
            toku_fill_dbt(&val, &upgrade_footprint_d, sizeof(upgrade_footprint_d));
            r = toku_db_put(persistent_environment, txn, &key, &val, put_flag, false);
            assert_zero(r);

            char* upgrade_last_lsn_key = get_upgrade_last_lsn_key(version);
            toku_fill_dbt(&key, upgrade_last_lsn_key, strlen(upgrade_last_lsn_key));
            toku_fill_dbt(&val, &upgrade_last_lsn_d, sizeof(upgrade_last_lsn_d));
            r = toku_db_put(persistent_environment, txn, &key, &val, put_flag, false);
            assert_zero(r);
        }

    }
    return r;
}



// Capture contents of persistent_environment dictionary so that it can be read by engine status
static void
capture_persistent_env_contents (DB_ENV * env, DB_TXN * txn) {
    int r;
    DBT key, val;
    DB *persistent_environment = env->i->persistent_environment;

    toku_fill_dbt(&key, curr_env_ver_key, strlen(curr_env_ver_key));
    toku_init_dbt(&val);
    r = toku_db_get(persistent_environment, txn, &key, &val, 0);
    assert_zero(r);
    uint32_t curr_env_version = toku_dtoh32(*(uint32_t*)val.data);
    assert(curr_env_version == FT_LAYOUT_VERSION);

    toku_fill_dbt(&key, orig_env_ver_key, strlen(orig_env_ver_key));
    toku_init_dbt(&val);
    r = toku_db_get(persistent_environment, txn, &key, &val, 0);
    assert_zero(r);
    uint64_t persistent_original_env_version = toku_dtoh32(*(uint32_t*)val.data);
    PERSISTENT_UPGRADE_STATUS_VALUE(PERSISTENT_UPGRADE_ORIGINAL_ENV_VERSION) = persistent_original_env_version;
    assert(persistent_original_env_version <= curr_env_version);

    // make no assertions about timestamps, clock may have been reset
    if (persistent_original_env_version >= FT_LAYOUT_VERSION_14) {
        toku_fill_dbt(&key, creation_time_key, strlen(creation_time_key));
        toku_init_dbt(&val);
        r = toku_db_get(persistent_environment, txn, &key, &val, 0);
        assert_zero(r);
        //STATUS_VALUE(YDB_LAYER_TIME_CREATION) = toku_dtoh64((*(time_t*)val.data));
    }

    if (persistent_original_env_version != curr_env_version) {
        // an upgrade was performed at some time, capture info about the upgrade

        char * last_lsn_key = get_upgrade_last_lsn_key(curr_env_version);
        toku_fill_dbt(&key, last_lsn_key, strlen(last_lsn_key));
        toku_init_dbt(&val);
        r = toku_db_get(persistent_environment, txn, &key, &val, 0);
        assert_zero(r);
        PERSISTENT_UPGRADE_STATUS_VALUE(PERSISTENT_UPGRADE_LAST_LSN_OF_V13) = toku_dtoh64(*(uint64_t*)val.data);

        char * time_key = get_upgrade_time_key(curr_env_version);
        toku_fill_dbt(&key, time_key, strlen(time_key));
        toku_init_dbt(&val);
        r = toku_db_get(persistent_environment, txn, &key, &val, 0);
        assert_zero(r);
        PERSISTENT_UPGRADE_STATUS_VALUE(PERSISTENT_UPGRADE_V14_TIME) = toku_dtoh64(*(time_t*)val.data);

        char * footprint_key = get_upgrade_footprint_key(curr_env_version);
        toku_fill_dbt(&key, footprint_key, strlen(footprint_key));
        toku_init_dbt(&val);
        r = toku_db_get(persistent_environment, txn, &key, &val, 0);
        assert_zero(r);
        PERSISTENT_UPGRADE_STATUS_VALUE(PERSISTENT_UPGRADE_V14_FOOTPRINT) = toku_dtoh64(*(uint64_t*)val.data);
    }

}

static void
env_fs_report_in_yellow(DB_ENV *UU(env)) {
    char tbuf[26];
    time_t tnow = time(NULL);
    fprintf(stderr, "%.24s TokuFT file system space is low\n", ctime_r(&tnow, tbuf)); fflush(stderr);
}

static void
env_fs_report_in_red(DB_ENV *UU(env)) {
    char tbuf[26];
    time_t tnow = time(NULL);
    fprintf(stderr, "%.24s TokuFT file system space is really low and access is restricted\n", ctime_r(&tnow, tbuf)); fflush(stderr);
}

static inline uint64_t
env_fs_redzone(DB_ENV *env, uint64_t total) {
    return total * env->i->redzone / 100;
}

#define ZONEREPORTLIMIT 12
// Check the available space in the file systems used by tokuft and erect barriers when available space gets low.
static int
env_fs_poller(void *arg) {
    DB_ENV *env = (DB_ENV *) arg;
    int r;

    int in_yellow; // set true to issue warning to user
    int in_red;    // set true to prevent certain operations (returning ENOSPC)

    // get the fs sizes for the home dir
    uint64_t avail_size, total_size;
    r = toku_get_filesystem_sizes(env->i->dir, &avail_size, NULL, &total_size);
    assert(r == 0);
    in_yellow = (avail_size < 2 * env_fs_redzone(env, total_size));
    in_red = (avail_size < env_fs_redzone(env, total_size));

    // get the fs sizes for the data dir if different than the home dir
    if (strcmp(env->i->dir, env->i->real_data_dir) != 0) {
        r = toku_get_filesystem_sizes(env->i->real_data_dir, &avail_size, NULL, &total_size);
        assert(r == 0);
        in_yellow += (avail_size < 2 * env_fs_redzone(env, total_size));
        in_red += (avail_size < env_fs_redzone(env, total_size));
    }

    // get the fs sizes for the log dir if different than the home dir and data dir
    if (strcmp(env->i->dir, env->i->real_log_dir) != 0 && strcmp(env->i->real_data_dir, env->i->real_log_dir) != 0) {
        r = toku_get_filesystem_sizes(env->i->real_log_dir, &avail_size, NULL, &total_size);
        assert(r == 0);
        in_yellow += (avail_size < 2 * env_fs_redzone(env, total_size));
        in_red += (avail_size < env_fs_redzone(env, total_size));
    }

    env->i->fs_seq++;                    // how many times through this polling loop?
    uint64_t now = env->i->fs_seq;

    // Don't issue report if we have not been out of this fs_state for a while, unless we're at system startup
    switch (env->i->fs_state) {
    case FS_RED:
        if (!in_red) {
            if (in_yellow) {
                env->i->fs_state = FS_YELLOW;
            } else {
                env->i->fs_state = FS_GREEN;
            }
        }
        break;
    case FS_YELLOW:
        if (in_red) {
            if ((now - env->i->last_seq_entered_red > ZONEREPORTLIMIT) || (now < ZONEREPORTLIMIT))
                env_fs_report_in_red(env);
            env->i->fs_state = FS_RED;
            env->i->last_seq_entered_red = now;
        } else if (!in_yellow) {
            env->i->fs_state = FS_GREEN;
        }
        break;
    case FS_GREEN:
        if (in_red) {
            if ((now - env->i->last_seq_entered_red > ZONEREPORTLIMIT) || (now < ZONEREPORTLIMIT))
                env_fs_report_in_red(env);
            env->i->fs_state = FS_RED;
            env->i->last_seq_entered_red = now;
        } else if (in_yellow) {
            if ((now - env->i->last_seq_entered_yellow > ZONEREPORTLIMIT) || (now < ZONEREPORTLIMIT))
                env_fs_report_in_yellow(env);
            env->i->fs_state = FS_YELLOW;
            env->i->last_seq_entered_yellow = now;
        }
        break;
    default:
        assert(0);
    }
    return 0;
}
#undef ZONEREPORTLIMIT

// Initialize the minicron that polls file system space
static int
env_fs_init_minicron(DB_ENV *env) {
    int r = toku_minicron_setup(&env->i->fs_poller, env->i->fs_poll_time*1000, env_fs_poller, env); 
    if (r == 0)
        env->i->fs_poller_is_init = true;
    return r;
}

static int
env_fsync_log_on_minicron(void *arg) {
    DB_ENV *env = (DB_ENV *) arg;
    int r = env->log_flush(env, 0);
    assert(r == 0);
    return 0;
}

static int
env_fsync_log_cron_init(DB_ENV *env) {
    int r = toku_minicron_setup(&env->i->fsync_log_cron, env->i->fsync_log_period_ms, env_fsync_log_on_minicron, env);
    if (r == 0)
        env->i->fsync_log_cron_is_init = true;
    return r;
}

static void
unlock_single_process(DB_ENV *env) {
    int r;
    r = toku_single_process_unlock(&env->i->envdir_lockfd);
    lazy_assert_zero(r);
    r = toku_single_process_unlock(&env->i->datadir_lockfd);
    lazy_assert_zero(r);
    r = toku_single_process_unlock(&env->i->logdir_lockfd);
    lazy_assert_zero(r);
    r = toku_single_process_unlock(&env->i->tmpdir_lockfd);
    lazy_assert_zero(r);
}

extern int env_is_panicked;
extern DB_ENV * most_recent_env;   // most recently opened env, used for engine status on crash.  Note there are likely to be races on this if you have multiple threads creating and closing environments in parallel.  We'll declare it volatile since at least that helps make sure the compiler doesn't optimize away certain code (e.g., if while debugging, you write a code that spins on most_recent_env, you'd like to compiler not to optimize your code away.)


// Set panic code and panic string if not already panicked,
// intended for use by toku_assert when about to abort().
static void 
toku_maybe_set_env_panic(int code, const char * msg) {
    if (code == 0) 
        code = -1;
    if (msg == NULL)
        msg = "Unknown cause from abort (failed assert)\n";
    env_is_panicked = code;  // disable library destructor no matter what
    DB_ENV * env = most_recent_env;
    if (env && 
        env->i &&
        (env->i->is_panicked == 0)) {
        env_panic(env, code, msg);
    }
}

static uint32_t understood_flags = (
    DB_PRIVATE |
    DB_INIT_TXN |
    DB_INIT_LOG |
    DB_INIT_MPOOL |
    DB_CREATE |
    DB_INIT_LOCK |
    DB_RECOVER |
    DB_THREAD
    );


static int handle_user_errors(DB_ENV* env, const char *home, uint32_t flags) {
    int r = 0;

    if ((flags |= understood_flags) != understood_flags) {
        r = toku_ydb_do_error(env, EINVAL, "Flags not understood by tokuft: %u\n", flags);
        goto cleanup;
    }

    if (env_opened(env)) {
        r = toku_ydb_do_error(env, EINVAL, "The environment is already open\n");
        goto cleanup;
    }

    if (toku_os_huge_pages_enabled()) {
        r = toku_ydb_do_error(env, TOKUDB_HUGE_PAGES_ENABLED,
                              "Huge pages are enabled, disable them before continuing\n");
        goto cleanup;
    }


    assert(sizeof(time_t) == sizeof(uint64_t));

    HANDLE_EXTRA_FLAGS(env, flags, 
                       DB_CREATE|DB_PRIVATE|DB_INIT_LOG|DB_INIT_TXN|DB_RECOVER|DB_INIT_MPOOL|DB_INIT_LOCK|DB_THREAD);

    // DB_CREATE means create if env does not exist, and TokuFT requires it because
    // TokuFT requries DB_PRIVATE.
    if ((flags & DB_PRIVATE) && !(flags & DB_CREATE)) {
        r = toku_ydb_do_error(env, ENOENT, "DB_PRIVATE requires DB_CREATE (seems gratuitous to us, but that's BDB's behavior\n");
        goto cleanup;
    }

    if (!(flags & DB_PRIVATE)) {
        r = toku_ydb_do_error(env, ENOENT, "TokuFT requires DB_PRIVATE\n");
        goto cleanup;
    }

    if ((flags & DB_INIT_LOG) && !(flags & DB_INIT_TXN)) {
        r = toku_ydb_do_error(env, EINVAL, "TokuFT requires transactions for logging\n");
        goto cleanup;
    }

    if (!home) home = ".";

    // Verify that the home exists.
    toku_struct_stat buf;
    r = toku_stat(home, &buf);
    if (r != 0) {
        int e = get_error_errno();
        r = toku_ydb_do_error(env, e, "Error from toku_stat(\"%s\",...)\n", home);
        goto cleanup;
    }
cleanup:
    return r;
}

static int setup_and_lock_dirs(DB_ENV* env, const char *home, uint32_t flags, int mode) {
    int r = 0;
    if (!home) home = ".";

    if (env->i->dir) {
        toku_free(env->i->dir);
    }
    env->i->dir = toku_strdup(home);
    if (env->i->dir == 0) {
        r = toku_ydb_do_error(env, ENOMEM, "Out of memory\n");
        goto cleanup;
    }
    env->i->open_flags = flags;
    env->i->open_mode = mode;

    env_setup_real_data_dir(env);
    env_setup_real_log_dir(env);
    env_setup_real_tmp_dir(env);

    r = toku_single_process_lock(env->i->dir, "environment", &env->i->envdir_lockfd);
    if (r!=0) goto cleanup;
    r = toku_single_process_lock(env->i->real_data_dir, "data", &env->i->datadir_lockfd);
    if (r!=0) goto cleanup;
    r = toku_single_process_lock(env->i->real_log_dir, "logs", &env->i->logdir_lockfd);
    if (r!=0) goto cleanup;
    r = toku_single_process_lock(env->i->real_tmp_dir, "temp", &env->i->tmpdir_lockfd);
    if (r!=0) goto cleanup;
cleanup:
    return r;
}

static int handle_recovery(DB_ENV* env, uint32_t flags) {
    int r = 0;
    if (flags & DB_INIT_LOG) {
        // the log does exist
        if (flags & DB_RECOVER) {
            r = ydb_do_recovery(env);
            if (r != 0) goto cleanup;
        } else {
            // the log is required to have clean shutdown if recovery is not requested
            r = needs_recovery(env);
            if (r != 0) goto cleanup;
        }
    }
cleanup:
    return r;
}

static int setup_metadata_files(DB_ENV* env, bool newenv, DB_TXN* txn, int mode, LSN last_lsn_of_clean_shutdown_read_from_log) {
    int r = 0;
    r = toku_db_create(&env->i->persistent_environment, env, 0);
    assert_zero(r);
    r = toku_db_use_builtin_key_cmp(env->i->persistent_environment);
    assert_zero(r);
    r = toku_db_open_iname(env->i->persistent_environment, txn, toku_product_name_strings.environmentdictionary, DB_CREATE, mode);
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
        r = toku_db_put(env->i->persistent_environment, txn, &key, &val, 0, false);
        assert_zero(r);

        toku_fill_dbt(&key, curr_env_ver_key, strlen(curr_env_ver_key));
        toku_fill_dbt(&val, &environment_version, sizeof(environment_version));
        r = toku_db_put(env->i->persistent_environment, txn, &key, &val, 0, false);
        assert_zero(r);

        time_t creation_time_d = toku_htod64(time(NULL));
        toku_fill_dbt(&key, creation_time_key, strlen(creation_time_key));
        toku_fill_dbt(&val, &creation_time_d, sizeof(creation_time_d));
        r = toku_db_put(env->i->persistent_environment, txn, &key, &val, 0, false);
        assert_zero(r);
    }
    else {
        r = maybe_upgrade_persistent_environment_dictionary(env, txn, last_lsn_of_clean_shutdown_read_from_log);
        assert_zero(r);
    }
    capture_persistent_env_contents(env, txn);
    r = toku_db_create(&env->i->directory, env, 0);
    assert_zero(r);
    r = toku_db_use_builtin_key_cmp(env->i->directory);
    assert_zero(r);
    r = toku_db_open_iname(env->i->directory, txn, toku_product_name_strings.fileopsdirectory, DB_CREATE, mode);
    if (r != 0) {
        r = toku_ydb_do_error(env, r, "Cant open %s\n", toku_product_name_strings.fileopsdirectory);
        goto cleanup;
    }
cleanup:
    return r;
}

static int setup_minicrons(DB_ENV* env) {
    env_fs_poller(env);          // get the file system state at startup
    int r = env_fs_init_minicron(env);
    if (r != 0) {
        r = toku_ydb_do_error(env, r, "Cant create fs minicron\n");
        goto cleanup;
    }
    r = env_fsync_log_cron_init(env);
    if (r != 0) {
        r = toku_ydb_do_error(env, r, "Cant create fsync log minicron\n");
        goto cleanup;
    }
cleanup:
    return r;
}

// Open the environment.
// If this is a new environment, then create the necessary files.
// Return 0 on success, ENOENT if any of the expected necessary files are missing.
// (The set of necessary files is defined in the function validate_env() above.)
int 
env_open(DB_ENV* env, const char *home, uint32_t flags, int mode) 
{
    HANDLE_PANICKED_ENV(env);
    int r;
    bool newenv;  // true iff creating a new environment
    CHECKPOINTER cp;
    DB_TXN *txn = NULL;
    bool need_rollback_cachefile = false;
    if (flags & (DB_INIT_TXN | DB_INIT_LOG)) {
        need_rollback_cachefile = true;
    }

    r = handle_user_errors(env, home, flags);
    if (r != 0) goto cleanup;

    most_recent_env = NULL;

    r = setup_and_lock_dirs(env, home, flags, mode);
    if (r != 0) goto cleanup;


    ydb_layer_status_init();  // do this before possibly upgrading, so upgrade work is counted in status counters

    LSN last_lsn_of_clean_shutdown_read_from_log;
    last_lsn_of_clean_shutdown_read_from_log = ZERO_LSN;
    bool upgrade_in_progress;
    upgrade_in_progress = false;
    r = ydb_maybe_upgrade_env(env, &last_lsn_of_clean_shutdown_read_from_log, &upgrade_in_progress);
    if (r!=0) goto cleanup;

    if (upgrade_in_progress) {
        // Delete old rollback file.  There was a clean shutdown, so it has nothing useful,
        // and there is no value in upgrading it.  It is simpler to just create a new one.
        char* rollback_filename = toku_construct_full_name(2, env->i->dir, toku_product_name_strings.rollback_cachefile);
        assert(rollback_filename);
        r = unlink(rollback_filename);
        if (r != 0) {
            assert(get_error_errno() == ENOENT);
        }
        toku_free(rollback_filename);
        need_rollback_cachefile = false;  // we're not expecting it to exist now
    }
    
    r = validate_env(env, &newenv, need_rollback_cachefile);  // make sure that environment is either new or complete
    if (r != 0) goto cleanup;

    // do recovery only if there exists a log and recovery is requested
    // otherwise, a log is created when the logger is opened later
    if (!newenv) {
        r = handle_recovery(env, flags);
        if (r!=0) goto cleanup;
    }
    
    toku_loader_cleanup_temp_files(env);

    if (flags & (DB_INIT_TXN | DB_INIT_LOG)) {
        assert(env->i->logger);
        toku_logger_write_log_files(env->i->logger, (bool)((flags & DB_INIT_LOG) != 0));
        if (!toku_logger_is_open(env->i->logger)) {
            r = toku_logger_open(env->i->real_log_dir, env->i->logger);
            if (r!=0) {
                toku_ydb_do_error(env, r, "Could not open logger\n");
            }
        }
    } else {
        r = toku_logger_close(&env->i->logger); // if no logging system, then kill the logger
        assert_zero(r);
    }

    if (env->i->cachetable==NULL) {
        // If we ran recovery then the cachetable should be set here.
        r = toku_cachetable_create(&env->i->cachetable, env->i->cachetable_size, ZERO_LSN, env->i->logger);
        if (r != 0) {
            r = toku_ydb_do_error(env, r, "Cant create a cachetable\n");
            goto cleanup;
        }
    }

    toku_cachetable_set_env_dir(env->i->cachetable, env->i->dir);

    int using_txns;
    using_txns = env->i->open_flags & DB_INIT_TXN;
    if (env->i->logger) {
        // if this is a newborn env or if this is an upgrade, then create a brand new rollback file
        assert (using_txns);
        toku_logger_set_cachetable(env->i->logger, env->i->cachetable);
        if (!toku_logger_rollback_is_open(env->i->logger)) {
            bool create_new_rollback_file = newenv | upgrade_in_progress;
            r = toku_logger_open_rollback(env->i->logger, env->i->cachetable, create_new_rollback_file);
            if (r != 0) {
                r = toku_ydb_do_error(env, r, "Cant open rollback\n");
                goto cleanup;
            }
        }
    }

    if (using_txns) {
        r = toku_txn_begin(env, 0, &txn, 0);
        assert_zero(r);
    }

    setup_metadata_files(env, newenv, txn, mode, last_lsn_of_clean_shutdown_read_from_log);
    if (r!=0) goto cleanup;

    if (using_txns) {
        r = locked_txn_commit(txn, 0);
        assert_zero(r);
        txn = NULL;
    }
    cp = toku_cachetable_get_checkpointer(env->i->cachetable);
    r = toku_checkpoint(cp, env->i->logger, NULL, NULL, NULL, NULL, STARTUP_CHECKPOINT);
    assert_zero(r);

    r = setup_minicrons(env);
    if (r != 0) goto cleanup;

cleanup:
    if (r!=0) {
        if (txn) {
            locked_txn_abort(txn);
        }
        if (env && env->i) {
            unlock_single_process(env);
        }
    }
    if (r == 0) {
        set_errno(0); // tabula rasa.   If there's a crash after env was successfully opened, no misleading errno will have been left around by this code.
        most_recent_env = env;
        uint64_t num_rows;
        env_get_engine_status_num_rows(env, &num_rows);
        toku_assert_set_fpointers(toku_maybe_get_engine_status_text, toku_maybe_err_engine_status, toku_maybe_set_env_panic, num_rows);
    }
    return r;
}

static void
env_fsync_log_cron_destroy(DB_ENV *env) {
    if (env->i->fsync_log_cron_is_init) {
        int r = toku_minicron_shutdown(&env->i->fsync_log_cron);
        assert(r == 0);
        env->i->fsync_log_cron_is_init = false;
    }
}

// Destroy the file system space minicron
static void
env_fs_destroy(DB_ENV *env) {
    if (env->i->fs_poller_is_init) {
        int r = toku_minicron_shutdown(&env->i->fs_poller);
        assert(r == 0);
        env->i->fs_poller_is_init = false;
    }
}

int 
env_close(DB_ENV * env, uint32_t flags) {
    int r = 0;
    const char * err_msg = NULL;
    bool clean_shutdown = true;

    if (flags & TOKUFT_DIRTY_SHUTDOWN) {
        clean_shutdown = false;
        flags &= ~TOKUFT_DIRTY_SHUTDOWN;
    }

    most_recent_env = NULL; // Set most_recent_env to NULL so that we don't have a dangling pointer (and if there's an error, the toku assert code would try to look at the env.)

    // if panicked, or if any open transactions, or any open dbs, then do nothing.

    if (toku_env_is_panicked(env)) {
        goto panic_and_quit_early;
    }
    if (env->i->logger && toku_logger_txns_exist(env->i->logger)) {
        err_msg = "Cannot close environment due to open transactions\n";
        r = toku_ydb_do_error(env, EINVAL, "%s", err_msg);
        goto panic_and_quit_early;
    }
    if (env->i->open_dbs_by_dname) { //Verify that there are no open dbs.
        if (env->i->open_dbs_by_dname->size() > 0) {
            err_msg = "Cannot close environment due to open DBs\n";
            r = toku_ydb_do_error(env, EINVAL, "%s", err_msg);
            goto panic_and_quit_early;
        }
    }
    if (env->i->persistent_environment) {
        r = toku_db_close(env->i->persistent_environment);
        if (r) {
            err_msg = "Cannot close persistent environment dictionary (DB->close error)\n";
            toku_ydb_do_error(env, r, "%s", err_msg);
            goto panic_and_quit_early;
        }
    }
    if (env->i->directory) {
        r = toku_db_close(env->i->directory);
        if (r) {
            err_msg = "Cannot close Directory dictionary (DB->close error)\n";
            toku_ydb_do_error(env, r, "%s", err_msg);
            goto panic_and_quit_early;
        }
    }
    env_fsync_log_cron_destroy(env);
    if (env->i->cachetable) {
        toku_cachetable_prepare_close(env->i->cachetable);
        toku_cachetable_minicron_shutdown(env->i->cachetable);
        if (env->i->logger) {
            CHECKPOINTER cp = nullptr;
            if (clean_shutdown) {
                cp = toku_cachetable_get_checkpointer(env->i->cachetable);
                r = toku_checkpoint(cp, env->i->logger, NULL, NULL, NULL, NULL, SHUTDOWN_CHECKPOINT);
                if (r) {
                    err_msg = "Cannot close environment (error during checkpoint)\n";
                    toku_ydb_do_error(env, r, "%s", err_msg);
                    goto panic_and_quit_early;
                }
            }
            toku_logger_close_rollback_check_empty(env->i->logger, clean_shutdown);
            if (clean_shutdown) {
                //Do a second checkpoint now that the rollback cachefile is closed.
                r = toku_checkpoint(cp, env->i->logger, NULL, NULL, NULL, NULL, SHUTDOWN_CHECKPOINT);
                if (r) {
                    err_msg = "Cannot close environment (error during checkpoint)\n";
                    toku_ydb_do_error(env, r, "%s", err_msg);
                    goto panic_and_quit_early;
                }
                toku_logger_shutdown(env->i->logger); 
            }
        }
        toku_cachetable_close(&env->i->cachetable);
    }
    if (env->i->logger) {
        r = toku_logger_close(&env->i->logger);
        if (r) {
            err_msg = "Cannot close environment (logger close error)\n";
            env->i->logger = NULL;
            toku_ydb_do_error(env, r, "%s", err_msg);
            goto panic_and_quit_early;
        }
    }
    // Even if nothing else went wrong, but we were panicked, then raise an error.
    // But if something else went wrong then raise that error (above)
    if (toku_env_is_panicked(env)) {
        goto panic_and_quit_early;
    } else {
        assert(env->i->panic_string == 0);
    }

    env_fs_destroy(env);
    env->i->ltm.destroy();
    env->i->dict_manager.destroy();
    if (env->i->data_dir)
        toku_free(env->i->data_dir);
    if (env->i->lg_dir)
        toku_free(env->i->lg_dir);
    if (env->i->tmp_dir)
        toku_free(env->i->tmp_dir);
    if (env->i->real_data_dir)
        toku_free(env->i->real_data_dir);
    if (env->i->real_log_dir)
        toku_free(env->i->real_log_dir);
    if (env->i->real_tmp_dir)
        toku_free(env->i->real_tmp_dir);
    if (env->i->open_dbs_by_dname) {
        env->i->open_dbs_by_dname->destroy();
        toku_free(env->i->open_dbs_by_dname);
    }
    if (env->i->open_dbs_by_dict_id) {
        env->i->open_dbs_by_dict_id->destroy();
        toku_free(env->i->open_dbs_by_dict_id);
    }
    if (env->i->dir)
        toku_free(env->i->dir);
    toku_pthread_rwlock_destroy(&env->i->open_dbs_rwlock);

    // Immediately before freeing internal environment unlock the directories
    unlock_single_process(env);
    toku_free(env->i);
    toku_free(env);
    toku_sync_fetch_and_add(&tokuft_num_envs, -1);
    if (flags != 0) {
        r = EINVAL;
    }
    return r;

panic_and_quit_early:
    //release lock files.
    unlock_single_process(env);
    //r is the panic error
    if (toku_env_is_panicked(env)) {
        char *panic_string = env->i->panic_string;
        r = toku_ydb_do_error(env, toku_env_is_panicked(env), "Cannot close environment due to previous error: %s\n", panic_string);
    }
    else {
        env_panic(env, r, err_msg);
    }
    return r;
}

