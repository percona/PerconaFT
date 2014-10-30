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
const char *toku_copyright_string = "Copyright (c) 2007-2013 Tokutek Inc.  All rights reserved.";

#include <db.h>
#include <errno.h>
#include <string.h>

#include "portability/memory.h"
#include "portability/toku_assert.h"
#include "portability/toku_portability.h"
#include "portability/toku_pthread.h"
#include "portability/toku_stdlib.h"

#include "ft/ft-flusher.h"
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

// Include ydb_lib.cc here so that its constructor/destructor gets put into
// ydb.o, to make sure they don't get erased at link time (when linking to
// a static libtokufractaltree.a that was compiled with gcc).  See #5094.
#include "ydb_lib.cc"

#ifdef TOKUTRACE
 #define DB_ENV_CREATE_FUN db_env_create_toku10
 #define DB_CREATE_FUN db_create_toku10
#else
 #define DB_ENV_CREATE_FUN db_env_create
 #define DB_CREATE_FUN db_create
 int toku_set_trace_file (const char *fname __attribute__((__unused__))) { return 0; }
 int toku_close_trace_file (void) { return 0; } 
#endif

// Set when env is panicked, never cleared.
int env_is_panicked = 0;

void
env_panic(DB_ENV * env, int cause, const char * msg) {
    if (cause == 0)
        cause = -1;  // if unknown cause, at least guarantee panic
    if (msg == NULL)
        msg = "Unknown cause in env_panic\n";
    env_is_panicked = cause;
    env->i->is_panicked = cause;
    env->i->panic_string = toku_strdup(msg);
}


/********************************************************************************
 * Status is intended for display to humans to help understand system behavior.
 * It does not need to be perfectly thread-safe.
 */

typedef enum {
    YDB_LAYER_TIME_CREATION = 0,            /* timestamp of environment creation, read from persistent environment */
    YDB_LAYER_TIME_STARTUP,                 /* timestamp of system startup */
    YDB_LAYER_TIME_NOW,                     /* timestamp of engine status query */
    YDB_LAYER_NUM_DB_OPEN,
    YDB_LAYER_NUM_DB_CLOSE,
    YDB_LAYER_NUM_OPEN_DBS,
    YDB_LAYER_MAX_OPEN_DBS,
    YDB_LAYER_FSYNC_LOG_PERIOD,
#if 0
    YDB_LAYER_ORIGINAL_ENV_VERSION,         /* version of original environment, read from persistent environment */
    YDB_LAYER_STARTUP_ENV_VERSION,          /* version of environment at this startup, read from persistent environment (curr_env_ver_key) */
    YDB_LAYER_LAST_LSN_OF_V13,              /* read from persistent environment */
    YDB_LAYER_UPGRADE_V14_TIME,             /* timestamp of upgrade to version 14, read from persistent environment */
    YDB_LAYER_UPGRADE_V14_FOOTPRINT,        /* footprint of upgrade to version 14, read from persistent environment */
#endif
    YDB_LAYER_STATUS_NUM_ROWS              /* number of rows in this status array */
} ydb_layer_status_entry;

typedef struct {
    bool initialized;
    TOKU_ENGINE_STATUS_ROW_S status[YDB_LAYER_STATUS_NUM_ROWS];
} YDB_LAYER_STATUS_S, *YDB_LAYER_STATUS;

static YDB_LAYER_STATUS_S ydb_layer_status;
#define STATUS_VALUE(x) ydb_layer_status.status[x].value.num

#define STATUS_INIT(k,c,t,l,inc) TOKUFT_STATUS_INIT(ydb_layer_status, k, c, t, l, inc)

void
ydb_layer_status_init (void) {
    // Note, this function initializes the keyname, type, and legend fields.
    // Value fields are initialized to zero by compiler.

    STATUS_INIT(YDB_LAYER_TIME_CREATION,              nullptr, UNIXTIME, "time of environment creation", TOKU_ENGINE_STATUS);
    STATUS_INIT(YDB_LAYER_TIME_STARTUP,               nullptr, UNIXTIME, "time of engine startup", TOKU_ENGINE_STATUS);
    STATUS_INIT(YDB_LAYER_TIME_NOW,                   nullptr, UNIXTIME, "time now", TOKU_ENGINE_STATUS);
    STATUS_INIT(YDB_LAYER_NUM_DB_OPEN,                DB_OPENS, UINT64,   "db opens", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(YDB_LAYER_NUM_DB_CLOSE,               DB_CLOSES, UINT64,   "db closes", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(YDB_LAYER_NUM_OPEN_DBS,               DB_OPEN_CURRENT, UINT64,   "num open dbs now", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(YDB_LAYER_MAX_OPEN_DBS,               DB_OPEN_MAX, UINT64,   "max open dbs", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(YDB_LAYER_FSYNC_LOG_PERIOD,           nullptr, UINT64,   "period, in ms, that recovery log is automatically fsynced", TOKU_ENGINE_STATUS);

    STATUS_VALUE(YDB_LAYER_TIME_STARTUP) = time(NULL);
    ydb_layer_status.initialized = true;
}
#undef STATUS_INIT

static void
ydb_layer_get_status(DB_ENV* env, YDB_LAYER_STATUS statp) {
    STATUS_VALUE(YDB_LAYER_TIME_NOW) = time(NULL);
    STATUS_VALUE(YDB_LAYER_FSYNC_LOG_PERIOD) = toku_minicron_get_period_in_ms_unlocked(&env->i->fsync_log_cron);
    *statp = ydb_layer_status;
}

/********************************************************************************
 * End of ydb_layer local status section.
 */

DB_ENV * volatile most_recent_env;   // most recently opened env, used for engine status on crash.  Note there are likely to be races on this if you have multiple threads creating and closing environments in parallel.  We'll declare it volatile since at least that helps make sure the compiler doesn't optimize away certain code (e.g., if while debugging, you write a code that spins on most_recent_env, you'd like to compiler not to optimize your code away.)

static int env_get_iname(DB_ENV* env, DBT* dname_dbt, DBT* iname_dbt);

int 
toku_ydb_init(void) {
    int r = 0;
    //Lower level must be initialized first.
    r = toku_ft_layer_init();
    return r;
}

// Do not clean up resources if env is panicked, just exit ugly
void 
toku_ydb_destroy(void) {
    if (env_is_panicked == 0) {
        toku_ft_layer_destroy();
    }
}

/* env methods */

static void
env_fs_init(DB_ENV *env) {
    env->i->fs_state = FS_GREEN;
    env->i->fs_poll_time = 5;  // seconds
    env->i->redzone = 5;       // percent of total space
    env->i->fs_poller_is_init = false;
}

static void
env_fsync_log_init(DB_ENV *env) {
    env->i->fsync_log_period_ms = 0;
    env->i->fsync_log_cron_is_init = false;
}

static void UU()
env_change_fsync_log_period(DB_ENV* env, uint32_t period_ms) {
    env->i->fsync_log_period_ms = period_ms;
    if (env->i->fsync_log_cron_is_init) {
        toku_minicron_change_period(&env->i->fsync_log_cron, period_ms);
    }
}

static int 
env_log_archive(DB_ENV * env, char **list[], uint32_t flags) {
    return toku_logger_log_archive(env->i->logger, list, flags);
}

static int 
env_log_flush(DB_ENV * env, const DB_LSN * lsn __attribute__((__unused__))) {
    HANDLE_PANICKED_ENV(env);
    // do nothing if no logger
    if (env->i->logger) {
        // We just flush everything. MySQL uses lsn == 0 which means flush everything. 
        // For anyone else using the log, it is correct to flush too much, so we are OK.
        toku_logger_fsync(env->i->logger);
    }
    return 0;
}

static int 
env_set_cachesize(DB_ENV * env, uint32_t gbytes, uint32_t bytes, int ncache) {
    HANDLE_PANICKED_ENV(env);
    if (ncache != 1) {
        return EINVAL;
    }
    uint64_t cs64 = ((uint64_t) gbytes << 30) + bytes;
    unsigned long cs = cs64;
    if (cs64 > cs) {
        return EINVAL;
    }
    env->i->cachetable_size = cs;
    return 0;
}

static int env_dbremove(DB_ENV * env, DB_TXN *txn, const char *fname, const char *dbname, uint32_t flags);

static int
locked_env_dbremove(DB_ENV * env, DB_TXN *txn, const char *fname, const char *dbname, uint32_t flags) {
    int ret, r;
    HANDLE_ILLEGAL_WORKING_PARENT_TXN(env, txn);
    HANDLE_READ_ONLY_TXN(txn);

    DB_TXN *child_txn = NULL;
    int using_txns = env->i->open_flags & DB_INIT_TXN;
    if (using_txns) {
        ret = toku_txn_begin(env, txn, &child_txn, 0);
        lazy_assert_zero(ret);
    }

    // cannot begin a checkpoint
    toku_multi_operation_client_lock();
    r = env_dbremove(env, child_txn, fname, dbname, flags);
    toku_multi_operation_client_unlock();

    if (using_txns) {
        if (r == 0) {
            ret = locked_txn_commit(child_txn, 0);
            lazy_assert_zero(ret);
        } else {
            ret = locked_txn_abort(child_txn);
            lazy_assert_zero(ret);
        }
    }
    return r;
}

static int env_dbrename(DB_ENV *env, DB_TXN *txn, const char *fname, const char *dbname, const char *newname, uint32_t flags);

static int
locked_env_dbrename(DB_ENV *env, DB_TXN *txn, const char *fname, const char *dbname, const char *newname, uint32_t flags) {
    int ret, r;
    HANDLE_READ_ONLY_TXN(txn);
    HANDLE_ILLEGAL_WORKING_PARENT_TXN(env, txn);

    DB_TXN *child_txn = NULL;
    int using_txns = env->i->open_flags & DB_INIT_TXN;
    if (using_txns) {
        ret = toku_txn_begin(env, txn, &child_txn, 0);
        lazy_assert_zero(ret);
    }

    // cannot begin a checkpoint
    toku_multi_operation_client_lock();
    r = env_dbrename(env, child_txn, fname, dbname, newname, flags);
    toku_multi_operation_client_unlock();

    if (using_txns) {
        if (r == 0) {
            ret = locked_txn_commit(child_txn, 0);
            lazy_assert_zero(ret);
        } else {
            ret = locked_txn_abort(child_txn);
            lazy_assert_zero(ret);
        }
    }
    return r;
}

#if DB_VERSION_MAJOR == 4 && DB_VERSION_MINOR >= 3

static int 
env_get_cachesize(DB_ENV * env, uint32_t *gbytes, uint32_t *bytes, int *ncache) {
    HANDLE_PANICKED_ENV(env);
    *gbytes = env->i->cachetable_size >> 30;
    *bytes = env->i->cachetable_size & ((1<<30)-1);
    *ncache = 1;
    return 0;
}

#endif

static int 
env_set_data_dir(DB_ENV * env, const char *dir) {
    HANDLE_PANICKED_ENV(env);
    int r;
    
    if (env_opened(env) || !dir) {
        r = toku_ydb_do_error(env, EINVAL, "You cannot set the data dir after opening the env\n");
    }
    else if (env->i->data_dir)
        r = toku_ydb_do_error(env, EINVAL, "You cannot set the data dir more than once.\n");
    else {
        env->i->data_dir = toku_strdup(dir);
        if (env->i->data_dir==NULL) {
            assert(get_error_errno() == ENOMEM);
            r = toku_ydb_do_error(env, ENOMEM, "Out of memory\n");
        }
        else r = 0;
    }
    return r;
}

static void 
env_set_errcall(DB_ENV * env, toku_env_errcall_t errcall) {
    env->i->errcall = errcall;
}

static void 
env_set_errfile(DB_ENV*env, FILE*errfile) {
    env->i->errfile = errfile;
}

static void 
env_set_errpfx(DB_ENV * env, const char *errpfx) {
    env->i->errpfx = errpfx;
}

static int 
env_set_flags(DB_ENV * env, uint32_t flags, int onoff) {
    HANDLE_PANICKED_ENV(env);

    uint32_t change = 0;
    if (flags & DB_AUTO_COMMIT) {
        change |=  DB_AUTO_COMMIT;
        flags  &= ~DB_AUTO_COMMIT;
    }
    if (flags != 0 && onoff) {
        return toku_ydb_do_error(env, EINVAL, "TokuFT does not (yet) support any nonzero ENV flags other than DB_AUTO_COMMIT\n");
    }
    if   (onoff) env->i->open_flags |=  change;
    else         env->i->open_flags &= ~change;
    return 0;
}

static int 
env_set_lg_bsize(DB_ENV * env, uint32_t bsize) {
    HANDLE_PANICKED_ENV(env);
    return toku_logger_set_lg_bsize(env->i->logger, bsize);
}

static int 
env_set_lg_dir(DB_ENV * env, const char *dir) {
    HANDLE_PANICKED_ENV(env);
    if (env_opened(env)) {
        return toku_ydb_do_error(env, EINVAL, "Cannot set log dir after opening the env\n");
    }

    if (env->i->lg_dir) toku_free(env->i->lg_dir);
    if (dir) {
        env->i->lg_dir = toku_strdup(dir);
        if (!env->i->lg_dir) {
            return toku_ydb_do_error(env, ENOMEM, "Out of memory\n");
        }
    }
    else env->i->lg_dir = NULL;
    return 0;
}

static int 
env_set_lg_max(DB_ENV * env, uint32_t lg_max) {
    HANDLE_PANICKED_ENV(env);
    return toku_logger_set_lg_max(env->i->logger, lg_max);
}

static int 
env_get_lg_max(DB_ENV * env, uint32_t *lg_maxp) {
    HANDLE_PANICKED_ENV(env);
    return toku_logger_get_lg_max(env->i->logger, lg_maxp);
}

static int 
env_set_lk_detect(DB_ENV * env, uint32_t UU(detect)) {
    HANDLE_PANICKED_ENV(env);
    return toku_ydb_do_error(env, EINVAL, "TokuFT does not (yet) support set_lk_detect\n");
}

static int 
env_set_lk_max_memory(DB_ENV *env, uint64_t lock_memory_limit) {
    HANDLE_PANICKED_ENV(env);
    int r = 0;
    if (env_opened(env)) {
        r = EINVAL;
    } else {
        r = env->i->ltm.set_max_lock_memory(lock_memory_limit);
    }
    return r;
}

static int 
env_get_lk_max_memory(DB_ENV *env, uint64_t *lk_maxp) {
    HANDLE_PANICKED_ENV(env);
    uint32_t max_lock_memory = env->i->ltm.get_max_lock_memory();
    *lk_maxp = max_lock_memory;
    return 0;
}

//void toku__env_set_noticecall (DB_ENV *env, void (*noticecall)(DB_ENV *, db_notices)) {
//    env->i->noticecall = noticecall;
//}

static int 
env_set_tmp_dir(DB_ENV * env, const char *tmp_dir) {
    HANDLE_PANICKED_ENV(env);
    if (env_opened(env)) {
        return toku_ydb_do_error(env, EINVAL, "Cannot set the tmp dir after opening an env\n");
    }
    if (!tmp_dir) {
        return toku_ydb_do_error(env, EINVAL, "Tmp dir bust be non-null\n");
    }
    if (env->i->tmp_dir)
        toku_free(env->i->tmp_dir);
    env->i->tmp_dir = toku_strdup(tmp_dir);
    return env->i->tmp_dir ? 0 : ENOMEM;
}

static int 
env_set_verbose(DB_ENV * env, uint32_t UU(which), int UU(onoff)) {
    HANDLE_PANICKED_ENV(env);
    return 1;
}

static int 
toku_env_txn_checkpoint(DB_ENV * env, uint32_t kbyte __attribute__((__unused__)), uint32_t min __attribute__((__unused__)), uint32_t flags __attribute__((__unused__))) {
    CHECKPOINTER cp = toku_cachetable_get_checkpointer(env->i->cachetable);
    int r = toku_checkpoint(cp, env->i->logger,
                            checkpoint_callback_f,  checkpoint_callback_extra,
                            checkpoint_callback2_f, checkpoint_callback2_extra,
                            CLIENT_CHECKPOINT);
    if (r) {
        // Panicking the whole environment may be overkill, but I'm not sure what else to do.
        env_panic(env, r, "checkpoint error\n");
        toku_ydb_do_error(env, r, "Checkpoint\n");
    }
    return r;
}

static int 
env_txn_stat(DB_ENV * env, DB_TXN_STAT ** UU(statp), uint32_t UU(flags)) {
    HANDLE_PANICKED_ENV(env);
    return 1;
}

//
// We can assume the client calls this function right after recovery 
// to return a list of prepared transactions to the user. When called,
// we can assume that no other work is being done in the system, 
// as we are in the state of being after recovery, 
// but before client operations should commence
//
static int
env_txn_xa_recover (DB_ENV *env, TOKU_XA_XID xids[/*count*/], long count, /*out*/ long *retp, uint32_t flags) {
    struct tokulogger_preplist *MALLOC_N(count,preps);
    int r = toku_logger_recover_txn(env->i->logger, preps, count, retp, flags);
    if (r==0) {
        assert(*retp<=count);
        for (int i=0; i<*retp; i++) {
            xids[i] = preps[i].xid;
        }
    }
    toku_free(preps);
    return r;
}

//
// We can assume the client calls this function right after recovery 
// to return a list of prepared transactions to the user. When called,
// we can assume that no other work is being done in the system, 
// as we are in the state of being after recovery, 
// but before client operations should commence
//
static int
env_txn_recover (DB_ENV *env, DB_PREPLIST preplist[/*count*/], long count, /*out*/ long *retp, uint32_t flags) {
    struct tokulogger_preplist *MALLOC_N(count,preps);
    int r = toku_logger_recover_txn(env->i->logger, preps, count, retp, flags);
    if (r==0) {
        assert(*retp<=count);
        for (int i=0; i<*retp; i++) {
            preplist[i].txn = preps[i].txn;
            memcpy(preplist[i].gid, preps[i].xid.data, preps[i].xid.gtrid_length + preps[i].xid.bqual_length);
        }
    }
    toku_free(preps);
    return r;
}

static int
env_get_txn_from_xid (DB_ENV *env, /*in*/ TOKU_XA_XID *xid, /*out*/ DB_TXN **txnp) {
    return toku_txn_manager_get_root_txn_from_xid(toku_logger_get_txn_manager(env->i->logger), xid, txnp);
}

static int
env_checkpointing_set_period(DB_ENV * env, uint32_t seconds) {
    HANDLE_PANICKED_ENV(env);
    int r = 0;
    if (!env_opened(env)) {
        r = EINVAL;
    } else {
        toku_set_checkpoint_period(env->i->cachetable, seconds);
    }
    return r;
}

static int
env_cleaner_set_period(DB_ENV * env, uint32_t seconds) {
    HANDLE_PANICKED_ENV(env);
    int r = 0;
    if (!env_opened(env)) {
        r = EINVAL;
    } else {
        toku_set_cleaner_period(env->i->cachetable, seconds);
    }
    return r;
}

static int
env_cleaner_set_iterations(DB_ENV * env, uint32_t iterations) {
    HANDLE_PANICKED_ENV(env);
    int r = 0;
    if (!env_opened(env)) {
        r = EINVAL;
    } else {
        toku_set_cleaner_iterations(env->i->cachetable, iterations);
    }
    return r;
}

static int
env_create_loader(DB_ENV *env,
                  DB_TXN *txn,
                  DB_LOADER **blp,
                  DB *src_db,
                  int N,
                  DB *dbs[],
                  uint32_t db_flags[/*N*/],
                  uint32_t dbt_flags[/*N*/],
                  uint32_t loader_flags) {
    int r = toku_loader_create_loader(env, txn, blp, src_db, N, dbs, db_flags, dbt_flags, loader_flags, true);
    return r;
}

static int
env_checkpointing_get_period(DB_ENV * env, uint32_t *seconds) {
    HANDLE_PANICKED_ENV(env);
    int r = 0;
    if (!env_opened(env)) r = EINVAL;
    else 
        *seconds = toku_get_checkpoint_period_unlocked(env->i->cachetable);
    return r;
}

static int
env_cleaner_get_period(DB_ENV * env, uint32_t *seconds) {
    HANDLE_PANICKED_ENV(env);
    int r = 0;
    if (!env_opened(env)) r = EINVAL;
    else 
        *seconds = toku_get_cleaner_period_unlocked(env->i->cachetable);
    return r;
}

static int
env_cleaner_get_iterations(DB_ENV * env, uint32_t *iterations) {
    HANDLE_PANICKED_ENV(env);
    int r = 0;
    if (!env_opened(env)) r = EINVAL;
    else 
        *iterations = toku_get_cleaner_iterations(env->i->cachetable);
    return r;
}

static int
env_checkpointing_postpone(DB_ENV * env) {
    HANDLE_PANICKED_ENV(env);
    int r = 0;
    if (!env_opened(env)) r = EINVAL;
    else toku_checkpoint_safe_client_lock();
    return r;
}

static int
env_checkpointing_resume(DB_ENV * env) {
    HANDLE_PANICKED_ENV(env);
    int r = 0;
    if (!env_opened(env)) r = EINVAL;
    else toku_checkpoint_safe_client_unlock();
    return r;
}

static int
env_checkpointing_begin_atomic_operation(DB_ENV * env) {
    HANDLE_PANICKED_ENV(env);
    int r = 0;
    if (!env_opened(env)) r = EINVAL;
    else toku_multi_operation_client_lock();
    return r;
}

static int
env_checkpointing_end_atomic_operation(DB_ENV * env) {
    HANDLE_PANICKED_ENV(env);
    int r = 0;
    if (!env_opened(env)) r = EINVAL;
    else toku_multi_operation_client_unlock();
    return r;
}

static int
env_set_default_bt_compare(DB_ENV * env, ft_compare_func bt_compare) {
    HANDLE_PANICKED_ENV(env);
    int r = 0;
    if (env_opened(env)) r = EINVAL;
    else {
        env->i->bt_compare = bt_compare;
    }
    return r;
}

static void
env_set_update (DB_ENV *env, int (*update_function)(DB *, const DBT *key, const DBT *old_val, const DBT *extra, void (*set_val)(const DBT *new_val, void *set_extra), void *set_extra)) {
    env->i->update_function = update_function;
}

static int
env_set_generate_row_callback_for_put(DB_ENV *env, generate_row_for_put_func generate_row_for_put) {
    HANDLE_PANICKED_ENV(env);
    int r = 0;
    if (env_opened(env)) r = EINVAL;
    else {
        env->i->generate_row_for_put = generate_row_for_put;
    }
    return r;
}

static int
env_set_generate_row_callback_for_del(DB_ENV *env, generate_row_for_del_func generate_row_for_del) {
    HANDLE_PANICKED_ENV(env);
    int r = 0;
    if (env_opened(env)) r = EINVAL;
    else {
        env->i->generate_row_for_del = generate_row_for_del;
    }
    return r;
}
static int
env_set_redzone(DB_ENV *env, int redzone) {
    HANDLE_PANICKED_ENV(env);
    int r;
    if (env_opened(env))
        r = EINVAL;
    else {
        env->i->redzone = redzone;
        r = 0;
    }
    return r;
}

static int env_get_lock_timeout(DB_ENV *env, uint64_t *lock_timeout_msec) {
    uint64_t t = env->i->default_lock_timeout_msec;
    if (env->i->get_lock_timeout_callback)
        t = env->i->get_lock_timeout_callback(t);
    *lock_timeout_msec = t;
    return 0;
}

static int env_set_lock_timeout(DB_ENV *env, uint64_t default_lock_timeout_msec, uint64_t (*get_lock_timeout_callback)(uint64_t default_lock_timeout_msec)) {
    env->i->default_lock_timeout_msec = default_lock_timeout_msec;
    env->i->get_lock_timeout_callback = get_lock_timeout_callback;
    return 0;
}

static int
env_set_lock_timeout_callback(DB_ENV *env, lock_timeout_callback callback) {
    env->i->lock_wait_timeout_callback = callback;
    return 0;
}

static void
format_time(const time_t *timer, char *buf) {
    ctime_r(timer, buf);
    size_t len = strlen(buf);
    assert(len < 26);
    char end;

    assert(len>=1);
    end = buf[len-1];
    while (end == '\n' || end == '\r') {
        buf[len-1] = '\0';
        len--;
        assert(len>=1);
        end = buf[len-1];
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////
// Local definition of status information from portability layer, which should not include db.h.
// Local status structs are used to concentrate file system information collected from various places
// and memory information collected from memory.c.
//
typedef enum {
    FS_ENOSPC_REDZONE_STATE = 0,  // possible values are enumerated by fs_redzone_state
    FS_ENOSPC_THREADS_BLOCKED,    // how many threads currently blocked on ENOSPC
    FS_ENOSPC_REDZONE_CTR,        // number of operations rejected by enospc prevention (red zone)
    FS_ENOSPC_MOST_RECENT,        // most recent time that file system was completely full
    FS_ENOSPC_COUNT,              // total number of times ENOSPC was returned from an attempt to write
    FS_FSYNC_TIME,
    FS_FSYNC_COUNT,
    FS_LONG_FSYNC_TIME,
    FS_LONG_FSYNC_COUNT,
    FS_STATUS_NUM_ROWS,           // must be last
} fs_status_entry;

typedef struct {
    bool initialized;
    TOKU_ENGINE_STATUS_ROW_S status[FS_STATUS_NUM_ROWS];
} FS_STATUS_S, *FS_STATUS;

static FS_STATUS_S fsstat;

#define FS_STATUS_INIT(k,c,t,l,inc) TOKUFT_STATUS_INIT(fsstat, k, c, t, "filesystem: " l, inc)

static void
fs_status_init(void) {
    FS_STATUS_INIT(FS_ENOSPC_REDZONE_STATE,   nullptr, FS_STATE, "ENOSPC redzone state", TOKU_ENGINE_STATUS);
    FS_STATUS_INIT(FS_ENOSPC_THREADS_BLOCKED, FILESYSTEM_THREADS_BLOCKED_BY_FULL_DISK, UINT64,   "threads currently blocked by full disk", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FS_STATUS_INIT(FS_ENOSPC_REDZONE_CTR,     nullptr, UINT64,   "number of operations rejected by enospc prevention (red zone)", TOKU_ENGINE_STATUS);
    FS_STATUS_INIT(FS_ENOSPC_MOST_RECENT,     nullptr, UNIXTIME, "most recent disk full", TOKU_ENGINE_STATUS);
    FS_STATUS_INIT(FS_ENOSPC_COUNT,           nullptr, UINT64,   "number of write operations that returned ENOSPC", TOKU_ENGINE_STATUS);
    FS_STATUS_INIT(FS_FSYNC_TIME,             FILESYSTEM_FSYNC_TIME, UINT64,   "fsync time", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FS_STATUS_INIT(FS_FSYNC_COUNT,            FILESYSTEM_FSYNC_NUM, UINT64,   "fsync count", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FS_STATUS_INIT(FS_LONG_FSYNC_TIME,        FILESYSTEM_LONG_FSYNC_TIME, UINT64,   "long fsync time", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    FS_STATUS_INIT(FS_LONG_FSYNC_COUNT,       FILESYSTEM_LONG_FSYNC_NUM, UINT64,   "long fsync count", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    fsstat.initialized = true;
}
#undef FS_STATUS_INIT

#define FS_STATUS_VALUE(x) fsstat.status[x].value.num

static void
fs_get_status(DB_ENV * env, fs_redzone_state * redzone_state) {
    if (!fsstat.initialized)
        fs_status_init();
    
    time_t   enospc_most_recent_timestamp;
    uint64_t enospc_threads_blocked, enospc_total;
    toku_fs_get_write_info(&enospc_most_recent_timestamp, &enospc_threads_blocked, &enospc_total);
    if (enospc_threads_blocked)
        FS_STATUS_VALUE(FS_ENOSPC_REDZONE_STATE) = FS_BLOCKED;
    else
        FS_STATUS_VALUE(FS_ENOSPC_REDZONE_STATE) = env->i->fs_state;
    *redzone_state = (fs_redzone_state) FS_STATUS_VALUE(FS_ENOSPC_REDZONE_STATE);
    FS_STATUS_VALUE(FS_ENOSPC_THREADS_BLOCKED) = enospc_threads_blocked;
    FS_STATUS_VALUE(FS_ENOSPC_REDZONE_CTR) = env->i->enospc_redzone_ctr;
    FS_STATUS_VALUE(FS_ENOSPC_MOST_RECENT) = enospc_most_recent_timestamp;
    FS_STATUS_VALUE(FS_ENOSPC_COUNT) = enospc_total;
    
    uint64_t fsync_count, fsync_time, long_fsync_threshold, long_fsync_count, long_fsync_time;
    toku_get_fsync_times(&fsync_count, &fsync_time, &long_fsync_threshold, &long_fsync_count, &long_fsync_time);
    FS_STATUS_VALUE(FS_FSYNC_COUNT) = fsync_count;
    FS_STATUS_VALUE(FS_FSYNC_TIME) = fsync_time;
    FS_STATUS_VALUE(FS_LONG_FSYNC_COUNT) = long_fsync_count;
    FS_STATUS_VALUE(FS_LONG_FSYNC_TIME) = long_fsync_time;
}
#undef FS_STATUS_VALUE

// Local status struct used to get information from memory.c
typedef enum {
    MEMORY_MALLOC_COUNT = 0,
    MEMORY_FREE_COUNT,
    MEMORY_REALLOC_COUNT,
    MEMORY_MALLOC_FAIL,
    MEMORY_REALLOC_FAIL,
    MEMORY_REQUESTED,
    MEMORY_USED,
    MEMORY_FREED,
    MEMORY_MAX_REQUESTED_SIZE,
    MEMORY_LAST_FAILED_SIZE,
    MEMORY_MAX_IN_USE,
    MEMORY_MALLOCATOR_VERSION,
    MEMORY_MMAP_THRESHOLD,
    MEMORY_STATUS_NUM_ROWS
} memory_status_entry;

typedef struct {
    bool initialized;
    TOKU_ENGINE_STATUS_ROW_S status[MEMORY_STATUS_NUM_ROWS];
} MEMORY_STATUS_S, *MEMORY_STATUS;

static MEMORY_STATUS_S memory_status;

#define STATUS_INIT(k,c,t,l,inc) TOKUFT_STATUS_INIT(memory_status, k, c, t, "memory: " l, inc)

static void
memory_status_init(void) {
    // Note, this function initializes the keyname, type, and legend fields.
    // Value fields are initialized to zero by compiler.
    STATUS_INIT(MEMORY_MALLOC_COUNT,       nullptr, UINT64,  "number of malloc operations", TOKU_ENGINE_STATUS);
    STATUS_INIT(MEMORY_FREE_COUNT,         nullptr, UINT64,  "number of free operations", TOKU_ENGINE_STATUS);
    STATUS_INIT(MEMORY_REALLOC_COUNT,      nullptr, UINT64,  "number of realloc operations", TOKU_ENGINE_STATUS);
    STATUS_INIT(MEMORY_MALLOC_FAIL,        nullptr, UINT64,  "number of malloc operations that failed", TOKU_ENGINE_STATUS);
    STATUS_INIT(MEMORY_REALLOC_FAIL,       nullptr, UINT64,  "number of realloc operations that failed" , TOKU_ENGINE_STATUS);
    STATUS_INIT(MEMORY_REQUESTED,          nullptr, UINT64,  "number of bytes requested", TOKU_ENGINE_STATUS);
    STATUS_INIT(MEMORY_USED,               nullptr, UINT64,  "number of bytes used (requested + overhead)", TOKU_ENGINE_STATUS);
    STATUS_INIT(MEMORY_FREED,              nullptr, UINT64,  "number of bytes freed", TOKU_ENGINE_STATUS);
    STATUS_INIT(MEMORY_MAX_REQUESTED_SIZE, nullptr, UINT64,  "largest attempted allocation size", TOKU_ENGINE_STATUS);
    STATUS_INIT(MEMORY_LAST_FAILED_SIZE,   nullptr, UINT64,  "size of the last failed allocation attempt", TOKU_ENGINE_STATUS);
    STATUS_INIT(MEMORY_MAX_IN_USE,         MEM_ESTIMATED_MAXIMUM_MEMORY_FOOTPRINT, UINT64,  "estimated maximum memory footprint", TOKU_ENGINE_STATUS|TOKU_GLOBAL_STATUS);
    STATUS_INIT(MEMORY_MALLOCATOR_VERSION, nullptr, CHARSTR, "mallocator version", TOKU_ENGINE_STATUS);
    STATUS_INIT(MEMORY_MMAP_THRESHOLD,     nullptr, UINT64,  "mmap threshold", TOKU_ENGINE_STATUS);
    memory_status.initialized = true;  
}
#undef STATUS_INIT

#define MEMORY_STATUS_VALUE(x) memory_status.status[x].value.num

static void
memory_get_status(void) {
    if (!memory_status.initialized)
        memory_status_init();
    LOCAL_MEMORY_STATUS_S local_memstat;
    toku_memory_get_status(&local_memstat);
    MEMORY_STATUS_VALUE(MEMORY_MALLOC_COUNT) = local_memstat.malloc_count;
    MEMORY_STATUS_VALUE(MEMORY_FREE_COUNT) = local_memstat.free_count;  
    MEMORY_STATUS_VALUE(MEMORY_REALLOC_COUNT) = local_memstat.realloc_count;
    MEMORY_STATUS_VALUE(MEMORY_MALLOC_FAIL) = local_memstat.malloc_fail;
    MEMORY_STATUS_VALUE(MEMORY_REALLOC_FAIL) = local_memstat.realloc_fail;
    MEMORY_STATUS_VALUE(MEMORY_REQUESTED) = local_memstat.requested; 
    MEMORY_STATUS_VALUE(MEMORY_USED) = local_memstat.used;
    MEMORY_STATUS_VALUE(MEMORY_FREED) = local_memstat.freed;
    MEMORY_STATUS_VALUE(MEMORY_MAX_IN_USE) = local_memstat.max_in_use;
    MEMORY_STATUS_VALUE(MEMORY_MMAP_THRESHOLD) = local_memstat.mmap_threshold;
    memory_status.status[MEMORY_MALLOCATOR_VERSION].value.str = local_memstat.mallocator_version;
}
#undef MEMORY_STATUS_VALUE

// how many rows are in engine status?
int
env_get_engine_status_num_rows (DB_ENV * UU(env), uint64_t * num_rowsp) {
    uint64_t num_rows = 0;
    num_rows += YDB_LAYER_STATUS_NUM_ROWS;
    num_rows += YDB_C_LAYER_STATUS_NUM_ROWS;
    num_rows += YDB_WRITE_LAYER_STATUS_NUM_ROWS;
    num_rows += LE_STATUS_NUM_ROWS;
    num_rows += CP_STATUS_NUM_ROWS;
    num_rows += CT_STATUS_NUM_ROWS;
    num_rows += LTM_STATUS_NUM_ROWS;
    num_rows += FT_STATUS_NUM_ROWS;
    num_rows += FT_FLUSHER_STATUS_NUM_ROWS;
    num_rows += FT_HOT_STATUS_NUM_ROWS;
    num_rows += TXN_STATUS_NUM_ROWS;
    num_rows += LOGGER_STATUS_NUM_ROWS;
    num_rows += MEMORY_STATUS_NUM_ROWS;
    num_rows += FS_STATUS_NUM_ROWS;
    num_rows += INDEXER_STATUS_NUM_ROWS;
    num_rows += LOADER_STATUS_NUM_ROWS;
    num_rows += CTX_STATUS_NUM_ROWS;
#if 0
    // enable when upgrade is supported
    num_rows += FT_UPGRADE_STATUS_NUM_ROWS;
#endif
    *num_rowsp = num_rows;
    return 0;
}

// Do not take ydb lock or any other lock around or in this function.  
// If the engine is blocked because some thread is holding a lock, this function
// can help diagnose the problem.
// This function only collects information, and it does not matter if something gets garbled
// because of a race condition.  
// Note, engine status is still collected even if the environment or logger is panicked
static int
env_get_engine_status (DB_ENV * env, TOKU_ENGINE_STATUS_ROW engstat, uint64_t maxrows,  uint64_t *num_rows, fs_redzone_state* redzone_state, uint64_t * env_panicp, char * env_panic_string_buf, int env_panic_string_length, toku_engine_status_include_type include_flags) {
    int r;

    if (env_panic_string_buf) {
        if (env && env->i && env->i->is_panicked && env->i->panic_string) {
            strncpy(env_panic_string_buf, env->i->panic_string, env_panic_string_length);
            env_panic_string_buf[env_panic_string_length - 1] = '\0';  // just in case
        }
        else 
            *env_panic_string_buf = '\0';
    }

    if ( !(env)     ||
         !(env->i)  ||
         !(env_opened(env)) ||
         !num_rows ||
         !include_flags)
        r = EINVAL;
    else {
        r = 0;
        uint64_t row = 0;  // which row to fill next
        *env_panicp = env->i->is_panicked;

        {
            YDB_LAYER_STATUS_S ydb_stat;
            ydb_layer_get_status(env, &ydb_stat);
            for (int i = 0; i < YDB_LAYER_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (ydb_stat.status[i].include & include_flags) {
                    engstat[row++] = ydb_stat.status[i];
                }
            }
        }
        {
            YDB_C_LAYER_STATUS_S ydb_c_stat;
            ydb_c_layer_get_status(&ydb_c_stat);
            for (int i = 0; i < YDB_C_LAYER_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (ydb_c_stat.status[i].include & include_flags) {
                    engstat[row++] = ydb_c_stat.status[i];
                }
            }
        }
        {
            YDB_WRITE_LAYER_STATUS_S ydb_write_stat;
            ydb_write_layer_get_status(&ydb_write_stat);
            for (int i = 0; i < YDB_WRITE_LAYER_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (ydb_write_stat.status[i].include & include_flags) {
                    engstat[row++] = ydb_write_stat.status[i];
                }
            }
        }
        {
            LE_STATUS_S lestat;                    // Rice's vampire
            toku_le_get_status(&lestat);
            for (int i = 0; i < LE_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (lestat.status[i].include & include_flags) {
                    engstat[row++] = lestat.status[i];
                }
            }
        }
        {
            CHECKPOINT_STATUS_S cpstat;
            toku_checkpoint_get_status(env->i->cachetable, &cpstat);
            for (int i = 0; i < CP_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (cpstat.status[i].include & include_flags) {
                    engstat[row++] = cpstat.status[i];
                }
            }
        }
        {
            CACHETABLE_STATUS_S ctstat;
            toku_cachetable_get_status(env->i->cachetable, &ctstat);
            for (int i = 0; i < CT_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (ctstat.status[i].include & include_flags) {
                    engstat[row++] = ctstat.status[i];
                }
            }
        }
        {
            LTM_STATUS_S ltmstat;
            env->i->ltm.get_status(&ltmstat);
            for (int i = 0; i < LTM_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (ltmstat.status[i].include & include_flags) {
                    engstat[row++] = ltmstat.status[i];
                }
            }
        }
        {
            FT_STATUS_S ftstat;
            toku_ft_get_status(&ftstat);
            for (int i = 0; i < FT_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (ftstat.status[i].include & include_flags) {
                    engstat[row++] = ftstat.status[i];
                }
            }
        }
        {
            FT_FLUSHER_STATUS_S flusherstat;
            toku_ft_flusher_get_status(&flusherstat);
            for (int i = 0; i < FT_FLUSHER_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (flusherstat.status[i].include & include_flags) {
                    engstat[row++] = flusherstat.status[i];
                }
            }
        }
        {
            FT_HOT_STATUS_S hotstat;
            toku_ft_hot_get_status(&hotstat);
            for (int i = 0; i < FT_HOT_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (hotstat.status[i].include & include_flags) {
                    engstat[row++] = hotstat.status[i];
                }
            }
        }
        {
            TXN_STATUS_S txnstat;
            toku_txn_get_status(&txnstat);
            for (int i = 0; i < TXN_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (txnstat.status[i].include & include_flags) {
                    engstat[row++] = txnstat.status[i];
                }
            }
        }
        {
            LOGGER_STATUS_S loggerstat;
            toku_logger_get_status(env->i->logger, &loggerstat);
            for (int i = 0; i < LOGGER_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (loggerstat.status[i].include & include_flags) {
                    engstat[row++] = loggerstat.status[i];
                }
            }
        }

        {
            INDEXER_STATUS_S indexerstat;
            toku_indexer_get_status(&indexerstat);
            for (int i = 0; i < INDEXER_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (indexerstat.status[i].include & include_flags) {
                    engstat[row++] = indexerstat.status[i];
                }
            }
        }
        {
            LOADER_STATUS_S loaderstat;
            toku_loader_get_status(&loaderstat);
            for (int i = 0; i < LOADER_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (loaderstat.status[i].include & include_flags) {
                    engstat[row++] = loaderstat.status[i];
                }
            }
        }

        {
            // memory_status is local to this file
            memory_get_status();
            for (int i = 0; i < MEMORY_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (memory_status.status[i].include & include_flags) {
                    engstat[row++] = memory_status.status[i];
                }
            }
        }
        {
            // Note, fs_get_status() and the fsstat structure are local to this file because they
            // are used to concentrate file system information collected from various places.
            fs_get_status(env, redzone_state);
            for (int i = 0; i < FS_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (fsstat.status[i].include & include_flags) {
                    engstat[row++] = fsstat.status[i];
                }
            }
        }
        {
            struct context_status ctxstatus;
            toku_context_get_status(&ctxstatus);
            for (int i = 0; i < CTX_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (ctxstatus.status[i].include & include_flags) {
                    engstat[row++] = ctxstatus.status[i];
                }
            }
        }
#if 0
        // enable when upgrade is supported
        {
            for (int i = 0; i < PERSISTENT_UPGRADE_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (persistent_upgrade_status.status[i].include & include_flags) {
                    engstat[row++] = persistent_upgrade_status.status[i];
                }
            }
            FT_UPGRADE_STATUS_S ft_upgradestat;
            toku_ft_upgrade_get_status(&ft_upgradestat);
            for (int i = 0; i < FT_UPGRADE_STATUS_NUM_ROWS && row < maxrows; i++) {
                if (ft_upgradestat.status[i].include & include_flags) {
                    engstat[row++] = ft_upgradestat.status[i];
                }
            }

        }
#endif
        if (r==0) {
            *num_rows = row;
        }
    }
    return r;
}

// Fill buff with text description of engine status up to bufsiz bytes.
// Intended for use by test programs that do not have the handlerton available,
// and for use by toku_assert logic to print diagnostic info on crash.
static int
env_get_engine_status_text(DB_ENV * env, char * buff, int bufsiz) {
    uint32_t stringsize = 1024;
    uint64_t panic;
    char panicstring[stringsize];
    int n = 0;  // number of characters printed so far
    uint64_t num_rows;
    uint64_t max_rows;
    fs_redzone_state redzone_state;

    n = snprintf(buff, bufsiz - n, "BUILD_ID = %d\n", BUILD_ID);

    (void) env_get_engine_status_num_rows (env, &max_rows);
    TOKU_ENGINE_STATUS_ROW_S mystat[max_rows];
    int r = env->get_engine_status (env, mystat, max_rows, &num_rows, &redzone_state, &panic, panicstring, stringsize, TOKU_ENGINE_STATUS);

    if (r) {
        n += snprintf(buff + n, bufsiz - n, "Engine status not available: ");
        if (!env) {
            n += snprintf(buff + n, bufsiz - n, "no environment\n");
        }
        else if (!(env->i)) {
            n += snprintf(buff + n, bufsiz - n, "environment internal struct is null\n");
        }
        else if (!env_opened(env)) {
            n += snprintf(buff + n, bufsiz - n, "environment is not open\n");
        }
    }
    else {
        if (panic) {
            n += snprintf(buff + n, bufsiz - n, "Env panic code: %" PRIu64 "\n", panic);
            if (strlen(panicstring)) {
                invariant(strlen(panicstring) <= stringsize);
                n += snprintf(buff + n, bufsiz - n, "Env panic string: %s\n", panicstring);
            }
        }

        for (uint64_t row = 0; row < num_rows; row++) {
            n += snprintf(buff + n, bufsiz - n, "%s: ", mystat[row].legend);
            switch (mystat[row].type) {
            case FS_STATE:
                n += snprintf(buff + n, bufsiz - n, "%" PRIu64 "\n", mystat[row].value.num);
                break;
            case UINT64:
                n += snprintf(buff + n, bufsiz - n, "%" PRIu64 "\n", mystat[row].value.num);
                break;
            case CHARSTR:
                n += snprintf(buff + n, bufsiz - n, "%s\n", mystat[row].value.str);
                break;
            case UNIXTIME:
                {
                    char tbuf[26];
                    format_time((time_t*)&mystat[row].value.num, tbuf);
                    n += snprintf(buff + n, bufsiz - n, "%s\n", tbuf);
                }
                break;
            case TOKUTIME:
                {
                    double t = tokutime_to_seconds(mystat[row].value.num);
                    n += snprintf(buff + n, bufsiz - n, "%.6f\n", t);
                }
                break;
            case PARCOUNT:
                {
                    uint64_t v = read_partitioned_counter(mystat[row].value.parcount);
                    n += snprintf(buff + n, bufsiz - n, "%" PRIu64 "\n", v);
                }
                break;
            default:
                n += snprintf(buff + n, bufsiz - n, "UNKNOWN STATUS TYPE: %d\n", mystat[row].type);
                break;                
            }
        }
    }
        
    if (n > bufsiz) {
        const char * errmsg = "BUFFER TOO SMALL\n";
        int len = strlen(errmsg) + 1;
        (void) snprintf(buff + (bufsiz - 1) - len, len, "%s", errmsg);
    }

    return r;
}

// prints engine status using toku_env_err line-by-line
static int
env_err_engine_status(DB_ENV * env) {
    uint32_t stringsize = 1024;
    uint64_t panic;
    char panicstring[stringsize];
    uint64_t num_rows;
    uint64_t max_rows;
    fs_redzone_state redzone_state;

    toku_env_err(env, 0, "BUILD_ID = %d", BUILD_ID);

    (void) env_get_engine_status_num_rows (env, &max_rows);
    TOKU_ENGINE_STATUS_ROW_S mystat[max_rows];
    int r = env->get_engine_status (env, mystat, max_rows, &num_rows, &redzone_state, &panic, panicstring, stringsize, TOKU_ENGINE_STATUS);

    if (r) {
        toku_env_err(env, 0, "Engine status not available: ");
        if (!env) {
            toku_env_err(env, 0, "no environment");
        }
        else if (!(env->i)) {
            toku_env_err(env, 0, "environment internal struct is null");
        }
        else if (!env_opened(env)) {
            toku_env_err(env, 0, "environment is not open");
        }
    }
    else {
        if (panic) {
            toku_env_err(env, 0, "Env panic code: %" PRIu64, panic);
            if (strlen(panicstring)) {
                invariant(strlen(panicstring) <= stringsize);
                toku_env_err(env, 0, "Env panic string: %s", panicstring);
            }
        }

        for (uint64_t row = 0; row < num_rows; row++) {
            switch (mystat[row].type) {
            case FS_STATE:
                toku_env_err(env, 0, "%s: %" PRIu64, mystat[row].legend, mystat[row].value.num);
                break;
            case UINT64:
                toku_env_err(env, 0, "%s: %" PRIu64, mystat[row].legend, mystat[row].value.num);
                break;
            case CHARSTR:
                toku_env_err(env, 0, "%s: %s", mystat[row].legend, mystat[row].value.str);
                break;
            case UNIXTIME:
                {
                    char tbuf[26];
                    format_time((time_t*)&mystat[row].value.num, tbuf);
                    toku_env_err(env, 0, "%s: %s", mystat[row].legend, tbuf);
                }
                break;
            case TOKUTIME:
                {
                    double t = tokutime_to_seconds(mystat[row].value.num);
                    toku_env_err(env, 0, "%s: %.6f", mystat[row].legend, t);
                }
                break;
            case PARCOUNT:
                {
                    uint64_t v = read_partitioned_counter(mystat[row].value.parcount);
                    toku_env_err(env, 0, "%s: %" PRIu64, mystat[row].legend, v);
                }
                break;
            default:
                toku_env_err(env, 0, "%s: UNKNOWN STATUS TYPE: %d", mystat[row].legend, mystat[row].type);
                break;
            }
        }
    }

    return r;
}

// intended for use by toku_assert logic, when env is not known
int 
toku_maybe_get_engine_status_text (char * buff, int buffsize) {
    DB_ENV * env = most_recent_env;
    int r;
    if (engine_status_enable && env != NULL) {
        r = env_get_engine_status_text(env, buff, buffsize);
    }
    else {
        r = EOPNOTSUPP;
        snprintf(buff, buffsize, "Engine status not available: disabled by user.  This should only happen in test programs.\n");
    }
    return r;
}

int
toku_maybe_err_engine_status (void) {
    DB_ENV * env = most_recent_env;
    int r;
    if (engine_status_enable && env != NULL) {
        r = env_err_engine_status(env);
    }
    else {
        r = EOPNOTSUPP;
    }
    return r;
}

// handlerton's call to fractal tree layer on failed assert in handlerton
static int 
env_crash(DB_ENV * UU(db_env), const char* msg, const char * fun, const char* file, int line, int caller_errno) {
    toku_do_assert_fail(msg, fun, file, line, caller_errno);
    return -1;  // placate compiler
}

static int
env_get_cursor_for_persistent_environment(DB_ENV* env, DB_TXN* txn, DBC** c) {
    if (!env_opened(env)) {
        return EINVAL;
    }
    return env->i->dict_manager.get_persistent_environment_cursor(txn, c);
}

static int
env_get_cursor_for_directory(DB_ENV* env, DB_TXN* txn, DBC** c) {
    if (!env_opened(env)) {
        return EINVAL;
    }
    return env->i->dict_manager.get_directory_cursor(txn, c);
}

struct ltm_iterate_requests_callback_extra {
    ltm_iterate_requests_callback_extra(DB_ENV *e,
                                        iterate_requests_callback cb,
                                        void *ex) :
        env(e), callback(cb), extra(ex) {
    }
    DB_ENV *env;
    iterate_requests_callback callback;
    void *extra;
};

static int ltm_iterate_requests_callback(DICTIONARY_ID dict_id UU(), TXNID txnid,
                                         const DBT *left_key,
                                         const DBT *right_key,
                                         TXNID blocking_txnid,
                                         uint64_t start_time,
                                         void *extra) {
    ltm_iterate_requests_callback_extra *info =
        reinterpret_cast<ltm_iterate_requests_callback_extra *>(extra);

    int r = 0;
    assert(false);
    DB *db = NULL;
    //DB *db = locked_get_db_by_dict_id(info->env, dict_id);
    if (db != nullptr) {
        r = info->callback(db, txnid, left_key, right_key,
                           blocking_txnid, start_time, info->extra);
    }
    return r;
}

static int
env_iterate_pending_lock_requests(DB_ENV *env,
                                  iterate_requests_callback callback,
                                  void *extra) {
    if (!env_opened(env)) {
        return EINVAL;
    }

    toku::locktree_manager *mgr = &env->i->ltm;
    ltm_iterate_requests_callback_extra e(env, callback, extra);
    return mgr->iterate_pending_lock_requests(ltm_iterate_requests_callback, &e);
}

// for the lifetime of this object:
// - txn_mutex must be held
struct iter_txn_row_locks_callback_extra {
    iter_txn_row_locks_callback_extra(DB_ENV *e, toku::omt<txn_lt_key_ranges> *m) :
        env(e), current_db(nullptr), which_lt(0), lt_map(m) {
        if (lt_map->size() > 0) {
            set_iterator_and_current_db();
        }
    }

    void set_iterator_and_current_db() {
        txn_lt_key_ranges ranges;
        const int r = lt_map->fetch(which_lt, &ranges);
        invariant_zero(r);
        assert(false);
        //current_db = locked_get_db_by_dict_id(env, ranges.lt->get_dict_id());
        iter = toku::range_buffer::iterator(ranges.buffer);
    }

    DB_ENV *env;
    DB *current_db;
    size_t which_lt;
    toku::omt<txn_lt_key_ranges> *lt_map;
    toku::range_buffer::iterator iter;
    toku::range_buffer::iterator::record rec;
};

static int iter_txn_row_locks_callback(DB **db, DBT *left_key, DBT *right_key, void *extra) {
    iter_txn_row_locks_callback_extra *info =
        reinterpret_cast<iter_txn_row_locks_callback_extra *>(extra);

    while (info->which_lt < info->lt_map->size()) {
        const bool more = info->iter.current(&info->rec);
        if (more) {
            *db = info->current_db;
            // The caller should interpret data/size == 0 to mean infinity.
            // Therefore, when we copyref pos/neg infinity into left/right_key,
            // the caller knows what we're talking about.
            toku_copyref_dbt(left_key, *info->rec.get_left_key());
            toku_copyref_dbt(right_key, *info->rec.get_right_key());
            info->iter.next();
            return 0;
        } else {
            info->which_lt++;
            if (info->which_lt < info->lt_map->size()) {
                info->set_iterator_and_current_db();
            }
        }
    }
    return DB_NOTFOUND;
}

struct iter_txns_callback_extra {
    iter_txns_callback_extra(DB_ENV *e, iterate_transactions_callback cb, void *ex) :
        env(e), callback(cb), extra(ex) {
    }
    DB_ENV *env;
    iterate_transactions_callback callback;
    void *extra;
};

static int iter_txns_callback(TOKUTXN txn, void *extra) {
    iter_txns_callback_extra *info =
        reinterpret_cast<iter_txns_callback_extra *>(extra);

    DB_TXN *dbtxn = toku_txn_get_container_db_txn(txn);
    invariant_notnull(dbtxn);

    toku_mutex_lock(&db_txn_struct_i(dbtxn)->txn_mutex);

    iter_txn_row_locks_callback_extra e(info->env, &db_txn_struct_i(dbtxn)->lt_map);
    const int r = info->callback(toku_txn_get_txnid(txn).parent_id64,
                                 toku_txn_get_client_id(txn),
                                 iter_txn_row_locks_callback,
                                 &e,
                                 info->extra);

    toku_mutex_unlock(&db_txn_struct_i(dbtxn)->txn_mutex);

    return r;
}

static int
env_iterate_live_transactions(DB_ENV *env,
                              iterate_transactions_callback callback,
                              void *extra) {
    if (!env_opened(env)) {
        return EINVAL;
    }
    if (false) {
        // TODO: FIX!!
    TXN_MANAGER txn_manager = toku_logger_get_txn_manager(env->i->logger);
    iter_txns_callback_extra e(env, callback, extra);
    return toku_txn_manager_iter_over_live_root_txns(txn_manager, iter_txns_callback, &e);
    }
    return 0;
}

static void env_set_loader_memory_size(DB_ENV *env, uint64_t (*get_loader_memory_size_callback)(void)) {
    env->i->get_loader_memory_size_callback = get_loader_memory_size_callback;
}

static uint64_t env_get_loader_memory_size(DB_ENV *env) {
    uint64_t memory_size = 0;
    if (env->i->get_loader_memory_size_callback)
        memory_size = env->i->get_loader_memory_size_callback();
    return memory_size;
}

static void env_set_killed_callback(DB_ENV *env, uint64_t default_killed_time_msec, uint64_t (*get_killed_time_callback)(uint64_t default_killed_time_msec), int (*killed_callback)(void)) {
    env->i->default_killed_time_msec = default_killed_time_msec;
    env->i->get_killed_time_callback = get_killed_time_callback;
    env->i->killed_callback = killed_callback;
}

static void env_do_backtrace(DB_ENV *env) {
    if (env->i->errcall) {
        db_env_do_backtrace_errfunc((toku_env_err_func) toku_env_err, (const void *) env);
    }
    if (env->i->errfile) {
        db_env_do_backtrace((FILE *) env->i->errfile);
    } else {
        db_env_do_backtrace(stderr);
    }
}

static int 
toku_env_create(DB_ENV ** envp, uint32_t flags) {
    int r = ENOSYS;
    DB_ENV* result = NULL;

    if (flags!=0)    { r = EINVAL; goto cleanup; }
    MALLOC(result);
    if (result == 0) { r = ENOMEM; goto cleanup; }
    memset(result, 0, sizeof *result);

    // locked methods
    result->err = (void (*)(const DB_ENV * env, int error, const char *fmt, ...)) toku_env_err;
#define SENV(name) result->name = locked_env_ ## name
    SENV(dbremove);
    SENV(dbrename);
    //SENV(set_noticecall);
#undef SENV
#define USENV(name) result->name = env_ ## name
    // methods with locking done internally
    USENV(put_multiple);
    USENV(del_multiple);
    USENV(update_multiple);
    // unlocked methods
    USENV(open);
    USENV(close);
    USENV(set_default_bt_compare);
    USENV(set_update);
    USENV(set_generate_row_callback_for_put);
    USENV(set_generate_row_callback_for_del);
    USENV(set_lg_bsize);
    USENV(set_lg_dir);
    USENV(set_lg_max);
    USENV(get_lg_max);
    USENV(set_lk_max_memory);
    USENV(get_lk_max_memory);
    USENV(get_iname);
    USENV(set_errcall);
    USENV(set_errfile);
    USENV(set_errpfx);
    USENV(set_data_dir);
    USENV(checkpointing_set_period);
    USENV(checkpointing_get_period);
    USENV(cleaner_set_period);
    USENV(cleaner_get_period);
    USENV(cleaner_set_iterations);
    USENV(cleaner_get_iterations);
    USENV(set_cachesize);
#if DB_VERSION_MAJOR == 4 && DB_VERSION_MINOR >= 3
    USENV(get_cachesize);
#endif
#if DB_VERSION_MAJOR == 4 && DB_VERSION_MINOR <= 4
    USENV(set_lk_max);
#endif
    USENV(set_lk_detect);
    USENV(set_flags);
    USENV(set_tmp_dir);
    USENV(set_verbose);
    USENV(txn_recover);
    USENV(txn_xa_recover);
    USENV(get_txn_from_xid);
    USENV(txn_stat);
    USENV(get_lock_timeout);
    USENV(set_lock_timeout);
    USENV(set_lock_timeout_callback);
    USENV(set_redzone);
    USENV(log_flush);
    USENV(log_archive);
    USENV(create_loader);
    USENV(get_cursor_for_persistent_environment);
    USENV(get_cursor_for_directory);
    USENV(iterate_pending_lock_requests);
    USENV(iterate_live_transactions);
    USENV(change_fsync_log_period);
    USENV(set_loader_memory_size);
    USENV(get_loader_memory_size);
    USENV(set_killed_callback);
    USENV(do_backtrace);
#undef USENV
    
    // unlocked methods
    result->create_indexer = toku_indexer_create_indexer;
    result->txn_checkpoint = toku_env_txn_checkpoint;
    result->checkpointing_postpone = env_checkpointing_postpone;
    result->checkpointing_resume = env_checkpointing_resume;
    result->checkpointing_begin_atomic_operation = env_checkpointing_begin_atomic_operation;
    result->checkpointing_end_atomic_operation = env_checkpointing_end_atomic_operation;
    result->get_engine_status_num_rows = env_get_engine_status_num_rows;
    result->get_engine_status = env_get_engine_status;
    result->get_engine_status_text = env_get_engine_status_text;
    result->crash = env_crash;  // handlerton's call to fractal tree layer on failed assert
    result->txn_begin = toku_txn_begin;

    MALLOC(result->i);
    if (result->i == 0) { r = ENOMEM; goto cleanup; }
    memset(result->i, 0, sizeof *result->i);
    result->i->envdir_lockfd  = -1;
    result->i->datadir_lockfd = -1;
    result->i->logdir_lockfd  = -1;
    result->i->tmpdir_lockfd  = -1;
    env_fs_init(result);
    env_fsync_log_init(result);

    result->i->bt_compare = toku_builtin_compare_fun;

    r = toku_logger_create(&result->i->logger);
    invariant_zero(r);
    invariant_notnull(result->i->logger);

    // Create the locktree manager, passing in the create/destroy/escalate callbacks.
    // The extra parameter for escalation is simply a pointer to this environment.
    // The escalate callback will need it to translate txnids to DB_TXNs
    result->i->ltm.create(toku_db_lt_on_create_callback, toku_db_lt_on_destroy_callback, toku_db_txn_escalate_callback, result);
    result->i->dict_manager.create();

    *envp = result;
    r = 0;
    toku_sync_fetch_and_add(&tokuft_num_envs, 1);
cleanup:
    if (r!=0) {
        if (result) {
            toku_free(result->i);
            toku_free(result);
        }
    }
    return r;
}

int 
DB_ENV_CREATE_FUN (DB_ENV ** envp, uint32_t flags) {
    int r = toku_env_create(envp, flags); 
    return r;
}

//We do not (yet?) support deleting subdbs by deleting the enclosing 'fname'
static int
env_dbremove_subdb(DB_ENV * env, DB_TXN * txn, const char *fname, const char *dbname, int32_t flags) {
    int r;
    if (!fname || !dbname) r = EINVAL;
    else {
        char subdb_full_name[strlen(fname) + sizeof("/") + strlen(dbname)];
        int bytes = snprintf(subdb_full_name, sizeof(subdb_full_name), "%s/%s", fname, dbname);
        assert(bytes==(int)sizeof(subdb_full_name)-1);
        const char *null_subdbname = NULL;
        r = env_dbremove(env, txn, subdb_full_name, null_subdbname, flags);
    }
    return r;
}

static int
env_dbremove(DB_ENV * env, DB_TXN *txn, const char *fname, const char *dbname, uint32_t flags) {
    HANDLE_PANICKED_ENV(env);
    if (!env_opened(env) || flags != 0) {
        return EINVAL;
    }
    HANDLE_READ_ONLY_TXN(txn);
    if (dbname != NULL) {
        // env_dbremove_subdb() converts (fname, dbname) to dname
        return env_dbremove_subdb(env, txn, fname, dbname, flags);
    }

    const char * dname = fname;
    assert(dbname == NULL);
    return env->i->dict_manager.remove(dname, env, txn);
}

static int
env_dbrename_subdb(DB_ENV *env, DB_TXN *txn, const char *fname, const char *dbname, const char *newname, uint32_t flags) {
    int r;
    if (!fname || !dbname || !newname) r = EINVAL;
    else {
        char subdb_full_name[strlen(fname) + sizeof("/") + strlen(dbname)];
        {
            int bytes = snprintf(subdb_full_name, sizeof(subdb_full_name), "%s/%s", fname, dbname);
            assert(bytes==(int)sizeof(subdb_full_name)-1);
        }
        char new_full_name[strlen(fname) + sizeof("/") + strlen(dbname)];
        {
            int bytes = snprintf(new_full_name, sizeof(new_full_name), "%s/%s", fname, dbname);
            assert(bytes==(int)sizeof(new_full_name)-1);
        }
        const char *null_subdbname = NULL;
        r = env_dbrename(env, txn, subdb_full_name, null_subdbname, new_full_name, flags);
    }
    return r;
}

static int
env_dbrename(DB_ENV *env, DB_TXN *txn, const char *fname, const char *dbname, const char *newname, uint32_t flags) {
    HANDLE_PANICKED_ENV(env);
    if (!env_opened(env) || flags != 0) {
        return EINVAL;
    }
    HANDLE_READ_ONLY_TXN(txn);
    if (dbname != NULL) {
        // env_dbrename_subdb() converts (fname, dbname) to dname and (fname, newname) to newdname
        return env_dbrename_subdb(env, txn, fname, dbname, newname, flags);
    }

    const char * dname = fname;
    assert(dbname == NULL);

    return env->i->dict_manager.rename(env, txn, dname, newname);
}

int 
DB_CREATE_FUN (DB ** db, DB_ENV * env, uint32_t flags) {
    int r = toku_db_create(db, env, flags); 
    return r;
}

/* need db_strerror_r for multiple threads */

const char *
db_strerror(int error) {
    char *errorstr;
    if (error >= 0) {
        errorstr = strerror(error);
        if (errorstr)
            return errorstr;
    }
    
    switch (error) {
        case DB_BADFORMAT:
            return "Database Bad Format (probably a corrupted database)";
        case DB_NOTFOUND:
            return "Not found";
        case TOKUDB_OUT_OF_LOCKS:
            return "Out of locks";
        case TOKUDB_DICTIONARY_TOO_OLD:
            return "Dictionary too old for this version of TokuFT";
        case TOKUDB_DICTIONARY_TOO_NEW:
            return "Dictionary too new for this version of TokuFT";
        case TOKUDB_CANCELED:
            return "User cancelled operation";
        case TOKUDB_NO_DATA:
            return "Ran out of data (not EOF)";
        case TOKUDB_HUGE_PAGES_ENABLED:
            return "Transparent huge pages are enabled but TokuFT's memory allocator will oversubscribe main memory with transparent huge pages.  This check can be disabled by setting the environment variable TOKU_HUGE_PAGES_OK.";
    }

    static char unknown_result[100];    // Race condition if two threads call this at the same time. However even in a bad case, it should be some sort of null-terminated string.
    errorstr = unknown_result;
    snprintf(errorstr, sizeof unknown_result, "Unknown error code: %d", error);
    return errorstr;
}

const char *
db_version(int *major, int *minor, int *patch) {
    if (major)
        *major = DB_VERSION_MAJOR;
    if (minor)
        *minor = DB_VERSION_MINOR;
    if (patch)
        *patch = DB_VERSION_PATCH;
    return toku_product_name_strings.db_version;
}
 
// HACK: To ensure toku_pthread_yield gets included in the .so
// non-static would require a prototype in a header
// static (since unused) would give a warning
// static + unused would not actually help toku_pthread_yield get in the .so
// static + used avoids all the warnings and makes sure toku_pthread_yield is in the .so
static void __attribute__((__used__))
include_toku_pthread_yield (void) {
    toku_pthread_yield();
}

// For test purposes only, translate dname to iname
// YDB lock is NOT held when this function is called,
// as it is called by user
static int 
env_get_iname(DB_ENV* env, DBT* dname_dbt, DBT* iname_dbt) {
    return env->i->dict_manager.get_iname_in_dbt(dname_dbt, iname_dbt);
}

// TODO 2216:  Patch out this (dangerous) function when loader is working and 
//             we don't need to test the low-level redirect anymore.
// for use by test programs only, just a wrapper around ft call:
int
toku_test_db_redirect_dictionary(DB * db, const char * dname_of_new_file, DB_TXN *dbtxn) {
    int r;
    char * new_iname_in_env;

    FT_HANDLE ft_handle = db->i->ft_handle;
    TOKUTXN tokutxn = db_txn_struct_i(dbtxn)->tokutxn;

    r = db->dbenv->i->dict_manager.get_iname(dname_of_new_file, dbtxn, &new_iname_in_env);
    assert_zero(r);

    toku_multi_operation_client_lock(); //Must hold MO lock for dictionary_redirect.
    r = toku_dictionary_redirect(new_iname_in_env, ft_handle, tokutxn);
    toku_multi_operation_client_unlock();

    toku_free(new_iname_in_env);
    return r;
}

//Tets only function
uint64_t
toku_test_get_latest_lsn(DB_ENV *env) {
    LSN rval = ZERO_LSN;
    if (env && env->i->logger) {
        rval = toku_logger_last_lsn(env->i->logger);
    }
    return rval.lsn;
}

int 
toku_test_get_checkpointing_user_data_status (void) {
    return toku_cachetable_get_checkpointing_user_data_status();
}

#undef STATUS_VALUE
#undef PERSISTENT_UPGRADE_STATUS_VALUE

#include <toku_race_tools.h>
void __attribute__((constructor)) toku_ydb_helgrind_ignore(void);
void
toku_ydb_helgrind_ignore(void) {
    TOKU_VALGRIND_HG_DISABLE_CHECKING(&ydb_layer_status, sizeof ydb_layer_status);
}
