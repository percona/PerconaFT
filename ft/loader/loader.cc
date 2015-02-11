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

#include <toku_portability.h>

#include <arpa/inet.h>

#include <stdio.h>
#include <memory.h>
#include <errno.h>
#include <toku_assert.h>
#include <string.h>
#include <fcntl.h>

#include "ft/ft-internal.h"
#include "ft/loader/loader-internal.h"
#include "ft/loader/pqueue.h"
#include "ft/loader/dbufio.h"

static size_t (*os_fwrite_fun)(const void *,size_t,size_t,FILE*)=NULL;
void ft_loader_set_os_fwrite (size_t (*fwrite_fun)(const void*,size_t,size_t,FILE*)) {
    os_fwrite_fun=fwrite_fun;
}

static size_t do_fwrite (const void *ptr, size_t size, size_t nmemb, FILE *stream) {
    if (os_fwrite_fun) {
        return os_fwrite_fun(ptr, size, nmemb, stream);
    } else {
        return fwrite(ptr, size, nmemb, stream);
    }
}


// 1024 is the right size_factor for production.  
// Different values for these sizes may be used for testing.
static uint32_t size_factor = 1024;
static uint32_t default_loader_nodesize = FT_DEFAULT_NODE_SIZE;

void
toku_ft_loader_set_size_factor(uint32_t factor) {
// For test purposes only
    size_factor = factor;
    default_loader_nodesize = (size_factor==1) ? (1<<15) : FT_DEFAULT_NODE_SIZE;
}

uint64_t
toku_ft_loader_get_rowset_budget_for_testing (void)
// For test purposes only.  In production, the rowset size is determined by negotation with the cachetable for some memory.  (See #2613).
{
    return 16ULL*size_factor*1024ULL;
}

void ft_loader_lock_init(FTLOADER bl) {
    invariant(!bl->mutex_init);
    toku_mutex_init(&bl->mutex, NULL); 
    bl->mutex_init = true;
}

void ft_loader_lock_destroy(FTLOADER bl) {
    if (bl->mutex_init) {
        toku_mutex_destroy(&bl->mutex);
        bl->mutex_init = false;
    }
}

static void ft_loader_lock(FTLOADER bl) {
    invariant(bl->mutex_init);
    toku_mutex_lock(&bl->mutex);
}

static void ft_loader_unlock(FTLOADER bl) {
    invariant(bl->mutex_init);
    toku_mutex_unlock(&bl->mutex);
}

static int add_big_buffer(struct file_info *file) {
    int result = 0;
    bool newbuffer = false;
    if (file->buffer == NULL) {
        file->buffer = toku_malloc(file->buffer_size);
        if (file->buffer == NULL)
            result = get_error_errno();
        else
            newbuffer = true;
    }
    if (result == 0) {
        int r = setvbuf(file->file, (char *) file->buffer, _IOFBF, file->buffer_size);
        if (r != 0) {
            result = get_error_errno();
            if (newbuffer) {
                toku_free(file->buffer);
                file->buffer = NULL;
            }
        }
    } 
    return result;
}

static void cleanup_big_buffer(struct file_info *file) {
    if (file->buffer) {
        toku_free(file->buffer);
        file->buffer = NULL;
    }
}

int ft_loader_init_file_infos (struct file_infos *fi) {
    int result = 0;
    toku_mutex_init(&fi->lock, NULL);
    fi->n_files = 0;
    fi->n_files_limit = 1;
    fi->n_files_open = 0;
    fi->n_files_extant = 0;
    MALLOC_N(fi->n_files_limit, fi->file_infos);
    if (fi->file_infos == NULL)
        result = get_error_errno();
    return result;
}

void ft_loader_fi_destroy (struct file_infos *fi, bool is_error)
// Effect: Free the resources in the fi.
// If is_error then we close and unlink all the temp files.
// If !is_error then requires that all the temp files have been closed and destroyed
// No error codes are returned.  If anything goes wrong with closing and unlinking then it's only in an is_error case, so we don't care.
{
    if (fi->file_infos == NULL) {
        // ft_loader_init_file_infos guarantees this isn't null, so if it is, we know it hasn't been inited yet and we don't need to destroy it.
        return;
    }
    toku_mutex_destroy(&fi->lock);
    if (!is_error) {
        invariant(fi->n_files_open==0);
        invariant(fi->n_files_extant==0);
    }
    for (int i=0; i<fi->n_files; i++) {
        if (fi->file_infos[i].is_open) {
            invariant(is_error);
            toku_os_fclose(fi->file_infos[i].file); // don't check for errors, since we are in an error case.
        }
        if (fi->file_infos[i].is_extant) {
            invariant(is_error);
            unlink(fi->file_infos[i].fname);
            toku_free(fi->file_infos[i].fname);
        }
        cleanup_big_buffer(&fi->file_infos[i]);
    }
    toku_free(fi->file_infos);
    fi->n_files=0;
    fi->n_files_limit=0;
    fi->file_infos = NULL;
}

static int open_file_add (struct file_infos *fi,
                          FILE *file,
                          char *fname,
                          /* out */ FIDX *idx)
{
    int result = 0;
    toku_mutex_lock(&fi->lock);
    if (fi->n_files >= fi->n_files_limit) {
        fi->n_files_limit *=2;
        XREALLOC_N(fi->n_files_limit, fi->file_infos);
    }
    invariant(fi->n_files < fi->n_files_limit);
    fi->file_infos[fi->n_files].is_open   = true;
    fi->file_infos[fi->n_files].is_extant = true;
    fi->file_infos[fi->n_files].fname     = fname;
    fi->file_infos[fi->n_files].file      = file;
    fi->file_infos[fi->n_files].n_rows    = 0;
    fi->file_infos[fi->n_files].buffer_size = FILE_BUFFER_SIZE;
    fi->file_infos[fi->n_files].buffer    = NULL;
    result = add_big_buffer(&fi->file_infos[fi->n_files]);
    if (result == 0) {
        idx->idx = fi->n_files;
        fi->n_files++;
        fi->n_files_extant++;
        fi->n_files_open++;
    }
   toku_mutex_unlock(&fi->lock);
    return result;
}

int ft_loader_fi_reopen (struct file_infos *fi, FIDX idx, const char *mode) {
    int result = 0;
    toku_mutex_lock(&fi->lock);
    int i = idx.idx;
    invariant(i>=0 && i<fi->n_files);
    invariant(!fi->file_infos[i].is_open);
    invariant(fi->file_infos[i].is_extant);
    fi->file_infos[i].file = toku_os_fopen(fi->file_infos[i].fname, mode);
    if (fi->file_infos[i].file == NULL) { 
        result = get_error_errno();
    } else {
        fi->file_infos[i].is_open = true;
        // No longer need the big buffer for reopened files.  Don't allocate the space, we need it elsewhere.
        //add_big_buffer(&fi->file_infos[i]);
        fi->n_files_open++;
    }
    toku_mutex_unlock(&fi->lock);
    return result;
}

int ft_loader_fi_close (struct file_infos *fi, FIDX idx, bool require_open)
{
    int result = 0;
    toku_mutex_lock(&fi->lock); 
    invariant(idx.idx >=0 && idx.idx < fi->n_files);
    if (fi->file_infos[idx.idx].is_open) {
        invariant(fi->n_files_open>0);   // loader-cleanup-test failure
        fi->n_files_open--;
        fi->file_infos[idx.idx].is_open = false;
        int r = toku_os_fclose(fi->file_infos[idx.idx].file);
        if (r)
            result = get_error_errno();
        cleanup_big_buffer(&fi->file_infos[idx.idx]);
    } else if (require_open)
        result = EINVAL;
    toku_mutex_unlock(&fi->lock); 
    return result;
}

int ft_loader_fi_unlink (struct file_infos *fi, FIDX idx) {
    int result = 0;
    toku_mutex_lock(&fi->lock);
    int id = idx.idx;
    invariant(id >=0 && id < fi->n_files);
    if (fi->file_infos[id].is_extant) { // must still exist
        invariant(fi->n_files_extant>0);
        fi->n_files_extant--;
        invariant(!fi->file_infos[id].is_open); // must be closed before we unlink
        fi->file_infos[id].is_extant = false;
        int r = unlink(fi->file_infos[id].fname);  
        if (r != 0) 
            result = get_error_errno();
        toku_free(fi->file_infos[id].fname);
        fi->file_infos[id].fname = NULL;
    } else
        result = EINVAL;
    toku_mutex_unlock(&fi->lock);
    return result;
}

int
ft_loader_fi_close_all(struct file_infos *fi) {
    int rval = 0;
    for (int i = 0; i < fi->n_files; i++) {
        int r;
        FIDX idx = { i };
        r = ft_loader_fi_close(fi, idx, false);  // ignore files that are already closed
        if (rval == 0 && r)
            rval = r;  // capture first error
    }
    return rval;
}

int ft_loader_open_temp_file (FTLOADER bl, FIDX *file_idx)
/* Effect: Open a temporary file in read-write mode.  Save enough information to close and delete the file later.
 * Return value: 0 on success, an error number otherwise.
 *  On error, *file_idx and *fnamep will be unmodified.
 *  The open file will be saved in bl->file_infos so that even if errors happen we can free them all.
 */
{
    int result = 0;
    if (result) // debug hack
        return result;
    FILE *f = NULL;
    int fd = -1;
    char *fname = toku_strdup(bl->temp_file_template);    
    if (fname == NULL)
        result = get_error_errno();
    else {
        fd = mkstemp(fname);
        if (fd < 0) { 
            result = get_error_errno();
        } else {
            f = toku_os_fdopen(fd, "r+");
            if (f == NULL)
                result = get_error_errno();
            else
                result = open_file_add(&bl->file_infos, f, fname, file_idx);
        }
    }
    if (result != 0) {
        if (fd >= 0) {
            toku_os_close(fd);
            unlink(fname);
        }
        if (f != NULL)
            toku_os_fclose(f);  // don't check for error because we're already in an error case
        if (fname != NULL)
            toku_free(fname);
    }
    return result;
}

void toku_ft_loader_internal_destroy (FTLOADER bl, bool is_error) {
    ft_loader_lock_destroy(bl);

    // These frees rely on the fact that if you free a NULL pointer then nothing bad happens.
    toku_free(bl->dbs);
    toku_free(bl->bt_compare_funs);
    toku_free((char*)bl->temp_file_template);
    ft_loader_fi_destroy(&bl->file_infos, is_error);

    for (int i = 0; i < bl->N; i++) 
        destroy_rowset(&bl->rows[i]);
    toku_free(bl->rows);

    for (int i = 0; i < bl->N; i++)
        destroy_merge_fileset(&bl->fs[i]);
    toku_free(bl->fs);

    destroy_rowset(&bl->primary_rowset);
    if (bl->primary_rowset_queue) {
        toku_queue_destroy(bl->primary_rowset_queue);
        bl->primary_rowset_queue = nullptr;
    }

    for (int i=0; i<bl->N; i++) {
        if ( bl->fractal_queues ) {
            invariant(bl->fractal_queues[i]==NULL);
        }
    }
    toku_free(bl->fractal_threads);
    toku_free(bl->fractal_queues);
    toku_free(bl->fractal_threads_live);

    if (bl->did_reserve_memory) {
        invariant(bl->cachetable);
        toku_cachetable_release_reserved_memory(bl->cachetable, bl->reserved_memory);
    }

    ft_loader_destroy_error_callback(&bl->error_callback);
    ft_loader_destroy_poll_callback(&bl->poll_callback);

    //printf("Progress=%d/%d\n", bl->progress, PROGRESS_MAX);

    toku_free(bl);
}

static void *extractor_thread (void*);

#define MAX(a,b) (((a)<(b)) ? (b) : (a))

static uint64_t memory_per_rowset_during_extract (FTLOADER bl)
// Return how much memory can be allocated for each rowset.
{
    if (size_factor==1) {
        return 16*1024;
    } else {
        // There is a primary rowset being maintained by the foreground thread.
        // There could be two more in the queue.
        // There is one rowset for each index (bl->N) being filled in.
        // Later we may have sort_and_write operations spawning in parallel, and will need to account for that.
        int n_copies = (1 // primary rowset
                        +EXTRACTOR_QUEUE_DEPTH  // the number of primaries in the queue
                        +bl->N // the N rowsets being constructed by the extractor thread.
                        +bl->N // the N sort buffers
                        +1     // Give the extractor thread one more so that it can have temporary space for sorting.  This is overkill.
                        );
        int64_t extra_reserved_memory = bl->N * FILE_BUFFER_SIZE;  // for each index we are writing to a file at any given time.
        int64_t tentative_rowset_size = ((int64_t)(bl->reserved_memory - extra_reserved_memory))/(n_copies);
        return MAX(tentative_rowset_size, (int64_t)MIN_ROWSET_MEMORY);
    }
}

static unsigned ft_loader_get_fractal_workers_count(FTLOADER bl) {
    unsigned w = 0;
    while (1) {
        ft_loader_lock(bl);
        w = bl->fractal_workers;
        ft_loader_unlock(bl);
        if (w != 0)
            break;
        toku_pthread_yield();  // maybe use a cond var instead
    }
    return w;
}

static void ft_loader_set_fractal_workers_count(FTLOADER bl) {
    ft_loader_lock(bl);
    if (bl->fractal_workers == 0)
        bl->fractal_workers = 1;
    ft_loader_unlock(bl);
}

// To compute a merge, we have a certain amount of memory to work with.
// We perform only one fanin at a time.
// If the fanout is F then we are using
//   F merges.  Each merge uses
//   DBUFIO_DEPTH buffers for double buffering.  Each buffer is of size at least MERGE_BUF_SIZE
// so the memory is
//   F*MERGE_BUF_SIZE*DBUFIO_DEPTH storage.
// We use some additional space to buffer the outputs. 
//  That's FILE_BUFFER_SIZE for writing to a merge file if we are writing to a mergefile.
//  And we have FRACTAL_WRITER_ROWSETS*MERGE_BUF_SIZE per queue
//  And if we are doing a fractal, each worker could have have a fractal tree that it's working on.
//
// DBUFIO_DEPTH*F*MERGE_BUF_SIZE + FRACTAL_WRITER_ROWSETS*MERGE_BUF_SIZE + WORKERS*NODESIZE*2 <= RESERVED_MEMORY

static int64_t memory_avail_during_merge(FTLOADER bl, bool is_fractal_node) {
    // avail memory = reserved memory - WORKERS*NODESIZE*2 for the last merge stage only
    int64_t avail_memory = bl->reserved_memory;
    if (is_fractal_node) {
        // reserve space for the fractal writer thread buffers
        avail_memory -= (int64_t)ft_loader_get_fractal_workers_count(bl) * (int64_t)default_loader_nodesize * 2; // compressed and uncompressed buffers
    }
    return avail_memory;
}

static int merge_fanin (FTLOADER bl, bool is_fractal_node) {
    // return number of temp files to read in this pass
    int64_t memory_avail = memory_avail_during_merge(bl, is_fractal_node);
    int64_t nbuffers = memory_avail / (int64_t)TARGET_MERGE_BUF_SIZE;
    if (is_fractal_node)
        nbuffers -= FRACTAL_WRITER_ROWSETS;
    return MAX(nbuffers / (int64_t)DBUFIO_DEPTH, (int)MIN_MERGE_FANIN);
}

static uint64_t memory_per_rowset_during_merge (FTLOADER bl, int merge_factor, bool is_fractal_node // if it is being sent to a q
                                                ) {
    int64_t memory_avail = memory_avail_during_merge(bl, is_fractal_node);
    int64_t nbuffers = DBUFIO_DEPTH * merge_factor;
    if (is_fractal_node)
        nbuffers += FRACTAL_WRITER_ROWSETS;
    return MAX(memory_avail / nbuffers, (int64_t)MIN_MERGE_BUF_SIZE);
}

int toku_ft_loader_internal_init (/* out */ FTLOADER *blp,
                                   ft_loader_write_func write_func,
                                   void* write_func_extra,
                                   CACHETABLE cachetable,
                                   generate_row_for_put_func g,
                                   DB *src_db,
                                   int N, DB* dbs[/*N*/],
                                   ft_compare_func bt_compare_functions[/*N*/],
                                   const char *temp_file_template,
                                   bool reserve_memory,
                                   uint64_t reserve_memory_size,
                                   bool compress_intermediates)
// Effect: Allocate and initialize a FTLOADER, but do not create the extractor thread.
{
    FTLOADER CALLOC(bl); // initialized to all zeros (hence CALLOC)
    if (!bl) return get_error_errno();
    bl->write_func = write_func;
    bl->write_func_extra = write_func_extra;
    bl->generate_row_for_put = g;
    bl->cachetable = cachetable;
    if (reserve_memory && bl->cachetable) {
        bl->did_reserve_memory = true;
        bl->reserved_memory = toku_cachetable_reserve_memory(bl->cachetable, 2.0/3.0, reserve_memory_size); // allocate 2/3 of the unreserved part (which is 3/4 of the memory to start with).
    }
    else {
        bl->did_reserve_memory = false;
        bl->reserved_memory = 512*1024*1024; // if no cache table use 512MB.
    }
    bl->compress_intermediates = compress_intermediates;
    bl->src_db = src_db;
    bl->N = N;
    
    ft_loader_init_error_callback(&bl->error_callback);
    ft_loader_init_poll_callback(&bl->poll_callback);

#define MY_CALLOC_N(n,v) CALLOC_N(n,v); if (!v) { int r = get_error_errno(); toku_ft_loader_internal_destroy(bl, true); return r; }
#define SET_TO_MY_STRDUP(lval, s) do { char *v = toku_strdup(s); if (!v) { int r = get_error_errno(); toku_ft_loader_internal_destroy(bl, true); return r; } lval = v; } while (0)

    MY_CALLOC_N(N, bl->dbs);
    for (int i=0; i<N; i++) if (dbs && dbs[i]) bl->dbs[i]=dbs[i];
    MY_CALLOC_N(N, bl->bt_compare_funs);
    for (int i=0; i<N; i++) bl->bt_compare_funs[i] = bt_compare_functions[i];

    MY_CALLOC_N(N, bl->fractal_queues);
    for (int i=0; i<N; i++) bl->fractal_queues[i]=NULL;
    MY_CALLOC_N(N, bl->fractal_threads);
    MY_CALLOC_N(N, bl->fractal_threads_live);
    for (int i=0; i<N; i++) bl->fractal_threads_live[i] = false;

    {
        int r = ft_loader_init_file_infos(&bl->file_infos); 
        if (r!=0) { toku_ft_loader_internal_destroy(bl, true); return r; }
    }

    SET_TO_MY_STRDUP(bl->temp_file_template, temp_file_template);

    bl->n_rows   = 0; 
    bl->progress = 0;
    bl->progress_callback_result = 0;

    MY_CALLOC_N(N, bl->rows);
    MY_CALLOC_N(N, bl->fs);
    for(int i=0;i<N;i++) {
        { 
            int r = init_rowset(&bl->rows[i], memory_per_rowset_during_extract(bl)); 
            if (r!=0) { toku_ft_loader_internal_destroy(bl, true); return r; } 
        }
        init_merge_fileset(&bl->fs[i]);
    }

    { 
        int r = init_rowset(&bl->primary_rowset, memory_per_rowset_during_extract(bl)); 
        if (r!=0) { toku_ft_loader_internal_destroy(bl, true); return r; }
    }
    {   int r = toku_queue_create(&bl->primary_rowset_queue, EXTRACTOR_QUEUE_DEPTH); 
        if (r!=0) { toku_ft_loader_internal_destroy(bl, true); return r; }
    }
    {
        ft_loader_lock_init(bl);
    }

    *blp = bl;

    return 0;
}

int toku_ft_loader_open (FTLOADER *blp, /* out */
                          ft_loader_write_func write_func,
                          void* write_func_extra,
                          CACHETABLE cachetable,
                          generate_row_for_put_func g,
                          DB *src_db,
                          int N, DB* dbs[/*N*/],
                          ft_compare_func bt_compare_functions[/*N*/],
                          const char *temp_file_template,
                          bool reserve_memory,
                          uint64_t reserve_memory_size,
                          bool compress_intermediates) {
// Effect: called by DB_ENV->create_loader to create an ft loader.
// Arguments:
//   blp                  Return a ft loader ("bulk loader") here.
//   g                    The function for generating a row
//   src_db               The source database.  Needed by g.  May be NULL if that's ok with g.
//   N                    The number of dbs to create.
//   dbs                  An array of open databases.  Used by g.  The data will be put in these database.
//   new_fnames           The file names (these strings are owned by the caller: we make a copy for our own purposes).
//   temp_file_template   A template suitable for mkstemp()
//   reserve_memory       Cause the loader to reserve memory for its use from the cache table.
//   compress_intermediates  Cause the loader to compress intermediate loader files.
// Return value: 0 on success, an error number otherwise.
    int result = 0;
    {
        int r = toku_ft_loader_internal_init(blp, write_func, write_func_extra,
                                              cachetable, g, src_db,
                                              N, dbs,
                                              bt_compare_functions,
                                              temp_file_template,
                                              reserve_memory,
                                              reserve_memory_size,
                                              compress_intermediates);
        if (r!=0) result = r;
    }
    if (result==0) {
        FTLOADER bl = *blp;
        int r = toku_pthread_create(&bl->extractor_thread, NULL, extractor_thread, (void*)bl); 
        if (r==0) {
            bl->extractor_live = true;
        } else  { 
            result = r;
            (void) toku_ft_loader_internal_destroy(bl, true);
        }
    }
    return result;
}

static void ft_loader_set_panic(FTLOADER bl, int error, bool callback, int which_db, DBT *key, DBT *val) {
    DB *db = nullptr;
    if (bl && bl->dbs && which_db >= 0 && which_db < bl->N) {
        db = bl->dbs[which_db];
    }
    int r = ft_loader_set_error(&bl->error_callback, error, db, which_db, key, val);
    if (r == 0 && callback)
        ft_loader_call_error_function(&bl->error_callback);
}

// One of the tests uses this.
FILE *toku_bl_fidx2file (FTLOADER bl, FIDX i) {
    toku_mutex_lock(&bl->file_infos.lock);
    invariant(i.idx >=0 && i.idx < bl->file_infos.n_files);
    invariant(bl->file_infos.file_infos[i.idx].is_open);
    FILE *result=bl->file_infos.file_infos[i.idx].file;
    toku_mutex_unlock(&bl->file_infos.lock);
    return result;
}

static int bl_finish_compressed_write(FILE *stream, struct wbuf *wb) {
    int r;
    char *compressed_buf = NULL;
    const size_t data_size = wb->ndone;
    invariant(data_size > 0);
    invariant(data_size <= MAX_UNCOMPRESSED_BUF);

    int n_sub_blocks = 0;
    int sub_block_size = 0;

    r = choose_sub_block_size(wb->ndone, max_sub_blocks, &sub_block_size, &n_sub_blocks);
    invariant(r==0);
    invariant(0 < n_sub_blocks && n_sub_blocks <= max_sub_blocks);
    invariant(sub_block_size > 0);

    struct sub_block sub_block[max_sub_blocks];
    // set the initial sub block size for all of the sub blocks
    for (int i = 0; i < n_sub_blocks; i++) {
        sub_block_init(&sub_block[i]);
    }
    set_all_sub_block_sizes(data_size, sub_block_size, n_sub_blocks, sub_block);

    size_t compressed_len = get_sum_compressed_size_bound(n_sub_blocks, sub_block, TOKU_DEFAULT_COMPRESSION_METHOD);
    const size_t sub_block_header_len = sub_block_header_size(n_sub_blocks);
    const size_t other_overhead = sizeof(uint32_t); //total_size
    const size_t header_len = sub_block_header_len + other_overhead;
    MALLOC_N(header_len + compressed_len, compressed_buf);
    if (compressed_buf == nullptr) {
        return ENOMEM;
    }

    // compress all of the sub blocks
    char *uncompressed_ptr = (char*)wb->buf;
    char *compressed_ptr = compressed_buf + header_len;
    compressed_len = compress_all_sub_blocks(n_sub_blocks, sub_block, uncompressed_ptr, compressed_ptr,
                                             get_num_cores(), get_ft_pool(), TOKU_DEFAULT_COMPRESSION_METHOD);

    //total_size does NOT include itself
    uint32_t total_size = compressed_len + sub_block_header_len;
    // serialize the sub block header
    uint32_t *ptr = (uint32_t *)(compressed_buf);
    *ptr++ = toku_htod32(total_size);
    *ptr++ = toku_htod32(n_sub_blocks);
    for (int i=0; i<n_sub_blocks; i++) {
        ptr[0] = toku_htod32(sub_block[i].compressed_size);
        ptr[1] = toku_htod32(sub_block[i].uncompressed_size);
        ptr[2] = toku_htod32(sub_block[i].xsum);
        ptr += 3;
    }
    // Mark as written
    wb->ndone = 0;

    size_t size_to_write = total_size + 4; // Includes writing total_size

    {
        size_t written = do_fwrite(compressed_buf, 1, size_to_write, stream);
        if (written!=size_to_write) {
            if (os_fwrite_fun)    // if using hook to induce artificial errors (for testing) ...
                r = get_maybe_error_errno();        // ... then there is no error in the stream, but there is one in errno
            else
                r = ferror(stream);
            invariant(r!=0);
            goto exit;
        }
    }
    r = 0;
exit:
    if (compressed_buf) {
        toku_free(compressed_buf);
    }
    return r;
}

static int bl_compressed_write(void *ptr, size_t nbytes, FILE *stream, struct wbuf *wb) {
    invariant(wb->size <= MAX_UNCOMPRESSED_BUF);
    size_t bytes_left = nbytes;
    char *buf = (char*)ptr;

    while (bytes_left > 0) {
        size_t bytes_to_copy = bytes_left;
        if (wb->ndone + bytes_to_copy > wb->size) {
            bytes_to_copy = wb->size - wb->ndone;
        }
        wbuf_nocrc_literal_bytes(wb, buf, bytes_to_copy);
        if (wb->ndone == wb->size) {
            //Compress, write to disk, and empty out wb
            int r = bl_finish_compressed_write(stream, wb);
            if (r != 0) {
                errno = r;
                return -1;
            }
            wb->ndone = 0;
        }
        bytes_left -= bytes_to_copy;
        buf += bytes_to_copy;
    }
    return 0;
}

static int bl_fwrite(void *ptr, size_t size, size_t nmemb, FILE *stream, struct wbuf *wb, FTLOADER bl)
/* Effect: this is a wrapper for fwrite that returns 0 on success, otherwise returns an error number.
 * Arguments:
 *   ptr    the data to be writen.
 *   size   the amount of data to be written.
 *   nmemb  the number of units of size to be written.
 *   stream write the data here.
 *   wb     where to write uncompressed data (if we're compressing) or ignore if NULL
 *   bl     passed so we can panic the ft_loader if something goes wrong (recording the error number).
 * Return value: 0 on success, an error number otherwise.
 */
{
    if (!bl->compress_intermediates || !wb) {
        size_t r = do_fwrite(ptr, size, nmemb, stream);
        if (r!=nmemb) {
            int e;
            if (os_fwrite_fun)    // if using hook to induce artificial errors (for testing) ...
                e = get_maybe_error_errno();        // ... then there is no error in the stream, but there is one in errno
            else
                e = ferror(stream);
            invariant(e!=0);
            return e;
        }
    } else {
        size_t num_bytes = size * nmemb;
        int r = bl_compressed_write(ptr, num_bytes, stream, wb);
        if (r != 0) {
            return r;
        }
    }
    return 0;
}

static int bl_fread (void *ptr, size_t size, size_t nmemb, FILE *stream)
/* Effect: this is a wrapper for fread that returns 0 on success, otherwise returns an error number.
 * Arguments:
 *  ptr      read data into here.
 *  size     size of data element to be read.
 *  nmemb    number of data elements to be read.
 *  stream   where to read the data from.
 * Return value: 0 on success, an error number otherwise.
 */
{
    size_t r = fread(ptr, size, nmemb, stream);
    if (r==0) {
        if (feof(stream)) return EOF;
        else {
        do_error: ;
            int e = ferror(stream);
            // r == 0 && !feof && e == 0, how does this happen? invariant(e!=0);
            return e;
        }
    } else if (r<nmemb) {
        goto do_error;
    } else {
        return 0;
    }
}

static int bl_write_dbt (DBT *dbt, FILE* datafile, uint64_t *dataoff, struct wbuf *wb, FTLOADER bl)
{
    int r;
    int dlen = dbt->size;
    if ((r=bl_fwrite(&dlen,     sizeof(dlen), 1,    datafile, wb, bl))) return r;
    if ((r=bl_fwrite(dbt->data, 1,            dlen, datafile, wb, bl))) return r;
    if (dataoff)
        *dataoff += dlen + sizeof(dlen);
    return 0;
}

static int bl_read_dbt (/*in*/DBT *dbt, FILE *stream)
{
    int len;
    {
        int r;
        if ((r = bl_fread(&len, sizeof(len), 1, stream))) return r;
        invariant(len>=0);
    }
    if ((int)dbt->ulen<len) { dbt->ulen=len; dbt->data=toku_xrealloc(dbt->data, len); }
    {
        int r;
        if ((r = bl_fread(dbt->data, 1, len, stream)))     return r;
    }
    dbt->size = len;
    return 0;
}

static int bl_read_dbt_from_dbufio (/*in*/DBT *dbt, DBUFIO_FILESET bfs, int filenum)
{
    int result = 0;
    uint32_t len;
    {
        size_t n_read;
        int r = dbufio_fileset_read(bfs, filenum, &len, sizeof(len), &n_read);
        if (r!=0) {
            result = r;
        } else if (n_read<sizeof(len)) {
            result = TOKUDB_NO_DATA; // must have run out of data prematurely.  This is not EOF, it's a real error.
        }
    }
    if (result==0) {
        if (dbt->ulen<len) {
            void * data = toku_realloc(dbt->data, len);
            if (data==NULL) {
                result = get_error_errno();
            } else {
                dbt->ulen=len;
                dbt->data=data;
            }
        }
    }
    if (result==0) {
        size_t n_read;
        int r = dbufio_fileset_read(bfs, filenum, dbt->data, len, &n_read);
        if (r!=0) {
            result = r;
        } else if (n_read<len) {
            result = TOKUDB_NO_DATA; // must have run out of data prematurely.  This is not EOF, it's a real error.
        } else {
            dbt->size = len;
        }
    }
    return result;
}


int loader_write_row(DBT *key, DBT *val, FIDX data, FILE *dataf, uint64_t *dataoff, struct wbuf *wb, FTLOADER bl)
/* Effect: Given a key and a val (both DBTs), write them to a file.  Increment *dataoff so that it's up to date.
 * Arguments:
 *   key, val   write these.
 *   data       the file to write them to
 *   dataoff    a pointer to a counter that keeps track of the amount of data written so far.
 *   wb         a pointer (possibly NULL) to buffer uncompressed output
 *   bl         the ft_loader (passed so we can panic if needed).
 * Return value: 0 on success, an error number otherwise.
 */
{
    //int klen = key->size;
    //int vlen = val->size;
    int r;
    // we have a chance to handle the errors because when we close we can delete all the files.
    if ((r=bl_write_dbt(key, dataf, dataoff, wb, bl))) return r;
    if ((r=bl_write_dbt(val, dataf, dataoff, wb, bl))) return r;
    toku_mutex_lock(&bl->file_infos.lock);
    bl->file_infos.file_infos[data.idx].n_rows++;
    toku_mutex_unlock(&bl->file_infos.lock);
    return 0;
}

int loader_read_row (FILE *f, DBT *key, DBT *val)
/* Effect: Read a key value pair from a file.  The DBTs must have DB_DBT_REALLOC set.
 * Arguments:
 *    f         where to read it from.
 *    key, val  read it into these.
 *    bl        passed so we can panic if needed.
 * Return value: 0 on success, an error number otherwise.
 * Requires:   The DBTs must have DB_DBT_REALLOC
 */
{
    {
        int r = bl_read_dbt(key, f);
        if (r!=0) return r;
    }
    {
        int r = bl_read_dbt(val, f);
        if (r!=0) return r;
    }
    return 0;
}

static int loader_read_row_from_dbufio (DBUFIO_FILESET bfs, int filenum, DBT *key, DBT *val)
/* Effect: Read a key value pair from a file.  The DBTs must have DB_DBT_REALLOC set.
 * Arguments:
 *    f         where to read it from.
 *    key, val  read it into these.
 *    bl        passed so we can panic if needed.
 * Return value: 0 on success, an error number otherwise.
 * Requires:   The DBTs must have DB_DBT_REALLOC
 */
{
    {
        int r = bl_read_dbt_from_dbufio(key, bfs, filenum);
        if (r!=0) return r;
    }
    {
        int r = bl_read_dbt_from_dbufio(val, bfs, filenum);
        if (r!=0) return r;
    }
    return 0;
}


int init_rowset (struct rowset *rows, uint64_t memory_budget) 
/* Effect: Initialize a collection of rows to be empty. */
{
    int result = 0;

    rows->memory_budget = memory_budget;

    rows->rows = NULL;
    rows->data = NULL;

    rows->n_rows = 0;
    rows->n_rows_limit = 100;
    MALLOC_N(rows->n_rows_limit, rows->rows);
    if (rows->rows == NULL)
        result = get_error_errno();
    rows->n_bytes = 0;
    rows->n_bytes_limit = (size_factor==1) ? 1024*size_factor*16 : memory_budget;
    //printf("%s:%d n_bytes_limit=%ld (size_factor based limit=%d)\n", __FILE__, __LINE__, rows->n_bytes_limit, 1024*size_factor*16);
    rows->data = (char *) toku_malloc(rows->n_bytes_limit);
    if (rows->rows==NULL || rows->data==NULL) {
        if (result == 0)
            result = get_error_errno();
        toku_free(rows->rows);
        toku_free(rows->data);
        rows->rows = NULL;
        rows->data = NULL;
    }
    return result;
}

static void zero_rowset (struct rowset *rows) {
    memset(rows, 0, sizeof(*rows));
}

void destroy_rowset (struct rowset *rows) {
    if ( rows ) {
        toku_free(rows->data);
        toku_free(rows->rows);
        zero_rowset(rows);
    }
}

static int row_wont_fit (struct rowset *rows, size_t size)
/* Effect: Return nonzero if adding a row of size SIZE would be too big (bigger than the buffer limit) */ 
{
    // Account for the memory used by the data and also the row structures.
    size_t memory_in_use = (rows->n_rows*sizeof(struct row)
                            + rows->n_bytes);
    return (rows->memory_budget <  memory_in_use + size);
}

int add_row (struct rowset *rows, DBT *key, DBT *val)
/* Effect: add a row to a collection. */
{
    int result = 0;
    if (rows->n_rows >= rows->n_rows_limit) {
        struct row *old_rows = rows->rows;
        size_t old_n_rows_limit = rows->n_rows_limit;
        rows->n_rows_limit *= 2;
        REALLOC_N(rows->n_rows_limit, rows->rows);
        if (rows->rows == NULL) {
            result = get_error_errno();
            rows->rows = old_rows;
            rows->n_rows_limit = old_n_rows_limit;
            return result;
        }
    }
    size_t off      = rows->n_bytes;
    size_t next_off = off + key->size + val->size;

    struct row newrow; 
    memset(&newrow, 0, sizeof newrow); newrow.off = off; newrow.klen = key->size; newrow.vlen = val->size;

    rows->rows[rows->n_rows++] = newrow;
    if (next_off > rows->n_bytes_limit) {
        size_t old_n_bytes_limit = rows->n_bytes_limit;
        while (next_off > rows->n_bytes_limit) {
            rows->n_bytes_limit = rows->n_bytes_limit*2; 
        }
        invariant(next_off <= rows->n_bytes_limit);
        char *old_data = rows->data;
        REALLOC_N(rows->n_bytes_limit, rows->data);
        if (rows->data == NULL) {
            result = get_error_errno();
            rows->data = old_data;
            rows->n_bytes_limit = old_n_bytes_limit;
            return result;
        }
    }
    memcpy(rows->data+off,           key->data, key->size);
    memcpy(rows->data+off+key->size, val->data, val->size);
    rows->n_bytes = next_off;
    return result;
}

static int process_primary_rows (FTLOADER bl, struct rowset *primary_rowset);

static int finish_primary_rows_internal (FTLOADER bl)
// now we have been asked to finish up.
// Be sure to destroy the rowsets.
{
    int *MALLOC_N(bl->N, ra);
    if (ra==NULL) return get_error_errno();

    for (int i = 0; i < bl->N; i++) {
        //printf("%s:%d extractor finishing index %d with %ld rows\n", __FILE__, __LINE__, i, rows->n_rows);
        ra[i] = sort_and_write_rows(bl->rows[i], &(bl->fs[i]), bl, i, bl->dbs[i], bl->bt_compare_funs[i]);
        zero_rowset(&bl->rows[i]);
    }

    // accept any of the error codes (in this case, the last one).
    int r = 0;
    for (int i = 0; i < bl->N; i++)
        if (ra[i] != 0)
            r = ra[i];

    toku_free(ra);
    return r;
}

static int finish_primary_rows (FTLOADER bl) {
    return           finish_primary_rows_internal (bl);
}

static void* extractor_thread (void *blv) {
    FTLOADER bl = (FTLOADER)blv;
    int r = 0;
    while (1) {
        void *item;
        {
            int rq = toku_queue_deq(bl->primary_rowset_queue, &item, NULL, NULL);
            if (rq==EOF) break;
            invariant(rq==0); // other errors are arbitrarily bad.
        }
        struct rowset *primary_rowset = (struct rowset *)item;

        //printf("%s:%d extractor got %ld rows\n", __FILE__, __LINE__, primary_rowset.n_rows);

        // Now we have some rows to output
        {
            r = process_primary_rows(bl, primary_rowset);
            if (r)
                ft_loader_set_panic(bl, r, false, 0, nullptr, nullptr);
        }
    }

    //printf("%s:%d extractor finishing\n", __FILE__, __LINE__);
    if (r == 0) {
        r = finish_primary_rows(bl); 
        if (r) 
            ft_loader_set_panic(bl, r, false, 0, nullptr, nullptr);
        
    }
    return NULL;
}

static void enqueue_for_extraction (FTLOADER bl) {
    //printf("%s:%d enqueing %ld items\n", __FILE__, __LINE__, bl->primary_rowset.n_rows);
    struct rowset *XMALLOC(enqueue_me);
    *enqueue_me = bl->primary_rowset;
    zero_rowset(&bl->primary_rowset);
    int r = toku_queue_enq(bl->primary_rowset_queue, (void*)enqueue_me, 1, NULL);
    resource_assert_zero(r); 
}

static int loader_do_put(FTLOADER bl,
                         DBT *pkey,
                         DBT *pval)
{
    int result;
    result = add_row(&bl->primary_rowset, pkey, pval);
    if (result == 0 && row_wont_fit(&bl->primary_rowset, 0)) {
        // queue the rows for further processing by the extractor thread.
        //printf("%s:%d please extract %ld\n", __FILE__, __LINE__, bl->primary_rowset.n_rows);
        enqueue_for_extraction(bl);
        {
            int r = init_rowset(&bl->primary_rowset, memory_per_rowset_during_extract(bl)); 
            // bl->primary_rowset will get destroyed by toku_ft_loader_abort
            if (r != 0) 
                result = r;
        }
    }
    return result;
}

static int 
finish_extractor (FTLOADER bl) {
    //printf("%s:%d now finishing extraction\n", __FILE__, __LINE__);

    int rval;

    if (bl->primary_rowset.n_rows>0) {
        enqueue_for_extraction(bl);
    } else {
        destroy_rowset(&bl->primary_rowset);
    }
    //printf("%s:%d please finish extraction\n", __FILE__, __LINE__);
    {
        int r = toku_queue_eof(bl->primary_rowset_queue);
        invariant(r==0);
    }
    //printf("%s:%d joining\n", __FILE__, __LINE__);
    {
        void *toku_pthread_retval;
        int r = toku_pthread_join(bl->extractor_thread, &toku_pthread_retval);
        resource_assert_zero(r); 
        invariant(toku_pthread_retval == NULL);
        bl->extractor_live = false;
    }
    {
        int r = toku_queue_destroy(bl->primary_rowset_queue);
        invariant(r==0);
        bl->primary_rowset_queue = nullptr;
    }

    rval = ft_loader_fi_close_all(&bl->file_infos);

   //printf("%s:%d joined\n", __FILE__, __LINE__);
    return rval;
}

static const DBT zero_dbt = {0,0,0,0};

static DBT make_dbt (void *data, uint32_t size) {
    DBT result = zero_dbt;
    result.data = data;
    result.size = size;
    return result;
}

#define inc_error_count() error_count++

static int process_primary_rows_internal (FTLOADER bl, struct rowset *primary_rowset)
// process the rows in primary_rowset, and then destroy the rowset.
// if FLUSH is true then write all the buffered rows out.
// if primary_rowset is NULL then treat it as empty.
{
    int error_count = 0;
    int *XMALLOC_N(bl->N, error_codes);

    // If we parallelize the first for loop, dest_keys/dest_vals init&cleanup need to move inside
    DBT_ARRAY dest_keys;
    DBT_ARRAY dest_vals;
    toku_dbt_array_init(&dest_keys, 1);
    toku_dbt_array_init(&dest_vals, 1);

    for (int i = 0; i < bl->N; i++) {
        unsigned int klimit,vlimit; // maximum row sizes.
        toku_ft_get_maximum_advised_key_value_lengths(&klimit, &vlimit);

        error_codes[i] = 0;
        struct rowset *rows = &(bl->rows[i]);
        struct merge_fileset *fs = &(bl->fs[i]);
        ft_compare_func compare = bl->bt_compare_funs[i];

        // Don't parallelize this loop, or we have to lock access to add_row() which would be a lot of overehad.
        // Also this way we can reuse the DB_DBT_REALLOC'd values inside dest_keys/dest_vals without a race.
        for (size_t prownum=0; prownum<primary_rowset->n_rows; prownum++) {
            if (error_count) break;

            struct row *prow = &primary_rowset->rows[prownum];
            DBT pkey = zero_dbt;
            DBT pval = zero_dbt;
            pkey.data = primary_rowset->data + prow->off;
            pkey.size = prow->klen;
            pval.data = primary_rowset->data + prow->off + prow->klen;
            pval.size = prow->vlen;


            DBT_ARRAY key_array;
            DBT_ARRAY val_array;
            if (bl->dbs[i] != bl->src_db) {
                int r = bl->generate_row_for_put(bl->dbs[i], bl->src_db, &dest_keys, &dest_vals, &pkey, &pval);
                if (r != 0) {
                    error_codes[i] = r;
                    inc_error_count();
                    break;
                }
                paranoid_invariant(dest_keys.size <= dest_keys.capacity);
                paranoid_invariant(dest_vals.size <= dest_vals.capacity);
                paranoid_invariant(dest_keys.size == dest_vals.size);

                key_array = dest_keys;
                val_array = dest_vals;
            } else {
                key_array.size = key_array.capacity = 1;
                key_array.dbts = &pkey;

                val_array.size = val_array.capacity = 1;
                val_array.dbts = &pval;
            }
            for (uint32_t row = 0; row < key_array.size; row++) {
                DBT *dest_key = &key_array.dbts[row];
                DBT *dest_val = &val_array.dbts[row];
                if (dest_key->size > klimit) {
                    error_codes[i] = EINVAL;
                    fprintf(stderr, "Key too big (keysize=%d bytes, limit=%d bytes)\n", dest_key->size, klimit);
                    inc_error_count();
                    break;
                }
                if (dest_val->size > vlimit) {
                    error_codes[i] = EINVAL;
                    fprintf(stderr, "Row too big (rowsize=%d bytes, limit=%d bytes)\n", dest_val->size, vlimit);
                    inc_error_count();
                    break;
                }

                if (row_wont_fit(rows, dest_key->size + dest_val->size)) {
                    //printf("%s:%d rows.n_rows=%ld rows.n_bytes=%ld\n", __FILE__, __LINE__, rows->n_rows, rows->n_bytes);
                    int r = sort_and_write_rows(*rows, fs, bl, i, bl->dbs[i], compare); // cannot spawn this because of the race on rows.  If we were to create a new rows, and if sort_and_write_rows were to destroy the rows it is passed, we could spawn it, however.
                    // If we do spawn this, then we must account for the additional storage in the memory_per_rowset() function.
                    init_rowset(rows, memory_per_rowset_during_extract(bl)); // we passed the contents of rows to sort_and_write_rows.
                    if (r != 0) {
                        error_codes[i] = r;
                        inc_error_count();
                        break;
                    }
                }
                int r = add_row(rows, dest_key, dest_val);
                if (r != 0) {
                    error_codes[i] = r;
                    inc_error_count();
                    break;
                }
            }
        }
    }
    toku_dbt_array_destroy(&dest_keys);
    toku_dbt_array_destroy(&dest_vals);
    
    destroy_rowset(primary_rowset);
    toku_free(primary_rowset);
    int r = 0;
    if (error_count > 0) {
        for (int i=0; i<bl->N; i++) {
            if (error_codes[i]) {
                r = error_codes[i];
                ft_loader_set_panic(bl, r, false, i, nullptr, nullptr);
            }
        }
        invariant(r); // found the error 
    }
    toku_free(error_codes);
    return r;
}

static int process_primary_rows (FTLOADER bl, struct rowset *primary_rowset) {
    int r = process_primary_rows_internal (bl, primary_rowset);
    return r;
}
 
int toku_ft_loader_put (FTLOADER bl, DBT *key, DBT *val)
/* Effect: Put a key-value pair into the ft loader.  Called by DB_LOADER->put().
 * Return value: 0 on success, an error number otherwise.
 */
{
    if (ft_loader_get_error(&bl->error_callback)) 
        return EINVAL; // previous panic
    bl->n_rows++;
    return loader_do_put(bl, key, val);
}

void toku_ft_loader_set_n_rows(FTLOADER bl, uint64_t n_rows) {
    bl->n_rows = n_rows;
}

uint64_t toku_ft_loader_get_n_rows(FTLOADER bl) {
    return bl->n_rows;
}

int merge_row_arrays_base (struct row dest[/*an+bn*/], struct row a[/*an*/], int an, struct row b[/*bn*/], int bn,
                           int which_db, DB *dest_db, ft_compare_func compare,
                           
                           FTLOADER bl,
                           struct rowset *rowset)
/* Effect: Given two arrays of rows, a and b, merge them using the comparison function, and write them into dest.
 *   This function is suitable for use in a mergesort.
 *   If a pair of duplicate keys is ever noticed, then call the error_callback function (if it exists), and return DB_KEYEXIST.
 * Arguments:
 *   dest    write the rows here
 *   a,b     the rows being merged
 *   an,bn   the lenth of a and b respectively.
 *   dest_db We need the dest_db to run the comparison function.
 *   compare We need the compare function for the dest_db.
 */
{
    while (an>0 && bn>0) {
        DBT akey; memset(&akey, 0, sizeof akey); akey.data=rowset->data+a->off; akey.size=a->klen;
        DBT bkey; memset(&bkey, 0, sizeof bkey); bkey.data=rowset->data+b->off; bkey.size=b->klen;

        int compare_result = compare(&akey, &bkey);
        if (compare_result==0) {
            if (bl->error_callback.error_callback) {
                DBT aval; memset(&aval, 0, sizeof aval); aval.data=rowset->data + a->off + a->klen; aval.size = a->vlen;
                ft_loader_set_error(&bl->error_callback, DB_KEYEXIST, dest_db, which_db, &akey, &aval);
            }
            return DB_KEYEXIST;
        } else if (compare_result<0) {
            // a is smaller
            *dest = *a;
            dest++; a++; an--;
        } else {
            *dest = *b;
            dest++; b++; bn--;
        }
    }
    while (an>0) {
        *dest = *a;
        dest++; a++; an--;
    }
    while (bn>0) {
        *dest = *b;
        dest++; b++; bn--;
    }
    return 0;
}

static int binary_search (int *location,
                          const DBT *key,
                          struct row a[/*an*/], int an,
                          int abefore,
                          int which_db, DB *dest_db, ft_compare_func compare,
                          FTLOADER bl,
                          struct rowset *rowset)
// Given a sorted array of rows a, and a dbt key, find the first row in a that is > key.
// If no such row exists, then consider the result to be equal to an.
// On success store abefore+the index into *location
// Return 0 on success.
// Return DB_KEYEXIST if we find a row that is equal to key.
{
    if (an==0) {
        *location = abefore;
        return 0;
    } else {
        int a2 = an/2;
        DBT akey = make_dbt(rowset->data+a[a2].off,  a[a2].klen);
        int compare_result = compare(key, &akey);
        if (compare_result==0) {
            if (bl->error_callback.error_callback) {
                DBT aval = make_dbt(rowset->data + a[a2].off + a[a2].klen,  a[a2].vlen);
                ft_loader_set_error(&bl->error_callback, DB_KEYEXIST, dest_db, which_db, &akey, &aval);
            }
            return DB_KEYEXIST;
        } else if (compare_result<0) {
            // key is before a2
            if (an==1) {
                *location = abefore;
                return 0;
            } else {
                return binary_search(location, key,
                                     a,    a2,
                                     abefore,
                                     which_db, dest_db, compare, bl, rowset);
            }
        } else {
            // key is after a2
            if (an==1) {
                *location = abefore + 1;
                return 0;
            } else {
                return binary_search(location, key,
                                     a+a2, an-a2,
                                     abefore+a2,
                                     which_db, dest_db, compare, bl, rowset);
            }
        }
    }
}
                   

#define SWAP(typ,x,y) { typ tmp = x; x=y; y=tmp; }

static int merge_row_arrays (struct row dest[/*an+bn*/], struct row a[/*an*/], int an, struct row b[/*bn*/], int bn,
                             int which_db, DB *dest_db, ft_compare_func compare,
                             FTLOADER bl,
                             struct rowset *rowset)
/* Effect: Given two sorted arrays of rows, a and b, merge them using the comparison function, and write them into dest.
 * Arguments:
 *   dest    write the rows here
 *   a,b     the rows being merged
 *   an,bn   the lenth of a and b respectively.
 *   dest_db We need the dest_db to run the comparison function.
 *   compare We need the compare function for the dest_db.
 */
{
    if (an + bn < 10000) {
        return merge_row_arrays_base(dest, a, an, b, bn, which_db, dest_db, compare, bl, rowset);
    }
    if (an < bn) {
        SWAP(struct row *,a, b)
        SWAP(int         ,an,bn)
    }
    // an >= bn
    int a2 = an/2;
    DBT akey = make_dbt(rowset->data+a[a2].off, a[a2].klen);
    int b2 = 0; // initialize to zero so we can add the answer in.
    {
        int r = binary_search(&b2, &akey, b, bn, 0, which_db, dest_db, compare, bl, rowset);
        if (r!=0) return r; // for example if we found a duplicate, called the error_callback, and now we return an error code.
    }
    int ra, rb;
    ra = merge_row_arrays(dest,       a,    a2,    b,    b2,    which_db, dest_db, compare, bl, rowset);
    rb = merge_row_arrays(dest+a2+b2, a+a2, an-a2, b+b2, bn-b2, which_db, dest_db, compare, bl, rowset);
    if (ra!=0) return ra;
    else       return rb;
}

int mergesort_row_array (struct row rows[/*n*/], int n, int which_db, DB *dest_db, ft_compare_func compare, FTLOADER bl, struct rowset *rowset)
/* Sort an array of rows (using mergesort).
 * Arguments:
 *   rows   sort this array of rows.
 *   n      the length of the array.
 *   dest_db  used by the comparison function.
 *   compare  the compare function
 */
{
    if (n<=1) return 0; // base case is sorted
    int mid = n/2;
    int r1, r2;
    r1 = mergesort_row_array (rows,     mid,   which_db, dest_db, compare, bl, rowset);

    // Don't spawn this one explicitly
    r2 =            mergesort_row_array (rows+mid, n-mid, which_db, dest_db, compare, bl, rowset);

    if (r1!=0) return r1;
    if (r2!=0) return r2;

    struct row *MALLOC_N(n, tmp); 
    if (tmp == NULL) return get_error_errno();
    {
        int r = merge_row_arrays(tmp, rows, mid, rows+mid, n-mid, which_db, dest_db, compare, bl, rowset);
        if (r!=0) {
            toku_free(tmp);
            return r;
        }
    }
    memcpy(rows, tmp, sizeof(*tmp)*n);
    toku_free(tmp);
    return 0;
}

// C function for testing mergesort_row_array 
int ft_loader_mergesort_row_array (struct row rows[/*n*/], int n, int which_db, DB *dest_db, ft_compare_func compare, FTLOADER bl, struct rowset *rowset) {
    return mergesort_row_array (rows, n, which_db, dest_db, compare, bl, rowset);
}

static int sort_rows (struct rowset *rows, int which_db, DB *dest_db, ft_compare_func compare,
                      FTLOADER bl)
/* Effect: Sort a collection of rows.
 * If any duplicates are found, then call the error_callback function and return non zero.
 * Otherwise return 0.
 * Arguments:
 *   rowset    the */
{
    return mergesort_row_array(rows->rows, rows->n_rows, which_db, dest_db, compare, bl, rows);
}

/* filesets Maintain a collection of files.  Typically these files are each individually sorted, and we will merge them.
 * These files have two parts, one is for the data rows, and the other is a collection of offsets so we an more easily parallelize the manipulation (e.g., by allowing us to find the offset of the ith row quickly). */

void init_merge_fileset (struct merge_fileset *fs)
/* Effect: Initialize a fileset */ 
{
    fs->have_sorted_output = false;
    fs->sorted_output      = FIDX_NULL;
    fs->prev_key           = zero_dbt;
    fs->prev_key.flags     = DB_DBT_REALLOC;

    fs->n_temp_files = 0;
    fs->n_temp_files_limit = 0;
    fs->data_fidxs = NULL;
}

void destroy_merge_fileset (struct merge_fileset *fs)
/* Effect: Destroy a fileset. */
{
    if ( fs ) {
        toku_destroy_dbt(&fs->prev_key);
        fs->n_temp_files = 0;
        fs->n_temp_files_limit = 0;
        toku_free(fs->data_fidxs);
        fs->data_fidxs = NULL;
    }
}


static int extend_fileset (FTLOADER bl, struct merge_fileset *fs, FIDX*ffile)
/* Effect: Add two files (one for data and one for idx) to the fileset.
 * Arguments:
 *   bl   the ft_loader (needed to panic if anything goes wrong, and also to get the temp_file_template.
 *   fs   the fileset
 *   ffile  the data file (which will be open)
 *   fidx   the index file (which will be open)
 */
{
    FIDX sfile;
    int r;
    r = ft_loader_open_temp_file(bl, &sfile); if (r!=0) return r;

    if (fs->n_temp_files+1 > fs->n_temp_files_limit) {
        fs->n_temp_files_limit = (fs->n_temp_files+1)*2;
        XREALLOC_N(fs->n_temp_files_limit, fs->data_fidxs);
    }
    fs->data_fidxs[fs->n_temp_files] = sfile;
    fs->n_temp_files++;

    *ffile = sfile;
    return 0;
}

// RFP maybe this should be buried in the ft_loader struct
static toku_mutex_t update_progress_lock = TOKU_MUTEX_INITIALIZER;

static int update_progress (int N,
                            FTLOADER bl,
                            const char *UU(message))
{
    // Must protect the increment and the call to the poll_function.
    toku_mutex_lock(&update_progress_lock);
//printf("updateing with %d, %d\n", N, x);
    bl->progress+=N;

    int result;
    if (bl->progress_callback_result == 0) {
        //printf(" %20s: %d ", message, bl->progress);
        result = ft_loader_call_poll_function(&bl->poll_callback, (float)bl->progress/(float)PROGRESS_MAX);
        if (result!=0) {
            bl->progress_callback_result = result;
        }
    } else {
        result = bl->progress_callback_result;
    }
    toku_mutex_unlock(&update_progress_lock);
    return result;
}


static int write_rowset_to_file (FTLOADER bl, FIDX sfile, const struct rowset rows) {
    int r = 0;
    // Allocate a buffer if we're compressing intermediates.
    char *uncompressed_buffer = nullptr;
    if (bl->compress_intermediates) {
        MALLOC_N(MAX_UNCOMPRESSED_BUF, uncompressed_buffer);
        if (uncompressed_buffer == nullptr) {
            return ENOMEM;
        }
    }
    struct wbuf wb;
    wbuf_init(&wb, uncompressed_buffer, MAX_UNCOMPRESSED_BUF);

    FILE *sstream = toku_bl_fidx2file(bl, sfile);
    for (size_t i=0; i<rows.n_rows; i++) {
        DBT skey = make_dbt(rows.data + rows.rows[i].off,                     rows.rows[i].klen);
        DBT sval = make_dbt(rows.data + rows.rows[i].off + rows.rows[i].klen, rows.rows[i].vlen);
        
        uint64_t soffset=0; // don't really need this.
        r = loader_write_row(&skey, &sval, sfile, sstream, &soffset, &wb, bl);
        if (r != 0) {
            goto exit;
        }
    }

    if (bl->compress_intermediates && wb.ndone > 0) {
        r = bl_finish_compressed_write(sstream, &wb);
        if (r != 0) {
            goto exit;
        }
    }
    r = 0;
exit:
    if (uncompressed_buffer) {
        toku_free(uncompressed_buffer);
    }
    return r;
}


int sort_and_write_rows (struct rowset rows, struct merge_fileset *fs, FTLOADER bl, int which_db, DB *dest_db, ft_compare_func compare)
/* Effect: Given a rowset, sort it and write it to a temporary file.
 * Note:  The loader maintains for each index the most recently written-to file, as well as the DBT for the last key written into that file.
 *   If this rowset is sorted and all greater than that dbt, then we append to the file (skipping the sort, and reducing the number of temporary files).
 * Arguments:
 *   rows    the rowset
 *   fs      the fileset into which the sorted data will be added
 *   bl      the ft_loader
 *   dest_db the DB, needed for the comparison function.
 *   compare The comparison function.
 * Returns 0 on success, otherwise an error number.
 * Destroy the rowset after finishing it.
 * Note: There is no sense in trying to calculate progress by this function since it's done concurrently with the loader->put operation.
 * Note first time called: invariant: fs->have_sorted_output == false
 */
{
    //printf(" sort_and_write use %d progress=%d fin at %d\n", progress_allocation, bl->progress, bl->progress+progress_allocation);

    // TODO: erase the files, and deal with all the cleanup on error paths
    //printf("%s:%d sort_rows n_rows=%ld\n", __FILE__, __LINE__, rows->n_rows);
    //bl_time_t before_sort = bl_time_now();

    int result;
    if (rows.n_rows == 0) {
        result = 0;
    } else {
        result = sort_rows(&rows, which_db, dest_db, compare, bl);

        //bl_time_t after_sort = bl_time_now();

        if (result == 0) {
            DBT min_rowset_key = make_dbt(rows.data+rows.rows[0].off, rows.rows[0].klen);
            if (fs->have_sorted_output && compare(&fs->prev_key, &min_rowset_key) < 0) {
                // write everything to the same output if the max key in the temp file (prev_key) is < min of the sorted rowset
                result = write_rowset_to_file(bl, fs->sorted_output, rows);
                if (result == 0) {
                    // set the max key in the temp file to the max key in the sorted rowset
                    result = toku_dbt_set(rows.rows[rows.n_rows-1].klen, rows.data + rows.rows[rows.n_rows-1].off, &fs->prev_key, NULL);
                }
            } else {
                // write the sorted rowset into a new temp file
                if (fs->have_sorted_output) {
                    fs->have_sorted_output = false;
                    result = ft_loader_fi_close(&bl->file_infos, fs->sorted_output, true);
                }
                if (result == 0) {
                    FIDX sfile = FIDX_NULL;
                    result = extend_fileset(bl, fs, &sfile);
                    if (result == 0) {
                        result = write_rowset_to_file(bl, sfile, rows);
                        if (result == 0) {
                            fs->have_sorted_output = true; fs->sorted_output = sfile;
                            // set the max key in the temp file to the max key in the sorted rowset
                            result = toku_dbt_set(rows.rows[rows.n_rows-1].klen, rows.data + rows.rows[rows.n_rows-1].off, &fs->prev_key, NULL);
                        }
                    }
                }
                // Note: if result == 0 then invariant fs->have_sorted_output == true
            }
        }
    }

    destroy_rowset(&rows);

    //bl_time_t after_write = bl_time_now();
    
    return result;
}

// C function for testing sort_and_write_rows
int ft_loader_sort_and_write_rows (struct rowset *rows, struct merge_fileset *fs, FTLOADER bl, int which_db, DB *dest_db, ft_compare_func compare) {
    return sort_and_write_rows (*rows, fs, bl, which_db, dest_db, compare);
}

int toku_merge_some_files_using_dbufio (const bool to_q, FIDX dest_data, QUEUE q, int n_sources, DBUFIO_FILESET bfs, FIDX srcs_fidxs[/*n_sources*/], FTLOADER bl, int which_db, DB *dest_db, ft_compare_func compare, int progress_allocation)
/* Effect: Given an array of FILE*'s each containing sorted, merge the data and write it to an output.  All the files remain open after the merge.
 *   This merge is performed in one pass, so don't pass too many files in.  If you need a tree of merges do it elsewhere.
 *   If TO_Q is true then we write rowsets into queue Q.  Otherwise we write into dest_data.
 * Modifies:  May modify the arrays of files (but if modified, it must be a permutation so the caller can use that array to close everything.)
 * Requires: The number of sources is at least one, and each of the input files must have at least one row in it.
 * Arguments:
 *   to_q         boolean indicating that output is queue (true) or a file (false)
 *   dest_data    where to write the sorted data
 *   q            where to write the sorted data
 *   n_sources    how many source files.
 *   srcs_data    the array of source data files.
 *   bl           the ft_loader.
 *   dest_db      the destination DB (used in the comparison function).
 * Return value: 0 on success, otherwise an error number.
 * The fidxs are not closed by this function.
 */
{
    int result = 0;

    FILE *dest_stream = to_q ? NULL : toku_bl_fidx2file(bl, dest_data);

    //printf(" merge_some_files progress=%d fin at %d\n", bl->progress, bl->progress+progress_allocation);
    DBT keys[n_sources];
    DBT vals[n_sources];
    uint64_t dataoff[n_sources];
    DBT zero = zero_dbt;  zero.flags=DB_DBT_REALLOC;

    for (int i=0; i<n_sources; i++) {
        keys[i] = vals[i] = zero; // fill these all in with zero so we can delete stuff more reliably.
    }

    pqueue_t      *pq = NULL;
    pqueue_node_t *MALLOC_N(n_sources, pq_nodes); // freed in cleanup
    if (pq_nodes == NULL) { result = get_error_errno(); }

    if (result==0) {
        int r = pqueue_init(&pq, n_sources, which_db, dest_db, compare, &bl->error_callback);
        if (r!=0) result = r; 
    }

    uint64_t n_rows = 0;
    if (result==0) {
        // load pqueue with first value from each source
        for (int i=0; i<n_sources; i++) {
            int r = loader_read_row_from_dbufio(bfs, i, &keys[i], &vals[i]);
            if (r==EOF) continue; // if the file is empty, don't initialize the pqueue.
            if (r!=0) {
                result = r;
                break;
            }

            pq_nodes[i].key = &keys[i];
            pq_nodes[i].val = &vals[i];
            pq_nodes[i].i   = i;
            r = pqueue_insert(pq, &pq_nodes[i]);
            if (r!=0) {
                result = r;
                // path tested by loader-dup-test5.tdbrun
                // printf("%s:%d returning\n", __FILE__, __LINE__);
                break;
            }

            dataoff[i] = 0;
            toku_mutex_lock(&bl->file_infos.lock);
            n_rows += bl->file_infos.file_infos[srcs_fidxs[i].idx].n_rows;
            toku_mutex_unlock(&bl->file_infos.lock);
        }
    }
    uint64_t n_rows_done = 0;

    struct rowset *output_rowset = NULL;
    if (result==0 && to_q) {
        XMALLOC(output_rowset); // freed in cleanup
        int r = init_rowset(output_rowset, memory_per_rowset_during_merge(bl, n_sources, to_q));
        if (r!=0) result = r;
    }

    // Allocate a buffer if we're compressing intermediates.
    char *uncompressed_buffer = nullptr;
    struct wbuf wb;
    if (bl->compress_intermediates && !to_q) {
        MALLOC_N(MAX_UNCOMPRESSED_BUF, uncompressed_buffer);
        if (uncompressed_buffer == nullptr) {
            result = ENOMEM;
        }
    }
    wbuf_init(&wb, uncompressed_buffer, MAX_UNCOMPRESSED_BUF);
    
    //printf(" n_rows=%ld\n", n_rows);
    while (result==0 && pqueue_size(pq)>0) {
        int mini;
        {
            // get the minimum 
            pqueue_node_t *node;
            int r = pqueue_pop(pq, &node);
            if (r!=0) {
                result = r;
                invariant(0);
                break;
            }
            mini = node->i;
        }
        if (to_q) {
            if (row_wont_fit(output_rowset, keys[mini].size + vals[mini].size)) {
                {
                    int r = toku_queue_enq(q, (void*)output_rowset, 1, NULL);
                    if (r!=0) {
                        result = r;
                        break;
                    }
                }
                XMALLOC(output_rowset); // freed in cleanup
                {
                    int r = init_rowset(output_rowset, memory_per_rowset_during_merge(bl, n_sources, to_q));
                    if (r!=0) {        
                        result = r;
                        break;
                    }
                }
            }
            {
                int r = add_row(output_rowset, &keys[mini], &vals[mini]);
                if (r!=0) {
                    result = r;
                    break;
                }
            }
        } else {
            // write it to the dest file
            int r = loader_write_row(&keys[mini], &vals[mini], dest_data, dest_stream, &dataoff[mini], &wb, bl);
            if (r!=0) {
                result = r;
                break;
            }
        }
        
        {
            // read next row from file that just sourced min value 
            int r = loader_read_row_from_dbufio(bfs, mini, &keys[mini], &vals[mini]);
            if (r!=0) {
                if (r==EOF) {
                    // on feof, queue size permanently smaller
                    toku_free(keys[mini].data);  keys[mini].data = NULL;
                    toku_free(vals[mini].data);  vals[mini].data = NULL;
                } else {
                    fprintf(stderr, "%s:%d r=%d errno=%d bfs=%p mini=%d\n", __FILE__, __LINE__, r, get_maybe_error_errno(), bfs, mini);
                    dbufio_print(bfs);
                    result = r;
                    break;
                }
            } else {
                // insert value into queue (re-populate queue)
                pq_nodes[mini].key = &keys[mini];
                r = pqueue_insert(pq, &pq_nodes[mini]);
                if (r!=0) {
                    // Note: This error path tested by loader-dup-test1.tdbrun (and by loader-dup-test4)
                    result = r;
                    // printf("%s:%d returning\n", __FILE__, __LINE__);
                    break;
                }
            }
        }
 
        n_rows_done++;
        const uint64_t rows_per_report = size_factor*1024;
        if (n_rows_done%rows_per_report==0) {
            // need to update the progress.
            double fraction_of_remaining_we_just_did = (double)rows_per_report / (double)(n_rows - n_rows_done + rows_per_report);
            invariant(0<= fraction_of_remaining_we_just_did && fraction_of_remaining_we_just_did<=1);
            int progress_just_done = fraction_of_remaining_we_just_did * progress_allocation;
            progress_allocation -= progress_just_done;
            // ignore the result from update_progress here, we'll call update_progress again below, which will give us the nonzero result.
            int r = update_progress(progress_just_done, bl, "in file merge");
            if (0) printf("%s:%d Progress=%d\n", __FILE__, __LINE__, r);
        }
    }
    if (result == 0 && uncompressed_buffer != nullptr && wb.ndone > 0) {
        result = bl_finish_compressed_write(dest_stream, &wb);
    }

    if (result==0 && to_q) {
        int r = toku_queue_enq(q, (void*)output_rowset, 1, NULL);
        if (r!=0) 
            result = r;
        else 
            output_rowset = NULL;
    }

    // cleanup
    if (uncompressed_buffer) {
        toku_free(uncompressed_buffer);
    }
    for (int i=0; i<n_sources; i++) {
        toku_free(keys[i].data);  keys[i].data = NULL;
        toku_free(vals[i].data);  vals[i].data = NULL;
    }
    if (output_rowset) {
        destroy_rowset(output_rowset);
        toku_free(output_rowset);
    }
    if (pq) { pqueue_free(pq); pq=NULL; }
    toku_free(pq_nodes);
    {
        int r = update_progress(progress_allocation, bl, "end of merge_some_files");
        //printf("%s:%d Progress=%d\n", __FILE__, __LINE__, r);
        if (r!=0 && result==0) result = r;
    }
    return result;
}

static int merge_some_files (const bool to_q, FIDX dest_data, QUEUE q, int n_sources, FIDX srcs_fidxs[/*n_sources*/], FTLOADER bl, int which_db, DB *dest_db, ft_compare_func compare, int progress_allocation)
{
    int result = 0;
    DBUFIO_FILESET bfs = NULL;
    int *MALLOC_N(n_sources, fds);
    if (fds==NULL) result=get_error_errno();
    if (result==0) {
        for (int i=0; i<n_sources; i++) {
            int r = fileno(toku_bl_fidx2file(bl, srcs_fidxs[i])); // we rely on the fact that when the files are closed, the fd is also closed.
            if (r==-1) {
                result=get_error_errno();
                break;
            }
            fds[i] = r;
        }
    }
    if (result==0) {
        int r = create_dbufio_fileset(&bfs, n_sources, fds,
                memory_per_rowset_during_merge(bl, n_sources, to_q), bl->compress_intermediates);
        if (r!=0) { result = r; }
    }
        
    if (result==0) {
        int r = toku_merge_some_files_using_dbufio (to_q, dest_data, q, n_sources, bfs, srcs_fidxs, bl, which_db, dest_db, compare, progress_allocation);
        if (r!=0) { result = r; }
    }

    if (bfs!=NULL) {
        if (result != 0)
            (void) panic_dbufio_fileset(bfs, result);
        int r = destroy_dbufio_fileset(bfs);
        if (r!=0 && result==0) result=r;
        bfs = NULL;
    }
    if (fds!=NULL) {
        toku_free(fds);
        fds = NULL;
    }
    return result;
}

static int int_min (int a, int b)
{
    if (a<b) return a;
    else return b;
}

static int n_passes (int N, int B) {
    int result = 0;
    while (N>1) {
        N = (N+B-1)/B;
        result++;
    }
    return result;
}

int merge_files (struct merge_fileset *fs,
                 FTLOADER bl,
                 // These are needed for the comparison function and error callback.
                 int which_db, DB *dest_db, ft_compare_func compare,
                 int progress_allocation,
                 // Write rowsets into this queue.
                 QUEUE output_q
                 )
/* Effect:  Given a fileset, merge all the files writing all the answers into a queue.
 *   All the files in fs, and any temporary files will be closed and unlinked (and the fileset will be empty)
 * Return value: 0 on success, otherwise an error number.
 *   On error *fs will contain no open files.  All the files (including any temporary files) will be closed and unlinked.
 *    (however the fs will still need to be deallocated.)
 */
{
    //printf(" merge_files %d files\n", fs->n_temp_files);
    //printf(" merge_files use %d progress=%d fin at %d\n", progress_allocation, bl->progress, bl->progress+progress_allocation);
    const int final_mergelimit   = (size_factor == 1) ? 4 : merge_fanin(bl, true); // try for a merge to the leaf level
    const int earlier_mergelimit = (size_factor == 1) ? 4 : merge_fanin(bl, false); // try for a merge at nonleaf.
    int n_passes_left  = (fs->n_temp_files<=final_mergelimit)
        ? 1
        : 1+n_passes((fs->n_temp_files+final_mergelimit-1)/final_mergelimit, earlier_mergelimit);
    // printf("%d files, %d on last pass, %d on earlier passes, %d passes\n", fs->n_temp_files, final_mergelimit, earlier_mergelimit, n_passes_left);
    int result = 0;
    while (fs->n_temp_files > 0) {
        int progress_allocation_for_this_pass = progress_allocation/n_passes_left;
        progress_allocation -= progress_allocation_for_this_pass;
        //printf("%s:%d n_passes_left=%d progress_allocation_for_this_pass=%d\n", __FILE__, __LINE__, n_passes_left, progress_allocation_for_this_pass);

        invariant(fs->n_temp_files>0);
        struct merge_fileset next_file_set;
        bool to_queue = (bool)(fs->n_temp_files <= final_mergelimit);
        init_merge_fileset(&next_file_set);
        while (fs->n_temp_files>0) {
            // grab some files and merge them.
            int n_to_merge = int_min(to_queue?final_mergelimit:earlier_mergelimit, fs->n_temp_files);

            // We are about to do n_to_merge/n_temp_files of the remaining for this pass.
            int progress_allocation_for_this_subpass = progress_allocation_for_this_pass * (double)n_to_merge / (double)fs->n_temp_files;
            // printf("%s:%d progress_allocation_for_this_subpass=%d n_temp_files=%d b=%llu\n", __FILE__, __LINE__, progress_allocation_for_this_subpass, fs->n_temp_files, (long long unsigned) memory_per_rowset_during_merge(bl, n_to_merge, to_queue));
            progress_allocation_for_this_pass -= progress_allocation_for_this_subpass;

            //printf("%s:%d merging\n", __FILE__, __LINE__);
            FIDX merged_data = FIDX_NULL;

            FIDX *XMALLOC_N(n_to_merge, data_fidxs);
            for (int i=0; i<n_to_merge; i++) {
                data_fidxs[i] = FIDX_NULL;
            }
            for (int i=0; i<n_to_merge; i++) {
                int idx = fs->n_temp_files -1 -i;
                FIDX fidx = fs->data_fidxs[idx];
                result = ft_loader_fi_reopen(&bl->file_infos, fidx, "r");
                if (result) break;
                data_fidxs[i] = fidx;
            }
            if (result==0 && !to_queue) {
                result = extend_fileset(bl, &next_file_set,  &merged_data);
            }

            if (result==0) {
                result = merge_some_files(to_queue, merged_data, output_q, n_to_merge, data_fidxs, bl, which_db, dest_db, compare, progress_allocation_for_this_subpass);
                // if result!=0, fall through
                if (result==0) {
                    /*nothing*/;// this is gratuitous, but we need something to give code coverage tools to help us know that it's important to distinguish between result==0 and result!=0
                }
            }

            //printf("%s:%d merged\n", __FILE__, __LINE__);
            for (int i=0; i<n_to_merge; i++) {
                if (!fidx_is_null(data_fidxs[i])) {
                    {
                        int r = ft_loader_fi_close(&bl->file_infos, data_fidxs[i], true);
                        if (r!=0 && result==0) result = r;
                    }
                    {
                        int r = ft_loader_fi_unlink(&bl->file_infos, data_fidxs[i]);
                        if (r!=0 && result==0) result = r;
                    }
                    data_fidxs[i] = FIDX_NULL;
                }
            }

            fs->n_temp_files -= n_to_merge;
            if (!to_queue && !fidx_is_null(merged_data)) {
                int r = ft_loader_fi_close(&bl->file_infos, merged_data, true);
                if (r!=0 && result==0) result = r;
            }
            toku_free(data_fidxs);

            if (result!=0) break;
        }

        destroy_merge_fileset(fs);
        *fs = next_file_set;

        // Update the progress
        n_passes_left--;

        if (result==0) { invariant(progress_allocation_for_this_pass==0); }

        if (result!=0) break;
    }
    if (result) ft_loader_set_panic(bl, result, true, which_db, nullptr, nullptr);

    {
        int r = toku_queue_eof(output_q);
        if (r!=0 && result==0) result = r;
    }
    // It's conceivable that the progress_allocation could be nonzero (for example if bl->N==0)
    {
        int r = update_progress(progress_allocation, bl, "did merge_files");
        if (r!=0 && result==0) result = r;
    }
    return result;
}

// dbuf will always contained 512-byte aligned buffer, but the length might not be a multiple of 512 bytes.  If that's what you want, then pad it.
struct dbuf {
    unsigned char *buf;
    int buflen;
    int off;
    int error;
};

static void drain_writer_q(QUEUE q) {
    void *item;
    while (1) {
        int r = toku_queue_deq(q, &item, NULL, NULL);
        if (r == EOF)
            break;
        invariant(r == 0);
        struct rowset *rowset = (struct rowset *) item;
        destroy_rowset(rowset);
        toku_free(rowset);
    }
}

static int toku_loader_write_ft_from_q (FTLOADER bl,
                                         int progress_allocation,
                                         QUEUE q,
                                         int which_db)
// Effect: Consume a sequence of rowsets work from a queue, creating a fractal tree.  Closes fd.
{
    // set the number of fractal tree writer threads so that we can partition memory in the merger
    ft_loader_set_fractal_workers_count(bl);
    int progress_quantile = 2*(bl->n_rows)/100;
    uint64_t n_rows_processed = 0;
    int result = 0;
    int r = 0;
    while (result == 0) {
        void *item;
        {
            int rr = toku_queue_deq(q, &item, NULL, NULL);
            if (rr == EOF) break;
            if (rr != 0) {
                ft_loader_set_panic(bl, rr, true, which_db, nullptr, nullptr);
                break;
            }
        }
        struct rowset *output_rowset = (struct rowset *)item;

        for (unsigned int i = 0; i < output_rowset->n_rows; i++) {
            n_rows_processed++;
            DBT key = make_dbt(output_rowset->data+output_rowset->rows[i].off, output_rowset->rows[i].klen);
            DBT val = make_dbt(output_rowset->data+output_rowset->rows[i].off + output_rowset->rows[i].klen, output_rowset->rows[i].vlen);
            // some unit tests may not set a write function
            if (bl->write_func) {
                int rr = bl->write_func(bl->dbs[which_db], &key, &val, bl->write_func_extra);
                if (rr != 0) {
                    ft_loader_set_panic(bl, rr, true, which_db, nullptr, nullptr);
                    goto error;
                }
            }
            // TODO: need calls to update_progress to check if we should abort early
            if (progress_quantile && n_rows_processed % progress_quantile == 0) {
                r = update_progress((progress_allocation/100), bl, "writing rows");
                if (r) {
                    result = r; goto error;
                }
            }
        }

        destroy_rowset(output_rowset);
        toku_free(output_rowset);

        if (result == 0) {
            result = ft_loader_get_error(&bl->error_callback); // check if an error was posted and terminate this quickly
        }
    }

    if (result == 0) {
        result = ft_loader_get_error(&bl->error_callback); // if there were any prior errors then exit
    }

    if (result != 0) goto error;

    // Do we need to pay attention to user_said_stop?  Or should the guy at the other end of the queue pay attention and send in an EOF.

 error:
    drain_writer_q(q);
    return result;
}

int toku_loader_write_ft_from_q_in_C (FTLOADER                bl,
                                      int                      fd UU(), // write to here
                                      int                      progress_allocation,
                                      QUEUE                    q,
                                      uint64_t                 total_disksize_estimate UU(),
                                      int                      which_db,
                                      uint32_t                 target_nodesize UU(),
                                      uint32_t                 target_basementnodesize UU(),
                                      enum toku_compression_method target_compression_method UU(),
                                      uint32_t                 target_fanout UU())
// This is probably only for testing.
{
    return toku_loader_write_ft_from_q (bl, progress_allocation, q, which_db);
}


static void* fractal_thread (void *ftav) {
    struct fractal_thread_args *fta = (struct fractal_thread_args *)ftav;
    int r = toku_loader_write_ft_from_q (fta->bl, fta->progress_allocation, fta->q, fta->which_db);
    fta->errno_result = r;
    return NULL;
}

static int loader_do_i (FTLOADER bl,
                        int which_db,
                        DB *dest_db,
                        ft_compare_func compare,
                        int progress_allocation // how much progress do I need to add into bl->progress by the end..
                        )
/* Effect: Handle the file creating for one particular DB in the bulk loader. */
/* Requires: The data is fully extracted, so we can do merges out of files and write the ft file. */
{
    //printf("doing i use %d progress=%d fin at %d\n", progress_allocation, bl->progress, bl->progress+progress_allocation);
    struct merge_fileset *fs = &(bl->fs[which_db]);
    struct rowset *rows = &(bl->rows[which_db]);
    invariant(rows->data==NULL); // the rows should be all cleaned up already

    int r = toku_queue_create(&bl->fractal_queues[which_db], FRACTAL_WRITER_QUEUE_DEPTH);
    if (r) goto error;
    {
        // a better allocation would be to figure out roughly how many merge passes we'll need.
        int allocation_for_merge = (2*progress_allocation)/3;
        progress_allocation -= allocation_for_merge;
        
        // This structure must stay live until the join below.
        struct fractal_thread_args fta = { 
            bl,
            progress_allocation,
            bl->fractal_queues[which_db],
            0,
            which_db
        };

        r = toku_pthread_create(bl->fractal_threads+which_db, NULL, fractal_thread, (void*)&fta);
        if (r) {
            int r2 __attribute__((__unused__)) = toku_queue_destroy(bl->fractal_queues[which_db]);            
            // ignore r2, since we already have an error
            bl->fractal_queues[which_db] = nullptr;
            goto error;
        }
        invariant(bl->fractal_threads_live[which_db]==false);
        bl->fractal_threads_live[which_db] = true;

        r = merge_files(fs, bl, which_db, dest_db, compare, allocation_for_merge, bl->fractal_queues[which_db]);

        {
            void *toku_pthread_retval;
            int r2 = toku_pthread_join(bl->fractal_threads[which_db], &toku_pthread_retval);
            invariant(fta.bl==bl); // this is a gratuitous assertion to make sure that the fta struct is still live here.  A previous bug put that struct into a C block statement.
            resource_assert_zero(r2);
            invariant(toku_pthread_retval==NULL);
            invariant(bl->fractal_threads_live[which_db]);
            bl->fractal_threads_live[which_db] = false;
            if (r == 0) r = fta.errno_result;
        }
    }

error: // this is the cleanup code.  Even if r==0 (no error) we fall through to here.
    if (bl->fractal_queues[which_db]) {
        int r2 = toku_queue_destroy(bl->fractal_queues[which_db]);
        invariant(r2==0);
        bl->fractal_queues[which_db] = nullptr;
    }

    // if we get here we need to free up the merge_fileset and the rowset, as well as the keys
    toku_free(rows->data); rows->data = NULL;
    toku_free(rows->rows); rows->rows = NULL;
    toku_free(fs->data_fidxs); fs->data_fidxs = NULL;
    return r;
}

static int toku_ft_loader_close_internal (FTLOADER bl)
/* Effect: Close the bulk loader.
 * Return all the file descriptors in the array fds. */
{
    int result = 0;
    if (bl->N == 0)
        result = update_progress(PROGRESS_MAX, bl, "done");
    else {
        int remaining_progress = PROGRESS_MAX;
        for (int i = 0; i < bl->N; i++) {
            // Take the unallocated progress and divide it among the unfinished jobs.
            // This calculation allocates all of the PROGRESS_MAX bits of progress to some job.
            int allocate_here = remaining_progress/(bl->N - i);
            remaining_progress -= allocate_here;
            result = loader_do_i(bl, i, bl->dbs[i], bl->bt_compare_funs[i], allocate_here);
            if (result != 0) 
                goto error;
            invariant(0 <= bl->progress && bl->progress <= PROGRESS_MAX);
        }
        if (result==0) invariant(remaining_progress==0);
    }
    invariant(bl->file_infos.n_files_open   == 0);
    invariant(bl->file_infos.n_files_extant == 0);
 error:
    toku_ft_loader_internal_destroy(bl, (bool)(result!=0));
    return result;
}

int toku_ft_loader_close (FTLOADER bl,
                           ft_loader_error_func error_function, void *error_extra,
                           ft_loader_poll_func  poll_function,  void *poll_extra
                           )
{
    int result = 0;

    int r;

    //printf("Closing\n");

    if (error_function) {
        ft_loader_set_error_function(&bl->error_callback, error_function, error_extra);
    }
    if (poll_function) {
        ft_loader_set_poll_function(&bl->poll_callback, poll_function, poll_extra);
    }

    if (bl->extractor_live) {
        r = finish_extractor(bl);
        if (r)
            result = r;
        invariant(!bl->extractor_live);
    } else {
        r = finish_primary_rows(bl);
        if (r)
            result = r;
    }

    // check for an error during extraction
    if (result == 0) {
        r = ft_loader_call_error_function(&bl->error_callback);
        if (r)
            result = r;
    }

    if (result == 0) {
        r = toku_ft_loader_close_internal(bl);
        if (r && result == 0)
            result = r;
    } else
        toku_ft_loader_internal_destroy(bl, true);

    return result;
}

int toku_ft_loader_finish_extractor(FTLOADER bl) {
    int result = 0;
    if (bl->extractor_live) {
        int r = finish_extractor(bl);
        if (r)
            result = r;
        invariant(!bl->extractor_live);
    } else
        result = EINVAL;
    return result;
}

int toku_ft_loader_abort(FTLOADER bl, bool is_error) 
/* Effect : Abort the bulk loader, free ft_loader resources */
{
    int result = 0;

    // cleanup the extractor thread
    if (bl->extractor_live) {
        int r = finish_extractor(bl);
        if (r)
            result = r;
        invariant(!bl->extractor_live);
    }

    for (int i = 0; i < bl->N; i++)
        invariant(!bl->fractal_threads_live[i]);

    toku_ft_loader_internal_destroy(bl, is_error);
    return result;
}

int toku_ft_loader_get_error(FTLOADER bl, int *error) {
    *error = ft_loader_get_error(&bl->error_callback);
    return 0;
}

void ft_loader_set_fractal_workers_count_from_c(FTLOADER bl) {
    ft_loader_set_fractal_workers_count (bl);
}


