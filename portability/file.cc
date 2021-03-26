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

#include "toku_portability.h"
#include <unistd.h>
#include <errno.h>
#include "toku_assert.h"
#include <stdio.h>
#include <string.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "memory.h"
#include "toku_time.h"
#include "toku_path.h"
#include "toku_atomic.h"
#include <atomic>

toku_instr_key *tokudb_file_data_key;

static bool toku_assert_on_write_enospc = false;
static constexpr int toku_write_enospc_sleep = 1;

// timestamp of most recent report to error log
static std::atomic<uint64_t> toku_write_enospc_last_report;

// timestamp of most recent ENOSPC
static std::atomic<time_t> toku_write_enospc_last_time;

// number of threads currently blocked on ENOSPC
static std::atomic<uint32_t> toku_write_enospc_current;

// total number of times ENOSPC was returned from an attempt to write
static std::atomic<uint64_t> toku_write_enospc_total;

void toku_set_assert_on_write_enospc(int do_assert) {
    toku_assert_on_write_enospc = do_assert;
}

void toku_fs_get_write_info(time_t *enospc_last_time, uint64_t *enospc_current, uint64_t *enospc_total) {
    *enospc_last_time = toku_write_enospc_last_time;
    *enospc_current = toku_write_enospc_current;
    *enospc_total = toku_write_enospc_total;
}

static void __attribute__((format(printf, 2, 3)))
report_io_problem(int fd, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);

    constexpr int tstr_length = 26;
    char tstr[tstr_length];
    time_t t = time(0);
    ctime_r(&t, tstr);

    char buf[4096];

    vsnprintf(buf, sizeof(buf), fmt, ap);

#if __linux__
    char fdname[256];
    snprintf(fdname, sizeof fdname, "/proc/%d/fd/%d", getpid(), fd);
    char lname[256];
    ssize_t s = readlink(fdname, lname, sizeof lname);
    if (0 < s && s < (ssize_t)sizeof lname)
        lname[s] = 0;

    fprintf(stderr,
            "%.24s : PerconaFT I/O problem, fd[%d], file[%s] : %s\n",
            tstr,
            fd,
            s > 0 ? lname : "?",
            buf);
#else
    fprintf(stderr,
            "%.24s : PerconaFT I/O problem, fd[%d], file[?] : %s\n",
            tstr,
            fd,
            buf);
#endif
    va_end(ap);
    fflush(stderr);
}

// Determines whether or not a retry is in order for any 'write' type
// operations and logs failures. These include write, fsync, and close.
// May assert/abort on fatal errors or in debug builds
static bool handle_write_error(int fd,
                             int what_errno,
                             uint64_t retry_count,
                             const char *operation) {
    bool ret = false;
    switch (what_errno) {
        case ENOSPC:
            if (toku_assert_on_write_enospc) {
                report_io_problem(
                    fd, "[%s] failed with ENOSPC.  Aborting.", operation);
                resource_assert(0);
            } else {
                toku_write_enospc_total++;
                toku_write_enospc_current++;

                time_t tnow = time(0);
                toku_write_enospc_last_time = tnow;
                if (toku_write_enospc_last_report == 0 ||
                    tnow - toku_write_enospc_last_report >= 60) {
                    toku_write_enospc_last_report = tnow;

                    report_io_problem(
                        fd,
                        "[%s] failed with ENOSPC.  Retry in %d second%s on %" PRIu64
                        " tries",
                        operation,
                        toku_write_enospc_sleep,
                        toku_write_enospc_sleep > 1 ? "s" : "",
                        retry_count);
                }
                sleep(toku_write_enospc_sleep);
                toku_write_enospc_current--;
                ret = true;
                break;
            }
            break;

        case ENOLCK:
        // fallthrough to EINTR

        case EINTR:
            if (retry_count % 100 == 0) {
                report_io_problem(fd,
                                  "[%s] failed with %s. Retry on %" PRIu64
                                  " tries.",
                                  operation,
                                  what_errno == ENOLCK ? "ENOLCK" : "EINTR",
                                  retry_count);
            }
            ret = true;
            break;

        case EIO:
            // This is a non-recoverable fatal kernel error. A buffered write
            // was forever lost and detected duiring close or fsync.
            // Documentation also indicates that this may be returned from write
            // if fd is some type of pipe from a orphaned background process to
            // its now missing controlling terminal. Not likely something we
            // expect to run into.
            report_io_problem(fd,
                              "[%s] failed with EIO.  Aborting.",
                              operation);
            resource_assert(0);
            break;

        default:
            report_io_problem(
                fd, "[%s] failed with errno %d.", operation, what_errno);
            break;
    }

    // If we have a debug build, lets go ahead and assert to try and get a call
    // stack and some context on where we are getting these unusual I/O errors
    paranoid_invariant(0);

    return ret;
}

static ssize_t (*t_write)(int, const void *, size_t);
static ssize_t (*t_full_write)(int, const void *, size_t);
static ssize_t (*t_pwrite)(int, const void *, size_t, off_t);
static ssize_t (*t_full_pwrite)(int, const void *, size_t, off_t);
static FILE *  (*t_fdopen)(int, const char *);
static FILE *  (*t_fopen)(const char *, const char *);
static int     (*t_open)(const char *, int, int);
static int (*t_fclose)(FILE *);
static ssize_t (*t_read)(int, void *, size_t);
static ssize_t (*t_pread)(int, void *, size_t, off_t);
static size_t (*os_fwrite_fun)(const void *, size_t, size_t, FILE *) = nullptr;

void toku_set_func_fwrite(
    size_t (*fwrite_fun)(const void *, size_t, size_t, FILE *)) {
    os_fwrite_fun = fwrite_fun;
}

void toku_set_func_write(ssize_t (*write_fun)(int, const void *, size_t)) {
    t_write = write_fun;
}

void toku_set_func_full_write (ssize_t (*write_fun)(int, const void *, size_t)) {
    t_full_write = write_fun;
}

void toku_set_func_pwrite (ssize_t (*pwrite_fun)(int, const void *, size_t, off_t)) {
    t_pwrite = pwrite_fun;
}

void toku_set_func_full_pwrite (ssize_t (*pwrite_fun)(int, const void *, size_t, off_t)) {
    t_full_pwrite = pwrite_fun;
}

void toku_set_func_fdopen(FILE * (*fdopen_fun)(int, const char *)) {
    t_fdopen = fdopen_fun;
}

void toku_set_func_fopen(FILE * (*fopen_fun)(const char *, const char *)) {
    t_fopen = fopen_fun;
}

void toku_set_func_open(int (*open_fun)(const char *, int, int)) {
    t_open = open_fun;
}

void toku_set_func_fclose(int (*fclose_fun)(FILE*)) {
    t_fclose = fclose_fun;
}

void toku_set_func_read (ssize_t (*read_fun)(int, void *, size_t)) {
    t_read = read_fun;
}

void toku_set_func_pread (ssize_t (*pread_fun)(int, void *, size_t, off_t)) {
    t_pread = pread_fun;
}

int toku_os_delete_with_source_location(const char *name,
                                        const char *src_file,
                                        uint src_line) {

    toku_io_instrumentation io_annotation;
    toku_instr_file_name_close_begin(io_annotation,
                                     *tokudb_file_data_key,
                                     toku_instr_file_op::file_delete,
                                     name,
                                     src_file,
                                     src_line);
    const int result = unlink(name);

    /* Register the result value with the instrumentation system */
    toku_instr_file_close_end(io_annotation, result);

    return result;
}

int toku_os_rename_with_source_location(const char *old_name,
                                        const char *new_name,
                                        const char *src_file,
                                        uint src_line) {
    int result;

    toku_io_instrumentation io_annotation;
    toku_instr_file_name_io_begin(io_annotation,
                                  *tokudb_file_data_key,
                                  toku_instr_file_op::file_rename,
                                  new_name,
                                  0,
                                  src_file,
                                  src_line);

    result = rename(old_name, new_name);
    /* Regsiter the result value with the instrumentation system */
    toku_instr_file_io_end(io_annotation, 0);

    return result;
}

void toku_os_full_write_with_source_location(int fd,
                                             const void *buf,
                                             size_t len,
                                             const char *src_file,
                                             uint src_line) {
    const char *bp = (const char *)buf;
    size_t bytes_written = len;
    bool retry = true;
    uint64_t retry_count = 0;

    toku_io_instrumentation io_annotation;
    toku_instr_file_io_begin(io_annotation,
                             toku_instr_file_op::file_write,
                             fd,
                             len,
                             src_file,
                             src_line);

    while (len > 0 && retry) {
        ssize_t r;
        if (t_full_write) {
            r = t_full_write(fd, bp, len);
        } else {
            r = write(fd, bp, len);
        }
        if (r > 0) {
            len -= r;
            bp += r;
        } else {
            retry =
                handle_write_error(fd, get_error_errno(), retry_count, "write");
            retry_count++;
        }
    }
    assert(len == 0);

    /* Register the result value with the instrumentaion system */
    toku_instr_file_io_end(io_annotation, bytes_written);
}

int toku_os_write_with_source_location(int fd,
                                       const void *buf,
                                       size_t len,
                                       const char *src_file,
                                       uint src_line) {
    const char *bp = (const char *)buf;
    int result = 0;
    ssize_t r;

    size_t bytes_written = len;
    toku_io_instrumentation io_annotation;
    toku_instr_file_io_begin(io_annotation,
                             toku_instr_file_op::file_write,
                             fd,
                             len,
                             src_file,
                             src_line);

    while (len > 0) {
        if (t_write) {
            r = t_write(fd, bp, len);
        } else {
            r = write(fd, bp, len);
        }
        if (r < 0) {
            result = errno;
            break;
        }
        len -= r;
        bp += r;
    }
    /* Register the result value with the instrumentation system */
    toku_instr_file_io_end(io_annotation, bytes_written - len);

    return result;
}

void toku_os_full_pwrite_with_source_location(int fd,
                                              const void *buf,
                                              size_t len,
                                              toku_off_t off,
                                              const char *src_file,
                                              uint src_line) {
    assert(0 == ((long long)buf) % 512);
    assert((len % 512 == 0) && (off % 512) == 0);  // to make pwrite work.
    const char *bp = (const char *)buf;
    bool retry = true;
    uint64_t retry_count = 0;

    size_t bytes_written = len;
    toku_io_instrumentation io_annotation;
    toku_instr_file_io_begin(io_annotation,
                             toku_instr_file_op::file_write,
                             fd,
                             len,
                             src_file,
                             src_line);
    while (len > 0 && retry) {
        ssize_t r;
        if (t_full_pwrite) {
            r = t_full_pwrite(fd, bp, len, off);
        } else {
            r = pwrite(fd, bp, len, off);
        }
        if (r > 0) {
            len -= r;
            bp += r;
            off += r;
        } else {
            retry =
                handle_write_error(fd, get_error_errno(), retry_count, "write");
            retry_count++;
        }
    }
    assert(len == 0);

    /* Register the result value with the instrumentation system */
    toku_instr_file_io_end(io_annotation, bytes_written);
}

ssize_t toku_os_pwrite_with_source_location(int fd,
                                            const void *buf,
                                            size_t len,
                                            toku_off_t off,
                                            const char *src_file,
                                            uint src_line) {
    assert(0 ==
           ((long long)buf) %
               512);  // these asserts are to ensure that direct I/O will work.
    assert(0 == len % 512);
    assert(0 == off % 512);
    const char *bp = (const char *)buf;
    ssize_t result = 0;
    ssize_t r;

    size_t bytes_written = len;
    toku_io_instrumentation io_annotation;
    toku_instr_file_io_begin(io_annotation,
                             toku_instr_file_op::file_write,
                             fd,
                             len,
                             src_file,
                             src_line);
    while (len > 0) {
        r = (t_pwrite) ? t_pwrite(fd, bp, len, off) : pwrite(fd, bp, len, off);

        if (r < 0) {
            result = errno;
            break;
        }
        len           -= r;
        bp += r;
        off += r;
    }
    /* Register the result value with the instrumentation system */
    toku_instr_file_io_end(io_annotation, bytes_written - len);

    return result;
}

int toku_os_fwrite_with_source_location(const void *ptr,
                                        size_t size,
                                        size_t nmemb,
                                        TOKU_FILE *stream,
                                        const char *src_file,
                                        uint src_line) {
    int result = 0;
    size_t bytes_written;

    toku_io_instrumentation io_annotation;
    toku_instr_file_stream_io_begin(io_annotation,
                                    toku_instr_file_op::file_write,
                                    *stream,
                                    nmemb,
                                    src_file,
                                    src_line);

    if (os_fwrite_fun) {
        bytes_written = os_fwrite_fun(ptr, size, nmemb, stream->file);
    } else {
        bytes_written = fwrite(ptr, size, nmemb, stream->file);
    }

    if (bytes_written != nmemb) {
        if (os_fwrite_fun)  // if using hook to induce artificial errors (for
                            // testing) ...
            result = get_maybe_error_errno();  // ... then there is no error in
                                               // the stream, but there is one
                                               // in errno
        else
            result = ferror(stream->file);
        invariant(result != 0);  // Should we assert here?
    }
    /* Register the result value with the instrumentation system */
    toku_instr_file_io_end(io_annotation, bytes_written);

    return result;
}

int toku_os_fread_with_source_location(void *ptr,
                                       size_t size,
                                       size_t nmemb,
                                       TOKU_FILE *stream,
                                       const char *src_file,
                                       uint src_line) {
    int result = 0;
    size_t bytes_read;

    toku_io_instrumentation io_annotation;
    toku_instr_file_stream_io_begin(io_annotation,
                                    toku_instr_file_op::file_read,
                                    *stream,
                                    nmemb,
                                    src_file,
                                    src_line);

    if ((bytes_read = fread(ptr, size, nmemb, stream->file)) != nmemb) {
        if ((feof(stream->file)))
            result = EOF;
        else
            result = ferror(stream->file);
        invariant(result != 0);  // Should we assert here?
    }
    /* Register the result value with the instrumentation system */
    toku_instr_file_io_end(io_annotation, bytes_read);

    return result;
}

TOKU_FILE *toku_os_fdopen_with_source_location(int fildes,
                                               const char *mode,
                                               const char *filename,
                                               const toku_instr_key &instr_key,
                                               const char *src_file,
                                               uint src_line) {
    TOKU_FILE *XMALLOC(rval);
    if (FT_LIKELY(rval != nullptr)) {
        toku_io_instrumentation io_annotation;
        toku_instr_file_open_begin(io_annotation,
                                   instr_key,
                                   toku_instr_file_op::file_stream_open,
                                   filename,
                                   src_file,
                                   src_line);

        rval->file = (t_fdopen) ? t_fdopen(fildes, mode) : fdopen(fildes, mode);
        toku_instr_file_stream_open_end(io_annotation, *rval);

        if (FT_UNLIKELY(rval->file == nullptr)) {
            toku_free(rval);
            rval = nullptr;
        }
    }
    return rval;
}

TOKU_FILE *toku_os_fopen_with_source_location(const char *filename,
                                              const char *mode,
                                              const toku_instr_key &instr_key,
                                              const char *src_file,
                                              uint src_line) {
    TOKU_FILE *XMALLOC(rval);
    if (FT_UNLIKELY(rval == nullptr))
        return nullptr;

    toku_io_instrumentation io_annotation;
    toku_instr_file_open_begin(io_annotation,
                               instr_key,
                               toku_instr_file_op::file_stream_open,
                               filename,
                               src_file,
                               src_line);
    rval->file = t_fopen ? t_fopen(filename, mode) : fopen(filename, mode);
    /* Register the returning "file" value with the system */
    toku_instr_file_stream_open_end(io_annotation, *rval);

    if (FT_UNLIKELY(rval->file == nullptr)) {
        toku_free(rval);
        rval = nullptr;
    }
    return rval;
}

int toku_os_open_with_source_location(const char *path,
                                      int oflag,
                                      int mode,
                                      const toku_instr_key &instr_key,
                                      const char *src_file,
                                      uint src_line) {
    int fd;
    toku_io_instrumentation io_annotation;
    /* register a file open or creation depending on "oflag" */
    toku_instr_file_open_begin(
        io_annotation,
        instr_key,
        ((oflag & O_CREAT) ? toku_instr_file_op::file_create
                           : toku_instr_file_op::file_open),
        path,
        src_file,
        src_line);
    if (t_open)
        fd = t_open(path, oflag, mode);
    else
        fd = open(path, oflag, mode);

    toku_instr_file_open_end(io_annotation, fd);
    return fd;
}

int toku_os_open_direct(const char *path,
                        int oflag,
                        int mode,
                        const toku_instr_key &instr_key) {
    int rval;
#if defined(HAVE_O_DIRECT)
    rval = toku_os_open(path, oflag | O_DIRECT, mode, instr_key);
#elif defined(HAVE_F_NOCACHE)
    rval = toku_os_open(path, oflag, mode, instr_key);
    if (rval >= 0) {
        int r = fcntl(rval, F_NOCACHE, 1);
        if (r == -1) {
            perror("setting F_NOCACHE");
        }
    }
#else
# error "No direct I/O implementation found."
#endif
    return rval;
}

int toku_os_fclose_with_source_location(TOKU_FILE *stream,
                                        const char *src_file,
                                        uint src_line) {
    int rval = -1;
    if (FT_LIKELY(stream != nullptr)) {
        /* register a file stream close " */
        toku_io_instrumentation io_annotation;
        toku_instr_file_stream_close_begin(
            io_annotation,
            toku_instr_file_op::file_stream_close,
            *stream,
            src_file,
            src_line);

        if (t_fclose)
            rval = t_fclose(stream->file);
        else {  // if EINTR, retry until success
            while (rval != 0) {
                rval = fclose(stream->file);
                if (rval && (errno != EINTR))
                    break;
            }
        }
        /* Register the returning "rval" value with the system */
        toku_instr_file_close_end(io_annotation, rval);
        toku_free(stream);
        stream = nullptr;
    }
    return rval;
}

int toku_os_close_with_source_location(int fd,
                                       const char *src_file,
                                       uint src_line) {
    int r = -1;
    uint64_t retry_count = 0;
    bool retry = false;

    /* register a file descriptor close " */
    toku_io_instrumentation io_annotation;
    toku_instr_file_fd_close_begin(
        io_annotation, toku_instr_file_op::file_close, fd, src_file, src_line);

    do {
        r = close(fd);
        if (r == -1) {
            retry =
                handle_write_error(fd, get_error_errno(), retry_count, "close");
            retry_count++;
        }
    } while (retry);

    /* Regsiter the returning value with the system */
    toku_instr_file_close_end(io_annotation, r);

    return r;
}

ssize_t toku_os_read_with_source_location(int fd,
                                          void *buf,
                                          size_t count,
                                          const char *src_file,
                                          uint src_line) {
    ssize_t bytes_read;

    toku_io_instrumentation io_annotation;
    toku_instr_file_io_begin(io_annotation,
                             toku_instr_file_op::file_read,
                             fd,
                             count,
                             src_file,
                             src_line);

    bytes_read = (t_read) ? t_read(fd, buf, count) : read(fd, buf, count);

    toku_instr_file_io_end(io_annotation, bytes_read);

    return bytes_read;
}

ssize_t inline_toku_os_pread_with_source_location(int fd,
                                                  void *buf,
                                                  size_t count,
                                                  off_t offset,
                                                  const char *src_file,
                                                  uint src_line) {
    assert(0 == ((long long)buf) % 512);
    assert(0 == count % 512);
    assert(0 == offset % 512);
    ssize_t bytes_read;

    toku_io_instrumentation io_annotation;
    toku_instr_file_io_begin(io_annotation,
                             toku_instr_file_op::file_read,
                             fd,
                             count,
                             src_file,
                             src_line);
    if (t_pread) {
        bytes_read = t_pread(fd, buf, count, offset);
    } else {
        bytes_read = pread(fd, buf, count, offset);
    }
    toku_instr_file_io_end(io_annotation, bytes_read);

    return bytes_read;
}

void toku_os_recursive_delete(const char *path) {
    char buf[TOKU_PATH_MAX + sizeof("rm -rf ")];
    strcpy(buf, "rm -rf ");
    strncat(buf, path, TOKU_PATH_MAX);
    int r = system(buf);
    assert_zero(r);
}

// fsync logic:

// t_fsync exists for testing purposes only
static int (*t_fsync)(int) = 0;
static std::atomic<uint64_t> toku_fsync_count;
static std::atomic<uint64_t> toku_fsync_time;
static constexpr uint64_t toku_long_fsync_threshold = 1000000;
static std::atomic<uint64_t> toku_long_fsync_count;
static std::atomic<uint64_t> toku_long_fsync_time;
static std::atomic<uint64_t> toku_long_fsync_retry_count;
static constexpr bool toku_fsync_debug = false;

void toku_set_func_fsync(int (*fsync_function)(int)) {
    t_fsync = fsync_function;
}

// keep trying if fsync fails because of EINTR
void file_fsync_internal_with_source_location(int fd,
                                              const char *src_file,
                                              uint src_line) {
    uint64_t tstart = toku_current_time_microsec();
    int r = -1;
    uint64_t retry_count = 0;
    bool retry = false;

    toku_io_instrumentation io_annotation;
    toku_instr_file_io_begin(io_annotation,
                             toku_instr_file_op::file_sync,
                             fd,
                             0,
                             src_file,
                             src_line);

    do {
        if (t_fsync) {
            r = t_fsync(fd);
        } else {
            r = fsync(fd);
        }

        if (r == -1) {
            retry =
                handle_write_error(fd, get_error_errno(), retry_count, "fsync");
            retry_count++;
        }
    } while (retry);

    toku_fsync_count++;
    uint64_t duration = toku_current_time_microsec() - tstart;
    toku_fsync_time += duration;

    toku_instr_file_io_end(io_annotation, 0);

    if (duration >= toku_long_fsync_threshold) {
        toku_long_fsync_count++;
        toku_long_fsync_time += duration;
        toku_long_fsync_retry_count += retry_count;
        if (toku_fsync_debug) {
            report_io_problem(fd,
                              "toku_file_fsync took %" PRIu64
                              " usec with %" PRIu64 " retries.",
                              duration,
                              retry_count);
            fflush(stderr);
        }
    }
}

void toku_file_fsync_without_accounting(int fd) {
    file_fsync_internal(fd);
}

void toku_fsync_dirfd_without_accounting(DIR *dir) {
    int fd = dirfd(dir);
    toku_file_fsync_without_accounting(fd);
}

int toku_fsync_dir_by_name_without_accounting(const char *dir_name) {
    int r = 0;
    DIR * dir = opendir(dir_name);
    if (!dir) {
        r = get_error_errno();
    } else {
        toku_fsync_dirfd_without_accounting(dir);
        r = closedir(dir);
        if (r != 0) {
            r = get_error_errno();
        }
    }
    return r;
}

// include fsync in scheduling accounting
void toku_file_fsync(int fd) {
    file_fsync_internal (fd);
}

// for real accounting
void toku_get_fsync_times(uint64_t *fsync_count, uint64_t *fsync_time, uint64_t *long_fsync_threshold, uint64_t *long_fsync_count, uint64_t *long_fsync_time) {
    *fsync_count = toku_fsync_count;
    *fsync_time = toku_fsync_time;
    *long_fsync_threshold = toku_long_fsync_threshold;
    *long_fsync_count = toku_long_fsync_count;
    *long_fsync_time = toku_long_fsync_time;
}

int toku_fsync_directory(const char *fname) {
    int result = 0;
    
    // extract dirname from fname
    const char *sp = strrchr(fname, '/');
    size_t len;
    char *dirname = NULL;
    if (sp) {
        resource_assert(sp >= fname);
        len = sp - fname + 1;
        MALLOC_N(len+1, dirname);
        if (dirname == NULL) {
            result = get_error_errno();
        } else {
            strncpy(dirname, fname, len);
            dirname[len] = 0;
        }
    } else {
        dirname = toku_strdup(".");
        if (dirname == NULL) {
            result = get_error_errno();
        }
    }

    if (result == 0) {
        result = toku_fsync_dir_by_name_without_accounting(dirname);
    }
    toku_free(dirname);
    return result;
}
