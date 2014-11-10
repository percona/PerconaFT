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

#include "portability/toku_compiler.h"
#include "portability/toku_crash.h"
#include "portability/toku_stdlib.h"

#if TOKU_WINDOWS

// TODO: Write me

#else

#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#ifdef HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

#include "portability/toku_atomic.h"
#include "portability/toku_os.h"
#include "portability/toku_race_tools.h"

//Simulate as hard a crash as possible.
//Choices:
//  raise(SIGABRT)
//  kill -SIGKILL $pid
//  divide by 0
//  null dereference
//  abort()
//  assert(false) (from <assert.h>)
//  assert(false) (from <toku_assert.h>)
//
//Linux:
//  abort() and both assert(false) cause FILE buffers to be flushed and written to disk: Unacceptable
//
//kill -SIGKILL $pid is annoying (and so far untested)
//
//raise(SIGABRT) has the downside that perhaps it could be caught?
//I'm choosing raise(SIGABRT), followed by divide by 0, followed by null dereference, followed by all the others just in case one gets caught.
NORETURN
void toku_hard_crash_on_purpose(void) {
    raise(SIGKILL); //Does not flush buffers on linux; cannot be caught.
    {
        int zero = 0;
        int infinity = 1/zero;
        fprintf(stderr, "Force use of %d\n", infinity);
        fflush(stderr); //Make certain the string is calculated.
    }
    {
        void * intothevoid = NULL;
        (*(int*)intothevoid)++;
        fprintf(stderr, "Force use of *(%p) = %d\n", intothevoid, *(int*)intothevoid);
        fflush(stderr);
    }
    abort();
    fprintf(stderr, "This line should never be printed\n");
    fflush(stderr);
}

// Similar to toku_hard_crash_on_purpose, but the goal isn't to crash hard, the primary goal is to get a corefile, the secondary goal is to terminate in any way possible.
// We don't really care if buffers get flushed etc, in fact they may as well flush since there may be useful output in stdout or stderr.
//
// By default, the following signals generate cores:
//  Linux, from signal(7):
//     SIGQUIT       3       Core
//     SIGILL        4       Core
//     SIGABRT       6       Core
//     SIGFPE        8       Core
//     SIGSEGV      11       Core
//
//  Darwin and FreeBSD, from signal(3):
//     3     SIGQUIT      create core image
//     4     SIGILL       create core image
//     5     SIGTRAP      create core image
//     6     SIGABRT      create core image
//     7     SIGEMT       create core image
//     8     SIGFPE       create core image
//     10    SIGBUS       create core image
//     11    SIGSEGV      create core image
//     12    SIGSYS       create core image
//
// We'll raise these in some sequence (common ones first), then try emulating the things that would cause these signals to be raised, then eventually just try to die normally and then loop like abort does.
// Start with a toku assert because that hopefully prints a stacktrace.
NORETURN
void toku_crash_and_dump_core_on_purpose(void) {
    assert(false);
    invariant(0);
    raise(SIGQUIT);
    raise(SIGILL);
    raise(SIGABRT);
    raise(SIGFPE);
    raise(SIGSEGV);
#if defined(__FreeBSD__) || defined(__APPLE__)
    raise(SIGTRAP);
    raise(SIGEMT);
    raise(SIGBUS);
    raise(SIGSYS);
#endif
    abort();
    {
        int zero = 0;
        int infinity = 1/zero;
        fprintf(stderr, "Force use of %d\n", infinity);
        fflush(stderr); //Make certain the string is calculated.
    }
    {
        void * intothevoid = NULL;
        (*(int*)intothevoid)++;
        fprintf(stderr, "Force use of *(%p) = %d\n", intothevoid, *(int*)intothevoid);
        fflush(stderr);
    }
    raise(SIGKILL);
    while (true) {
        // don't return
    }
}

enum { MAX_GDB_ARGS = 128 };

static void
run_gdb(pid_t parent_pid, const char *gdb_path) {
    // 3 bytes per intbyte, null byte
    char pid_buf[sizeof(pid_t) * 3 + 1];
    char exe_buf[sizeof(pid_buf) + sizeof("/proc//exe")];

    // Get pid and path to executable.
    int n;
    n = snprintf(pid_buf, sizeof(pid_buf), "%d", parent_pid);
    invariant(n >= 0 && n < (int)sizeof(pid_buf));
    n = snprintf(exe_buf, sizeof(exe_buf), "/proc/%d/exe", parent_pid);
    invariant(n >= 0 && n < (int)sizeof(exe_buf));

    dup2(2, 1); // redirect output to stderr
    // Arguments are not dynamic due to possible security holes.
    execlp(gdb_path, gdb_path, "--batch", "-n",
           "-ex", "thread",
           "-ex", "bt",
           "-ex", "bt full",
           "-ex", "thread apply all bt",
           "-ex", "thread apply all bt full",
           exe_buf, pid_buf,
           NULL);
}

static void
intermediate_process(pid_t parent_pid, const char *gdb_path) {
    // Disable generating of core dumps
#if defined(HAVE_SYS_PRCTL_H)
    prctl(PR_SET_DUMPABLE, 0, 0, 0);
#endif
    pid_t worker_pid = fork();
    if (worker_pid < 0) {
        perror("spawn gdb fork: ");
        goto failure;
    }
    if (worker_pid == 0) {
        // Child (debugger)
        run_gdb(parent_pid, gdb_path);
        // Normally run_gdb will not return.
        // In case it does, kill the process.
        goto failure;
    } else {
        pid_t timeout_pid = fork();
        if (timeout_pid < 0) {
            perror("spawn timeout fork: ");
            kill(worker_pid, SIGKILL);
            goto failure;
        }

        if (timeout_pid == 0) {
            toku_os_sleep(5);  // Timeout of 5 seconds
            goto success;
        } else {
            pid_t exited_pid = wait(NULL);  // Wait for first child to exit
            if (exited_pid == worker_pid) {
                // Kill slower child
                kill(timeout_pid, SIGKILL);
                goto success;
            } else if (exited_pid == timeout_pid) {
                // Kill slower child
                kill(worker_pid, SIGKILL);
                goto failure;  // Timed out.
            } else {
                perror("error while waiting for gdb or timer to end: ");
                //Some failure.  Kill everything.
                kill(timeout_pid, SIGKILL);
                kill(worker_pid, SIGKILL);
                goto failure;
            }
        }
    }
success:
    _exit(EXIT_SUCCESS);
failure:
    _exit(EXIT_FAILURE);
}

static void
spawn_gdb(const char *gdb_path) {
    pid_t parent_pid = toku_os_getpid();
#if defined(HAVE_SYS_PRCTL_H)
    // On systems that require permission for the same user to ptrace,
    // give permission for this process and (more importantly) all its children to debug this process.
    prctl(PR_SET_PTRACER, parent_pid, 0, 0, 0);
#endif
    fprintf(stderr, "Attempting to use gdb @[%s] on pid[%d]\n", gdb_path, parent_pid);
    fflush(stderr);
    int intermediate_pid = fork();
    if (intermediate_pid < 0) {
        perror("spawn_gdb intermediate process fork: ");
    } else if (intermediate_pid == 0) {
        intermediate_process(parent_pid, gdb_path);
    } else {
        waitpid(intermediate_pid, NULL, 0);
    }
}

void
toku_try_gdb_stack_trace(const char *gdb_path) {
    char default_gdb_path[] = "/usr/bin/gdb";
    static bool started = false;
    if (RUNNING_ON_VALGRIND) {
        fprintf(stderr, "gdb stack trace skipped due to running under valgrind\n");
        fflush(stderr);
    } else if (toku_sync_bool_compare_and_swap(&started, false, true)) {
        spawn_gdb(gdb_path ? gdb_path : default_gdb_path);
    }
}

#endif // !TOKU_WINDOWS
