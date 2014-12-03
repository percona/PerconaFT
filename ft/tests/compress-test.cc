/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
// Test quicklz.
// Compare to compress-test which tests the toku compression (which is a composite of quicklz and zlib).
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

#ident "Copyright (c) 2010-2013 Tokutek Inc.  All rights reserved."
#ident "$Id$"

#include <sys/time.h>
#include "test.h"
#include "serialize/compress.h"

static float tdiff (struct timeval *start, struct timeval *end) {
    return (end->tv_sec-start->tv_sec) + 1e-6*(end->tv_usec - start->tv_usec);
}

static uLongf test_compress_buf_method (unsigned char *buf, int i, enum toku_compression_method m) {
    int bound = toku_compress_bound(m, i);
    unsigned char *MALLOC_N(bound, cb);
    uLongf actual_clen = bound;
    toku_compress(m, cb, &actual_clen, buf, i);
    unsigned char *MALLOC_N(i, ubuf);
    toku_decompress(ubuf, i, cb, actual_clen);
    assert(0==memcmp(ubuf, buf, i));
    toku_free(ubuf);
    toku_free(cb);
    return actual_clen;
}

static void test_compress_i (int i, enum toku_compression_method m, uLongf *compress_size, uLongf *uncompress_size) {
    unsigned char *MALLOC_N(i, b);
    for (int j=0; j<i; j++) b[j] = random()%256;
    *compress_size += test_compress_buf_method (b, i, m);
    *uncompress_size += i;

    for (int j=0; j<i; j++) b[j] = 0;
    *compress_size += test_compress_buf_method (b, i, m);
    *uncompress_size += i;

    for (int j=0; j<i; j++) b[j] = 0xFF;
    *compress_size += test_compress_buf_method(b, i, m);
    *uncompress_size += i;

    toku_free(b);
}

static void test_compress (enum toku_compression_method m, uLongf *compress_size, uLongf *uncompress_size) {
    // unlike quicklz, we can handle length 0.
    for (int i=0; i<100; i++) {
        test_compress_i(i, m, compress_size, uncompress_size);
    }
    test_compress_i(1024, m, compress_size, uncompress_size);
    test_compress_i(1024*1024*4, m, compress_size, uncompress_size);
    test_compress_i(1024*1024*4 - 123, m, compress_size, uncompress_size); // just some random lengths
}

static void test_compress_methods () {
    struct timeval start, end;
    uLongf compress_size = 0;
    uLongf uncompress_size = 0;

    gettimeofday(&start, NULL);
    test_compress(TOKU_ZLIB_METHOD, &compress_size, &uncompress_size);
    gettimeofday(&end, NULL);
    printf("TOKU_ZLIB_METHOD Time=%.6fs , Ratio=%.2f[%d/%d]\n",
            tdiff(&start, &end),
            (float)compress_size / (float)uncompress_size, (int)compress_size, (int)uncompress_size);

    compress_size = 0;
    uncompress_size = 0;
    gettimeofday(&start, NULL);
    test_compress(TOKU_ZLIB_WITHOUT_CHECKSUM_METHOD, &compress_size, &uncompress_size);
    gettimeofday(&end, NULL);
    printf("TOKU_ZLIB_WITHOUT_CHECKSUM_METHOD Time=%.6fs, Ratio=%.2f[%d/%d]\n",
            tdiff(&start, &end),
            (float)compress_size / (float)uncompress_size, (int)compress_size, (int)uncompress_size);

    compress_size = 0;
    uncompress_size = 0;
    gettimeofday(&start, NULL);
    test_compress(TOKU_QUICKLZ_METHOD, &compress_size, &uncompress_size);
    gettimeofday(&end, NULL);
    printf("TOKU_QUICKLZ_METHOD Time=%.6fs, Ratio=%.2f[%d/%d]\n",
            tdiff(&start, &end),
            (float)compress_size / (float)uncompress_size, (int)compress_size, (int)uncompress_size);

    compress_size = 0;
    uncompress_size = 0;
    gettimeofday(&start, NULL);
    test_compress(TOKU_LZMA_METHOD, &compress_size, &uncompress_size);
    gettimeofday(&end, NULL);
    printf("TOKU_LZMA_METHOD Time=%.6fs, Ratio=%.2f[%d/%d]\n",
            tdiff(&start, &end),
            (float)compress_size / (float)uncompress_size, (int)compress_size, (int)uncompress_size);

    compress_size = 0;
    uncompress_size = 0;
    gettimeofday(&start, NULL);
    test_compress(TOKU_SNAPPY_METHOD, &compress_size, &uncompress_size);
    gettimeofday(&end, NULL);
    printf("TOKU_SNAPPY_METHOD Time=%.6fs, Ratio=%.2f[%d/%d]\n",
            tdiff(&start, &end),
            (float)compress_size / (float)uncompress_size, (int)compress_size, (int)uncompress_size);
}

int test_main (int argc, const char *argv[]) {
    default_parse_args(argc, argv);
    
    test_compress_methods();

    return 0;
}
