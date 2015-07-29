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

// Test quicklz.
// Compare to compress-test which tests the toku compression (which is a composite of quicklz and zlib).

#include "test.h"
#include "serialize/compress.h"

static void test_compress_buf_method (unsigned char *buf, int i, enum toku_compression_method m) {
    int bound = toku_compress_bound(m, i);
    unsigned char *MALLOC_N(bound, cb);
    uLongf actual_clen = bound;
    toku_compress(m, cb, &actual_clen, buf, i);
    unsigned char *MALLOC_N(i, ubuf);
    toku_decompress(ubuf, i, cb, actual_clen);
    assert(0==memcmp(ubuf, buf, i));
    toku_free(ubuf);
    toku_free(cb);
}

static void test_compress_buf (unsigned char *buf, int i) {
    test_compress_buf_method(buf, i, TOKU_ZLIB_METHOD);
    test_compress_buf_method(buf, i, TOKU_ZLIB_WITHOUT_CHECKSUM_METHOD);
    test_compress_buf_method(buf, i, TOKU_QUICKLZ_METHOD);
    test_compress_buf_method(buf, i, TOKU_LZMA_METHOD);
}

static void test_compress_i (int i) {
    unsigned char *MALLOC_N(i, b);
    for (int j=0; j<i; j++) b[j] = random()%256;
    test_compress_buf (b, i);
    for (int j=0; j<i; j++) b[j] = 0;
    test_compress_buf (b, i);
    for (int j=0; j<i; j++) b[j] = 0xFF;
    test_compress_buf (b, i);
    toku_free(b);
}

static void test_compress (void) {
    // unlike quicklz, we can handle length 0.
    for (int i=0; i<100; i++) {
	test_compress_i(i);
    }
    test_compress_i(1024);
    test_compress_i(1024*1024*4);
    test_compress_i(1024*1024*4 - 123); // just some random lengths
}

int test_main (int argc, const char *argv[]) {
    default_parse_args(argc, argv);
    
    test_compress();

    return 0;
}

