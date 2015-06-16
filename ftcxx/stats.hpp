/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

namespace ftcxx {

    struct Stats {
        Stats() : data_size(0), file_size(0), num_keys(0) {};
        size_t data_size;
        size_t file_size;
        size_t num_keys;
    };
}
