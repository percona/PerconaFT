/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

namespace ftcxx {

    struct Stats {
        Stats() : dataSize(0), fileSize(0), numberOfKeys(0) {};
        size_t dataSize;
        size_t fileSize;
        size_t numberOfKeys;
    };
}
