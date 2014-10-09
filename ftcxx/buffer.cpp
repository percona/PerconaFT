/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <memory>

#include "buffer.hpp"
#include "malloc_utils.hpp"

namespace ftcxx {

    const size_t Buffer::INITIAL_CAPACITY = 1<<10;
    const size_t Buffer::MAXIMUM_CAPACITY = 1<<18;
    const double Buffer::FULLNESS_RATIO = 0.9;

    void Buffer::init() {
        _buf.reset(std::malloc(_capacity));
    }

    /**
     * Implements our growth strategy.  Currently we double until we get
     * up to 4kB so that we can quickly reach the point where jemalloc can
     * help us resize in-place, but after that point we grow by a factor
     * of 1.5x.
     *
     * FBVector doubles once it is bigger than 128kB, but I don't think we
     * actually want to because that's about when we want to stop growing.
     */
    size_t Buffer::next_alloc_size(size_t sz) {
        if (sz < malloc_utils::jemallocMinInPlaceExpandable) {
            return sz * 2;
        }
#if 0
        else if (sz > (128<<10)) {
            return sz * 2;
        }
#endif
        else {
            return (sz * 3 + 1) / 2;
        }
    }

    void Buffer::grow(size_t sz) {
        size_t new_capacity = _capacity;
        while (new_capacity < _end + sz) {
            new_capacity = next_alloc_size(new_capacity);
        }
        assert(new_capacity >= _capacity);  // overflow?
        if (new_capacity > _capacity) {
            // This section isn't exception-safe, but smartRealloc already
            // isn't.  The only thing we can throw in here is
            // std::bad_alloc, in which case we're kind of screwed anyway.
            new_capacity = malloc_utils::goodMallocSize(new_capacity);
            _buf.reset(malloc_utils::smartRealloc(_buf.release(), new_capacity, 0, 0, _capacity));
        }
    }

} // namespace ftcxx
