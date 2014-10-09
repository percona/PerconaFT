/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <cstdlib>
#include <memory>

namespace ftcxx {

    /**
     * Buffer implements a flat memory buffer intended for FIFO usage
     * where allocations are piecemeal but consumption is total.  That is,
     * we repeatedly fill up the buffer with small allocations, and
     * periodically consume all entries and clear the buffer.
     *
     * For now, the implementation uses a doubling array strategy,
     * starting at 1kB growing to a maximum advised capacity of 256kB,
     * never shrinking the buffer.
     *
     * However, we hope to find a better strategy.
     *
     * Facebook's FBVector claims that a reallocation growth factor of 1.5
     * rather than 2 hits their sweet spot, and they claim to have
     * additional improvements by integrating with jemalloc (which we use
     * as well).
     *
     * Additionally, it may be advantageous to use some memarena-style
     * tricks like allocating a separate overflow buffer to avoid
     * memcpying when we're close to our intended maximum capacity, and
     * also to avoid wasting extra memory if we overflow our maximum
     * capacity once but never do so again.
     */
    class Buffer {
    public:

        Buffer()
            : _cur(0),
              _end(0),
              _capacity(INITIAL_CAPACITY),
              _buf(nullptr, &std::free)
        {
            init();
        }

        Buffer(size_t capacity)
            : _end(0),
              _capacity(capacity),
              _buf(nullptr, &std::free)
        {
            init();
        }

        // Producer API:

        /**
         * Allocate room for sz more bytes at the end, and return a
         * pointer to the allocated space.  This causes at most one
         * realloc and memcpy of existing data.
         */
        char *alloc(size_t sz) {
            grow(sz);
            char *p = raw(_end);
            _end += sz;
            return p;
        }

        /**
         * Returns true if we're close to our maximum capacity.  If so,
         * the producer should stop and allow the consumer to clear the
         * buffer.
         */
        bool full() const {
            return _end > MAXIMUM_CAPACITY * FULLNESS_RATIO;
        }

        // Consumer API:

        /**
         * Returns true if there are more unconsumed bytes in the buffer.
         */
        bool more() const {
            return _cur < _end;
        }

        /**
         * Returns a pointer to the next unconsumed byte in the buffer.
         */
        char *current() const {
            return raw(_cur);
        }

        /**
         * Advances the unconsumed position pointer by sz bytes.
         */
        void advance(size_t sz) const {
            _cur += sz;
        }

        /**
         * Free all allocated space.
         */
        void clear() {
            _cur = 0;
            _end = 0;
        }

    private:

        size_t _cur;
        size_t _end;
        size_t _capacity;
        std::unique_ptr<char, void (*)(void*)> _buf;

        static const size_t INITIAL_CAPACITY;
        static const size_t MAXIMUM_CAPACITY;
        static const double FULLNESS_RATIO;

        void init();

        static size_t next_alloc_size(size_t sz);

        void grow(size_t sz);

        char *raw(size_t i=0) const {
            return &(_buf.get()[i]);
        }
    };

} // namespace ftcxx
