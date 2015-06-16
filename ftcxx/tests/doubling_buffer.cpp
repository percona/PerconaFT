/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <memory>

#include "buffer.hpp"

namespace ftcxx {

    const size_t Buffer::INITIAL_CAPACITY = 1<<10;
    const size_t Buffer::MAXIMUM_CAPACITY = 1<<18;
    const double Buffer::FULLNESS_RATIO = 0.9;

    Buffer::Buffer()
        : _cur(0),
          _end(0),
          _capacity(INITIAL_CAPACITY),
          _buf(nullptr, &std::free)
    {
        init();
    }

    Buffer::Buffer(size_t capacity)
        : _end(0),
          _capacity(capacity),
          _buf(nullptr, &std::free)
    {
        init();
    }

    char *Buffer::alloc(size_t sz) {
        grow(sz);
        char *p = raw(_end);
        _end += sz;
        return p;
    }

    bool Buffer::full() const {
        return _end > MAXIMUM_CAPACITY * FULLNESS_RATIO;
    }

    bool Buffer::more() const {
        return _cur < _end;
    }

    char *Buffer::current() const {
        return raw(_cur);
    }

    void Buffer::advance(size_t sz) {
        _cur += sz;
    }

    void Buffer::clear() {
        _cur = 0;
        _end = 0;
    }

    void Buffer::init() {
        _buf.reset(static_cast<char *>(std::malloc(_capacity)));
    }

    size_t Buffer::next_alloc_size(size_t sz) {
        return sz * 2;
    }

    void Buffer::grow(size_t sz) {
        size_t new_capacity = _capacity;
        while (new_capacity < _end + sz) {
            new_capacity = next_alloc_size(new_capacity);
        }
        assert(new_capacity >= _capacity);  // overflow?
        if (new_capacity > _capacity) {
            std::unique_ptr<char, void (*)(void *)> new_buf(static_cast<char *>(std::malloc(new_capacity)), &std::free);
            std::copy(raw(0), raw(_end), &new_buf.get()[0]);
            std::swap(_buf, new_buf);
            _capacity = new_capacity;
        }
    }

} // namespace ftcxx
