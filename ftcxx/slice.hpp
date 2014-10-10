/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <cassert>
#include <memory>

#include <db.h>

namespace ftcxx {

    class Slice {
    public:
        Slice()
            : _data(nullptr),
              _size(0)
        {}

        explicit Slice(size_t sz)
            : _buf(new char[sz], std::default_delete<char[]>()),
              _data(_buf.get()),
              _size(sz)
        {}

        Slice(const char *p, size_t sz)
            : _data(p),
              _size(sz)
        {}
              

        explicit Slice(const std::string &str)
            : _data(str.c_str()),
              _size(str.size())
        {}

        Slice(const Slice &other)
            : _data(other._data),
              _size(other._size)
        {}

        Slice& operator=(const Slice &other) {
            _data = other._data;
            _size = other._size;
            return *this;
        }

        Slice(Slice&& other)
            : _data(nullptr),
              _size(0)
        {
            std::swap(_data, other._data);
            std::swap(_size, other._size);
            std::swap(_buf, other._buf);
        }

        Slice& operator=(Slice&& other) {
            std::swap(_data, other._data);
            std::swap(_size, other._size);
            std::swap(_buf, other._buf);
            return *this;
        }

        template<typename T>
        static Slice slice_of(const T &v) {
            return Slice(reinterpret_cast<const char *>(&v), sizeof v);
        }

        template<typename T>
        T as() const {
            assert(size() == sizeof(T));
            const T *p = reinterpret_cast<const T *>(_data);
            return *p;
        }

        const char *data() const { return _data; }

        char *mutable_data() const {
            assert(_buf);
            return _buf.get();
        }

        size_t size() const { return _size; }

        Slice copy() const {
            Slice s(_size);
            std::copy(_data, _data + _size, s.mutable_data());
            return s;
        }

        Slice owned() const {
            if (_buf) {
                return *this;
            } else {
                return copy();
            }
        }

        DBT dbt() const {
            DBT d;
            d.data = const_cast<void *>(static_cast<const void *>(_data));
            d.size = _size;
            d.ulen = _size;
            d.flags = 0;
            return d;
        }

    private:
        std::shared_ptr<char> _buf;
        const char *_data;
        size_t _size;
    };

} // namespace ftcxx
