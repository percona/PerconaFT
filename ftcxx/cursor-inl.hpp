/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <algorithm>
#include <cstdint>

#include <db.h>

#include "buffer.hpp"
#include "exceptions.hpp"

namespace ftcxx {

    template<class Comparator, class Predicate>
    Cursor::Iterator<Comparator, Predicate>::Iterator(Cursor &cur, DBT *left, DBT *right,
                                                      Comparator cmp, Predicate filter,
                                                      bool forward, bool end_exclusive, bool prelock)
        : _cur(cur),
          _left(left),
          _right(right),
          _cmp(cmp),
          _filter(filter),
          _forward(forward),
          _end_exclusive(end_exclusive),
          _prelock(prelock),
          _finished(false)
    {
        int r = _cur.dbc()->c_set_bounds(_cur.dbc(), left, right, _prelock, 0);
        handle_ft_retval(r);

        if (_forward) {
            r = _cur.dbc()->c_getf_set_range(_cur.dbc(), getf_flags(), left, getf_callback, this);
        } else {
            r = _cur.dbc()->c_getf_set_range_reverse(_cur.dbc(), getf_flags(), right, getf_callback, this);
        }
        if (r != 0 && r != DB_NOTFOUND && r != -1) {
            handle_ft_retval(r);
        }
    }

    template<class Comparator, class Predicate>
    void Cursor::Iterator<Comparator, Predicate>::marshall(char *dest, const DBT *key, const DBT *val) {
        uint32_t *keylen = reinterpret_cast<uint32_t *>(&dest[0]);
        uint32_t *vallen = reinterpret_cast<uint32_t *>(&dest[sizeof *keylen]);
        *keylen = key->size;
        *vallen = val->size;

        char *p = &dest[(sizeof *keylen) + (sizeof *vallen)];

        const char *kp = static_cast<char *>(key->data);
        std::copy(kp, kp + key->size, p);

        p += key->size;

        const char *vp = static_cast<char *>(val->data);
        std::copy(vp, vp + val->size, p);
    }

    template<class Comparator, class Predicate>
    void Cursor::Iterator<Comparator, Predicate>::unmarshall(char *src, DBT *key, DBT *val) {
        const uint32_t *keylen = reinterpret_cast<uint32_t *>(&src[0]);
        const uint32_t *vallen = reinterpret_cast<uint32_t *>(&src[sizeof *keylen]);
        key->size = *keylen;
        val->size = *vallen;
        char *p = &src[(sizeof *keylen) + (sizeof *vallen)];
        key->data = p;
        val->data = p + key->size;
    }

    template <class Comparator, class Predicate>
    int Cursor::Iterator<Comparator, Predicate>::getf(const DBT *key, const DBT *val) {
        int c;
        if (_forward) {
            c = _cmp(key, _right);
        } else {
            c = _cmp(_left, key);
        }
        if (c > 0 || (c == 0 && _end_exclusive)) {
            _finished = true;
            return -1;
        }

        if (_filter(key, val)) {
            size_t needed = marshalled_size(key, val);
            char *dest = _buf.alloc(needed);
            marshall(dest, key, val);
            if (_buf.full()) {
                return 0;
            }
        }

        return TOKUDB_CURSOR_CONTINUE;
    }

    template <class Comparator, class Predicate>
    bool Cursor::Iterator<Comparator, Predicate>::next(DBT *key, DBT *val) {
        if (!_buf.more() && !_finished) {
            _buf.clear();

            int r;
            if (_forward) {
                r = _cur.dbc()->c_getf_next(_cur.dbc(), getf_flags(), getf_callback, this);
            } else {
                r = _cur.dbc()->c_getf_prev(_cur.dbc(), getf_flags(), getf_callback, this);
            }
            if (r != 0 && r != DB_NOTFOUND && r != -1) {
                handle_ft_retval(r);
            }
        }

        if (!_buf.more()) {
            return false;
        }

        char *src = _buf.current();
        unmarshall(src, key, val);
        _buf.advance(marshalled_size(key, val));
        return true;
    }

} // namespace ftcxx
