/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <algorithm>
#include <cstdint>

#include <db.h>

#include "buffer.hpp"
#include "exceptions.hpp"
#include "slice.hpp"

namespace ftcxx {

    template<class Comparator, class Predicate, class Handler>
    Cursor::Iterator<Comparator, Predicate, Handler>::Iterator(Cursor &cur, DBT *left, DBT *right,
                                                               Comparator cmp, Predicate filter, Handler handler,
                                                               bool forward, bool end_exclusive, bool prelock)
        : _cur(cur),
          _left(Slice(*left).owned()),
          _right(Slice(*right).owned()),
          _cmp(cmp),
          _filter(filter),
          _handler(handler),
          _forward(forward),
          _end_exclusive(end_exclusive),
          _prelock(prelock),
          _finished(false)
    {
        init();
    }

    template<class Comparator, class Predicate, class Handler>
    Cursor::Iterator<Comparator, Predicate, Handler>::Iterator(Cursor &cur, const Slice &left, const Slice &right,
                                                               Comparator cmp, Predicate filter, Handler handler,
                                                               bool forward, bool end_exclusive, bool prelock)
        : _cur(cur),
          _left(left.owned()),
          _right(right.owned()),
          _cmp(cmp),
          _filter(filter),
          _handler(handler),
          _forward(forward),
          _end_exclusive(end_exclusive),
          _prelock(prelock),
          _finished(false)
    {
        init();
    }

    template<class Comparator, class Predicate, class Handler>
    void Cursor::Iterator<Comparator, Predicate, Handler>::init() {
        DBT left_dbt = _left.dbt();
        DBT right_dbt = _right.dbt();
        int r = _cur.dbc()->c_set_bounds(_cur.dbc(), &left_dbt, &right_dbt, _prelock, 0);
        handle_ft_retval(r);

        if (_forward) {
            r = _cur.dbc()->c_getf_set_range(_cur.dbc(), getf_flags(), &left_dbt, getf_callback, this);
        } else {
            r = _cur.dbc()->c_getf_set_range_reverse(_cur.dbc(), getf_flags(), &right_dbt, getf_callback, this);
        }
        if (r != 0 && r != DB_NOTFOUND && r != -1) {
            handle_ft_retval(r);
        }
    }

    template <class Comparator, class Predicate, class Handler>
    int Cursor::Iterator<Comparator, Predicate, Handler>::getf(const DBT *key, const DBT *val) {
        int c;
        if (_forward) {
            c = _cmp(Slice(*key), _right);
        } else {
            c = _cmp(_left, Slice(*key));
        }
        if (c > 0 || (c == 0 && _end_exclusive)) {
            _finished = true;
            return -1;
        }

        if (_filter(key, val)) {
            if (!_handler(key, val)) {
                return 0;
            }
        }

        return TOKUDB_CURSOR_CONTINUE;
    }

    template <class Comparator, class Predicate, class Handler>
    bool Cursor::Iterator<Comparator, Predicate, Handler>::consume_batch() {
        int r;
        if (_forward) {
            r = _cur.dbc()->c_getf_next(_cur.dbc(), getf_flags(), getf_callback, this);
        } else {
            r = _cur.dbc()->c_getf_prev(_cur.dbc(), getf_flags(), getf_callback, this);
        }
        if (r != 0 && r != DB_NOTFOUND && r != -1) {
            handle_ft_retval(r);
        }

        return !_finished;
    }

    template<class Comparator, class Predicate>
    Cursor::BufferedIterator<Comparator, Predicate>::BufferedIterator(Cursor &cur, DBT *left, DBT *right,
                                                                      Comparator cmp, Predicate filter,
                                                                      bool forward, bool end_exclusive, bool prelock)
        : _buf(),
          _appender(_buf),
          _iter(cur, left, right,
                cmp, filter, _appender,
                forward, end_exclusive, prelock)
    {}

    template<class Comparator, class Predicate>
    Cursor::BufferedIterator<Comparator, Predicate>::BufferedIterator(Cursor &cur, const Slice &left, const Slice &right,
                                                                      Comparator cmp, Predicate filter,
                                                                      bool forward, bool end_exclusive, bool prelock)
        : _buf(),
          _appender(_buf),
          _iter(cur, left, right,
                cmp, filter, _appender,
                forward, end_exclusive, prelock)
    {}

    template<class Comparator, class Predicate>
    void Cursor::BufferedIterator<Comparator, Predicate>::BufferAppender::marshall(char *dest, const DBT *key, const DBT *val) {
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
    void Cursor::BufferedIterator<Comparator, Predicate>::BufferAppender::unmarshall(char *src, DBT *key, DBT *val) {
        const uint32_t *keylen = reinterpret_cast<uint32_t *>(&src[0]);
        const uint32_t *vallen = reinterpret_cast<uint32_t *>(&src[sizeof *keylen]);
        key->size = *keylen;
        val->size = *vallen;
        char *p = &src[(sizeof *keylen) + (sizeof *vallen)];
        key->data = p;
        val->data = p + key->size;
    }

    template<class Comparator, class Predicate>
    void Cursor::BufferedIterator<Comparator, Predicate>::BufferAppender::unmarshall(char *src, Slice &key, Slice &val) {
        const uint32_t *keylen = reinterpret_cast<uint32_t *>(&src[0]);
        const uint32_t *vallen = reinterpret_cast<uint32_t *>(&src[sizeof *keylen]);
        char *p = &src[(sizeof *keylen) + (sizeof *vallen)];
        key = Slice(p, *keylen);
        val = Slice(p + *keylen, *vallen);
    }

    template<class Comparator, class Predicate>
    bool Cursor::BufferedIterator<Comparator, Predicate>::BufferAppender::operator()(const DBT *key, const DBT *val) {
        size_t needed = marshalled_size(key->size, val->size);
        char *dest = _buf.alloc(needed);
        marshall(dest, key, val);
        return !_buf.full();
    }

    template<class Comparator, class Predicate>
    bool Cursor::BufferedIterator<Comparator, Predicate>::next(DBT *key, DBT *val) {
        if (!_buf.more() && !_iter.finished()) {
            _buf.clear();
            _iter.consume_batch();
        }

        if (!_buf.more()) {
            return false;
        }

        char *src = _buf.current();
        BufferAppender::unmarshall(src, key, val);
        _buf.advance(BufferAppender::marshalled_size(key->size, val->size));
        return true;
    }

    template<class Comparator, class Predicate>
    bool Cursor::BufferedIterator<Comparator, Predicate>::next(Slice &key, Slice &val) {
        if (!_buf.more() && !_iter.finished()) {
            _buf.clear();
            _iter.consume_batch();
        }

        if (!_buf.more()) {
            return false;
        }

        char *src = _buf.current();
        BufferAppender::unmarshall(src, key, val);
        _buf.advance(BufferAppender::marshalled_size(key.size(), val.size()));
        return true;
    }

} // namespace ftcxx
