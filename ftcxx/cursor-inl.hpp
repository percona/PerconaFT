/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <algorithm>
#include <cstdint>

#include <db.h>

#include "buffer.hpp"
#include "db.hpp"
#include "db_txn.hpp"
#include "exceptions.hpp"
#include "slice.hpp"

namespace ftcxx {

    template<class Comparator, class Handler>
    RangeCursor<Comparator, Handler>::RangeCursor(const DB &db, const DBTxn &txn, int flags,
                                                  DBT *left, DBT *right,
                                                  Comparator cmp, Handler handler,
                                                  bool forward, bool end_exclusive, bool prelock)
        : _dbc(db, txn, flags),
          _left(Slice(*left).owned()),
          _right(Slice(*right).owned()),
          _cmp(cmp),
          _handler(handler),
          _forward(forward),
          _end_exclusive(end_exclusive),
          _prelock(prelock),
          _finished(false)
    {
        init();
    }

    template<class Comparator, class Handler>
    RangeCursor<Comparator, Handler>::RangeCursor(const DB &db, const DBTxn &txn, int flags,
                                                  const Slice &left, const Slice &right,
                                                  Comparator cmp, Handler handler,
                                                  bool forward, bool end_exclusive, bool prelock)
        : _dbc(db, txn, flags),
          _left(left.owned()),
          _right(right.owned()),
          _cmp(cmp),
          _handler(handler),
          _forward(forward),
          _end_exclusive(end_exclusive),
          _prelock(prelock),
          _finished(false)
    {
        init();
    }

    template<class Comparator, class Handler>
    void RangeCursor<Comparator, Handler>::init() {
        DBT left_dbt = _left.dbt();
        DBT right_dbt = _right.dbt();
        int r = _dbc.dbc()->c_set_bounds(_dbc.dbc(), &left_dbt, &right_dbt, _prelock, 0);
        handle_ft_retval(r);

        if (_forward) {
            r = _dbc.dbc()->c_getf_set_range(_dbc.dbc(), getf_flags(), &left_dbt, getf_callback, this);
        } else {
            r = _dbc.dbc()->c_getf_set_range_reverse(_dbc.dbc(), getf_flags(), &right_dbt, getf_callback, this);
        }
        if (r != 0 && r != DB_NOTFOUND && r != -1) {
            handle_ft_retval(r);
        }
    }

    template<class Comparator, class Handler>
    int RangeCursor<Comparator, Handler>::getf(const DBT *key, const DBT *val) {
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

        if (!_handler(key, val)) {
            return 0;
        }

        return TOKUDB_CURSOR_CONTINUE;
    }

    template<class Comparator, class Handler>
    bool RangeCursor<Comparator, Handler>::consume_batch() {
        int r;
        if (_forward) {
            r = _dbc.dbc()->c_getf_next(_dbc.dbc(), getf_flags(), getf_callback, this);
        } else {
            r = _dbc.dbc()->c_getf_prev(_dbc.dbc(), getf_flags(), getf_callback, this);
        }
        if (r == DB_NOTFOUND) {
            _finished = true;
        } else if (r != 0 && r != -1) {
            handle_ft_retval(r);
        }

        return !_finished;
    }

    template<class Handler>
    ScanCursor<Handler>::ScanCursor(const DB &db, const DBTxn &txn, int flags,
                                    Handler handler, bool forward, bool prelock)
        : _dbc(db, txn, flags),
          _handler(handler),
          _forward(forward),
          _prelock(prelock),
          _finished(false)
    {
        init();
    }

    template<class Handler>
    void ScanCursor<Handler>::init() {
        int r = _dbc.dbc()->c_set_bounds(_dbc.dbc(),
                                         _dbc.dbc()->dbp->dbt_neg_infty(),
                                         _dbc.dbc()->dbp->dbt_pos_infty(),
                                         _prelock, 0);
        handle_ft_retval(r);

        if (_forward) {
            r = _dbc.dbc()->c_getf_first(_dbc.dbc(), getf_flags(), getf_callback, this);
        } else {
            r = _dbc.dbc()->c_getf_last(_dbc.dbc(), getf_flags(), getf_callback, this);
        }
        if (r != 0 && r != DB_NOTFOUND && r != -1) {
            handle_ft_retval(r);
        }
    }

    template<class Handler>
    int ScanCursor<Handler>::getf(const DBT *key, const DBT *val) {
        if (!_handler(key, val)) {
            return 0;
        }

        return TOKUDB_CURSOR_CONTINUE;
    }

    template<class Handler>
    bool ScanCursor<Handler>::consume_batch() {
        int r;
        if (_forward) {
            r = _dbc.dbc()->c_getf_next(_dbc.dbc(), getf_flags(), getf_callback, this);
        } else {
            r = _dbc.dbc()->c_getf_prev(_dbc.dbc(), getf_flags(), getf_callback, this);
        }
        if (r == DB_NOTFOUND) {
            _finished = true;
        } else if (r != 0 && r != -1) {
            handle_ft_retval(r);
        }

        return !_finished;
    }

    template<class Predicate>
    inline void BufferAppender<Predicate>::marshall(char *dest, const DBT *key, const DBT *val) {
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

    template<class Predicate>
    inline void BufferAppender<Predicate>::unmarshall(char *src, DBT *key, DBT *val) {
        const uint32_t *keylen = reinterpret_cast<uint32_t *>(&src[0]);
        const uint32_t *vallen = reinterpret_cast<uint32_t *>(&src[sizeof *keylen]);
        key->size = *keylen;
        val->size = *vallen;
        char *p = &src[(sizeof *keylen) + (sizeof *vallen)];
        key->data = p;
        val->data = p + key->size;
    }

    template<class Predicate>
    inline void BufferAppender<Predicate>::unmarshall(char *src, Slice &key, Slice &val) {
        const uint32_t *keylen = reinterpret_cast<uint32_t *>(&src[0]);
        const uint32_t *vallen = reinterpret_cast<uint32_t *>(&src[sizeof *keylen]);
        char *p = &src[(sizeof *keylen) + (sizeof *vallen)];
        key = Slice(p, *keylen);
        val = Slice(p + *keylen, *vallen);
    }

    template<class Predicate>
    inline bool BufferAppender<Predicate>::operator()(const DBT *key, const DBT *val) {
        if (_filter(Slice(*key), Slice(*val))) {
            size_t needed = marshalled_size(key->size, val->size);
            char *dest = _buf.alloc(needed);
            marshall(dest, key, val);
        }
        return !_buf.full();
    }

    template<class Comparator, class Predicate>
    BufferedRangeCursor<Comparator, Predicate>::BufferedRangeCursor(const DB &db, const DBTxn &txn, int flags,
                                                                    DBT *left, DBT *right,
                                                                    Comparator cmp, Predicate filter,
                                                                    bool forward, bool end_exclusive, bool prelock)
        : _buf(),
          _appender(_buf),
          _cur(new RangeCursor<Comparator, BufferAppender<Predicate> >(db, txn, flags,
                                                                       left, right,
                                                                       cmp, _appender,
                                                                       forward, end_exclusive, prelock))
    {}

    template<class Comparator, class Predicate>
    BufferedRangeCursor<Comparator, Predicate>::BufferedRangeCursor(const DB &db, const DBTxn &txn, int flags,
                                                                    const Slice &left, const Slice &right,
                                                                    Comparator cmp, Predicate filter,
                                                                    bool forward, bool end_exclusive, bool prelock)
        : _buf(),
          _appender(_buf, filter),
          _cur(new RangeCursor<Comparator, BufferAppender<Predicate> >(db, txn, flags,
                                                                       left, right,
                                                                       cmp, _appender,
                                                                       forward, end_exclusive, prelock))
    {}

    template<class Comparator, class Predicate>
    bool BufferedRangeCursor<Comparator, Predicate>::next(DBT *key, DBT *val) {
        if (!_buf.more() && !_cur->finished()) {
            _buf.clear();
            _cur->consume_batch();
        }

        if (!_buf.more()) {
            return false;
        }

        char *src = _buf.current();
        BufferAppender<Predicate>::unmarshall(src, key, val);
        _buf.advance(BufferAppender<Predicate>::marshalled_size(key->size, val->size));
        return true;
    }

    template<class Comparator, class Predicate>
    bool BufferedRangeCursor<Comparator, Predicate>::next(Slice &key, Slice &val) {
        if (!_buf.more() && !_cur->finished()) {
            _buf.clear();
            _cur->consume_batch();
        }

        if (!_buf.more()) {
            return false;
        }

        char *src = _buf.current();
        BufferAppender<Predicate>::unmarshall(src, key, val);
        _buf.advance(BufferAppender<Predicate>::marshalled_size(key.size(), val.size()));
        return true;
    }

    template<class Predicate>
    BufferedScanCursor<Predicate>::BufferedScanCursor(const DB &db, const DBTxn &txn, int flags,
                                                      Predicate filter, bool forward, bool prelock)
        : _buf(),
          _appender(_buf, filter),
          _cur(new ScanCursor<BufferAppender<Predicate>>(db, txn, flags,
                                                         _appender,
                                                         forward, prelock))
    {}

    template<class Predicate>
    bool BufferedScanCursor<Predicate>::next(DBT *key, DBT *val) {
        if (!_buf.more() && !_cur->finished()) {
            _buf.clear();
            _cur->consume_batch();
        }

        if (!_buf.more()) {
            return false;
        }

        char *src = _buf.current();
        BufferAppender<Predicate>::unmarshall(src, key, val);
        _buf.advance(BufferAppender<Predicate>::marshalled_size(key->size, val->size));
        return true;
    }

    template<class Predicate>
    bool BufferedScanCursor<Predicate>::next(Slice &key, Slice &val) {
        if (!_buf.more() && !_cur->finished()) {
            _buf.clear();
            _cur->consume_batch();
        }

        if (!_buf.more()) {
            return false;
        }

        char *src = _buf.current();
        BufferAppender<Predicate>::unmarshall(src, key, val);
        _buf.advance(BufferAppender<Predicate>::marshalled_size(key.size(), val.size()));
        return true;
    }

} // namespace ftcxx
