/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <utility>

#include <db.h>

#include "buffer.hpp"
#include "db.hpp"
#include "db_env.hpp"
#include "db_txn.hpp"
#include "slice.hpp"

namespace ftcxx {

    class DB;

    struct IterationStrategy {
        bool forward;
        bool prelock;

        IterationStrategy(bool forward_, bool prelock_)
            : forward(forward_),
              prelock(prelock_)
        {}

        int getf_flags() const {
            if (prelock) {
                return DB_PRELOCKED | DB_PRELOCKED_WRITE;
            } else {
                return DBC_DISABLE_PREFETCHING;
            }
        }
    };

    class Bounds {
        const DB *_db;
        Slice _left;
        Slice _right;
        DBT _left_dbt;
        DBT _right_dbt;
        bool _left_infinite;
        bool _right_infinite;
        bool _end_exclusive;

    public:
        Bounds(const DB *db, const Slice &left, const Slice &right, bool end_exclusive)
            : _db(db),
              _left(left.owned()),
              _right(right.owned()),
              _left_dbt(_left.dbt()),
              _right_dbt(_right.dbt()),
              _left_infinite(false),
              _right_infinite(false),
              _end_exclusive(end_exclusive)
        {}

        struct Infinite {};

        Bounds(const DB *db, Infinite, const Slice &right, bool end_exclusive)
            : _db(db),
              _left(),
              _right(right.owned()),
              _left_dbt(_left.dbt()),
              _right_dbt(_right.dbt()),
              _left_infinite(true),
              _right_infinite(false),
              _end_exclusive(end_exclusive)
        {}

        Bounds(const DB *db, const Slice &left, Infinite, bool end_exclusive)
            : _db(db),
              _left(left.owned()),
              _right(),
              _left_dbt(_left.dbt()),
              _right_dbt(_right.dbt()),
              _left_infinite(false),
              _right_infinite(true),
              _end_exclusive(end_exclusive)
        {}

        Bounds(const DB *db, Infinite, Infinite, bool end_exclusive)
            : _db(db),
              _left(),
              _right(),
              _left_dbt(_left.dbt()),
              _right_dbt(_right.dbt()),
              _left_infinite(true),
              _right_infinite(true),
              _end_exclusive(end_exclusive)
        {}

        Bounds(const Bounds &other) = delete;
        Bounds& operator=(const Bounds &) = delete;

        Bounds(Bounds&& other)
            : _db(other._db),
              _left(std::move(other._left)),
              _right(std::move(other._right)),
              _left_dbt(_left.dbt()),
              _right_dbt(_right.dbt()),
              _left_infinite(other._left_infinite),
              _right_infinite(other._right_infinite),
              _end_exclusive(other._end_exclusive)
        {}

        Bounds& operator=(Bounds&& other) {
            _db = other._db;
            std::swap(_left, other._left);
            std::swap(_right, other._right);
            _left_dbt = _left.dbt();
            _right_dbt = _right.dbt();
            _left_infinite = other._left_infinite;
            _right_infinite = other._right_infinite;
            _end_exclusive = other._end_exclusive;
            return *this;
        }

        const DBT *left_dbt() const {
            if (_left_infinite) {
                return _db->db()->dbt_neg_infty();
            } else {
                return &_left_dbt;
            }
        }

        const DBT *right_dbt() const {
            if (_right_infinite) {
                return _db->db()->dbt_pos_infty();
            } else {
                return &_right_dbt;
            }
        }

        bool left_infinite() const { return _left_infinite; }
        bool right_infinite() const { return _right_infinite; }

        template<class Comparator>
        bool check(Comparator &cmp, const IterationStrategy &strategy, const Slice &key) const;
    };

    /**
     * DBC is a simple RAII wrapper around a DBC object.
     */
    class DBC {
    public:
        DBC(const DB &db, const DBTxn &txn=DBTxn(), int flags=0);
        ~DBC();

        ::DBC *dbc() const { return _dbc; }

        void close();

        bool set_range(const IterationStrategy &strategy, const Bounds &bounds, YDB_CALLBACK_FUNCTION callback, void *extra) const;

        bool advance(const IterationStrategy &strategy, YDB_CALLBACK_FUNCTION callback, void *extra) const;

    protected:

        ::DBC *_dbc;
    };

    /**
     * Cursor supports iterating a cursor over a key range,
     * with bulk fetch buffering, and optional filtering.
     */
    template<class Comparator, class Handler>
    class Cursor {
    public:

        /**
         * Constructs an cursor.  Better to use DB::cursor instead to
         * avoid template parameters.
         */
        Cursor(const DB &db, const DBTxn &txn, int flags,
               IterationStrategy iteration_strategy,
               Bounds bounds,
               Comparator &&cmp, Handler &&handler);

        /**
         * Gets the next key/val pair in the iteration.  Returns true
         * if there is more data, and fills in key and val.  If the
         * range is exhausted, returns false.
         */
        bool consume_batch();

        bool finished() const { return _finished; }

        bool ok() const { return !finished(); }

    private:

        DBC _dbc;
        IterationStrategy _iteration_strategy;
        Bounds _bounds;
        Comparator _cmp;
        Handler _handler;

        bool _finished;

        void init();

        static int getf_callback(const DBT *key, const DBT *val, void *extra) {
            Cursor *i = static_cast<Cursor *>(extra);
            return i->getf(key, val);
        }

        int getf(const DBT *key, const DBT *val);
    };

    template<class Predicate>
    class BufferAppender {
        Buffer &_buf;
        Predicate _filter;

    public:
        BufferAppender(Buffer &buf, Predicate &&filter)
            : _buf(buf),
              _filter(std::forward<Predicate>(filter))
        {}

        bool operator()(const DBT *key, const DBT *val);

        static size_t marshalled_size(size_t keylen, size_t vallen) {
            return (sizeof(((DBT *)0)->size)) + (sizeof(((DBT *)0)->size)) + keylen + vallen;
        }

        static void marshall(char *dest, const DBT *key, const DBT *val);

        static void unmarshall(char *src, DBT *key, DBT *val);
        static void unmarshall(char *src, Slice &key, Slice &val);
    };

    template<class Comparator, class Predicate>
    class BufferedCursor {
    public:

        /**
         * Constructs an buffered cursor.  Better to use
         * DB::buffered_cursor instead to avoid template parameters.
         */
        BufferedCursor(const DB &db, const DBTxn &txn, int flags,
                       IterationStrategy iteration_strategy,
                       Bounds bounds,
                       Comparator &&cmp, Predicate &&filter);

        /**
         * Gets the next key/val pair in the iteration.  Returns true
         * if there is more data, and fills in key and val.  If the
         * range is exhausted, returns false.
         */
        bool next(DBT *key, DBT *val);
        bool next(Slice &key, Slice &val);

        bool ok() const {
            return _cur.ok() || _buf.more();
        }

    private:

        typedef BufferAppender<Predicate> Appender;

        Buffer _buf;
        Cursor<Comparator, Appender> _cur;
    };

} // namespace ftcxx

#include "cursor-inl.hpp"
