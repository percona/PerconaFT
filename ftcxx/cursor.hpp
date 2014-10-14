/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <db.h>

#include "buffer.hpp"
#include "db.hpp"
#include "db_env.hpp"
#include "db_txn.hpp"
#include "slice.hpp"

namespace ftcxx {

    /**
     * DBC is a simple RAII wrapper around a DBC object.
     */
    class DBC {
    public:
        DBC(const DB &db, const DBTxn &txn=DBTxn(), int flags=0);
        ~DBC();

        ::DBC *dbc() const { return _dbc; }

        void close();

    protected:

        ::DBC *_dbc;
    };

    /**
     * Cursor supports iterating a cursor over a key range,
     * with bulk fetch buffering, and optional filtering.
     */
    template<class Comparator, class Predicate, class Handler>
    class Cursor {
    public:

        /**
         * Constructs an cursor.  Better to use DB::cursor instead to
         * avoid template parameters.
         */
        Cursor(const DB &db, const DBTxn &txn, int flags,
               DBT *left, DBT *right,
               Comparator cmp, Predicate filter, Handler handler,
               bool forward, bool end_exclusive, bool prelock);

        Cursor(const DB &db, const DBTxn &txn, int flags,
               const Slice &left, const Slice &right,
               Comparator cmp, Predicate filter, Handler handler,
               bool forward, bool end_exclusive, bool prelock);

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
        Slice _left;
        Slice _right;
        bool _left_is_infinity;
        bool _right_is_infinity;
        Comparator &_cmp;
        Predicate &_filter;
        Handler &_handler;

        const bool _forward;
        const bool _end_exclusive;
        const bool _prelock;
        bool _finished;

        void init();

        static int getf_callback(const DBT *key, const DBT *val, void *extra) {
            Cursor *i = static_cast<Cursor *>(extra);
            return i->getf(key, val);
        }

        int getf_flags() const {
            if (_prelock) {
                return DB_PRELOCKED | DB_PRELOCKED_WRITE;
            } else {
                return DBC_DISABLE_PREFETCHING;
            }
        }

        int getf(const DBT *key, const DBT *val);
    };

    /**
     * Cursor supports iterating a cursor over a key range,
     * with bulk fetch buffering, and optional filtering.
     */
    template<class Predicate, class Handler>
    class ScanCursor {
    public:

        /**
         * Constructs an cursor.  Better to use DB::cursor instead to
         * avoid template parameters.
         */
        ScanCursor(const DB &db, const DBTxn &txn, int flags,
                   Predicate filter, Handler handler,
                   bool forward, bool prelock);

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
        Predicate &_filter;
        Handler &_handler;

        const bool _forward;
        const bool _prelock;
        bool _finished;

        void init();

        static int getf_callback(const DBT *key, const DBT *val, void *extra) {
            ScanCursor *i = static_cast<ScanCursor *>(extra);
            return i->getf(key, val);
        }

        int getf_flags() const {
            if (_prelock) {
                return DB_PRELOCKED | DB_PRELOCKED_WRITE;
            } else {
                return DBC_DISABLE_PREFETCHING;
            }
        }

        int getf(const DBT *key, const DBT *val);
    };

    class BufferAppender {
        Buffer &_buf;

    public:
        BufferAppender(Buffer &buf)
            : _buf(buf)
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
                       DBT *left, DBT *right,
                       Comparator cmp, Predicate filter,
                       bool forward, bool end_exclusive, bool prelock);

        BufferedCursor(const DB &db, const DBTxn &txn, int flags,
                       const Slice &left, const Slice &right,
                       Comparator cmp, Predicate filter,
                       bool forward, bool end_exclusive, bool prelock);

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

        Buffer _buf;
        BufferAppender _appender;
        Cursor<Comparator, Predicate, BufferAppender> _cur;
    };

    template<class Predicate>
    class BufferedScanCursor {
    public:

        /**
         * Constructs an buffered cursor.  Better to use
         * DB::buffered_cursor instead to avoid template parameters.
         */
        BufferedScanCursor(const DB &db, const DBTxn &txn, int flags,
                           Predicate filter, bool forward, bool prelock);

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

        Buffer _buf;
        BufferAppender _appender;
        ScanCursor<Predicate, BufferAppender> _cur;
    };

    struct NoFilter {
        bool operator()(const DBT *, const DBT *) const { return true; }
    };

} // namespace ftcxx

#include "cursor-inl.hpp"
