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

    class Cursor {
    public:
        virtual ~Cursor() {}

        virtual bool consume_batch() = 0;
        virtual bool finished() const = 0;
        virtual bool ok() const = 0;
    };

    /**
     * Cursor supports iterating a cursor over a key range,
     * with bulk fetch buffering, and optional filtering.
     */
    template<class Comparator, class Handler>
    class RangeCursor : public Cursor {
    public:

        /**
         * Constructs an cursor.  Better to use DB::cursor instead to
         * avoid template parameters.
         */
        RangeCursor(const DB &db, const DBTxn &txn, int flags,
                    DBT *left, DBT *right,
                    Comparator cmp, Handler handler,
                    bool forward, bool end_exclusive, bool prelock);

        RangeCursor(const DB &db, const DBTxn &txn, int flags,
                    const Slice &left, const Slice &right,
                    Comparator cmp, Handler handler,
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
        Comparator &_cmp;
        Handler &_handler;

        const bool _forward;
        const bool _end_exclusive;
        const bool _prelock;
        bool _finished;

        void init();

        static int getf_callback(const DBT *key, const DBT *val, void *extra) {
            RangeCursor *i = static_cast<RangeCursor *>(extra);
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
    template<class Handler>
    class ScanCursor : public Cursor {
    public:

        /**
         * Constructs an cursor.  Better to use DB::cursor instead to
         * avoid template parameters.
         */
        ScanCursor(const DB &db, const DBTxn &txn, int flags,
                   Handler handler, bool forward, bool prelock);

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

    template<class Predicate>
    class BufferAppender {
        Buffer &_buf;
        Predicate &_filter;

    public:
        BufferAppender(Buffer &buf, Predicate &filter)
            : _buf(buf),
              _filter(filter)
        {}

        bool operator()(const DBT *key, const DBT *val);

        static size_t marshalled_size(size_t keylen, size_t vallen) {
            return (sizeof(((DBT *)0)->size)) + (sizeof(((DBT *)0)->size)) + keylen + vallen;
        }

        static void marshall(char *dest, const DBT *key, const DBT *val);

        static void unmarshall(char *src, DBT *key, DBT *val);
        static void unmarshall(char *src, Slice &key, Slice &val);
    };

    class BufferedCursor {
    public:
        virtual ~BufferedCursor() {}

        virtual bool next(DBT *key, DBT *val) = 0;
        virtual bool next(Slice &key, Slice &val) = 0;
        virtual bool ok() const = 0;
    };

    template<class Comparator, class Predicate>
    class BufferedRangeCursor : public BufferedCursor {
    public:

        /**
         * Constructs an buffered cursor.  Better to use
         * DB::buffered_cursor instead to avoid template parameters.
         */
        BufferedRangeCursor(const DB &db, const DBTxn &txn, int flags,
                            DBT *left, DBT *right,
                            Comparator cmp, Predicate filter,
                            bool forward, bool end_exclusive, bool prelock);

        BufferedRangeCursor(const DB &db, const DBTxn &txn, int flags,
                            const Slice &left, const Slice &right,
                            Comparator cmp, Predicate filter,
                            bool forward, bool end_exclusive, bool prelock);

        /**
         * Gets the next key/val pair in the iteration.  Returns true
         * if there is more data, and fills in key and val.  If the
         * range is exhausted, returns false.
         */
        virtual bool next(DBT *key, DBT *val);
        virtual bool next(Slice &key, Slice &val);

        virtual bool ok() const {
            return _cur->ok() || _buf.more();
        }

    private:

        Buffer _buf;
        BufferAppender<Predicate> _appender;
        std::unique_ptr<Cursor> _cur;
    };

    template<class Predicate>
    class BufferedScanCursor : public BufferedCursor {
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
        virtual bool next(DBT *key, DBT *val);
        virtual bool next(Slice &key, Slice &val);

        virtual bool ok() const {
            return _cur->ok() || _buf.more();
        }

    private:

        Buffer _buf;
        BufferAppender<Predicate> _appender;
        std::unique_ptr<Cursor> _cur;
    };

    struct NoFilter {
        bool operator()(const Slice &, const Slice &) const { return true; }
    };

} // namespace ftcxx

#include "cursor-inl.hpp"
