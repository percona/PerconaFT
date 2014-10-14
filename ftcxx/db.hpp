/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <string>

#include <db.h>

#include "cursor.hpp"
#include "db_env.hpp"
#include "db_txn.hpp"
#include "exceptions.hpp"
#include "slice.hpp"

namespace ftcxx {

    class Cursor;
    class BufferedCursor;

    template<class Comparator, class Handler>
    class RangeCursor;
    template<class Handler>
    class ScanCursor;
    template<class Comparator, class Predicate>
    class BufferedRangeCursor;
    template<class Predicate>
    class BufferedScanCursor;

    class DB {
    public:
        DB()
            : _db(nullptr)
        {}

        explicit DB(::DB *d)
            : _db(d)
        {}

        ~DB() {
            if (_db) {
                close();
            }
        }

        DB(const DB &) = delete;
        DB& operator=(const DB &) = delete;

        DB(DB&& other)
            : _db(nullptr)
        {
            std::swap(_db, other._db);
        }

        DB& operator=(DB&& other) {
            std::swap(_db, other._db);
            return *this;
        }

        ::DB *db() const { return _db; }

        Slice descriptor() const {
            return Slice(_db->cmp_descriptor->dbt);
        }

        template<typename Callback>
        int getf_set(const DBTxn &txn, const Slice &key, int flags, Callback cb) const {
            class WrappedCallback {
                Callback &_cb;
            public:
                WrappedCallback(Callback &cb)
                    : _cb(cb)
                {}

                static int call(const DBT *key, const DBT *val, void *extra) {
                    WrappedCallback *wc = static_cast<WrappedCallback *>(extra);
                    return wc->call(key, val);
                }

                int call(const DBT *key, const DBT *val) {
                    return _cb(Slice(*key), Slice(*val));
                }
            } wc(cb);

            DBT kdbt = key.dbt();
            return _db->getf_set(_db, txn.txn(), flags, &kdbt, &WrappedCallback::call, &wc);
        }

        int put(const DBTxn &txn, DBT *key, DBT *val, int flags=0) const {
            return _db->put(_db, txn.txn(), key, val, flags);
        }

        int put(const DBTxn &txn, const Slice &key, const Slice &val, int flags=0) const {
            DBT kdbt = key.dbt();
            DBT vdbt = val.dbt();
            return put(txn, &kdbt, &vdbt, flags);
        }

        int del(const DBTxn &txn, DBT *key, int flags=0) const {
            return _db->del(_db, txn.txn(), key, flags);
        }

        int del(const DBTxn &txn, const Slice &key, int flags=0) const {
            DBT kdbt = key.dbt();
            return _db->del(_db, txn.txn(), &kdbt, flags);
        }

        /**
         * Constructs a Cursor over this DB, over the range from left to
         * right (or right to left if !forward).
         */
        template<class Comparator, class Handler>
        Cursor *cursor(const DBTxn &txn, DBT *left, DBT *right,
                       Comparator cmp, Handler handler, int flags=0,
                       bool forward=true, bool end_exclusive=false, bool prelock=true) const {
            return new RangeCursor<Comparator, Handler>(*this, txn, flags, left, right, cmp, handler, forward, end_exclusive, prelock);
        }

        template<class Comparator, class Handler>
        Cursor *cursor(const DBTxn &txn, const Slice &left, const Slice &right,
                       Comparator cmp, Handler handler, int flags=0,
                       bool forward=true, bool end_exclusive=false, bool prelock=true) const {
            return new RangeCursor<Comparator, Handler>(*this, txn, flags, left, right, cmp, handler, forward, end_exclusive, prelock);
        }

        template<class Handler>
        Cursor *cursor(const DBTxn &txn, Handler handler,
                       int flags=0, bool forward=true, bool prelock=true) const {
            return new ScanCursor<Handler>(*this, txn, flags, handler, forward, prelock);
        }

        template<class Comparator, class Predicate>
        BufferedCursor *buffered_cursor(const DBTxn &txn, DBT *left, DBT *right,
                                        Comparator cmp, Predicate filter, int flags=0,
                                        bool forward=true, bool end_exclusive=false, bool prelock=true) const {
            return new BufferedRangeCursor<Comparator, Predicate>(*this, txn, flags, left, right, cmp, filter, forward, end_exclusive, prelock);
        }

        template<class Comparator, class Predicate>
        BufferedCursor *buffered_cursor(const DBTxn &txn, const Slice &left, const Slice &right,
                                        Comparator cmp, Predicate filter, int flags=0,
                                        bool forward=true, bool end_exclusive=false, bool prelock=true) const {
            return new BufferedRangeCursor<Comparator, Predicate>(*this, txn, flags, left, right, cmp, filter, forward, end_exclusive, prelock);
        }

        template<class Predicate>
        BufferedCursor *buffered_cursor(const DBTxn &txn, Predicate filter,
                                        int flags=0, bool forward=true, bool prelock=true) const {
            return new BufferedScanCursor<Predicate>(*this, txn, flags, filter, forward, prelock);
        }

        void close() {
            int r = _db->close(_db, 0);
            handle_ft_retval(r);
            _db = nullptr;
        }

    private:
        ::DB *_db;
    };

    class DBBuilder {
        uint32_t _readpagesize;
        TOKU_COMPRESSION_METHOD _compression_method;
        uint32_t _fanout;
        uint8_t _memcmp_magic;
        uint32_t _pagesize;
        Slice _descriptor;

    public:
        DBBuilder()
            : _readpagesize(0),
              _compression_method(TOKU_COMPRESSION_METHOD(0)),
              _fanout(0),
              _memcmp_magic(0),
              _pagesize(0),
              _descriptor()
        {}

        DB open(const DBEnv &env, const DBTxn &txn, const char *fname, const char *dbname, DBTYPE dbtype, uint32_t flags, int mode) const {
            ::DB *db;
            int r = db_create(&db, env.env(), 0);
            handle_ft_retval(r);

            if (_readpagesize) {
                r = db->set_readpagesize(db, _readpagesize);
                handle_ft_retval(r);
            }

            if (_compression_method) {
                r = db->set_compression_method(db, _compression_method);
                handle_ft_retval(r);
            }

            if (_fanout) {
                r = db->set_fanout(db, _fanout);
                handle_ft_retval(r);
            }

            if (_memcmp_magic) {
                r = db->set_memcmp_magic(db, _memcmp_magic);
                handle_ft_retval(r);
            }

            if (_pagesize) {
                r = db->set_pagesize(db, _pagesize);
                handle_ft_retval(r);
            }

            r = db->open(db, txn.txn(), fname, dbname, dbtype, flags, mode);
            handle_ft_retval(r);

            if (!_descriptor.empty()) {
                DBT desc = _descriptor.dbt();
                r = db->change_descriptor(db, txn.txn(), &desc, DB_UPDATE_CMP_DESCRIPTOR);
                handle_ft_retval(r);
            }

            return DB(db);
        }

        DBBuilder& set_readpagesize(uint32_t readpagesize) {
            _readpagesize = readpagesize;
            return *this;
        }

        DBBuilder& set_compression_method(TOKU_COMPRESSION_METHOD _compressionmethod) {
            _compression_method = _compressionmethod;
            return *this;
        }

        DBBuilder& set_fanout(uint32_t fanout) {
            _fanout = fanout;
            return *this;
        }

        DBBuilder& set_memcmp_magic(uint8_t _memcmpmagic) {
            _memcmp_magic = _memcmpmagic;
            return *this;
        }

        DBBuilder& set_pagesize(uint32_t pagesize) {
            _pagesize = pagesize;
            return *this;
        }

        DBBuilder& set_descriptor(const Slice &desc) {
            _descriptor = desc;
            return *this;
        }
    };

} // namespace ftcxx
