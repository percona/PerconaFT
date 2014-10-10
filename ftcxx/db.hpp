/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <string>

#include <db.h>

#include "db_env.hpp"
#include "db_txn.hpp"
#include "exceptions.hpp"

namespace ftcxx {

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

        int put(const DBTxn &txn, DBT *key, DBT *val, int flags=0) const {
            return _db->put(_db, txn.txn(), key, val, flags);
        }

        int del(const DBTxn &txn, DBT *key, int flags=0) const {
            return _db->del(_db, txn.txn(), key, flags);
        }

        int get(const DBTxn &txn, DBT *key, DBT *val, int flags=0) const {
            return _db->get(_db, txn.txn(), key, val, flags);
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

    public:
        DBBuilder()
            : _readpagesize(0),
              _compression_method(TOKU_COMPRESSION_METHOD(0)),
              _fanout(0),
              _memcmp_magic(0),
              _pagesize(0)
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
    };

} // namespace ftcxx
