/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <db.h>

#include "db_env.hpp"
#include "exceptions.hpp"

namespace ftcxx {

    class DBTxn {
    public:
        DBTxn()
            : _flags(0),
              _txn(nullptr)
        {}

        explicit DBTxn(const DBEnv &env, int flags=0)
            : _flags(flags),
              _txn(nullptr)
        {
            DB_TXN *t;
            int r = env.env()->txn_begin(env.env(), nullptr, &t, _flags);
            handle_ft_retval(r);
            _txn = t;
        }

        DBTxn(const DBEnv &env, const DBTxn &parent, int flags=0)
            : _flags(flags),
              _txn(nullptr)
        {
            DB_TXN *t;
            int r = env.env()->txn_begin(env.env(), parent.txn(), &t, _flags);
            handle_ft_retval(r);
            _txn = t;
        }

        ~DBTxn() {
            if (_txn) {
                abort();
            }
        }

        DBTxn(const DBTxn &) = delete;
        DBTxn& operator=(const DBTxn &) = delete;

        DBTxn(DBTxn &&o)
            : _flags(0),
              _txn(nullptr)
        {
            std::swap(_flags, o._flags);
            std::swap(_txn, o._txn);
        }

        DBTxn& operator=(DBTxn &&o) {
            std::swap(_flags, o._flags);
            std::swap(_txn, o._txn);
            return *this;
        }

        DB_TXN *txn() const { return _txn; }

        void commit(int flags=0) {
            int r = _txn->commit(_txn, flags);
            handle_ft_retval(r);
            _txn = nullptr;
        }

        void abort() {
            int r = _txn->abort(_txn);
            handle_ft_retval(r);
            _txn = nullptr;
        }

        bool is_read_only() const {
            return _flags & DB_TXN_READ_ONLY;
        }

        uint64_t id() const {
            if (!_txn) {
                return 0;
            }
            return _txn->id64(_txn);
        }

    private:
        int _flags;
        DB_TXN *_txn;
    };

} // namespace ftcxx
