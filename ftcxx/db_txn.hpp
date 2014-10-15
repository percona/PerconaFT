/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <db.h>

#include "db_env.hpp"
#include "exceptions.hpp"

namespace ftcxx {

    class DBTxn {
    public:
        DBTxn()
            : _txn(nullptr)
        {}

        explicit DBTxn(const DBEnv &env, int flags=0)
            : _txn(nullptr)
        {
            DB_TXN *t;
            int r = env.env()->txn_begin(env.env(), nullptr, &t, flags);
            handle_ft_retval(r);
            _txn = t;
        }

        DBTxn(const DBEnv &env, const DBTxn &parent, int flags=0)
            : _txn(nullptr)
        {
            DB_TXN *t;
            int r = env.env()->txn_begin(env.env(), parent.txn(), &t, flags);
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
            : _txn(nullptr)
        {
            std::swap(_txn, o._txn);
        }

        DBTxn& operator=(DBTxn &&o) {
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

    private:
        DB_TXN *_txn;
    };

} // namespace ftcxx
