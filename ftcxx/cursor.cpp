/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <db.h>

#include "cursor.hpp"
#include "db.hpp"
#include "db_env.hpp"
#include "db_txn.hpp"
#include "exceptions.hpp"

namespace ftcxx {

    DBC::DBC(const DB &db, const DBTxn &txn, int flags)
        : _txn(),
          _dbc(nullptr)
    {
        if (db.db() != nullptr) {
            DB_TXN *txnp = txn.txn();
            if (txnp == nullptr) {
                _txn = DBTxn(DBEnv(db.db()->dbenv), DB_TXN_READ_ONLY | DB_READ_UNCOMMITTED);
                txnp = _txn.txn();
            }

            ::DBC *c;
            int r = db.db()->cursor(db.db(), txnp, &c, flags);
            handle_ft_retval(r);
            _dbc = c;
        }
    }

    DBC::DBC(const DBEnv &env, const DBTxn &txn)
        : _txn(),
          _dbc(nullptr)
    {
        if (env.env() != nullptr) {
            DB_TXN *txnp = txn.txn();
            if (txnp == nullptr) {
                _txn = DBTxn(env, DB_TXN_READ_ONLY | DB_READ_UNCOMMITTED);
                txnp = _txn.txn();
            }

            ::DBC *c;
            int r = env.env()->get_cursor_for_directory(env.env(), txnp, &c);
            handle_ft_retval(r);
            _dbc = c;
        }
    }

    DBC::~DBC() {
        if (_dbc != nullptr) {
            close();
        }
    }

    void DBC::close() {
        int r = _dbc->c_close(_dbc);
        handle_ft_retval(r);
        _dbc = nullptr;
    }

    bool DBC::set_range(const IterationStrategy &strategy, const Bounds &bounds, YDB_CALLBACK_FUNCTION callback, void *extra) const {
        int r = dbc()->c_set_bounds(dbc(), bounds.left_dbt(), bounds.right_dbt(), strategy.prelock, 0);
        handle_ft_retval(r);

        if (strategy.forward) {
            if (bounds.left_infinite()) {
                r = dbc()->c_getf_first(dbc(), strategy.getf_flags(), callback, extra);
            } else {
                r = dbc()->c_getf_set_range(dbc(), strategy.getf_flags(), const_cast<DBT *>(bounds.left_dbt()), callback, extra);
            }
        } else {
            if (bounds.right_infinite()) {
                r = dbc()->c_getf_last(dbc(), strategy.getf_flags(), callback, extra);
            } else {
                r = dbc()->c_getf_set_range_reverse(dbc(), strategy.getf_flags(), const_cast<DBT *>(bounds.right_dbt()), callback, extra);
            }
        }
        if (r == DB_NOTFOUND) {
            return false;
        } else if (r != 0 && r != -1) {
            handle_ft_retval(r);
        }
        return true;
    }

    bool DBC::advance(const IterationStrategy &strategy, YDB_CALLBACK_FUNCTION callback, void *extra) const {
        int r;
        if (strategy.forward) {
            r = dbc()->c_getf_next(dbc(), strategy.getf_flags(), callback, extra);
        } else {
            r = dbc()->c_getf_prev(dbc(), strategy.getf_flags(), callback, extra);
        }
        if (r == DB_NOTFOUND) {
            return false;
        } else if (r != 0 && r != -1) {
            handle_ft_retval(r);
        }
        return true;
    }

} // namespace ftcxx
