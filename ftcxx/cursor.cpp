/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <db.h>

#include "cursor.hpp"
#include "db.hpp"
#include "db_env.hpp"
#include "db_txn.hpp"
#include "exceptions.hpp"

namespace ftcxx {

    Cursor::Cursor(const DB &db, const DBTxn &txn, int flags)
        : _dbc(nullptr)
    {
        if (db.db() != nullptr) {
            DBC *c;
            int r = db.db()->cursor(db.db(), txn.txn(), &c, flags);
            handle_ft_retval(r);
            _dbc = c;
        }
    }

    Cursor::~Cursor() {
        if (_dbc != nullptr) {
            close();
        }
    }

    void Cursor::close() {
        int r = _dbc->c_close(_dbc);
        handle_ft_retval(r);
        _dbc = nullptr;
    }

    DirectoryCursor::DirectoryCursor(const DBEnv &env, const DBTxn &txn)
        : Cursor(DB())
    {
        DBC *c;
        int r = env.env()->get_cursor_for_directory(env.env(), txn.txn(), &c);
        handle_ft_retval(r);
        _dbc = c;
    }

} // namespace ftcxx
