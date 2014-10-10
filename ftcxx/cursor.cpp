/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <db.h>

#include "cursor.hpp"
#include "db_env.hpp"
#include "db_txn.hpp"
#include "exceptions.hpp"

namespace ftcxx {

    Cursor::Cursor(DB *db, const DBTxn &txn, int flags)
        : _dbc(nullptr)
    {
        if (db != nullptr) {
            DBC *c;
            int r = db->cursor(db, txn.txn(), &c, flags);
            handle_ft_retval(r);
            _dbc = c;
        }
    }

    Cursor::~Cursor() {
        if (_dbc != nullptr) {
            int r = _dbc->c_close(_dbc);
            handle_ft_retval(r);
        }
    }

    DirectoryCursor::DirectoryCursor(const DBEnv &env, const DBTxn &txn)
        : Cursor(nullptr)
    {
        DBC *c;
        int r = env.env()->get_cursor_for_directory(env.env(), txn.txn(), &c);
        handle_ft_retval(r);
        _dbc = c;
    }

} // namespace ftcxx
