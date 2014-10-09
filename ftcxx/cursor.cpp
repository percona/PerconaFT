/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <db.h>

#include "cursor.hpp"
#include "exceptions.hpp"

namespace ftcxx {

    Cursor::Cursor(DB *db, DB_TXN *txn, int flags)
        : _dbc(nullptr)
    {
        if (db != nullptr) {
            DBC *c;
            int r = db->cursor(db, txn, &c, flags);
            if (r != 0) {
                throw ft_exception(r);
            }
            _dbc = c;
        }
    }

    Cursor::~Cursor() {
        if (_dbc != nullptr) {
            int r = _dbc->c_close(_dbc);
            if (r != 0) {
                throw ft_exception(r);
            }
        }
    }

    DirectoryCursor::DirectoryCursor(DB_ENV *env, DB_TXN *txn)
        : Cursor(nullptr)
    {
        DBC *c;
        int r = env->get_cursor_for_directory(env, txn, &c);
        if (r != 0) {
            throw ft_exception(r);
        }
        _dbc = c;
    }

} // namespace ftcxx
