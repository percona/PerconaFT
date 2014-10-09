/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <db.h>

#include "buffer.hpp"

namespace ftcxx {

    /**
     * Cursor is a simple RAII wrapper around a DBC object.
     */
    class Cursor {
    public:
        Cursor(DB *db, DB_TXN *txn = nullptr, int flags = 0);
        ~Cursor();

        DBC *dbc() const { return _dbc; }

        /**
         * Cursor::Iterator supports iterating a cursor over a key range,
         * with bulk fetch buffering, and optional filtering.
         */
        template<class Comparator, class Predicate>
        class Iterator {
        public:

            /**
             * Constructs an iterator.  Better to use Cursor::iterator
             * instead (below) to avoid template parameters.
             */
            Iterator(Cursor &cur, const DBT *left, const DBT *right,
                     Comparator cmp, Predicate filter,
                     bool forward, bool end_exclusive, bool prelock);

            /**
             * Gets the next key/val pair in the iteration.  Returns true
             * if there is more data, and fills in key and val.  If the
             * range is exhausted, returns false.
             */
            bool next(DBT *key, DBT *val);

        private:

            Cursor &_cur;
            const DBT *_left;
            const DBT *_right;
            Comparator &_cmp;
            Predicate &_filter;

            const bool _forward;
            const bool _end_exclusive;
            const bool _prelock;

            Buffer _buf;

            static int getf_callback(const DBT *key, const DBT *val, void *extra) {
                Iterator *i = static_cast<Iterator *>(extra);
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

            static size_t marshalled_size(const DBT *key, const DBT *val) {
                return (sizeof key->size) + (sizeof val->size) + key->size + val->size;
            }

            static void marshall(char *dest, const DBT *key, const DBT *val);

            static void unmarshall(const char *src, DBT *key, DBT *val);
        };

        struct ConstantTrue {
            bool operator()(const DBT *, const DBT *) const { return true; }
        };

        /**
         * Constructs an Iterator with this Cursor, over the range from
         * left to right (or right to left if !forward).
         */
        template<class Comparator, class Predicate>
        Iterator<Comparator, Predicate>&& iterator(const DBT *left, const DBT *right,
                                                   Comparator cmp, Predicate filter=ConstantTrue(),
                                                   bool forward=true, bool end_exclusive=false, bool prelock=true) {
            return std::move(Iterator<Comparator, Predicate>(*this, left, right, filter, forward, end_exclusive, prelock));
        }

    protected:

        DBC *_dbc;
    };

    class DirectoryCursor : public Cursor {
    public:
        DirectoryCursor(DB_ENV *env, DB_TXN *txn);
    };

} // namespace ftcxx
