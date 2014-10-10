/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#pragma once

#include <string>

#include <db.h>

#include "exceptions.hpp"

namespace ftcxx {

    class DBEnv {
    public:
        explicit DBEnv(DB_ENV *e)
            : _env(e)
        {}

        ~DBEnv() {
            if (_env) {
                int r = _env->close(_env, 0);
                handle_ft_retval(r);
            }
        }

        DBEnv(const DBEnv &) = delete;
        DBEnv& operator=(const DBEnv &) = delete;

        DBEnv(DBEnv&& other)
            : _env(nullptr)
        {
            std::swap(_env, other._env);
        }

        DBEnv& operator=(DBEnv&& other) {
            std::swap(_env, other._env);
            return *this;
        }

        DB_ENV *env() const { return _env; }

    private:
        DB_ENV *_env;
    };

    class DBEnvBuilder {
        typedef int (*bt_compare_func)(DB *, const DBT *, const DBT *);
        bt_compare_func _bt_compare;

        typedef int (*update_func)(DB *, const DBT *, const DBT *, const DBT *, void (*)(const DBT *, void *), void *);
        update_func _update_function;

        generate_row_for_put_func _generate_row_for_put;
        generate_row_for_del_func _generate_row_for_del;

        uint32_t _cleaner_period;
        uint32_t _cleaner_iterations;
        uint32_t _checkpointing_period;

        uint64_t _lk_max_memory;
        uint64_t _lock_wait_time_msec;

        typedef uint64_t (*get_lock_wait_time_cb_func)(uint64_t);
        get_lock_wait_time_cb_func _get_lock_wait_time_cb;
        lock_timeout_callback _lock_timeout_callback;
        uint64_t (*_loader_memory_size_callback)(void);

        uint32_t _cachesize_gbytes;
        uint32_t _cachesize_bytes;

        std::string _lg_dir;
        std::string _tmp_dir;

    public:
        DBEnvBuilder()
            : _bt_compare(nullptr),
              _update_function(nullptr),
              _generate_row_for_put(nullptr),
              _generate_row_for_del(nullptr),
              _cleaner_period(0),
              _cleaner_iterations(0),
              _checkpointing_period(0),
              _lk_max_memory(0),
              _lock_wait_time_msec(0),
              _get_lock_wait_time_cb(nullptr),
              _lock_timeout_callback(nullptr),
              _loader_memory_size_callback(nullptr),
              _cachesize_gbytes(0),
              _cachesize_bytes(0),
              _lg_dir(""),
              _tmp_dir("")
        {}

        DBEnv open(const char *env_dir, uint32_t flags, int mode) const {
            DB_ENV *env;
            int r = db_env_create(&env, 0);
            handle_ft_retval(r);

            if (_bt_compare) {
                r = env->set_default_bt_compare(env, _bt_compare);
                handle_ft_retval(r);
            }

            if (_update_function) {
                env->set_update(env, _update_function);
            }

            if (_generate_row_for_put) {
                r = env->set_generate_row_callback_for_put(env, _generate_row_for_put);
                handle_ft_retval(r);
            }

            if (_generate_row_for_del) {
                r = env->set_generate_row_callback_for_del(env, _generate_row_for_del);
                handle_ft_retval(r);
            }

            if (_cleaner_period) {
                r = env->cleaner_set_period(env, _cleaner_period);
                handle_ft_retval(r);
            }

            if (_cleaner_iterations) {
                r = env->cleaner_set_iterations(env, _cleaner_iterations);
                handle_ft_retval(r);
            }

            if (_checkpointing_period) {
                r = env->checkpointing_set_period(env, _checkpointing_period);
                handle_ft_retval(r);
            }

            if (_lk_max_memory) {
                r = env->set_lk_max_memory(env, _lk_max_memory);
                handle_ft_retval(r);
            }

            if (_lock_wait_time_msec || _get_lock_wait_time_cb) {
                uint64_t wait_time = _lock_wait_time_msec;
                if (!wait_time) {
                    r = env->get_lock_timeout(env, &wait_time);
                    handle_ft_retval(r);
                }
                r = env->set_lock_timeout(env, wait_time, _get_lock_wait_time_cb);
                handle_ft_retval(r);
            }

            if (_lock_timeout_callback) {
                r = env->set_lock_timeout_callback(env, _lock_timeout_callback);
                handle_ft_retval(r);
            }

            if (_loader_memory_size_callback) {
                env->set_loader_memory_size(env, _loader_memory_size_callback);
            }

            if (_cachesize_gbytes || _cachesize_bytes) {
                r = env->set_cachesize(env, _cachesize_gbytes, _cachesize_bytes, 1);
                handle_ft_retval(r);
            }

            if (!_lg_dir.empty()) {
                r = env->set_lg_dir(env, _lg_dir.c_str());
                handle_ft_retval(r);
            }

            if (!_tmp_dir.empty()) {
                r = env->set_tmp_dir(env, _tmp_dir.c_str());
                handle_ft_retval(r);
            }

            r = env->open(env, env_dir, flags, mode);
            handle_ft_retval(r);

            return DBEnv(env);
        }

        DBEnvBuilder& set_default_bt_compare(bt_compare_func bt_compare) {
            _bt_compare = bt_compare;
            return *this;
        }

        DBEnvBuilder& set_update(update_func update_function) {
            _update_function = update_function;
            return *this;
        }

        DBEnvBuilder& set_generate_row_callback_for_put(generate_row_for_put_func generate_row_for_put) {
            _generate_row_for_put = generate_row_for_put;
            return *this;
        }

        DBEnvBuilder& set_generate_row_callback_for_del(generate_row_for_del_func generate_row_for_del) {
            _generate_row_for_del = generate_row_for_del;
            return *this;
        }

        DBEnvBuilder& cleaner_set_period(uint32_t period) {
            _cleaner_period = period;
            return *this;
        }

        DBEnvBuilder& cleaner_set_iterations(uint32_t iterations) {
            _cleaner_iterations = iterations;
            return *this;
        }

        DBEnvBuilder& checkpointing_set_period(uint32_t period) {
            _checkpointing_period = period;
            return *this;
        }

        DBEnvBuilder& set_lk_max_memory(uint64_t sz) {
            _lk_max_memory = sz;
            return *this;
        }

        DBEnvBuilder& set_lock_wait_time_msec(uint64_t lock_wait_time_msec) {
            _lock_wait_time_msec = lock_wait_time_msec;
            return *this;
        }

        DBEnvBuilder& set_lock_wait_time_cb(get_lock_wait_time_cb_func get_lock_wait_time_cb) {
            _get_lock_wait_time_cb = get_lock_wait_time_cb;
            return *this;
        }

        DBEnvBuilder& set_lock_timeout_callback(lock_timeout_callback callback) {
            _lock_timeout_callback = callback;
            return *this;
        }

        DBEnvBuilder& set_loader_memory_size(uint64_t (*callback)(void)) {
            _loader_memory_size_callback = callback;
            return *this;
        }

        DBEnvBuilder& set_cachesize(uint32_t gbytes, uint32_t bytes) {
            _cachesize_gbytes = gbytes;
            _cachesize_bytes = bytes;
            return *this;
        }

        DBEnvBuilder& set_lg_dir(const char *lg_dir) {
            _lg_dir = std::string(lg_dir);
            return *this;
        }

        DBEnvBuilder& set_tmp_dir(const char *tmp_dir) {
            _tmp_dir = std::string(tmp_dir);
            return *this;
        }
    };

} // namespace ftcxx
