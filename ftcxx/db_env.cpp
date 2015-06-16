/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <map>
#include <memory>
#include <string>

#include <db.h>

#include "db_env.hpp"

namespace ftcxx {

    void DBEnv::get_status(DBEnv::Status &status, fs_redzone_state &redzone_state, uint64_t &env_panic, std::string &panic_string) const{
        uint64_t num_rows;
        int r = _env->get_engine_status_num_rows(_env, &num_rows);
        handle_ft_retval(r);

        std::unique_ptr<TOKU_ENGINE_STATUS_ROW_S[]> buf(new TOKU_ENGINE_STATUS_ROW_S[num_rows]);
        char panic_string_buf[1<<12];
        panic_string_buf[0] = '\0';

        r = _env->get_engine_status(_env, buf.get(), num_rows, &num_rows,
                                    &redzone_state,
                                    &env_panic, panic_string_buf, sizeof panic_string_buf,
                                    toku_engine_status_include_type(TOKU_ENGINE_STATUS | TOKU_GLOBAL_STATUS));
        handle_ft_retval(r);

        panic_string = std::string(panic_string_buf);

        for (uint64_t i = 0; i < num_rows; ++i) {
            status[buf[i].keyname] = buf[i];
        }
    }

} // namespace ftcxx
