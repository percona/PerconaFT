include_directories(${CMAKE_CURRENT_SOURCE_DIR} "${CMAKE_CURRENT_BINARY_DIR}/..")

set(tokudb_srcs
  ydb
  ydb_cursor
  ydb_db
  ydb_env_func
  ydb_row_lock
  ydb_txn
  ydb_write
  errors
  loader
  indexer
  indexer-undo-do
  toku_patent
  )

## make the shared library
add_library(${LIBTOKUDB} SHARED ${tokudb_srcs})
add_dependencies(${LIBTOKUDB} install_tdb_h generate_log_code)
target_link_libraries(${LIBTOKUDB} LINK_PRIVATE locktree_static ft_static util_static lzma snappy ${LIBTOKUPORTABILITY})
target_link_libraries(${LIBTOKUDB} LINK_PUBLIC z)

## make the static library
add_library(${LIBTOKUDB}_static STATIC ${tokudb_srcs})
add_dependencies(${LIBTOKUDB}_static install_tdb_h generate_log_code)
target_link_libraries(${LIBTOKUDB}_static locktree_static ft_static util_static lzma snappy)

## add gcov and define _GNU_SOURCE
maybe_add_gcov_to_libraries(${LIBTOKUDB} ${LIBTOKUDB}_static)
set_property(TARGET ${LIBTOKUDB} ${LIBTOKUDB}_static APPEND PROPERTY COMPILE_DEFINITIONS _GNU_SOURCE)

## add a version script and set -fvisibility=hidden for the shared library
configure_file(export.map . COPYONLY)
if (NOT CMAKE_SYSTEM_NAME STREQUAL Darwin)
  add_space_separated_property(TARGET ${LIBTOKUDB} COMPILE_FLAGS "-fvisibility=hidden -fvisibility-inlines-hidden")
  add_space_separated_property(TARGET ${LIBTOKUDB} LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/export.map")
endif ()

# detect when we are being built as a subproject
if (NOT DEFINED MYSQL_PROJECT_NAME_DOCSTRING)
  install(
    TARGETS ${LIBTOKUDB}
    DESTINATION ${INSTALL_LIBDIR}
    COMPONENT tokukv_libs_shared
    )
  install(
    TARGETS ${LIBTOKUDB}_static
    DESTINATION ${INSTALL_LIBDIR}
    COMPONENT tokukv_libs_static
    )
endif ()

add_subdirectory(tests)
