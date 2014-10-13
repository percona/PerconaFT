set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fexceptions -Wno-deprecated-declarations")

add_library(ftcxx STATIC
  buffer
  cursor
  db_env
  malloc_utils
  )
add_dependencies(ftcxx install_tdb_h)

if (NOT DEFINED MYSQL_PROJECT_NAME_DOCSTRING)
  install(
    TARGETS ftcxx
    DESTINATION ${INSTALL_LIBDIR}
    COMPONENT tokukv_libs_static
    )

  file(GLOB ftcxx_headers "*.hpp")
  install(
    FILES ${ftcxx_headers}
    DESTINATION include/ftcxx
    COMPONENT tokukv_headers
    )
endif ()

add_subdirectory(tests)