set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fexceptions -Wno-deprecated-declarations")

add_library(ftcxx STATIC
  buffer
  cursor
  db_env
  malloc_utils
  )
add_dependencies(ftcxx install_tdb_h)

add_subdirectory(tests)