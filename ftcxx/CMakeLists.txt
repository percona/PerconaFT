set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fexceptions")

add_library(ftcxx STATIC
  buffer
  cursor
  malloc_utils
  )
add_dependencies(ftcxx install_tdb_h)

add_subdirectory(tests)