set(ac_cv_have_stdint_h ${HAVE_STDINT_H})
set(ac_cv_have_stddef_h ${HAVE_STDDEF_H})
set(ac_cv_have_sys_uio_h ${HAVE_SYS_UIO_H})

if (HAVE_STDLIB_H AND HAVE_STDARG_H AND HAVE_STRING_H AND HAVE_FLOAT_H)
  set(STDC_HEADERS 1)
else ()
  set(STDC_HEADERS 0)
endif ()

include(CheckCSourceCompiles)

check_c_source_compiles("int main(void) {
  return __builtin_expect(1, 1) ? 1 : 0;
}" HAVE_BUILTIN_EXPECT)
check_c_source_compiles("int main(void) {
  return (__builtin_ctzll(0x100000000LL) == 32) ? 1 : 0;
}" HAVE_BUILTIN_CTZ)

set(SNAPPY_MAJOR 1)
set(SNAPPY_MINOR 1)
set(SNAPPY_PATCHLEVEL 1)

set_source_files_properties("${CMAKE_CURRENT_BINARY_DIR}/config.h" PROPERTIES GENERATED TRUE)
configure_file(config.h.in config.h)
add_custom_target(generate_snappy_config_h DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/config.h")
set_source_files_properties("${CMAKE_CURRENT_BINARY_DIR}/snappy-stubs-public.h" PROPERTIES GENERATED TRUE)
configure_file(snappy-stubs-public.h.in snappy-stubs-public.h)
add_custom_target(generate_snappy_stubs_public_h DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/snappy-stubs-public.h")

include_directories("${CMAKE_CURRENT_BINARY_DIR}")
include_directories(.)
add_library(snappy STATIC
  snappy
  snappy-sinksource
  snappy-stubs-internal
  snappy-c
  )

set(CMAKE_CXX_FLAGS "-Wall -Wno-unused-parameter -Wno-unused-function")
set_target_properties(snappy PROPERTIES POSITION_INDEPENDENT_CODE ON)
