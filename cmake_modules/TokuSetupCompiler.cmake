function(add_c_defines)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS ${ARGN})
endfunction(add_c_defines)

## always want these
set(CMAKE_C_FLAGS "-Wall -Werror ${CMAKE_C_FLAGS}")
set(CMAKE_CXX_FLAGS "-Wall -Werror ${CMAKE_CXX_FLAGS}")

if (APPLE)
  add_c_defines(DARWIN=1 _DARWIN_C_SOURCE)
endif ()

## preprocessor definitions we want everywhere
add_c_defines(
  _FILE_OFFSET_BITS=64
  _LARGEFILE64_SOURCE
  __STDC_FORMAT_MACROS
  __STDC_LIMIT_MACROS
  __LONG_LONG_SUPPORTED
  )
if (NOT CMAKE_SYSTEM_NAME STREQUAL FreeBSD)
  ## on FreeBSD these types of macros actually remove functionality
  add_c_defines(
    _DEFAULT_SOURCE
    _XOPEN_SOURCE=600
    )
endif ()

## add TOKU_PTHREAD_DEBUG for debug builds
if (CMAKE_VERSION VERSION_LESS 3.0)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS_DEBUG TOKU_PTHREAD_DEBUG=1 TOKU_DEBUG_TXN_SYNC=1)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS_DRD TOKU_PTHREAD_DEBUG=1 TOKU_DEBUG_TXN_SYNC=1)
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS_DRD _FORTIFY_SOURCE=2)
else ()
  set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS
    $<$<OR:$<CONFIG:DEBUG>,$<CONFIG:DRD>>:TOKU_PTHREAD_DEBUG=1 TOKU_DEBUG_TXN_SYNC=1>
    $<$<CONFIG:DRD>:_FORTIFY_SOURCE=2>
    )
endif ()

## coverage
option(USE_GCOV "Use gcov for test coverage." OFF)
if (USE_GCOV)
  if (NOT CMAKE_CXX_COMPILER_ID MATCHES GNU)
    message(FATAL_ERROR "Must use the GNU compiler to compile for test coverage.")
  endif ()
  find_program(COVERAGE_COMMAND NAMES gcov47 gcov)
endif (USE_GCOV)

include(CheckCXXCompilerFlag)
include(CMakePushCheckState)
include(CheckCSourceCompiles)
include(CheckCXXSourceCompiles)

SET(fail_patterns
  FAIL_REGEX "unknown argument ignored"
  FAIL_REGEX "argument unused during compilation"
  FAIL_REGEX "unsupported .*option"
  FAIL_REGEX "unknown .*option"
  FAIL_REGEX "unrecognized .*option"
  FAIL_REGEX "ignoring unknown option"
  FAIL_REGEX "[Ww]arning: [Oo]ption"
  FAIL_REGEX "error: visibility"
  FAIL_REGEX "warning: visibility"
  FAIL_REGEX "warning:.*is valid for.*but not for"
  FAIL_REGEX "error:.*is valid for.*but not for"
  )

MACRO (TOKUDB_CHECK_C_COMPILER_FLAG FLAG RESULT)
  CMAKE_PUSH_CHECK_STATE(RESET)
  SET(OLD_CMAKE_C_FLAGS ${CMAKE_C_FLAGS})
  SET(CMAKE_C_FLAGS)

  SET(CMAKE_REQUIRED_FLAGS "-Werror ${FLAG}")
  CHECK_C_SOURCE_COMPILES("int main(void) { return 0; }" ${RESULT} ${fail_patterns})

  SET(CMAKE_C_FLAGS ${OLD_CMAKE_C_FLAGS})
  CMAKE_POP_CHECK_STATE()
ENDMACRO()

MACRO (TOKUDB_CHECK_CXX_COMPILER_FLAG FLAG RESULT)
  CMAKE_PUSH_CHECK_STATE(RESET)
  SET(OLD_CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS})
  SET(CMAKE_CXX_FLAGS)

  SET(CMAKE_REQUIRED_FLAGS "-Werror ${FLAG}")
  CHECK_CXX_SOURCE_COMPILES("int main(void) { return 0; }" ${RESULT} ${fail_patterns})

  SET(CMAKE_CXX_FLAGS ${OLD_CMAKE_CXX_FLAGS})
  CMAKE_POP_CHECK_STATE()
ENDMACRO()

## prepends a compiler flag if the compiler supports it or removes it in case
## flag isn't supported and already set somewhere else
MACRO (prepend_cflags_if_supported_remove_unsupported)
  FOREACH (flag ${ARGN})
    # Create a result variable per flag to avoid reusing the same cached result
    STRING(REGEX REPLACE "[-,= +]" "_" FLAG2 ${flag})

    SET(VAR_RES "C_RESULT_${FLAG2}")
    TOKUDB_CHECK_C_COMPILER_FLAG(${flag} ${VAR_RES})
    IF(${VAR_RES})
      SET (CMAKE_C_FLAGS "${flag} ${CMAKE_C_FLAGS}")
    ELSEIF(CMAKE_C_FLAGS MATCHES ${flag})
      STRING(REGEX REPLACE "${flag}( |$)" "" CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
    ENDIF()

    SET(VAR_RES "CXX_RESULT_${FLAG2}")
    TOKUDB_CHECK_CXX_COMPILER_FLAG(${flag} ${VAR_RES})
    IF(${VAR_RES})
      SET (CMAKE_CXX_FLAGS "${flag} ${CMAKE_CXX_FLAGS}")
    ELSEIF(CMAKE_CXX_FLAGS MATCHES ${flag})
      STRING(REGEX REPLACE "${flag}( |$)" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    ENDIF()
  ENDFOREACH (flag)
ENDMACRO (prepend_cflags_if_supported_remove_unsupported)

## adds a linker flag if the compiler supports it
macro(set_ldflags_if_supported)
  foreach(flag ${ARGN})
    check_cxx_compiler_flag(${flag} HAVE_${flag})
    if (HAVE_${flag})
      set(CMAKE_EXE_LINKER_FLAGS "${flag} ${CMAKE_EXE_LINKER_FLAGS}")
      set(CMAKE_SHARED_LINKER_FLAGS "${flag} ${CMAKE_SHARED_LINKER_FLAGS}")
    endif ()
  endforeach(flag)
endmacro(set_ldflags_if_supported)

## disable some warnings
prepend_cflags_if_supported_remove_unsupported(
  -Wno-missing-field-initializers
  -Wno-suggest-override
  -Wstrict-null-sentinel
  -Winit-self
  -Wswitch
  -Wtrampolines
  -Wlogical-op
  -fno-rtti
  -fno-exceptions
  -Wno-error=nonnull-compare
  )

if (CMAKE_CXX_FLAGS MATCHES -fno-implicit-templates)
  # must append this because mysql sets -fno-implicit-templates and we need to override it
  check_cxx_compiler_flag(-fimplicit-templates HAVE_CXX_-fimplicit-templates)
  if (HAVE_CXX_-fimplicit-templates)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fimplicit-templates")
  endif ()
endif()

## Clang has stricter POD checks.  So, only enable this warning on our other builds (Linux + GCC)
if (NOT CMAKE_CXX_COMPILER_ID MATCHES Clang)
  prepend_cflags_if_supported_remove_unsupported(
    -Wpacked
    )
endif ()

option (PROFILING "Allow profiling and debug" ON)
if (PROFILING)
  prepend_cflags_if_supported_remove_unsupported(
    -fno-omit-frame-pointer
  )
endif ()

# new flag sets in MySQL 8.0 seem to explicitly disable this
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fexceptions")

## set extra debugging flags and preprocessor definitions
set(CMAKE_C_FLAGS_DEBUG "-g3 -O0 ${CMAKE_C_FLAGS_DEBUG}")
set(CMAKE_CXX_FLAGS_DEBUG "-g3 -O0 ${CMAKE_CXX_FLAGS_DEBUG}")

## flags to use when we want to run DRD on the resulting binaries
## DRD needs debugging symbols.
## -O0 makes it too slow, and -O2 inlines too much for our suppressions to work.  -O1 is just right.
set(CMAKE_C_FLAGS_DRD "-g3 -O1 ${CMAKE_C_FLAGS_DRD}")
set(CMAKE_CXX_FLAGS_DRD "-g3 -O1 ${CMAKE_CXX_FLAGS_DRD}")

## set extra release flags
## need to set flags for RelWithDebInfo as well because we want the MySQL/MariaDB builds to use them
if (CMAKE_CXX_COMPILER_ID STREQUAL Clang)
  # have tried -flto and -O4, both make our statically linked executables break apple's linker
  set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} -g -O3 -UNDEBUG")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -g -O3 -UNDEBUG")
  set(CMAKE_C_FLAGS_RELEASE "-g -O3 ${CMAKE_C_FLAGS_RELEASE} -UNDEBUG")
  set(CMAKE_CXX_FLAGS_RELEASE "-g -O3 ${CMAKE_CXX_FLAGS_RELEASE} -UNDEBUG")
else ()
  if (APPLE)
    set(FLTO_OPTS "-fwhole-program")
  else ()
    set(FLTO_OPTS "-fuse-linker-plugin")
  endif()
  # we overwrite this because the default passes -DNDEBUG and we don't want that
  set(CMAKE_C_FLAGS_RELWITHDEBINFO "-flto ${FLTO_OPTS} ${CMAKE_C_FLAGS_RELWITHDEBINFO} -g -O3 -UNDEBUG")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-flto ${FLTO_OPTS} ${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -g -O3 -UNDEBUG")
  set(CMAKE_C_FLAGS_RELEASE "-g -O3 -flto ${FLTO_OPTS} ${CMAKE_C_FLAGS_RELEASE} -UNDEBUG")
  set(CMAKE_CXX_FLAGS_RELEASE "-g -O3 -flto ${FLTO_OPTS} ${CMAKE_CXX_FLAGS_RELEASE} -UNDEBUG")
  set(CMAKE_EXE_LINKER_FLAGS "-g ${FLTO_OPTS} ${CMAKE_EXE_LINKER_FLAGS}")
  set(CMAKE_SHARED_LINKER_FLAGS "-g ${FLTO_OPTS} ${CMAKE_SHARED_LINKER_FLAGS}")
endif ()

## set warnings
prepend_cflags_if_supported_remove_unsupported(
  -Wextra
  -Wbad-function-cast
  -Wno-missing-noreturn
  -Wstrict-prototypes
  -Wmissing-prototypes
  -Wmissing-declarations
  -Wpointer-arith
  -Wshadow
  -Wmissing-format-attribute
  ## other flags to try:
  #-Wunsafe-loop-optimizations
  #-Wpointer-arith
  #-Wc++-compat
  #-Wc++11-compat
  #-Wwrite-strings
  #-Wzero-as-null-pointer-constant
  #-Wlogical-op
  #-Wvector-optimization-performance
  )

if (NOT CMAKE_CXX_COMPILER_ID STREQUAL Clang)
  # Disabling -Wcast-align with clang.  TODO: fix casting and re-enable it, someday.
  prepend_cflags_if_supported_remove_unsupported(-Wcast-align)
endif ()

# pick language dialect
set(CMAKE_C_FLAGS "-std=c99 ${CMAKE_C_FLAGS}")
check_cxx_compiler_flag(-std=c++11 HAVE_STDCXX11)
check_cxx_compiler_flag(-std=c++0x HAVE_STDCXX0X)
if (HAVE_STDCXX11)
  set(CMAKE_CXX_FLAGS "-std=c++11 ${CMAKE_CXX_FLAGS}")
elseif (HAVE_STDCXX0X)
  set(CMAKE_CXX_FLAGS "-std=c++0x ${CMAKE_CXX_FLAGS}")
else ()
  message(FATAL_ERROR "${CMAKE_CXX_COMPILER} doesn't support -std=c++11 or -std=c++0x, you need one that does.")
endif ()

function(add_space_separated_property type obj propname val)
  get_property(oldval ${type} ${obj} PROPERTY ${propname})
  if (oldval MATCHES NOTFOUND)
    set_property(${type} ${obj} PROPERTY ${propname} "${val}")
  else ()
    set_property(${type} ${obj} PROPERTY ${propname} "${val} ${oldval}")
  endif ()
endfunction(add_space_separated_property)

## this function makes sure that the libraries passed to it get compiled
## with gcov-needed flags, we only add those flags to our libraries
## because we don't really care whether our tests get covered
function(maybe_add_gcov_to_libraries)
  if (USE_GCOV)
    foreach(lib ${ARGN})
      add_space_separated_property(TARGET ${lib} COMPILE_FLAGS --coverage)
      add_space_separated_property(TARGET ${lib} LINK_FLAGS --coverage)
      target_link_libraries(${lib} LINK_PRIVATE gcov)
    endforeach(lib)
  endif (USE_GCOV)
endfunction(maybe_add_gcov_to_libraries)
