if (CMAKE_PROJECT_NAME STREQUAL TokuDB)
  cmake_minimum_required(VERSION 2.8.12 FATAL_ERROR)
endif()
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules")

project(TokuDB)

# suppress -rdynamic
set(CMAKE_SHARED_LIBRARY_LINK_C_FLAGS "")
set(CMAKE_SHARED_LIBRARY_LINK_CXX_FLAGS "")

# See: https://jira.percona.com/browse/TDB-93
IF(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR
   (CMAKE_COMPILER_IS_GNUCXX AND
    NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)) # g++-9 or newer
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-address-of-packed-member")
  SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-address-of-packed-member")
ENDIF()

IF(CMAKE_COMPILER_IS_GNUCXX AND
   NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.0) # g++-7 or newer
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wimplicit-fallthrough=3")
ENDIF()

# detect when we are being built as a subproject
if (DEFINED MYSQL_PROJECT_NAME_DOCSTRING)
  add_definitions(-DMYSQL_TOKUDB_ENGINE=1)
  add_definitions(-DMYSQL_VERSION_ID=${MYSQL_VERSION_ID})
  # Extended PFS instrumentation:
  # -DTOKU_PFS_MUTEX_EXTENDED_CACHETABLEMMUTEX=1
  if (WITH_PERFSCHEMA_STORAGE_ENGINE)
    add_definitions(-DTOKU_MYSQL_WITH_PFS)
  endif ()
  include_directories(${CMAKE_SOURCE_DIR}/include)
  # Match exactly the flag `-DENABLED_DEBUG_SYNC` to prevent matching
  # `-DENABLED_DEBUG_SYNC_EXTRA` or `EXTRA-DENABLED_DEBUG_SYNC`
  if ((CMAKE_BUILD_TYPE MATCHES "Debug") AND
      (CMAKE_CXX_FLAGS_DEBUG MATCHES "( |^)-DENABLED_DEBUG_SYNC( |$)"))
    include_directories(${CMAKE_SOURCE_DIR}/sql)
  endif ()
endif ()

## Versions of gcc >= 4.9.0 require special version of 'ar' and 'ranlib' for
## link-time optimizations to work properly.
##
## From https://gcc.gnu.org/gcc-4.9/changes.html:
##
##     When using a linker plugin, compiling with the -flto option now
##     generates slim objects files (.o) which only contain intermediate
##     language representation for LTO. Use -ffat-lto-objects to create
##     files which contain additionally the object code. To generate
##     static libraries suitable for LTO processing, use gcc-ar and
##     gcc-ranlib; to list symbols from a slim object file use
##     gcc-nm. (Requires that ar, ranlib and nm have been compiled with
##     plugin support.)
if ((CMAKE_CXX_COMPILER_ID STREQUAL GNU) AND
    NOT (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.9.0"))
  find_program(gcc_ar "gcc-ar")
  if (gcc_ar)
    set(CMAKE_AR "${gcc_ar}")
  endif ()
  find_program(gcc_ranlib "gcc-ranlib")
  if (gcc_ranlib)
    set(CMAKE_RANLIB "${gcc_ranlib}")
  endif ()
endif()

include(TokuFeatureDetection)
include(TokuSetupCompiler)
include(TokuSetupCTest)
include(TokuThirdParty)

set(TOKU_CMAKE_SCRIPT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(TokuMergeLibs)

## need a way to change the name of libs we build
set(LIBTOKUPORTABILITY "tokuportability" CACHE STRING "Name of libtokuportability.so")
set(LIBTOKUDB "tokufractaltree" CACHE STRING "Name of libtokufractaltree.so")

set(INSTALL_LIBDIR "lib" CACHE STRING "where to install libs")

if (USE_VALGRIND AND NOT VALGRIND_INCLUDE_DIR MATCHES NOTFOUND)
  include_directories(
    ${VALGRIND_INCLUDE_DIR}
    )
endif()
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/portability
  ${CMAKE_CURRENT_SOURCE_DIR}  ## so you can include <ft/ft-ops.h> from inside src/
  ${CMAKE_CURRENT_BINARY_DIR}  ## for logging code
  )
## include where config.h will be generated
include_directories(${CMAKE_CURRENT_BINARY_DIR}/portability)

## build db.h and include where it will be generated
add_subdirectory(buildheader)
include_directories(BEFORE ${CMAKE_CURRENT_BINARY_DIR}/buildheader)

## default includes and libraries
include_directories(SYSTEM
  /usr/local/include
  ${ZLIB_INCLUDE_DIRS}
  )

## add subdirectories
add_subdirectory(util)
add_subdirectory(portability)
add_subdirectory(ft)
add_subdirectory(locktree)
add_subdirectory(src)
add_subdirectory(ftcxx)
add_subdirectory(tools)

install(
  FILES README.md COPYING.AGPLv3 COPYING.GPLv2 PATENTS
  DESTINATION .
  COMPONENT tokukv_misc
  )

## build tags
include(TokuBuildTagDatabases)
