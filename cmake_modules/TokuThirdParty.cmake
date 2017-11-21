include(ExternalProject)

if (NOT DEFINED LIBJEMALLOC)
    ## add jemalloc with an external project
    set(JEMALLOC_SOURCE_DIR "${TokuDB_SOURCE_DIR}/third_party/jemalloc" CACHE FILEPATH "Where to find jemalloc sources.")
    if (EXISTS "${JEMALLOC_SOURCE_DIR}/configure")
        set(jemalloc_configure_opts "CC=${CMAKE_C_COMPILER}" "--with-jemalloc-prefix=" "--with-private-namespace=tokudb_jemalloc_internal_" "--enable-cc-silence")
        option(JEMALLOC_DEBUG "Build jemalloc with --enable-debug." OFF)
        if (JEMALLOC_DEBUG)
            list(APPEND jemalloc_configure_opts --enable-debug)
        endif ()
        ExternalProject_Add(build_jemalloc
            PREFIX jemalloc
            SOURCE_DIR "${JEMALLOC_SOURCE_DIR}"
            CONFIGURE_COMMAND
                "${JEMALLOC_SOURCE_DIR}/configure" ${jemalloc_configure_opts}
                "--prefix=${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/jemalloc"
        )

        add_library(jemalloc STATIC IMPORTED GLOBAL)
        set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION
            "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/jemalloc/lib/libjemalloc_pic.a")
        add_dependencies(jemalloc build_jemalloc)
        add_library(jemalloc_nopic STATIC IMPORTED GLOBAL)
        set_target_properties(jemalloc_nopic PROPERTIES IMPORTED_LOCATION
            "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/jemalloc/lib/libjemalloc.a")
        add_dependencies(jemalloc_nopic build_jemalloc)

        # detect when we are being built as a subproject
        if (NOT DEFINED MYSQL_PROJECT_NAME_DOCSTRING)
            install(DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/jemalloc/lib" DESTINATION .
                COMPONENT tokukv_libs_extra)
        endif ()
    endif ()
endif ()

## add lzma with an external project
set(xz_configure_opts --with-pic --enable-static)
if (APPLE)
  ## lzma has some assembly that doesn't work on darwin
  list(APPEND xz_configure_opts --disable-assembler)
endif ()

list(APPEND xz_configure_opts "CC=${CMAKE_C_COMPILER} ${CMAKE_C_COMPILER_ARG1}")
if (CMAKE_BUILD_TYPE STREQUAL Debug OR CMAKE_BUILD_TYPE STREQUAL drd)
  list(APPEND xz_configure_opts --enable-debug)
endif ()

set(XZ_SOURCE_DIR "${TokuDB_SOURCE_DIR}/third_party/xz-4.999.9beta" CACHE FILEPATH "Where to find sources for xz (lzma).")
if (NOT EXISTS "${XZ_SOURCE_DIR}/configure")
    message(FATAL_ERROR "Can't find the xz sources.  Please check them out to ${XZ_SOURCE_DIR} or modify XZ_SOURCE_DIR.")
endif ()

if (CMAKE_GENERATOR STREQUAL Ninja)
  ## ninja doesn't understand "$(MAKE)"
  set(SUBMAKE_COMMAND make)
else ()
  ## use "$(MAKE)" for submakes so they can use the jobserver, doesn't
  ## seem to break Xcode...
  set(SUBMAKE_COMMAND $(MAKE))
endif ()

FILE(GLOB XZ_ALL_FILES ${XZ_SOURCE_DIR}/*)
ExternalProject_Add(build_lzma
    PREFIX xz
    DOWNLOAD_COMMAND
        cp -a "${XZ_ALL_FILES}" "<SOURCE_DIR>/"
    CONFIGURE_COMMAND
        "<SOURCE_DIR>/configure" ${xz_configure_opts}
        "--prefix=${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz"
        "--libdir=${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/lib"
    BUILD_COMMAND
        ${SUBMAKE_COMMAND} -C src/liblzma
    INSTALL_COMMAND
        ${SUBMAKE_COMMAND} -C src/liblzma install
)
FILE(GLOB_RECURSE XZ_ALL_FILES_RECURSIVE ${XZ_SOURCE_DIR}/*)
ExternalProject_Add_Step(build_lzma reclone_src # Names of project and custom step
    COMMENT "(re)cloning xz source..."     # Text printed when step executes
    DEPENDERS download configure   # Steps that depend on this step
    DEPENDS   ${XZ_ALL_FILES_RECURSIVE}   # Files on which this step depends
)

set_source_files_properties(
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include/lzma.h"
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include/lzma/base.h"
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include/lzma/bcj.h"
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include/lzma/block.h"
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include/lzma/check.h"
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include/lzma/container.h"
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include/lzma/delta.h"
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include/lzma/filter.h"
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include/lzma/index.h"
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include/lzma/index_hash.h"
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include/lzma/lzma.h"
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include/lzma/stream_flags.h"
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include/lzma/subblock.h"
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include/lzma/version.h"
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include/lzma/vli.h"
  PROPERTIES GENERATED TRUE)

include_directories("${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/include")

add_library(lzma STATIC IMPORTED)
set_target_properties(lzma PROPERTIES IMPORTED_LOCATION
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/xz/lib/liblzma.a")
add_dependencies(lzma build_lzma)


## add snappy with an external project
set(SNAPPY_SOURCE_DIR "${TokuDB_SOURCE_DIR}/third_party/snappy-1.1.2" CACHE FILEPATH "Where to find sources for snappy.")
if (NOT EXISTS "${SNAPPY_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "Can't find the snappy sources.  Please check them out to ${SNAPPY_SOURCE_DIR} or modify SNAPPY_SOURCE_DIR.")
endif ()

FILE(GLOB SNAPPY_ALL_FILES ${SNAPPY_SOURCE_DIR}/*)
ExternalProject_Add(build_snappy
    PREFIX snappy
    DOWNLOAD_COMMAND
        cp -a "${SNAPPY_ALL_FILES}" "<SOURCE_DIR>/"
    CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_AR=${CMAKE_AR}
        -DCMAKE_NM=${CMAKE_NM}
        -DCMAKE_RANLIB=${CMAKE_RANLIB}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        ${USE_PROJECT_CMAKE_MODULE_PATH}
)
FILE(GLOB_RECURSE SNAPPY_ALL_FILES_RECURSIVE ${SNAPPY_SOURCE_DIR}/*)
ExternalProject_Add_Step(build_snappy reclone_src # Names of project and custom step
    COMMENT "(re)cloning snappy source..."     # Text printed when step executes
    DEPENDERS download configure   # Steps that depend on this step
    DEPENDS   ${SNAPPY_ALL_FILES_RECURSIVE}   # Files on which this step depends
)

include_directories("${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/snappy/include")

add_library(snappy STATIC IMPORTED)
set_target_properties(snappy PROPERTIES IMPORTED_LOCATION
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/snappy/lib/libsnappy.a")
add_dependencies(snappy build_snappy)

## add zstd with an external project
set(ZSTD_SOURCE_DIR "${TokuDB_SOURCE_DIR}/third_party/zstd-1.1.0" CACHE FILEPATH "Where to find sources for zstd.")
if (NOT EXISTS "${SNAPPY_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "Can't find the zstd sources.  Please check them out to ${ZSTD_SOURCE_DIR} or modify ZSTD_SOURCE_DIR.")
endif ()

FILE(GLOB ZSTD_ALL_FILES ${ZSTD_SOURCE_DIR}/*)
ExternalProject_Add(build_zstd
    PREFIX zstd
    DOWNLOAD_COMMAND
        cp -a "${ZSTD_ALL_FILES}" "<SOURCE_DIR>/"
    CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        ${USE_PROJECT_CMAKE_MODULE_PATH}
)
FILE(GLOB_RECURSE ZSTD_ALL_FILES_RECURSIVE ${ZSTD_SOURCE_DIR}/*)
ExternalProject_Add_Step(build_zstd reclone_src # Names of project and custom step
    COMMENT "(re)cloning zstd source..."     # Text printed when step executes
    DEPENDERS download configure   # Steps that depend on this step
    DEPENDS   ${ZSTD_ALL_FILES_RECURSIVE}   # Files on which this step depends
)

include_directories("${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/zstd/include")

add_library(zstd STATIC IMPORTED)
set_target_properties(zstd PROPERTIES IMPORTED_LOCATION
  "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_CFG_INTDIR}/zstd/lib/libzstd.a")
add_dependencies(zstd build_zstd)
