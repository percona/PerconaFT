# ################################################################
# zstd - Makefile
# Copyright (C) Yann Collet 2014-2016
# All rights reserved.
#
# BSD license
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice, this
#   list of conditions and the following disclaimer in the documentation and/or
#   other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# You can contact the author at :
#  - zstd homepage : http://www.zstd.net/
# ################################################################

# Get library version based on information from input content (use regular exp)
function(GetLibraryVersion _content _outputVar1 _outputVar2 _outputVar3)
    string(REGEX MATCHALL ".*define ZSTD_VERSION_MAJOR+.* ([0-9]+).*define ZSTD_VERSION_MINOR+.* ([0-9]+).*define ZSTD_VERSION_RELEASE+.* ([0-9]+)" VERSION_REGEX "${_content}")
    SET(${_outputVar1} ${CMAKE_MATCH_1} PARENT_SCOPE)
    SET(${_outputVar2} ${CMAKE_MATCH_2} PARENT_SCOPE)
    SET(${_outputVar3} ${CMAKE_MATCH_3} PARENT_SCOPE)
endfunction()

PROJECT(libzstd)

SET(CMAKE_INCLUDE_CURRENT_DIR TRUE)

# Define library directory, where sources and header files are located
INCLUDE_DIRECTORIES(common)

# Read file content
FILE(READ zstd.h HEADER_CONTENT)

# Parse version
GetLibraryVersion("${HEADER_CONTENT}" LIBVER_MAJOR LIBVER_MINOR LIBVER_RELEASE)
MESSAGE("ZSTD VERSION ${LIBVER_MAJOR}.${LIBVER_MINOR}.${LIBVER_RELEASE}")

SET(Sources
        common/entropy_common.c
        common/zstd_common.c
        common/xxhash.c
        common/fse_decompress.c
        compress/fse_compress.c
        compress/huf_compress.c
        compress/zbuff_compress.c
        compress/zstd_compress.c
        decompress/huf_decompress.c
        decompress/zbuff_decompress.c
        decompress/zstd_decompress.c
        dictBuilder/divsufsort.c
        dictBuilder/zdict.c)

SET(Headers
        common/bitstream.h
        common/error_private.h
        common/error_public.h
        common/fse.h
        common/huf.h
        common/mem.h
        common/zbuff.h
        common/zstd_internal.h
        zstd.h
        dictBuilder/zdict.h)

IF (ZSTD_LEGACY_SUPPORT)
    SET(LIBRARY_LEGACY_DIR legacy)
    INCLUDE_DIRECTORIES(${LIBRARY_LEGACY_DIR})

    SET(Sources ${Sources}
            ${LIBRARY_LEGACY_DIR}/zstd_v01.c
            ${LIBRARY_LEGACY_DIR}/zstd_v02.c
            ${LIBRARY_LEGACY_DIR}/zstd_v03.c
            ${LIBRARY_LEGACY_DIR}/zstd_v04.c
            ${LIBRARY_LEGACY_DIR}/zstd_v05.c
            ${LIBRARY_LEGACY_DIR}/zstd_v06.c
            ${LIBRARY_LEGACY_DIR}/zstd_v07.c)

    SET(Headers ${Headers}
            ${LIBRARY_LEGACY_DIR}/zstd_legacy.h
            ${LIBRARY_LEGACY_DIR}/zstd_v01.h
            ${LIBRARY_LEGACY_DIR}/zstd_v02.h
            ${LIBRARY_LEGACY_DIR}/zstd_v03.h
            ${LIBRARY_LEGACY_DIR}/zstd_v04.h
            ${LIBRARY_LEGACY_DIR}/zstd_v05.h
            ${LIBRARY_LEGACY_DIR}/zstd_v06.h
            ${LIBRARY_LEGACY_DIR}/zstd_v07.h)
ENDIF (ZSTD_LEGACY_SUPPORT)

IF (MSVC)
    SET(MSVC_RESOURCE_DIR ${ROOT_DIR}/build/VS2010/zstdlib)
    SET(PlatformDependResources ${MSVC_RESOURCE_DIR}/zstdlib.rc)
ENDIF (MSVC)

# Split project to static and shared libraries build
ADD_LIBRARY(libzstd_static STATIC ${Sources} ${Headers} ${PlatformDependResources})
ADD_LIBRARY(libzstd_shared SHARED ${Sources} ${Headers} ${PlatformDependResources})
install(
  TARGETS libzstd_static
  DESTINATION lib
  )

install(
  FILES zstd.h
  DESTINATION include
  )

# Add specific compile definitions for MSVC project
IF (MSVC)
    SET_TARGET_PROPERTIES(libzstd_static PROPERTIES COMPILE_DEFINITIONS "ZSTD_HEAPMODE=0;_CRT_SECURE_NO_WARNINGS")
    SET_TARGET_PROPERTIES(libzstd_shared PROPERTIES COMPILE_DEFINITIONS "ZSTD_DLL_EXPORT=1;ZSTD_HEAPMODE=0;_CONSOLE;_CRT_SECURE_NO_WARNINGS")
ENDIF (MSVC)

# Define library base name
IF (MSVC)
    SET(LIBRARY_BASE_NAME zstdlib)
ELSE ()
    SET(LIBRARY_BASE_NAME libzstd)
ENDIF (MSVC)

# Define static and shared library names
SET(STATIC_LIBRARY_OUTPUT_NAME ${LIBRARY_BASE_NAME})
SET(SHARED_LIBRARY_OUTPUT_NAME ${LIBRARY_BASE_NAME}.${LIBVER_MAJOR}.${LIBVER_MINOR}.${LIBVER_RELEASE})

IF (MSVC)
    IF (CMAKE_SIZEOF_VOID_P MATCHES "8")
        SET(STATIC_LIBRARY_OUTPUT_NAME ${STATIC_LIBRARY_OUTPUT_NAME}_x64)
        SET(SHARED_LIBRARY_OUTPUT_NAME ${SHARED_LIBRARY_OUTPUT_NAME}_x64)
    ELSE ()
        SET(STATIC_LIBRARY_OUTPUT_NAME ${STATIC_LIBRARY_OUTPUT_NAME}_x86)
        SET(SHARED_LIBRARY_OUTPUT_NAME ${SHARED_LIBRARY_OUTPUT_NAME}_x86)
    ENDIF (CMAKE_SIZEOF_VOID_P MATCHES "8")
ENDIF (MSVC)

SET_TARGET_PROPERTIES(
        libzstd_static
        PROPERTIES
        PREFIX ""
        OUTPUT_NAME ${STATIC_LIBRARY_OUTPUT_NAME})

SET_TARGET_PROPERTIES(
        libzstd_shared
        PROPERTIES
        PREFIX ""
        OUTPUT_NAME ${SHARED_LIBRARY_OUTPUT_NAME})

IF (UNIX)
    # Create symlinks and setup this files
    SET(SHARED_LIBRARY_LINK ${SHARED_LIBRARY_OUTPUT_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
    SET(SHARED_LIBRARY_SYMLINK1 ${LIBRARY_BASE_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
    SET(SHARED_LIBRARY_SYMLINK2 ${LIBRARY_BASE_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}.${LIBVER_MAJOR})

    SET(SHARED_LIBRARY_LINK_PATH ${CMAKE_CURRENT_BINARY_DIR}/${SHARED_LIBRARY_LINK})
    SET(SHARED_LIBRARY_SYMLINK1_PATH ${CMAKE_CURRENT_BINARY_DIR}/${SHARED_LIBRARY_SYMLINK1})
    SET(SHARED_LIBRARY_SYMLINK2_PATH ${CMAKE_CURRENT_BINARY_DIR}/${SHARED_LIBRARY_SYMLINK2})

    if (EXISTS ${SHARED_LIBRARY_SYMLINK1_PATH})
        FILE(REMOVE ${SHARED_LIBRARY_SYMLINK1_PATH})
    endif (EXISTS ${SHARED_LIBRARY_SYMLINK1_PATH})

    if (EXISTS ${SHARED_LIBRARY_SYMLINK2_PATH})
        FILE(REMOVE ${SHARED_LIBRARY_SYMLINK2_PATH})
    endif (EXISTS ${SHARED_LIBRARY_SYMLINK2_PATH})

    ADD_CUSTOM_COMMAND(TARGET libzstd_shared POST_BUILD
            COMMAND ln -s ${SHARED_LIBRARY_LINK} ${SHARED_LIBRARY_SYMLINK1}
            DEPENDS ${SHARED_LIBRARY_LINK_PATH}
            COMMENT "Generating symbolic link")

    ADD_CUSTOM_COMMAND(TARGET libzstd_shared POST_BUILD
            COMMAND ln -s ${SHARED_LIBRARY_LINK} ${SHARED_LIBRARY_SYMLINK2}
            DEPENDS ${SHARED_LIBRARY_LINK_PATH}
            COMMENT "Generating symbolic link")

ENDIF (UNIX)
