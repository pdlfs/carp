#
# Copyright (c) 2019 Carnegie Mellon University,
# Copyright (c) 2019 Triad National Security, LLC, as operator of
#     Los Alamos National Laboratory.
#
# All rights reserved.
#
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#

#
# CMakeLists.txt  top-level cmake file for CARP
# 1-Feb-2021  ankushj@andrew.cmu.edu
#

#
# CARP is ... XXX
#

cmake_minimum_required (VERSION 2.8.12)

project (CARP)

# add pdlfs-common cmake module directory to the path
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
        "${CMAKE_CURRENT_SOURCE_DIR}/external/pdlfs-common/cmake")

set (PDLFS_COMMON_LIBNAME "pdlfs-common" CACHE
        STRING "Custom name to install pdlfs-common with")
#set (PDLFS_COMMON_DEFINES "DELTAFS" CACHE
#        STRING "Additional defines for this version of pdlfs-common")
#mark_as_advanced (PDLFS_COMMON_LIBNAME PDLFS_COMMON_DEFINES)
set (CARP_CXX_STANDARD "11" CACHE STRING "C++ std to probe")
set (CARP_CXX_STANDARD_REQUIRED "OFF" CACHE BOOL "C++ std must be met")
mark_as_advanced (CARP_CXX_STANDARD CARP_CXX_STANDARD_REQUIRED)
set_property (CACHE CARP_CXX_STANDARD PROPERTY STRINGS "98" "11" "17")

# note: CMAKE_CXX_STANDARD is not defined until cmake 3.1, and will be
# ignored by cmake 3.0 and before.
#
include (CheckCXXCompilerFlag)
if (CMAKE_VERSION VERSION_LESS "3.1")
    set (cxxstdflag "-std=c++${CARP_CXX_STANDARD}")
    CHECK_CXX_COMPILER_FLAG (${cxxstdflag} flag${cxxstdflag})
    if (${flag${cxxstdflag}})
        add_compile_options (${cxxstdflag})
    else ()
        if (CARP_CXX_STANDARD_REQUIRED)
            message (FATAL_ERROR "Fail to enable CXX ${CARP_CXX_STANDARD}")
        endif ()
    endif ()
else ()
    set (CMAKE_CXX_STANDARD ${CARP_CXX_STANDARD})
    set (CMAKE_CXX_STANDARD_REQUIRED ${CARP_CXX_STANDARD_REQUIRED})
endif ()

# pull in pdlfs handling of generic cmake config
include (cmake-options)

# handle all the common PDLFS options using cmake/pdlfs-options.cmake
include (pdlfs-options)

set (PDLFS_COMMON_INTREE "OFF" CACHE BOOL
        "Build in-tree common lib (for devel)")

# we build the in-tree pdlfs-common if DELTAFS_COMMON_INTREE is set,
# otherwise we look for one already built in our install or prefix path.
#
if (PDLFS_COMMON_INTREE)
    add_subdirectory (external/pdlfs-common/src)
else ()
    message ("OK ${PDLFS_COMPONENT_CFG}") # XXXCDC
    find_package (pdlfs-common REQUIRED COMPONENTS ${PDLFS_COMPONENT_CFG})
endif ()


add_subdirectory(src)
add_subdirectory(tools)
add_subdirectory(benchmarks)

