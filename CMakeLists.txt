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
# 01-Feb-2021  ankushj@andrew.cmu.edu
#

#
# CARP is ... XXX
#

cmake_minimum_required(VERSION 3.1)
project (carp C CXX)

set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# link shared lib with full rpath
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# setup cache variables for ccmake users
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release
            CACHE STRING "Choose the type of build" FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
            "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set (DEBUG_SANITIZER Off CACHE STRING "Sanitizer for debug builds")
set_property (CACHE DEBUG_SANITIZER PROPERTY STRINGS
        "Off" "Address" "Thread")
set (CMAKE_PREFIX_PATH "" CACHE STRING "External dependencies path")
set (BUILD_SHARED_LIBS "OFF" CACHE BOOL "Build a shared library")

#
# some of our tools (e.g. vpicwriter_runner) require H5Part (and HDF5).
# only try to build them if the CARP_H5PART option is set.
#
option(CARP_H5PART "Build CARP tools that require H5Part" OFF)

#
# for best query performance, we want a parallel sort.  CARP
# uses the parallel sort from thread building blocks (TBB).
# This makes TBB a dependency.  If you switch this off, CARP
# will use a slower serialized sort (and TBB is not required).
#
option(CARP_PARALLEL_SORT "Use thread building blocks parallel sort" ON)

#
# sanitizer config (XXX: does not probe compiler to see if sanitizer flags
# are supported... )
#
set (as_flags "-fsanitize=address -O1 -fno-omit-frame-pointer")
set (ts_flags "-fsanitize=thread  -O1 -fno-omit-frame-pointer")
if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    if (${DEBUG_SANITIZER} STREQUAL "Address")
        set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${as_flags}")
        set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${as_flags}")
    elseif (${DEBUG_SANITIZER} STREQUAL "Thread")
        set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${ts_flags}")
        set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${ts_flags}")
    endif ()
endif ()

#
# Find requrired packages
#
find_package(Threads REQUIRED)
find_package(deltafs CONFIG REQUIRED)
if(CARP_H5PART)
    find_package(H5PART REQUIRED)
endif()
if(CARP_PARALLEL_SORT)
    find_package(TBB CONFIG REQUIRED)
else()
    message(WARNING "parallel sort disabled, expect performance impact")
endif()

#
# now add all our subdirectories
#
add_subdirectory(src)
add_subdirectory(tools)
add_subdirectory(benchmarks)
