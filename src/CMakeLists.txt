#
# CMakeLists.txt  cmake file for carp src
#

set (carp-srcs
     #
     # nw srcs
     #
     carp/carp_utils.cc
     #
     # io srcs
     #
     reader/range_reader.cc reader/file_cache.cc reader/manifest_reader.cc
     reader/manifest.cc reader/plfs_wrapper.cc reader/sliding_sorter.cc
     reader/reader_base.cc reader/query_utils.cc reader/compactor.cc
     #
     # additional srcs
     #
     common.cc reader/fmt_checker.cc)

#
# load in helpers; set where to install cmake pkg files (for find_package)
#
include(CMakePackageConfigHelpers)
set(carp-pkg-loc "share/cmake/carp")

#
# library version set here (e.g. for shared libs).
#
set(CARP_VERSION_MAJOR 0)
set(CARP_VERSION_MINOR 1)
set(CARP_VERSION_PATCH 0)
set(carp-vers "${CARP_VERSION_MAJOR}.${CARP_VERSION_MINOR}")
set(CARP_VERSION "${carp-vers}.${CARP_VERSION_PATCH}")

#
# use carp_config.h.in template to create carp_config.h
#
configure_file("../include/carp/carp_config.h.in"
               "../include/carp/carp_config.h" @ONLY)

#
# create carp library target and setup its properties
#
add_library(carp ${carp-srcs})

#
# includes: main include dir once installed, but put local include
# first in the path when building so we don't pick up stale files
# from a previous build in the main include directory.
# also add current dir, since many files in other dirs include "common.h"
#
target_include_directories(carp PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories(carp BEFORE PUBLIC
          $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
          $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/../include>)
target_include_directories(carp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

#
# features: common.h needs constexpr to compile internal stuff
#
target_compile_features(carp PRIVATE cxx_constexpr)

#
# libs: use Threads::Threads imported target and TBB (if enabled).
# add our shared lib version to our target's properties.
#
target_link_libraries(carp PUBLIC deltafs Threads::Threads)
if (CARP_PARALLEL_SORT)
    target_link_libraries(carp PUBLIC TBB::tbb)
endif ()
set_target_properties(carp PROPERTIES VERSION   ${CARP_VERSION}
                                      SOVERSION ${CARP_VERSION_MAJOR})

#
# packaging stuff for external users who want to find_package() our lib
#
write_basic_package_version_file ("carp-config-version.cmake"
        VERSION ${CARP_VERSION}
        COMPATIBILITY AnyNewerVersion)
configure_file (carp-config.cmake.in carp-config.cmake @ONLY)

#
# "make install" rules
#
install (TARGETS carp EXPORT carp-targets
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)
install (EXPORT carp-targets
        DESTINATION ${carp-pkg-loc}
        FILE "carp-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/carp-config.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/carp-config-version.cmake"
        DESTINATION ${carp-pkg-loc} )
##install (FILES ../include/carp/FOO.h
##         DESTINATION include/carp)

add_subdirectory(reader)
