set(carp-nw-srcs carp/carp_utils.cc)
set(carp-io-srcs
        reader/range_reader.cc
        reader/file_cache.cc
        reader/manifest_reader.cc
        reader/manifest.cc)

# configure/load in standard modules we plan to use
include(CMakePackageConfigHelpers)
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)

# note: cmake 3.1 and newer define a Threads::Threads imported target
# that we should switch to when we are ready to require 3.1 or better.
# cmake 3.1 was released late 2014.
find_package(Threads REQUIRED)

# common lib is always required
set(CARP_REQUIRED_PACKAGES "pdlfs-common")

# where the cmake package files get installed (for find_package)
set(carp-pkg-loc "share/cmake/carp")

#
# library version set here (e.g. for generating shared libs if we want
#
set(CARP_VERSION_MAJOR 0)
set(CARP_VERSION_MINOR 1)
set(CARP_VERSION_PATCH 0)
set(carp-vers "${CARP_VERSION_MAJOR}.${CARP_VERSION_MINOR}")
set(CARP_VERSION "${carp-vers}.${CARP_VERSION_PATCH}")

# setup and configure carp_config.h.in
configure_file("../include/carp/carp_config.h.in"
        "../include/carp/carp_config.h" @ONLY)

set(carp-all-srcs ${carp-io-srcs} ${carp-nw-srcs} common.cc)

add_library(carp ${carp-all-srcs})
target_include_directories(carp PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories(carp BEFORE PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/../../include>)
target_include_directories(carp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(carp PUBLIC pdlfs-common)

if (PDLFS_TBB)
    find_package(TBB REQUIRED)
    message(STATUS "TBB optimizations will be enabled only if compiler suports them")
    target_link_libraries(carp PUBLIC TBB::tbb)
endif ()

if (THREADS_HAVE_PTHREAD_ARG)
    target_compile_options(carp PUBLIC "-pthread")
endif ()

if (CMAKE_THREAD_LIBS_INIT)
    target_link_libraries(carp PUBLIC "${CMAKE_THREAD_LIBS_INIT}")
endif ()
set_target_properties(carp PROPERTIES VERSION ${CARP_VERSION}
        SOVERSION ${CARP_VERSION_MAJOR})
#
#
# here's the compiler/machine/os dependent stuff...
# this likely isn't that useful (carryover from deltafs/leveldb)
#

# "-pthread" should take care of all thread related options.
# but if we don't have it, fall back to -D_REENTRANT
if (NOT THREADS_HAVE_PTHREAD_ARG)
    target_compile_definitions(carp PUBLIC -D_REENTRANT)
endif ()

# XXX: untested solaris, does it need "-mt"?
# XXX: leveldb also added "-lrt" .. but is that really needed?
if (${CMAKE_SYSTEM_NAME} STREQUAL "SunOS")
    CHECK_CXX_COMPILER_FLAG(-mt flag-mt)
    if (${flag-mt})
        target_compile_options(carp PUBLIC -mt)
    endif ()
endif ()

#
# end of the compiler/machine/os dependent stuff!
#