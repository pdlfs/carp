//
// Created by Ankush J on 2/2/21.
//

// For the __parallel_algo flag
#include <algorithm>

#pragma once

#if defined(__has_include)
  #if !__has_include(<execution>)
#undef PDLFS_TBB
  #endif
#else
#undef PDLFS_TBB
#endif

/* This check also needs to be enabled but fails because of a bug:
 * http://gcc.1065356.n8.nabble.com
 * /committed-0-8-libstdc-Add-update-fix-feature-test-macros-td1682441.html
 * Need a version of g++-9 built after May 2020 for the full check to work
 *
 * Full Check:
 * _has_include(<execution>)
 * __cpp_lib_execution >= 201603
 * __cpp_lib_parallel_algorithm >= 201603
 * From https://gcc.gnu.org/onlinedocs/libstdc++/manual/status.html
 */

// #if !defined(__cpp_lib_execution)
// #undef PDLFS_TBB
// #endif

#if !defined(__cpp_lib_parallel_algorithm)
#undef PDLFS_TBB
#endif
