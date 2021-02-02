//
// Created by Ankush J on 2/2/21.
//

#pragma once

#if defined(__has_include)
  #if !__has_include(<execution>)
#undef PDLFS_TBB
  #endif
#else
#undef PDLFS_TBB
#endif

#if !defined(__cpp_lib_execution)
#undef PDLFS_TBB
#endif
