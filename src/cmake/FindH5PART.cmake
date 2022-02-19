include(FindPackageHandleStandardArgs)

find_path(H5PART_INCLUDE
  NAMES 
  H5Part.h 
  H5PartTypes.h 
  H5PartErrors.h 
  H5PartAttrib.h)

find_library(H5PART_LIBRARY H5Part)

find_package_handle_standard_args(H5PART DEFAULT_MSG H5PART_LIBRARY H5PART_INCLUDE)

mark_as_advanced(H5PART_INCLUDE H5PART_LIBRARY)

if (H5PART_FOUND)
  message(STATUS "H5Part found")
  add_library(H5PART UNKNOWN IMPORTED)
  set_property(TARGET H5PART APPEND PROPERTY IMPORTED_LOCATION "${H5PART_LIBRARY}")
  target_compile_definitions(H5PART INTERFACE PARALLEL_IO MPICH_IGNORE_CXX_SEEK)
elseif (H5PART_FIND_REQUIRED)
  message(FATAL_ERROR "h5part not found")
endif()
