set(TARGETS compactor fmtcheck rangereader)

find_package(H5PART REQUIRED)

if (H5PART_FOUND)
  set(TARGETS ${TARGETS} vpicwriter)
endif()

foreach(TARGET ${TARGETS})
  message("Building executable: " ${TARGET})
  add_executable(${TARGET} ${TARGET}_runner.cc)
  target_link_libraries(${TARGET} PRIVATE carp H5PART)
  target_include_directories(${TARGET} PRIVATE
          "$<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include;${CMAKE_SOURCE_DIR}/src>")
endforeach()

