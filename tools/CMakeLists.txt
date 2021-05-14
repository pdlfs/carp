set(TARGETS compactor fmtcheck rangereader)
foreach(TARGET ${TARGETS})
  message("Building executable: " ${TARGET})
  add_executable(${TARGET} ${TARGET}_runner.cc fmtcheck_runner.cc)
  target_link_libraries(${TARGET} PRIVATE carp)
  target_include_directories(${TARGET} PRIVATE
          "$<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include;${CMAKE_SOURCE_DIR}/src>")
endforeach()