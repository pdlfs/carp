add_executable(range-reader rangereader_runner.cc)

target_link_libraries(range-reader PRIVATE carp)

target_include_directories(range-reader PRIVATE
        "$<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include;${CMAKE_SOURCE_DIR}/src>")
