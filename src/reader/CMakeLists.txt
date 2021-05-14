add_executable(reader-test reader-test.cc)
target_link_libraries(reader-test carp)
target_include_directories(reader-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
# some ctest hook, not tested
add_test(reader-test reader-test)