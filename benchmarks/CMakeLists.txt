add_executable(stdstr stdstr.cc)
target_link_libraries(stdstr carp pdlfs-common)

install(TARGETS stdstr RUNTIME DESTINATION bin)