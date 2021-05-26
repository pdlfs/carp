set(BUILD_TARGETS stdstr sstread)
foreach(TARGET ${BUILD_TARGETS})
    add_executable(${TARGET} ${TARGET}.cc)
    target_link_libraries(${TARGET} PRIVATE carp)
    target_include_directories(${TARGET} PRIVATE
            "$<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include;${CMAKE_SOURCE_DIR}/src>")
    install(TARGETS ${TARGET} RUNTIME DESTINATION bin)
endforeach()
