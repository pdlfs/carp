#
# build simple single file tools
#

function(dotool program exe)
    message(STATUS "Adding tool executable: ${program}")
    add_executable(${exe} ${program}_runner.cc)
    target_link_libraries(${exe} PRIVATE carp ${ARGN})
    target_include_directories(${exe} PRIVATE
     "$<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include;${CMAKE_SOURCE_DIR}/src>")

   install(TARGETS ${exe} DESTINATION bin)
endfunction()


dotool(compactor compactor)
dotool(fmtcheck fmtcheck)
dotool(rangereader range-reader)
if(CARP_H5PART)
    dotool(vpicwriter H5PART)
endif()
