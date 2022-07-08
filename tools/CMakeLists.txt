#
# build simple single file tools
#

function(dotool program)
    message(STATUS "Adding tool executable: ${program}")
    add_executable(${program} ${program}_runner.cc)
    target_link_libraries(${program} PRIVATE carp ${ARGN})
    target_include_directories(${program} PRIVATE
     "$<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include;${CMAKE_SOURCE_DIR}/src>")
endfunction()


dotool(compactor)
dotool(fmtcheck)
dotool(rangereader)
if(CARP_H5PART)
    dotool(vpicwriter H5PART)
endif()
