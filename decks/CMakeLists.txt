function(build_vpic_deck vpic target)
  # Construct the path to the vpic file
  set(vpic_path "${CMAKE_INSTALL_PREFIX}/bin/${vpic}")

  if(NOT EXISTS "${vpic_path}")
    message(FATAL_ERROR "File ${vpic_path} does not exist. Exiting.")
    return()
  endif()

  set(target_path "${CMAKE_CURRENT_SOURCE_DIR}/${target}")
  get_filename_component(target_name ${target} NAME_WE)
  set(target_bin "${CMAKE_BINARY_DIR}/${target_name}.${CMAKE_SYSTEM_NAME}")

  message(STATUS "Building VPIC deck ${target_bin}...")

  add_custom_target(${target_name} ALL
    COMMAND ${vpic_path} ${target_path}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS ${vpic_path} ${target_path}
    COMMENT "Building VPIC deck ${target_name}..."
  )

  message(STATUS "VPIC deck ${target_name} built successfully.")
  install(PROGRAMS ${target_bin} DESTINATION decks)

endfunction()

build_vpic_deck(vpic ./vpic-magrecon-deck-modified-3584M/reconnection.cc)
build_vpic_deck(vpic ./vpic-magrecon-deck-modified-3584M/reconnection512.cc)
