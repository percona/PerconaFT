if(BUILD_TESTING)
  file(GLOB srcs RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" *.cc)
  foreach(src ${srcs})
    get_filename_component(base ${src} NAME_WE)
    list(APPEND tests ${base})
  endforeach(src)

  foreach(test ${tests})
    add_executable(${test} ${test}.cc)
    target_link_libraries(${test} util ${LIBTOKUPORTABILITY})
  endforeach(test)

  add_helgrind_test(util helgrind_test_partitioned_counter $<TARGET_FILE:test_partitioned_counter>)
  add_helgrind_test(util helgrind_test_partitioned_counter_5833 $<TARGET_FILE:test_partitioned_counter_5833>)

  foreach(test ${tests})
    add_test(util/${test} ${test})
  endforeach(test)

endif(BUILD_TESTING)
