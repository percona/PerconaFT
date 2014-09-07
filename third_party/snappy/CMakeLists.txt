file( GLOB_RECURSE SOURCE RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
        "*.cpp"
        )
add_definitions("-Wno-sign-compare -Wno-unused-function -Wno-unused-parameter -fPIC")
add_library(snappyStatic STATIC ${SOURCE})
set_target_properties(snappyStatic PROPERTIES OUTPUT_NAME snappy)
