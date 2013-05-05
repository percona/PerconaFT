add_library(lz4 STATIC lz4 lz4hc)
add_space_separated_property(TARGET lz4 COMPILE_FLAGS
    "-fPIC -Wcast-align -Wno-missing-prototypes -Wno-missing-declarations -Wno-strict-prototypes -Wno-implicit-function-declaration")
