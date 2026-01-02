cmake_minimum_required(VERSION 3.15)
project(TelemetryProducer)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# -----------------------------
# vcpkg toolchain
# -----------------------------
set(CMAKE_TOOLCHAIN_FILE
    "C:/Users/Admin/vcpkg/scripts/buildsystems/vcpkg.cmake"
    CACHE STRING "Vcpkg toolchain file"
)

# -----------------------------
# MongoDB BSON ONLY (no mongocxx)
# -----------------------------
find_package(bsoncxx CONFIG REQUIRED)

# -----------------------------
# TCP Producer executable
# -----------------------------
add_executable(tcp_producer
    main.cpp
    # data_generator.cpp
)

set_target_properties(tcp_producer PROPERTIES
    WIN32_EXECUTABLE OFF
)

target_compile_features(tcp_producer PUBLIC cxx_std_20)

target_link_libraries(tcp_producer
    PRIVATE
        mongo::bsoncxx_shared
        ws2_32
)
