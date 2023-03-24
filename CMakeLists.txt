cmake_minimum_required(VERSION 3.25.2)

set(CMAKE_CXX_STANDARD 11)

project(KafkaProject)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(${PROJECT_NAME} main.cpp)

find_package(RdKafka REQUIRED)

target_include_directories(${PROJECT_NAME}
    PUBLIC app_kafka_library/src
    PUBLIC app_utils/src)

target_link_libraries(${PROJECT_NAME}
    PUBLIC app_kafka_library
    PUBLIC app_utils
    RdKafka::rdkafka++)

message(STATUS "Adding subdirectories to package")
add_subdirectory(app_kafka_library/src)
add_subdirectory(app_utils/src)