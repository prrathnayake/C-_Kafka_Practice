cmake_minimum_required(VERSION 3.25.2)

file(GLOB files
     "*/*.h"
     "*/*.cpp"
)

project(app_utils)

add_library(${PROJECT_NAME} SHARED ${files})



