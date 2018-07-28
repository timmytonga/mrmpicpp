cmake_minimum_required(VERSION 3.9)
project(mapreducecpp)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
set(CMAKE_CXX_COMPILE_FLAGS ${CMAKE_CXX_COMPILE_FLAGS}
        ${MPI_COMPILE_FLAGS})
set(CMAKE_CXX_LINK_FLAGS ${CMAKE_CXX_LINK_FLAGS}
        ${MPI_LINK_FLAGS})

find_package(MPI REQUIRED)
add_definitions(-DOMPI_SKIP_MPICXX)

add_executable(mapreducecpp
        wordcountmain.cpp
        mapreduce.cpp
        keyvalue.cpp)

target_link_libraries(mapreducecpp
        ${CMAKE_DL_LIBS}
        ${MPI_LIBRARIES})

include_directories(SYSTEM
        ${MPI_INCLUDE_PATH})