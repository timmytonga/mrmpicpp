cmake_minimum_required(VERSION 3.9)
project(mapreducecpp)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

add_executable(mapreducecpp workq.c wordcountmpi2.cpp)
target_link_libraries(mapreducecpp ${CMAKE_DL_LIBS})