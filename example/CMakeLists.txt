cmake_minimum_required(VERSION 3.14)

project(example LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include(FetchContent)

#boost
find_package(Boost CONFIG REQUIRED COMPONENTS system) # header-only `asio`, `beast`

#quickfix
option(QUICKFIX_SHARED_LIBS "" OFF)
option(QUICKFIX_EXAMPLES "" OFF)
option(QUICKFIX_TESTS "" OFF)
FetchContent_Declare(
    QuickFIX
    GIT_REPOSITORY https://github.com/quickfix/quickfix.git
    GIT_TAG master
    GIT_SHALLOW 1
)
FetchContent_MakeAvailable(QuickFIX)

add_library(FIXAsio INTERFACE)
target_include_directories(FIXAsio INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(FIXAsio INTERFACE quickfix ${Boost_LIBRARIES})


add_executable(acceptor acceptor.cpp)
target_link_libraries(acceptor PRIVATE FIXAsio)

add_executable(initiator initiator.cpp)
target_link_libraries(initiator PRIVATE FIXAsio)
