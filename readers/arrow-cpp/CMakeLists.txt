cmake_minimum_required(VERSION 3.10)
project(ParquetReader)

# Set the C++ standard to C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Find the required Arrow and Parquet packages
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

# Include directories
include_directories(${ARROW_INCLUDE_DIR})
include_directories(${PARQUET_INCLUDE_DIR})

# Add executable
add_executable(parquet_to_arrow src/main.cpp)

# Link Arrow and Parquet libraries
target_link_libraries(parquet_to_arrow PRIVATE arrow_shared parquet_shared)
