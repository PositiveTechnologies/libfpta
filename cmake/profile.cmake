cmake_minimum_required(VERSION 3.8.2)
cmake_policy(PUSH)
cmake_policy(VERSION 3.8.2)

include(CheckLibraryExists)
check_library_exists(gcov __gcov_flush "" HAVE_GCOV)

option(ENABLE_GCOV
  "Enable integration with gcov, a code coverage program" OFF)

option(ENABLE_GPROF
  "Enable integration with gprof, a performance analyzing tool" OFF)

if(CMAKE_CXX_COMPILER_LOADED)
  include(CheckIncludeFileCXX)
  check_include_file_cxx(valgrind/memcheck.h HAVE_VALGRIND_MEMCHECK_H)
else()
  include(CheckIncludeFile)
  check_include_file(valgrind/memcheck.h HAVE_VALGRIND_MEMCHECK_H)
endif()

option(ENABLE_VALGRIND "Enable integration with valgrind, a memory analyzing tool" OFF)
if(ENABLE_VALGRIND AND NOT HAVE_VALGRIND_MEMCHECK_H)
  message(FATAL_ERROR "ENABLE_VALGRIND option is set but valgrind/memcheck.h is not found")
endif()

option(ENABLE_ASAN
  "Enable AddressSanitizer, a fast memory error detector based on compiler instrumentation" OFF)

cmake_policy(POP)
