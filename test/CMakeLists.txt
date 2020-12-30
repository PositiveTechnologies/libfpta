##
##  Fast Positive Tables (libfpta), aka Позитивные Таблицы.
##  Copyright 2016-2020 Leonid Yuriev <leo@yuriev.ru>
##
##  Licensed under the Apache License, Version 2.0 (the "License");
##  you may not use this file except in compliance with the License.
##  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##

include_directories(${UT_INCLUDE_DIRECTORIES})

if(BUILD_SHARED_LIBS)
  add_definitions("-DGTEST_LINKED_AS_SHARED_LIBRARY=1")
endif()

# LY: reduce compilation time (significantly on Elbrus)
if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug"
    AND NOT CMAKE_INTERPROCEDURAL_OPTIMIZATION AND NOT LTO_ENABLED)
  if (CC_HAS_OMINIMAL)
    add_compile_options("-Ominimal")
  elseif(CMAKE_COMPILER_IS_CLANG OR CMAKE_COMPILER_IS_GNUCC)
    add_compile_options("-O1")
  endif()
endif()

add_library(testutils STATIC
  keygen.cxx
  keygen.hpp
  fpta_test.h
  tools.hpp
  tools.cxx)
target_link_libraries(testutils PUBLIC fpta ${UT_LIBRARIES})

if(${CMAKE_SYSTEM_NAME} STREQUAL "Windows" OR CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(fpta_small_timeout 10)
  set(fpta3_smoke_timeout 2000)
  set(fpta5_key_timeout 30)
  set(fpta6_index_primary_timeout 2000)
  set(fpta6_index_secondary_timeout 4500)
  set(fpta7_cursor_primary_timeout 3000)
  set(fpta7_cursor_secondary_unique_timeout 2000)
  set(fpta7_cursor_secondary_withdups_timeout 3000)
  set(fpta8_crud_timeout 2500)
  set(fpta8_thread_timeout 2000)
  set(fpta9_huge_timeout 7200)
else()
  set(fpta_small_timeout 3)
  set(fpta3_smoke_timeout 15)
  set(fpta5_key_timeout 5)
  set(fpta6_index_primary_timeout 60)
  set(fpta6_index_secondary_timeout 500)
  set(fpta7_cursor_primary_timeout 500)
  set(fpta7_cursor_secondary_unique_timeout 500)
  set(fpta7_cursor_secondary_withdups_timeout 1500)
  set(fpta8_crud_timeout 500)
  set(fpta8_thread_timeout 250)
  set(fpta9_huge_timeout 3600)
endif()

add_executable(fpta_c_mode c_mode.c)
target_link_libraries(fpta_c_mode fpta)
if(LIBFPTA_TYPE STREQUAL "PILE")
  set_target_properties(fpta_c_mode PROPERTIES LINKER_LANGUAGE CXX)
endif()

add_ut(fpta0_corny TIMEOUT ${fpta_small_timeout} SOURCE 0corny.cxx LIBRARY testutils fpta)
add_ut(fpta1_open TIMEOUT ${fpta_small_timeout} SOURCE 1open.cxx LIBRARY testutils fpta)
add_ut(fpta2_schema TIMEOUT ${fpta_small_timeout} SOURCE 2schema.cxx LIBRARY testutils fpta)
add_ut(fpta3_smoke TIMEOUT ${fpta3_smoke_timeout} SOURCE 3smoke.cxx LIBRARY testutils fpta)
add_ut(fpta4_data TIMEOUT ${fpta_small_timeout} SOURCE 4data.cxx LIBRARY testutils fpta)
add_ut(fpta5_key TIMEOUT ${fpta5_key_timeout} SOURCE 5key.cxx LIBRARY testutils fpta)
add_ut(fpta6_index_primary TIMEOUT ${fpta6_index_primary_timeout} SOURCE 6index_primary.cxx LIBRARY testutils fpta)
add_ut(fpta6_index_secondary TIMEOUT ${fpta6_index_secondary_timeout} SOURCE 6index_secondary.cxx LIBRARY testutils fpta)
add_ut(fpta7_cursor_primary TIMEOUT ${fpta7_cursor_primary_timeout} SOURCE 7cursor_primary.cxx LIBRARY testutils fpta)
add_ut(fpta7_cursor_secondary_unique TIMEOUT ${fpta7_cursor_secondary_unique_timeout} SOURCE 7cursor_secondary_unique.cxx cursor_secondary.hpp LIBRARY testutils fpta)
add_ut(fpta7_cursor_secondary_withdups TIMEOUT ${fpta7_cursor_secondary_withdups_timeout} SOURCE 7cursor_secondary_withdups.cxx cursor_secondary.hpp LIBRARY testutils fpta)
add_ut(fpta8_crud TIMEOUT ${fpta8_crud_timeout} SOURCE 8crud.cxx LIBRARY testutils fpta)
add_ut(fpta8_thread TIMEOUT ${fpta8_thread_timeout} SOURCE 8thread.cxx LIBRARY testutils fpta)
add_ut(fpta9_composite TIMEOUT ${fpta9_huge_timeout} SOURCE 9composite.cxx LIBRARY testutils fpta)
