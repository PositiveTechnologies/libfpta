##
##  Copyright 2016-2019 libfpta authors: please see AUTHORS file.
##
##  This file is part of libfpta, aka "Fast Positive Tables".
##
##  libfpta is free software: you can redistribute it and/or modify
##  it under the terms of the GNU General Public License as published by
##  the Free Software Foundation, either version 3 of the License, or
##  (at your option) any later version.
##
##  libfpta is distributed in the hope that it will be useful,
##  but WITHOUT ANY WARRANTY; without even the implied warranty of
##  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##  GNU General Public License for more details.
##
##  You should have received a copy of the GNU General Public License
##  along with libfpta.  If not, see <http://www.gnu.org/licenses/>.
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
  set(fpta9_crud_timeout 2500)
  set(fpta9_thread_timeout 2000)
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
  set(fpta9_crud_timeout 500)
  set(fpta9_thread_timeout 250)
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
add_ut(fpta8_composite TIMEOUT ${fpta9_huge_timeout} SOURCE 8composite.cxx LIBRARY testutils fpta)
add_ut(fpta9_crud TIMEOUT ${fpta9_crud_timeout} SOURCE 9crud.cxx LIBRARY testutils fpta)
add_ut(fpta9_thread TIMEOUT ${fpta9_thread_timeout} SOURCE 9thread.cxx LIBRARY testutils fpta)
