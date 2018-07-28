##
##  Copyright 2016-2017 libfptu authors: please see AUTHORS file.
##
##  This file is part of libfptu, aka "Fast Positive Tuples".
##
##  libfptu is free software: you can redistribute it and/or modify
##  it under the terms of the GNU General Public License as published by
##  the Free Software Foundation, either version 3 of the License, or
##  (at your option) any later version.
##
##  libfptu is distributed in the hope that it will be useful,
##  but WITHOUT ANY WARRANTY; without even the implied warranty of
##  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##  GNU General Public License for more details.
##
##  You should have received a copy of the GNU General Public License
##  along with libfptu.  If not, see <http://www.gnu.org/licenses/>.
##

##
##  libfptu = { Fast Positive Tuples, aka Позитивные Кортежи }
##
##  The kind of lightweight linearized tuples, which are extremely handy
##  to machining, including cases with shared memory.
##  Please see README.md at https://github.com/leo-yuriev/libfptu
##
##  The Future will Positive. Всё будет хорошо.
##
##  "Позитивные Кортежи" дают легковесное линейное представление небольших
##  JSON-подобных структур в экстремально удобной для машины форме,
##  в том числе при размещении в разделяемой памяти.
##

find_program(RE2C re2c)
if(RE2C AND NOT RE2C_VERSION)
  execute_process(COMMAND ${RE2C} --version
    OUTPUT_VARIABLE re2c_version_output
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE rc)
  if(rc EQUAL 0)
    string(REGEX MATCH "^re2c ([0-9]+\\.[0-9]+(\\.[0-9]+)*)" re2c_version_valid ${re2c_version_output})
    if(re2c_version_valid)
      set(RE2C_VERSION ${CMAKE_MATCH_1})
      message(STATUS "Found ${RE2C} version ${RE2C_VERSION}")
    endif()
  endif()
endif()

find_program(LEMON lemon)
if(LEMON AND NOT LEMON_VERSION)
  execute_process(COMMAND ${LEMON} -x
    OUTPUT_VARIABLE lemon_version_output
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE rc)
  if(rc EQUAL 0)
    string(REGEX MATCH "^Lemon version ([0-9]+\\.[0-9]+(\\.[0-9]+)*)" lemon_version_valid ${lemon_version_output})
    if(lemon_version_valid)
      set(LEMON_VERSION ${CMAKE_MATCH_1})
      message(STATUS "Found ${LEMON} version ${LEMON_VERSION}")
    endif()
  endif()
endif()

list(FIND CMAKE_CXX_COMPILE_FEATURES cxx_std_17 HAS_CXX17)
if(RE2C AND RE2C_VERSION VERSION_GREATER_EQUAL 1.0
    AND LEMON AND LEMON_VERSION VERSION_GREATER_EQUAL 1.0
    AND HAS_CXX17 GREATER -1)
  add_executable(bomstrip bomstrip.cxx)
  include_directories(${CMAKE_CURRENT_BINARY_DIR})

  add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/lexer.cxx
    MAIN_DEPENDENCY lexer.re2c
    DEPENDS bomstrip
    COMMAND ${CMAKE_COMMAND} -E remove -f lexer.re lexer.cxx
    COMMAND ${CMAKE_CURRENT_BINARY_DIR}/bomstrip < ${CMAKE_CURRENT_SOURCE_DIR}/lexer.re2c > lexer.re
    COMMAND ${RE2C} -W -Werror --utf-8 --bit-vectors -i lexer.re -o lexer.cxx
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generate schema-lexer by ${RE2C}"
    )

  add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/parser.cxx ${CMAKE_CURRENT_BINARY_DIR}/grammar.h
    MAIN_DEPENDENCY grammar.lemon
    DEPENDS bomstrip
    COMMAND ${CMAKE_COMMAND} -E remove -f grammar.c grammar.h grammar.y parser.cxx
    COMMAND ${CMAKE_CURRENT_BINARY_DIR}/bomstrip < ${CMAKE_CURRENT_SOURCE_DIR}/grammar.lemon > grammar.y
    COMMAND ${LEMON} -l -s grammar.y
    COMMAND ${CMAKE_COMMAND} -E rename grammar.c parser.cxx
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generate schema-parser by ${LEMON}"
    )

  add_executable(ptsc
    ast.h
    interfaces.h
    ast.cxx
    ${CMAKE_CURRENT_BINARY_DIR}/lexer.cxx
    ${CMAKE_CURRENT_BINARY_DIR}/grammar.h
    ${CMAKE_CURRENT_BINARY_DIR}/parser.cxx
    sourcer.cxx
    frontend.cxx
    backend.cxx
    main.cxx
    )
  target_compile_features(ptsc PRIVATE cxx_std_17)
  target_link_libraries(ptsc fptu ${LIBCXX_FILESYSTEM})

  add_test(NAME SchemaCompiler
    COMMAND ptsc
    ${CMAKE_CURRENT_SOURCE_DIR}/test.pts
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

else()
  message(STATUS "Schema compiler will be disabled because not all components available: C++17, re2c >= 1.0, lemon >= 1.0")
endif()
