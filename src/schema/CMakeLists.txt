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

set(Boost_DETAILED_FAILURE_MSG ON)
# set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.44 COMPONENTS filesystem iostreams program_options system)
find_program(RE2C re2c)
find_program(LEMON lemon)
if(Boost_FILESYSTEM_FOUND AND Boost_IOSTREAMS_FOUND
    AND Boost_PROGRAM_OPTIONS_FOUND AND Boost_SYSTEM_FOUND
    AND RE2C AND LEMON)

  add_executable(bomstrip bomstrip.cxx)
  include_directories(${CMAKE_CURRENT_BINARY_DIR} ${Boost_INCLUDE_DIR})

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
  target_link_libraries(ptsc
    ${Boost_FILESYSTEM_LIBRARY} ${Boost_IOSTREAMS_LIBRARY}
    ${Boost_PROGRAM_OPTIONS_LIBRARY} ${Boost_SYSTEM_LIBRARY}
    fptu)

  add_test(NAME SchemaCompiler
    COMMAND ptsc
    ${CMAKE_CURRENT_SOURCE_DIR}/test.pts
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

else()
  message(STATUS "Schema compiler will be disabled because not all components available: re2c, lemon, boost-libraries")
endif()
