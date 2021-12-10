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

##
##  libfpta = { Fast Positive Tables, aka Позитивные Таблицы }
##
##  Ultra fast, compact, embeddable storage engine for (semi)structured data:
##  multiprocessing with zero-overhead, full ACID semantics with MVCC,
##  variety of indexes, saturation, sequences and much more.
##  Please see README.md at https://github.com/erthink/libfpta
##
##  The Future will (be) Positive. Всё будет хорошо.
##
##  "Позитивные таблицы" предназначены для построения высокоскоростных
##  локальных хранилищ структурированных данных, с целевой производительностью
##  до 1.000.000 запросов в секунду на каждое ядро процессора.
##

#
# Get version
if(HAVE_FPTA_VERSIONINFO)
  fetch_version(FPTA "${CMAKE_CURRENT_SOURCE_DIR}/.." TRUE)
  message(STATUS "libfpta version is ${FPTA_VERSION}")
else()
  set(FPTA_VERSION "unversioned")
endif()

if(LIBFPTA_STATIC)
  if(CMAKE_VERSION VERSION_LESS 3.12)
    message(WARNING "CMake >= 3.12 is required for build static libfpta.")
  endif()
  set(LIBFPTA_TYPE STATIC)
else()
  set(LIBFPTA_TYPE SHARED)
endif()

add_library(fpta ${LIBFPTA_TYPE}
  ${FAST_POSITIVE_CONFIG_H}
  ../fast_positive/tables.h
  ../fast_positive/tables_internal.h
  ../externals/libfptu/fast_positive/defs.h
  ../externals/libfptu/fast_positive/tuples.h
  ../externals/libfptu/fast_positive/tuples_internal.h
  ../externals/libmdbx/mdbx.h
  ../externals/t1ha/t1ha.h
  details.h
  osal.h
  composite.cxx
  common.cxx
  dbi.cxx
  table.cxx
  filter.cxx
  cursor.cxx
  schema.cxx
  index.cxx
  data.cxx
  misc.cxx
  inplace.cxx
  ${CMAKE_CURRENT_BINARY_DIR}/version.cxx
  )

set_target_properties(fpta PROPERTIES
  PROJECT_LABEL "Fast Positive Tables"
  VERSION "${FPTA_VERSION}"
  PUBLIC_HEADER "../fast_positive/tables.h;${FAST_POSITIVE_CONFIG_H};../externals/libfptu/fast_positive/defs.h;../externals/libfptu/fast_positive/tuples.h"
  PRIVATE_HEADER "../fast_positive/tables_internal.h;../externals/libfptu/fast_positive/tuples_internal.h;../externals/libmdbx/mdbx.h;../externals/t1ha/t1ha.h"
  INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>)

if(FPTA_CXX_STANDARD)
  set_target_properties(fpta PROPERTIES
    CXX_STANDARD ${FPTA_CXX_STANDARD} CXX_STANDARD_REQUIRED ON)
  target_compile_features(fpta INTERFACE "cxx_std_${FPTA_CXX_STANDARD}")
endif()
if(FPTA_C_STANDARD)
  set_target_properties(fpta PROPERTIES
    C_STANDARD ${FPTA_C_STANDARD} C_STANDARD_REQUIRED ON)
endif()

add_dependencies(fpta t1ha)
target_link_libraries(fpta
  PRIVATE fptu mdbx-static t1ha
  PUBLIC ${LIB_MATH} ${CMAKE_THREAD_LIBS_INIT})
if(UNIX AND NOT APPLE)
  target_link_libraries(fpta LINK_PUBLIC rt)
endif()

if(CC_HAS_FASTMATH
    AND NOT (CMAKE_COMPILER_IS_CLANG AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 10))
  target_compile_options(fpta PRIVATE "-ffast-math")
endif()

if(BUILD_FOR_NATIVE_CPU AND CC_HAS_ARCH_NATIVE)
  target_compile_options(fpta PUBLIC "-march=native")
endif()

if(CC_HAS_VISIBILITY)
  target_compile_options(fpta PRIVATE "-fvisibility=hidden")
  if(LTO_ENABLED OR INTERPROCEDURAL_OPTIMIZATION)
    set_target_properties(fpta PROPERTIES LINK_FLAGS "-fvisibility=hidden")
  endif()
endif()

if(NOT LIBFPTA_STATIC)
  target_compile_definitions(fpta
    PRIVATE -Dfpta_EXPORTS -Dfptu_EXPORTS -Dt1ha_EXPORTS -DLIBMDBX_EXPORTS
    INTERFACE -Dfpta_IMPORTS -Dfptu_IMPORTS -Dt1ha_IMPORTS -DLIBMDBX_IMPORTS)
else()
  target_compile_definitions(fpta PRIVATE -Dfpta_STATIC)
endif()

if(CMAKE_VERSION VERSION_LESS 3.12)
  install(TARGETS fpta EXPORT fpta
    RUNTIME DESTINATION bin COMPONENT runtime
    LIBRARY DESTINATION bin COMPONENT runtime
    OBJECTS DESTINATION lib COMPONENT devel
    ARCHIVE DESTINATION lib COMPONENT devel
    PUBLIC_HEADER DESTINATION include/fast_positive INCLUDES DESTINATION include COMPONENT devel
    PRIVATE_HEADER DESTINATION include/fast_positive INCLUDES DESTINATION include COMPONENT devel
    INCLUDES DESTINATION include/fast_positive/ COMPONENT devel)
else()
  install(TARGETS fpta EXPORT fpta
    RUNTIME DESTINATION bin COMPONENT runtime
    LIBRARY DESTINATION bin COMPONENT runtime
    NAMELINK_COMPONENT devel
    OBJECTS DESTINATION lib COMPONENT devel
    ARCHIVE DESTINATION lib COMPONENT devel
    PUBLIC_HEADER DESTINATION include/fast_positive INCLUDES DESTINATION include COMPONENT devel
    PRIVATE_HEADER DESTINATION include/fast_positive INCLUDES DESTINATION include COMPONENT devel
    INCLUDES DESTINATION include/fast_positive/ COMPONENT devel)
endif()

###############################################################################
#
# library build info (used in library version output)
#

# get definitions as a string of "-Dxyz=124 ..."
get_target_property(FPTA_DEFINITIONS fpta COMPILE_DEFINITIONS)
if(NOT FPTA_DEFINITIONS)
  set(FPTA_DEFINITIONS "")
endif()
list(REMOVE_DUPLICATES FPTA_DEFINITIONS)
string(REGEX REPLACE "([^;]+)" " -D\\1" FPTA_DEFINITIONS "${FPTA_DEFINITIONS}")
string(STRIP FPTA_DEFINITIONS "${FPTA_DEFINITIONS}")

# get target compile options as a list
get_target_property(fpta_compile_options fpta COMPILE_OPTIONS)
if(NOT fpta_compile_options)
  set(fpta_compile_options "")
endif()

# append cmake's common cxx flags and defines
string(REPLACE " " ";" cmake_cxx_options "${CMAKE_CXX_FLAGS}" "${CMAKE_CXX_DEFINES}")
list(INSERT fpta_compile_options 0 "${cmake_cxx_options}")
unset(cmake_cxx_options)

# append cmake's build-type flags and defines
if(NOT CMAKE_CONFIGURATION_TYPES)
  string(REPLACE " " ";" cmake_cxx_options "${CMAKE_CXX_FLAGS_${CMAKE_BUILD_TYPE_UPPERCASE}}" "${CMAKE_CXX_DEFINES_${CMAKE_BUILD_TYPE_UPPERCASE}}")
  list(APPEND fpta_compile_options "${cmake_cxx_options}")
  unset(cmake_cxx_options)
endif()

# append linker dll's options
if(NOT LIBFPTA_STATIC)
  string(REPLACE " " ";" cmake_shared_linker_options "${CMAKE_SHARED_LINKER_FLAGS}")
  list(APPEND fpta_compile_options ${cmake_shared_linker_options})
  unset(cmake_shared_linker_options)
endif()

# drop duplicates in the option list
list(REMOVE_DUPLICATES fpta_compile_options)

# make string of space separated flags
string(REPLACE ";" " " FPTA_COMPILE_FLAGS "${fpta_compile_options}")
unset(fpta_compile_options)
string(STRIP "${FPTA_COMPILE_FLAGS}${FPTA_DEFINITIONS}" FPTA_COMPILE_FLAGS)
if(CMAKE_CONFIGURATION_TYPES)
  # add dynamic part via per-configuration define
  message(STATUS "FPTA Compile Flags: ${FPTA_COMPILE_FLAGS} <AND CONFIGURATION DEPENDENT>")
  set(FPTA_COMPILE_FLAGS "FPTA_COMPILE_FLAGS \"${FPTA_COMPILE_FLAGS}\"")
  add_definitions(
    -DFPTA_COMPILE_FLAGS="$<$<CONFIG:Debug>:${CMAKE_CXX_FLAGS_DEBUG} ${CMAKE_CXX_DEFINES_DEBUG}>$<$<CONFIG:Release>:${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_CXX_DEFINES_RELEASE}>$<$<CONFIG:RelWithDebInfo>:${CMAKE_CXX_FLAGS_RELWITHDEBINFO} ${CMAKE_CXX_DEFINES_RELWITHDEBINFO}>$<$<CONFIG:MinSizeRel>:${CMAKE_CXX_FLAGS_MINSIZEREL} ${CMAKE_CXX_DEFINES_MINSIZEREL}>"
    )
else()
  message(STATUS "FPTA Compile Flags: ${FPTA_COMPILE_FLAGS}")
  set(FPTA_COMPILE_FLAGS "\"${FPTA_COMPILE_FLAGS}\"")
endif()

# make a build-target triplet
if(CMAKE_C_COMPILER_TARGET)
  set(FPTA_BUILD_TARGET "${CMAKE_C_COMPILER_TARGET}")
elseif(CMAKE_C_PLATFORM_ID AND NOT CMAKE_C_PLATFORM_ID STREQUAL CMAKE_SYSTEM_NAME)
  string(STRIP "${CMAKE_C_PLATFORM_ID}-${CMAKE_SYSTEM_NAME}" FPTA_BUILD_TARGET)
elseif(CMAKE_LIBRARY_ARCHITECTURE)
  string(STRIP "${CMAKE_LIBRARY_ARCHITECTURE}-${CMAKE_SYSTEM_NAME}" FPTA_BUILD_TARGET)
elseif(CMAKE_GENERATOR_PLATFORM AND NOT CMAKE_C_PLATFORM_ID STREQUAL CMAKE_SYSTEM_NAME)
  string(STRIP "${CMAKE_GENERATOR_PLATFORM}-${CMAKE_SYSTEM_NAME}" FPTA_BUILD_TARGET)
elseif(CMAKE_SYSTEM_ARCH)
  string(STRIP "${CMAKE_SYSTEM_ARCH}-${CMAKE_SYSTEM_NAME}" FPTA_BUILD_TARGET)
else()
  string(STRIP "${CMAKE_SYSTEM_PROCESSOR}-${CMAKE_SYSTEM_NAME}" FPTA_BUILD_TARGET)
endif()

# provide build-type
if(CMAKE_CONFIGURATION_TYPES)
  # via per-configuration define
  add_definitions(-DFPTA_BUILD_TYPE="$<CONFIG>")
  set(FPTA_BUILD_TYPE "<CONFIGURATION DEPENDENT>")
else()
  set(FPTA_BUILD_TYPE ${CMAKE_BUILD_TYPE})
endif()

# generate version file
string(TIMESTAMP FPTA_BUILD_TIMESTAMP UTC)
string(REPLACE "  " " " FPTA_CMAKE_OPTIONS "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}"
  " -DFPTA_ALLOW_DOT4NAMES=${FPTA_ALLOW_DOT4NAMES}"
  " -DFPTA_PROHIBIT_UPSERT_NAN=${FPTA_PROHIBIT_UPSERT_NAN}"
  " -DFPTA_PROHIBIT_UPSERT_DENIL=${FPTA_PROHIBIT_UPSERT_DENIL}"
  " -DFPTA_CLEAN_DENIL=${FPTA_CLEAN_DENIL}"
  " -DFPTA_PROHIBIT_NEARBY4UNORDERED=${FPTA_PROHIBIT_NEARBY4UNORDERED}"
  " -DFPTA_PROHIBIT_LOSS_PRECISION=${FPTA_PROHIBIT_LOSS_PRECISION}"
  " -DFPTA_ENABLE_ABORT_ON_PANIC=${FPTA_ENABLE_ABORT_ON_PANIC}"
  " -DT1HA_USE_FAST_ONESHOT_READ=${T1HA_USE_FAST_ONESHOT_READ}"
  " -DENABLE_ASAN=${ENABLE_ASAN}"
  " -DENABLE_VALGRIND=${ENABLE_VALGRIND}"
  " -DENABLE_GPROF=${ENABLE_GPROF}"
  " -DENABLE_GCOV=${ENABLE_GCOV}"
  " -DENABLE_BACKTRACE=${ENABLE_BACKTRACE}"
  )

string(STRIP "${CMAKE_CXX_COMPILER_ID}-${CMAKE_CXX_COMPILER_VERSION}" FPTA_BUILD_COMPILER)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/version.cxx.in ${CMAKE_CURRENT_BINARY_DIR}/version.cxx)

set(options VERSION C_COMPILER CXX_COMPILER DEFINITIONS BUILD_TARGET BUILD_TYPE
  ENABLE_GCOV ENABLE_GPROF ENABLE_VALGRIND ENABLE_BACKTRACE
  HAVE_BFD ENABLE_ASAN ENABLE_UBSAN)
foreach(option IN LISTS options)
  if(DEFINED "FPTA_${option}")
    set(value "${FPTA_${option}}")
  elseif(DEFINED "${option}")
    set(value "${${option}}")
  else()
    set(value "${CMAKE_${option}}")
  endif()
  message(STATUS "FPTA_${option}" "FPTA_${option}: ${value}" "${value}")
endforeach(option)
