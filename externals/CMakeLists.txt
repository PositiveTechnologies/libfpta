if(FPTA_CXX_STANDARD)
  set(FPTU_CXX_STANDARD ${FPTA_CXX_STANDARD})
  set(MDBX_CXX_STANDARD ${FPTA_CXX_STANDARD})
endif()
if(FPTA_C_STANDARD)
  set(FPTU_C_STANDARD ${FPTA_C_STANDARD})
  set(MDBX_C_STANDARD ${FPTA_C_STANDARD})
endif()

if(CMAKE_VERSION VERSION_LESS 3.12)
  set(LIBFPTU_TYPE PILE)
else()
  set(LIBFPTU_TYPE OBJECT)
endif()

if(ENABLE_UBSAN)
  add_definitions(-DFPTU_UNALIGNED_OK=0 -DMDBX_UNALIGNED_OK=0 -DT1HA_SYS_UNALIGNED_ACCESS=0)
endif()

add_definitions(-Dfptu_EXPORTS -DLIBMDBX_NO_EXPORTS_LEGACY_API)
add_subdirectory(libfptu/src)
add_subdirectory(libfptu/test)

###############################################################################

set(T1HA_SOURCES t1ha/t1ha.h t1ha/src/t1ha2.c t1ha/src/t1ha_selfcheck.c t1ha/src/t1ha2_selfcheck.c)

if(CMAKE_VERSION VERSION_LESS 3.12)
  add_library(t1ha_objects OBJECT ${T1HA_SOURCES})
  add_library(t1ha INTERFACE)
  add_dependencies(t1ha t1ha_objects)
  target_link_libraries(t1ha INTERFACE $<TARGET_OBJECTS:t1ha_objects>)
  set(t1ha_target t1ha_objects)
else()
  add_library(t1ha OBJECT ${T1HA_SOURCES})
  set(t1ha_target t1ha)
endif()

target_compile_definitions(${t1ha_target} PRIVATE t1ha_EXPORTS T1HA_USE_FAST_ONESHOT_READ=$<BOOL:${T1HA_USE_FAST_ONESHOT_READ}>)

set_target_properties(${t1ha_target} PROPERTIES
  PROJECT_LABEL "Fast Positive Hash"
  #VERSION
  PUBLIC_HEADER "t1ha/t1ha.h"
  INTERPROCEDURAL_OPTIMIZATION $<BOOL:${INTERPROCEDURAL_OPTIMIZATION}>
  )

if(FPTA_C_STANDARD)
  set_target_properties(${t1ha_target} PROPERTIES
    C_STANDARD ${FPTA_C_STANDARD} C_STANDARD_REQUIRED ON)
endif()

###############################################################################

set(MDBX_BUILD_SHARED_LIBRARY OFF)
add_subdirectory(libmdbx)
target_compile_definitions(mdbx-static PRIVATE LIBMDBX_EXPORTS)
if(ENABLE_VALGRIND)
  target_compile_definitions(mdbx-static PRIVATE MDBX_USE_VALGRIND)
endif()
