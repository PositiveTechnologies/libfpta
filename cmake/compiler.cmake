#
# Check if the same compile family is used for both C and CXX
#
if (NOT (CMAKE_C_COMPILER_ID STREQUAL CMAKE_CXX_COMPILER_ID))
    message(WARNING "CMAKE_C_COMPILER_ID (${CMAKE_C_COMPILER_ID}) is different "
                    "from CMAKE_CXX_COMPILER_ID (${CMAKE_CXX_COMPILER_ID})."
                    "The final binary may be unusable.")
endif()

# We support building with Clang and gcc. First check
# what we're using for build.
#
if (CMAKE_C_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_COMPILER_IS_CLANG  ON)
    set(CMAKE_COMPILER_IS_GNUCC  OFF)
    set(CMAKE_COMPILER_IS_GNUCXX OFF)
endif()

#
# Hard coding the compiler version is ugly from cmake POV, but
# at least gives user a friendly error message. The most critical
# demand for C++ compiler is support of C++11 lambdas, added
# only in version 4.5 https://gcc.gnu.org/projects/cxx0x.html
#
if (CMAKE_COMPILER_IS_GNUCC)
# cmake 2.8.9 and earlier doesn't support CMAKE_CXX_COMPILER_VERSION
       if (NOT CMAKE_CXX_COMPILER_VERSION)
               execute_process(COMMAND ${CMAKE_C_COMPILER} -dumpversion
                               OUTPUT_VARIABLE CMAKE_CXX_COMPILER_VERSION)
       endif()
       if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.5)
               message(FATAL_ERROR "
               Your GCC version is ${CMAKE_CXX_COMPILER_VERSION}, please update
                       ")
       endif()
endif()

#
# Check supported standards
#
if((NOT HAVE_STD_C11 AND NOT HAVE_STD_C99 AND NOT HAVE_STD_GNU99) OR
   (NOT HAVE_STD_CXX11 AND NOT HAVE_STD_GNUXX0X))
    set(CMAKE_REQUIRED_FLAGS "-std=c11")
    check_c_source_compiles("
        #if __STDC_VERSION__ < 201112L
        #   error C11 not available
        #endif
        /*
         * FreeBSD 10 ctype.h header fail to compile on gcc4.8 in c11 mode.
         * Make sure we aren't affected.
         */
        #include <ctype.h>
        int main(void) { return 0; }
        " HAVE_STD_C11)

    set(CMAKE_REQUIRED_FLAGS "-std=c99")
    check_c_source_compiles("
        #if (__STDC_VERSION__ < 199901L) && (_MSC_FULL_VER < 180040629)
        #   error C99 not available
        #endif
	int main(void) { return 0; }
	" HAVE_STD_C99)

    set(CMAKE_REQUIRED_FLAGS "-std=gnu99")
    check_c_source_compiles("
        #if __STDC_VERSION__ < 199901L
        #   error C99 not available
        #endif
	int main(void) { return 0; }
	" HAVE_STD_GNU99)

    set(CMAKE_REQUIRED_FLAGS "-std=c++11")
    check_cxx_source_compiles("
        #if (__cplusplus < 201103L) && (_MSC_FULL_VER < 180040629)
        #   error C++11 not available
        #endif
        int main(void) { return 0; }
        " HAVE_STD_CXX11)
    set(CMAKE_REQUIRED_FLAGS "-std=gnu++0x")

    check_cxx_source_compiles("
        #if __cplusplus < 201103L && !defined(__GXX_EXPERIMENTAL_CXX0X__)
        #   error GNU C++0x not available
        #endif
        int main(void) { return 0; }
        " HAVE_STD_GNUXX0X)
    set(CMAKE_REQUIRED_FLAGS "")
endif()
if((NOT HAVE_STD_C11 AND NOT HAVE_STD_C99 AND NOT HAVE_STD_GNU99) OR
   (NOT HAVE_STD_CXX11 AND NOT HAVE_STD_GNUXX0X))
    message (FATAL_ERROR
        "${CMAKE_C_COMPILER} should support -std=c11 or -std=c99. "
        "${CMAKE_CXX_COMPILER} should support -std=c++11 or -std=gnu++0x. "
        "Please consider upgrade to gcc 4.5+ or clang 3.2+.")
endif()

#
# GCC started to warn for unused result starting from 4.2, and
# this is when it introduced -Wno-unused-result
# GCC can also be built on top of llvm runtime (on mac).
#
check_c_compiler_flag("-Wno-unused-const-variable" CC_HAS_WNO_UNUSED_CONST_VARIABLE)
check_c_compiler_flag("-Wno-unused-result" CC_HAS_WNO_UNUSED_RESULT)
check_c_compiler_flag("-Wno-unused-value" CC_HAS_WNO_UNUSED_VALUE)
check_c_compiler_flag("-Wno-unused-function" CC_HAS_WNO_UNUSED_FUNCTION)
check_c_compiler_flag("-fno-strict-aliasing" CC_HAS_FNO_STRICT_ALIASING)
check_c_compiler_flag("-Wno-comment" CC_HAS_WNO_COMMENT)
check_c_compiler_flag("-Wno-parentheses" CC_HAS_WNO_PARENTHESES)
check_c_compiler_flag("-Wno-parentheses-equality" CC_HAS_WNO_PARENTHESES_EQUALITY)
check_c_compiler_flag("-Wno-undefined-inline" CC_HAS_WNO_UNDEFINED_INLINE)
check_c_compiler_flag("-Wno-dangling-else" CC_HAS_WNO_DANGLING_ELSE)
check_c_compiler_flag("-Wno-tautological-compare" CC_HAS_WNO_TAUTOLOGICAL_COMPARE)
check_c_compiler_flag("-Wno-misleading-indentation" CC_HAS_WNO_MISLEADING_INDENTATION)

check_c_compiler_flag("-Wno-unknown-pragmas" CC_HAS_WNO_UNKNOWN_PRAGMAS)
check_c_compiler_flag("-Wall" CC_HAS_WALL)
check_c_compiler_flag("-Wextra" CC_HAS_WEXTRA)
check_c_compiler_flag("-Werror" CC_HAS_WERROR)
check_c_compiler_flag("-fexceptions" CC_HAS_FEXCEPTIONS)
check_c_compiler_flag("-funwind-tables" CC_HAS_FUNWIND_TABLES)
check_c_compiler_flag("-fno-omit-frame-pointer" CC_HAS_FNO_OMIT_FRAME_POINTER)
check_c_compiler_flag("-fno-stack-protector" CC_HAS_FNO_STACK_PROTECTOR)
check_c_compiler_flag("-fno-common" CC_HAS_FNO_COMMON)
check_c_compiler_flag("-Wno-strict-aliasing" CC_HAS_WNO_STRICT_ALIASING)
check_c_compiler_flag("-ggdb" CC_HAS_GGDB)

#
# Check for an omp support
set(CMAKE_REQUIRED_FLAGS "-fopenmp -Werror")
check_cxx_source_compiles("int main(void) {
#pragma omp parallel
    return 0;
}" HAVE_OPENMP)
set(CMAKE_REQUIRED_FLAGS "")

#
# Perform build type specific configuration.
option(ENABLE_BACKTRACE "Enable output of fiber backtrace information in 'show
fiber' administrative command. Only works on x86 architectures, if compiled
with gcc. If GNU binutils and binutils-dev libraries are installed, backtrace
is output with resolved function (symbol) names. Otherwise only frame
addresses are printed." ${CMAKE_COMPILER_IS_GNUCC})

set (HAVE_BFD False)
if (ENABLE_BACKTRACE)
    if (NOT ${CMAKE_COMPILER_IS_GNUCC})
        # We only know this option to work with gcc
        message (FATAL_ERROR "ENABLE_BACKTRACE option is set but the system
                is not x86 based (${CMAKE_SYSTEM_PROCESSOR}) or the compiler
                is not GNU GCC (${CMAKE_C_COMPILER}).")
    endif()
    # Use GNU bfd if present.
    find_library(BFD_LIBRARY NAMES libbfd.a)
    if(BFD_LIBRARY)
        check_library_exists (${BFD_LIBRARY} bfd_init ""  HAVE_BFD_LIB)
    endif()
    find_library(IBERTY_LIBRARY NAMES libiberty.a)
    if(IBERTY_LIBRARY)
        check_library_exists (${IBERTY_LIBRARY} cplus_demangle ""  HAVE_IBERTY_LIB)
    endif()
    set(CMAKE_REQUIRED_DEFINITIONS -DPACKAGE=${PACKAGE} -DPACKAGE_VERSION=${PACKAGE_VERSION})
    check_include_files(bfd.h HAVE_BFD_H)
    set(CMAKE_REQUIRED_DEFINITIONS)
    find_package(ZLIB)
    if (HAVE_BFD_LIB AND HAVE_BFD_H AND HAVE_IBERTY_LIB AND ZLIB_FOUND)
        set (HAVE_BFD ON)
        set (BFD_LIBRARIES ${BFD_LIBRARY} ${IBERTY_LIBRARY} ${ZLIB_LIBRARIES})
        find_package_message(BFD_LIBRARIES "Found libbfd and dependencies"
            ${BFD_LIBRARIES})
        if (TARGET_OS_FREEBSD AND NOT TARGET_OS_DEBIAN_FREEBSD)
            set (BFD_LIBRARIES ${BFD_LIBRARIES} iconv)
        endif()
    endif()
endif()

# In C a global variable without a storage specifier (static/extern) and
# without an initialiser is called a ’tentative definition’. The
# language permits multiple tentative definitions in the single
# translation unit; i.e. int foo; int foo; is perfectly ok. GNU
# toolchain goes even further, allowing multiple tentative definitions
# in *different* translation units. Internally, variables introduced via
# tentative definitions are implemented as ‘common’ symbols. Linker
# permits multiple definitions if they are common symbols, and it picks
# one arbitrarily for inclusion in the binary being linked.
#
# -fno-common forces GNU toolchain to behave in a more
# standard-conformant way in respect to tentative definitions and it
# prevents common symbols generation. Since we are a cross-platform
# project it really makes sense. There are toolchains that don’t
# implement GNU style handling of the tentative definitions and there
# are platforms lacking proper support for common symbols (osx).
#

macro(setup_compile_flags)
    if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
        # Remove VALGRIND code and assertions in *any* type of release build.
        add_definitions("-DNDEBUG" "-DNVALGRIND")
    endif()

    if (CC_HAS_FNO_COMMON)
        add_compile_flags("C;CXX" "-fno-common")
    endif()

    if (CC_HAS_GGDB)
        add_compile_flags("C;CXX" "-ggdb")
    endif()

    if (CC_HAS_WNO_UNKNOWN_PRAGMAS AND NOT HAVE_OPENMP)
        add_compile_flags("C;CXX" -Wno-unknown-pragmas)
    endif()

    # We must set -fno-omit-frame-pointer here, since we rely
    # on frame pointer when getting a backtrace, and it must
    # be used consistently across all object files.
    # The same reasoning applies to -fno-stack-protector switch.
    if (ENABLE_BACKTRACE)
        if (CC_HAS_FNO_OMIT_FRAME_POINTER)
            add_compile_flags("C;CXX" "-fno-omit-frame-pointer")
	endif()
        if (CC_HAS_FNO_STACK_PROTECTOR)
            add_compile_flags("C;CXX" "-fno-stack-protector")
        endif()
    endif()

    # libfptu code is written in GNU C dialect.
    # Additionally, compile it with more strict flags than the rest
    # of the code.

    # Set standard
    if (HAVE_STD_C11)
        add_compile_flags("C" "-std=c11")
    elseif(HAVE_STD_GNU99)
        add_compile_flags("C" "-std=gnu99")
    elseif(HAVE_STD_C99 AND NOT (CMAKE_C_COMPILER_ID STREQUAL "MSVC"))
        add_compile_flags("C" "-std=c99")
    endif()

    if (HAVE_STD_CXX11 AND NOT (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC"))
        add_compile_flags("CXX" "-std=c++11")
    elseif(HAVE_STD_GNUXX0X)
        add_compile_flags("CXX" "-std=gnu++0x")
        add_definitions("-Doverride=")
    endif()

    if (CC_HAS_WALL)
        add_compile_flags("C;CXX" "-Wall")
    endif()
    if (CC_HAS_WEXTRA)
        add_compile_flags("C;CXX" "-Wextra")
    endif()

    if (CMAKE_COMPILER_IS_GNUCXX)
        # G++ bug. http://gcc.gnu.org/bugzilla/show_bug.cgi?id=31488
        add_compile_flags("CXX"
            "-Wno-invalid-offsetof"
        )
    endif()

    if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_CLANG)
        add_compile_flags("C;CXX" "-march=native")
    endif()

    add_definitions("-D__STDC_FORMAT_MACROS=1")
    add_definitions("-D__STDC_LIMIT_MACROS=1")
    add_definitions("-D__STDC_CONSTANT_MACROS=1")

    # Only add -Werror if it's a debug build, done by developers.
    # Release builds should not cause extra trouble.
    if (CC_HAS_WERROR AND (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
        AND HAVE_STD_C11 AND HAVE_STD_CXX11)
        add_compile_flags("C;CXX" "-Werror")
    endif()

    if (HAVE_OPENMP)
        add_compile_flags("C;CXX" "-fopenmp")
    endif()
endmacro(setup_compile_flags)

if (CMAKE_COMPILER_IS_CLANG OR CMAKE_COMPILER_IS_GNUCC)
    set(HAVE_BUILTIN_CTZ 1)
    set(HAVE_BUILTIN_CTZLL 1)
    set(HAVE_BUILTIN_CLZ 1)
    set(HAVE_BUILTIN_CLZLL 1)
    set(HAVE_BUILTIN_POPCOUNT 1)
    set(HAVE_BUILTIN_POPCOUNTLL 1)
    set(HAVE_BUILTIN_BSWAP32 1)
    set(HAVE_BUILTIN_BSWAP64 1)
else()
    set(HAVE_BUILTIN_CTZ 0)
    set(HAVE_BUILTIN_CTZLL 0)
    set(HAVE_BUILTIN_CLZ 0)
    set(HAVE_BUILTIN_CLZLL 0)
    set(HAVE_BUILTIN_POPCOUNT 0)
    set(HAVE_BUILTIN_POPCOUNTLL 0)
    set(HAVE_BUILTIN_BSWAP32 0)
    set(HAVE_BUILTIN_BSWAP64 0)
    find_package_message(CC_BIT "Using slow implementation of bit operations"
        "${CMAKE_COMPILER_IS_CLANG}:${CMAKE_COMPILER_IS_GNUCC}")
endif()

if (NOT HAVE_BUILTIN_CTZ OR NOT HAVE_BUILTIN_CTZLL)
    # Check if -D_GNU_SOURCE has been defined and add this flag to
    # CMAKE_REQUIRED_DEFINITIONS in order to get check_prototype_definition work
    get_property(var DIRECTORY PROPERTY COMPILE_DEFINITIONS)
    list(FIND var "_GNU_SOURCE" var)
    if (NOT var EQUAL -1)
        set(CMAKE_REQUIRED_FLAGS "-Wno-error")
        set(CMAKE_REQUIRED_DEFINITIONS "-D_GNU_SOURCE")
        check_c_source_compiles("#include <string.h>\n#include <strings.h>\nint main(void) { return ffsl(0L); }"
            HAVE_FFSL)
        check_c_source_compiles("#include <string.h>\n#include <strings.h>\nint main(void) { return ffsll(0UL); }"
            HAVE_FFSLL)
    endif()
endif()
