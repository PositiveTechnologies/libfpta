/*
 *  Copyright (c) 1994-2019 Leonid Yuriev <leo@yuriev.ru>.
 *  https://github.com/leo-yuriev/erthink
 *  ZLib License
 *
 *  This software is provided 'as-is', without any express or implied
 *  warranty. In no event will the authors be held liable for any damages
 *  arising from the use of this software.
 *
 *  Permission is granted to anyone to use this software for any purpose,
 *  including commercial applications, and to alter it and redistribute it
 *  freely, subject to the following restrictions:
 *
 *  1. The origin of this software must not be misrepresented; you must not
 *     claim that you wrote the original software. If you use this software
 *     in a product, an acknowledgement in the product documentation would be
 *     appreciated but is not required.
 *  2. Altered source versions must be plainly marked as such, and must not be
 *     misrepresented as being the original software.
 *  3. This notice may not be removed or altered from any source distribution.
 */

#pragma once

#include "erthink_defs.h"
#include <type_traits>

namespace erthink {

#if __cplusplus < 201402L
template <bool B, class T = void>
using enable_if_t = typename std::enable_if<B, T>::type;
#else
using std::enable_if_t;
#endif /* C++14 */

template <typename TO, typename FROM,
          typename erthink::enable_if_t<std::is_pointer<TO>::value, int> = 0,
          typename erthink::enable_if_t<std::is_pointer<FROM>::value, int> = 0,
          typename erthink::enable_if_t<
              std::is_const<typename std::remove_pointer<FROM>::type>::value,
              int> = 0>
inline constexpr TO constexpr_pointer_cast(FROM from) {
  return static_cast<TO>(static_cast<const void *>(from));
}

template <typename TO, typename FROM,
          typename erthink::enable_if_t<std::is_pointer<TO>::value, int> = 0,
          typename erthink::enable_if_t<std::is_pointer<FROM>::value, int> = 0,
          typename erthink::enable_if_t<
              !std::is_const<typename std::remove_pointer<FROM>::type>::value,
              int> = 0>
inline constexpr TO constexpr_pointer_cast(FROM from) {
  return static_cast<TO>(static_cast<void *>(from));
}

//------------------------------------------------------------------------------

constexpr inline bool is_constant_evaluated() noexcept {
#if defined(__cpp_lib_is_constant_evaluated)
  return std::is_constant_evaluated();
#elif __GNUC_PREREQ(9, 0) || __has_builtin(__builtin_is_constant_evaluated)
  return ::__builtin_is_constant_evaluated();
#else
  return false;
#endif
}

#if defined(__cpp_lib_is_constant_evaluated) &&                                \
    __cpp_lib_is_constant_evaluated >= 201811L

#define erthink_dynamic_constexpr constexpr
#define ERTHINK_DYNAMIC_CONSTEXPR(RESULT_TYPE, NAME, DECLARGS_PARENTHESIZED,   \
                                  CALLARGS_PARENTHESIZED, PROBE_ARG)           \
  static inline constexpr RESULT_TYPE NAME DECLARGS_PARENTHESIZED noexcept {   \
    return ::std::is_constant_evaluated()                                      \
               ? NAME##_constexpr CALLARGS_PARENTHESIZED                       \
               : NAME##_dynamic CALLARGS_PARENTHESIZED;                        \
  }

#elif __GNUC_PREREQ(9, 0) || __has_builtin(__builtin_is_constant_evaluated)

#define erthink_dynamic_constexpr constexpr
#define ERTHINK_DYNAMIC_CONSTEXPR(RESULT_TYPE, NAME, DECLARGS_PARENTHESIZED,   \
                                  CALLARGS_PARENTHESIZED, PROBE_ARG)           \
  static inline constexpr RESULT_TYPE NAME DECLARGS_PARENTHESIZED noexcept {   \
    return __builtin_is_constant_evaluated()                                   \
               ? NAME##_constexpr CALLARGS_PARENTHESIZED                       \
               : NAME##_dynamic CALLARGS_PARENTHESIZED;                        \
  }

#elif __GNUC_PREREQ(5, 4) || __has_builtin(__builtin_constant_p)

#define erthink_dynamic_constexpr cxx14_constexpr
#define ERTHINK_DYNAMIC_CONSTEXPR(RESULT_TYPE, NAME, DECLARGS_PARENTHESIZED,   \
                                  CALLARGS_PARENTHESIZED, PROBE_ARG)           \
  static inline cxx14_constexpr RESULT_TYPE NAME                               \
      DECLARGS_PARENTHESIZED noexcept {                                        \
    return __builtin_constant_p(PROBE_ARG)                                     \
               ? NAME##_constexpr CALLARGS_PARENTHESIZED                       \
               : NAME##_dynamic CALLARGS_PARENTHESIZED;                        \
  }

#else

#define erthink_dynamic_constexpr
#define ERTHINK_DYNAMIC_CONSTEXPR(RESULT_TYPE, NAME, DECLARGS_PARENTHESIZED,   \
                                  CALLARGS_PARENTHESIZED, PROBE_ARG)           \
  static RESULT_TYPE NAME DECLARGS_PARENTHESIZED noexcept {                    \
    return NAME##_dynamic CALLARGS_PARENTHESIZED;                              \
  }

#endif

} // namespace erthink
