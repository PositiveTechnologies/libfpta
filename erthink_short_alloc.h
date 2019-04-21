// The MIT License (MIT)
//
// Copyright (c) 2019 Leonid Yuriev <leo@yuriev.ru>
// https://github.com/leo-yuriev/erthink
//
// Copyright (c) 2015 Howard Hinnant
// https://howardhinnant.github.io/stack_alloc.html
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#pragma once
#include <cassert>
#include <cstddef>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "erthink_defs.h"

namespace erthink {

template <bool ALLOW_OUTLIVE, std::size_t N_BYTES,
          std::size_t ALIGN = alignof(std::max_align_t)>
class allocation_arena {
public:
  static constexpr auto allow_outlive = ALLOW_OUTLIVE;
  static constexpr auto size = N_BYTES;
  static auto constexpr alignment = ALIGN;

private:
  alignas(alignment) char buf_[size];
  char *ptr_;

  constexpr static std::size_t align_up(std::size_t n) noexcept {
    return (n + (alignment - 1)) & ~(alignment - 1);
  }

  constexpr bool pointer_in_buffer(char *p) noexcept {
    return buf_ <= p && p <= buf_ + size;
  }

public:
  ~allocation_arena() { ptr_ = nullptr; }
  constexpr allocation_arena() noexcept : ptr_(buf_) {
    static_assert(size > 1, "Oops, ALLOW_OUTLIVE is messed with N_BYTES?");
#if !ERTHINK_PROVIDE_ALIGNED_NEW
    static_assert(!allow_outlive || alignment <= alignof(std::max_align_t),
                  "you've chosen an alignment that is larger than "
                  "alignof(std::max_align_t), and cannot be guaranteed by "
                  "normal operator new");
#endif
    static_assert(size % alignment == 0,
                  "size N needs to be a multiple of alignment Align");
  }
  allocation_arena(const allocation_arena &) = delete;
  allocation_arena &operator=(const allocation_arena &) = delete;

  template <std::size_t ReqAlign> char *allocate(std::size_t n) {
    static_assert(ReqAlign <= alignment,
                  "alignment is too large for this arena");

    if (likely(pointer_in_buffer(ptr_))) {
      auto const aligned_n = align_up(n);
      if (likely(static_cast<decltype(aligned_n)>(buf_ + size - ptr_) >=
                 aligned_n)) {
        char *r = ptr_;
        ptr_ += aligned_n;
        return r;
      }
    }

    if (allow_outlive) {
#if ERTHINK_PROVIDE_ALIGNED_NEW
      return static_cast<char *>(
          ::operator new(n, std::align_val_t(alignment)));
#else
      return static_cast<char *>(::operator new(n));
#endif
    }
    throw std::runtime_error("short_alloc has exhausted allocation arena");
  }

  void deallocate(char *p, std::size_t n) {
    if (likely(pointer_in_buffer(p))) {
      n = align_up(n);
      if (p + n == ptr_)
        ptr_ = p;
      return;
    }

    if (allow_outlive) {
#if ERTHINK_PROVIDE_ALIGNED_NEW && defined(__cpp_sized_deallocation)
      ::operator delete(p, n, std::align_val_t(alignment));
#elif defined(__cpp_sized_deallocation)
      ::operator delete(p, n);
#else
      ::operator delete(p);
#endif
      return;
    }
    throw std::runtime_error("short_alloc was disabled to exhausted arena");
  }

  constexpr std::size_t used() const noexcept {
    return static_cast<std::size_t>(ptr_ - buf_);
  }

  constexpr void reset() noexcept { ptr_ = buf_; }
};

template <class T, typename ARENA> class short_alloc {
public:
  using value_type = T;
  using arena_type = ARENA;
  static auto constexpr alignment = arena_type::alignment;
  static auto constexpr size = arena_type::size;
  using is_always_equal = std::false_type;

  using propagate_on_container_copy_assignment = std::false_type;
  using propagate_on_container_move_assignment = std::false_type;
  using propagate_on_container_swap = std::false_type;

private:
  arena_type &arena_;

public:
  constexpr short_alloc(/*allocation arena must be provided */) = delete;
  constexpr short_alloc(const short_alloc &) noexcept = default;
  short_alloc &operator=(const short_alloc &) = delete;
  constexpr short_alloc &select_on_container_copy_construction() noexcept {
    return *this;
  }

  constexpr short_alloc(arena_type &area) noexcept : arena_(area) {}
  template <class U>
  constexpr short_alloc(const short_alloc<U, arena_type> &src) noexcept
      : short_alloc(src.arena_) {}

  template <class U> struct rebind {
    using other = short_alloc<U, arena_type>;
  };

  T *allocate(std::size_t n) {
    return reinterpret_cast<T *>(
        arena_.template allocate<alignof(T)>(n * sizeof(T)));
  }
  void deallocate(T *p, std::size_t n) noexcept {
    arena_.deallocate(reinterpret_cast<char *>(p), n * sizeof(T));
  }

  template <typename... Args> inline void construct(T *p, Args &&... args) {
    new (p) T(std::forward<Args>(args)...);
  }

  inline void destroy(T *p) { p->~T(); }

  template <class X, typename X_ARENA, class Y, typename Y_ARENA>
  friend inline bool operator==(const short_alloc<X, X_ARENA> &x,
                                const short_alloc<Y, Y_ARENA> &y) noexcept;

  template <class X, typename X_ARENA> friend class short_alloc;
};

template <class X, typename X_ARENA, class Y, typename Y_ARENA>
inline bool operator==(const short_alloc<X, X_ARENA> &x,
                       const short_alloc<Y, Y_ARENA> &y) noexcept {
  return X_ARENA::size == Y_ARENA::size &&
         X_ARENA::alignment == Y_ARENA::alignment && &x.arena_ == &y.arena_;
}

template <class X, typename X_ARENA, class Y, typename Y_ARENA>
inline bool operator!=(const short_alloc<X, Y_ARENA> &x,
                       const short_alloc<Y, Y_ARENA> &y) noexcept {
  return !(x == y);
}

} // namespace erthink
