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

#include "erthink_defs.h"
#include "erthink_short_alloc.h"
#include "testing.h"

#if __GLIBC_PREREQ(2, 4)
#include <malloc.h>
#endif

#include <deque>
#include <memory>
#include <stack>

template <std::size_t SIZE> struct Params {
  static constexpr std::size_t area_size = SIZE * alignof(std::max_align_t);
  using arena_NOoutlive_type = erthink::allocation_arena<false, area_size>;
  using alloc_NOoutlive_type = erthink::short_alloc<char, arena_NOoutlive_type>;

  using arena_outlive_type = erthink::allocation_arena<true, area_size>;
  using alloc_outlive_type = erthink::short_alloc<char, arena_outlive_type>;
};

typedef ::testing::Types<
    Params<1>, Params<2>, Params<4>, Params<8>, Params<16>, Params<32>,
    Params<64>, Params<81>, Params<128>, Params<256>, Params<512>, Params<777>,
    Params<1024>, Params<2048>, Params<1024 * 3>, Params<4096>, Params<7777>>
    Sizes;

template <typename TypeParam> class ShortAlloc : public ::testing::Test {};
#ifdef TYPED_TEST_SUITE_P
TYPED_TEST_SUITE_P(ShortAlloc);
#else
TYPED_TEST_CASE_P(ShortAlloc);
TYPED_TEST_CASE(ShortAlloc, Sizes);
#endif

//------------------------------------------------------------------------------

TYPED_TEST_P(ShortAlloc, stack_NOoutlive) {
  using arena_type = typename TypeParam::arena_NOoutlive_type;
  using alloc_type = typename TypeParam::alloc_NOoutlive_type;
  const auto area_size = TypeParam::area_size;

  for (std::size_t item_size = 1; item_size <= area_size + 1;
       item_size += 1 + item_size * 8 / 7) {

    std::unique_ptr<arena_type> arena(new arena_type);
    alloc_type short_alloc(*arena);
    std::stack<char *> stack;

    try {
      while (true) {
        char *ptr = short_alloc.allocate(item_size);
        *ptr = char(stack.size() - 42);
        stack.push(ptr);
      }
    } catch (const typename alloc_type::arena_exhausted_exception &) {
    }

    EXPECT_GE(area_size, arena->used());
    EXPECT_LE(area_size,
              arena->used() + alignof(std::max_align_t) + item_size - 1);

    while (!stack.empty()) {
      char *ptr = stack.top();
      stack.pop();
      EXPECT_EQ(char(stack.size() - 42), *ptr);
      short_alloc.deallocate(ptr, item_size);
    }

    EXPECT_EQ(0, arena->used());
  }
}

TYPED_TEST_P(ShortAlloc, fifo_NOoutlive) {
  using arena_type = typename TypeParam::arena_NOoutlive_type;
  using alloc_type = typename TypeParam::alloc_NOoutlive_type;
  const auto area_size = TypeParam::area_size;

  for (std::size_t item_size = 1; item_size <= area_size;
       item_size += 1 + item_size * 3 / 2) {

    std::unique_ptr<arena_type> arena(new arena_type);
    alloc_type short_alloc(*arena);
    std::deque<char *> fifo;

    try {
      while (true) {
        char *ptr = short_alloc.allocate(item_size);
        *ptr = char(fifo.size() - 42);
        fifo.push_back(ptr);
      }
    } catch (const typename alloc_type::arena_exhausted_exception &) {
    }

    EXPECT_GE(area_size, arena->used());
    EXPECT_LE(area_size,
              arena->used() + alignof(std::max_align_t) + item_size - 1);

    const std::size_t used_while_exhausted = arena->used();
    const bool single_allocation = fifo.size() < 2;
    const std::size_t n = fifo.size();
    while (!fifo.empty()) {
      char *ptr = fifo.front();
      EXPECT_EQ(char(n - fifo.size() - 42), *ptr);
      short_alloc.deallocate(ptr, item_size);
      fifo.pop_front();
    }

    if (single_allocation) {
      EXPECT_EQ(0, arena->used());
    } else {
      EXPECT_LT(0, arena->used());
      EXPECT_GT(used_while_exhausted, arena->used());
    }
  }
}

//------------------------------------------------------------------------------

TYPED_TEST_P(ShortAlloc, stack_outlive) {
  using arena_type = typename TypeParam::arena_outlive_type;
  using alloc_type = typename TypeParam::alloc_outlive_type;
  const auto area_size = TypeParam::area_size;

  for (std::size_t item_size = 1; item_size <= area_size;
       item_size += 1 + item_size * 3 / 2) {

    std::unique_ptr<arena_type> arena(new arena_type);
    alloc_type short_alloc(*arena);
    std::stack<char *> stack;

    std::size_t volume = 0;
    while (volume < area_size * 2) {
      char *ptr = short_alloc.allocate(item_size);
      *ptr = char(stack.size() - 42);
      stack.push(ptr);
      volume += item_size;
    }

    EXPECT_GE(area_size, arena->used());
    EXPECT_LE(area_size,
              arena->used() + alignof(std::max_align_t) + item_size - 1);

    while (!stack.empty()) {
      char *ptr = stack.top();
      stack.pop();
      EXPECT_EQ(char(stack.size() - 42), *ptr);
      short_alloc.deallocate(ptr, item_size);
    }

    EXPECT_EQ(0, arena->used());
  }
}

TYPED_TEST_P(ShortAlloc, fifo_outlive) {
  using arena_type = typename TypeParam::arena_outlive_type;
  using alloc_type = typename TypeParam::alloc_outlive_type;
  const auto area_size = TypeParam::area_size;

  for (std::size_t item_size = 1; item_size <= area_size;
       item_size += 1 + item_size * 3 / 2) {

    std::unique_ptr<arena_type> arena(new arena_type);
    alloc_type short_alloc(*arena);
    std::deque<char *> fifo;

    std::size_t max_used = 0;
    std::size_t volume = 0;
    std::size_t allocations_inside_arena = 0;
    while (volume < area_size * 2) {
      char *ptr = short_alloc.allocate(item_size);
      max_used = std::max(max_used, arena->used());
      allocations_inside_arena += arena->pointer_in_bounds(ptr);
      *ptr = char(fifo.size() - 42);
      fifo.push_back(ptr);
      volume += item_size;
    }

    EXPECT_EQ(max_used, arena->used());
    EXPECT_GE(area_size, arena->used());
    EXPECT_LE(area_size,
              arena->used() + alignof(std::max_align_t) + item_size - 1);

    const std::size_t n = fifo.size();
    while (!fifo.empty()) {
      char *ptr = fifo.front();
      EXPECT_EQ(char(n - fifo.size() - 42), *ptr);
      short_alloc.deallocate(ptr, item_size);
      fifo.pop_front();
    }

    if (allocations_inside_arena < 2) {
      EXPECT_EQ(0, arena->used());
    } else {
      EXPECT_LT(0, arena->used());
      EXPECT_GT(max_used, arena->used());
    }
  }
}

//------------------------------------------------------------------------------

#ifdef REGISTER_TYPED_TEST_SUITE_P
REGISTER_TYPED_TEST_SUITE_P(ShortAlloc, stack_NOoutlive, fifo_NOoutlive,
                            stack_outlive, fifo_outlive);

INSTANTIATE_TYPED_TEST_SUITE_P(Test, ShortAlloc, Sizes);
#endif

int main(int argc, char **argv) {
#if __GLIBC_PREREQ(2, 4)
  mallopt(M_CHECK_ACTION, 7);
  mallopt(M_PERTURB, 42);
#endif
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
