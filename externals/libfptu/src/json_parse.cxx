/*
 * Copyright 2016-2018 libfptu authors: please see AUTHORS file.
 *
 * This file is part of libfptu, aka "Fast Positive Tuples".
 *
 * libfptu is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * libfptu is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with libfptu.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "bitset4tags.h"
#include "fast_positive/tuples_internal.h"

/* TODO */

char *make_utf8(unsigned code, char *ptr) {
  if (code < 0x80) {
    *ptr++ = static_cast<char>(code);
  } else if (code < 0x800) {
    *ptr++ = static_cast<char>(0xC0 | (code >> 6));
    *ptr++ = static_cast<char>(0x80 | (code & 0x3F));
  } else if (code < 0x10000) {
    *ptr++ = static_cast<char>(0xE0 | (code >> 12));
    *ptr++ = static_cast<char>(0x80 | ((code >> 6) & 0x3F));
    *ptr++ = static_cast<char>(0x80 | (code & 0x3F));
  } else {
    assert(code < 0x200000);
    *ptr++ = static_cast<char>(0xF0 | (code >> 18));
    *ptr++ = static_cast<char>(0x80 | ((code >> 12) & 0x3F));
    *ptr++ = static_cast<char>(0x80 | ((code >> 6) & 0x3F));
    *ptr++ = static_cast<char>(0x80 | (code & 0x3F));
  }
  return ptr;
}
