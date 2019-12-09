/*
 *  Fast Positive Tuples (libfptu), aka Позитивные Кортежи
 *  Copyright 2016-2019 Leonid Yuriev <leo@yuriev.ru>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
