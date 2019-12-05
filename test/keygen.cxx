/*
 *  Fast Positive Tables (libfpta), aka Позитивные Таблицы.
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

#include "keygen.hpp"
#include "fpta_test.h"

template <fpta_index_type _index, fptu_type _type>
void any_keygen::init_tier::glue() {
  type = _type;
  index = _index;
  maker = keygen<_index, _type>::make;
}

template <fpta_index_type _index>
void any_keygen::init_tier::unroll(fptu_type _type) {
  switch (_type) {
  default /* arrays */:
  case fptu_null:
    assert(false && "wrong type");
    return stub(_type, _index);
  case fptu_uint16:
    return glue<_index, fptu_uint16>();
  case fptu_int32:
    return glue<_index, fptu_int32>();
  case fptu_uint32:
    return glue<_index, fptu_uint32>();
  case fptu_fp32:
    return glue<_index, fptu_fp32>();
  case fptu_int64:
    return glue<_index, fptu_int64>();
  case fptu_uint64:
    return glue<_index, fptu_uint64>();
  case fptu_fp64:
    return glue<_index, fptu_fp64>();
  case fptu_96:
    return glue<_index, fptu_96>();
  case fptu_128:
    return glue<_index, fptu_128>();
  case fptu_160:
    return glue<_index, fptu_160>();
  case fptu_datetime:
    return glue<_index, fptu_datetime>();
  case fptu_256:
    return glue<_index, fptu_256>();
  case fptu_cstr:
    return glue<_index, fptu_cstr>();
  case fptu_opaque:
    return glue<_index, fptu_opaque>();
  case fptu_nested:
    return glue<_index, fptu_nested>();
  }
}

any_keygen::init_tier::init_tier(fptu_type _type, fpta_index_type _index) {
  switch (_index & ~fpta_index_fnullable) {
  default:
    assert(false && "wrong index");
    stub(_type, _index);
    break;
  case fpta_primary_withdups_ordered_obverse:
    unroll<fpta_primary_withdups_ordered_obverse>(_type);
    break;
  case fpta_primary_unique_ordered_obverse:
    unroll<fpta_primary_unique_ordered_obverse>(_type);
    break;
  case fpta_primary_withdups_unordered:
    unroll<fpta_primary_withdups_unordered>(_type);
    break;
  case fpta_primary_unique_unordered:
    unroll<fpta_primary_unique_unordered>(_type);
    break;
  case fpta_primary_withdups_ordered_reverse:
    unroll<fpta_primary_withdups_ordered_reverse>(_type);
    break;
  case fpta_primary_unique_ordered_reverse:
    unroll<fpta_primary_unique_ordered_reverse>(_type);
    break;
  case fpta_secondary_withdups_ordered_obverse:
    unroll<fpta_secondary_withdups_ordered_obverse>(_type);
    break;
  case fpta_secondary_unique_ordered_obverse:
    unroll<fpta_secondary_unique_ordered_obverse>(_type);
    break;
  case fpta_secondary_withdups_unordered:
    unroll<fpta_secondary_withdups_unordered>(_type);
    break;
  case fpta_secondary_unique_unordered:
    unroll<fpta_secondary_unique_unordered>(_type);
    break;
  case fpta_secondary_withdups_ordered_reverse:
    unroll<fpta_secondary_withdups_ordered_reverse>(_type);
    break;
  case fpta_secondary_unique_ordered_reverse:
    unroll<fpta_secondary_unique_ordered_reverse>(_type);
    break;
  }
}

any_keygen::any_keygen(const init_tier &init, fptu_type type,
                       fpta_index_type index)
    : type(type), index(index), maker(init.maker) {
  // страховка от опечаток в параметрах при инстанцировании шаблонов.
  assert(init.type == type);
  assert(init.index == (index & ~fpta_index_fnullable));
}

any_keygen::any_keygen(fptu_type type, fpta_index_type index)
    : any_keygen(init_tier(type, index), type, index) {}

//----------------------------------------------------------------------------

/* простейший медленный тест на простоту,
 * метод Миллера-Рабина будет излишен. */
bool isPrime(unsigned number) {
  if (number < 3)
    return number == 2;
  if (number % 2 == 0)
    return false;
  for (unsigned i = 3; (i * i) <= number; i += 2) {
    if (number % i == 0)
      return false;
  }
  return true;
}

/* кол-во единичных бит */
unsigned hamming_weight(unsigned number) {
  unsigned count = 0;
  while (number) {
    count += number & 1;
    number >>= 1;
  }
  return count;
}
