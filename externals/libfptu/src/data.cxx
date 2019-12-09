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

#include "fast_positive/tuples_internal.h"

const char fptu_empty_cstr[1] = {'\0'};

const uint8_t fptu_internal_map_t2b[fptu_cstr] = {
    /* void */ 0,
    /* uint16 */ 0,

    /* int32 */ 4,
    /* unt32 */ 4,
    /* fp32 */ 4,

    /* int64 */ 8,
    /* uint64 */ 8,
    /* fp64 */ 8,
    /* datetime */ 8,

    /* 96 */ 12,
    /* 128 */ 16,
    /* 160 */ 20,
    /* 256 */ 32};

const uint8_t fptu_internal_map_t2u[fptu_cstr] = {
    /* void */ 0,
    /* uint16 */ 0,

    /* int32 */ 1,
    /* unt32 */ 1,
    /* fp32 */ 1,

    /* int64 */ 2,
    /* uint64 */ 2,
    /* fp64 */ 2,
    /* datetime */ 2,

    /* 96 */ 3,
    /* 128 */ 4,
    /* 160 */ 5,
    /* 256 */ 8};
