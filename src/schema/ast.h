/*
 * Copyright 2016-2017 libfptu authors: please see AUTHORS file.
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

/*
 * libfptu = { Fast Positive Tuples, aka Позитивные Кортежи }
 *
 * The kind of lightweight linearized tuples, which are extremely handy
 * to machining, including cases with shared memory.
 * Please see README.md at https://github.com/leo-yuriev/libfptu
 *
 * The Future will Positive. Всё будет хорошо.
 *
 * "Позитивные Кортежи" дают легковесное линейное представление небольших
 * JSON-подобных структур в экстремально удобной для машины форме,
 * в том числе при размещении в разделяемой памяти.
 */

#pragma once

#include <cstdio>
#include <deque>
#include <functional>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <vector>

#include "fast_positive/schema.h"

namespace fptu {
namespace Schema {
namespace Compiler {

/* Символ, с которыми работают лексер и парсер. */
typedef unsigned char Symbol;

/* Идентификатор токенов. */
typedef unsigned TokenId;

/* Не определяется в grammar.h, но 0 НЕ ДОЛЖЕН использоваться */
#define TOKEN_COMMENT 0
/* Здесь раннее определение для независимости фроненда от grammar.h */
#define TOKEN_EOF 1

/* Токен при взаимодействии с лексером и парсером. Должен быть POD. */
struct Token {
  const Symbol *text;
  unsigned length;

  const Symbol *begin() const { return text; }
  const Symbol *end() const { return text + length; }

  std::string string() const {
    static_assert(sizeof(char) == sizeof(Symbol), "sizeof(Symbol)?");
    return std::string((const char *)text, length);
  }
};

inline std::ostream &operator<<(std::ostream &os, const Token &text) {
  return os.write((const char *)text.text, text.length * sizeof(Symbol));
}

/* Локация (строка, позиция) в исходном тексте схемы. */
struct Location {
  const char *filename;
  unsigned line;
  unsigned position;

  Location(const char *_filename, unsigned _line, unsigned _position)
      : filename(_filename), line(_line), position(_position) {}

  Location() : Location("unknown", 0, 0) {}
};

inline unsigned length(const Symbol *str) {
  return ::strlen((const char *)str);
}

class exception : public std::exception {
  const Token token_;
  const std::string message_;

public:
  exception(const Token &token, std::string &&message)
      : token_(token), message_(message) {}
  const Token &token() const { return token_; }
  virtual const char *what() const noexcept { return message_.c_str(); }

  // This declaration is not useless:
  // http://gcc.gnu.org/onlinedocs/gcc-3.0.2/gcc_6.html#SEC118
  virtual ~exception() {}
};

void Throw_InvalidTypenameLength(const Token &token, const char *reason);
void Throw_InvalidTypeIdValue(const Token &token);

/* Имя базового типа - это имя уже определенного типа при обращении к нему.
 * Например при декларации поля или производного типа. */
struct BaseName : public std::vector<Token> {
  BaseName(const BaseName &) = delete;

  /* Полная длина имени в символах.
   * Используется только(?) для контроля ограничений. */
  unsigned total;

  BaseName(const Token &word) : total(0) {
    reserve(4);
    Append(word);
  }

  void Append(const Token &token);
  std::ostream &Join(std::ostream &, const char *delimiter = ".") const;
  std::string Join(const char *delimiter = ".") const;
};

std::ostream &operator<<(std::ostream &os, const BaseName &name);

/* Идентификатор поля, типа или enum-элемента при объявлении.
 * Имеет опциональный тэг для связи с внешнимми справочниками. */
struct Ident : public Token {
  Token tag;

  Ident(const Token &name) : Token(name) {
    tag.text = nullptr;
    tag.length = 0;
  }

  enum {
    kMinimalLength = 1,
    kMaximalLength = 64,
    kMaximalTotalLength = 1024,
    kMaximalTotalWords = 32
  };

  Ident(const Token &name, const Token &tag) : Token(name), tag(tag) {}
};

class NodeList;
class Node;

std::string fqtn(const BaseName *);
std::string fqtn(const Node *);

/* Узел AST-дерева.
 * Внутри компилятора используется для представления и обработки всех
 * деклараций, кроме "designated" */
struct Node {
  Node(const Node &) = delete;

  /* Идентификтор (имя и внешний тэг). */
  std::unique_ptr<Ident> ident_;

  /* Полное имя базового типа. */
  std::unique_ptr<BaseName> base_;

  /* Дочерние узлы. */
  std::unique_ptr<NodeList> child_;

  /* Флажки */
  enum Flags : unsigned {
    deprecated = 1 << 0,
    type = 1 << 1,
    field = 1 << 2,
    edef = 1 << 3,

    optional = 1 << 4,
    repeated = 1 << 5,
    array = 1 << 6,
    map = 1 << 7,
  };
  unsigned flags_;

  /* Указатель на родительский узел. */
  Node *parent_;

  /* Уровень глубины, начиная с 1. */
  unsigned deep_;

  /* Идентификатор нативного типа */
  unsigned native_;

  Node(unsigned flags, std::unique_ptr<Ident> &&ident,
       std::unique_ptr<BaseName> &&base, std::unique_ptr<NodeList> &&child)
      : ident_(std::move(ident)), base_(std::move(base)),
        child_(std::move(child)), flags_(flags), parent_(nullptr), deep_(0),
        native_(0) {}

  Ident *ident() { return ident_.get(); }

  const Ident *ident() const { return ident_.get(); }

  const Token &name() const { return *ident(); }

  const Node *parent() const { return parent_; }

  int compare(const Node &node) const { return compare(this, &node); }

  int compare(const BaseName &fqtn) const { return compare(this, fqtn); }

  static int compare(const Node *a, const Node *b);
  static int compare(const Node *node, const BaseName &fqtn);

  fptu_typeKey typekey() const;
};

/* Список узлов.
 * Хранит указатели и владеет этими объектами посредством unique_ptr.
 * Все остальные контейнеры используют только ссылки на узлы,
 * не владеют объектами и не разрушают их. */
struct NodeList : public std::deque<std::unique_ptr<Node>> {
  NodeList(const NodeList &) = delete;

  NodeList() {}
  Node *Append(std::unique_ptr<Node> &&node) {
    push_back(std::move(node));
    return back().get();
  }
};

struct Designation {
  unsigned field_id;
  BaseName *name_ptr;
  BaseName *base_ptr;
  unsigned native;
};

struct DesignationList : public std::deque<std::unique_ptr<Designation>> {
  DesignationList(const DesignationList &) = delete;
  DesignationList() {}
  Designation *Append(std::unique_ptr<Designation> &&designation) {
    push_back(std::move(designation));
    return back().get();
  }
};

struct Schema {
  unsigned version_major;
  unsigned version_minor;
  unsigned version_revision;
  uint64_t digest_lo, digest_hi;

  std::unique_ptr<NodeList> declaration;
  std::unique_ptr<NodeList> designation;

  Schema(unsigned major, unsigned minor, unsigned revision, uint64_t digest_lo,
         uint64_t digest_hi)
      : version_major(major), version_minor(minor), version_revision(revision),
        digest_lo(digest_lo), digest_hi(digest_hi) {}

  Schema(unsigned major) : Schema(major, 0, 0, 0, 0) {}
};

} /* namespace Compiler */
} /* namespace Schema */
} /* namespace fptu */
