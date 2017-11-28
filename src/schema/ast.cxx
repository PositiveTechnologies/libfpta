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

#include "ast.h"

#include <sstream>

namespace fptu {
namespace Schema {
namespace Compiler {

/* Реализация сравнения узла и полного имени типа на уровне AST. */
int Node::compare(const Node *node, const BaseName &fqtn) {
  int r = node->deep_ - fqtn.size();
  BaseName::const_reverse_iterator i = fqtn.rbegin();

  while (!r && node) {
    assert(i != fqtn.rend());
    r = node->compare(*i);
    node = node->parent_;
    ++i;
  }

  return r;
}

//------------------------------------------------------------------------------

void BaseName::Append(const Token &token) {
  if (token.length < Ident::kMinimalLength)
    Throw_InvalidTypenameLength(token, "too short");

  if (size() > Ident::kMaximalTotalWords ||
      token.length > Ident::kMaximalLength ||
      total + token.length > Ident::kMaximalTotalLength)
    Throw_InvalidTypenameLength(token, "too long");

  push_back(token);
  total += token.length;
}

std::ostream &operator<<(std::ostream &os, const BaseName &name) {
  return name.Join(os);
}

std::ostream &BaseName::Join(std::ostream &os, const char *delimiter) const {
  for (BaseName::const_iterator i = begin(); i != end(); ++i) {
    if (i != begin())
      os << delimiter;
    os << *i;
  }

  return os;
}

std::string BaseName::Join(const char *delimiter) const {
  std::stringstream r;

  Join(r, delimiter);
  return r.str();
}

/* Формируем читабельную строку с полным именем типа по AST-имени. */
std::string fqtn(const BaseName *name) {
  if (name) {
    std::stringstream r;
    r << *name;
    return r.str();
  }

  return "fptu_void";
}

/* Формируем читабельную строку с полным именем типа по AST-узлу. */
std::string fqtn(const Node *node) {
  if (node) {
    BaseName ntqf(node->name());
    while (true) {
      node = node->parent();
      if (!node)
        break;
      ntqf.Append(node->name());
    }

    std::stringstream r;
    BaseName::const_reverse_iterator i = ntqf.rbegin();
    if (i != ntqf.rend()) {
      while (true) {
        r << *i;
        if (++i == ntqf.rend())
          break;
        r << ".";
      }
    }
    return r.str();
  }

  return "fptu_void";
}

} /* namespace Compiler */
} /* namespace Schema */
} /* namespace fptu */
