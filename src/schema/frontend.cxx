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
#include "interfaces.h"

#include <map>
#include <set>
#include <stdarg.h>
#include <stdio.h>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/stream.hpp>
#include <sstream>

namespace fptu {
namespace Schema {
namespace Compiler {

void Throw_InvalidTypenameLength(const Token &token, const char *reason) {
  throw exception(
      token, format("Typename '%*s' is %s", token.length, token.text, reason));
}

void Throw_InvalidTypeIdValue(const Token &token) {
  throw exception(token,
                  format("TypeId '%*s' is invalid", token.length, token.text));
}

class Engine : public IFrontend {
protected:
  typedef std::map<TokenId, Node *> nodes_by_id;
  typedef std::map<std::string, Node *> nodes_by_name;

  /* схема как плоский список всех узлов без иерархии. */
  NodeList scheme_;
  /* узлы по номеру типов. */
  nodes_by_id nodes_by_id_;
  /* узлы по полному имени типов. */
  nodes_by_name nodes_by_name_;

  void collect(Node *node);
  void collect(std::unique_ptr<NodeList> &&list, Node *parent);
  Node *find(const BaseName *);
  Node *find(const unsigned);
  void generate_builtins();

  std::unique_ptr<IBackend> builder_;
  typedef std::map<const Symbol *, std::unique_ptr<ISourcer>,
                   std::greater_equal<const Symbol *>>
      sources_list;

  /* Все исходники отсортированные по адресам токенов */
  sources_list sources_;
  /* Стек с путями к исходникам,
   * нужен для правильной обработки вложенности import-директив */
  std::stack<const char *> stack_;

  /* Имя обрабатываемого сейчас файла.
   * Требуется для сообщений об ошибках при обработке исключений */
  const char *current_filename_;

public:
  Engine() { generate_builtins(); }

  void Append(std::unique_ptr<NodeList> &&list) {
    collect(std::move(list), nullptr);
  }

  void Import(std::unique_ptr<BaseName> &&name);
  void Commit();
  void Load(const char *filename);
  void Update();
  void Product(const std::string &basename);
  Location Where(const Token &) const;
  void HandleException(const std::exception *trouble);
};

IFrontend *IFrontend::Create() { return new Engine(); }

/* Вывод сообщения об ошибке. */
void IFrontend::Error(const char *msg, ...) {
  ok_ = false;
  fputs("pts-compiler.Error: ", stderr);
  va_list ap;
  va_start(ap, msg);
  vfprintf(stderr, msg, ap);
  putc('\n', stderr);
  va_end(ap);
}

/* Вывод предупреждения. */
void IFrontend::Warning(const char *msg, ...) {
  fputs("pts-compiler.Warning: ", stderr);
  va_list ap;
  va_start(ap, msg);
  vfprintf(stderr, msg, ap);
  putc('\n', stderr);
  va_end(ap);
}

/* Поиск типа по полному имени с диагностикой. */
Node *Engine::find(const BaseName *name) {
  if (name) {
    std::string key = fqtn(name);
    nodes_by_name::iterator at = nodes_by_name_.find(key);
    if (at != nodes_by_name_.end())
      return at->second;

    Error("Undefined type-name '%s'", key.c_str());
  }

  return nullptr;
}

/* Поиск типа номеру с диагностикой. */
Node *Engine::find(const unsigned id) {
  if (id) {
    nodes_by_id::iterator at = nodes_by_id_.find(id);
    if (at != nodes_by_id_.end())
      return at->second;

    Error("Undefined type '%u'", id);
  }

  return nullptr;
}

/* Первый проход, собираем узлы из "дочернего" списка. */
void Engine::collect(std::unique_ptr<NodeList> &&list, Node *parent) {
  if (list) {
    while (!list->empty()) {
      std::unique_ptr<Node> node = std::move(list->front());
      list->pop_front();

      node->parent_ = parent;
      node->deep_ = parent ? parent->deep_ + 1 : 1;
      collect(node.get());
      scheme_.push_back(std::move(node));
    }
  }
}

/* Первый проход, добавляем узел и рекурсивно его дочерние. */
void Engine::collect(Node *node) {
  /* TODO */
  collect(std::move(node->child_), node);
}

void Engine::Commit() {}

//------------------------------------------------------------------------------

/* Импорт (загрузка) части схемы из файла по директиве 'import'. */
void Engine::Import(std::unique_ptr<BaseName> &&name) {
  boost::filesystem::path filename(
      boost::filesystem::path(stack_.top()).parent_path());

  for (BaseName::iterator i = name->begin(), e = name->end(); i != e; ++i)
    filename /= i->string();

  Load(boost::filesystem::change_extension(filename, ".pts").string().c_str());
}

/* Штатный костыль на отсутствие finally в C++. */
template <typename I> class raii_stacker {
  std::stack<I> &stack_;

public:
  raii_stacker(std::stack<I> &stack, const I &i) : stack_(stack) {
    stack_.push(i);
  }

  ~raii_stacker() { stack_.pop(); }
};

/* Загрузка описания схемы из исходного файла. */
void Engine::Load(const char *filename) {
  boost::filesystem::file_status status(boost::filesystem::status(filename));

  if (!boost::filesystem::exists(status))
    Error("the file '%s' is not exists.", filename);
  else if (!boost::filesystem::is_regular_file(status))
    Error("the file '%s' is not a regular file.", filename);
  else if (!boost::filesystem::file_size(filename))
    Warning("the file '%s' is empty.", filename);
  else {
    const size_t before = scheme_.size();
    current_filename_ = filename;

    try {
      std::unique_ptr<ISourcer> sourcer(ISourcer::Create(filename));
      auto ptr = sourcer.get();
      auto key = sourcer->begin();
      sources_[key] = std::move(sourcer);
      std::unique_ptr<ILexer> lexer(ILexer::Create(this, ptr));
      std::unique_ptr<IParser> parser(IParser::Create(this));
      raii_stacker<const char *> guard(stack_, filename);
      while (true) {
        ILexer::Result token = lexer->Scan();
        if (options.verbose) {
          ::fprintf(::stdout, "%d %.*s\n", token.id, token.length, token.text);
          ::fflush(::stdout);
        }
        if (TOKEN_COMMENT != token.id)
          parser->Push(token);
        if (TOKEN_EOF == token.id)
          break;
      }
    } catch (const std::exception &trouble) {
      HandleException(&trouble);
    }
    current_filename_ = stack_.empty() ? nullptr : stack_.top();

    if (before == scheme_.size())
      Warning("no definitions in the file '%s'.", filename);
  }
}

//------------------------------------------------------------------------------

/* Обновление (простановка) назначенных номеров типов в исходных файлах
 * описания схемы. */
void Engine::Update() { current_filename_ = nullptr; }

/* Формирование продуктов компиляции (бинарного справочника схемы и
 * h-файла с определениями). */
void Engine::Product(const std::string &basename) {
  std::string filename;
  //  std::ofstream file;
  //  file.exceptions(std::ios::failbit | std::ios::badbit);

  filename = basename + "-scheme.h";
  current_filename_ = filename.c_str();
  //  file.open(current_filename_, std::ios::out | std::ios::trunc);
  //  builder_.todo(file);
  //  file.close();
  current_filename_ = nullptr;
}

fptu_typeKey Node::typekey() const { return 0 /* FIXME */; }

//------------------------------------------------------------------------------

/* Генерация базовых/встроенных liro-типов. */
void Engine::generate_builtins() { current_filename_ = nullptr; }

/* Возвращает локацию для переданной лексемы. */
Location Engine::Where(const Token &token) const {
  auto i = sources_.lower_bound(token.end());
  if (i == sources_.end() || token.begin() < i->second->begin())
    throw std::out_of_range("Token out of sources");

  return i->second->Where(token.begin());
}

void Engine::HandleException(const std::exception *trouble) {
  const auto trouble_with_token = dynamic_cast<const exception *>(trouble);
  if (trouble_with_token) {
    const Location location = Where(trouble_with_token->token());
    if (location.line || location.position)
      Error("%s, file '%s', line %u, position %u", trouble->what(),
            location.filename, location.line, location.position);
    else
      Error("%s, file '%s'", trouble_with_token->what(), location.filename);
  } else if (current_filename_) {
    Error("exception %s, file '%s'", trouble->what(), current_filename_);
  } else {
    Error("exception %s", trouble->what());
  }
}

void IFrontend::SyntaxError(const Token &token) {
  const Location location = Where(token);
  Error("Syntax at file '%s', line %u, position %u", location.filename,
        location.line, location.position);
}

} /* namespace Compiler */
} /* namespace Schema */
} /* namespace fptu */
