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
#include "filesystem.h"

namespace fptu {
namespace Schema {
namespace Compiler {

struct Options {
  /* Нужно ли проставить в исходниках назначенные дентификаторы */
  bool update;

  /* Переназначить все идентификаторы */
  bool reset;

  /* Вывод протокола */
  bool verbose;

  /* Базовое имя для продуктов компиляции */
  std_filesystem::path output_basename;

  std::vector<std_filesystem::path> sources; /*  имена исходных файлов */

  Options() : update(false), reset(false), verbose(false) {}
};

extern Options options;

class IFrontend;

/* Базовый пустой абстрактный интерфейс.
 * Не копируемый, но с виртуальным деструктором */
class IPure {
  IPure(const IPure &) = delete;

protected:
  IPure() {}
  virtual ~IPure() {}
};

/* Интерфейс поставщика исходных текстов схемы.
 *
 * Используется лексером для чтения лексем, и компилятором для простановки
 * номеров типов после их назначения.
 *
 * Предполагается размещение исходного текста целиком в памяти,
 * но в целом это дело реализации. */
class ISourcer : public IPure {
protected:
  const Symbol *const begin_;
  const Symbol *const end_;
  const std_filesystem::path filename_;

  ISourcer(const std_filesystem::path &filename, const Symbol *const begin,
           const Symbol *const end)
      : begin_(begin), end_(end), filename_(filename) {}

public:
  /* Для лексера - начало исходного кода. */
  const Symbol *begin() const { return begin_; }

  /* Для лексера - конец исходного кода. */
  const Symbol *end() const { return end_; }

  const std_filesystem::path *filename() const { return &filename_; }

  /* Начинает процесс обновления. */
  virtual void Start() = 0;

  /* Завершает, либо откатывает изменения. */
  virtual void Done(bool commit) = 0;

  /* Возвращает локацию по указателю на символ. */
  virtual Location Where(const Symbol *) const = 0;

  /* Статическая фабрика */
  static ISourcer *Create(const std_filesystem::path &filename);
};

/* Интерфейс лексера для использования компилятором.
 *
 * Лексер должен сам организовать взаимодействие с ISourcer и возвращать
 * токены из итеративных вызовов ILexer->scan().
 *
 * С целью генерации сообщений об ошибках, компилятор может вызывать
 * методы ILexer->Where() для получения как текущей позиции,
 * так и для конкретного токена. */
class ILexer : public IPure {
public:
  /* Результат возвращаемый от лексера. К тексту токена добавляется id . */
  struct Result : public Token {
    TokenId id;

    Result(TokenId &id, const Symbol *text, unsigned length) : id(id) {
      this->text = text;
      this->length = length;
    }
  };

  /* Выделяет и возвращает очередную лексему. */
  virtual Result Scan() = 0;

  /* Возвращает локацию для текущей позиции разбора. */
  virtual Location WhereNow() const = 0;

  /* Статическая фабрика */
  static ILexer *Create(IFrontend *frontend, ISourcer *source);
};

/* Интерфейс парсера для использования компилятором.
 *
 * Компилятор итеративно получает токены от Лексера и "кормит" ими Парсер.
 *
 * При этом компилятор отбрасывает комментарии, а в конце исходников
 * вызовет IParser->Commit() в конце исходников.
 *
 * Парсер должен строить AST-представление и когда надо вызывать
 * IСompiler->Import() и IСompiler->Append(). */
class IParser : public IPure {
public:
  virtual void Push(const ILexer::Result &token) = 0;

  /* Статическая фабрика */
  static IParser *Create(IFrontend *frontend);
};

class IBackend : public IPure {
public:
  /* TODO */

  /* Статическая фабрика */
  static IBackend *Create();
};

/* Интерфейс компилятора схемы.
 * Не предполагается что будет потребность заместить реализацию
 * компилятора альтернативой. Тем не мнее, абстрактный интерфейс определен
 * для взаимной изоляции компилятора компонентов. */
class IFrontend : public IPure {
protected:
  bool ok_;          /* не было ошибок */
  bool need_update_; /* нужно обновить исходные тексты */
public:
  virtual void Append(std::unique_ptr<NodeList> &&list) = 0;
  virtual void Import(std::unique_ptr<BaseName> &&name) = 0;
  virtual void Commit() = 0;
  virtual void Load(const std_filesystem::path &filename) = 0;
  virtual void Update() = 0;
  virtual void Product(const std_filesystem::path &basename) = 0;

  IFrontend() : ok_(true), need_update_(false) {}
  __printf_args(2, 3) virtual void Error(const char *, ...);
  __printf_args(2, 3) virtual void Warning(const char *, ...);
  virtual bool Ok() const { return ok_; }
  virtual bool NeedUpdate() const { return need_update_; }
  virtual void HandleException(const std::exception *trouble) = 0;
  void SyntaxError(const Token &);

  /* Возвращает локацию для переданной лексемы. */
  virtual Location Where(const Token &) const = 0;

  /* Статическая фабрика */
  static IFrontend *Create();
};

} /* namespace Compiler */
} /* namespace Schema */
} /* namespace fptu */
