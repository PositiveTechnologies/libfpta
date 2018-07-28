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
#include "fast_positive/tuples_internal.h"
#include "interfaces.h"
#include <fstream>

namespace fptu {
namespace Schema {
namespace Compiler {

static std::string slurp(const std_filesystem::path &filename) {
  std::string result;
  std::ifstream sink;
  sink.exceptions(std::ios::failbit | std::ios::badbit);
  sink.imbue(std::locale::classic());
  sink.open(filename);

  std::getline(
      sink, result,
      std::string::traits_type::to_char_type(std::string::traits_type::eof()));
  sink.close();
  return result;
}

/* Простейщая реализация ISource средствами std. */
class Sourcer final : protected std::string, public ISourcer {
protected:
  /* При обновлении указывает на первый НЕ записанный символ. */
  const Symbol *tail_;

  std::ofstream sink_;

  std_filesystem::path temporary(bool finally = false) const {
    std_filesystem::path path = filename_;
    return path.replace_extension(finally ? ".pts-old" : ".pts-new");
  }

public:
  Sourcer(const std_filesystem::path &filename)
      : std::string(slurp(filename)),
        ISourcer(filename, char2symbol(data()), char2symbol(data()) + size()),
        tail_(nullptr) {}

  /* Начинает процесс обновления. */
  void Start() {
    /* На всякий отменяем если что-то уже было начато. */
    Done(false);

    /* Теперь просто открываем файл для записи. */
    sink_.exceptions(std::ios::failbit | std::ios::badbit);
    sink_.open(temporary(), std::ios::out | std::ios::trunc);
    tail_ = ISourcer::begin();
  }

  /* Завершает, либо откатывает изменения. */
  void Done(bool commit) {
    std_filesystem::path save = temporary(true);
    std_filesystem::path temp = temporary(false);

    if (sink_.is_open()) {
      if (!commit) {
        sink_.close();
        std_filesystem::remove(temporary());
      } else {
        if (tail_ < ISourcer::end())
          sink_.write(symbol2char(tail_), ISourcer::end() - tail_);

        sink_.close();
        if (std_filesystem::exists(save))
          std_filesystem::remove(save);

        std_filesystem::rename(filename_, save);
        std_filesystem::rename(temp, filename_);
        std_filesystem::remove(save);
      }
    }
  }

  Location Where(const Symbol *at) const {
    if (at < begin_ || at > end_)
      throw std::out_of_range("Position out of source scope");

    unsigned line = 1;
    unsigned column = 1;
    for (const Symbol *seek = begin_; seek != at; ++seek) {
      switch (*seek) {
      case '\n':
        ++line;
        column = 1;
        [[fallthrough]];
      case '\r':
        continue;
      default:
        ++column;
      }
    }
    return Location(filename(), line, column);
  }

  ~Sourcer() { Done(false); }
};

ISourcer *ISourcer::Create(const std_filesystem::path &filename) {
  return new Sourcer(filename);
}

} /* namespace Compiler */
} /* namespace Schema */
} /* namespace fptu */
