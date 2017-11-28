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

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/iostreams/stream.hpp>

namespace fptu {
namespace Schema {
namespace Compiler {

/* Простейщая реализация ISource средствами boost. */
class Sourcer : protected boost::iostreams::mapped_file_source,
                public ISourcer {
protected:
  /* При обновлении указывает на первый НЕ записанный символ. */
  const Symbol *tail_;

  boost::iostreams::stream<boost::iostreams::file_sink> sink_;

  std::string temporary(bool finally = false) const {
    return boost::filesystem::change_extension(filename_, finally ? ".pts-old"
                                                                  : ".pts-new")
        .string();
  }

public:
  Sourcer(const char *filename)
      : boost::iostreams::mapped_file_source(filename),
        ISourcer(filename, (Symbol *)data(), (Symbol *)(data() + size())),
        tail_(0) {}

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
    std::string save = temporary(true);
    std::string temp = temporary(false);

    if (sink_.is_open()) {
      if (!commit) {
        sink_.close();
        boost::filesystem::remove(temporary());
      } else {
        if (tail_ < ISourcer::end())
          sink_.write((const char *)tail_, ISourcer::end() - tail_);

        sink_.close();
        if (boost::filesystem::exists(save))
          boost::filesystem::remove(save);

        boost::filesystem::rename(filename_, save);
        boost::filesystem::rename(temp, filename_);
        boost::filesystem::remove(save);
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

ISourcer *ISourcer::Create(const char *filename) {
  return new Sourcer(filename);
}

} /* namespace Compiler */
} /* namespace Schema */
} /* namespace fptu */
