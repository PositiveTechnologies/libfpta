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

#include <boost/program_options.hpp>
#include <cstdarg>
#include <iostream>

namespace fptu {
namespace Schema {
namespace Compiler {

Options options;

} /* namespace Compiler */
} /* namespace Schema */
} /* namespace fptu */

int main(int argc, char *argv[]) {
  using namespace fptu::Schema::Compiler;
  namespace po = boost::program_options;

  po::options_description options_desc("fptu Scheme Compiler options");
  options_desc.add_options()("help,h", "produce help message");
  options_desc.add_options()("version,v", "print version information");
  options_desc.add_options()("verbose,V", "turn verbose mode");
  options_desc.add_options()("update,u", "update source for ID's injection");
  options_desc.add_options()("reset,r", "reset all ID's assignation");
  options_desc.add_options()("output,o",
                             po::value<std::string>(&options.output_basename),
                             "a basename for place output to files");
  options_desc.add_options()(
      "source-file", po::value<std::vector<std::string>>(), "source file");

  po::positional_options_description positional_options;
  positional_options.add("source-file", -1);

  po::variables_map options_mapping;
  po::store(po::command_line_parser(argc, argv)
                .options(options_desc)
                .positional(positional_options)
                .run(),
            options_mapping);
  po::notify(options_mapping);

  if (options_mapping.count("help")) {
    std::cout << options_desc << std::endl;
    return EXIT_SUCCESS;
  }

  if (options_mapping.count("version")) {
    std::cout << "Version 0.0, " << __DATE__ " " __TIME__ << std::endl;
    return EXIT_SUCCESS;
  }

  if (options_mapping.count("verbose"))
    options.verbose = true;

  if (options_mapping.count("update"))
    options.update = true;

  if (options_mapping.count("reset"))
    options.reset = true;

  std::unique_ptr<IFrontend> engine(IFrontend::Create());

  if (options_mapping.count("source-file")) {
    std::vector<std::string> sources =
        options_mapping["source-file"].as<std::vector<std::string>>();

    for (const auto &item : sources)
      engine->Load(item.c_str());

    engine->Commit();
    if (engine->Ok() && options.update && engine->NeedUpdate())
      engine->Update();

    if (engine->Ok() && !options.output_basename.empty()) {
      if (engine->NeedUpdate())
        engine->Error("the sources need a fixup");
      else
        engine->Product(options.output_basename);
    }
  } else
    engine->Error("no input files");

  return engine->Ok() ? EXIT_SUCCESS : EXIT_FAILURE;
}
