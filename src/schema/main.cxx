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
#include "fast_positive/defs.h"
#include "filesystem.h"
#include "interfaces.h"

#include <cstdarg>
#include <cstdio>
#include <iostream>

namespace fptu {
namespace Schema {
namespace Compiler {

Options options;

} /* namespace Compiler */
} /* namespace Schema */
} /* namespace fptu */

using namespace fptu::Schema::Compiler;
static std::unique_ptr<IFrontend> engine;

static void usage() {
  std::cout
      << "Usage: %s [OPTIONS]... SOURCE-FILE...\n"
         "\n"
         "fptu Scheme Compiler options:\n"
         "  -h, --help         display this help and exit\n"
         "      --version      output version information and exit\n"
         "      --verbose      turn verbose mode\n"
         "  -u, --update       update source for ID's injection\n"
         "  -r, --reset        reset all ID's assignation\n"
         "  -o, --output       basename for place output to files\n"
         "\n"
         "Copyright 2016-2017 libfptu authors: please see AUTHORS file at "
         "https://github.com/leo-yuriev/libfptu.\n"
         "\n"
         "License GPLv3+: GNU GPL version 3 or later "
         "<http://gnu.org/licenses/gpl.html>.\n"
         "This is free software: you are free to change and redistribute it.\n"
         "There is NO WARRANTY, to the extent permitted by law.\n"
         "\n"
         "Written by Leonid V. Yuriev.\n"
         "\n";
}

static bool parse_option(int argc, char *const argv[], int &narg,
                         const char option_short, const char *option_long,
                         const char **value = nullptr) {
  assert(narg < argc);
  const char *current = argv[narg];
  const size_t optlen = strlen(option_long);

  if (option_short && current[0] == '-' && current[1] == option_short &&
      current[2] == '\0') {
    if (value) {
      if (narg + 1 >= argc || argv[narg + 1][0] == '-') {
        engine->Error("No value given for '-%c' option\n", option_short);
        exit(EXIT_FAILURE);
      }
      *value = argv[narg + 1];
      ++narg;
    }
    return true;
  }

  if (strncmp(current, "--", 2) || strncmp(current + 2, option_long, optlen))
    return false;

  if (!value) {
    if (current[optlen + 2] == '=') {
      engine->Error("Option '--%s' doen't accept any value\n", option_long);
      exit(EXIT_FAILURE);
    }
    return true;
  }

  if (current[optlen + 2] == '=') {
    *value = &current[optlen + 3];
    return true;
  }

  if (narg + 1 >= argc || argv[narg + 1][0] == '-') {
    engine->Error("No value given for '--%s' option\n", option_long);
    exit(EXIT_FAILURE);
  }

  *value = argv[narg + 1];
  ++narg;
  return true;
}

int main(int argc, char *argv[]) {
  engine.reset(IFrontend::Create());

  std::vector<std_filesystem::path> sources;
  for (int narg = 1; narg < argc; ++narg) {
    const char *value = nullptr;

    if (parse_option(argc, argv, narg, 'h', "help")) {
      usage();
      return EXIT_SUCCESS;
    }
    if (parse_option(argc, argv, narg, '\0', "version")) {
      std::cout << "Version 0.0, " << __DATE__ " " __TIME__ << std::endl;
      return EXIT_SUCCESS;
    }
    if (parse_option(argc, argv, narg, '\0', "verbose")) {
      options.verbose = true;
      continue;
    }
    if (parse_option(argc, argv, narg, 'u', "update")) {
      options.update = true;
      continue;
    }
    if (parse_option(argc, argv, narg, 'r', "reset")) {
      options.update = true;
      continue;
    }
    if (parse_option(argc, argv, narg, 'o', "output", &value)) {
      if (!options.output_basename.empty())
        engine->Error("Basename for output files already set\n");
      options.output_basename = value;
      if (options.output_basename.empty())
        engine->Error("Invalid value '%s' for output files basename\n", value);
      continue;
    }

    if (argv[narg][0] == '-') {
      engine->Error("Unknown option '%s'\n", argv[narg]);
      return EXIT_FAILURE;
    }

    sources.push_back(argv[narg]);
  }

  if (sources.empty())
    engine->Error("No source file(s)\n");
  else {
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
  }

  return engine->Ok() ? EXIT_SUCCESS : EXIT_FAILURE;
}
