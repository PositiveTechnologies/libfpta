/* https://www.ueber.net/who/mjl/projects/bomstrip/ */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

void usage(char *prog) {
  fprintf(stderr, "usage: %s\n", prog);
  exit(EXIT_FAILURE);
}

const char utf8bom[3] = {'\xef', '\xbb', '\xbf'};

int main(int argc, char *argv[]) {
  char buf[4096];
  if (argc > 1)
    usage(argv[0]);

  size_t nread = fread(buf, 1, sizeof(utf8bom), stdin);
  if (nread > 0) {
    if (nread != sizeof(utf8bom) || memcmp(buf, utf8bom, sizeof(utf8bom)) != 0)
      fwrite(buf, 1, nread, stdout);
    for (;;) {
      nread = fread(buf, 1, sizeof buf, stdin);
      if (nread == 0)
        break;
      fwrite(buf, 1, nread, stdout);
    }
  }
  return feof(stdin) ? EXIT_SUCCESS : EXIT_FAILURE;
}
