/*
 * POSIX getopt for Windows
 *
 * AT&T Public License
 *
 * Code given out at the 1985 UNIFORUM conference in Dallas.
 */

#include "wingetopt.h"
#include <stdio.h>
#include <string.h>

#ifndef NULL
#define NULL 0
#endif

#ifndef EOF
#define EOF (-1)
#endif

#define ERR(s, c)                                                              \
  if (opterr) {                                                                \
    fputs(argv[0], stderr);                                                    \
    fputs(s, stderr);                                                          \
    fputc(c, stderr);                                                          \
  }

int opterr = 1;
int optind = 1;
int optopt;
char *optarg;

int getopt(int argc, char *const argv[], const char *opts) {
  static int sp = 1;
  int c;
  char *cp;

  if (sp == 1) {
    if (optind >= argc || argv[optind][0] != '-' || argv[optind][1] == '\0')
      return EOF;
    else if (strcmp(argv[optind], "--") == 0) {
      optind++;
      return EOF;
    }
  }
  optopt = c = argv[optind][sp];
  if (c == ':' || (cp = strchr(opts, c)) == NULL) {
    ERR(": illegal option -- ", c);
    if (argv[optind][++sp] == '\0') {
      optind++;
      sp = 1;
    }
    return '?';
  }
  if (*++cp == ':') {
    if (argv[optind][sp + 1] != '\0')
      optarg = &argv[optind++][sp + 1];
    else if (++optind >= argc) {
      ERR(": option requires an argument -- ", c);
      sp = 1;
      return '?';
    } else
      optarg = argv[optind++];
    sp = 1;
  } else {
    if (argv[optind][++sp] == '\0') {
      sp = 1;
      optind++;
    }
    optarg = NULL;
  }
  return c;
}
