/* C++ code produced by gperf version 3.1 */
/* Command-line: gperf --constants-prefix=ECMAScript_keywords__ -Z
 * ECMAScript_keywords -7 --readonly-tables --pic --language=C++ */
/* Computed positions: -k'1-2' */

#if !(                                                                         \
    (' ' == 32) && ('!' == 33) && ('"' == 34) && ('#' == 35) && ('%' == 37) && \
    ('&' == 38) && ('\'' == 39) && ('(' == 40) && (')' == 41) &&               \
    ('*' == 42) && ('+' == 43) && (',' == 44) && ('-' == 45) && ('.' == 46) && \
    ('/' == 47) && ('0' == 48) && ('1' == 49) && ('2' == 50) && ('3' == 51) && \
    ('4' == 52) && ('5' == 53) && ('6' == 54) && ('7' == 55) && ('8' == 56) && \
    ('9' == 57) && (':' == 58) && (';' == 59) && ('<' == 60) && ('=' == 61) && \
    ('>' == 62) && ('?' == 63) && ('A' == 65) && ('B' == 66) && ('C' == 67) && \
    ('D' == 68) && ('E' == 69) && ('F' == 70) && ('G' == 71) && ('H' == 72) && \
    ('I' == 73) && ('J' == 74) && ('K' == 75) && ('L' == 76) && ('M' == 77) && \
    ('N' == 78) && ('O' == 79) && ('P' == 80) && ('Q' == 81) && ('R' == 82) && \
    ('S' == 83) && ('T' == 84) && ('U' == 85) && ('V' == 86) && ('W' == 87) && \
    ('X' == 88) && ('Y' == 89) && ('Z' == 90) && ('[' == 91) &&                \
    ('\\' == 92) && (']' == 93) && ('^' == 94) && ('_' == 95) &&               \
    ('a' == 97) && ('b' == 98) && ('c' == 99) && ('d' == 100) &&               \
    ('e' == 101) && ('f' == 102) && ('g' == 103) && ('h' == 104) &&            \
    ('i' == 105) && ('j' == 106) && ('k' == 107) && ('l' == 108) &&            \
    ('m' == 109) && ('n' == 110) && ('o' == 111) && ('p' == 112) &&            \
    ('q' == 113) && ('r' == 114) && ('s' == 115) && ('t' == 116) &&            \
    ('u' == 117) && ('v' == 118) && ('w' == 119) && ('x' == 120) &&            \
    ('y' == 121) && ('z' == 122) && ('{' == 123) && ('|' == 124) &&            \
    ('}' == 125) && ('~' == 126))
/* The character set is not based on ISO-646.  */
#error                                                                         \
    "gperf generated tables don't work with this execution character set. Please report a bug to <bug-gperf@gnu.org>."
#endif

#define ECMAScript_keywords__TOTAL_KEYWORDS 42
#define ECMAScript_keywords__MIN_WORD_LENGTH 2
#define ECMAScript_keywords__MAX_WORD_LENGTH 10
#define ECMAScript_keywords__MIN_HASH_VALUE 2
#define ECMAScript_keywords__MAX_HASH_VALUE 55
/* maximum key range = 54, duplicates = 0 */

class ECMAScript_keywords {
private:
  static inline unsigned int hash(const char *str, size_t len);

public:
  static const char *in_word_set(const char *str, size_t len);
};

inline unsigned int ECMAScript_keywords::hash(const char *str, size_t len) {
  static const unsigned char asso_values[] = {
      56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56,
      56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56,
      56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56,
      56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56,
      56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56,
      56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56,
      56, 10, 15, 0,  0,  0,  10, 56, 30, 0,  56, 56, 15, 40, 0,  20,
      10, 56, 15, 5,  5,  20, 30, 20, 35, 0,  56, 56, 56, 56, 56, 56};
  return (unsigned)len + asso_values[static_cast<unsigned char>(str[1])] +
         asso_values[static_cast<unsigned char>(str[0])];
}

const char *ECMAScript_keywords::in_word_set(const char *str, size_t len) {
  struct stringpool_t {
    char stringpool_str2[sizeof("in")];
    char stringpool_str3[sizeof("new")];
    char stringpool_str4[sizeof("enum")];
    char stringpool_str5[sizeof("yield")];
    char stringpool_str6[sizeof("delete")];
    char stringpool_str7[sizeof("default")];
    char stringpool_str8[sizeof("debugger")];
    char stringpool_str9[sizeof("interface")];
    char stringpool_str10[sizeof("instanceof")];
    char stringpool_str11[sizeof("typeof")];
    char stringpool_str12[sizeof("if")];
    char stringpool_str14[sizeof("case")];
    char stringpool_str15[sizeof("catch")];
    char stringpool_str16[sizeof("static")];
    char stringpool_str17[sizeof("finally")];
    char stringpool_str18[sizeof("let")];
    char stringpool_str19[sizeof("else")];
    char stringpool_str20[sizeof("class")];
    char stringpool_str21[sizeof("return")];
    char stringpool_str22[sizeof("do")];
    char stringpool_str23[sizeof("try")];
    char stringpool_str24[sizeof("with")];
    char stringpool_str25[sizeof("const")];
    char stringpool_str27[sizeof("package")];
    char stringpool_str28[sizeof("continue")];
    char stringpool_str30[sizeof("super")];
    char stringpool_str31[sizeof("switch")];
    char stringpool_str32[sizeof("private")];
    char stringpool_str33[sizeof("for")];
    char stringpool_str34[sizeof("protected")];
    char stringpool_str35[sizeof("break")];
    char stringpool_str36[sizeof("public")];
    char stringpool_str38[sizeof("function")];
    char stringpool_str39[sizeof("this")];
    char stringpool_str40[sizeof("throw")];
    char stringpool_str41[sizeof("export")];
    char stringpool_str42[sizeof("extends")];
    char stringpool_str43[sizeof("var")];
    char stringpool_str46[sizeof("import")];
    char stringpool_str50[sizeof("implements")];
    char stringpool_str54[sizeof("void")];
    char stringpool_str55[sizeof("while")];
  };
  static const struct stringpool_t stringpool_contents = {
      "in",      "new",      "enum",      "yield",      "delete",
      "default", "debugger", "interface", "instanceof", "typeof",
      "if",      "case",     "catch",     "static",     "finally",
      "let",     "else",     "class",     "return",     "do",
      "try",     "with",     "const",     "package",    "continue",
      "super",   "switch",   "private",   "for",        "protected",
      "break",   "public",   "function",  "this",       "throw",
      "export",  "extends",  "var",       "import",     "implements",
      "void",    "while"};
#define stringpool ((const char *)&stringpool_contents)
  static const int wordlist[] = {
      -1,
      -1,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str2,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str3,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str4,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str5,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str6,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str7,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str8,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str9,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str10,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str11,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str12,
      -1,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str14,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str15,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str16,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str17,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str18,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str19,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str20,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str21,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str22,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str23,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str24,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str25,
      -1,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str27,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str28,
      -1,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str30,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str31,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str32,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str33,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str34,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str35,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str36,
      -1,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str38,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str39,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str40,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str41,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str42,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str43,
      -1,
      -1,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str46,
      -1,
      -1,
      -1,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str50,
      -1,
      -1,
      -1,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str54,
      (int)(size_t) & ((struct stringpool_t *)0)->stringpool_str55};

  if (len <= ECMAScript_keywords__MAX_WORD_LENGTH &&
      len >= ECMAScript_keywords__MIN_WORD_LENGTH) {
    unsigned int key = hash(str, len);

    if (key <= ECMAScript_keywords__MAX_HASH_VALUE) {
      int o = wordlist[key];
      if (o >= 0) {
        const char *s = o + stringpool;

        if (*str == *s && !strcmp(str + 1, s + 1))
          return s;
      }
    }
  }
  return 0;
}
