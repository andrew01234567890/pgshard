package pgwire

import "errors"

// errUnterminated is returned by countStatements for text with an open
// quote, comment or dollar-quote; the caller refuses such queries.
var errUnterminated = errors.New("unterminated quoted string or comment")

// countStatements returns how many non-empty statements sql contains at the
// top level. The lexing rules for quotes, dollar quotes and comments follow
// PostgreSQL's psql lexer (src/fe_utils/psqlscan.l), which shares its rules
// with the backend scanner (src/backend/parser/scan.l): standard strings with
// doubled quotes, E” strings with backslash escapes, quoted identifiers,
// $tag$ ... $tag$ where the tag is an identifier that cannot start with a
// digit, "--" line comments and nested /* */ block comments. Identifiers may
// contain '$', so "a$b$" is an identifier, not a dollar quote.
func countStatements(sql string) (int, error) {
	n := 0
	seen := false
	i := 0
	for i < len(sql) {
		c := sql[i]
		switch {
		case c == ';':
			if seen {
				n++
				seen = false
			}
			i++
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			end, ok := skipBlockComment(sql, i)
			if !ok {
				return 0, errUnterminated
			}
			i = end
		case c == '\'':
			seen = true
			end, ok := skipStandardString(sql, i)
			if !ok {
				return 0, errUnterminated
			}
			i = end
		case c == '"':
			seen = true
			end, ok := skipQuoted(sql, i, '"')
			if !ok {
				return 0, errUnterminated
			}
			i = end
		case (c == 'e' || c == 'E') && i+1 < len(sql) && sql[i+1] == '\'':
			seen = true
			end, ok := skipEscapeString(sql, i+1)
			if !ok {
				return 0, errUnterminated
			}
			i = end
		case c == '$':
			seen = true
			if end, ok, isDollar := skipDollarQuote(sql, i); isDollar {
				if !ok {
					return 0, errUnterminated
				}
				i = end
			} else {
				i++
			}
		case isIdentStart(c):
			seen = true
			for i < len(sql) && isIdentCont(sql[i]) {
				i++
			}
		default:
			seen = true
			i++
		}
	}
	if seen {
		n++
	}
	return n, nil
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c >= 0x80
}

func isIdentCont(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9') || c == '$'
}

func skipBlockComment(s string, i int) (int, bool) {
	depth := 0
	for i < len(s) {
		switch {
		case s[i] == '/' && i+1 < len(s) && s[i+1] == '*':
			depth++
			i += 2
		case s[i] == '*' && i+1 < len(s) && s[i+1] == '/':
			depth--
			i += 2
			if depth == 0 {
				return i, true
			}
		default:
			i++
		}
	}
	return i, false
}

// skipQuoted handles '...' and "..." where a doubled delimiter is an escape.
func skipQuoted(s string, i int, q byte) (int, bool) {
	i++
	for i < len(s) {
		if s[i] == q {
			if i+1 < len(s) && s[i+1] == q {
				i += 2
				continue
			}
			return i + 1, true
		}
		i++
	}
	return i, false
}

func skipStandardString(s string, i int) (int, bool) { return skipQuoted(s, i, '\'') }

func skipEscapeString(s string, i int) (int, bool) {
	i++
	for i < len(s) {
		switch s[i] {
		case '\\':
			i += 2
		case '\'':
			if i+1 < len(s) && s[i+1] == '\'' {
				i += 2
				continue
			}
			return i + 1, true
		default:
			i++
		}
	}
	return i, false
}

// skipDollarQuote returns (end, terminated, isDollarQuote). It is only a
// dollar quote when '$' is followed by a valid tag and another '$'; "$1"
// style parameters are not.
func skipDollarQuote(s string, i int) (int, bool, bool) {
	j := i + 1
	if j < len(s) && s[j] != '$' {
		if !isIdentStart(s[j]) {
			return 0, false, false
		}
		for j < len(s) && (isIdentStart(s[j]) || (s[j] >= '0' && s[j] <= '9')) {
			j++
		}
	}
	if j >= len(s) || s[j] != '$' {
		return 0, false, false
	}
	tag := s[i : j+1]
	k := j + 1
	for k+len(tag) <= len(s) {
		if s[k:k+len(tag)] == tag {
			return k + len(tag), true, true
		}
		k++
	}
	return len(s), false, true
}
