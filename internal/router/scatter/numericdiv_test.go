package scatter

import (
	"math/big"
	"testing"
)

func ratOf(t *testing.T, s string) *big.Rat {
	t.Helper()
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		t.Fatalf("bad rational %q", s)
	}
	return r
}

// TestAverageMatchesPostgresScale: a numeric's text IS its scale, so "2.5"
// and "2.5000000000000000" are different answers to the same question and
// the differential suite compares them as text.
//
// Each expectation is derived from select_div_scale (numeric.c): rscale =
// 16 - qweight*4, floored by each input's display scale, where qweight is
// the difference of the inputs' base-10000 weights, minus one when the
// leading base-10000 digit of the numerator is not greater than the
// denominator's. The corpus in test/e2e/router/scatter_test.go checks the
// same shapes against a real PostgreSQL, which is what settles them.
func TestAverageMatchesPostgresScale(t *testing.T) {
	for _, c := range []struct {
		name     string
		sum      string
		sumScale int
		count    int64
		want     string
		why      string
	}{
		// weights 0 and 0, leading digits 10 and 4, so qweight stays 0.
		{"integers", "10", 0, 4, "2.5000000000000000", "rscale 16"},
		{"exact", "8", 0, 4, "2.0000000000000000", "an exact quotient still carries the scale"},
		// 1000000 is base-10000 digits [100, 0], weight 1; 4 is weight 0,
		// and 100 > 4, so qweight is 1 and four decimals are given back.
		{"large quotient", "1000000", 0, 4, "250000.000000000000", "rscale 16-4"},
		// Leading digit 1 is not greater than 8, so qweight drops to -1.
		{"small quotient", "1", 0, 8, "0.12500000000000000000", "rscale 16+4"},
		{"negative", "-10", 0, 4, "-2.5000000000000000", "the sign does not change the scale"},
		// The numerator's display scale is a floor, and here it is below
		// what the significant-digit rule already gives.
		{"numerator scale below the rule", "1.500", 3, 3, "0.50000000000000000000", "rscale 16+4 beats dscale 3"},
	} {
		t.Run(c.name, func(t *testing.T) {
			num := value{rat: ratOf(t, c.sum), scale: c.sumScale}
			den := value{rat: new(big.Rat).SetInt(big.NewInt(c.count))}
			if got := formatNumeric(divideRounded(num, den)); got != c.want {
				t.Errorf("sum %s / count %d = %s, want %s (%s)", c.sum, c.count, got, c.want, c.why)
			}
		})
	}
}

// A repeating quotient is where the rounding rule shows: half away from
// zero, not to even.
func TestAverageRoundsARepeatingQuotient(t *testing.T) {
	q := divideRounded(value{rat: ratOf(t, "1")}, value{rat: ratOf(t, "3")})
	if got := formatNumeric(q); got != "0.33333333333333333333" {
		t.Errorf("1/3 = %s", got)
	}
	q = divideRounded(value{rat: ratOf(t, "2")}, value{rat: ratOf(t, "3")})
	if got := formatNumeric(q); got != "0.66666666666666666667" {
		t.Errorf("2/3 = %s, want the last digit rounded up", got)
	}
}

// TestAverageRoundsAnExactHalfAwayFromZero: the rounding rule is only
// visible where the chosen scale cuts off a 5 with nothing after it. Half
// to even -- Go's default for many things, and IEEE's -- would answer
// 50000000000000000 here, and PostgreSQL's round_var answers ...001.
//
// Reaching it needs a quotient big enough that select_div_scale gives scale
// 0: 1e17+1 over 2 is 50000000000000000.5, and the numerator's leading
// base-10000 digit (10) is greater than the denominator's (2), so qweight
// is 4 and rscale is 16-16.
func TestAverageRoundsAnExactHalfAwayFromZero(t *testing.T) {
	for _, c := range []struct{ sum, want string }{
		{"100000000000000001", "50000000000000001"},
		{"-100000000000000001", "-50000000000000001"},
	} {
		q := divideRounded(value{rat: ratOf(t, c.sum)}, value{rat: ratOf(t, "2")})
		if q.scale != 0 {
			t.Fatalf("%s/2 got scale %d, want 0 -- the fixture no longer exercises the rounding", c.sum, q.scale)
		}
		if got := formatNumeric(q); got != c.want {
			t.Errorf("%s/2 = %s, want %s", c.sum, got, c.want)
		}
	}
}

// TestWeightAndFirstDigit pins the base-10000 reading select_div_scale
// depends on. PostgreSQL stores a numeric as digits base 10000 and the
// weight is the power of 10000 the leading digit carries, so 12345 is
// 1|2345 at weight 1 and 0.5 is 5000 at weight -1.
func TestWeightAndFirstDigit(t *testing.T) {
	for _, c := range []struct {
		in     string
		weight int
		first  int
	}{
		{"0", 0, 0},
		{"1", 0, 1},
		{"9999", 0, 9999},
		{"10000", 1, 1},
		{"12345", 1, 1},
		{"1000000", 1, 100},
		{"0.5", -1, 5000},
		{"0.0001", -1, 1},
		{"0.00001", -2, 1000},
		{"-12345", 1, 1},
	} {
		w, f := weightAndFirstDigit(ratOf(t, c.in))
		if w != c.weight || f != c.first {
			t.Errorf("%s: weight %d first %d, want %d %d", c.in, w, f, c.weight, c.first)
		}
	}
}
