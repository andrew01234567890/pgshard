package scatter

import "math/big"

// PostgreSQL's numeric is stored base 10000, and its division picks a result
// scale from the weights of its inputs in that base. These are its constants
// (src/backend/utils/adt/numeric.c).
const (
	decDigits            = 4
	numericMinSigDigits  = 16
	numericMinDisplayScl = 0
	numericMaxDisplayScl = 1000
)

// divideRounded divides num by den the way PostgreSQL divides two numerics:
// a result scale chosen by select_div_scale, and half-up rounding away from
// zero at that scale.
//
// avg() has to reproduce this rather than pick its own precision, because
// the answer is compared against what one PostgreSQL would have returned --
// and a numeric's text is its scale, so "3.5" and "3.50000" are different
// answers to the same question.
func divideRounded(num, den value) value {
	scale := selectDivScale(num, den)
	q := new(big.Rat).Quo(num.rat, den.rat)
	return value{rat: roundRat(q, scale), scale: scale}
}

// roundRat rounds to scale decimal places, half away from zero, which is
// what numeric division does.
func roundRat(q *big.Rat, scale int) *big.Rat {
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	n := new(big.Int).Mul(q.Num(), pow)
	d := q.Denom()
	quo, rem := new(big.Int).QuoRem(n, d, new(big.Int))
	rem.Abs(rem)
	rem.Lsh(rem, 1)
	if rem.Cmp(d) >= 0 {
		if q.Sign() < 0 {
			quo.Sub(quo, big.NewInt(1))
		} else {
			quo.Add(quo, big.NewInt(1))
		}
	}
	return new(big.Rat).SetFrac(quo, pow)
}

// selectDivScale is numeric.c's select_div_scale: enough scale to give at
// least numericMinSigDigits significant digits, and never less than either
// input's display scale.
func selectDivScale(num, den value) int {
	weight1, first1 := weightAndFirstDigit(num.rat)
	weight2, first2 := weightAndFirstDigit(den.rat)
	qweight := weight1 - weight2
	if first1 <= first2 {
		qweight--
	}
	rscale := numericMinSigDigits - qweight*decDigits
	rscale = max(rscale, num.scale)
	rscale = max(rscale, den.scale)
	rscale = max(rscale, numericMinDisplayScl)
	rscale = min(rscale, numericMaxDisplayScl)
	return rscale
}

// weightAndFirstDigit reports the base-10000 weight of a value's leading
// digit and that digit, the two things select_div_scale reads. A zero has
// weight 0 and first digit 0, as it does there.
func weightAndFirstDigit(r *big.Rat) (int, int) {
	if r == nil || r.Sign() == 0 {
		return 0, 0
	}
	abs := new(big.Rat).Abs(r)
	// Scale into an integer whose base-10000 length gives the weight. The
	// exponent only has to be large enough that a fractional value becomes
	// an integer; the weight is then read back off the shift.
	shift := 0
	for abs.Denom().Cmp(big.NewInt(1)) != 0 {
		abs.Mul(abs, big.NewRat(10000, 1))
		shift++
	}
	digits := abs.Num().String()
	// len(digits) digits base 10 is ceil(len/4) digits base 10000, and the
	// leading base-10000 digit is the first len mod 4 (or 4) of them.
	lead := len(digits) % decDigits
	if lead == 0 {
		lead = decDigits
	}
	first := 0
	for _, c := range digits[:lead] {
		first = first*10 + int(c-'0')
	}
	groups := (len(digits) + decDigits - 1) / decDigits
	return groups - 1 - shift, first
}
