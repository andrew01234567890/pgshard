package scatter

import (
	"encoding/binary"
	"math"
	"math/big"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

// average folds the sum and the count each shard returned and divides once,
// over the totals.
//
// An average of averages is not the average, so the shards cannot answer
// avg() on their own; the planner rewrote it into sum(x) plus a count(x)
// the client never sees.
type average struct {
	sum    valueAccumulator
	count  valueAccumulator
	sumAt  int
	cntAt  int
	sumOID uint32
	// cntFormat is the count column's own format. Mixed client result
	// formats pad the hidden column with the client's LAST format, which
	// need not be the format of the avg column beside it.
	cntFormat int16
	// numeric is false for a float average, which PostgreSQL answers in
	// float8 and divides as floats.
	numeric bool
	format  int16
}

func newAverage(a plan.Agg, cols []Column) (accumulator, error) {
	if a.Count < 0 {
		return nil, pgwire.Errorf(pgwire.CodeInternalError, "router: avg() planned without a count column")
	}
	col := cols[a.Col]
	fam := familyOf(col.TypeOID)
	if fam != famInt && fam != famNumeric && fam != famFloat {
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "multi-shard avg() over a column of type oid %d is not available yet", col.TypeOID)
		err.Hint = "average integer, numeric or double precision columns, or filter on one shard key value"
		return nil, err
	}
	// PostgreSQL's avg(real) accumulates in double precision while its
	// sum(real) accumulates in real, so a sum taken per shard has already
	// been rounded to real and dividing it would not be the same number.
	// The other types have no such split.
	if col.TypeOID == oidFloat4 {
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "multi-shard avg() over a real column is not available yet")
		err.Hint = "cast to double precision (avg(x::float8)), or filter on one shard key value"
		return nil, err
	}
	sum, err := newValueAccumulator(plan.AggSum, col)
	if err != nil {
		return nil, err
	}
	count, err := newValueAccumulator(plan.AggCount, cols[a.Count])
	if err != nil {
		return nil, err
	}
	return &average{sum: sum, count: count, sumAt: a.Col, cntAt: a.Count, sumOID: col.TypeOID,
		cntFormat: cols[a.Count].Format, numeric: fam != famFloat, format: col.Format}, nil
}

func (a *average) add(row [][]byte) error {
	if err := a.sum.add(row[a.sumAt]); err != nil {
		return err
	}
	return a.count.add(row[a.cntAt])
}

func (a *average) result() ([]byte, error) {
	sum, err := a.sum.result()
	if err != nil {
		return nil, err
	}
	cnt, err := a.count.result()
	if err != nil {
		return nil, err
	}
	// No rows anywhere: PostgreSQL's avg is NULL, and so is its sum.
	if sum == nil || cnt == nil {
		return nil, nil
	}
	n, err := decoderFor(famInt, oidInt8, a.cntFormat)(cnt)
	if err != nil {
		return nil, err
	}
	if n.i == 0 {
		return nil, nil
	}
	if !a.numeric {
		return a.divideFloat(sum, n.i)
	}
	return a.divideNumeric(sum, n.i)
}

func (a *average) divideFloat(sum []byte, count int64) ([]byte, error) {
	v, err := decoderFor(famFloat, oidFloat8, a.format)(sum)
	if err != nil {
		return nil, err
	}
	q := v.f / float64(count)
	switch v.class {
	case classNaN:
		q = math.NaN()
	case classPosInf:
		q = math.Inf(1)
	case classNegInf:
		q = math.Inf(-1)
	}
	if a.format == FormatBinary {
		var out [8]byte
		binary.BigEndian.PutUint64(out[:], math.Float64bits(q))
		return out[:], nil
	}
	return []byte(FormatFloat(q)), nil
}

func (a *average) divideNumeric(sum []byte, count int64) ([]byte, error) {
	// sum(int2) and sum(int4) come back as int8, sum(int8) and sum(numeric)
	// as numeric, so the numerator is read by the shard column's own type
	// rather than by the client's.
	var num value
	if familyOf(a.sumOID) == famInt {
		x, err := decoderFor(famInt, oidInt8, a.format)(sum)
		if err != nil {
			return nil, err
		}
		num = value{rat: new(big.Rat).SetInt(big.NewInt(x.i))}
	} else {
		n, err := decoderFor(famNumeric, oidNumeric, a.format)(sum)
		if err != nil {
			return nil, err
		}
		num = n
	}
	switch num.class {
	case classNaN, classPosInf, classNegInf:
		return a.encodeNumeric(value{class: num.class})
	}
	den := value{rat: new(big.Rat).SetInt(big.NewInt(count))}
	q := divideRounded(num, den)
	return a.encodeNumeric(q)
}

func (a *average) encodeNumeric(v value) ([]byte, error) {
	if a.format == FormatBinary {
		return encodeNumericBinary(v)
	}
	return []byte(formatNumeric(v)), nil
}
