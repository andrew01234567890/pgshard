package scatter

import (
	"container/heap"
	"encoding/binary"
	"math"
	"math/big"

	"github.com/andrew01234567890/pgshard/internal/pgwire"
	"github.com/andrew01234567890/pgshard/internal/router/plan"
)

// Source is one shard's row stream. Next returns the next row, ok=false at
// the end of the stream, or an error.
type Source interface {
	Next() (row [][]byte, ok bool, err error)
}

// Merge combines the shard streams as spec says and passes every result
// row (hidden columns stripped) to emit; it returns the number of rows
// emitted. cols describes the columns of the shard rows.
func Merge(spec *plan.Merge, cols []Column, sources []Source, emit func([][]byte) error) (int64, error) {
	out := &limiter{spec: spec, emit: emit, hidden: spec.Hidden, width: len(cols)}
	var err error
	switch {
	case len(spec.Aggregates) > 0:
		err = combineAggregates(spec.Aggregates, cols, sources, out)
	case len(spec.OrderBy) > 0:
		err = mergeOrdered(spec.OrderBy, cols, spec.Hidden, sources, out)
	default:
		err = concatenate(sources, out)
	}
	return out.emitted, err
}

// limiter applies OFFSET/LIMIT and strips hidden columns. Done reports that
// no further rows are wanted.
type limiter struct {
	spec    *plan.Merge
	emit    func([][]byte) error
	hidden  int
	width   int
	seen    int64
	emitted int64
}

func (l *limiter) done() bool { return l.spec.Limit >= 0 && l.emitted >= l.spec.Limit }

func (l *limiter) push(row [][]byte) error {
	if len(row) != l.width {
		return pgwire.Errorf(pgwire.CodeInternalError, "router: shard row has %d columns, expected %d", len(row), l.width)
	}
	l.seen++
	if l.spec.Offset > 0 && l.seen <= l.spec.Offset {
		return nil
	}
	if l.done() {
		return nil
	}
	l.emitted++
	return l.emit(row[:len(row)-l.hidden])
}

func concatenate(sources []Source, out *limiter) error {
	for _, s := range sources {
		for !out.done() {
			row, ok, err := s.Next()
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			if err := out.push(row); err != nil {
				return err
			}
		}
	}
	return nil
}

type headRow struct {
	row [][]byte
	src int
}

type rowHeap struct {
	rows []headRow
	cmp  *RowComparator
	err  error
}

func (h *rowHeap) Len() int { return len(h.rows) }
func (h *rowHeap) Less(i, j int) bool {
	c, err := h.cmp.Compare(h.rows[i].row, h.rows[j].row)
	if err != nil && h.err == nil {
		h.err = err
	}
	// Ties keep shard order so the merge is deterministic.
	if c == 0 {
		return h.rows[i].src < h.rows[j].src
	}
	return c < 0
}
func (h *rowHeap) Swap(i, j int)  { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }
func (h *rowHeap) Push(x any)     { h.rows = append(h.rows, x.(headRow)) }
func (h *rowHeap) Pop() any       { n := len(h.rows); x := h.rows[n-1]; h.rows = h.rows[:n-1]; return x }
func (h *rowHeap) errored() error { return h.err }

// mergeOrdered streams the k-way merge: every shard already returned its
// rows in the requested order.
func mergeOrdered(keys []plan.SortKey, cols []Column, hidden int, sources []Source, out *limiter) error {
	cmp, err := NewRowComparator(keys, cols, hidden)
	if err != nil {
		return err
	}
	h := &rowHeap{cmp: cmp}
	for i, s := range sources {
		row, ok, err := s.Next()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(h, headRow{row: row, src: i})
		}
		if err := h.errored(); err != nil {
			return err
		}
	}
	for h.Len() > 0 && !out.done() {
		top := heap.Pop(h).(headRow)
		if err := h.errored(); err != nil {
			return err
		}
		if err := out.push(top.row); err != nil {
			return err
		}
		row, ok, err := sources[top.src].Next()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(h, headRow{row: row, src: top.src})
			if err := h.errored(); err != nil {
				return err
			}
		}
	}
	return nil
}

// combineAggregates folds the one row every shard returns into one row.
func combineAggregates(aggs []plan.AggFunc, cols []Column, sources []Source, out *limiter) error {
	if len(aggs) != len(cols) {
		return pgwire.Errorf(pgwire.CodeInternalError, "router: %d aggregates planned but the shard row has %d columns", len(aggs), len(cols))
	}
	accs := make([]accumulator, len(aggs))
	for i, a := range aggs {
		acc, err := newAccumulator(a, cols[i])
		if err != nil {
			return err
		}
		accs[i] = acc
	}
	for _, s := range sources {
		row, ok, err := s.Next()
		if err != nil {
			return err
		}
		if !ok {
			return pgwire.Errorf(pgwire.CodeInternalError, "router: a shard returned no row for an aggregate query")
		}
		if len(row) != len(accs) {
			return pgwire.Errorf(pgwire.CodeInternalError, "router: shard row has %d columns, expected %d", len(row), len(accs))
		}
		for i, v := range row {
			if err := accs[i].add(v); err != nil {
				return err
			}
		}
		if _, ok, err := s.Next(); err != nil {
			return err
		} else if ok {
			return pgwire.Errorf(pgwire.CodeInternalError, "router: a shard returned more than one row for an aggregate query")
		}
	}
	row := make([][]byte, len(accs))
	for i, acc := range accs {
		v, err := acc.result()
		if err != nil {
			return err
		}
		row[i] = v
	}
	return out.push(row)
}

// accumulator folds per-shard aggregate values; NULL inputs are skipped and
// the result is NULL when every input was.
type accumulator interface {
	add(v []byte) error
	result() ([]byte, error)
}

func newAccumulator(fn plan.AggFunc, col Column) (accumulator, error) {
	fam := familyOf(col.TypeOID)
	switch fn {
	case plan.AggCount:
		return &intSum{format: col.Format}, nil
	case plan.AggSum:
		switch fam {
		case famInt:
			return &intSum{format: col.Format}, nil
		case famNumeric:
			return &numericSum{format: col.Format}, nil
		case famFloat:
			return &floatSum{format: col.Format, oid: col.TypeOID}, nil
		}
		err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "multi-shard sum() over a column of type oid %d is not available yet", col.TypeOID)
		err.Hint = "sum integer, numeric or float columns, or filter on one shard key value"
		return nil, err
	case plan.AggMin, plan.AggMax:
		if fam == famTextCollated {
			err := pgwire.Errorf(pgwire.CodeFeatureNotSupported, "multi-shard min()/max() over a text column is not available yet")
			err.Hint = "the router cannot apply the column's collation; filter on one shard key value"
			return nil, err
		}
		cmp, err := ComparatorFor(col.TypeOID, col.Format, true)
		if err != nil {
			return nil, err
		}
		return &extreme{cmp: cmp, max: fn == plan.AggMax}, nil
	}
	return nil, pgwire.Errorf(pgwire.CodeInternalError, "router: unknown aggregate %d", fn)
}

// intSum adds int8 results (count, sum of int2/int4); a combined value past
// int8 is the same 22003 PostgreSQL raises.
type intSum struct {
	format int16
	sum    big.Int
	set    bool
}

func (a *intSum) add(v []byte) error {
	if v == nil {
		return nil
	}
	x, err := decoderFor(famInt, oidInt8, a.format)(v)
	if err != nil {
		return err
	}
	a.sum.Add(&a.sum, big.NewInt(x.i))
	a.set = true
	return nil
}

func (a *intSum) result() ([]byte, error) {
	if !a.set {
		return nil, nil
	}
	if !a.sum.IsInt64() {
		return nil, pgwire.Errorf("22003", "bigint out of range")
	}
	if a.format == FormatBinary {
		var out [8]byte
		binary.BigEndian.PutUint64(out[:], uint64(a.sum.Int64()))
		return out[:], nil
	}
	return []byte(a.sum.String()), nil
}

type numericSum struct {
	format int16
	sum    *big.Rat
	scale  int
	set    bool
	nan    bool
	pinf   bool
	ninf   bool
}

func (a *numericSum) add(v []byte) error {
	if v == nil {
		return nil
	}
	n, err := decoderFor(famNumeric, oidNumeric, a.format)(v)
	if err != nil {
		return err
	}
	a.set = true
	switch n.class {
	case classNaN:
		a.nan = true
	case classPosInf:
		a.pinf = true
	case classNegInf:
		a.ninf = true
	default:
		if a.sum == nil {
			a.sum = new(big.Rat)
		}
		a.sum.Add(a.sum, n.rat)
		if n.scale > a.scale {
			a.scale = n.scale
		}
	}
	return nil
}

func (a *numericSum) result() ([]byte, error) {
	if !a.set {
		return nil, nil
	}
	v := value{rat: a.sum, scale: a.scale}
	switch {
	case a.nan || (a.pinf && a.ninf):
		v = value{class: classNaN}
	case a.pinf:
		v = value{class: classPosInf}
	case a.ninf:
		v = value{class: classNegInf}
	case v.rat == nil:
		v.rat = new(big.Rat)
	}
	if a.format == FormatBinary {
		return encodeNumericBinary(v)
	}
	return []byte(formatNumeric(v)), nil
}

type floatSum struct {
	format int16
	oid    uint32
	sum    float64
	set    bool
}

func (a *floatSum) add(v []byte) error {
	if v == nil {
		return nil
	}
	x, err := decoderFor(famFloat, a.oid, a.format)(v)
	if err != nil {
		return err
	}
	a.set = true
	switch x.class {
	case classNaN:
		a.sum = math.NaN()
	case classPosInf:
		a.sum += math.Inf(1)
	case classNegInf:
		a.sum += math.Inf(-1)
	default:
		a.sum += x.f
	}
	if a.oid == oidFloat4 {
		a.sum = float64(float32(a.sum))
	}
	return nil
}

func (a *floatSum) result() ([]byte, error) {
	if !a.set {
		return nil, nil
	}
	if a.format == FormatBinary {
		if a.oid == oidFloat4 {
			var out [4]byte
			binary.BigEndian.PutUint32(out[:], math.Float32bits(float32(a.sum)))
			return out[:], nil
		}
		var out [8]byte
		binary.BigEndian.PutUint64(out[:], math.Float64bits(a.sum))
		return out[:], nil
	}
	if a.oid == oidFloat4 {
		return []byte(formatFloatBits(a.sum, 32)), nil
	}
	return []byte(FormatFloat(a.sum)), nil
}

type extreme struct {
	cmp  Comparator
	max  bool
	best []byte
	set  bool
}

func (a *extreme) add(v []byte) error {
	if v == nil {
		return nil
	}
	if !a.set {
		a.best, a.set = v, true
		return nil
	}
	c, err := a.cmp(v, a.best)
	if err != nil {
		return err
	}
	if a.max && c > 0 || !a.max && c < 0 {
		a.best = v
	}
	return nil
}

func (a *extreme) result() ([]byte, error) {
	if !a.set {
		return nil, nil
	}
	return a.best, nil
}
