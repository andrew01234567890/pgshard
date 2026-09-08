package router

import (
	"testing"
)

// BenchmarkBindReq measures the conversion every parameterised statement
// pays. Each parameter used to be its own allocation, so a wide bind cost
// one per column on the router's hottest path.
func BenchmarkBindReq(b *testing.B) {
	for _, n := range []int{1, 8, 40} {
		params := make([][]byte, n)
		for i := range params {
			params[i] = []byte("value")
		}
		params[0] = nil
		formats := make([]int16, n)
		b.Run(sizeName(n), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if r := bindReq("p", "s", formats, params, formats); r == nil {
					b.Fatal("nil")
				}
			}
		})
	}
}

func sizeName(n int) string {
	switch n {
	case 1:
		return "params/1"
	case 8:
		return "params/8"
	default:
		return "params/40"
	}
}

// The conversion has to say the same thing however it allocates: a nil
// parameter is SQL NULL and not an empty value, and every parameter keeps
// its own identity and position.
func TestBindReqKeepsEveryParameterDistinct(t *testing.T) {
	params := [][]byte{nil, []byte("a"), []byte("b"), {}}
	got := bindReq("p", "s", nil, params, nil).GetBind().GetParams()
	if len(got) != len(params) {
		t.Fatalf("got %d params, want %d", len(got), len(params))
	}
	if !got[0].GetNull() || got[0].GetData() != nil {
		t.Fatalf("param 0 = %+v, want NULL", got[0])
	}
	for i, want := range []string{"", "a", "b", ""} {
		if i == 0 {
			continue
		}
		if got[i].GetNull() {
			t.Fatalf("param %d is NULL; only a nil parameter is", i)
		}
		if string(got[i].GetData()) != want {
			t.Fatalf("param %d = %q, want %q", i, got[i].GetData(), want)
		}
	}
	// Distinct messages, not one shared: a marshaller that saw the same
	// pointer twice would send the same value twice.
	for i := range got {
		for j := i + 1; j < len(got); j++ {
			if got[i] == got[j] {
				t.Fatalf("params %d and %d are the same message", i, j)
			}
		}
	}
}
