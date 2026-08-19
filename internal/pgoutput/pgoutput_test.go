package pgoutput

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// readCapture splits a captured .bin (uint32 length-prefixed frames).
func readCapture(t testing.TB, path string) [][]byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var frames [][]byte
	for len(raw) > 0 {
		if len(raw) < 4 {
			t.Fatalf("%s: trailing bytes", path)
		}
		n := int(binary.BigEndian.Uint32(raw))
		raw = raw[4:]
		if len(raw) < n {
			t.Fatalf("%s: truncated frame", path)
		}
		frames = append(frames, raw[:n])
		raw = raw[n:]
	}
	return frames
}

func captures(t testing.TB) []string {
	t.Helper()
	bins, err := filepath.Glob(filepath.Join("testdata", "*", "*.bin"))
	if err != nil || len(bins) == 0 {
		t.Fatalf("no captures: %v", err)
	}
	return bins
}

func TestGoldens(t *testing.T) {
	majors := map[string]bool{}
	seen := map[string]bool{}
	for _, bin := range captures(t) {
		majors[filepath.Base(filepath.Dir(bin))] = true
		t.Run(filepath.Base(filepath.Dir(bin))+"/"+strings.TrimSuffix(filepath.Base(bin), ".bin"), func(t *testing.T) {
			want, err := os.ReadFile(strings.TrimSuffix(bin, ".bin") + ".golden")
			if err != nil {
				t.Fatal(err)
			}
			d := NewDecoder()
			var got strings.Builder
			for _, f := range readCapture(t, bin) {
				m, err := d.Decode(f)
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				seen[strings.Fields(Format(m))[0]] = true
				got.WriteString(Format(m))
				got.WriteByte('\n')
			}
			if got.String() != string(want) {
				t.Errorf("golden mismatch\n--- want\n%s\n--- got\n%s", want, got.String())
			}
		})
	}
	if !majors["pg18"] || !majors["pg19"] {
		t.Fatalf("captures must cover pg18 and pg19: %v", majors)
	}
	for _, kind := range []string{"Begin", "Commit", "Relation", "Type", "Insert", "Update", "Delete", "Truncate", "Message",
		"StreamStart", "StreamStop", "StreamCommit", "StreamAbort", "BeginPrepare", "Prepare", "CommitPrepared", "RollbackPrepared", "StreamPrepare"} {
		if !seen[kind] {
			t.Errorf("no capture exercises %s", kind)
		}
	}
}

func TestGoldenDetails(t *testing.T) {
	d := NewDecoder()
	var unchanged, keyUpdate, fullOld, streamedXid, abortLSN, truncates, nonTxMsg bool
	for _, f := range readCapture(t, filepath.Join("testdata", "pg18", "dml.bin")) {
		m, err := d.Decode(f)
		if err != nil {
			t.Fatal(err)
		}
		switch v := m.(type) {
		case *Update:
			for _, c := range v.New.Columns {
				if c.Kind == ColumnUnchanged {
					unchanged = true
				}
			}
			if v.Key != nil && len(v.Key.Columns) == 3 && v.Key.Columns[1].Kind == ColumnNull {
				keyUpdate = true
			}
			if v.Old != nil {
				fullOld = true
			}
		case *Truncate:
			truncates = len(v.RelationIDs) == 2 && v.RestartIdentity && !v.Cascade
		case *LogicalMessage:
			nonTxMsg = !v.Transactional && v.Prefix == "pgshard"
		case *Relation:
			if rel, ok := d.Relation(v.ID); !ok || rel != v || !rel.Columns[0].Key {
				t.Fatalf("relation cache: %v", rel)
			}
		}
	}
	for _, f := range readCapture(t, filepath.Join("testdata", "pg18", "streaming.bin")) {
		m, err := d.Decode(f)
		if err != nil {
			t.Fatal(err)
		}
		switch v := m.(type) {
		case *Insert:
			if v.Xid != 0 && d.InStream() {
				streamedXid = true
			}
		case *StreamAbort:
			abortLSN = v.AbortLSN != 0 && v.SubXid != v.Xid && !v.AbortTime.IsZero()
		}
	}
	if d.InStream() {
		t.Fatal("decoder still in stream after capture")
	}
	for name, ok := range map[string]bool{"unchanged toast": unchanged, "key-only old image": keyUpdate, "full old image": fullOld,
		"streamed xid": streamedXid, "parallel abort lsn": abortLSN, "truncate": truncates, "non-transactional message": nonTxMsg} {
		if !ok {
			t.Errorf("capture did not show %s", name)
		}
	}
}

func TestDecodeErrors(t *testing.T) {
	d := NewDecoder()
	cases := map[string][]byte{
		"empty":           nil,
		"unknown":         {'z'},
		"short begin":     {'B', 1, 2},
		"insert kind":     append([]byte{'I', 0, 0, 0, 1, 'X'}, 0, 0),
		"update kind":     append([]byte{'U', 0, 0, 0, 1, 'X'}, 0, 0),
		"delete kind":     append([]byte{'D', 0, 0, 0, 1, 'X'}, 0, 0),
		"tuple kind":      {'I', 0, 0, 0, 1, 'N', 0, 1, 'q'},
		"string unterm":   {'O', 0, 0, 0, 0, 0, 0, 0, 0, 'a', 'b'},
		"message length":  {'M', 0, 0, 0, 0, 0, 0, 0, 0, 0, 'p', 0, 0xff, 0xff, 0xff, 0xff},
		"tuple text len":  {'I', 0, 0, 0, 1, 'N', 0, 1, 't', 0, 0, 0, 9, 'a'},
		"relation column": {'R', 0, 0, 0, 1, 'n', 0, 't', 0, 'd', 0, 2, 1, 'a', 0},
	}
	for name, in := range cases {
		if m, err := d.Decode(in); err == nil {
			t.Errorf("%s: decoded %v", name, m)
		}
	}
	if d.InStream() {
		t.Fatal("errors changed stream state")
	}
}

func TestStreamStateAndXid(t *testing.T) {
	d := NewDecoder()
	// Outside a stream an insert has no xid prefix.
	ins := []byte{'I', 0, 0, 0, 7, 'N', 0, 1, 'n'}
	m, err := d.Decode(ins)
	if err != nil || m.(*Insert).Xid != 0 || m.(*Insert).RelationID != 7 {
		t.Fatalf("insert outside stream: %v %v", m, err)
	}
	if _, err := d.Decode([]byte{'S', 0, 0, 0, 42, 1}); err != nil {
		t.Fatal(err)
	}
	if !d.InStream() {
		t.Fatal("not in stream")
	}
	m, err = d.Decode(append([]byte{'I', 0, 0, 0, 42}, ins[1:]...))
	if err != nil || m.(*Insert).Xid != 42 || m.(*Insert).RelationID != 7 {
		t.Fatalf("insert inside stream: %v %v", m, err)
	}
	if _, err := d.Decode([]byte{'E'}); err != nil || d.InStream() {
		t.Fatal("stream stop")
	}
	// A short stream start must not flip the state.
	if _, err := d.Decode([]byte{'S', 0, 0}); err == nil || d.InStream() {
		t.Fatal("short stream start")
	}
	// Stream abort without parallel info (protocol < 4 shape).
	m, err = d.Decode([]byte{'A', 0, 0, 0, 1, 0, 0, 0, 2})
	if err != nil || m.(*StreamAbort).AbortLSN != 0 || m.(*StreamAbort).SubXid != 2 {
		t.Fatalf("abort: %v %v", m, err)
	}
}

func TestFormatAndTime(t *testing.T) {
	if got := PGTime(0); !got.Equal(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatal(got)
	}
	tup := Tuple{Columns: []TupleColumn{{Kind: ColumnBinary, Data: []byte{1, 2}}, {Kind: ColumnText, Data: bytes.Repeat([]byte("a"), 50)}}}
	s := Format(&Insert{RelationID: 1, New: tup})
	if !strings.Contains(s, "b:0102") || !strings.Contains(s, "...(50 bytes)") {
		t.Fatal(s)
	}
	if Format(&StreamStop{}) != "StreamStop" {
		t.Fatal("stream stop format")
	}
	if !errors.Is(func() error { _, err := NewDecoder().Decode([]byte{'B'}); return err }(), ErrShort) {
		t.Fatal("short begin should be ErrShort")
	}
}

func FuzzDecode(f *testing.F) {
	for _, bin := range captures(f) {
		for _, fr := range readCapture(f, bin) {
			f.Add(fr)
		}
	}
	f.Add([]byte{'S', 0, 0, 0, 1, 1})
	f.Fuzz(func(_ *testing.T, data []byte) {
		d := NewDecoder()
		_, _ = d.Decode([]byte{'S', 0, 0, 0, 1, 1})
		_, _ = d.Decode(data)
		_, _ = NewDecoder().Decode(data)
	})
}
