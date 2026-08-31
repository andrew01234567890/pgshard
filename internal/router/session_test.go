package router

import (
	"slices"
	"testing"
)

// recordingWriter counts the ParameterStatus messages that reach a client.
type recordingWriter struct {
	discardWriter
	seen [][2]string
}

func (r *recordingWriter) ParameterStatus(name, value string) error {
	r.seen = append(r.seen, [2]string{name, value})
	return nil
}

// TestOnlyAChangedParameterReachesTheClient: the router replays session
// state onto every backend it moves a session to, so a backend reporting
// back what the session already asked for is routine and is not news. Only
// a value the client does not already hold is forwarded.
func TestOnlyAChangedParameterReachesTheClient(t *testing.T) {
	e := &Executor{}
	w := &recordingWriter{}
	for _, p := range [][2]string{
		{"TimeZone", "UTC"},
		{"TimeZone", "UTC"},
		{"TimeZone", "Asia/Tokyo"},
		{"TimeZone", "Asia/Tokyo"},
		{"DateStyle", "ISO, MDY"},
		{"TimeZone", "UTC"},
		{"", "ignored"},
	} {
		if err := e.reportParameter(w, p[0], p[1]); err != nil {
			t.Fatal(err)
		}
	}
	want := [][2]string{{"TimeZone", "UTC"}, {"TimeZone", "Asia/Tokyo"}, {"DateStyle", "ISO, MDY"}, {"TimeZone", "UTC"}}
	if !slices.Equal(w.seen, want) {
		t.Fatalf("forwarded %v, want %v", w.seen, want)
	}
}
