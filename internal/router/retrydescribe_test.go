package router

import (
	"slices"
	"testing"
)

// describeRecorder records the answers that reach a client before any
// output does.
type describeRecorder struct {
	discardWriter
	seen []string
}

func (d *describeRecorder) ParameterDescription([]uint32) error {
	d.seen = append(d.seen, "ParameterDescription")
	return nil
}

func (d *describeRecorder) NoData() error {
	d.seen = append(d.seen, "NoData")
	return nil
}

func (d *describeRecorder) ParseComplete() error {
	d.seen = append(d.seen, "ParseComplete")
	return nil
}

func (d *describeRecorder) BindComplete() error {
	d.seen = append(d.seen, "BindComplete")
	return nil
}

func (d *describeRecorder) CloseComplete() error {
	d.seen = append(d.seen, "CloseComplete")
	return nil
}

// TestARetryDoesNotDescribeTwice: a statement retried after a cluster write
// pause or a shard failover re-sends the whole batch, so the shard answers
// its Describe again. The client has that answer already, and a second one
// is not a protocol PostgreSQL produces: a client reading a described
// statement takes the first and treats the repeat as a message it cannot
// place, which pgx answers by destroying the connection rather than
// retrying.
//
// The retry exists so a describing client sees latency instead of a 25006
// it could not have avoided, so the describe answers must not count as
// output either -- counting them switched the retry off for every client
// that describes before it executes.
func TestARetryDoesNotDescribeTwice(t *testing.T) {
	client := &describeRecorder{}
	cw := &countingWriter{w: client}

	attempt := func() {
		if err := cw.ParameterDescription([]uint32{20}); err != nil {
			t.Fatal(err)
		}
		if err := cw.NoData(); err != nil {
			t.Fatal(err)
		}
	}

	attempt()
	if cw.wrote {
		t.Fatal("a described statement has told the client nothing about its outcome, so it stays retryable")
	}
	cw.retrying()
	attempt()
	cw.retrying()
	attempt()

	want := []string{"ParameterDescription", "NoData"}
	if !slices.Equal(client.seen, want) {
		t.Fatalf("the client saw %v, want %v", client.seen, want)
	}
	if cw.wrote {
		t.Fatal("describe answers must not count as output")
	}

	// A batch describing two statements answers for both, and a retry of
	// it repeats both: the second statement's answer is not a duplicate of
	// the first's and has to arrive.
	client.seen = nil
	cw = &countingWriter{w: client}
	attempt()
	attempt()
	cw.retrying()
	attempt()
	attempt()
	want = []string{"ParameterDescription", "NoData", "ParameterDescription", "NoData"}
	if !slices.Equal(client.seen, want) {
		t.Fatalf("two described statements: the client saw %v, want %v", client.seen, want)
	}

	// A retry that gets further than the last one still says the rest.
	client.seen = nil
	cw = &countingWriter{w: client}
	attempt()
	cw.retrying()
	attempt()
	attempt()
	want = []string{"ParameterDescription", "NoData", "ParameterDescription", "NoData"}
	if !slices.Equal(client.seen, want) {
		t.Fatalf("a longer retry: the client saw %v, want %v", client.seen, want)
	}
}

// TestARetryDoesNotAnswerParseOrBindTwice: the batch pgx sends on its
// common path is Parse, Describe, Bind, Execute, Sync, and a failover
// retry re-runs all of it. ParseComplete and BindComplete answer the
// client's Parse and Bind, which it sent once, so a second one is a
// message PostgreSQL never sends. pgx does not resynchronise on it: it
// destroys the connection, and every later statement on that connection
// fails with "conn closed" -- 152 of them in one cutover -- while the
// router session that caused it is still healthy and has been told
// nothing. That is PGS-940, and it is why these are counted like the
// describe answers next to them rather than written on every attempt.
func TestARetryDoesNotAnswerParseOrBindTwice(t *testing.T) {
	client := &describeRecorder{}
	cw := &countingWriter{w: client}

	batch := func() {
		for _, write := range []func() error{
			cw.ParseComplete,
			func() error { return cw.ParameterDescription([]uint32{20}) },
			cw.NoData,
			cw.BindComplete,
		} {
			if err := write(); err != nil {
				t.Fatal(err)
			}
		}
	}

	batch()
	cw.retrying()
	batch()

	want := []string{"ParseComplete", "ParameterDescription", "NoData", "BindComplete"}
	if !slices.Equal(client.seen, want) {
		t.Fatalf("the client saw %v, want %v", client.seen, want)
	}
	if cw.wrote {
		t.Fatal("a bound statement has told the client nothing about its outcome, so it stays retryable")
	}

	// An attempt that gets further than the last one still says the rest,
	// and a Close answered once is not answered again either.
	client.seen = nil
	cw = &countingWriter{w: client}
	if err := cw.ParseComplete(); err != nil {
		t.Fatal(err)
	}
	cw.retrying()
	batch()
	if err := cw.CloseComplete(); err != nil {
		t.Fatal(err)
	}
	cw.retrying()
	batch()
	if err := cw.CloseComplete(); err != nil {
		t.Fatal(err)
	}
	want = []string{"ParseComplete", "ParameterDescription", "NoData", "BindComplete", "CloseComplete"}
	if !slices.Equal(client.seen, want) {
		t.Fatalf("a longer attempt: the client saw %v, want %v", client.seen, want)
	}
}
