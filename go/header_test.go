package multipath

import (
	"bytes"
	"strings"
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	cases := []Header{
		{Service: "anthropic"},
		{Service: "greeter", Ticket: []byte("opaque-cap")},
		{Service: "echo", Ticket: []byte("t")},
	}
	for _, want := range cases {
		var buf bytes.Buffer
		if err := WriteHeader(&buf, want); err != nil {
			t.Fatalf("write %+v: %v", want, err)
		}
		// A payload byte follows the header; ReadHeader must stop exactly at the boundary.
		buf.WriteByte('X')
		got, err := ReadHeader(&buf)
		if err != nil {
			t.Fatalf("read %+v: %v", want, err)
		}
		if got.Service != want.Service || !bytes.Equal(got.Ticket, want.Ticket) {
			t.Fatalf("round trip: got %+v want %+v", got, want)
		}
		if b, _ := buf.ReadByte(); b != 'X' {
			t.Fatalf("ReadHeader consumed into the payload")
		}
	}
}

func TestHeaderRejectsOversizeField(t *testing.T) {
	if err := WriteHeader(&bytes.Buffer{}, Header{Service: strings.Repeat("a", maxHeaderField+1)}); err == nil {
		t.Fatal("expected an error writing an oversize field")
	}
}
