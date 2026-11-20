// The shared test vectors (spec/vectors/frames.json) checked against the Go codecs. Every
// implementation runs the same file, so the encodings cannot drift apart:
//
//   - "link" and "mux": encoding the listed fields must give exactly the listed hex, and decoding
//     the hex then encoding it again must give it back.
//   - "invalid": decoding the hex must fail with the listed error ("corrupt" or "too large").
//
// Running `go test ./protocol -run Vectors -update` rewrites the hex of the valid vectors from their
// fields, for use after a deliberate change to PROTOCOL.md.
package protocol

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "rewrite the hex of spec/vectors/frames.json from its fields")

const vectorsPath = "../../spec/vectors/frames.json"

type vector struct {
	Name   string         `json:"name"`
	Hex    string         `json:"hex"`
	Type   string         `json:"type,omitempty"`
	Fields map[string]any `json:"fields,omitempty"`
	Error  string         `json:"error,omitempty"`
}

type vectorFile struct {
	Description string   `json:"description"`
	Link        []vector `json:"link"`
	Mux         []vector `json:"mux"`
	Invalid     []vector `json:"invalid"`
}

func str(f map[string]any, k string) string { s, _ := f[k].(string); return s }
func num(f map[string]any, k string) int    { n, _ := f[k].(float64); return int(n) }
func u64(t *testing.T, f map[string]any, k string) uint64 {
	v, err := strconv.ParseUint(str(f, k), 10, 64)
	if err != nil {
		t.Fatalf("field %s: %v", k, err)
	}
	return v
}
func unhex(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

// encodeLink builds the frame a link vector describes.
func encodeLink(t *testing.T, v vector) []byte {
	f := v.Fields
	ms := func(k string) time.Duration { return time.Duration(num(f, k)) * time.Millisecond }
	switch v.Type {
	case "HELLO":
		h := HelloFrame{Version: uint16(num(f, "version")), Link: uint8(num(f, "link")), Impl: str(f, "impl")}
		copy(h.Session[:], unhex(t, str(f, "session")))
		if str(f, "mode") == "join" {
			h.Mode = ModeJoin
		}
		return EncodeHello(h)
	case "WELCOME":
		created, _ := f["created"].(bool)
		return EncodeWelcome(WelcomeFrame{Version: uint16(num(f, "version")), Created: created, Impl: str(f, "impl"),
			Limits: Limits{MaxPayload: num(f, "maxPayload"), Window: num(f, "window"), StreamWindow: num(f, "streamWindow"),
				MaxStreams: num(f, "maxStreams"), MaxLinks: num(f, "maxLinks"), Ping: ms("pingMs"),
				ClientDead: ms("clientDeadMs"), ServerDead: ms("serverDeadMs"), Progress: ms("progressMs")}})
	case "REJECT":
		return EncodeReject(byte(num(f, "code")), str(f, "reason"))
	case "DATA":
		return EncodeData(u64(t, f, "offset"), unhex(t, str(f, "data")))
	case "ACK":
		return EncodeAck(u64(t, f, "value"))
	case "PING":
		return EncodePing(u64(t, f, "value"))
	case "PONG":
		return EncodePong(u64(t, f, "value"))
	case "GOODBYE":
		return EncodeGoodbye(str(f, "reason"))
	case "UNKNOWN":
		p := unhex(t, str(f, "payload"))
		return append(frame(byte(num(f, "code")), len(p)), p...)
	}
	t.Fatalf("unknown vector type %q", v.Type)
	return nil
}

// reencodeLink decodes a frame and encodes what it decoded.
func reencodeLink(typ byte, p []byte) ([]byte, error) {
	switch typ {
	case Hello:
		h, err := DecodeHello(p)
		return EncodeHello(h), err
	case Welcome:
		w, err := DecodeWelcome(p)
		return EncodeWelcome(w), err
	case Reject:
		r, err := DecodeReject(p)
		return EncodeReject(r.Code, r.Reason), err
	case Data:
		off, data, err := DecodeData(p)
		return EncodeData(off, data), err
	case Ack, Ping, Pong:
		v, err := DecodeU64(p)
		return encodeU64(typ, v), err
	case Goodbye:
		r, err := DecodeReason(p)
		return EncodeGoodbye(r), err
	}
	return append(frame(typ, len(p)), p...), nil
}

var muxTypes = map[string]byte{"SYN": MuxSyn, "DATA": MuxData, "FIN": MuxFin, "RST": MuxRst, "WINDOW": MuxWindow, "UNKNOWN": 0x7f}

func encodeMuxVector(t *testing.T, v vector) []byte {
	typ, ok := muxTypes[v.Type]
	if !ok {
		t.Fatalf("unknown mux vector type %q", v.Type)
	}
	return EncodeMux(typ, uint32(num(v.Fields, "stream")), unhex(t, str(v.Fields, "body")))
}

func TestVectors(t *testing.T) {
	data, err := os.ReadFile(vectorsPath)
	if err != nil {
		t.Fatal(err)
	}
	var file vectorFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	if *update {
		for i := range file.Link {
			file.Link[i].Hex = hex.EncodeToString(encodeLink(t, file.Link[i]))
		}
		for i := range file.Mux {
			file.Mux[i].Hex = hex.EncodeToString(encodeMuxVector(t, file.Mux[i]))
		}
		out, err := json.MarshalIndent(file, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(vectorsPath, append(out, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, v := range file.Link {
		want := unhex(t, v.Hex)
		if got := encodeLink(t, v); !bytes.Equal(got, want) {
			t.Errorf("%s: encoded %x, want %s", v.Name, got, v.Hex)
		}
		typ, p, err := ParseFrame(want, DefaultLimits().MaxPayload)
		if err != nil {
			t.Errorf("%s: read: %v", v.Name, err)
			continue
		}
		if got, err := reencodeLink(typ, p); err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s: round trip gave %x, %v", v.Name, got, err)
		}
	}
	for _, v := range file.Mux {
		want := unhex(t, v.Hex)
		if got := encodeMuxVector(t, v); !bytes.Equal(got, want) {
			t.Errorf("%s: encoded %x, want %s", v.Name, got, v.Hex)
		}
		m, n, err := ParseMux(want)
		if err != nil || n != len(want) || !bytes.Equal(EncodeMux(m.Type, m.Stream, m.Body), want) {
			t.Errorf("%s: parse gave %+v, %d, %v", v.Name, m, n, err)
		}
	}
	for _, v := range file.Invalid {
		if err := decodeInvalid(t, v); err == nil {
			t.Errorf("%s: decoded without error", v.Name)
		} else if (v.Error == "corrupt") != errors.Is(err, ErrCorrupt) || (v.Error == "too large") != errors.Is(err, ErrTooLarge) {
			t.Errorf("%s: got %v, want %s", v.Name, err, v.Error)
		}
	}
}

// decodeInvalid decodes an invalid vector: as a mux frame when its type says "mux", else as a link
// frame read with the smallest maxPayload a server may announce.
func decodeInvalid(t *testing.T, v vector) error {
	b := unhex(t, v.Hex)
	if v.Type == "mux" {
		_, _, err := ParseMux(b)
		return err
	}
	typ, p, err := ParseFrame(b, DataOverhead+MaxData)
	if err != nil {
		return err
	}
	if _, err := reencodeLink(typ, p); err != nil {
		return err
	}
	return fmt.Errorf("type 0x%02x decoded", typ)
}
