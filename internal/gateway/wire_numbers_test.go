package gateway

import (
	"io"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

func TestWireCaptureFramingAndAssociation(t *testing.T) {
	id, err := jsonrpc.MakeID(float64(7))
	if err != nil {
		t.Fatal(err)
	}
	const result = `{"structuredContent":{"id":9007199254740993}}`
	const response = `{"jsonrpc":"2.0","id":7,"result":` + result + `}`
	for _, tc := range []struct {
		name, media, data string
		eof, valid        bool
	}{
		{"json", "application/json", response, true, true},
		{"wrong_id", "application/json", `{"jsonrpc":"2.0","id":8,"result":{}}`, true, false},
		{"error", "application/json", `{"jsonrpc":"2.0","id":7,"error":{"code":-32603,"message":"secret"}}`, true, false},
		{"truncated", "application/json", response[:len(response)-1], false, false},
		{"sse", "text/event-stream", "data: " + response + "\n\n", false, true},
		{"crlf_comments", "text/event-stream", ": keepalive\r\nignored: value\r\nevent: message\r\ndata: " + response + "\r\n\r\n", false, true},
		{"multiline", "text/event-stream", "data: {\"jsonrpc\":\"2.0\",\n" + "data: \"id\":7,\"result\":" + result + "}\n\n", false, true},
		{"notification_first", "text/event-stream", "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{}}\n\ndata: " + response + "\n\n", false, true},
		{"eof_flush", "text/event-stream", "data: " + response, true, true},
		{"partial_line", "text/event-stream", "data: " + response, false, false},
		{"foreign_event", "text/event-stream", "event: other\ndata: " + response + "\n\n", true, false},
		{"bad_media", "text/plain", response, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture := &wireCapture{attached: true, id: id, media: tc.media, data: []byte(tc.data), eof: tc.eof}
			raw, err := capture.result()
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
			if tc.valid && string(raw) != result {
				t.Fatalf("precision/association changed: %s", raw)
			}
			if _, err := capture.result(); err == nil {
				t.Fatal("capture reused")
			}
		})
	}
}

func TestWireCaptureBoundedAndDetached(t *testing.T) {
	c := &wireCapture{}
	body := &numberCaptureBody{ReadCloser: io.NopCloser(strings.NewReader(strings.Repeat("x", maxCapturedResponse+1))), capture: c}
	if _, err := io.Copy(io.Discard, body); err != nil {
		t.Fatal(err)
	}
	if !c.overflow || len(c.data) != maxCapturedResponse {
		t.Fatal("capture grew beyond response limit")
	}
	if _, err := c.result(); err == nil {
		t.Fatal("overflow accepted")
	}
	c = &wireCapture{taken: true}
	body = &numberCaptureBody{ReadCloser: io.NopCloser(strings.NewReader("late bytes")), capture: c}
	if _, err := io.Copy(io.Discard, body); err != nil {
		t.Fatal(err)
	}
	if len(c.data) != 0 {
		t.Fatal("detached capture retained late bytes")
	}
}
