package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const maxCapturedResponse = 1 << 20

// Arguments already have a 64 KiB limit. The SDK adds its JSON-RPC envelope,
// method/tool name and request ID, so the capture ceiling must not reuse that
// argument-only limit. Discovery cursors and tool names are separately bounded.
const maxCapturedRequest = 128 << 10

var errWireNumbers = errors.New("invalid downstream numeric representation")

type wireCaptureKey struct{}

// A capture belongs to one SDK method invocation, not a session, user, tool name
// or most-recent response. The SDK still owns HTTP/SSE, negotiation and cleanup.
// Capture happens before SDK decoding; restoration happens only after that same
// SDK invocation succeeds. Never convert an SDK error into a successful result.
type wireCapture struct {
	mu                             sync.Mutex
	method                         string
	id                             jsonrpc.ID
	media                          string
	data                           []byte
	attached, overflow, eof, taken bool
}

type numberCaptureTransport struct{ base http.RoundTripper }

func (t numberCaptureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	capture, _ := r.Context().Value(wireCaptureKey{}).(*wireCapture)
	if capture == nil || r.Method != http.MethodPost || r.GetBody == nil {
		return t.base.RoundTrip(r)
	}
	body, err := r.GetBody()
	if err != nil {
		return nil, errWireNumbers
	}
	raw, err := io.ReadAll(io.LimitReader(body, maxCapturedRequest+1))
	_ = body.Close()
	if err != nil || len(raw) > maxCapturedRequest {
		return nil, errWireNumbers
	}
	message, err := jsonrpc.DecodeMessage(raw)
	request, ok := message.(*jsonrpc.Request)
	// Cancellation/other protocol messages can inherit a call context. They are
	// not its result carrier and must not consume or overwrite its capture.
	if err != nil || !ok || request.Method != capture.method || !request.IsCall() {
		return t.base.RoundTrip(r)
	}
	capture.mu.Lock()
	if capture.attached || capture.taken {
		capture.mu.Unlock()
		return nil, errWireNumbers
	}
	capture.attached, capture.id = true, request.ID
	capture.mu.Unlock()
	response, err := t.base.RoundTrip(r)
	if err != nil {
		return response, err
	}
	media, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	capture.mu.Lock()
	capture.media = media
	capture.mu.Unlock()
	response.Body = &numberCaptureBody{ReadCloser: response.Body, capture: capture}
	return response, nil
}

type numberCaptureBody struct {
	io.ReadCloser
	capture *wireCapture
}

func (b *numberCaptureBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	c := b.capture
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.taken {
		room := maxCapturedResponse - len(c.data)
		if n > room {
			c.overflow = true
			c.data = append(c.data, p[:room]...)
		} else {
			c.data = append(c.data, p[:n]...)
		}
		if err == io.EOF {
			c.eof = true
		}
	}
	return n, err
}

func (c *wireCapture) result() (json.RawMessage, error) {
	c.mu.Lock()
	if c.taken || !c.attached || c.overflow {
		c.mu.Unlock()
		return nil, errWireNumbers
	}
	// Transfer ownership rather than copying the whole response. Subsequent SDK
	// SSE draining discards bytes and cannot mutate the detached capture.
	data, id, media, eof := c.data, c.id, c.media, c.eof
	c.data = nil
	c.taken = true
	c.mu.Unlock()
	extract := func(data []byte) (json.RawMessage, bool, error) {
		msg, err := jsonrpc.DecodeMessage(data)
		if err != nil {
			return nil, false, errWireNumbers
		}
		response, ok := msg.(*jsonrpc.Response)
		if !ok || response.ID != id {
			return nil, false, nil
		}
		if response.Error != nil || len(response.Result) == 0 {
			return nil, false, errWireNumbers
		}
		return response.Result, true, nil
	}
	switch media {
	case "application/json":
		raw, matched, err := extract(data)
		if err != nil || !matched {
			return nil, errWireNumbers
		}
		return raw, nil
	case "text/event-stream":
		// Match the pinned SDK's event framing (event.go): LF/CRLF, trimmed
		// field values, joined data lines, optional "message" event name, EOF
		// flush, and ignored comments/unknown fields. This reads captured bytes,
		// never opens a stream or implements reconnection/session behavior.
		var payload []byte
		name := ""
		emit := func() (json.RawMessage, bool, error) {
			if len(payload) == 0 || (name != "" && name != "message") {
				return nil, false, nil
			}
			return extract(payload)
		}
		for len(data) > 0 {
			end := bytes.IndexByte(data, '\n')
			last := end < 0
			if last && !eof {
				break
			}
			var line []byte
			if last {
				line = data
				data = nil
			} else {
				line = data[:end+1]
				data = data[end+1:]
			}
			line = bytes.TrimRight(line, "\r\n")
			if len(line) == 0 {
				raw, matched, err := emit()
				if err != nil {
					return nil, err
				}
				if matched {
					return raw, nil
				}
				payload = nil
				name = ""
			} else {
				key, value, found := bytes.Cut(line, []byte{':'})
				if !found {
					return nil, errWireNumbers
				}
				switch string(key) {
				case "event":
					name = string(bytes.TrimSpace(value))
				case "data":
					if payload != nil {
						payload = append(payload, '\n')
					}
					payload = append(payload, bytes.TrimSpace(value)...)
				}
			}
			if last {
				break
			}
		}
		if eof {
			raw, matched, err := emit()
			if err == nil && matched {
				return raw, nil
			}
		}
	}
	return nil, errWireNumbers
}

// Restore only exact, case-sensitive wire fields already accepted by the SDK.
// Other protocol fields and result/error classification remain SDK-owned.
func restoreWireNumbers(result mcp.Result, raw json.RawMessage) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return errWireNumbers
	}
	if meta, ok := fields["_meta"]; ok {
		value, err := exactWireMeta(meta)
		if err != nil {
			return err
		}
		result.SetMeta(value)
	}
	switch result := result.(type) {
	case *mcp.ListToolsResult:
		var tools []json.RawMessage
		if json.Unmarshal(fields["tools"], &tools) != nil || len(tools) != len(result.Tools) {
			return errWireNumbers
		}
		for i, tool := range result.Tools {
			if tool == nil {
				continue
			} // Existing catalog admission rejects nil tools.
			var fields map[string]json.RawMessage
			if json.Unmarshal(tools[i], &fields) != nil || fields == nil {
				return errWireNumbers
			}
			var name string
			if json.Unmarshal(fields["name"], &name) != nil || name != tool.Name {
				return errWireNumbers
			}
			for key, target := range map[string]*any{"inputSchema": &tool.InputSchema, "outputSchema": &tool.OutputSchema} {
				if data, ok := fields[key]; ok {
					value, err := decodeExactJSON(data, MaxSchemaDepth, MaxSchemaRawNodes)
					if err != nil {
						return schemaAdmissionFailure(err)
					}
					*target = value
				}
			}
			if meta, ok := fields["_meta"]; ok {
				value, err := exactWireMeta(meta)
				if err != nil {
					return err
				}
				tool.Meta = value
			}
		}
	case *mcp.CallToolResult:
		if value, ok := fields["structuredContent"]; ok && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			// Opaque output: retain JSON, not a float64 reconstruction, and do not
			// apply input limits/defaults or claim output-schema validation.
			result.StructuredContent = append(json.RawMessage(nil), value...)
		}
		var contents []map[string]json.RawMessage
		if data, ok := fields["content"]; ok {
			if json.Unmarshal(data, &contents) != nil || len(contents) != len(result.Content) {
				return errWireNumbers
			}
			for i, content := range result.Content {
				// Resource contents have their own metadata, independently of the
				// enclosing content block's metadata. Do not skip it when the outer
				// _meta is absent.
				if embedded, ok := content.(*mcp.EmbeddedResource); ok && embedded.Resource != nil {
					var resource map[string]json.RawMessage
					if json.Unmarshal(contents[i]["resource"], &resource) != nil {
						return errWireNumbers
					}
					if meta, ok := resource["_meta"]; ok {
						value, err := exactWireMeta(meta)
						if err != nil {
							return err
						}
						embedded.Resource.Meta = value
					}
				}
				meta, ok := contents[i]["_meta"]
				if !ok {
					continue
				}
				value, err := exactWireMeta(meta)
				if err != nil {
					return err
				}
				switch content := content.(type) {
				case *mcp.TextContent:
					content.Meta = value
				case *mcp.ImageContent:
					content.Meta = value
				case *mcp.AudioContent:
					content.Meta = value
				case *mcp.ResourceLink:
					content.Meta = value
				case *mcp.EmbeddedResource:
					content.Meta = value
				default:
					return errWireNumbers
				}
			}
		}
	default:
		return errWireNumbers
	}
	return nil
}

func exactWireMeta(raw []byte) (map[string]any, error) {
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, errWireNumbers
	}
	return value, nil
}

func withWireNumbers(ctx context.Context, method string) (context.Context, *wireCapture) {
	capture := &wireCapture{method: method}
	return context.WithValue(ctx, wireCaptureKey{}, capture), capture
}
