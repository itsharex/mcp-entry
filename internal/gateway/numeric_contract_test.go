package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/yangkangbug/mcp-entry/internal/gateway"
)

func TestNumericContractAcrossGateway(t *testing.T) {
	cases := []struct {
		name, property, value string
		valid                 bool
	}{
		{"integer", `{"type":"integer"}`, `3`, true},
		{"ordinary_fraction", `{"type":"integer"}`, `1.5`, false},
		{"precise_fraction", `{"type":"integer"}`, `1.00000000000000001`, false},
		{"large_fraction", `{"type":"integer"}`, `9007199254740992.5`, false},
		{"large_integer", `{"type":"integer"}`, `9007199254740993`, true},
		{"uint64", `{"type":"integer"}`, `18446744073709551615`, true},
		{"minimum", `{"minimum":9007199254740993}`, `9007199254740992`, false},
		{"maximum", `{"maximum":9007199254740993}`, `9007199254740994`, false},
		{"exclusive_minimum", `{"exclusiveMinimum":1}`, `1.00000000000000001`, true},
		{"exclusive_maximum", `{"exclusiveMaximum":1.00000000000000001}`, `1`, true},
		{"enum", `{"enum":[9007199254740993]}`, `9007199254740992`, false},
		{"const", `{"const":1.00000000000000001}`, `1`, false},
		{"multiple_of_large", `{"multipleOf":3}`, `1e20`, false},
		{"multiple_of_decimal", `{"multipleOf":0.1}`, `0.3`, true},
		{"not", `{"not":{"type":"integer"}}`, `1.00000000000000001`, true},
		{"anyOf", `{"anyOf":[{"const":1.00000000000000001},{"const":2}]}`, `1`, false},
		{"oneOf", `{"oneOf":[{"const":1},{"const":1.00000000000000001}]}`, `1`, true},
		{"nested_array", `{"type":"array","items":{"type":"integer"}}`, `[1.00000000000000001]`, false},
		{"unique_items", `{"type":"array","uniqueItems":true}`, `[1,1.00000000000000001]`, true},
		{"common_decimal", `{"type":"number","minimum":0.1,"maximum":0.3}`, `0.2`, true},
		{"underflow", `{"type":"number","exclusiveMinimum":0}`, `1e-400`, true},
		{"numeric_string", `{"type":"string"}`, `1`, false},
		{"excess_exponent", `{"type":"number"}`, `1e-4097`, false},
		{"overflow", `{"type":"number"}`, `1e309`, false},
		{"near_argument_limit", `{"type":"string"}`, `"` + strings.Repeat("x", (64<<10)-30) + `"`, true},
	}
	for _, jsonResponse := range []bool{true, false} {
		wire := "json"
		if !jsonResponse {
			wire = "sse"
		}
		for _, tc := range cases {
			t.Run(wire+"/"+tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				schema := fmt.Sprintf(`{"type":"object","properties":{"value":%s},"required":["value"]}`, tc.property)
				const output = `{"id":9007199254740993,"fraction":1.00000000000000001}`
				var calls, requests atomic.Int32
				seen := make(chan string, 2)
				provider := mcp.NewServer(&mcp.Implementation{Name: "numeric-fixture", Version: "1"}, nil)
				provider.AddTool(&mcp.Tool{Name: "number", InputSchema: json.RawMessage(schema), OutputSchema: json.RawMessage(`{"type":"object","properties":{"id":{"const":9007199254740993}}}`), Meta: mcp.Meta{"serial": json.Number("9007199254740993")}}, func(_ context.Context, r *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
					calls.Add(1)
					seen <- string(r.Params.Arguments)
					return &mcp.CallToolResult{StructuredContent: json.RawMessage(output), Meta: mcp.Meta{"serial": json.Number("9007199254740993")}, Content: []mcp.Content{
						&mcp.TextContent{Text: "ok", Meta: mcp.Meta{"serial": json.Number("9007199254740993")}},
						&mcp.EmbeddedResource{Resource: &mcp.ResourceContents{URI: "synthetic:///number", Text: "ok", Meta: mcp.Meta{"serial": json.Number("9007199254740993")}}},
					}}, nil
				})
				downstreamHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return provider }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: jsonResponse})
				downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); downstreamHandler.ServeHTTP(w, r) }))
				defer downstream.Close()
				g, err := gateway.New(gateway.Config{Systems: []gateway.System{{Name: "svc", Endpoint: downstream.URL}}, Users: map[string]gateway.User{"u": {Token: "synthetic-entry", Allowed: []string{"svc__number"}, Credentials: map[string]string{"svc": "synthetic-provider"}}}})
				if err != nil {
					t.Fatal(err)
				}
				defer g.Close()
				if issues := g.Refresh(ctx); len(issues) != 0 {
					t.Fatal(issues)
				}
				entry := httptest.NewServer(g.Handler())
				defer entry.Close()
				tap := &numericResponseTap{base: http.DefaultTransport}
				client := mcp.NewClient(&mcp.Implementation{Name: "numeric-client", Version: "1"}, nil)
				session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: entry.URL, HTTPClient: &http.Client{Transport: tap}, DisableStandaloneSSE: true, MaxRetries: -1}, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer session.Close()
				if _, err := session.ListTools(ctx, nil); err != nil {
					t.Fatal(err)
				}
				// Inspect actual gateway wire bytes. An ordinary third-party SDK consumer
				// may itself round numbers; that must not hide gateway serialization bugs.
				var list struct {
					Tools []struct {
						Input  json.RawMessage `json:"inputSchema"`
						Output json.RawMessage `json:"outputSchema"`
						Meta   json.RawMessage `json:"_meta"`
					}
				}
				if err := json.Unmarshal(tap.result(t), &list); err != nil || len(list.Tools) != 1 {
					t.Fatalf("catalog: %v", err)
				}
				if !reflect.DeepEqual(numericJSON(t, list.Tools[0].Input), numericJSON(t, []byte(schema))) {
					t.Fatal("advertised input schema changed")
				}
				if !bytes.Contains(list.Tools[0].Output, []byte("9007199254740993")) || !bytes.Contains(list.Tools[0].Meta, []byte("9007199254740993")) {
					t.Fatal("advertised schema/metadata rounded")
				}
				raw := `{"value":` + tc.value + `}`
				for _, route := range []string{"direct", "http"} {
					beforeRequests, beforeCalls := requests.Load(), calls.Load()
					var result *mcp.CallToolResult
					if route == "direct" {
						result, err = g.Call(ctx, "u", "svc__number", json.RawMessage(raw))
					} else {
						result, err = session.CallTool(ctx, &mcp.CallToolParams{Name: "svc__number", Arguments: json.RawMessage(raw)})
					}
					valid := err == nil && result != nil && !result.IsError
					wantValid := tc.valid
					// The existing HTTP ingress ceiling counts the full request,
					// unlike the direct API's argument-only ceiling. Preserve both.
					if route == "http" && tc.name == "near_argument_limit" {
						wantValid = false
					}
					if valid != wantValid {
						t.Fatalf("%s accept=%v want=%v result=%v err=%v", route, valid, wantValid, result, err)
					}
					if !valid {
						if calls.Load() != beforeCalls || requests.Load() != beforeRequests {
							t.Fatal("invalid arguments contacted downstream")
						}
						continue
					}
					if calls.Load() != beforeCalls+1 {
						t.Fatal("valid operation repeated or not dispatched")
					}
					if got := <-seen; got != raw {
						t.Fatalf("arguments changed: %s", got)
					}
					var returned []byte
					if route == "direct" {
						returned, err = json.Marshal(result)
						if err != nil {
							t.Fatal(err)
						}
					} else {
						returned = tap.result(t)
					}
					var fields map[string]json.RawMessage
					if json.Unmarshal(returned, &fields) != nil {
						t.Fatal("bad result")
					}
					if string(fields["structuredContent"]) != output {
						t.Fatalf("%s structured output changed: %s", route, fields["structuredContent"])
					}
					if !bytes.Contains(fields["_meta"], []byte("9007199254740993")) || !bytes.Contains(fields["content"], []byte("9007199254740993")) {
						t.Fatal("output metadata rounded")
					}
					var contents []struct {
						Resource struct {
							Meta map[string]json.RawMessage `json:"_meta"`
						} `json:"resource"`
					}
					if json.Unmarshal(fields["content"], &contents) != nil || len(contents) != 2 || string(contents[1].Resource.Meta["serial"]) != "9007199254740993" {
						t.Fatal("embedded resource metadata rounded")
					}
				}
			})
		}
	}
}

func numericJSON(t *testing.T, data []byte) any {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

type numericResponseTap struct {
	base http.RoundTripper
	mu   sync.Mutex
	last []byte
}

func (t *numericResponseTap) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer synthetic-entry")
	response, err := t.base.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	if response.Header.Get("Content-Type") == "application/json" {
		data, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			return nil, err
		}
		response.Body = io.NopCloser(bytes.NewReader(data))
		t.mu.Lock()
		t.last = append([]byte(nil), data...)
		t.mu.Unlock()
	}
	return response, nil
}
func (t *numericResponseTap) result(test *testing.T) json.RawMessage {
	test.Helper()
	t.mu.Lock()
	defer t.mu.Unlock()
	var fields map[string]json.RawMessage
	if json.Unmarshal(t.last, &fields) != nil || len(fields["result"]) == 0 {
		test.Fatalf("missing raw result: %s", t.last)
	}
	return fields["result"]
}
