package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/yangkangbug/mcp-entry/internal/gateway"
)

func TestHTTPWriteBudgetCoversLargestAcceptedOperation(t *testing.T) {
	// First prove that this profile is accepted, not merely a hypothetical value.
	maximumMS := int(gateway.MaxOperationTimeout / time.Millisecond)
	g, err := gateway.New(gateway.Config{Systems: []gateway.System{{Name: "svc", Endpoint: "http://127.0.0.1:1", Timeouts: &gateway.TimeoutConfig{DiscoveryMS: maximumMS, CallMS: maximumMS, ResponseHeaderMS: maximumMS}}}})
	if err != nil {
		t.Fatal("largest documented profile was rejected", err)
	}
	defer g.Close()
	server := newGatewayHTTPServer(http.NotFoundHandler(), nil)
	required := inboundHeaderTimeout + inboundReadTimeout + gateway.MaxOperationTimeout + 2*gateway.AuditCommitTimeout + 10*time.Second
	if server.WriteTimeout != required || server.WriteTimeout <= 0 {
		t.Fatalf("write budget %s cannot cover accepted operation plus ingress/audit/delivery allowance %s", server.WriteTimeout, required)
	}
}

// Default CI compresses only the transport write timer and the synthetic wait,
// retaining real TCP/TLS, HTTP/1.1 or HTTP/2, and the complete MCP gateway path.
// MCP_ENTRY_LONG_HTTP_TEST=1 reruns the documented real 61-second regression
// with the unmodified production server timers and a legal 70-second profile.
func TestHTTPWriteBudgetDeliversConfirmedSlowOperation(t *testing.T) {
	long := os.Getenv("MCP_ENTRY_LONG_HTTP_TEST") == "1"
	for _, h2 := range []bool{false, true} {
		name := "h1"
		if h2 {
			name = "h2"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			wait := 800 * time.Millisecond
			if long {
				wait = 61 * time.Second
			}
			var calls atomic.Int32
			var protocol atomic.Value
			provider := mcp.NewServer(&mcp.Implementation{Name: "synthetic-slow", Version: "1"}, nil)
			provider.AddTool(&mcp.Tool{Name: "slow", InputSchema: map[string]any{"type": "object"}}, func(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				calls.Add(1)
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "synthetic-complete"}}}, nil
			})
			downstream := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return provider }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true}))
			defer downstream.Close()
			g, err := gateway.New(gateway.Config{Systems: []gateway.System{{Name: "svc", Endpoint: downstream.URL, Timeouts: &gateway.TimeoutConfig{DiscoveryMS: 5000, CallMS: 70000, ResponseHeaderMS: 70000}}}, Users: map[string]gateway.User{"u": {Token: "synthetic-entry", Allowed: []string{"svc__slow"}, Credentials: map[string]string{"svc": "synthetic-provider"}}}})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Second)
			defer cancel()
			if issues := g.Refresh(ctx); len(issues) != 0 {
				t.Fatal(issues)
			}
			handler := g.Handler()
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { protocol.Store(r.Proto); handler.ServeHTTP(w, r) }))
			server.Config = newGatewayHTTPServer(server.Config.Handler, nil)
			if !long {
				server.Config.WriteTimeout = server.Config.WriteTimeout / 100
			}
			server.EnableHTTP2 = h2
			if h2 {
				server.StartTLS()
			} else {
				server.Start()
			}
			defer server.Close()
			client := server.Client()
			client.Timeout = 75 * time.Second
			client.Transport = budgetContractBearer{client.Transport}
			sdk := mcp.NewClient(&mcp.Implementation{Name: "synthetic-budget", Version: "1"}, nil)
			session, err := sdk.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: server.URL, HTTPClient: client, MaxRetries: -1, DisableStandaloneSSE: true}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			start := time.Now()
			result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "svc__slow", Arguments: map[string]any{}})
			if err != nil || result == nil || result.IsError {
				t.Fatalf("confirmed response lost: result=%v err=%v", result, err)
			}
			if len(result.Content) != 1 {
				t.Fatal("missing result content")
			}
			text, ok := result.Content[0].(*mcp.TextContent)
			if !ok || text.Text != "synthetic-complete" {
				t.Fatal("incorrect confirmed result")
			}
			wantProtocol := "HTTP/1.1"
			if h2 {
				wantProtocol = "HTTP/2.0"
			}
			if protocol.Load() != wantProtocol || calls.Load() != 1 {
				t.Fatalf("protocol=%v calls=%d", protocol.Load(), calls.Load())
			}
			page, err := g.Audit(gateway.Identity{Tenant: "demo", Principal: "u"}, 0, 20)
			if err != nil {
				t.Fatal(err)
			}
			terminals := 0
			for _, event := range page.Events {
				if event.Action == gateway.AuditActionCall && event.Phase == gateway.AuditPhaseTerminal {
					terminals++
					if event.Result != gateway.AuditResultSuccess || event.OutcomeUnknown {
						t.Fatalf("unexpected audit evidence: %+v", event)
					}
				}
			}
			if terminals != 1 {
				t.Fatal("missing single terminal audit")
			}
			t.Logf("protocol=%v elapsed=%s received_result=true calls=%d production_timers=%v", protocol.Load(), time.Since(start), calls.Load(), long)
		})
	}
}

type budgetContractBearer struct{ base http.RoundTripper }

func (b budgetContractBearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer synthetic-entry")
	return b.base.RoundTrip(r)
}
