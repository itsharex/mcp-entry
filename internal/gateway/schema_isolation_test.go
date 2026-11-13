//go:build linux || darwin

package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func isolatedGatewayFixture(t *testing.T, schema string) (*Gateway, *atomic.Int32) {
	t.Helper()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "initialize":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"synthetic","version":"1"}}}`, req.ID)
		case "notifications/initialized", "notifications/cancelled":
			w.WriteHeader(202)
		case "tools/list":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"number","inputSchema":%s}]}}`, req.ID, schema)
		case "tools/call":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"ok"}]}}`, req.ID)
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(server.Close)
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Config{SchemaIsolation: &SchemaIsolation{Executable: path, Capacity: 1, Timeout: 10 * time.Second}, Systems: []System{{Name: "svc", Endpoint: server.URL, Timeouts: &TimeoutConfig{DiscoveryMS: 20000, CallMS: 20000, ResponseHeaderMS: 1000}}}, Users: map[string]User{"u": {Token: "synthetic-entry", Allowed: []string{"svc__number"}, Credentials: map[string]string{"svc": "synthetic-key"}}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	return g, &requests
}

func TestIsolatedGatewayPrecisionAndFailures(t *testing.T) {
	g, requests := isolatedGatewayFixture(t, `{"type":"object","properties":{"n":{"type":"integer"}}}`)
	if issues := g.Refresh(context.Background()); len(issues) > 0 {
		t.Fatal(issues)
	}
	e := g.state.Load().catalogs["u"]["svc__number"]
	if e.input.resolved != nil || e.input.executor == nil || len(e.input.raw) == 0 {
		t.Fatal("isolated catalog retained in-process validator")
	}
	before := requests.Load()
	if _, err := g.Call(context.Background(), "u", "svc__number", json.RawMessage(`{"n":1.00000000000000001}`)); err == nil {
		t.Fatal("fraction accepted")
	}
	if requests.Load() != before {
		t.Fatal("invalid instance contacted downstream")
	}
	if result, err := g.Call(context.Background(), "u", "svc__number", json.RawMessage(`{"n":9007199254740993}`)); err != nil || result == nil || result.IsError {
		t.Fatal("exact integer failed", err)
	}
	before = requests.Load()
	g.schemaExecutor.path = filepath.Join(t.TempDir(), "missing")
	if _, err := g.Call(context.Background(), "u", "svc__number", json.RawMessage(`{"n":3}`)); !errors.Is(err, ErrSchemaExecution) {
		t.Fatalf("execution failure classified as bad arguments: %v", err)
	}
	if requests.Load() != before {
		t.Fatal("failed helper fell back/dispatched")
	}
}

func TestIsolatedGatewayCloseAndRevocationFence(t *testing.T) {
	for _, action := range []string{"close", "revoke", "busy"} {
		t.Run(action, func(t *testing.T) {
			marker, release := filepath.Join(t.TempDir(), "pid"), filepath.Join(t.TempDir(), "release")
			data, _ := json.Marshal(map[string]any{"type": "object", "x-test-helper": "block_validate", "x-test-marker": marker, "x-test-release": release})
			g, requests := isolatedGatewayFixture(t, string(data))
			if issues := g.Refresh(context.Background()); len(issues) > 0 {
				t.Fatal(issues)
			}
			_ = os.Remove(marker)
			before := requests.Load()
			done := make(chan error, 1)
			go func() { _, err := g.Call(context.Background(), "u", "svc__number", map[string]any{}); done <- err }()
			pid := helperMarker(t, marker)
			switch action {
			case "close":
				if err := g.Close(); err != nil {
					t.Fatal(err)
				}
			case "revoke":
				if _, err := g.RevokeBinding(Identity{"demo", "u"}, "svc", 1); err != nil {
					t.Fatal(err)
				}
				_ = os.WriteFile(release, nil, 0600)
			case "busy":
				if _, err := g.Call(context.Background(), "u", "svc__number", map[string]any{}); !errors.Is(err, ErrSchemaBusy) {
					t.Fatal("busy helper not rejected", err)
				}
				if err := g.Close(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-done:
				if action == "revoke" && !errors.Is(err, ErrDenied) {
					t.Fatal("revocation fence bypassed", err)
				}
				if err == nil {
					t.Fatal("canceled/revoked validation dispatched")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("validation remained alive")
			}
			assertHelperReaped(t, pid)
			if requests.Load() != before {
				t.Fatal("blocked validation contacted downstream")
			}
			if len(g.schemaExecutor.slots) != 0 {
				t.Fatal("gateway close leaked helper")
			}
		})
	}
}

func TestIsolatedDiscoveryCancellationAndNoPublication(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "pid")
	data, _ := json.Marshal(map[string]any{"type": "object", "x-test-helper": "hang", "x-test-marker": marker})
	g, _ := isolatedGatewayFixture(t, string(data))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan []DiscoveryIssue, 1)
	go func() { done <- g.Refresh(ctx) }()
	pid := helperMarker(t, marker)
	cancel()
	select {
	case issues := <-done:
		if len(issues) == 0 {
			t.Fatal("canceled discovery reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("discovery helper not canceled")
	}
	assertHelperReaped(t, pid)
	if len(g.Names("u")) != 0 {
		t.Fatal("canceled schema published")
	}
}

func TestIsolatedGatewayExpensiveValidationNeverDispatches(t *testing.T) {
	branches := make([]any, 32)
	for i := range branches {
		branches[i] = map[string]any{"pattern": "a{1000}b"}
	}
	schema, _ := json.Marshal(map[string]any{"type": "object", "properties": map[string]any{"v": map[string]any{"anyOf": branches}}})
	g, requests := isolatedGatewayFixture(t, string(schema))
	if issues := g.Refresh(context.Background()); len(issues) > 0 {
		t.Fatal(issues)
	}
	g.schemaExecutor.budget = 150 * time.Millisecond
	before := requests.Load()
	value := make([]byte, 60000)
	for i := range value {
		value[i] = 'a'
	}
	if _, err := g.Call(context.Background(), "u", "svc__number", map[string]any{"v": string(value)}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expensive validation not canceled", err)
	}
	if requests.Load() != before || len(g.schemaExecutor.slots) != 0 {
		t.Fatal("deadline dispatched or leaked capacity")
	}
}
