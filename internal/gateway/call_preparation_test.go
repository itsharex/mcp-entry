package gateway_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yangkangbug/mcp-entry/internal/gateway"
	"github.com/yangkangbug/mcp-entry/internal/mock"
)

// A synchronous caller-provided encoder gives this test a deterministic slow
// preparation phase without expensive regular expressions or production hooks.
// It deliberately cannot be interrupted by Call's context.
type preparationArguments struct{ prepare func() }

func (a preparationArguments) MarshalJSON() ([]byte, error) {
	a.prepare()
	return []byte(`{"message":"synthetic"}`), nil
}

func TestCallBudgetIncludesArgumentPreparation(t *testing.T) {
	for _, mode := range []string{"configured", "parent_deadline", "parent_cancel", "success"} {
		t.Run(mode, func(t *testing.T) {
			var requests atomic.Int32
			downstream := mock.Handler("svc", map[string]string{"synthetic-key": "u"})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				downstream.ServeHTTP(w, r)
			}))
			defer server.Close()
			callMS := 1000
			if mode == "configured" {
				callMS = 25
			}
			g, err := gateway.New(gateway.Config{
				Systems: []gateway.System{{Name: "svc", Endpoint: server.URL, Timeouts: &gateway.TimeoutConfig{DiscoveryMS: 1000, CallMS: callMS, ResponseHeaderMS: 1000}}},
				Users:   map[string]gateway.User{"u": {Token: "synthetic-entry", Allowed: []string{"svc__echo"}, Credentials: map[string]string{"svc": "synthetic-key"}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			if issues := g.Refresh(context.Background()); len(issues) != 0 {
				t.Fatal(issues)
			}
			before := requests.Load()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "parent_deadline" {
				var deadlineCancel context.CancelFunc
				ctx, deadlineCancel = context.WithTimeout(ctx, 25*time.Millisecond)
				defer deadlineCancel()
			}
			prepared := false
			args := preparationArguments{prepare: func() {
				switch mode {
				case "configured":
					<-time.After(100 * time.Millisecond)
				case "parent_deadline":
					<-ctx.Done()
				case "parent_cancel":
					cancel()
				}
				prepared = true
			}}
			result, err := g.Call(ctx, "u", "svc__echo", args)
			// An earlier parent deadline may expire before Call starts on a
			// heavily loaded runner; rejecting before preparation is also correct.
			if !prepared && mode != "parent_deadline" {
				t.Fatal("argument preparation was not exercised")
			}
			if mode == "success" {
				if err != nil || result == nil || result.IsError || requests.Load() <= before {
					t.Fatalf("healthy control failed: result=%v err=%v", result, err)
				}
				return
			}
			want := context.DeadlineExceeded
			if mode == "parent_cancel" {
				want = context.Canceled
			}
			if result != nil || !errors.Is(err, want) {
				t.Fatalf("result=%v err=%v; want %v", result, err, want)
			}
			if requests.Load() != before {
				t.Fatal("preparation exhausted budget but downstream was contacted")
			}
			if mode == "configured" && ctx.Err() != nil {
				t.Fatal("test exhausted parent instead of service budget")
			}
		})
	}
}
