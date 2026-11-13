package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/yangkangbug/mcp-entry/internal/gateway"
)

func TestSchemaHelperRealCLIWithoutConfiguration(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("isolation platform unsupported")
	}
	binaryPath := filepath.Join(t.TempDir(), "gateway")
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-buildvcs=false", "-o", binaryPath, "./cmd/gateway")
	build.Dir = "../.."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	schema := []byte(`{"type":"object","properties":{"n":{"type":"integer"}}}`)
	for _, tc := range []struct {
		mode, status byte
		instance     string
	}{{1, 0, ""}, {3, 0, `{"n":9007199254740993}`}, {3, 1, `{"n":1.00000000000000001}`}} {
		data := make([]byte, 16)
		copy(data, "SI01")
		data[4] = tc.mode
		binary.BigEndian.PutUint32(data[8:12], uint32(len(schema)))
		binary.BigEndian.PutUint32(data[12:16], uint32(len(tc.instance)))
		data = append(append(data, schema...), tc.instance...)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cmd := exec.CommandContext(ctx, binaryPath, gateway.SchemaHelperArgument)
		// No configs/demo.json exists here. The private entry must not reach normal
		// configuration, credentials, database, flags, logging, or listener startup.
		cmd.Dir = t.TempDir()
		cmd.Env = []string{"GOMAXPROCS=1"}
		cmd.Stdin = bytes.NewReader(data)
		started := time.Now()
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil || len(out) != 8 || string(out[:4]) != "SO01" || out[4] != tc.status {
			t.Fatalf("helper err=%v reply=%v", err, out)
		}
		t.Logf("mode=%d real CLI startup+execution=%s", tc.mode, time.Since(started))
	}
	// Invalid IPC stays a fixed protocol reply and cannot reveal a local path.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binaryPath, gateway.SchemaHelperArgument)
	cmd.Dir = t.TempDir()
	cmd.Stdin = bytes.NewReader([]byte("secret-canary"))
	cmd.Env = os.Environ()
	if out, err := cmd.CombinedOutput(); err != nil || !bytes.Equal(out, []byte{'S', 'O', '0', '1', 4, 0, 0, 0}) {
		t.Fatalf("invalid input leaked: %v %v", err, out)
	}
}
