//go:build linux || darwin

package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The test binary implements the same private entry before test initialization.
// Test-only hang/crash/flood modes are never part of the shipped CLI helper.
func TestMain(m *testing.M) {
	if len(os.Args) == 2 && os.Args[1] == SchemaHelperArgument {
		data, _ := io.ReadAll(io.LimitReader(os.Stdin, (128<<10)+17))
		if len(data) >= 16 {
			n := int(binary.BigEndian.Uint32(data[8:12]))
			if n <= len(data)-16 {
				var fields map[string]any
				_ = json.Unmarshal(data[16:16+n], &fields)
				if path, ok := fields["x-test-marker"].(string); ok {
					_ = os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0600)
				}
				switch fields["x-test-helper"] {
				case "block_validate":
					if data[4] == schemaValidateInput {
						path, _ := fields["x-test-release"].(string)
						for {
							if _, err := os.Stat(path); err == nil {
								break
							}
							time.Sleep(time.Millisecond)
						}
					}
				case "hang":
					for {
						time.Sleep(time.Hour)
					}
				case "crash":
					os.Exit(7)
				case "short":
					_, _ = os.Stdout.Write([]byte("SO01"))
					os.Exit(0)
				case "bad_reply":
					_, _ = os.Stdout.Write([]byte{'S', 'O', '0', '1', 99, 0, 0, 0})
					os.Exit(0)
				case "stderr":
					_, _ = os.Stderr.Write([]byte("secret-canary"))
					os.Exit(7)
				case "flood":
					_, _ = os.Stdout.Write(bytes.Repeat([]byte("x"), 1<<20))
					os.Exit(0)
				case "environment":
					if len(os.Environ()) != 1 || os.Getenv("GOMAXPROCS") != "1" {
						os.Exit(8)
					}
					cwd, _ := os.Getwd()
					if cwd != "/" {
						os.Exit(9)
					}
				}
			}
		}
		if RunSchemaHelper(bytes.NewReader(data), os.Stdout) != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func testSchemaExecutor(t *testing.T, capacity int, budget time.Duration) *schemaExecutor {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	e, err := newSchemaExecutor(&SchemaIsolation{Executable: path, Capacity: capacity, Timeout: budget})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.stop(); e.wait() })
	return e
}
func helperFrame(mode byte, schema, instance []byte) []byte {
	h := make([]byte, 16)
	copy(h, "SI01")
	h[4] = mode
	binary.BigEndian.PutUint32(h[8:12], uint32(len(schema)))
	binary.BigEndian.PutUint32(h[12:16], uint32(len(instance)))
	return append(append(h, schema...), instance...)
}
func helperMarker(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(path); err == nil {
			pid, err := strconv.Atoi(string(data))
			if err == nil {
				return pid
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("helper did not start")
	return 0
}
func assertHelperReaped(t *testing.T, pid int) {
	t.Helper()
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("child still present after Wait: %v", err)
	}
}

func TestSchemaHelperProtocolAndExactSemantics(t *testing.T) {
	schema := []byte(`{"type":"object","properties":{"n":{"type":"integer"}}}`)
	for _, tc := range []struct {
		mode             byte
		schema, instance []byte
		status           byte
	}{
		{schemaCompileInput, schema, nil, schemaOK},
		{schemaCompileOutput, []byte(`{"type":"number"}`), nil, schemaOK},
		{schemaValidateInput, schema, []byte(`{"n":9007199254740993}`), schemaOK},
		{schemaValidateInput, schema, []byte(`{"n":1.00000000000000001}`), schemaInvalid},
		{schemaCompileInput, []byte(`{"type":"object","pattern":"["}`), nil, schemaRegexpRejected},
		{schemaCompileInput, []byte(`{"type":"object","$recursiveRef":"#"}`), nil, schemaRejected},
	} {
		var out bytes.Buffer
		if err := RunSchemaHelper(bytes.NewReader(helperFrame(tc.mode, tc.schema, tc.instance)), &out); err != nil {
			t.Fatal(err)
		}
		if out.Len() != 8 || out.Bytes()[4] != tc.status {
			t.Fatalf("reply=%v want=%v", out.Bytes(), tc.status)
		}
	}
	valid := helperFrame(schemaCompileInput, schema, nil)
	for _, data := range [][]byte{nil, []byte("secret"), valid[:15], append(append([]byte(nil), valid...), 0), helperFrame(99, schema, nil), helperFrame(schemaCompileInput, bytes.Repeat([]byte("x"), (64<<10)+1), nil)} {
		var out bytes.Buffer
		_ = RunSchemaHelper(bytes.NewReader(data), &out)
		if out.Len() != 8 || out.Bytes()[4] != schemaProtocol {
			t.Fatal("malformed request accepted")
		}
	}
}

func TestSchemaExecutorCancelCapacityCloseAndReap(t *testing.T) {
	for _, mode := range []string{"cancel", "close", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			budget := 10 * time.Second
			if mode == "timeout" {
				budget = 300 * time.Millisecond
			}
			e := testSchemaExecutor(t, 1, budget)
			marker := filepath.Join(t.TempDir(), "pid")
			schema, _ := json.Marshal(map[string]any{"type": "object", "x-test-helper": "hang", "x-test-marker": marker})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- e.execute(ctx, schemaCompileInput, schema, nil) }()
			pid := helperMarker(t, marker)
			if err := e.execute(context.Background(), schemaCompileInput, []byte(`{"type":"object"}`), nil); !errors.Is(err, ErrSchemaBusy) {
				t.Fatalf("capacity not bounded: %v", err)
			}
			if mode == "cancel" {
				cancel()
			}
			if mode == "close" {
				e.stop()
				e.wait()
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("wrong cancellation: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("helper not reaped")
			}
			assertHelperReaped(t, pid)
			if len(e.slots) != 0 {
				t.Fatal("slot leaked")
			}
			if mode == "close" {
				if err := e.execute(context.Background(), schemaCompileInput, []byte(`{"type":"object"}`), nil); !errors.Is(err, ErrSchemaExecution) {
					t.Fatal("closed admission accepted")
				}
			} else {
				// A larger per-helper ceiling allows race-instrumented test binaries to
				// perform their exit cleanup. Never changes gateway production defaults.
				e.budget = 3 * time.Second
				if err := e.execute(context.Background(), schemaCompileInput, []byte(`{"type":"object"}`), nil); err != nil {
					t.Fatal("capacity did not recover", err)
				}
			}
		})
	}
}

func TestSchemaExecutorFailuresEnvironmentAndSlowValidation(t *testing.T) {
	e := testSchemaExecutor(t, 1, 3*time.Second)
	t.Setenv("SCHEMA_SECRET_CANARY", "must-not-inherit")
	if err := e.execute(context.Background(), schemaCompileInput, []byte(`{"type":"object","x-test-helper":"environment"}`), nil); err != nil {
		t.Fatal("child inherited environment/cwd", err)
	}
	for _, mode := range []string{"crash", "flood", "short", "bad_reply", "stderr"} {
		schema, _ := json.Marshal(map[string]any{"type": "object", "x-test-helper": mode})
		if err := e.execute(context.Background(), schemaCompileInput, schema, nil); !errors.Is(err, ErrSchemaExecution) {
			t.Fatal("helper failure exposed", err)
		}
	}
	// Trusted path disappearing after validation fails closed, never in-process.
	e.path = filepath.Join(t.TempDir(), "missing")
	if err := e.execute(context.Background(), schemaCompileInput, []byte(`{"type":"object"}`), nil); !errors.Is(err, ErrSchemaExecution) {
		t.Fatal("launch failure did not close", err)
	}
	e = testSchemaExecutor(t, 1, 150*time.Millisecond)
	branches := make([]any, 32)
	for i := range branches {
		branches[i] = map[string]any{"pattern": "a{1000}b"}
	}
	schema, _ := json.Marshal(map[string]any{"type": "object", "properties": map[string]any{"v": map[string]any{"anyOf": branches}}})
	instance, _ := json.Marshal(map[string]any{"v": strings.Repeat("a", 60000)})
	if err := e.execute(context.Background(), schemaValidateInput, schema, instance); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expensive validation was not stopped", err)
	}
	if len(e.slots) != 0 {
		t.Fatal("slow validation slot leaked")
	}
}

func TestSchemaExecutorConcurrentStop(t *testing.T) {
	e := testSchemaExecutor(t, 2, 50*time.Millisecond)
	done := make(chan error, 32)
	for i := 0; i < 32; i++ {
		go func() { done <- e.execute(context.Background(), schemaCompileInput, []byte(`{"type":"object"}`), nil) }()
	}
	e.stop()
	e.wait()
	for i := 0; i < 32; i++ {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent admission survived stop")
		}
	}
	if len(e.slots) != 0 {
		t.Fatal("concurrent stop leaked capacity")
	}
}
