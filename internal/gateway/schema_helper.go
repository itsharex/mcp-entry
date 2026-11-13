package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// SchemaHelperArgument is a private process mode, not a network API. A host
// binary must dispatch it before loading credentials/configuration or serving.
const SchemaHelperArgument = "--mcp-entry-schema-helper-v1"

// SchemaIsolation is a trusted runtime option, never accepted from wire JSON.
// The executable must implement SchemaHelperArgument and spawn no descendants.
// No filesystem/network sandbox or hard memory limit is provided.
type SchemaIsolation struct {
	Executable string
	Capacity   int
	Timeout    time.Duration
}

var (
	ErrSchemaBusy      = errors.New("schema execution capacity exhausted")
	ErrSchemaExecution = errors.New("schema execution unavailable")
	errSchemaInstance  = errors.New("schema instance rejected")
)

const (
	schemaCompileInput   byte = 1
	schemaCompileOutput  byte = 2
	schemaValidateInput  byte = 3
	schemaOK             byte = 0
	schemaInvalid        byte = 1
	schemaRejected       byte = 2
	schemaRegexpRejected byte = 3
	schemaProtocol       byte = 4
)

type schemaExecutor struct {
	path   string
	budget time.Duration
	slots  chan struct{}
	mu     sync.Mutex
	closed bool
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newSchemaExecutor(cfg *SchemaIsolation) (*schemaExecutor, error) {
	if cfg == nil {
		return nil, nil
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		return nil, errors.New("schema isolation platform unsupported")
	}
	if !filepath.IsAbs(cfg.Executable) || cfg.Capacity < 1 || cfg.Capacity > 16 || cfg.Timeout < time.Millisecond || cfg.Timeout > 120*time.Second {
		return nil, errors.New("invalid schema isolation configuration")
	}
	info, err := os.Stat(cfg.Executable)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return nil, errors.New("invalid schema helper executable")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &schemaExecutor{path: cfg.Executable, budget: cfg.Timeout, slots: make(chan struct{}, cfg.Capacity), ctx: ctx, cancel: cancel}, nil
}

func resolveIsolatedSchema(ctx context.Context, executor *schemaExecutor, value any, input bool) (*boundedSchema, error) {
	data, err := json.Marshal(value)
	if err != nil || len(data) > 64<<10 {
		return nil, schemaAdmissionFailure(errExactJSON)
	}
	mode := schemaCompileOutput
	if input {
		mode = schemaCompileInput
	}
	if err := executor.execute(ctx, mode, data, nil); err != nil {
		return nil, err
	}
	return &boundedSchema{executor: executor, raw: data}, nil
}

// stop may run under Gateway.mu; it only prevents admission and cancels work.
// wait must run outside that lock. Registration and shutdown share e.mu.
func (e *schemaExecutor) stop() {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.closed = true
	e.cancel()
	e.mu.Unlock()
}
func (e *schemaExecutor) wait() {
	if e != nil {
		e.wg.Wait()
	}
}

func (e *schemaExecutor) execute(parent context.Context, mode byte, schema, instance []byte) error {
	if len(schema) > 64<<10 || len(instance) > 64<<10 {
		return ErrSchemaExecution
	}
	ctx, cancel := context.WithTimeout(parent, e.budget)
	defer cancel()
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrSchemaExecution
	}
	if err := ctx.Err(); err != nil {
		e.mu.Unlock()
		return err
	}
	select {
	case e.slots <- struct{}{}:
	default:
		e.mu.Unlock()
		return ErrSchemaBusy
	}
	e.wg.Add(1)
	e.mu.Unlock()
	defer func() { <-e.slots; e.wg.Done() }()
	stop := context.AfterFunc(e.ctx, cancel)
	defer stop()
	// The deadline starts before spawn/IPC. Cancellation triggers kill, but Run
	// does not return until Wait completes; OS reaping is not a strict wall-clock
	// guarantee. No compute goroutine is abandoned when the deadline expires.
	var request bytes.Buffer
	header := make([]byte, 16)
	copy(header, "SI01")
	header[4] = mode
	binary.BigEndian.PutUint32(header[8:12], uint32(len(schema)))
	binary.BigEndian.PutUint32(header[12:16], uint32(len(instance)))
	request.Write(header)
	request.Write(schema)
	request.Write(instance)
	response := &schemaReply{}
	cmd := exec.CommandContext(ctx, e.path, SchemaHelperArgument)
	cmd.Env = []string{"GOMAXPROCS=1"}
	cmd.Dir = "/"
	cmd.Stdin = &request
	cmd.Stdout = response
	cmd.Stderr = io.Discard
	cmd.WaitDelay = 100 * time.Millisecond
	err := cmd.Run()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil || response.overflow || response.n != 8 || string(response.data[:4]) != "SO01" || response.data[5] != 0 || response.data[6] != 0 || response.data[7] != 0 {
		return ErrSchemaExecution
	}
	switch response.data[4] {
	case schemaOK:
		return nil
	case schemaInvalid:
		if mode == schemaValidateInput {
			return errSchemaInstance
		}
	case schemaRejected:
		return &schemaAdmissionError{reason: SchemaInvalidOrUnsupported}
	case schemaRegexpRejected:
		return &schemaAdmissionError{reason: SchemaRegexpInvalidOrUnsupported}
	}
	return ErrSchemaExecution
}

type schemaReply struct {
	data     [8]byte
	n        int
	overflow bool
}

func (r *schemaReply) Write(p []byte) (int, error) {
	n := copy(r.data[r.n:], p)
	r.n += n
	if n < len(p) {
		r.overflow = true
	}
	return len(p), nil
}

// RunSchemaHelper reads one bounded request and emits exactly one fixed reply.
// It reuses the production schema policy and precision engine, without secrets,
// configuration, network, SQLite, logging, or child process creation.
func RunSchemaHelper(in io.Reader, out io.Writer) error {
	status := runSchemaHelper(in)
	reply := []byte{'S', 'O', '0', '1', status, 0, 0, 0}
	_, err := out.Write(reply)
	return err
}
func runSchemaHelper(in io.Reader) byte {
	var h [16]byte
	if _, err := io.ReadFull(in, h[:]); err != nil {
		return schemaProtocol
	}
	mode := h[4]
	ns, ni := binary.BigEndian.Uint32(h[8:12]), binary.BigEndian.Uint32(h[12:16])
	if string(h[:4]) != "SI01" || h[5] != 0 || h[6] != 0 || h[7] != 0 || mode < schemaCompileInput || mode > schemaValidateInput || ns == 0 || ns > 64<<10 || ni > 64<<10 || (mode != schemaValidateInput && ni != 0) {
		return schemaProtocol
	}
	data := make([]byte, int(ns)+int(ni))
	if _, err := io.ReadFull(in, data); err != nil {
		return schemaProtocol
	}
	var tail [1]byte
	if n, err := in.Read(tail[:]); n != 0 || err != io.EOF {
		return schemaProtocol
	}
	schema, err := decodeExactJSON(data[:ns], MaxSchemaDepth, MaxSchemaRawNodes)
	if err != nil {
		return schemaRejected
	}
	compiled, err := resolveSchema(schema, mode != schemaCompileOutput)
	if err != nil {
		if schemaAdmissionFailure(err).reason == SchemaRegexpInvalidOrUnsupported {
			return schemaRegexpRejected
		}
		return schemaRejected
	}
	if mode != schemaValidateInput {
		return schemaOK
	}
	instance, err := decodeExactJSON(data[ns:], MaxInstanceDepth, MaxInstanceNodes)
	if err != nil {
		return schemaInvalid
	}
	if compiled.Validate(instance) != nil {
		return schemaInvalid
	}
	return schemaOK
}
