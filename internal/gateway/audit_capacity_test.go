package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

func reserveFixture(t *testing.T) (*Gateway, Config, PersistenceOptions) {
	t.Helper()
	cfg := maintenanceConfig()
	// JSON keeps the red test runnable against the previous AuditConfig shape.
	if err := json.Unmarshal([]byte(`{"capacity":6,"restriction_reserve":2}`), &cfg.Audit); err != nil {
		t.Fatal(err)
	}
	opts := maintenanceOptions(t)
	g, err := NewPersistent(cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	opts.Initialize = false
	return g, cfg, opts
}
func statusFields(t *testing.T, g *Gateway) map[string]any {
	t.Helper()
	data, _ := json.Marshal(g.AuditStatus())
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func TestAuditCapacityAccountsReservedTerminalAndRestrictionReserve(t *testing.T) {
	g, cfg, opts := reserveFixture(t)
	archiveFill(t, g, 2)
	g.mu.Lock()
	event, err := g.startAuditLocked("active", "shared", "shared__echo", AuditActionCall)
	g.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	fields := statusFields(t, g)
	for key, want := range map[string]float64{"retained": 3, "reserved": 1, "remaining": 2, "regular_remaining": 0, "restriction_reserve": 2} {
		if fields[key] != want {
			t.Fatalf("%s=%v want %v", key, fields[key], want)
		}
	}
	if fields["operation_ready"] != false || fields["restriction_ready"] != true {
		t.Fatalf("wrong readiness: %+v", fields)
	}
	if _, err := g.SetPolicy(maintenanceIdentity("active"), 1, []string{"shared__echo"}); !errors.Is(err, ErrAuditFull) {
		t.Fatal("nonrestrictive mutation consumed reserve", err)
	}
	// A committed restrictive action cancels the admitted operation, not before.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g.mu.Lock()
	g.operations[99] = admittedOperation{user: "active", system: "shared", cancel: cancel}
	g.mu.Unlock()
	info, err := g.RevokeBinding(maintenanceIdentity("active"), "shared", 1)
	if err != nil || info.State != BindingRevoked || ctx.Err() == nil {
		t.Fatal("reserved revoke did not commit/cancel", info, err)
	}
	if _, err := g.RevokeBinding(maintenanceIdentity("active"), "shared", info.Generation); !errors.Is(err, ErrAuditFull) {
		t.Fatal("repeat revoke consumed reserve", err)
	}
	if _, err := g.SetPolicy(maintenanceIdentity("active"), 1, nil); err != nil {
		t.Fatal("strict reduction could not use final reserve", err)
	}
	if _, err := g.DisablePrincipal(maintenanceIdentity("expired"), 1); !errors.Is(err, ErrAuditFull) {
		t.Fatal("exhausted reserve silently bypassed capacity", err)
	}
	g.mu.Lock()
	delete(g.operations, 99)
	g.auditPending--
	event.Phase = AuditPhaseTerminal
	event.Result = AuditResultCanceled
	err = g.appendAuditLocked(event)
	g.mu.Unlock()
	if err != nil {
		t.Fatal("restriction stole reserved terminal", err)
	}
	if g.AuditStatus().LastSequence != 6 {
		t.Fatal("audit silently lost events")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewPersistent(cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	b, err := reopened.Binding(maintenanceIdentity("active"), "shared")
	if err != nil || b.State != BindingRevoked {
		t.Fatal("revoke did not survive reopen", b, err)
	}
	if len(reopened.principals["active"].allowed) != 0 {
		t.Fatal("reduced policy did not survive reopen")
	}
}

func TestAuditCapacityStatusUsesArchiveBase(t *testing.T) {
	g, _, _ := archiveFixture(t, 3)
	archiveFill(t, g, 3)
	if _, err := g.ArchiveAudit(filepath.Join(t.TempDir(), "archive.json"), 3); err != nil {
		t.Fatal(err)
	}
	fields := statusFields(t, g)
	if fields["retained"] != float64(1) || fields["remaining"] != float64(2) || fields["last_sequence"] != float64(4) {
		t.Fatalf("archive capacity incorrectly used cumulative sequence: %+v", fields)
	}
	if fields["operation_ready"] != false {
		t.Fatal("maintenance marked ready for business")
	}
}

func TestAuditRestrictionReserveCannotBeUsedByRegularMutations(t *testing.T) {
	g, _, _ := reserveFixture(t)
	archiveFill(t, g, 4)
	if _, err := g.SetPolicy(maintenanceIdentity("active"), 1, []string{"shared__echo"}); !errors.Is(err, ErrAuditFull) {
		t.Fatal("ordinary policy update consumed reserved capacity", err)
	}
}

func TestAuditReservePersistenceFailureAndUnknownCommit(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "rollback", true: "ack_lost"}[unknown], func(t *testing.T) {
			g, cfg, opts := reserveFixture(t)
			archiveFill(t, g, 4)
			if unknown {
				maintenanceLoseCommitAck(t, g, opts.Path)
			} else {
				if _, err := g.store.conn.ExecContext(context.Background(), "CREATE TRIGGER audit_reserve_fail BEFORE UPDATE ON meta BEGIN SELECT RAISE(ABORT, 'synthetic reserve failure'); END"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := g.RevokeBinding(maintenanceIdentity("active"), "shared", 1); !errors.Is(err, ErrStorage) {
				t.Fatal("uncertain/failed revoke reported success", err)
			}
			if !g.closed || !g.auditFault || g.principals["active"].bindings["shared"].info.State != BindingActive {
				t.Fatal("failure published unacknowledged state or stayed open")
			}
			if status := g.AuditStatus(); status.OperationReady || status.RestrictionReady {
				t.Fatal("failed storage reported ready")
			}
			_ = g.Close()
			if !unknown {
				maintenanceSQL(t, opts.Path, "DROP TRIGGER audit_reserve_fail")
			}
			reopened, err := NewPersistent(cfg, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			b, err := reopened.Binding(maintenanceIdentity("active"), "shared")
			if err != nil {
				t.Fatal(err)
			}
			want := BindingActive
			sequence := uint64(4)
			if unknown {
				want = BindingRevoked
				sequence = 5
			}
			if b.State != want || reopened.AuditStatus().LastSequence != sequence {
				t.Fatal("state/audit not atomic after reopen", b, reopened.AuditStatus())
			}
		})
	}
}

func TestAuditReserveStrictMutationClassification(t *testing.T) {
	for _, mode := range []string{"disable", "entry", "duplicate_policy", "expand"} {
		t.Run(mode, func(t *testing.T) {
			g, cfg, opts := reserveFixture(t)
			archiveFill(t, g, 4)
			switch mode {
			case "disable":
				info, err := g.DisablePrincipal(maintenanceIdentity("active"), 1)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = g.DisablePrincipal(maintenanceIdentity("active"), info.Version); !errors.Is(err, ErrAuditFull) {
					t.Fatal("repeat disable spent reserve", err)
				}
			case "entry":
				_ = g.Close()
				m, err := OpenMaintenance(cfg, opts)
				if err != nil {
					t.Fatal(err)
				}
				defer m.Close()
				info, err := m.RevokeEntry(maintenanceIdentity("active"), 1)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = m.RevokeEntry(maintenanceIdentity("active"), info.Generation); !errors.Is(err, ErrAuditFull) {
					t.Fatal("repeat entry revoke spent reserve", err)
				}
			case "duplicate_policy":
				if _, err := g.SetPolicy(maintenanceIdentity("active"), 1, []string{"shared__echo", "shared__echo"}); !errors.Is(err, ErrAuditFull) {
					t.Fatal("duplicate-only policy spent reserve", err)
				}
			case "expand":
				if _, err := g.SetPolicy(maintenanceIdentity("active"), 1, []string{"shared__echo", "shared__new"}); !errors.Is(err, ErrAuditFull) {
					t.Fatal("expansion spent reserve", err)
				}
			}
		})
	}
}

func TestAuditCapacityMemoryRingAndReserveConfiguration(t *testing.T) {
	cfg := maintenanceConfig()
	cfg.Audit = AuditConfig{Capacity: 2}
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	archiveFill(t, g, 3)
	status := g.AuditStatus()
	if status.Persistent || status.Retained != 2 || status.Remaining != 0 || !status.OperationReady || !status.RestrictionReady || status.Dropped != 1 {
		t.Fatal("ring eviction confused with durable capacity", status)
	}
	for _, reserve := range []int{-1, 1, 2, 3} {
		cfg.Audit = AuditConfig{Capacity: 2, RestrictionReserve: reserve}
		if other, err := New(cfg); err == nil {
			other.Close()
			t.Fatal("invalid reserve accepted", reserve)
		}
	}
}

func TestAuditReserveOnlineEntryRevoke(t *testing.T) {
	g, _, _ := reserveFixture(t)
	archiveFill(t, g, 4)
	h := adminTestHandler(t, g)
	identity := maintenanceIdentity("active")
	req := lifecyclePreview(t, h, lifecycleRequest{Action: "revoke_entry", Tenant: identity.Tenant, Principal: identity.Principal, Expected: 1})
	if response := lifecyclePost(h, req); response.Code != 200 {
		t.Fatalf("authorized online revoke cannot use reserve: %d %s", response.Code, response.Body)
	}
	if !g.principals["active"].entry.Revoked || g.AuditStatus().LastSequence != 5 {
		t.Fatal("online revoke not atomically recorded")
	}
	if response := lifecyclePost(h, req); response.Code != 409 || g.AuditStatus().LastSequence != 5 {
		t.Fatal("replayed ticket spent reserve")
	}
}
