package landing

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/telemetry"
)

func newHealthService(t *testing.T) (*HealthService, *store.HeartbeatStore) {
	t.Helper()
	snapshots := store.NewSnapshotStore(filepath.Join(t.TempDir(), "snapshots"))
	heartbeats := store.NewHeartbeatStore(store.NewCheckpoints(snapshots))
	evaluator, err := telemetry.NewAgeEvaluator(func() time.Time { return time.Now().UTC() }, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return NewHealthService(evaluator, heartbeats), heartbeats
}

// A heartbeat with a larger sequence but an earlier observed time describes the
// past: it must be ignored rather than rewinding LastSeenAt and overwriting the
// persisted checkpoint.
func TestRecordRejectsHigherSequenceWithEarlierObservedAt(t *testing.T) {
	service, heartbeats := newHealthService(t)
	base := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	station, err := NewStation("east", "East Landing", base)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := service.Record(station, 1, base); err != nil {
		t.Fatal(err)
	}
	fresh := base.Add(100 * time.Second)
	if snapshot, err := service.Record(station, 10, fresh); err != nil {
		t.Fatal(err)
	} else if snapshot.LastSeenAt != fresh {
		t.Fatalf("fresh heartbeat not recorded: %+v", snapshot)
	}

	// Late frame: sequence 11 > 10 but observed at t=50s, earlier than t=100s.
	snapshot, err := service.Record(station, 11, base.Add(50*time.Second))
	if err != nil {
		t.Fatalf("stale heartbeat should be silently ignored, got error: %v", err)
	}
	if !snapshot.LastSeenAt.Equal(fresh) || snapshot.Sequence != 10 {
		t.Fatalf("health clock rewound by stale heartbeat: %+v", snapshot)
	}
	if snapshot.State != StationOnline {
		t.Fatalf("stale heartbeat changed station state: %+v", snapshot)
	}

	stored, found, err := heartbeats.Load("east")
	if err != nil || !found {
		t.Fatalf("checkpoint missing: found=%v err=%v", found, err)
	}
	if stored.Sequence != 10 || !stored.SeenAt.Equal(fresh) {
		t.Fatalf("checkpoint overwritten with stale heartbeat: %+v", stored)
	}

	// The real sequence-11 heartbeat observed later must still be accepted.
	later := base.Add(110 * time.Second)
	if snapshot, err := service.Record(station, 11, later); err != nil {
		t.Fatal(err)
	} else if !snapshot.LastSeenAt.Equal(later) {
		t.Fatalf("genuine later heartbeat rejected: %+v", snapshot)
	}
}

// Once a stale frame is ignored, age evaluation must run off the newest real
// observation and eventually flip the station offline.
func TestAgeEvaluationUsesNewestObservation(t *testing.T) {
	base := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	now := base
	evaluator, err := telemetry.NewAgeEvaluator(func() time.Time { return now }, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	service := NewHealthService(evaluator, nil)
	station, err := NewStation("east", "East Landing", base)
	if err != nil {
		t.Fatal(err)
	}

	if _, accepted := station.Heartbeat(10, base.Add(100*time.Second)); !accepted {
		t.Fatal("fresh heartbeat rejected")
	}
	if _, accepted := station.Heartbeat(11, base.Add(50*time.Second)); accepted {
		t.Fatal("stale heartbeat accepted")
	}

	now = base.Add(105 * time.Second)
	if got := service.Evaluate(station, now); got.State != StationOnline {
		t.Fatalf("station offline while still fresh: %+v", got)
	}
	now = base.Add(200 * time.Second)
	if got := service.Evaluate(station, now); got.State != StationOffline {
		t.Fatalf("age evaluation delayed by stale heartbeat: %+v", got)
	}
}

// A persisted heartbeat is authoritative during recovery even though its
// observed time predates the freshly constructed station.
func TestRecoverRestoresOlderPersistedHeartbeat(t *testing.T) {
	service, _ := newHealthService(t)
	base := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	seed, err := NewStation("east", "East Landing", base)
	if err != nil {
		t.Fatal(err)
	}
	seenAt := base.Add(60 * time.Second)
	if _, err := service.Record(seed, 7, seenAt); err != nil {
		t.Fatal(err)
	}

	// Restart: a new station constructed later recovers the persisted clock.
	restarted, err := NewStation("east", "East Landing", base.Add(120*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := service.Recover(restarted)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Sequence != 7 || !snapshot.LastSeenAt.Equal(seenAt) {
		t.Fatalf("recovery did not restore persisted heartbeat: %+v", snapshot)
	}
}
