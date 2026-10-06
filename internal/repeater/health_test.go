package repeater

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/telemetry"
)

type recordingIncidents struct {
	opened   []uint64
	active   map[string]uint64
	resolved map[string]uint64
}

func newRecordingIncidents() *recordingIncidents {
	return &recordingIncidents{active: make(map[string]uint64), resolved: make(map[string]uint64)}
}

func (r *recordingIncidents) OpenRepeaterIncident(_ string, generation uint64) error {
	r.opened = append(r.opened, generation)
	r.active["R07"] = generation
	return nil
}

func (r *recordingIncidents) ResolveRepeaterIncident(id string, generation uint64) error {
	if active, ok := r.active[id]; ok && generation >= active {
		delete(r.active, id)
		r.resolved[id] = generation
	}
	return nil
}

func (r *recordingIncidents) Active(id string) (uint64, bool) {
	generation, ok := r.active[id]
	return generation, ok
}

func (r *recordingIncidents) LastResolution(id string) uint64 {
	return r.resolved[id]
}

func temperatureSample(sequence, generation uint64, at time.Time, value float64) telemetry.Sample {
	return telemetry.Sample{
		ID: "sample", AssetID: "R07", Kind: telemetry.KindTemperature, Value: value,
		Sequence: sequence, Generation: generation, ObservedAt: at,
	}
}

func newProjectedRepeater(t *testing.T, incidents IncidentSink) (*HealthProjector, *Repeater, time.Time) {
	t.Helper()
	base := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	asset, err := New("R07", base)
	if err != nil {
		t.Fatal(err)
	}
	projector := NewHealthProjector(incidents, 85)
	projector.Register(asset)
	return projector, asset, base
}

// A late frame can carry a larger sequence while describing an earlier
// observation. It must not move the health clock backwards or flip health.
func TestApplySampleRejectsHigherSequenceWithEarlierObservedAt(t *testing.T) {
	incidents := newRecordingIncidents()
	projector, asset, base := newProjectedRepeater(t, incidents)

	if err := projector.ApplySample(context.Background(), temperatureSample(10, 1, base.Add(100*time.Second), 40)); err != nil {
		t.Fatal(err)
	}
	latest := asset.Snapshot()

	// Late/reordered frame: sequence 11 > 10, but observed at t=50s, before
	// the already projected observation at t=100s, and it claims the device is
	// cool again.
	if err := projector.ApplySample(context.Background(), temperatureSample(11, 1, base.Add(50*time.Second), 40)); err != nil {
		t.Fatal(err)
	}

	stale := asset.Snapshot()
	if stale.UpdatedAt != latest.UpdatedAt {
		t.Fatalf("UpdatedAt rewound from %s to %s", latest.UpdatedAt, stale.UpdatedAt)
	}
	if stale.LastSequence != latest.LastSequence {
		t.Fatalf("LastSequence advanced for a stale sample: %d -> %d", latest.LastSequence, stale.LastSequence)
	}
	if stale.TemperatureC != latest.TemperatureC {
		t.Fatalf("temperature overwritten by a stale sample: %v -> %v", latest.TemperatureC, stale.TemperatureC)
	}
	if len(incidents.opened) != 0 {
		t.Fatalf("stale sample opened incidents: %v", incidents.opened)
	}

	// The genuine sequence-11 frame, observed later, must still be accepted
	// (the rejected frame must not have consumed the sequence number).
	if err := projector.ApplySample(context.Background(), temperatureSample(11, 1, base.Add(110*time.Second), 40)); err != nil {
		t.Fatal(err)
	}
	fresh := asset.Snapshot()
	if !fresh.UpdatedAt.Equal(base.Add(110 * time.Second)) {
		t.Fatalf("fresh sample did not advance UpdatedAt: %s", fresh.UpdatedAt)
	}
}

// An out-of-order cool reading must not clear a trip that was latched by a
// later, genuinely newer observation, and must not open a phantom incident.
func TestApplySampleStaleFrameCannotClearLatchedTrip(t *testing.T) {
	incidents := newRecordingIncidents()
	projector, asset, base := newProjectedRepeater(t, incidents)

	if err := projector.ApplySample(context.Background(), temperatureSample(10, 1, base.Add(100*time.Second), 90)); err != nil {
		t.Fatal(err)
	}
	if asset.Snapshot().Health != HealthTripped || len(incidents.opened) != 1 {
		t.Fatalf("expected latched trip and one incident, got %+v openings=%v", asset.Snapshot(), incidents.opened)
	}

	// Earlier observation (t=90s) with a larger sequence claiming 40C.
	if err := projector.ApplySample(context.Background(), temperatureSample(11, 1, base.Add(90*time.Second), 40)); err != nil {
		t.Fatal(err)
	}
	if asset.Snapshot().Health != HealthTripped {
		t.Fatalf("stale cool frame cleared the latched trip: %+v", asset.Snapshot())
	}
	if len(incidents.opened) != 1 {
		t.Fatalf("stale frame changed incidents: %v", incidents.opened)
	}
}

func TestApplySampleRejectsOlderGeneration(t *testing.T) {
	incidents := newRecordingIncidents()
	projector, asset, base := newProjectedRepeater(t, incidents)

	if err := projector.ApplySample(context.Background(), temperatureSample(10, 5, base.Add(100*time.Second), 90)); err != nil {
		t.Fatal(err)
	}
	before := asset.Snapshot()

	// Newer sequence and a later timestamp, but generation predates the trip.
	if err := projector.ApplySample(context.Background(), temperatureSample(11, 4, base.Add(110*time.Second), 40)); err != nil {
		t.Fatal(err)
	}
	if got := asset.Snapshot(); got.LastSequence != before.LastSequence || got.Health != HealthTripped {
		t.Fatalf("stale-generation sample was projected: %+v", got)
	}
}

func TestApplySampleRejectsEqualObservedAtAndAcceptsAfterReset(t *testing.T) {
	incidents := newRecordingIncidents()
	projector, asset, base := newProjectedRepeater(t, incidents)

	tripAt := base.Add(100 * time.Second)
	if err := projector.ApplySample(context.Background(), temperatureSample(10, 1, tripAt, 90)); err != nil {
		t.Fatal(err)
	}
	// Same observation instant must not re-project (no clock rewind, no
	// duplicate processing).
	if err := projector.ApplySample(context.Background(), temperatureSample(11, 1, tripAt, 40)); err != nil {
		t.Fatal(err)
	}
	if asset.Snapshot().Health != HealthTripped {
		t.Fatalf("equal-time frame altered the projection: %+v", asset.Snapshot())
	}

	if err := projector.Reset("R07", 1); err != nil {
		t.Fatal(err)
	}
	if asset.Snapshot().Health != HealthNormal {
		t.Fatalf("reset did not clear trip: %+v", asset.Snapshot())
	}
	// Same generation as the resolved trip with a newer observation applies.
	if err := projector.ApplySample(context.Background(), temperatureSample(12, 1, base.Add(120*time.Second), 40)); err != nil {
		t.Fatal(err)
	}
	if got := asset.Snapshot(); got.Health != HealthNormal || !got.UpdatedAt.Equal(base.Add(120*time.Second)) {
		t.Fatalf("post-reset sample not projected: %+v", got)
	}
}
