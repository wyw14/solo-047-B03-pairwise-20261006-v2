package repeater

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/telemetry"
)

const testTripAtC = 85

func newProjector(t *testing.T, at time.Time) (*HealthProjector, *Repeater, *incident.RepeaterLifecycle) {
	t.Helper()
	lifecycle := incident.NewRepeaterLifecycle()
	projector := NewHealthProjector(lifecycle, testTripAtC)
	target, err := New("R07", at)
	if err != nil {
		t.Fatal(err)
	}
	projector.Register(target)
	return projector, target, lifecycle
}

func temperatureSample(sequence, generation uint64, observedAt time.Time, value float64) telemetry.Sample {
	return telemetry.Sample{
		ID: "sample", AssetID: "R07", Kind: telemetry.KindTemperature,
		Value: value, Sequence: sequence, Generation: generation, ObservedAt: observedAt,
	}
}

func apply(t *testing.T, projector *HealthProjector, sample telemetry.Sample) {
	t.Helper()
	if err := projector.ApplySample(context.Background(), sample); err != nil {
		t.Fatal(err)
	}
}

func TestHealthProjectorRejectsSampleObservedBeforeHealthClock(t *testing.T) {
	base := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	projector, target, _ := newProjector(t, base)

	apply(t, projector, temperatureSample(1, 1, base.Add(time.Hour), 40))
	apply(t, projector, temperatureSample(2, 1, base.Add(30*time.Minute), 50))

	snapshot := target.Snapshot()
	if snapshot.LastSequence != 1 {
		t.Fatalf("stale sample advanced sequence: %+v", snapshot)
	}
	if snapshot.TemperatureC != 40 {
		t.Fatalf("stale sample overwrote temperature: %+v", snapshot)
	}
	if !snapshot.UpdatedAt.Equal(base.Add(time.Hour)) {
		t.Fatalf("health clock rolled back to %s", snapshot.UpdatedAt)
	}
}

func TestHealthProjectorRejectsBackfilledHotSample(t *testing.T) {
	base := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	projector, target, lifecycle := newProjector(t, base)

	apply(t, projector, temperatureSample(1, 1, base.Add(2*time.Hour), 40))
	apply(t, projector, temperatureSample(2, 1, base.Add(time.Hour), 95))

	snapshot := target.Snapshot()
	if snapshot.Health != HealthNormal {
		t.Fatalf("stale hot sample changed health: %+v", snapshot)
	}
	if !snapshot.UpdatedAt.Equal(base.Add(2 * time.Hour)) {
		t.Fatalf("health clock rolled back to %s", snapshot.UpdatedAt)
	}
	if _, active := lifecycle.Active("R07"); active {
		t.Fatal("stale hot sample opened an incident")
	}
}

func TestHealthProjectorAppliesJointlyFreshSample(t *testing.T) {
	base := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	projector, target, lifecycle := newProjector(t, base)

	apply(t, projector, temperatureSample(1, 1, base.Add(time.Hour), 40))
	apply(t, projector, temperatureSample(2, 1, base.Add(2*time.Hour), 95))

	snapshot := target.Snapshot()
	if snapshot.Health != HealthTripped || snapshot.TripGeneration != 1 {
		t.Fatalf("fresh hot sample did not trip: %+v", snapshot)
	}
	if !snapshot.UpdatedAt.Equal(base.Add(2 * time.Hour)) {
		t.Fatalf("health clock did not advance: %s", snapshot.UpdatedAt)
	}
	if generation, active := lifecycle.Active("R07"); !active || generation != 1 {
		t.Fatalf("incident not opened for generation 1: %d %v", generation, active)
	}
}

func TestHealthProjectorKeepsSequenceAndGenerationGuards(t *testing.T) {
	base := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	projector, target, _ := newProjector(t, base)

	apply(t, projector, temperatureSample(1, 2, base.Add(time.Hour), 95))
	apply(t, projector, temperatureSample(2, 1, base.Add(2*time.Hour), 40))
	apply(t, projector, temperatureSample(1, 2, base.Add(3*time.Hour), 50))

	snapshot := target.Snapshot()
	if snapshot.Health != HealthTripped || snapshot.TripGeneration != 2 {
		t.Fatalf("stale generation or sequence changed health: %+v", snapshot)
	}
	if snapshot.LastSequence != 1 || snapshot.TemperatureC != 95 {
		t.Fatalf("stale sample mutated projection: %+v", snapshot)
	}
	if !snapshot.UpdatedAt.Equal(base.Add(time.Hour)) {
		t.Fatalf("health clock moved for stale sample: %s", snapshot.UpdatedAt)
	}
}

func TestHealthProjectorAppliesSampleAtSameObservationTime(t *testing.T) {
	base := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	projector, target, _ := newProjector(t, base)

	observed := base.Add(time.Hour)
	apply(t, projector, temperatureSample(1, 1, observed, 40))
	apply(t, projector, temperatureSample(2, 1, observed, 41))

	snapshot := target.Snapshot()
	if snapshot.LastSequence != 2 || snapshot.TemperatureC != 41 {
		t.Fatalf("same-time sample was rejected: %+v", snapshot)
	}
	if !snapshot.UpdatedAt.Equal(observed) {
		t.Fatalf("health clock moved for same-time sample: %s", snapshot.UpdatedAt)
	}
}

func TestHealthProjectorResetThenFreshSample(t *testing.T) {
	base := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	projector, target, lifecycle := newProjector(t, base)

	apply(t, projector, temperatureSample(1, 1, base.Add(time.Hour), 95))
	if err := projector.Reset("R07", 2); err != nil {
		t.Fatal(err)
	}
	if _, active := lifecycle.Active("R07"); active {
		t.Fatal("incident still active after reset")
	}

	apply(t, projector, temperatureSample(2, 2, base.Add(30*time.Minute), 40))
	snapshot := target.Snapshot()
	if snapshot.LastSequence != 1 {
		t.Fatalf("pre-reset observation advanced sequence: %+v", snapshot)
	}

	apply(t, projector, temperatureSample(3, 2, base.Add(2*time.Hour), 40))
	snapshot = target.Snapshot()
	if snapshot.Health != HealthNormal || snapshot.LastSequence != 3 {
		t.Fatalf("fresh sample after reset not applied: %+v", snapshot)
	}
	if !snapshot.UpdatedAt.Equal(base.Add(2 * time.Hour)) {
		t.Fatalf("health clock did not advance after reset: %s", snapshot.UpdatedAt)
	}
}
