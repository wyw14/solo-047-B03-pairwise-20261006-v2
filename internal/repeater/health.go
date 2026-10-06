package repeater

import (
	"context"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/telemetry"
)

type IncidentSink interface {
	OpenRepeaterIncident(repeaterID string, generation uint64) error
	ResolveRepeaterIncident(repeaterID string, generation uint64) error
	Active(repeaterID string) (uint64, bool)
	LastResolution(repeaterID string) uint64
}

// trackedRepeater pairs a repeater with the watermark of the newest
// temperature observation already projected. The watermark is kept separate
// from Snapshot.UpdatedAt, which other controllers (ramp, reset) also stamp:
// freshness for temperature must be judged against temperature only.
type trackedRepeater struct {
	repeater   *Repeater
	lastTempAt time.Time
}

type HealthProjector struct {
	mu        sync.Mutex
	repeaters map[string]*trackedRepeater
	incidents IncidentSink
	tripAtC   float64
}

func NewHealthProjector(incidents IncidentSink, tripAtC float64) *HealthProjector {
	return &HealthProjector{repeaters: make(map[string]*trackedRepeater), incidents: incidents, tripAtC: tripAtC}
}

func (p *HealthProjector) Register(repeater *Repeater) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.repeaters[repeater.Snapshot().ID] = &trackedRepeater{repeater: repeater}
}

func (p *HealthProjector) ApplySample(_ context.Context, sample telemetry.Sample) error {
	if sample.Kind != telemetry.KindTemperature {
		return nil
	}
	var openID string
	var openGeneration uint64
	p.mu.Lock()
	if tracked := p.repeaters[sample.AssetID]; tracked != nil && sampleAdvances(tracked.repeater.Snapshot(), tracked.lastTempAt, sample) {
		observedAt := sample.ObservedAt.UTC()
		before := tracked.repeater.Snapshot()
		after := tracked.repeater.update(func(current *Snapshot) {
			current.LastSequence = sample.Sequence
			current.TemperatureC = sample.Value
			current.UpdatedAt = observedAt
			if sample.Value >= p.tripAtC {
				current.Health = HealthTripped
				current.TripGeneration = sample.Generation
			} else if current.Health != HealthTripped {
				current.Health = HealthNormal
			}
		})
		tracked.lastTempAt = observedAt
		if before.Health != HealthTripped && after.Health == HealthTripped {
			openID, openGeneration = after.ID, after.TripGeneration
		}
	}
	p.mu.Unlock()
	if openID != "" && p.incidents != nil {
		return p.incidents.OpenRepeaterIncident(openID, openGeneration)
	}
	return nil
}

func (p *HealthProjector) Reset(repeaterID string, generation uint64) error {
	p.mu.Lock()
	tracked := p.repeaters[repeaterID]
	p.mu.Unlock()
	if tracked == nil {
		return nil
	}
	if active, ok := p.incidents.Active(repeaterID); ok && generation < active {
		return nil
	}
	tracked.repeater.update(func(current *Snapshot) {
		if generation >= current.TripGeneration {
			current.Health = HealthNormal
			current.TripGeneration = generation
		}
	})
	if p.incidents != nil {
		if err := p.incidents.ResolveRepeaterIncident(repeaterID, generation); err != nil {
			return err
		}
		_ = p.incidents.LastResolution(repeaterID)
	}
	return nil
}

// sampleAdvances is the joint freshness guard for a temperature sample:
// sequence, observed time and generation must all be newer than what the
// projection already holds. Sequence alone is assigned at the source and does
// not survive reordering or retransmission — a late frame with a larger
// sequence but an earlier ObservedAt describes the past. Applying it would
// move the health clock backwards, so the station page would show a fresh
// recovery and stale-health ageing would be wrongly delayed. The observed-time
// check only kicks in after the first temperature sample, when the watermark
// becomes non-zero.
func sampleAdvances(current Snapshot, lastTempAt time.Time, sample telemetry.Sample) bool {
	if sample.Sequence <= current.LastSequence {
		return false
	}
	if sample.Generation < current.TripGeneration {
		return false
	}
	if !lastTempAt.IsZero() && !sample.ObservedAt.After(lastTempAt) {
		return false
	}
	return true
}
