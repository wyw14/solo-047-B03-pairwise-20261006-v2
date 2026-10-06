package landing

import (
	"errors"
	"sync"
	"time"
)

type StationState string

const (
	StationOnline   StationState = "online"
	StationDegraded StationState = "degraded"
	StationOffline  StationState = "offline"
)

type StationSnapshot struct {
	ID         string       `json:"id"`
	Name       string       `json:"name"`
	State      StationState `json:"state"`
	LastSeenAt time.Time    `json:"last_seen_at"`
	Sequence   uint64       `json:"sequence"`
	UpdatedAt  time.Time    `json:"updated_at"`
}

type Station struct {
	mu       sync.RWMutex
	snapshot StationSnapshot
}

func NewStation(id, name string, at time.Time) (*Station, error) {
	if id == "" || name == "" {
		return nil, errors.New("station identity is required")
	}
	return &Station{snapshot: StationSnapshot{
		ID: id, Name: name, State: StationOnline, LastSeenAt: at.UTC(), UpdatedAt: at.UTC(),
	}}, nil
}

// Heartbeat applies a fresh station heartbeat. It returns the resulting
// snapshot and whether the heartbeat was accepted.
func (s *Station) Heartbeat(sequence uint64, at time.Time) (StationSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	at = at.UTC()
	// Joint freshness guard: a heartbeat must advance both the sequence and
	// the observed time. A reordered or retransmitted frame can carry a larger
	// sequence while describing an earlier instant; accepting it would move
	// LastSeenAt backwards, so the age evaluator would see a fresh heartbeat
	// long after the station actually went silent.
	if sequence <= s.snapshot.Sequence || at.Before(s.snapshot.LastSeenAt) {
		return s.snapshot, false
	}
	s.snapshot.Sequence = sequence
	s.snapshot.LastSeenAt = at
	s.snapshot.State = StationOnline
	s.snapshot.UpdatedAt = at
	return s.snapshot, true
}

// restore rebuilds the station from a persisted heartbeat. Persisted state is
// authoritative even though its observed time predates the freshly constructed
// station, so it bypasses the heartbeat freshness guard.
func (s *Station) restore(sequence uint64, at time.Time) StationSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.Sequence = sequence
	s.snapshot.LastSeenAt = at.UTC()
	return s.snapshot
}

func (s *Station) SetState(state StationState, at time.Time) StationSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.State = state
	s.snapshot.UpdatedAt = at.UTC()
	return s.snapshot
}

func (s *Station) Snapshot() StationSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot
}
