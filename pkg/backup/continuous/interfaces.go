// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package continuous

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/backup/continuous/continuouspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
)

// frontierPersister abstracts durable storage of the span frontier.
// This enables clean separation of concerns and testability.
type frontierPersister interface {
	// Load loads the span frontier from persistent storage.
	// Returns the frontier and whether data was found.
	Load(ctx context.Context) (span.Frontier, bool, error)

	// Store persists the span frontier to durable storage.
	Store(ctx context.Context, frontier span.Frontier) error
}

// checkpointWriter abstracts writing RESOLVED checkpoint files to storage.
type checkpointWriter interface {
	// WriteCheckpoint writes a RESOLVED checkpoint file.
	WriteCheckpoint(ctx context.Context, checkpoint *continuouspb.ResolvedCheckpoint) error
}

// stopChecker determines when the continuous backup job should stop.
type stopChecker interface {
	// ShouldStop checks if the job should terminate.
	// Returns true if a newer backup exists or max duration exceeded.
	ShouldStop(ctx context.Context) (bool, error)
}

// shardRunner represents a shard that can be run to capture rangefeed events.
type shardRunner interface {
	// Run starts the shard's rangefeed and processing loop.
	// It blocks until the context is canceled or an error occurs.
	Run(ctx context.Context) error
}

// flushReport is sent from a shard to the coordinator after each flush.
// This is the "fast path" update containing just the min timestamp.
type flushReport struct {
	shardID    int
	fileID     int64         // 0 if no file was written (empty buffer)
	minTS      hlc.Timestamp // Min timestamp in the flushed file
	frontierTS hlc.Timestamp // Shard's current min frontier timestamp
	err        error
}

// frontierReport is sent from a shard to the coordinator during periodic frontier sync.
// This contains the full span-level frontier for precise resume.
type frontierReport struct {
	shardID       int
	resolvedSpans []jobspb.ResolvedSpan // Full span frontier from rangefeed
}

// spanWithStartTS pairs a span with its resume start timestamp.
type spanWithStartTS struct {
	Span    roachpb.Span
	StartTS hlc.Timestamp
}

// logReplayer applies continuous backup log events to restore data to a
// point-in-time. Implementations may use different strategies for handling
// scale - from simple in-memory deduplication to distributed merge.
type logReplayer interface {
	// Replay applies log events from the given files to reach the target state.
	// It reads events from fileIDs, filters to (startTS, endTS], deduplicates
	// by keeping the latest event per key, and applies the result.
	//
	// Parameters:
	//   - fileIDs: IDs of data files to read (each maps to data/<id>.sst)
	//   - startTS: exclusive lower bound (backup end time)
	//   - endTS: inclusive upper bound (target restore time)
	//
	// Returns statistics about the replay operation.
	Replay(ctx context.Context, fileIDs []int64, startTS, endTS hlc.Timestamp) (*ReplayStats, error)
}
