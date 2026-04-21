// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// progressCheckpointInterval is how often the progress-updater
// goroutine wakes to publish the most-recently-closed tick's end
// time to the job's HighWater. Long enough to keep system.jobs
// write traffic minimal, short enough that operators see the log's
// position move within a tick or two of when it actually advances.
const progressCheckpointInterval = 30 * time.Second

// runProgressUpdater periodically copies the manager's lastClosed
// value into the job's Progress.HighWater field. It returns when
// ctx is cancelled (job pause / cancel / fail / completion).
//
// TODO(revlog): v1 only persists the most-recently-closed tick
// time. We do NOT yet persist:
//   - the in-flight aggregate frontier (which may be ahead of
//     lastClosed if some ticks have data flushed but haven't been
//     sealed yet), or
//   - the per-open-tick file lists (file_id + flushorder for
//     files that are durable on S3 but not yet listed in a close
//     marker).
//
// Consequence: on resume, the writer restarts from the HighWater
// (= last closed tick end), re-derives every rangefeed event
// between then and the in-flight frontier, and re-flushes them
// into new randomly-named files in the still-open ticks. The
// duplicates are inert — the eventual close marker only references
// the new file IDs — but it is wasteful work.
//
// See RFC §1 Implementation sketch / Progress checkpointing for
// the full design (frontier + open-tick file lists persisted; on
// resume, hand each producer a starting flushorder = max(persisted)
// + 1 per open tick).
func runProgressUpdater(ctx context.Context, job *jobs.Job, manager *TickManager) error {
	ticker := time.NewTicker(progressCheckpointInterval)
	defer ticker.Stop()

	// lastWritten remembers what we most recently persisted so we can
	// skip the system.jobs write when nothing has advanced.
	var lastWritten hlc.Timestamp
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			closed := manager.LastClosed()
			if !lastWritten.Less(closed) {
				continue
			}
			if err := writeHighWater(ctx, job, closed); err != nil {
				// Don't fail the job on a transient progress write
				// failure; log and try again on the next tick. The
				// HighWater is informational — losing one update
				// just means operators see a slightly stale value
				// until the next successful write.
				log.Dev.Warningf(ctx, "revlogjob: persisting progress high-water %s: %v", closed, err)
				continue
			}
			lastWritten = closed
		}
	}
}

// writeHighWater persists hw into the job's Progress.HighWater
// field via the standard Update path.
func writeHighWater(ctx context.Context, job *jobs.Job, hw hlc.Timestamp) error {
	return job.NoTxn().Update(ctx, func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}
		md.Progress.Progress = &jobspb.Progress_HighWater{HighWater: &hw}
		ju.UpdateProgress(md.Progress)
		return nil
	})
}
