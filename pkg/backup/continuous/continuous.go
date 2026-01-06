// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package continuous implements continuous backup (PITR logging) for CockroachDB.
// Continuous backup captures rangefeed events and writes them to time-ordered
// SST files alongside RESOLVED checkpoints, enabling point-in-time recovery
// between incremental backups.
package continuous

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/continuous/continuouspb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Resume is the main entry point for continuous backup jobs.
// It is called from backup.Resume() when the job details indicate this is a
// continuous (PITR logging) job rather than a regular backup job.
func Resume(
	ctx context.Context,
	job *jobs.Job,
	execCtx sql.JobExecContext,
	details jobspb.BackupDetails,
) error {
	if !details.Continuous {
		return errors.AssertionFailedf("Resume called for non-continuous backup job")
	}

	startTime := details.EndTime // Log starts at the backup's end time
	logFolder := details.ContinuousLogFolder

	if logFolder == "" {
		return errors.Errorf("continuous backup job missing log folder path")
	}

	// Get tenant spans to watch.
	// For now, we watch the entire tenant keyspace.
	// TODO: Get the actual tenant spans from the job details.
	tenantSpans := []roachpb.Span{
		{
			Key:    keys.TenantTableDataMin,
			EndKey: keys.TenantTableDataMax,
		},
	}

	// Create the component implementations.
	persister := newJobInfoPersister(job)
	checkpointWriter := newCloudCheckpointWriter(logFolder, execCtx)
	stopChecker := newBackupIndexStopChecker(details.CollectionURI, startTime, execCtx)

	// Try to load persisted frontier for resume.
	spanStarts := loadResumeState(ctx, persister, tenantSpans, startTime)

	// Create channels for shard communication.
	flushReportCh := make(chan flushReport, 100)
	frontierReportCh := make(chan frontierReport, 100)

	// Create the coordinator with injected dependencies.
	coord := newCoordinator(coordinatorConfig{
		persister:        persister,
		checkpointWriter: checkpointWriter,
		stopChecker:      stopChecker,
		settings:         execCtx.ExecCfg().Settings,
		spanStarts:       spanStarts,
		flushReportCh:    flushReportCh,
		frontierReportCh: frontierReportCh,
	})

	// Create shards.
	stopper := execCtx.ExecCfg().DistSQLSrv.Stopper
	numShards := 1 // Start with single shard; expand later
	shards := make([]shardRunner, 0, numShards)
	for i := 0; i < numShards; i++ {
		shard, err := newShard(shardConfig{
			id:               i,
			spans:            spanStarts,
			logFolder:        logFolder,
			execCtx:          execCtx,
			stopper:          stopper,
			flushReportCh:    flushReportCh,
			frontierReportCh: frontierReportCh,
		})
		if err != nil {
			return errors.Wrapf(err, "creating shard %d", i)
		}
		shards = append(shards, shard)
	}

	log.Dev.Infof(ctx, "starting continuous backup from %s to folder %s with %d shards",
		startTime, logFolder, len(shards))

	return coord.run(ctx, shards)
}

// loadResumeState attempts to load persisted frontier state for resume.
// Returns per-span start timestamps. If no persisted state is found,
// all spans start from startTime.
func loadResumeState(
	ctx context.Context,
	persister frontierPersister,
	tenantSpans []roachpb.Span,
	startTime hlc.Timestamp,
) []spanWithStartTS {
	// Try to load persisted frontier.
	persistedFrontier, found, err := persister.Load(ctx)
	if err != nil {
		log.Dev.Warningf(ctx, "failed to load persisted frontier: %v", err)
		found = false
	}

	// Build per-span start times. If we have a persisted frontier, use
	// max(frontier[span], startTime) for each span, since the RESOLVED
	// timestamp is a lower bound guarantee.
	spanStarts := make([]spanWithStartTS, 0, len(tenantSpans))

	if found && persistedFrontier != nil {
		lastResolvedTS := persistedFrontier.Frontier()
		for _, sp := range tenantSpans {
			// Find the frontier timestamp for this span by checking entries.
			// The frontier may have finer granularity than our tenant spans.
			spanTS := startTime
			for entrySpan, entryTS := range persistedFrontier.Entries() {
				if entrySpan.Overlaps(sp) && spanTS.Less(entryTS) {
					spanTS = entryTS
				}
			}
			// Use max of persisted frontier and startTime.
			if startTime.Less(spanTS) {
				spanStarts = append(spanStarts, spanWithStartTS{Span: sp, StartTS: spanTS})
			} else {
				spanStarts = append(spanStarts, spanWithStartTS{Span: sp, StartTS: startTime})
			}
		}
		log.Dev.Infof(ctx, "resuming continuous backup with persisted frontier at %s", lastResolvedTS)
	} else {
		// No persisted frontier - start all spans from backup end time.
		for _, sp := range tenantSpans {
			spanStarts = append(spanStarts, spanWithStartTS{Span: sp, StartTS: startTime})
		}
		log.Dev.Infof(ctx, "starting continuous backup from %s (no persisted frontier)", startTime)
	}

	return spanStarts
}

// coordinatorConfig contains the configuration for creating a coordinator.
type coordinatorConfig struct {
	persister        frontierPersister
	checkpointWriter checkpointWriter
	stopChecker      stopChecker
	settings         *cluster.Settings
	spanStarts       []spanWithStartTS
	flushReportCh    <-chan flushReport
	frontierReportCh <-chan frontierReport
}

// coordinator manages the continuous backup process.
// It maintains a unified span frontier, writes RESOLVED checkpoints, and
// persists the frontier to job storage when the system is lagging.
//
// The coordinator is designed for testability - all external dependencies
// are injected via interfaces.
type coordinator struct {
	// Injected dependencies.
	persister        frontierPersister
	checkpointWriter checkpointWriter
	stopChecker      stopChecker
	settings         *cluster.Settings

	// Configuration.
	spanStarts []spanWithStartTS

	// Shard communication channels.
	flushReportCh    <-chan flushReport
	frontierReportCh <-chan frontierReport

	// Unified span frontier tracking. Updated from shard frontier reports.
	frontier span.Frontier

	// State tracking.
	globalResolved      hlc.Timestamp // min(all frontier entries)
	pendingFileIDs      []int64
	lastCheckpoint      time.Time
	lastFrontierPersist time.Time
}

// newCoordinator creates a new coordinator with the given configuration.
func newCoordinator(cfg coordinatorConfig) *coordinator {
	return &coordinator{
		persister:        cfg.persister,
		checkpointWriter: cfg.checkpointWriter,
		stopChecker:      cfg.stopChecker,
		settings:         cfg.settings,
		spanStarts:       cfg.spanStarts,
		flushReportCh:    cfg.flushReportCh,
		frontierReportCh: cfg.frontierReportCh,
		pendingFileIDs:   make([]int64, 0),
	}
}

// run executes the coordinator loop.
func (c *coordinator) run(ctx context.Context, shards []shardRunner) error {
	// Initialize the unified frontier from span starts.
	var err error
	c.frontier, err = span.MakeFrontier()
	if err != nil {
		return errors.Wrap(err, "creating frontier")
	}
	for _, ss := range c.spanStarts {
		if err := c.frontier.AddSpansAt(ss.StartTS, ss.Span); err != nil {
			return errors.Wrapf(err, "adding span %s to frontier", ss.Span)
		}
	}
	c.globalResolved = c.frontier.Frontier()

	// Start shard processors.
	g := ctxgroup.WithContext(ctx)
	for _, shard := range shards {
		s := shard // capture for closure
		g.GoCtx(func(ctx context.Context) error {
			return s.Run(ctx)
		})
	}

	// Coordinator loop processes shard reports and writes checkpoints.
	g.GoCtx(func(ctx context.Context) error {
		return c.processLoop(ctx)
	})

	return g.Wait()
}

// processLoop is the main coordinator processing loop.
func (c *coordinator) processLoop(ctx context.Context) error {
	ticker := time.NewTicker(ResolvedCheckpointInterval.Get(&c.settings.SV))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Graceful shutdown - context was canceled.
			return ctx.Err()

		case report := <-c.flushReportCh:
			// Fast path: shard flushed a data file, update pending files list.
			// These reports arrive every 5-10s from each shard.
			if err := c.handleFlushReport(report); err != nil {
				return errors.Wrap(err, "handling flush report")
			}

		case report := <-c.frontierReportCh:
			// Slow path: shard sent full span frontier for precise resume tracking.
			// These reports arrive every 30s from each shard.
			c.handleFrontierReport(report)

		case <-ticker.C:
			// Periodic maintenance: write checkpoints, persist frontier, check stop.
			if err := c.onTick(ctx); err != nil {
				return err
			}
		}
	}
}

// onTick handles periodic work: checkpoints, persistence, and stop checks.
func (c *coordinator) onTick(ctx context.Context) error {
	// Write a checkpoint if conditions are met.
	if err := c.maybeWriteCheckpoint(ctx); err != nil {
		return errors.Wrap(err, "writing checkpoint")
	}

	// Persist frontier to job storage if we're lagging.
	// Note: We log but don't fail on persist errors since the job can still
	// make progress. The frontier will be re-persisted on the next tick, and
	// in the worst case we can resume from the last RESOLVED checkpoint.
	if err := c.maybePersistFrontier(ctx); err != nil {
		log.Dev.Warningf(ctx, "failed to persist frontier (non-fatal): %v", err)
	}

	// Check if we should stop (newer backup exists or timeout).
	shouldStop, err := c.stopChecker.ShouldStop(ctx)
	if err != nil {
		return errors.Wrap(err, "checking stop condition")
	}
	if shouldStop {
		log.Dev.Infof(ctx, "continuous backup stopping: newer backup detected or timeout")
		return nil
	}

	return nil
}

// handleFlushReport processes a flush report from a shard (fast path).
func (c *coordinator) handleFlushReport(report flushReport) error {
	if report.err != nil {
		return errors.Wrapf(report.err, "shard %d error", report.shardID)
	}

	// Add file ID to pending list if one was written.
	if report.fileID != 0 {
		c.pendingFileIDs = append(c.pendingFileIDs, report.fileID)
	}

	// Update global resolved from the shard's frontier timestamp.
	// This is a fast-path update; full frontier comes via frontierReportCh.
	if !report.frontierTS.IsEmpty() && report.frontierTS.Less(c.globalResolved) {
		c.globalResolved = report.frontierTS
	}

	return nil
}

// handleFrontierReport processes a full frontier report from a shard.
func (c *coordinator) handleFrontierReport(report frontierReport) {
	// Merge the shard's resolved spans into our unified frontier.
	for _, rs := range report.resolvedSpans {
		if _, err := c.frontier.Forward(rs.Span, rs.Timestamp); err != nil {
			log.Dev.Warningf(context.Background(),
				"shard %d: failed to forward frontier for span %s: %v",
				report.shardID, rs.Span, err)
		}
	}

	// Update global resolved from the unified frontier.
	c.globalResolved = c.frontier.Frontier()
}

// maybePersistFrontier persists the frontier to job storage if we're lagging.
func (c *coordinator) maybePersistFrontier(ctx context.Context) error {
	// Check how far behind we are.
	lag := timeutil.Since(c.globalResolved.GoTime())
	if lag < FrontierPersistLag.Get(&c.settings.SV) {
		return nil
	}

	// Rate-limit persistence.
	if timeutil.Since(c.lastFrontierPersist) < FrontierSyncInterval.Get(&c.settings.SV) {
		return nil
	}

	if err := c.persister.Store(ctx, c.frontier); err != nil {
		return errors.Wrap(err, "storing frontier")
	}

	log.Dev.Infof(ctx, "persisted frontier at %s (lag: %s)", c.globalResolved, lag)
	c.lastFrontierPersist = timeutil.Now()
	return nil
}

// maybeWriteCheckpoint writes a RESOLVED checkpoint if conditions are met.
func (c *coordinator) maybeWriteCheckpoint(ctx context.Context) error {
	// Check if enough time has passed since last checkpoint.
	if timeutil.Since(c.lastCheckpoint) < ResolvedCheckpointInterval.Get(&c.settings.SV) {
		return nil
	}

	// Skip if no pending files.
	if len(c.pendingFileIDs) == 0 {
		c.lastCheckpoint = timeutil.Now()
		return nil
	}

	checkpoint := &continuouspb.ResolvedCheckpoint{
		ResolvedTs: c.globalResolved,
		FileIds:    c.pendingFileIDs,
	}

	if err := c.checkpointWriter.WriteCheckpoint(ctx, checkpoint); err != nil {
		return err
	}

	log.Dev.Infof(ctx, "wrote RESOLVED checkpoint at %s with %d files",
		c.globalResolved, len(c.pendingFileIDs))

	c.pendingFileIDs = make([]int64, 0)
	c.lastCheckpoint = timeutil.Now()
	return nil
}
