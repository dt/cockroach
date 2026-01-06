// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package continuous

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/objstorage"
)

// shard processes rangefeed events for a subset of the keyspace.
// It buffers events, periodically flushes them to SST files, and reports
// progress to the coordinator.
type shard struct {
	id        int
	spans     []spanWithStartTS
	logFolder string
	execCtx   sql.JobExecContext
	settings  *cluster.Settings
	stopper   *stop.Stopper

	// Channels for reporting to coordinator
	flushReportCh    chan<- flushReport
	frontierReportCh chan<- frontierReport

	// State protected by mu
	mu struct {
		sync.Mutex
		frontier   span.Frontier // Span-level frontier from rangefeed
		buffer     []bufferedEvent
		bufferSize int64
		minTS      hlc.Timestamp
		maxTS      hlc.Timestamp
	}

	// Timing
	lastFlush        time.Time
	lastFrontierSync time.Time
}

// bufferedEvent represents a single rangefeed event waiting to be flushed.
type bufferedEvent struct {
	key       roachpb.Key
	value     roachpb.Value
	prevValue roachpb.Value
	ts        hlc.Timestamp
}

// shardConfig contains the configuration for creating a shard.
type shardConfig struct {
	id               int
	spans            []spanWithStartTS
	logFolder        string
	execCtx          sql.JobExecContext
	stopper          *stop.Stopper
	flushReportCh    chan<- flushReport
	frontierReportCh chan<- frontierReport
}

// newShard creates a new shard processor.
func newShard(cfg shardConfig) (*shard, error) {
	settings := cfg.execCtx.ExecCfg().Settings

	// Create span frontier initialized at each span's start timestamp
	frontier, err := span.MakeFrontier()
	if err != nil {
		return nil, errors.Wrap(err, "creating span frontier")
	}
	for _, s := range cfg.spans {
		if err := frontier.AddSpansAt(s.StartTS, s.Span); err != nil {
			return nil, errors.Wrapf(err, "adding span %s to frontier", s.Span)
		}
	}

	sh := &shard{
		id:               cfg.id,
		spans:            cfg.spans,
		logFolder:        cfg.logFolder,
		execCtx:          cfg.execCtx,
		settings:         settings,
		stopper:          cfg.stopper,
		flushReportCh:    cfg.flushReportCh,
		frontierReportCh: cfg.frontierReportCh,
		lastFlush:        timeutil.Now(),
		lastFrontierSync: timeutil.Now(),
	}
	sh.mu.frontier = frontier
	sh.mu.buffer = make([]bufferedEvent, 0, 1024)
	return sh, nil
}

var _ shardRunner = (*shard)(nil)

// Run starts the shard's rangefeed and processing loop.
// It implements the shardRunner interface.
func (s *shard) Run(ctx context.Context) error {
	db := s.execCtx.ExecCfg().DB

	// Create rangefeed factory
	rfFactory, err := rangefeed.NewFactory(s.stopper, db, s.settings, nil)
	if err != nil {
		return errors.Wrap(err, "creating rangefeed factory")
	}

	// Extract raw spans and find minimum start time
	rawSpans := make([]roachpb.Span, len(s.spans))
	minStartTS := hlc.MaxTimestamp
	for i, sp := range s.spans {
		rawSpans[i] = sp.Span
		if sp.StartTS.Less(minStartTS) {
			minStartTS = sp.StartTS
		}
	}

	// Start rangefeed with callbacks
	rf, err := rfFactory.RangeFeed(
		ctx,
		fmt.Sprintf("continuous-backup-shard-%d", s.id),
		rawSpans,
		minStartTS, // Use min start time; frontier handles per-span filtering
		s.onValue,
		rangefeed.WithDiff(true), // Need prev_value for LWW replay
		rangefeed.WithOnFrontierAdvance(s.onFrontierAdvance),
		rangefeed.WithOnCheckpoint(s.onCheckpoint),
	)
	if err != nil {
		return errors.Wrap(err, "starting rangefeed")
	}
	defer rf.Close()

	log.Dev.Infof(ctx, "shard %d started rangefeed on %d spans from %s",
		s.id, len(s.spans), minStartTS)

	// Processing loop with two tickers:
	// 1. Flush ticker (5-10s) - writes files and reports min timestamp
	// 2. Frontier sync ticker (30s) - sends full span frontier
	flushTicker := time.NewTicker(FlushInterval.Get(&s.settings.SV))
	defer flushTicker.Stop()

	frontierSyncTicker := time.NewTicker(FrontierSyncInterval.Get(&s.settings.SV))
	defer frontierSyncTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final flush before exit
			if err := s.flush(ctx); err != nil {
				log.Dev.Warningf(ctx, "shard %d final flush error: %v", s.id, err)
			}
			return ctx.Err()

		case <-flushTicker.C:
			if err := s.maybeFlush(ctx); err != nil {
				return errors.Wrap(err, "flush error")
			}

		case <-frontierSyncTicker.C:
			s.sendFrontierSync()
		}
	}
}

// onValue is called for each rangefeed value event.
func (s *shard) onValue(ctx context.Context, value *kvpb.RangeFeedValue) {
	s.mu.Lock()
	defer s.mu.Unlock()

	event := bufferedEvent{
		key:       value.Key,
		value:     value.Value,
		prevValue: value.PrevValue,
		ts:        value.Value.Timestamp,
	}

	s.mu.buffer = append(s.mu.buffer, event)
	s.mu.bufferSize += int64(len(value.Key) + len(value.Value.RawBytes) + len(value.PrevValue.RawBytes))

	// Track min/max timestamps
	if s.mu.minTS.IsEmpty() || event.ts.Less(s.mu.minTS) {
		s.mu.minTS = event.ts
	}
	if s.mu.maxTS.Less(event.ts) {
		s.mu.maxTS = event.ts
	}
}

// onFrontierAdvance is called when the rangefeed frontier advances for a span.
func (s *shard) onFrontierAdvance(ctx context.Context, ts hlc.Timestamp) {
	// The rangefeed library calls this with the overall frontier advancement.
	// We track per-span frontiers via checkpoint callbacks.
}

// onCheckpoint is called for rangefeed checkpoints, which include span information.
func (s *shard) onCheckpoint(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Advance our frontier for this span
	if _, err := s.mu.frontier.Forward(checkpoint.Span, checkpoint.ResolvedTS); err != nil {
		log.Dev.Warningf(ctx, "shard %d failed to advance frontier for %s: %v",
			s.id, checkpoint.Span, err)
	}
}

// shouldFlush returns true if the buffer should be flushed.
func (s *shard) shouldFlush() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.bufferSize >= MaxBufferSize.Get(&s.settings.SV) ||
		timeutil.Since(s.lastFlush) >= FlushInterval.Get(&s.settings.SV)
}

// maybeFlush flushes the buffer if conditions are met.
func (s *shard) maybeFlush(ctx context.Context) error {
	if s.shouldFlush() {
		return s.flush(ctx)
	}
	return nil
}

// flush writes buffered events to an SST file and reports to coordinator.
func (s *shard) flush(ctx context.Context) error {
	s.mu.Lock()
	buffer := s.mu.buffer
	frontierTS := s.mu.frontier.Frontier() // Get min timestamp across all spans
	s.mu.buffer = make([]bufferedEvent, 0, 1024)
	s.mu.bufferSize = 0
	minTS := s.mu.minTS
	s.mu.minTS = hlc.Timestamp{}
	s.mu.maxTS = hlc.Timestamp{}
	s.mu.Unlock()

	s.lastFlush = timeutil.Now()

	// Empty buffer - just report frontier update
	if len(buffer) == 0 {
		s.flushReportCh <- flushReport{
			shardID:    s.id,
			frontierTS: frontierTS,
		}
		return nil
	}

	// Write SST file
	fileID, err := s.writeSST(ctx, buffer)
	if err != nil {
		s.flushReportCh <- flushReport{
			shardID: s.id,
			err:     err,
		}
		return err
	}

	log.Dev.Infof(ctx, "shard %d flushed %d events to data/%d.sst", s.id, len(buffer), fileID)

	s.flushReportCh <- flushReport{
		shardID:    s.id,
		fileID:     fileID,
		minTS:      minTS,
		frontierTS: frontierTS,
	}

	return nil
}

// sendFrontierSync sends the full span frontier to the coordinator.
func (s *shard) sendFrontierSync() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Extract all span entries from the frontier
	var resolvedSpans []jobspb.ResolvedSpan
	for sp, ts := range s.mu.frontier.Entries() {
		resolvedSpans = append(resolvedSpans, jobspb.ResolvedSpan{
			Span:      sp,
			Timestamp: ts,
		})
	}

	s.frontierReportCh <- frontierReport{
		shardID:       s.id,
		resolvedSpans: resolvedSpans,
	}

	s.lastFrontierSync = timeutil.Now()
}

// writeSST writes buffered events to an SST file in cloud storage.
// Returns the unique file ID which maps to path "data/<id>.sst".
func (s *shard) writeSST(ctx context.Context, buffer []bufferedEvent) (int64, error) {
	// Create storage for the data subdirectory.
	dataFolder := fmt.Sprintf("%s/data", s.logFolder)
	store, err := s.execCtx.ExecCfg().DistSQLSrv.ExternalStorageFromURI(
		ctx, dataFolder, s.execCtx.User(),
	)
	if err != nil {
		return 0, errors.Wrap(err, "creating storage")
	}
	defer store.Close()

	// Generate globally unique file ID using node ID for uniqueness across nodes.
	nodeID := s.execCtx.ExecCfg().NodeInfo.NodeID.SQLInstanceID()
	fileID := unique.GenerateUniqueInt(unique.ProcessUniqueID(nodeID))
	filename := fmt.Sprintf("%d.sst", fileID)

	// Build SST in memory.
	var sstBuf objstorage.MemObj
	sstWriter := storage.MakeTransportSSTWriter(ctx, s.settings, &sstBuf)
	defer sstWriter.Close()

	// Write events in timestamp order.
	for i, event := range buffer {
		key := encodeEventKey(event.ts, uint32(i))
		value := encodeEventValue(event)
		if err := sstWriter.PutUnversioned(key, value); err != nil {
			return 0, errors.Wrap(err, "writing to SST")
		}
	}

	if err := sstWriter.Finish(); err != nil {
		return 0, errors.Wrap(err, "finishing SST")
	}

	// Write to storage.
	if err := cloud.WriteFile(ctx, store, filename, bytes.NewReader(sstBuf.Data())); err != nil {
		return 0, errors.Wrap(err, "uploading SST")
	}

	return fileID, nil
}
