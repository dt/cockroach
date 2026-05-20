// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revert

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

var maxRevertSpanNumWorkers = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.revert.max_span_parallelism",
	"the maximum number of workers used to issue RevertRange request",
	8,
	settings.PositiveInt,
)

// UseMVCCHistoryTruncation controls whether RevertRange uses per-SST MVCC
// suffix masks instead of iterating keys and writing tombstones. The default
// is metamorphic so tests exercise both paths in CI.
var UseMVCCHistoryTruncation = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.revert_range.use_mvcc_history_truncation.enabled",
	"use per-SST MVCC suffix masks for RevertRange instead of writing tombstones",
	metamorphic.ConstantWithTestBool(
		"kv.revert_range.use_mvcc_history_truncation.enabled", true),
)

// RevertSpansFanout calls RevertSpans in parallel. The span is
// divided using DistSQL's PartitionSpans.
//
// We do this to get parallel execution of RevertRange even in the
// case of a non-zero batch size. DistSender will not parallelize
// requests with non-zero MaxSpanRequestKeys set.
// RevertSpansFanoutOpts configures the behavior of RevertSpansFanout.
type RevertSpansFanoutOpts struct {
	// UseHistoryTruncation, when set, causes RevertRange to apply per-SST MVCC
	// suffix masks instead of writing tombstones. The caller must ensure no
	// intents exist in the spans and that no concurrent writes are in flight.
	UseHistoryTruncation bool
}

func RevertSpansFanout(
	ctx context.Context,
	db *kv.DB,
	rsCtx sql.JobExecContext,
	spans []roachpb.Span,
	targetTime hlc.Timestamp,
	ignoreGCThreshold bool,
	batchSize int64,
	onCompletedCallback func(context.Context, roachpb.Span) error,
	opts ...RevertSpansFanoutOpts,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "sql.RevertSpansFanout")
	defer sp.Finish()

	var o RevertSpansFanoutOpts
	if len(opts) > 0 {
		o = opts[0]
	}

	execCfg := rsCtx.ExecCfg()
	maxWorkerCount := int(maxRevertSpanNumWorkers.Get(execCfg.SV()))
	if maxWorkerCount == 1 {
		return RevertSpans(ctx, db, spans, targetTime, ignoreGCThreshold, batchSize, onCompletedCallback, o)
	}

	dsp := rsCtx.DistSQLPlanner()
	planCtx, _, err := dsp.SetupAllNodesPlanning(ctx, rsCtx.ExtendedEvalContext(), execCfg)
	if err != nil {
		return err
	}

	spanPartitions, err := dsp.PartitionSpans(ctx, planCtx, spans, sql.PartitionSpansBoundDefault)
	if err != nil {
		return err
	}

	var workerPartitions []sql.SpanPartition
	if len(spanPartitions) <= maxWorkerCount {
		workerPartitions = spanPartitions
	} else {
		workerPartitions = make([]sql.SpanPartition, maxWorkerCount)
		for i, sp := range spanPartitions {
			idx := i % maxWorkerCount
			workerPartitions[idx].Spans = append(workerPartitions[idx].Spans, sp.Spans...)
		}
	}

	var callback func(context.Context, roachpb.Span) error
	if onCompletedCallback != nil {
		// If we have an onCompletedCallback arrange for it to
		// be called serially so that callers don't need to
		// worry about making it concurrency safe.
		var callbackMutex syncutil.Mutex
		callback = func(ctx context.Context, completed roachpb.Span) error {
			callbackMutex.Lock()
			defer callbackMutex.Unlock()
			return onCompletedCallback(ctx, completed)
		}
	}

	errGroup, workerCtx := errgroup.WithContext(ctx)
	for i := range workerPartitions {
		workerIdx := i
		errGroup.Go(func() error {
			spans := workerPartitions[workerIdx].Spans
			return RevertSpans(workerCtx, db, spans,
				targetTime, ignoreGCThreshold, batchSize, callback, o)
		})
	}
	return errGroup.Wait()
}

// RevertSpans reverts the passed span to the target time, which must be above
// the GC threshold for every range (unless the flag ignoreGCThreshold is passed
// which should be done with care -- see RevertRangeRequest.IgnoreGCThreshold).
//
// The onCompletedSpan is called after each response to a RevertRange
// request.
func RevertSpans(
	ctx context.Context,
	db *kv.DB,
	spans []roachpb.Span,
	targetTime hlc.Timestamp,
	ignoreGCThreshold bool,
	batchSize int64,
	onCompletedSpan func(ctx context.Context, completed roachpb.Span) error,
	opts ...RevertSpansFanoutOpts,
) error {
	var o RevertSpansFanoutOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	ctx, sp := tracing.ChildSpan(ctx, "sql.RevertSpans")
	defer sp.Finish()

	// revertOne issues a single RevertRangeRequest and returns the resume span
	// (or nil). It is wrapped in its own child span so that per-request cost
	// (one iteration of the ResumeSpan pagination loop) is visible in trace
	// aggregates, alongside the per-worker sql.RevertSpans span above.
	revertOne := func(ctx context.Context, span *roachpb.Span) (*roachpb.Span, error) {
		ctx, sp := tracing.ChildSpan(ctx, "sql.RevertRange")
		defer sp.Finish()

		var b kv.Batch
		b.AddRawRequest(&kvpb.RevertRangeRequest{
			RequestHeader: kvpb.RequestHeader{
				Key:    span.Key,
				EndKey: span.EndKey,
			},
			TargetTime:               targetTime,
			IgnoreGcThreshold:        ignoreGCThreshold,
			UseMVCCHistoryTruncation: o.UseHistoryTruncation,
		})
		// TODO(ssd): We should probably be setting an
		// admission header here has well.
		b.Header.MaxSpanRequestKeys = batchSize

		log.VEventf(ctx, 2, "RevertRange %s %s", span, targetTime)
		if err := db.Run(ctx, &b); err != nil {
			return nil, err
		}

		if l := len(b.RawResponse().Responses); l != 1 {
			return nil, errors.AssertionFailedf("expected single response, got %d", l)
		}

		resp := b.RawResponse().Responses[0].GetRevertRange()
		if resp == nil {
			return nil, errors.AssertionFailedf("expected RevertRangeResponse, got: %v", resp)
		}

		completed := *span
		if resp.ResumeSpan != nil {
			if !resp.ResumeSpan.Valid() {
				return nil, errors.Errorf("invalid resume span: %s", resp.ResumeSpan)
			}
			completed.EndKey = resp.ResumeSpan.Key
		}

		if onCompletedSpan != nil {
			if err := onCompletedSpan(ctx, completed); err != nil {
				return nil, err
			}
		}
		return resp.ResumeSpan, nil
	}

	for _, sp := range spans {
		span := &sp
		for span != nil {
			next, err := revertOne(ctx, span)
			if err != nil {
				return err
			}
			span = next
		}
	}

	return nil
}

// RevertDefaultBatchSize is the default batch size for reverting spans.
// This only needs to be small enough to keep raft/pebble happy -- there is no
// reply size to worry about.
// TODO(dt): tune this via experimentation.
const RevertDefaultBatchSize = 500000
