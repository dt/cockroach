// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"bytes"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(kvpb.CloneData, declareKeysCloneData, EvalCloneData)
}

func declareKeysCloneData(
	rs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	// CloneData reads the local LSM over the request span and mounts virtual
	// SSTs aliasing it under DstPrefix. It does not write any MVCC keys to the
	// source range, so a non-MVCC read latch over the request span is enough
	// to serialize against MVCC writes that would otherwise race.
	reqHeader := req.Header()
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
		Key: reqHeader.Key, EndKey: reqHeader.EndKey,
	})
	return nil
}

// EvalCloneData evaluates a CloneDataRequest. The source span (the request
// header's key range) must lie within [SrcPrefix, SrcPrefix.PrefixEnd()).
// Eval just records the parameters in the Replicated.CloneData side-effect;
// the actual virtual-SST mounting happens on each replica during apply.
func EvalCloneData(
	ctx context.Context, _ storage.ReadWriter, cArgs CommandArgs, _ kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.CloneDataRequest)

	if len(args.SrcPrefix) == 0 {
		return result.Result{}, errors.New("CloneData: SrcPrefix must be non-empty")
	}
	if len(args.DstPrefix) == 0 {
		return result.Result{}, errors.New("CloneData: DstPrefix must be non-empty")
	}

	srcSpan := roachpb.Span{Key: args.Key, EndKey: args.EndKey}
	if len(srcSpan.Key) == 0 || len(srcSpan.EndKey) == 0 {
		return result.Result{}, errors.New("CloneData: request must have a non-empty span")
	}
	if len(args.DstSpan.Key) == 0 || len(args.DstSpan.EndKey) == 0 {
		return result.Result{}, errors.New("CloneData: DstSpan must be non-empty")
	}
	srcPrefixEnd := roachpb.Key(args.SrcPrefix).PrefixEnd()
	if bytes.Compare(srcSpan.Key, args.SrcPrefix) < 0 ||
		bytes.Compare(srcSpan.EndKey, srcPrefixEnd) > 0 {
		return result.Result{}, errors.Errorf(
			"CloneData: source span [%s, %s) must lie within [SrcPrefix=%q, %s)",
			srcSpan.Key, srcSpan.EndKey, args.SrcPrefix, srcPrefixEnd,
		)
	}
	dstPrefixEnd := roachpb.Key(args.DstPrefix).PrefixEnd()
	if bytes.Compare(args.DstSpan.Key, args.DstPrefix) < 0 ||
		bytes.Compare(args.DstSpan.EndKey, dstPrefixEnd) > 0 {
		return result.Result{}, errors.Errorf(
			"CloneData: destination span [%s, %s) must lie within [DstPrefix=%q, %s)",
			args.DstSpan.Key, args.DstSpan.EndKey, args.DstPrefix, dstPrefixEnd,
		)
	}

	return result.Result{
		Replicated: kvserverpb.ReplicatedEvalResult{
			CloneData: &kvserverpb.ReplicatedEvalResult_CloneData{
				SrcSpan:   srcSpan,
				SrcPrefix: args.SrcPrefix,
				DstSpan:   args.DstSpan,
				DstPrefix: args.DstPrefix,
			},
		},
	}, nil
}
