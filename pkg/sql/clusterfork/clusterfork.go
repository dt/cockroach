// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package clusterfork orchestrates the per-tenant copy-on-write fork
// (CREATE TENANT FROM) flow: it locks a destination tenant's parent
// range, pre-splits the destination to mirror the source tenant's range
// layout, fires CloneData at every source range, unlocks each
// destination range, and runs a cluster-wide RevertRange to bring the
// destination to a single MVCC timestamp.
//
// See the design brief for context:
// https://gist.github.com/dt/43c293c781ff8508fde592a432af30c2
package clusterfork

import (
	"bytes"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// defaultLeaseDuration is how long the orchestration takes the
// inconsistency lease for. Generously sized so the deadman won't fire
// during a normal-latency fork; the orchestration is expected to clear
// the lease itself well before this lapses.
//
// TODO(dt): heartbeat / extend the lease while the orchestration runs.
const defaultLeaseDuration = 30 * time.Minute

// ForkTenant produces dst tenant as a copy-on-write fork of src tenant
// as of T. The destination tenant must already be created and empty
// (no user data, just whatever the bootstrap path lays down). On
// success, dst's user keyspace is a logical snapshot of src's at T.
//
// This is the prototype-level orchestration: it skips PTS install,
// WaitForApplication-before-unlock, excise-pass for PCR-elided tables,
// and value-level descriptor rewrites. Demos work modulo the
// VirtualClone Pebble surface being wired (currently stubbed to error).
func ForkTenant(ctx context.Context, db *kv.DB, src, dst roachpb.TenantID, t hlc.Timestamp) error {
	ctx, sp := tracing.ChildSpan(ctx, "clusterfork.ForkTenant")
	defer sp.Finish()

	srcPrefix := roachpb.Key(keys.MakeTenantPrefix(src))
	dstPrefix := roachpb.Key(keys.MakeTenantPrefix(dst))
	srcSpan := roachpb.Span{Key: srcPrefix, EndKey: srcPrefix.PrefixEnd()}
	dstSpan := roachpb.Span{Key: dstPrefix, EndKey: dstPrefix.PrefixEnd()}

	log.Dev.Infof(ctx, "fork tenant %s → %s at %s (src=%s dst=%s)",
		src, dst, t, srcSpan, dstSpan)

	// step runs fn under a child span of ctx named "clusterfork.<name>",
	// so the caller can read a flame-graph-shaped breakdown of where a
	// fork spends its wall time. Each step's span finishes regardless of
	// whether fn returns an error.
	step := func(name string, fn func(ctx context.Context) error) error {
		ctx, sp := tracing.ChildSpan(ctx, "clusterfork."+name)
		defer sp.Finish()
		return fn(ctx)
	}

	// 1. Install PTS at T on src tenant's keyspan.
	// TODO(dt): not implemented; trusting the operator to keep the
	// duration short enough that GC doesn't run on src below T.

	// 2. Lock the destination tenant's parent range. We assume dst was
	// just created and is covered by a single range whose start key is
	// the tenant prefix.
	if err := step("lock-dst-parent", func(ctx context.Context) error {
		expiresAt := db.Clock().Now().Add(defaultLeaseDuration.Nanoseconds(), 0)
		return setReplicaInconsistency(ctx, db, dstPrefix, expiresAt, false /* ackAborted */)
	}); err != nil {
		return errors.Wrap(err, "lock destination parent range")
	}

	// 3. Pre-split destination to mirror source layout.
	var srcDescs []roachpb.RangeDescriptor
	if err := step("pre-split-dst", func(ctx context.Context) error {
		var err error
		srcDescs, err = scanRangeDescriptors(ctx, db, srcSpan)
		if err != nil {
			return errors.Wrap(err, "list source ranges")
		}
		for _, srcDesc := range srcDescs {
			// First range starts at the tenant boundary; no split needed.
			if bytes.Equal(srcDesc.StartKey.AsRawKey(), srcSpan.Key) {
				continue
			}
			dstSplitKey := rewritePrefix(srcDesc.StartKey.AsRawKey(), srcPrefix, dstPrefix)
			if err := db.AdminSplit(ctx, dstSplitKey, hlc.MaxTimestamp); err != nil {
				return errors.Wrapf(err, "pre-split destination at %s", dstSplitKey)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// 4. AdminRelocateRange each destination range to colocate with the
	// corresponding source range. The CloneData apply mounts virtual SSTs
	// into the local store of every source-range replica; for the
	// destination range to actually expose that data, its replicas must
	// live on those same stores.
	if err := step("relocate-dst", func(ctx context.Context) error {
		dstDescsForRelocate, err := scanRangeDescriptors(ctx, db, dstSpan)
		if err != nil {
			return errors.Wrap(err, "list destination ranges for relocate")
		}
		srcByDstStartKey := make(map[string]*roachpb.RangeDescriptor, len(srcDescs))
		for i := range srcDescs {
			dstStartKey := rewritePrefix(srcDescs[i].StartKey.AsRawKey(), srcPrefix, dstPrefix)
			srcByDstStartKey[string(dstStartKey)] = &srcDescs[i]
		}
		for _, dstDesc := range dstDescsForRelocate {
			// The leftmost range may begin before dstPrefix (it inherited
			// from the previous tenant boundary). We only relocate ranges
			// fully inside the destination tenant.
			if bytes.Compare(dstDesc.StartKey.AsRawKey(), dstSpan.Key) < 0 {
				continue
			}
			src, ok := srcByDstStartKey[string(dstDesc.StartKey.AsRawKey())]
			if !ok {
				// No matching source range. Either the layout is asymmetric or
				// we're looking at a range outside the source tenant span.
				// Skip; CloneData on this dst range is a no-op.
				continue
			}
			voters := replicationTargets(src.Replicas().VoterDescriptors())
			nonVoters := replicationTargets(src.Replicas().NonVoterDescriptors())
			if err := db.AdminRelocateRange(
				ctx, dstDesc.StartKey.AsRawKey(), voters, nonVoters,
				true, /* transferLeaseToFirstVoter */
			); err != nil {
				return errors.Wrapf(err,
					"AdminRelocateRange dst range %s to colocate with src range %s",
					dstDesc.RSpan(), src.RSpan())
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// 5. Fire CloneData at every source range. Wrapped in a single
	// "clone-data-all" span; each individual range's CloneData gets its
	// own child span so per-range timing is visible in the trace.
	if err := step("clone-data-all", func(ctx context.Context) error {
		for _, srcDesc := range srcDescs {
			rangeSrcSpan := roachpb.Span{
				Key:    srcDesc.StartKey.AsRawKey(),
				EndKey: srcDesc.EndKey.AsRawKey(),
			}
			// Clamp the source span to the tenant prefix (the leftmost or
			// rightmost source range may extend beyond the tenant on either
			// side; we only want to clone what's inside this tenant).
			if bytes.Compare(rangeSrcSpan.Key, srcSpan.Key) < 0 {
				rangeSrcSpan.Key = srcSpan.Key
			}
			if bytes.Compare(rangeSrcSpan.EndKey, srcSpan.EndKey) > 0 {
				rangeSrcSpan.EndKey = srcSpan.EndKey
			}
			rangeCtx, rangeSp := tracing.ChildSpan(ctx, "clusterfork.clone-data-range")
			err := sendCloneData(rangeCtx, db, rangeSrcSpan, srcPrefix, dstPrefix)
			rangeSp.Finish()
			if err != nil {
				return errors.Wrapf(err, "CloneData on source range %s", rangeSrcSpan)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// 6. Unlock each destination range.
	// TODO(dt): WaitForApplication-style verification per source-range
	// replica before unlocking, so we don't unlock while a follower
	// still has data to mount.
	if err := step("unlock-dst", func(ctx context.Context) error {
		dstDescs, err := scanRangeDescriptors(ctx, db, dstSpan)
		if err != nil {
			return errors.Wrap(err, "list destination ranges")
		}
		for _, dstDesc := range dstDescs {
			startKey := dstDesc.StartKey.AsRawKey()
			// Only unlock destination ranges that are inside the tenant
			// span; the leftmost destination range may start before
			// dstPrefix (e.g., a tenant boundary range), in which case
			// AdminSetReplicaInconsistency on it would refuse "key is not
			// the start of a range" — handled by clamping to dstPrefix.
			if bytes.Compare(startKey, dstSpan.Key) < 0 {
				startKey = dstSpan.Key
			}
			if err := setReplicaInconsistency(ctx, db, startKey, hlc.Timestamp{}, false /* ackAborted */); err != nil {
				return errors.Wrapf(err, "unlock destination range starting at %s", startKey)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// 7. RevertRange the entire destination tenant span back to T.
	if err := step("revert-dst-to-t", func(ctx context.Context) error {
		return revertSpan(ctx, db, dstSpan, t)
	}); err != nil {
		return errors.Wrap(err, "revert destination tenant to target time")
	}

	// 8. Clear the spans that PCR's KeyRewriter elides during stream
	// ingestion: sql_instances, sqlliveness, lease. These hold ephemeral
	// SQL-pod / session state that should not carry over to the
	// destination tenant; without clearing them the dst tenant would
	// observe stale leases / liveness records / pod registrations from
	// the source tenant until they expire.
	if err := step("clear-elided-tables", func(ctx context.Context) error {
		return clearElidedTables(ctx, db, dst)
	}); err != nil {
		return errors.Wrap(err, "clear PCR-elided system table spans")
	}

	log.Dev.Infof(ctx, "fork tenant %s → %s complete", src, dst)
	return nil
}

// elidedTableIDs is the set of system tables PCR's KeyRewriter drops on
// the floor during stream ingestion (see pkg/backup/key_rewriter.go).
// We mirror that policy at the end of a fork: the destination tenant
// starts with empty versions of these tables.
var elidedTableIDs = []uint32{
	keys.SQLInstancesTableID,
	keys.SqllivenessID,
	keys.LeaseTableID,
}

// clearElidedTables issues a ClearRange against each of the elided
// table spans on the destination tenant.
func clearElidedTables(ctx context.Context, db *kv.DB, dst roachpb.TenantID) error {
	codec := keys.MakeSQLCodec(dst)
	for _, tableID := range elidedTableIDs {
		span := codec.TableSpan(tableID)
		b := &kv.Batch{}
		b.AddRawRequest(&kvpb.ClearRangeRequest{
			RequestHeader: kvpb.RequestHeader{Key: span.Key, EndKey: span.EndKey},
		})
		if err := db.Run(ctx, b); err != nil {
			return errors.Wrapf(err, "ClearRange of %s (table %d)", span, tableID)
		}
	}
	return nil
}

// replicationTargets converts a set of ReplicaDescriptors into the
// {NodeID, StoreID} pairs AdminRelocateRange takes.
func replicationTargets(descs []roachpb.ReplicaDescriptor) []roachpb.ReplicationTarget {
	out := make([]roachpb.ReplicationTarget, len(descs))
	for i, d := range descs {
		out[i] = roachpb.ReplicationTarget{NodeID: d.NodeID, StoreID: d.StoreID}
	}
	return out
}

// rewritePrefix returns key with srcPrefix replaced by dstPrefix.
// key is expected to start with srcPrefix.
func rewritePrefix(key, srcPrefix, dstPrefix roachpb.Key) roachpb.Key {
	suffix := key[len(srcPrefix):]
	out := make(roachpb.Key, 0, len(dstPrefix)+len(suffix))
	out = append(out, dstPrefix...)
	out = append(out, suffix...)
	return out
}

// scanRangeDescriptors returns the RangeDescriptors for ranges that
// intersect the given span.
func scanRangeDescriptors(
	ctx context.Context, db *kv.DB, span roachpb.Span,
) ([]roachpb.RangeDescriptor, error) {
	var descs []roachpb.RangeDescriptor
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		kvs, err := kvclient.ScanMetaKVs(ctx, txn, span)
		if err != nil {
			return err
		}
		descs = make([]roachpb.RangeDescriptor, 0, len(kvs))
		for _, kv := range kvs {
			var desc roachpb.RangeDescriptor
			if err := kv.ValueProto(&desc); err != nil {
				return errors.Wrapf(err, "decoding RangeDescriptor at %s", kv.Key)
			}
			descs = append(descs, desc)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return descs, nil
}

// setReplicaInconsistency issues an AdminSetReplicaInconsistency at the
// given range's start key. untilTS is the new value: zero unlocks,
// non-zero locks. ackAborted=true accepts a pre-existing aborted state.
func setReplicaInconsistency(
	ctx context.Context, db *kv.DB, key roachpb.Key, untilTS hlc.Timestamp, ackAborted bool,
) error {
	req := &kvpb.AdminSetReplicaInconsistencyRequest{
		RequestHeader: kvpb.RequestHeader{Key: key},
		UntilTS:       untilTS,
		AckAborted:    ackAborted,
	}
	b := &kv.Batch{}
	b.AddRawRequest(req)
	return db.Run(ctx, b)
}

// sendCloneData issues a CloneData raft command against the source
// range covering srcSpan, asking it to mount virtual SSTs aliasing
// srcSpan under dstPrefix on every replica's local store. The
// destination span is the substitution image of srcSpan: srcPrefix
// swapped for dstPrefix on both bounds.
func sendCloneData(
	ctx context.Context, db *kv.DB, srcSpan roachpb.Span, srcPrefix, dstPrefix roachpb.Key,
) error {
	dstSpan := roachpb.Span{
		Key:    rewriteSpanBound(srcSpan.Key, srcPrefix, dstPrefix),
		EndKey: rewriteSpanBound(srcSpan.EndKey, srcPrefix, dstPrefix),
	}
	req := &kvpb.CloneDataRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    srcSpan.Key,
			EndKey: srcSpan.EndKey,
		},
		SrcPrefix: srcPrefix,
		DstSpan:   dstSpan,
		DstPrefix: dstPrefix,
	}
	b := &kv.Batch{}
	b.AddRawRequest(req)
	return db.Run(ctx, b)
}

// rewriteSpanBound translates a span boundary key from srcPrefix space
// to dstPrefix space. Two cases:
//
//   - key starts with srcPrefix → swap prefix (rewritePrefix).
//   - key equals srcPrefix.PrefixEnd() → return dstPrefix.PrefixEnd().
//     This is the canonical exclusive upper bound of a tenant span and
//     is by construction *outside* srcPrefix, so a byte-level prefix
//     swap would produce dstPrefix instead of dstPrefix.PrefixEnd().
//
// Any other shape is an invariant violation (the caller has clamped
// the source span to lie within [srcPrefix, srcPrefix.PrefixEnd())).
func rewriteSpanBound(key, srcPrefix, dstPrefix roachpb.Key) roachpb.Key {
	if bytes.HasPrefix(key, srcPrefix) {
		return rewritePrefix(key, srcPrefix, dstPrefix)
	}
	if bytes.Equal(key, srcPrefix.PrefixEnd()) {
		return dstPrefix.PrefixEnd()
	}
	panic(errors.AssertionFailedf(
		"span bound %s is neither under srcPrefix=%s nor equal to srcPrefix.PrefixEnd()=%s",
		key, srcPrefix, srcPrefix.PrefixEnd()))
}

// revertSpan issues RevertRange across span back to targetTime.
func revertSpan(ctx context.Context, db *kv.DB, span roachpb.Span, targetTime hlc.Timestamp) error {
	req := &kvpb.RevertRangeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    span.Key,
			EndKey: span.EndKey,
		},
		TargetTime: targetTime,
	}
	b := &kv.Batch{}
	b.AddRawRequest(req)
	return db.Run(ctx, b)
}
