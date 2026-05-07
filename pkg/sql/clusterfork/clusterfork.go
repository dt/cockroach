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
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// defaultLeaseDuration is how long the orchestration takes the
// inconsistency lease for. Generously sized so the deadman won't fire
// during a normal-latency fork; the orchestration is expected to clear
// the lease itself well before this lapses.
//
// TODO(dt): heartbeat / extend the lease while the orchestration runs.
//
// TODO(dt): force-drop tenant. A regular DROP TENANT issues
// non-admin writes (ClearRange) which the InconsistentReplicas
// dispatch reject blocks on locked dst ranges, and it does not clear
// the per-replica missing-spans state. For cleanup of half-baked
// forks (CreateTenantRecord committed but ForkTenant errored
// partway), we need a force-drop path that, through raft on each dst
// replica, excises the tenant span at the LSM level and clears both
// the InconsistentReplicas bit on the descriptor and the
// missing-spans range-id-local key — bypassing the normal
// non-admin-write reject. Without this, a partial-fork tenant in
// DataStateAdd cannot be cleanly removed and its keyspace remains
// untouchable until the inconsistency lease expires.
const defaultLeaseDuration = 30 * time.Minute

// cloneDataParallelism caps how many CloneData RPCs the orchestration
// fans out concurrently in the clone-data-all step. The per-range RPCs
// are independent (different leaseholders, different replicas), so the
// limit is mostly about not overwhelming the cluster with thousands of
// simultaneous raft commands when a tenant has many ranges. Tenants
// with fewer than this many ranges run all clones in parallel; larger
// tenants stream them through this many workers.
const cloneDataParallelism = 16

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

	srcPrefix := keys.MakeTenantPrefix(src)
	dstPrefix := keys.MakeTenantPrefix(dst)
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
		// Scope the missing-spans init to the destination tenant span so
		// out-of-tenant keyspace the parent range may cover (e.g. the
		// open-ended right-neighbor range that extends past the highest
		// tenant) is excluded from what unlock-time completeness must
		// satisfy. CloneData will only fill the in-tenant portion.
		return setReplicaInconsistency(ctx, db, dstPrefix, expiresAt,
			false /* ackAborted */, dstSpan)
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
			startKey := dstDesc.StartKey.AsRawKey()
			voters := replicationTargets(src.Replicas().VoterDescriptors())
			nonVoters := replicationTargets(src.Replicas().NonVoterDescriptors())
			if err := relocateWithRetry(ctx, db, startKey, voters, nonVoters); err != nil {
				return errors.Wrapf(err,
					"AdminRelocateRange dst range %s to colocate with src range %s",
					dstDesc.RSpan(), src.RSpan())
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// 5+6. Wave loop: clone CloneData for each todo's src span, then
	// unlock its dst range. Any dst range whose unlock fails with "clone
	// is incomplete" goes back into the next wave; the re-fired CloneData
	// gives any followers / late-initialized dst replicas another chance
	// to mount the data. CloneData is destructive-then-install at the
	// engine layer, so re-applying on already-complete replicas is safe
	// while the dst is still locked.
	//
	// Each todo carries the src span that produces the dst range's
	// content, plus the dst start key to unlock; the 1:1 mapping comes
	// from the pre-split layout (each src range's prefix-rewritten start
	// key was the split boundary on dst).
	type todoItem struct {
		srcSpan     roachpb.Span
		dstStartKey roachpb.Key
	}
	todo := make([]todoItem, 0, len(srcDescs))
	for _, srcDesc := range srcDescs {
		s := roachpb.Span{
			Key:    srcDesc.StartKey.AsRawKey(),
			EndKey: srcDesc.EndKey.AsRawKey(),
		}
		if bytes.Compare(s.Key, srcSpan.Key) < 0 {
			s.Key = srcSpan.Key
		}
		if bytes.Compare(s.EndKey, srcSpan.EndKey) > 0 {
			s.EndKey = srcSpan.EndKey
		}
		dstStartKey := rewritePrefix(srcDesc.StartKey.AsRawKey(), srcPrefix, dstPrefix)
		// The leftmost dst range may begin before dstPrefix (it inherited
		// from the previous tenant boundary); clamp so unlock targets the
		// in-tenant start key and AdminSetReplicaInconsistency doesn't
		// reject with "key is not the start of a range".
		if bytes.Compare(dstStartKey, dstSpan.Key) < 0 {
			dstStartKey = dstSpan.Key
		}
		todo = append(todo, todoItem{srcSpan: s, dstStartKey: dstStartKey})
	}

	const maxWaves = 5
	for wave := 0; wave < maxWaves && len(todo) > 0; wave++ {
		waveCtx, waveSp := tracing.ChildSpan(ctx, fmt.Sprintf("clusterfork.wave-%d", wave))
		// Shuffle so adjacent ranges (which tend to share leaseholders /
		// storage paths) don't pile onto the same nodes when all workers
		// start at once.
		rand.Shuffle(len(todo), func(i, j int) { todo[i], todo[j] = todo[j], todo[i] })
		workCh := make(chan roachpb.Span, len(todo))
		for _, t := range todo {
			workCh <- t.srcSpan
		}
		close(workCh)
		if err := ctxgroup.GroupWorkers(waveCtx, cloneDataParallelism, func(ctx context.Context, _ int) error {
			for s := range workCh {
				rangeCtx, rangeSp := tracing.ChildSpan(ctx, "clusterfork.clone-data-range")
				err := sendCloneData(rangeCtx, db, s, srcPrefix, dstPrefix)
				rangeSp.Finish()
				if err != nil {
					return errors.Wrapf(err, "CloneData on source range %s", s)
				}
			}
			return nil
		}); err != nil {
			waveSp.Finish()
			return err
		}
		var next []todoItem
		for _, t := range todo {
			err := unlockWithRetry(waveCtx, db, t.dstStartKey)
			if err == nil {
				continue
			}
			if !strings.Contains(err.Error(), "clone is incomplete") {
				waveSp.Finish()
				return errors.Wrapf(err, "unlock destination range starting at %s", t.dstStartKey)
			}
			log.Dev.Infof(waveCtx,
				"wave %d: unlock %s incomplete; re-queueing for next wave: %v",
				wave, t.dstStartKey, err)
			next = append(next, t)
		}
		waveSp.Finish()
		todo = next
	}
	if len(todo) > 0 {
		return errors.Newf(
			"after %d waves, %d destination ranges still incomplete; first: %s",
			maxWaves, len(todo), todo[0].dstStartKey)
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

// relocateWithRetry calls AdminRelocateRange in a bounded retry loop.
// AdminRelocateRange's "descriptor changed" check fires when the
// allocator/replicate queue races with our orchestration on the
// destination range — we set the InconsistencyLease bit on dst ranges
// to keep the consistency / merge / replica-GC queues hands-off, but
// the replicate queue still rebalances to satisfy zone configs. Each
// retry re-issues against the latest descriptor; AdminRelocateRange is
// idempotent for already-correct placements, so successive attempts
// converge.
func relocateWithRetry(
	ctx context.Context,
	db *kv.DB,
	startKey roachpb.Key,
	voters, nonVoters []roachpb.ReplicationTarget,
) error {
	opts := retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     2,
		MaxRetries:     10,
	}
	var lastErr error
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		lastErr = db.AdminRelocateRange(
			ctx, startKey, voters, nonVoters,
			true, /* transferLeaseToFirstVoter */
		)
		if lastErr == nil {
			return nil
		}
		log.Dev.Infof(ctx, "AdminRelocateRange at %s failed (will retry): %v", startKey, lastErr)
	}
	return lastErr
}

// unlockWithRetry clears the InconsistencyLease on a range in a short
// inner retry loop intended only for the in-window raft propagation
// race: sendCloneData returns once the leaseholder applies, but
// followers may take tens of milliseconds to catch up before the
// unlock-time verifyAllReplicasComplete fanout sees them as complete.
// A few short-backoff attempts close that window cheaply.
//
// Persistent incompleteness (e.g., a dst replica that wasn't
// initialized on a target store at CloneData apply time, so the apply
// silently no-op'd) is left for the caller's outer wave loop, which
// re-fires CloneData and retries. Don't bloat this inner loop trying
// to handle it — successive identical retries here can't make a missing
// apply re-apply.
//
// The retry only fires on the "clone is incomplete" shape; other errors
// (lease problems, descriptor changes, etc.) are returned immediately —
// retrying those would mask real failures.
func unlockWithRetry(ctx context.Context, db *kv.DB, startKey roachpb.Key) error {
	opts := retry.Options{
		InitialBackoff: 25 * time.Millisecond,
		MaxBackoff:     250 * time.Millisecond,
		Multiplier:     2,
		MaxRetries:     3,
	}
	var lastErr error
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		lastErr = setReplicaInconsistency(ctx, db, startKey, hlc.Timestamp{},
			false /* ackAborted */, roachpb.Span{})
		if lastErr == nil {
			return nil
		}
		// "clone is incomplete on N/M replicas" comes from
		// verifyAllReplicasComplete and resolves once raft propagates
		// the CloneData apply to lagging followers. Match by substring
		// rather than introducing a typed error to keep this scoped.
		if !strings.Contains(lastErr.Error(), "clone is incomplete") {
			return lastErr
		}
		log.Dev.Infof(ctx, "unlock at %s waiting for follower CloneData apply: %v", startKey, lastErr)
	}
	return lastErr
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
// scope is the keyspace the caller intends to clone into the locked
// range; it bounds the missing-spans bookkeeping the unlock-time
// completeness check consumes. Empty (zero-valued) scope means "the
// entire range" (back-compat).
func setReplicaInconsistency(
	ctx context.Context,
	db *kv.DB,
	key roachpb.Key,
	untilTS hlc.Timestamp,
	ackAborted bool,
	scope roachpb.Span,
) error {
	req := &kvpb.AdminSetReplicaInconsistencyRequest{
		RequestHeader: kvpb.RequestHeader{Key: key},
		UntilTS:       untilTS,
		AckAborted:    ackAborted,
		Scope:         scope,
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
//
// CloneData is sent as a range request scoped to a single source
// range. If the source range was split since the orchestration scanned
// the descriptor list, the request span no longer fits in the actual
// range and we receive RangeKeyMismatchError. The error carries the
// actual range bounds (the leftmost range our request touched); we
// recover by cloning that range's portion of srcSpan and recursing on
// the remainder. Each level handles one split; recursion bounds itself
// at the actual number of post-split ranges in srcSpan. Re-runs of an
// already-cloned span are safe because Pebble's VirtualClone is
// excise-on-clone-idempotent at the LSM level.
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
	err := db.Run(ctx, b)
	if err == nil {
		return nil
	}
	var mismatch *kvpb.RangeKeyMismatchError
	if !errors.As(err, &mismatch) {
		return err
	}
	ri, riErr := mismatch.MismatchedRange()
	if riErr != nil {
		return errors.WithSecondaryError(err, riErr)
	}
	splitAt := ri.Desc.EndKey.AsRawKey()
	// Defensive: the actual range's end should fall strictly inside our
	// span (mismatch implies srcSpan extends past the range's end). If
	// it doesn't, fall through with the original error rather than
	// looping forever.
	if bytes.Compare(splitAt, srcSpan.Key) <= 0 ||
		bytes.Compare(splitAt, srcSpan.EndKey) >= 0 {
		return err
	}
	log.Dev.Infof(ctx, "CloneData on %s hit split at %s; subdividing", srcSpan, splitAt)
	if err := sendCloneData(ctx, db,
		roachpb.Span{Key: srcSpan.Key, EndKey: splitAt}, srcPrefix, dstPrefix); err != nil {
		return err
	}
	return sendCloneData(ctx, db,
		roachpb.Span{Key: splitAt, EndKey: srcSpan.EndKey}, srcPrefix, dstPrefix)
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
