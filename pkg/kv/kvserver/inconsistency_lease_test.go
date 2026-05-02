// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// setReplicaInconsistency sends an AdminSetReplicaInconsistencyRequest to the
// range starting at startKey. untilTS is the new InconsistentReplicas value.
func setReplicaInconsistency(
	t *testing.T,
	ctx context.Context,
	db *kv.DB,
	startKey roachpb.Key,
	untilTS hlc.Timestamp,
	ackAborted bool,
) error {
	t.Helper()
	b := &kv.Batch{}
	b.AddRawRequest(&kvpb.AdminSetReplicaInconsistencyRequest{
		RequestHeader: kvpb.RequestHeader{Key: startKey},
		UntilTS:       untilTS,
		AckAborted:    ackAborted,
	})
	return db.Run(ctx, b)
}

// rangeDescriptor reads the RangeDescriptor for the range starting at
// startKey directly from meta2.
func rangeDescriptor(
	t *testing.T, ctx context.Context, db *kv.DB, startKey roachpb.Key,
) roachpb.RangeDescriptor {
	t.Helper()
	descKey := keys.RangeDescriptorKey(roachpb.RKey(startKey))
	kv, err := db.Get(ctx, descKey)
	require.NoError(t, err)
	require.NotNil(t, kv.Value, "no descriptor at %s", descKey)
	var desc roachpb.RangeDescriptor
	require.NoError(t, kv.Value.GetProto(&desc))
	return desc
}

func TestInconsistencyLease_LockUnlockRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	scratch := tc.ScratchRange(t)
	db := tc.Server(0).DB()

	// Initially the InconsistentReplicas field is empty.
	desc := rangeDescriptor(t, ctx, db, scratch)
	require.True(t, desc.InconsistentReplicas.IsEmpty())

	// Lock: install a future-dated InconsistentReplicas timestamp.
	lockUntil := db.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	require.NoError(t, setReplicaInconsistency(t, ctx, db, scratch, lockUntil, false))

	desc = rangeDescriptor(t, ctx, db, scratch)
	require.Equal(t, lockUntil, desc.InconsistentReplicas)

	// Re-issuing the same lock is idempotent.
	require.NoError(t, setReplicaInconsistency(t, ctx, db, scratch, lockUntil, false))

	// Unlock: clear the field.
	require.NoError(t, setReplicaInconsistency(t, ctx, db, scratch, hlc.Timestamp{}, false))

	desc = rangeDescriptor(t, ctx, db, scratch)
	require.True(t, desc.InconsistentReplicas.IsEmpty())

	// Idempotent unlock (already cleared).
	require.NoError(t, setReplicaInconsistency(t, ctx, db, scratch, hlc.Timestamp{}, false))
}

func TestInconsistencyLease_InheritedThroughSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	scratch := tc.ScratchRange(t)
	db := tc.Server(0).DB()

	// Lock the parent range.
	lockUntil := db.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	require.NoError(t, setReplicaInconsistency(t, ctx, db, scratch, lockUntil, false))

	// Split it.
	splitKey := append(scratch[:len(scratch):len(scratch)], 0xff)
	require.NoError(t, db.AdminSplit(ctx, splitKey, hlc.MaxTimestamp))

	// Both halves should have inherited the lock.
	leftDesc := rangeDescriptor(t, ctx, db, scratch)
	rightDesc := rangeDescriptor(t, ctx, db, splitKey)
	require.Equal(t, lockUntil, leftDesc.InconsistentReplicas,
		"left half lost InconsistentReplicas after split")
	require.Equal(t, lockUntil, rightDesc.InconsistentReplicas,
		"right half didn't inherit InconsistentReplicas from split parent")
}

func TestInconsistencyLease_RejectsNonAdminRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	scratch := tc.ScratchRange(t)
	db := tc.Server(0).DB()

	// Sanity: a write to the scratch span succeeds before we lock.
	probeKey := append(scratch[:len(scratch):len(scratch)], 'x')
	require.NoError(t, db.Put(ctx, probeKey, "before"))

	// Lock the range.
	lockUntil := db.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	require.NoError(t, setReplicaInconsistency(t, ctx, db, scratch, lockUntil, false))

	// A subsequent non-admin write must be rejected.
	err := db.Put(ctx, probeKey, "while-locked")
	require.Error(t, err)
	require.Contains(t, err.Error(), "mid-clone")

	// Unlock and verify writes work again.
	require.NoError(t, setReplicaInconsistency(t, ctx, db, scratch, hlc.Timestamp{}, false))
	require.NoError(t, db.Put(ctx, probeKey, "after"))
}
