// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/splits"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestAdminSplitBatched_N2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	// Split off an empty range to work with.
	baseKey := roachpb.Key("batch-split-test")
	_, _, err := tc.SplitRange(baseKey)
	require.NoError(t, err)
	tc.AddVotersOrFatal(t, baseKey, tc.Targets(1, 2)...)
	tc.WaitForVotersOrFatal(t, baseKey, tc.Targets(1, 2)...)

	splitKeys := []roachpb.Key{
		roachpb.Key("batch-split-test-a"),
		roachpb.Key("batch-split-test-b"),
	}

	exp := hlc.Timestamp{WallTime: time.Now().Add(24 * time.Hour).UnixNano()}
	require.NoError(t, splits.AdminSplitMany(ctx, tc.Server(0).DB(), splitKeys, exp))

	// Verify 3 ranges exist: [baseKey, splitKeys[0]), [splitKeys[0],
	// splitKeys[1]), [splitKeys[1], ...).
	for storeIdx := 0; storeIdx < 3; storeIdx++ {
		store, err := tc.Server(storeIdx).GetStores().(*kvserver.Stores).GetStore(
			tc.Server(storeIdx).GetFirstStoreID())
		require.NoError(t, err)

		for _, sk := range splitKeys {
			repl := store.LookupReplica(roachpb.RKey(sk))
			require.NotNil(t, repl, "store %d: replica for key %s not found", storeIdx, sk)
			require.True(t, repl.Desc().StartKey.Equal(roachpb.RKey(sk)),
				"store %d: expected range starting at %s, got %s", storeIdx, sk, repl.Desc().StartKey)
		}

		// Verify contiguity of descriptors.
		repl0 := store.LookupReplica(roachpb.RKey(baseKey))
		require.NotNil(t, repl0)
		require.True(t, repl0.Desc().EndKey.Equal(roachpb.RKey(splitKeys[0])))

		repl1 := store.LookupReplica(roachpb.RKey(splitKeys[0]))
		require.NotNil(t, repl1)
		require.True(t, repl1.Desc().EndKey.Equal(roachpb.RKey(splitKeys[1])))

		// RHS replicas should be present.
		for _, sk := range splitKeys {
			repl := store.LookupReplica(roachpb.RKey(sk))
			require.Len(t, repl.Desc().Replicas().Descriptors(), 3,
				"store %d: key %s: expected 3 replicas", storeIdx, sk)
		}
	}
}

func TestAdminSplitBatched_N10(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testAdminSplitBatchedN(t, 10)
}

func TestAdminSplitBatched_N100(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testAdminSplitBatchedN(t, 100)
}

func TestAdminSplitBatched_N10000(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testAdminSplitBatchedN(t, 10000)
}

func testAdminSplitBatchedN(t *testing.T, n int) {
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
				DisableSplitQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	// Split off a dedicated empty range.
	prefix := fmt.Sprintf("batch-split-n%d-", n)
	baseKey := roachpb.Key(prefix + "000000")
	_, _, err = s.SplitRange(baseKey)
	require.NoError(t, err)

	// Create a sentinel end key to cap the range.
	endKey := roachpb.Key(prefix + "zzzzzz")
	_, _, err = s.SplitRange(endKey)
	require.NoError(t, err)

	splitKeys := make([]roachpb.Key, n)
	for i := range splitKeys {
		splitKeys[i] = roachpb.Key(fmt.Sprintf("%s%06d", prefix, i+1))
	}

	exp := hlc.Timestamp{WallTime: time.Now().Add(24 * time.Hour).UnixNano()}
	start := time.Now()
	require.NoError(t, splits.AdminSplitMany(ctx, s.DB(), splitKeys, exp))
	elapsed := time.Since(start)
	t.Logf("N=%d: batched split took %s (%.1f splits/sec)", n, elapsed,
		float64(n)/elapsed.Seconds())

	// Verify all ranges exist.
	for i, sk := range splitKeys {
		repl := store.LookupReplica(roachpb.RKey(sk))
		require.NotNil(t, repl, "split key %d (%s): replica not found", i, sk)
		require.True(t, repl.Desc().StartKey.Equal(roachpb.RKey(sk)),
			"split key %d (%s): expected range starting at %s, got %s",
			i, sk, sk, repl.Desc().StartKey)
	}

	// Spot-check contiguity at a few points.
	if n >= 2 {
		repl := store.LookupReplica(roachpb.RKey(splitKeys[0]))
		require.True(t, repl.Desc().EndKey.Equal(roachpb.RKey(splitKeys[1])),
			"expected contiguous ranges at keys 0 and 1")
	}
	if n >= 100 {
		repl := store.LookupReplica(roachpb.RKey(splitKeys[49]))
		require.True(t, repl.Desc().EndKey.Equal(roachpb.RKey(splitKeys[50])),
			"expected contiguous ranges at keys 49 and 50")
	}
}
