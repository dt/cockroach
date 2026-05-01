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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/splits"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// BenchmarkAdminSplitBatched measures batched multi-key split throughput
// against status-quo parallel per-key AdminSplit.
func BenchmarkAdminSplitBatched(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	for _, n := range []int{1, 10, 100, 1000, 10000, 20000} {
		b.Run(fmt.Sprintf("batched/N=%d", n), func(b *testing.B) {
			benchBatchedSplit(b, n)
		})
		b.Run(fmt.Sprintf("parallel/N=%d", n), func(b *testing.B) {
			benchParallelSplit(b, n)
		})
	}
}

func benchBatchedSplit(b *testing.B, n int) {
	ctx := context.Background()
	s := serverutils.StartServerOnly(b, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
				DisableSplitQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(b, err)

	exp := hlc.Timestamp{WallTime: time.Now().Add(24 * time.Hour).UnixNano()}

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		b.StopTimer()
		prefix := fmt.Sprintf("bench-batched-%d-%d-", n, iter)
		baseKey := roachpb.Key(prefix + "000000")
		endKey := roachpb.Key(prefix + "zzzzzz")
		_, _, err = s.SplitRange(baseKey)
		require.NoError(b, err)
		_, _, err = s.SplitRange(endKey)
		require.NoError(b, err)

		splitKeys := make([]roachpb.Key, n)
		for i := range splitKeys {
			splitKeys[i] = roachpb.Key(fmt.Sprintf("%s%06d", prefix, i+1))
		}
		b.StartTimer()

		require.NoError(b, splits.AdminSplitMany(ctx, s.DB(), splitKeys, exp))

		b.StopTimer()
		// Verify a sample of splits actually landed.
		repl := store.LookupReplica(roachpb.RKey(splitKeys[0]))
		require.NotNil(b, repl)
		require.True(b, repl.Desc().StartKey.Equal(roachpb.RKey(splitKeys[0])))
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(n*b.N)/b.Elapsed().Seconds(), "splits/sec")
}

func benchParallelSplit(b *testing.B, n int) {
	ctx := context.Background()
	s := serverutils.StartServerOnly(b, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
				DisableSplitQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(b, err)

	exp := hlc.Timestamp{WallTime: time.Now().Add(24 * time.Hour).UnixNano()}

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		b.StopTimer()
		prefix := fmt.Sprintf("bench-parallel-%d-%d-", n, iter)
		baseKey := roachpb.Key(prefix + "000000")
		endKey := roachpb.Key(prefix + "zzzzzz")
		_, _, err = s.SplitRange(baseKey)
		require.NoError(b, err)
		_, _, err = s.SplitRange(endKey)
		require.NoError(b, err)

		splitKeys := make([]roachpb.Key, n)
		for i := range splitKeys {
			splitKeys[i] = roachpb.Key(fmt.Sprintf("%s%06d", prefix, i+1))
		}
		b.StartTimer()

		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(64)
		for _, key := range splitKeys {
			key := key
			g.Go(func() error {
				req := &kvpb.AdminSplitRequest{
					RequestHeader:  kvpb.RequestHeader{Key: key},
					SplitKey:       key,
					ExpirationTime: exp,
				}
				_, pErr := kv.SendWrapped(gctx, s.DB().NonTransactionalSender(), req)
				if pErr != nil {
					return pErr.GoError()
				}
				return nil
			})
		}
		require.NoError(b, g.Wait())

		b.StopTimer()
		repl := store.LookupReplica(roachpb.RKey(splitKeys[0]))
		require.NotNil(b, repl)
		require.True(b, repl.Desc().StartKey.Equal(roachpb.RKey(splitKeys[0])))
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(n*b.N)/b.Elapsed().Seconds(), "splits/sec")
}
