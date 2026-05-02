// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterfork_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestCreateTenantFromTenant_EndToEnd exercises the synchronous DDL
// CREATE VIRTUAL CLUSTER <dst> FROM <src> against a single-node
// TestServer. The grammar entry point allocates the destination tenant
// itself (vs the crdb_internal.fork_tenant builtin which requires a
// pre-allocated dst), so this test only allocates a source and lets the
// statement do the rest.
//
// Requires building with --local-pebble while the cockroachdb/pebble
// `clone` branch is the one carrying VirtualClone.
func TestCreateTenantFromTenant_EndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(ctx)

	systemDB := sqlutils.MakeSQLRunner(s.SystemLayer().SQLConn(t))
	kvDB := s.SystemLayer().DB()

	// Pebble's VirtualClone iteration path does not yet support blob value
	// handles produced by value separation; force separation off so the
	// source LSM contains only inline values.
	systemDB.Exec(t, `SET CLUSTER SETTING storage.value_separation.enabled = false`)

	var srcID int64
	systemDB.QueryRow(t,
		`SELECT crdb_internal.create_tenant('{"name":"ddl-src","service_mode":"external"}'::jsonb)`,
	).Scan(&srcID)
	srcTenant, err := roachpb.MakeTenantID(uint64(srcID))
	require.NoError(t, err)
	srcPrefix := keys.MakeTenantPrefix(srcTenant)

	// Seed source KV.
	wantPairs := map[string]string{
		"alpha": "one",
		"beta":  "two",
		"gamma": "three",
	}
	for k, v := range wantPairs {
		key := append(append(roachpb.Key{}, srcPrefix...), []byte(k)...)
		require.NoError(t, kvDB.Put(ctx, key, []byte(v)))
	}

	// Run the DDL: CREATE VIRTUAL CLUSTER ddl-dst FROM ddl-src.
	systemDB.Exec(t, `CREATE VIRTUAL CLUSTER "ddl-dst" FROM "ddl-src"`)

	// Look up the freshly allocated destination tenant ID. The DDL
	// itself is Ack-only (no return rows), so we resolve by name.
	var dstID int64
	systemDB.QueryRow(t,
		`SELECT id FROM system.tenants WHERE name = 'ddl-dst'`,
	).Scan(&dstID)
	require.NotZero(t, dstID, "ddl-dst tenant should have been allocated")
	dstTenant, err := roachpb.MakeTenantID(uint64(dstID))
	require.NoError(t, err)
	dstPrefix := keys.MakeTenantPrefix(dstTenant)
	t.Logf("ddl allocated dst tenant %d (src=%d)", dstID, srcID)

	// Verify the same KV pairs are visible under the dst tenant prefix.
	for k, want := range wantPairs {
		dstKey := append(append(roachpb.Key{}, dstPrefix...), []byte(k)...)
		got, err := kvDB.Get(ctx, dstKey)
		require.NoError(t, err, "reading dst key %s", dstKey)
		require.NotNil(t, got.Value, "no value at dst key %s", dstKey)
		require.Equal(t, want, string(got.ValueBytes()),
			"value mismatch at dst key %q (orig src key %q)", dstKey, fmt.Sprintf("%s/%s", srcPrefix, k))
	}
}

// TestForkTenant_MultiNode runs CREATE VIRTUAL CLUSTER ... FROM ... against
// real multi-node TestClusters. Two configurations:
//
//   - "n=3": three nodes, one source range with all three stores as replicas.
//     Exercises the basic multi-replica path: every CloneData apply runs on
//     three different stores and the dst replicas read from those same stores.
//
//   - "n=9_disjoint": nine nodes, three source ranges placed on disjoint
//     replica triplets ({1,2,3}, {4,5,6}, {7,8,9}). No single store hosts a
//     replica of more than one range. Exercises the orchestration's
//     per-range AdminRelocateRange + per-range CloneData flow with
//     leaseholders and apply paths spread across the cluster, the way a
//     production fork would look.
//
// Both subtests verify all seeded user keys round-trip through the dst
// tenant. Multi-replica configurations also surface any gap between
// "leaseholder applied CloneData" (orchestration's current synchronization
// boundary) and "every src replica applied CloneData" (what the dst
// followers actually need).
//
// Requires --local-pebble while VirtualClone lives on cockroachdb/pebble's
// `clone` branch.
func TestForkTenant_MultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// shapeSource installs the source-side keyspace layout for this
	// configuration: any required splits, replica placement, and the
	// returned wantPairs map of src-relative key → value to seed and
	// later verify on the dst.
	type shaper func(t *testing.T, tc *testcluster.TestCluster, srcPrefix roachpb.Key) map[string]string

	tests := []struct {
		name        string
		nodes       int
		replication base.TestClusterReplicationMode
		shapeSource shaper
	}{
		{
			name:        "n=3",
			nodes:       3,
			replication: base.ReplicationAuto,
			// Auto replication factor 3 puts the lone source range on all
			// three stores; nothing more to set up.
			shapeSource: func(_ *testing.T, _ *testcluster.TestCluster, _ roachpb.Key) map[string]string {
				return map[string]string{
					"alpha": "one",
					"beta":  "two",
					"gamma": "three",
				}
			},
		},
		{
			name:        "n=9_disjoint",
			nodes:       9,
			replication: base.ReplicationManual,
			shapeSource: func(t *testing.T, tc *testcluster.TestCluster, srcPrefix roachpb.Key) map[string]string {
				// Three user-data chunks, split at lowercase byte
				// boundaries. ASCII lowercase sorts below CRDB's typed
				// table-ID byte encoding, so every source system table
				// (bootstrapped by CREATE VIRTUAL CLUSTER) lands in the
				// rightmost range — fine for the test; we only verify the
				// user keys round-trip.
				splits := []roachpb.Key{
					append(append(roachpb.Key{}, srcPrefix...), []byte("ccc")...),
					append(append(roachpb.Key{}, srcPrefix...), []byte("mmm")...),
				}
				db := tc.Server(0).DB()
				for _, s := range splits {
					_, _, err := tc.SplitRange(s)
					require.NoErrorf(t, err, "split src at %s", s)
				}

				// Three disjoint replica triplets. tc.Targets uses
				// 0-based indexing.
				triplets := [][]int{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}}

				// Range starts: tenant prefix, then each split key.
				rangeStarts := []roachpb.Key{srcPrefix, splits[0], splits[1]}

				for i, start := range rangeStarts {
					targets := tc.Targets(triplets[i]...)
					// AdminRelocateRange to set the exact replica set,
					// transferring the lease to the first voter so the
					// CloneData command lands on a leaseholder in the
					// intended triplet.
					require.NoErrorf(t, db.AdminRelocateRange(
						ctx, start, targets, nil, /* nonVoters */
						true, /* transferLeaseToFirstVoter */
					), "relocate src range starting at %s to %v", start, triplets[i])
				}

				// Seed at least one key per range so every range is
				// non-empty and every clone path is exercised. The keys
				// are picked to fall on either side of the split
				// boundaries so they distribute across the three ranges.
				return map[string]string{
					"aaa": "left-1",
					"bbb": "left-2",
					"ddd": "mid-1",
					"eee": "mid-2",
					"ppp": "right-1",
					"qqq": "right-2",
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := testcluster.StartTestCluster(t, tc.nodes, base.TestClusterArgs{
				ReplicationMode: tc.replication,
				ServerArgs: base.TestServerArgs{
					DefaultTestTenant: base.TestControlsTenantsExplicitly,
				},
			})
			defer cluster.Stopper().Stop(ctx)

			systemDB := sqlutils.MakeSQLRunner(cluster.Server(0).SystemLayer().SQLConn(t))
			kvDB := cluster.Server(0).SystemLayer().DB()

			// Pebble's VirtualClone path does not yet handle blob value
			// handles; force value separation off to keep source SSTs
			// inline-only.
			systemDB.Exec(t, `SET CLUSTER SETTING storage.value_separation.enabled = false`)

			// Allocate the source via the SQL grammar so the source
			// bootstraps a normal tenant (system tables and all).
			systemDB.Exec(t, `CREATE VIRTUAL CLUSTER "src"`)

			var srcID int64
			systemDB.QueryRow(t,
				`SELECT id FROM system.tenants WHERE name = 'src'`,
			).Scan(&srcID)
			srcTenant, err := roachpb.MakeTenantID(uint64(srcID))
			require.NoError(t, err)
			srcPrefix := keys.MakeTenantPrefix(srcTenant)

			wantPairs := tc.shapeSource(t, cluster, srcPrefix)

			for k, v := range wantPairs {
				key := append(append(roachpb.Key{}, srcPrefix...), []byte(k)...)
				require.NoError(t, kvDB.Put(ctx, key, []byte(v)))
			}

			systemDB.Exec(t, `CREATE VIRTUAL CLUSTER "dst" FROM "src"`)

			var dstID int64
			systemDB.QueryRow(t,
				`SELECT id FROM system.tenants WHERE name = 'dst'`,
			).Scan(&dstID)
			require.NotZero(t, dstID, "dst tenant should have been allocated")
			dstTenant, err := roachpb.MakeTenantID(uint64(dstID))
			require.NoError(t, err)
			dstPrefix := keys.MakeTenantPrefix(dstTenant)
			t.Logf("forked src=%d → dst=%d", srcID, dstID)

			verifyDstKeys(t, ctx, kvDB, srcPrefix, dstPrefix, wantPairs)
		})
	}
}

// verifyDstKeys reads every (key, value) under dstPrefix and asserts
// it matches the seeded src content. The error message includes the
// equivalent src key so a mismatch immediately points at which clone
// path went wrong.
func verifyDstKeys(
	t *testing.T,
	ctx context.Context,
	kvDB *kv.DB,
	srcPrefix, dstPrefix roachpb.Key,
	wantPairs map[string]string,
) {
	t.Helper()
	for k, want := range wantPairs {
		dstKey := append(append(roachpb.Key{}, dstPrefix...), []byte(k)...)
		got, err := kvDB.Get(ctx, dstKey)
		require.NoError(t, err, "reading dst key %s", dstKey)
		require.NotNil(t, got.Value, "no value at dst key %s", dstKey)
		require.Equal(t, want, string(got.ValueBytes()),
			"value mismatch at dst key %q (orig src key %q)",
			dstKey, fmt.Sprintf("%s/%s", srcPrefix, k))
	}
}
