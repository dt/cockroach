// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revert

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestRevertSpansFanout tests RevertSpansFanout using the same
// sequence of modifications used in TestTableRollback.
//
// We use more splits and scatter the ranges.
func TestRevertSpansFanout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "takes too long under race")

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{UseDatabase: "test"},
	})
	defer tc.Stopper().Stop(context.Background())
	s := tc.ApplicationLayer(0)
	sqlDB := tc.Conns[0]

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, "CREATE DATABASE IF NOT EXISTS test")
	db.Exec(t, "CREATE TABLE test (k INT PRIMARY KEY, rev INT DEFAULT 0, INDEX (rev))")

	kvDB := s.DB()
	desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, execCfg.Codec, "test", "test")
	span := desc.TableSpan(execCfg.Codec)
	tableID := desc.GetID()

	// Fill a table with some rows plus some revisions to those rows.
	const (
		initRows  = 1000
		extraRows = 500
		splits    = 100
	)

	db.Exec(t, "INSERT INTO test (k) SELECT generate_series(1, $1)", initRows)
	db.Exec(t, "UPDATE test SET rev = 1 WHERE k % 3 = 0")
	db.Exec(t, "DELETE FROM test WHERE k % 10 = 0")
	before, ts := fingerprintTableNoHistory(t, db, tableID, "")
	targetTime, err := hlc.ParseHLC(ts)
	require.NoError(t, err)

	beforeNumRows := db.QueryStr(t, "SELECT count(*) FROM test")

	// Make some more edits: delete some rows and edit others, insert into some of
	// the gaps made between previous rows, edit a large swath of rows and add a
	// large swath of new rows as well.
	db.Exec(t, "DELETE FROM test WHERE k % 5 = 2")
	db.Exec(t, "INSERT INTO test (k, rev) SELECT generate_series(10, $1, 10), 10", initRows)
	db.Exec(t, "INSERT INTO test (k, rev) SELECT generate_series($1+1, $1+$2, 1), $2", initRows, extraRows)
	db.Exec(t, "UPDATE test SET rev = 2 WHERE k % 4 = 0")
	db.Exec(t, "UPDATE test SET rev = 4 WHERE k > 150 and k < 350")

	var splitPointsBuf strings.Builder
	splitIncrement := (initRows + extraRows) / splits
	for i := 0; i < 100; i++ {
		splitPoint := splitIncrement * i
		if i == 0 {
			fmt.Fprintf(&splitPointsBuf, "(%d)", splitPoint)
		} else {
			fmt.Fprintf(&splitPointsBuf, ", (%d)", splitPoint)
		}
	}

	// Split and scatter the table.
	db.Exec(t, fmt.Sprintf("ALTER TABLE test SPLIT AT VALUES %s", splitPointsBuf.String()))
	db.Exec(t, "ALTER TABLE test SCATTER")

	rsCtx, close := sql.MakeJobExecContext(ctx, "revert-spans", username.RootUserName(), &sql.MemoryMetrics{}, &execCfg)
	defer close()

	verifyRevert := func() {
		reverted, _ := fingerprintTableNoHistory(t, db, tableID, "")
		require.Equal(t, before, reverted, "expected reverted table after edits to match before")
		db.CheckQueryResults(t, "SELECT count(*) FROM test", beforeNumRows)
	}

	t.Run("revert-fanout-with-callback", func(t *testing.T) {
		require.NoError(t,
			RevertSpansFanout(ctx,
				kvDB, rsCtx, []roachpb.Span{span}, targetTime, false, 10,
				func(context.Context, roachpb.Span) error {
					return nil
				}))
		verifyRevert()
	})
	t.Run("revert-without-callback", func(t *testing.T) {
		db.Exec(t, "DELETE FROM test WHERE k % 5 = 2")
		require.NoError(t,
			RevertSpansFanout(ctx,
				kvDB, rsCtx, []roachpb.Span{span}, targetTime, false, 10, nil))
		verifyRevert()
	})
	t.Run("revert-with-1-worker", func(t *testing.T) {
		db.Exec(t, "DELETE FROM test WHERE k % 5 = 2")
		db.Exec(t, "SET CLUSTER SETTING sql.revert.max_span_parallelism = 1")
		defer db.Exec(t, "RESET CLUSTER SETTING sql.revert.max_span_parallelism")
		require.NoError(t,
			RevertSpansFanout(ctx,
				kvDB, rsCtx, []roachpb.Span{span}, targetTime, false, 10,
				func(context.Context, roachpb.Span) error {
					return nil
				}))
		verifyRevert()

	})
	t.Run("revert-with-failing-callback", func(t *testing.T) {
		require.Error(t,
			RevertSpansFanout(ctx,
				kvDB, rsCtx, []roachpb.Span{span}, targetTime, false, 10,
				func(context.Context, roachpb.Span) error {
					return errors.New("callback failed")
				}), "callbackFailed")
		// No use verifying, the revert failed and we didn't
		// make any changes anyway.
	})
}

// fingerprintTableNoHistory returns the fingerprint of the given
// table as well as the time the fingerprint was taken at.
//
// If a non-empty ts string is specified, the fingerprint will be
// taken at that timestamp.
func fingerprintTableNoHistory(
	t *testing.T, db *sqlutils.SQLRunner, tableID catid.DescID, ts string,
) (int, string) {
	t.Helper()
	query := "SELECT cluster_logical_timestamp(), * FROM crdb_internal.fingerprint(crdb_internal.table_span($1), 0::timestamptz, false)"
	if ts != "" {
		query = fmt.Sprintf("%s AS OF SYSTEM TIME %s", query, ts)
	}

	var fingerprint int
	var timestamp string
	db.QueryRow(t, query, tableID).Scan(&timestamp, &fingerprint)
	return fingerprint, timestamp
}

func TestRevertGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	codec := srv.ApplicationLayer().Codec()

	req1 := kvpb.RevertRangeRequest{
		RequestHeader: kvpb.RequestHeader{Key: bootstrap.TestingUserTableDataMin(codec), EndKey: codec.TenantEndKey()},
		TargetTime:    hlc.Timestamp{WallTime: -1},
	}
	req2 := req1
	req2.IgnoreGcThreshold = true

	_, pErr := kv.SendWrapped(ctx, kvDB.NonTransactionalSender(), &req1)
	if !testutils.IsPError(pErr, "must be after replica GC threshold") {
		t.Fatalf(`expected "must be after replica GC threshold" error got: %+v`, pErr)
	}

	_, pErr = kv.SendWrapped(ctx, kvDB.NonTransactionalSender(), &req2)
	require.Nil(t, pErr)
}

// TestRevertRangeWithMVCCHistoryTruncation creates a SQL table, writes rows,
// captures a timestamp, writes more rows, and then sends a RevertRange with
// UseMVCCHistoryTruncation to verify the revert masks newer keys.
func TestRevertRangeWithMVCCHistoryTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)
	codec := srv.ApplicationLayer().Codec()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	runner.Exec(t, "CREATE TABLE t (k INT PRIMARY KEY, v STRING)")
	runner.Exec(t, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")

	targetTime := kvDB.Clock().Now()

	runner.Exec(t, "UPDATE t SET v = 'a2' WHERE k = 1")
	runner.Exec(t, "INSERT INTO t VALUES (4, 'd'), (5, 'e')")

	runner.CheckQueryResults(t, "SELECT count(*) FROM t", [][]string{{"5"}})

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(
		kvDB, codec, "defaultdb", "t",
	)
	tableSpan := tableDesc.TableSpan(codec)

	req := kvpb.RevertRangeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: tableSpan.Key, EndKey: tableSpan.EndKey,
		},
		TargetTime:               targetTime,
		UseMVCCHistoryTruncation: true,
	}
	_, pErr := kv.SendWrapped(ctx, kvDB.NonTransactionalSender(), &req)
	require.Nil(t, pErr)

	// After the revert, only the 3 original rows should be visible and
	// key 1 should have its original value.
	runner.CheckQueryResults(t, "SELECT k, v FROM t ORDER BY k",
		[][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
}

// TestRevertRangeWithMVCCHistoryTruncationMultiTable reproduces a Pebble
// fatal seen when a RevertRange with UseMVCCHistoryTruncation is applied
// over a span that crosses multiple table boundaries on a non-trivial LSM.
// The mask processing for files spanning several tables can produce a
// manifest version with inverted range-key bounds, which versionSet
// validation rejects fatally during apply.
//
// The reproduction here is deliberately minimal compared to the end-to-end
// PCR cutover roachtest that originally surfaced the bug: it sidesteps
// replication entirely and just drives a multi-table RevertRange against a
// flushed LSM. If the Pebble manifest validation fires, the testserver's
// in-process Pebble logger fatals and exits the process, failing the test.
func TestRevertRangeWithMVCCHistoryTruncationMultiTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)
	codec := srv.ApplicationLayer().Codec()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	store, err := srv.GetStores().(*kvserver.Stores).GetStore(srv.GetFirstStoreID())
	require.NoError(t, err)
	eng := store.StateEngine()

	// Create several tables so the eventual RevertRange span crosses multiple
	// table boundaries. The bug requires the mask edit to straddle file-level
	// boundaries that fall on different table prefixes.
	const nTables = 4
	for i := 0; i < nTables; i++ {
		runner.Exec(t, fmt.Sprintf("CREATE TABLE t%d (k INT PRIMARY KEY, v STRING)", i))
		runner.Exec(t, fmt.Sprintf("INSERT INTO t%d VALUES (1,'a'),(2,'b'),(3,'c')", i))
	}

	// Flush so the initial rows (and the schema-change side-effects) land in
	// L0 SSTs that span the table boundaries, rather than staying in the
	// memtable where the suffix-mask processing wouldn't see them.
	require.NoError(t, eng.Flush())

	targetTime := kvDB.Clock().Now()

	// Drive more writes past the target time across every table, then flush
	// again so the post-target versions also live in SSTs subject to masking.
	for i := 0; i < nTables; i++ {
		runner.Exec(t, fmt.Sprintf("UPDATE t%d SET v = 'updated' WHERE k = 1", i))
		runner.Exec(t, fmt.Sprintf("INSERT INTO t%d VALUES (4,'d'),(5,'e')", i))
	}
	require.NoError(t, eng.Flush())

	// Build a single span that covers all the tables we just wrote.
	firstDesc := desctestutils.TestingGetPublicTableDescriptor(
		kvDB, codec, "defaultdb", "t0",
	)
	lastDesc := desctestutils.TestingGetPublicTableDescriptor(
		kvDB, codec, "defaultdb", fmt.Sprintf("t%d", nTables-1),
	)
	span := roachpb.Span{
		Key:    firstDesc.TableSpan(codec).Key,
		EndKey: lastDesc.TableSpan(codec).EndKey,
	}

	// The bug fires inside apply (versionSet.UpdateVersionLocked after
	// DeleteSuffixRange), so SendWrapped won't return cleanly if the mask
	// produces an invalid manifest. A fatal here is the reproduction.
	req := kvpb.RevertRangeRequest{
		RequestHeader:            kvpb.RequestHeader{Key: span.Key, EndKey: span.EndKey},
		TargetTime:               targetTime,
		UseMVCCHistoryTruncation: true,
	}
	_, pErr := kv.SendWrapped(ctx, kvDB.NonTransactionalSender(), &req)
	require.Nil(t, pErr)

	// Drive a compaction too; some forms of the bug only surface when the
	// masked file is repacked rather than on the initial mask install.
	require.NoError(t, eng.Compact(ctx))

	// Sanity-check: every table should be back to its pre-target state.
	for i := 0; i < nTables; i++ {
		runner.CheckQueryResults(t,
			fmt.Sprintf("SELECT k, v FROM t%d ORDER BY k", i),
			[][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})
	}
}
