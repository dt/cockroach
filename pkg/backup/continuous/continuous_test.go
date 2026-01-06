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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/continuous/continuouspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Mock implementations for testing coordinator in isolation
// =============================================================================

// testPersister is an in-memory mock implementation of frontierPersister.
type testPersister struct {
	mu       sync.Mutex
	frontier span.Frontier
	loadErr  error
	storeErr error
	stores   int // count of Store calls
}

var _ frontierPersister = (*testPersister)(nil)

func (p *testPersister) Load(ctx context.Context) (span.Frontier, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.loadErr != nil {
		return nil, false, p.loadErr
	}
	if p.frontier == nil {
		return nil, false, nil
	}
	return p.frontier, true, nil
}

func (p *testPersister) Store(ctx context.Context, frontier span.Frontier) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.storeErr != nil {
		return p.storeErr
	}
	p.frontier = frontier
	p.stores++
	return nil
}

// testCheckpointWriter is an in-memory mock implementation of checkpointWriter.
type testCheckpointWriter struct {
	mu          sync.Mutex
	checkpoints []*continuouspb.ResolvedCheckpoint
	writeErr    error
}

var _ checkpointWriter = (*testCheckpointWriter)(nil)

func (w *testCheckpointWriter) WriteCheckpoint(
	ctx context.Context, checkpoint *continuouspb.ResolvedCheckpoint,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writeErr != nil {
		return w.writeErr
	}
	w.checkpoints = append(w.checkpoints, checkpoint)
	return nil
}

// testStopChecker is a configurable mock implementation of stopChecker.
type testStopChecker struct {
	mu         sync.Mutex
	shouldStop bool
	stopErr    error
	checks     int // count of ShouldStop calls
}

var _ stopChecker = (*testStopChecker)(nil)

func (c *testStopChecker) ShouldStop(ctx context.Context) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checks++
	if c.stopErr != nil {
		return false, c.stopErr
	}
	return c.shouldStop, nil
}

// testShardRunner is a mock shard that can be controlled in tests.
type testShardRunner struct {
	runFunc func(ctx context.Context) error
}

var _ shardRunner = (*testShardRunner)(nil)

func (s *testShardRunner) Run(ctx context.Context) error {
	if s.runFunc != nil {
		return s.runFunc(ctx)
	}
	<-ctx.Done()
	return ctx.Err()
}

// TestEncodeDecodeEventKey tests that event keys can be encoded and decoded correctly.
func TestEncodeDecodeEventKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name    string
		ts      hlc.Timestamp
		seq     uint32
	}{
		{
			name: "zero timestamp",
			ts:   hlc.Timestamp{},
			seq:  0,
		},
		{
			name: "simple timestamp",
			ts:   hlc.Timestamp{WallTime: 1704067200000000000, Logical: 0},
			seq:  0,
		},
		{
			name: "timestamp with logical",
			ts:   hlc.Timestamp{WallTime: 1704067200000000000, Logical: 42},
			seq:  100,
		},
		{
			name: "max values",
			ts:   hlc.Timestamp{WallTime: 1<<63 - 1, Logical: 1<<31 - 1},
			seq:  1<<32 - 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key := encodeEventKey(tc.ts, tc.seq)
			require.Len(t, key, 16, "encoded key should be 16 bytes")

			decoded := decodeEventTimestamp(key)
			require.Equal(t, tc.ts.WallTime, decoded.WallTime, "wall time mismatch")
			require.Equal(t, tc.ts.Logical, decoded.Logical, "logical time mismatch")
		})
	}
}

// TestEncodeDecodeEventValue tests that event values can be encoded and decoded correctly.
func TestEncodeDecodeEventValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name      string
		eventType byte
		key       roachpb.Key
		value     []byte
		prevValue []byte
	}{
		{
			name:      "put event",
			eventType: 0,
			key:       roachpb.Key("test-key"),
			value:     []byte("test-value"),
			prevValue: []byte("prev-value"),
		},
		{
			name:      "delete event",
			eventType: 1,
			key:       roachpb.Key("deleted-key"),
			value:     nil,
			prevValue: []byte("old-value"),
		},
		{
			name:      "empty values",
			eventType: 0,
			key:       roachpb.Key("k"),
			value:     nil,
			prevValue: nil,
		},
		{
			name:      "large values",
			eventType: 0,
			key:       bytes.Repeat([]byte("k"), 1000),
			value:     bytes.Repeat([]byte("v"), 10000),
			prevValue: bytes.Repeat([]byte("p"), 5000),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			event := bufferedEvent{
				key: tc.key,
				value: roachpb.Value{
					RawBytes: tc.value,
				},
				prevValue: roachpb.Value{
					RawBytes: tc.prevValue,
				},
				ts: hlc.Timestamp{WallTime: 12345},
			}

			encoded := encodeEventValue(event)

			decoded, err := decodeEvent(encoded)
			require.NoError(t, err)

			// Check event type
			if len(tc.value) == 0 {
				require.Equal(t, byte(1), decoded.eventType, "delete should have type 1")
			} else {
				require.Equal(t, byte(0), decoded.eventType, "put should have type 0")
			}

			require.Equal(t, tc.key, roachpb.Key(decoded.key), "key mismatch")
			// Compare using bytes.Equal for nil vs empty slice compatibility
			if tc.value == nil {
				require.Empty(t, decoded.value, "value should be empty")
			} else {
				require.Equal(t, tc.value, decoded.value, "value mismatch")
			}
			if tc.prevValue == nil {
				require.Empty(t, decoded.prevValue, "prev value should be empty")
			} else {
				require.Equal(t, tc.prevValue, decoded.prevValue, "prev value mismatch")
			}
		})
	}
}

// TestDecodeEventErrors tests error cases for event decoding.
func TestDecodeEventErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name string
		data []byte
	}{
		{
			name: "empty data",
			data: []byte{},
		},
		{
			name: "too short for key length",
			data: []byte{0, 0, 0},
		},
		{
			name: "key length but no key",
			data: []byte{0, 0, 0, 0, 10}, // type + key_len(10) but no key data
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeEvent(tc.data)
			require.Error(t, err, "expected error for malformed data")
		})
	}
}

// TestResolvedCheckpointSerialization tests that ResolvedCheckpoint can be serialized and deserialized.
func TestResolvedCheckpointSerialization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	checkpoint := &continuouspb.ResolvedCheckpoint{
		ResolvedTs: hlc.Timestamp{WallTime: 1704067200000000000, Logical: 42},
		FileIds: []int64{
			1234567890123456,
			1234567890123457,
			1234567890123458,
		},
	}

	data, err := protoutil.Marshal(checkpoint)
	require.NoError(t, err)

	var decoded continuouspb.ResolvedCheckpoint
	err = protoutil.Unmarshal(data, &decoded)
	require.NoError(t, err)

	require.Equal(t, checkpoint.ResolvedTs, decoded.ResolvedTs)
	require.Equal(t, checkpoint.FileIds, decoded.FileIds)
}

// TestEventKeyOrdering tests that encoded event keys sort correctly by timestamp.
func TestEventKeyOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	timestamps := []hlc.Timestamp{
		{WallTime: 1000, Logical: 0},
		{WallTime: 1000, Logical: 1},
		{WallTime: 2000, Logical: 0},
		{WallTime: 3000, Logical: 5},
	}

	var keys [][]byte
	for i, ts := range timestamps {
		keys = append(keys, encodeEventKey(ts, uint32(i)))
	}

	// Verify keys are in ascending order
	for i := 1; i < len(keys); i++ {
		require.True(t, bytes.Compare(keys[i-1], keys[i]) < 0,
			"key %d should be less than key %d", i-1, i)
	}
}

// TestWriteAndReadSST tests writing events to an SST and reading them back.
func TestWriteAndReadSST(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Create test events
	events := []bufferedEvent{
		{
			key:   roachpb.Key("key1"),
			value: roachpb.Value{RawBytes: []byte("value1")},
			ts:    hlc.Timestamp{WallTime: 1000},
		},
		{
			key:   roachpb.Key("key2"),
			value: roachpb.Value{RawBytes: []byte("value2")},
			ts:    hlc.Timestamp{WallTime: 2000},
		},
		{
			key:       roachpb.Key("key3"),
			value:     roachpb.Value{}, // delete
			prevValue: roachpb.Value{RawBytes: []byte("old-value3")},
			ts:        hlc.Timestamp{WallTime: 3000},
		},
	}

	// Write events to SST
	var sstBuf objstorage.MemObj
	settings := cluster.MakeTestingClusterSettings()
	sstWriter := storage.MakeTransportSSTWriter(ctx, settings, &sstBuf)

	for i, event := range events {
		key := encodeEventKey(event.ts, uint32(i))
		value := encodeEventValue(event)
		err := sstWriter.PutUnversioned(key, value)
		require.NoError(t, err)
	}

	err := sstWriter.Finish()
	require.NoError(t, err)
	sstWriter.Close()

	// Read events back from SST
	iter, err := storage.NewMemSSTIterator(sstBuf.Data(), false, storage.IterOptions{
		LowerBound: roachpb.KeyMin,
		UpperBound: roachpb.KeyMax,
	})
	require.NoError(t, err)
	defer iter.Close()

	var readEvents []decodedEvent
	var readTimestamps []hlc.Timestamp

	iter.SeekGE(storage.MVCCKey{Key: roachpb.KeyMin})
	for {
		ok, err := iter.Valid()
		require.NoError(t, err)
		if !ok {
			break
		}

		sstKey := iter.UnsafeKey()
		ts := decodeEventTimestamp(sstKey.Key)
		readTimestamps = append(readTimestamps, ts)

		sstValue, err := iter.UnsafeValue()
		require.NoError(t, err)

		event, err := decodeEvent(sstValue)
		require.NoError(t, err)
		readEvents = append(readEvents, event)

		iter.Next()
	}

	// Verify we read the right number of events
	require.Len(t, readEvents, len(events))

	// Verify timestamps
	require.Equal(t, hlc.Timestamp{WallTime: 1000}, readTimestamps[0])
	require.Equal(t, hlc.Timestamp{WallTime: 2000}, readTimestamps[1])
	require.Equal(t, hlc.Timestamp{WallTime: 3000}, readTimestamps[2])

	// Verify event contents
	require.Equal(t, roachpb.Key("key1"), roachpb.Key(readEvents[0].key))
	require.Equal(t, []byte("value1"), readEvents[0].value)

	require.Equal(t, roachpb.Key("key2"), roachpb.Key(readEvents[1].key))
	require.Equal(t, []byte("value2"), readEvents[1].value)

	require.Equal(t, roachpb.Key("key3"), roachpb.Key(readEvents[2].key))
	require.Equal(t, byte(1), readEvents[2].eventType) // delete
}

// TestDecodeEventTimestampShortKey tests that short keys are handled gracefully.
func TestDecodeEventTimestampShortKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Keys shorter than 12 bytes should return zero timestamp
	shortKey := []byte{1, 2, 3, 4, 5}
	ts := decodeEventTimestamp(shortKey)
	require.Equal(t, hlc.Timestamp{}, ts)
}

// TestInvertedTimestampOrdering tests the inverted timestamp scheme for RESOLVED files.
func TestInvertedTimestampOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// maxTimestamp is used to invert timestamps for descending sort
	const maxTS int64 = 1<<63 - 1

	timestamps := []int64{
		1704067200000000000, // 2024-01-01
		1704153600000000000, // 2024-01-02
		1704240000000000000, // 2024-01-03
	}

	var invertedNames []string
	for _, ts := range timestamps {
		inverted := maxTS - ts
		name := fmt.Sprintf("%019d-0000.resolved.pb", inverted)
		invertedNames = append(invertedNames, name)
	}

	// When sorted lexicographically, newest (largest ts) should come first
	// because it has the smallest inverted value
	require.True(t, invertedNames[2] < invertedNames[1],
		"newest should sort before older")
	require.True(t, invertedNames[1] < invertedNames[0],
		"older should sort after newer")
}

// =============================================================================
// Coordinator tests - verify coordinator logic in isolation using mocks
// =============================================================================

// TestCoordinatorHandleFlushReport tests that flush reports are handled correctly.
func TestCoordinatorHandleFlushReport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	settings := cluster.MakeTestingClusterSettings()
	flushCh := make(chan flushReport, 10)
	frontierCh := make(chan frontierReport, 10)

	coord := newCoordinator(coordinatorConfig{
		persister:        &testPersister{},
		checkpointWriter: &testCheckpointWriter{},
		stopChecker:      &testStopChecker{},
		settings:         settings,
		spanStarts: []spanWithStartTS{
			{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}},
		},
		flushReportCh:    flushCh,
		frontierReportCh: frontierCh,
	})

	// Test handling a flush report with a file.
	report := flushReport{
		shardID:    0,
		fileID:     1234567890123456,
		frontierTS: hlc.Timestamp{WallTime: 1000},
	}
	err := coord.handleFlushReport(report)
	require.NoError(t, err)
	require.Len(t, coord.pendingFileIDs, 1)
	require.Equal(t, int64(1234567890123456), coord.pendingFileIDs[0])

	// Test handling a flush report without a file (empty buffer).
	report2 := flushReport{
		shardID:    0,
		fileID:     0, // no file
		frontierTS: hlc.Timestamp{WallTime: 2000},
	}
	err = coord.handleFlushReport(report2)
	require.NoError(t, err)
	require.Len(t, coord.pendingFileIDs, 1) // still just 1 file

	// Test handling a flush report with an error.
	report3 := flushReport{
		shardID: 1,
		err:     fmt.Errorf("shard error"),
	}
	err = coord.handleFlushReport(report3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "shard 1 error")
}

// TestCoordinatorHandleFrontierReport tests that frontier reports are merged correctly.
func TestCoordinatorHandleFrontierReport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	settings := cluster.MakeTestingClusterSettings()
	flushCh := make(chan flushReport, 10)
	frontierCh := make(chan frontierReport, 10)

	testSpan := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}
	startTS := hlc.Timestamp{WallTime: 1000}

	coord := newCoordinator(coordinatorConfig{
		persister:        &testPersister{},
		checkpointWriter: &testCheckpointWriter{},
		stopChecker:      &testStopChecker{},
		settings:         settings,
		spanStarts:       []spanWithStartTS{{Span: testSpan, StartTS: startTS}},
		flushReportCh:    flushCh,
		frontierReportCh: frontierCh,
	})

	// Initialize the frontier (normally done in run()).
	var err error
	coord.frontier, err = span.MakeFrontier()
	require.NoError(t, err)
	require.NoError(t, coord.frontier.AddSpansAt(startTS, testSpan))
	coord.globalResolved = coord.frontier.Frontier()

	require.Equal(t, startTS, coord.globalResolved)

	// Handle a frontier report that advances the frontier.
	advancedTS := hlc.Timestamp{WallTime: 5000}
	report := frontierReport{
		shardID: 0,
		resolvedSpans: []jobspb.ResolvedSpan{
			{Span: testSpan, Timestamp: advancedTS},
		},
	}
	coord.handleFrontierReport(report)

	// Verify the global resolved advanced.
	require.Equal(t, advancedTS, coord.globalResolved)
}

// TestCoordinatorMaybeWriteCheckpoint tests checkpoint writing logic.
func TestCoordinatorMaybeWriteCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()

	// Set short interval for testing.
	ResolvedCheckpointInterval.Override(ctx, &settings.SV, 0)

	writer := &testCheckpointWriter{}
	coord := newCoordinator(coordinatorConfig{
		persister:        &testPersister{},
		checkpointWriter: writer,
		stopChecker:      &testStopChecker{},
		settings:         settings,
		spanStarts: []spanWithStartTS{
			{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}},
		},
		flushReportCh:    make(chan flushReport, 10),
		frontierReportCh: make(chan frontierReport, 10),
	})

	// No pending files - should not write checkpoint.
	err := coord.maybeWriteCheckpoint(ctx)
	require.NoError(t, err)
	require.Len(t, writer.checkpoints, 0)

	// Add pending file IDs.
	coord.pendingFileIDs = []int64{1234567890123456, 1234567890123457}
	coord.globalResolved = hlc.Timestamp{WallTime: 5000}

	// Now should write checkpoint.
	err = coord.maybeWriteCheckpoint(ctx)
	require.NoError(t, err)
	require.Len(t, writer.checkpoints, 1)
	require.Equal(t, hlc.Timestamp{WallTime: 5000}, writer.checkpoints[0].ResolvedTs)
	require.Equal(t, []int64{1234567890123456, 1234567890123457}, writer.checkpoints[0].FileIds)

	// Pending file IDs should be cleared.
	require.Empty(t, coord.pendingFileIDs)
}

// TestCoordinatorStopChecker tests that the stop checker is called correctly.
func TestCoordinatorStopChecker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()

	stopChecker := &testStopChecker{shouldStop: false}

	coord := newCoordinator(coordinatorConfig{
		persister:        &testPersister{},
		checkpointWriter: &testCheckpointWriter{},
		stopChecker:      stopChecker,
		settings:         settings,
		spanStarts: []spanWithStartTS{
			{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}},
		},
		flushReportCh:    make(chan flushReport, 10),
		frontierReportCh: make(chan frontierReport, 10),
	})

	// Set short intervals for testing.
	ResolvedCheckpointInterval.Override(ctx, &settings.SV, 0)

	// Initialize frontier.
	var err error
	coord.frontier, err = span.MakeFrontier()
	require.NoError(t, err)

	// onTick should check stop condition.
	err = coord.onTick(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stopChecker.checks)

	// Set stop to true.
	stopChecker.mu.Lock()
	stopChecker.shouldStop = true
	stopChecker.mu.Unlock()

	// onTick should return nil (clean stop).
	err = coord.onTick(ctx)
	require.NoError(t, err)
}

// =============================================================================
// Deduplication tests - verify replay deduplication logic
// =============================================================================

// TestDedupEntryKeepsLatest verifies that dedup map keeps the latest event per key.
func TestDedupEntryKeepsLatest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dedup := make(map[string]dedupEntry)

	// First event for key "foo" at t=1000.
	keyStr := "foo"
	dedup[keyStr] = dedupEntry{
		ts:          hlc.Timestamp{WallTime: 1000},
		value:       []byte("value1"),
		isTombstone: false,
	}

	// Later event for same key at t=2000 should replace.
	ts2 := hlc.Timestamp{WallTime: 2000}
	existing := dedup[keyStr]
	if existing.ts.Less(ts2) {
		dedup[keyStr] = dedupEntry{
			ts:          ts2,
			value:       []byte("value2"),
			isTombstone: false,
		}
	}

	require.Equal(t, ts2, dedup[keyStr].ts)
	require.Equal(t, []byte("value2"), dedup[keyStr].value)

	// Earlier event at t=500 should NOT replace.
	ts3 := hlc.Timestamp{WallTime: 500}
	existing = dedup[keyStr]
	if existing.ts.Less(ts3) {
		t.Fatal("should not update - existing is newer")
	}

	// Still has the t=2000 entry.
	require.Equal(t, ts2, dedup[keyStr].ts)
}

// TestDedupHandlesTombstones verifies that tombstones are tracked correctly.
func TestDedupHandlesTombstones(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dedup := make(map[string]dedupEntry)

	// First: put event.
	dedup["key1"] = dedupEntry{
		ts:          hlc.Timestamp{WallTime: 1000},
		value:       []byte("value"),
		isTombstone: false,
	}

	// Later: delete event (tombstone).
	dedup["key1"] = dedupEntry{
		ts:          hlc.Timestamp{WallTime: 2000},
		value:       nil,
		isTombstone: true,
	}

	require.True(t, dedup["key1"].isTombstone)
	require.Nil(t, dedup["key1"].value)

	// Another key: only tombstone (never had a value in log range).
	dedup["key2"] = dedupEntry{
		ts:          hlc.Timestamp{WallTime: 1500},
		value:       nil,
		isTombstone: true,
	}

	require.True(t, dedup["key2"].isTombstone)
}

// TestDedupMultipleKeys verifies dedup works across multiple keys.
func TestDedupMultipleKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dedup := make(map[string]dedupEntry)

	// Multiple keys with various events.
	events := []struct {
		key   string
		ts    int64
		value string
		del   bool
	}{
		{"a", 1000, "a1", false},
		{"b", 1000, "b1", false},
		{"a", 2000, "a2", false}, // supersedes a@1000
		{"c", 1500, "c1", false},
		{"b", 3000, "", true},    // delete supersedes b@1000
		{"a", 1500, "a1.5", false}, // does NOT supersede a@2000
	}

	for _, e := range events {
		ts := hlc.Timestamp{WallTime: e.ts}
		existing, exists := dedup[e.key]
		if !exists || existing.ts.Less(ts) {
			var value []byte
			if !e.del && e.value != "" {
				value = []byte(e.value)
			}
			dedup[e.key] = dedupEntry{
				ts:          ts,
				value:       value,
				isTombstone: e.del,
			}
		}
	}

	// Check results.
	require.Len(t, dedup, 3)

	// "a" should have value "a2" at t=2000.
	require.Equal(t, hlc.Timestamp{WallTime: 2000}, dedup["a"].ts)
	require.Equal(t, []byte("a2"), dedup["a"].value)
	require.False(t, dedup["a"].isTombstone)

	// "b" should be a tombstone at t=3000.
	require.Equal(t, hlc.Timestamp{WallTime: 3000}, dedup["b"].ts)
	require.True(t, dedup["b"].isTombstone)

	// "c" should have value "c1" at t=1500.
	require.Equal(t, hlc.Timestamp{WallTime: 1500}, dedup["c"].ts)
	require.Equal(t, []byte("c1"), dedup["c"].value)
}
