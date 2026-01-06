// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package continuous

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/continuous/continuouspb"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// ReplayStats tracks statistics from log replay.
type ReplayStats struct {
	FilesRead     int64
	EventsRead    int64
	EventsApplied int64
	EventsSkipped int64 // Events outside time range or superseded by later event
}

// ReplayLog replays continuous backup log events from the log folder.
// It reads all events with timestamps in (startTS, endTS] and applies them
// using in-memory deduplication - for each key, only the latest event is kept.
//
// Parameters:
//   - logFolder: the path to the continuous log folder
//   - startTS: the backup's end time (exclusive lower bound for replay)
//   - endTS: the target restore time (inclusive upper bound for replay)
func ReplayLog(
	ctx context.Context,
	execCtx sql.JobExecContext,
	logFolder string,
	startTS, endTS hlc.Timestamp,
) (*ReplayStats, error) {
	// Validate input parameters.
	if endTS.LessEq(startTS) {
		return nil, errors.Errorf("invalid time range for replay: endTS (%s) must be after startTS (%s)", endTS, startTS)
	}

	log.Dev.Infof(ctx, "starting log replay from %s: (%s, %s]", logFolder, startTS, endTS)

	// Create storage for the log folder.
	store, err := execCtx.ExecCfg().DistSQLSrv.ExternalStorageFromURI(
		ctx, logFolder, execCtx.User(),
	)
	if err != nil {
		return nil, errors.Wrap(err, "creating storage for log folder")
	}
	defer store.Close()

	// Find all RESOLVED checkpoint files.
	checkpoints, err := findResolvedCheckpoints(ctx, store)
	if err != nil {
		return nil, errors.Wrap(err, "finding resolved checkpoints")
	}

	if len(checkpoints) == 0 {
		log.Dev.Infof(ctx, "no resolved checkpoints found in %s", logFolder)
		return &ReplayStats{}, nil
	}

	// Collect all data file IDs from checkpoints that cover our time range.
	// A checkpoint is relevant if its resolved_ts > startTS (it contains
	// events after our backup's end time).
	var fileIDsToReplay []int64
	for _, cp := range checkpoints {
		if startTS.Less(cp.ResolvedTs) {
			fileIDsToReplay = append(fileIDsToReplay, cp.FileIDs...)
		}
		// Stop once we've collected files up to our target time.
		if endTS.LessEq(cp.ResolvedTs) {
			break
		}
	}

	log.Dev.Infof(ctx, "found %d files to replay from %d checkpoints", len(fileIDsToReplay), len(checkpoints))

	// Create replayer and execute replay.
	replayer := newSimpleReplayer(store, execCtx.ExecCfg().DB)
	return replayer.Replay(ctx, fileIDsToReplay, startTS, endTS)
}

// resolvedCheckpoint is a parsed RESOLVED checkpoint file.
type resolvedCheckpoint struct {
	ResolvedTs hlc.Timestamp
	FileIDs    []int64
}

// findResolvedCheckpoints finds and parses all RESOLVED checkpoint files.
// Returns checkpoints sorted by resolved timestamp (ascending).
func findResolvedCheckpoints(
	ctx context.Context, store cloud.ExternalStorage,
) ([]resolvedCheckpoint, error) {
	var checkpoints []resolvedCheckpoint

	// List the resolved/ directory.
	err := store.List(ctx, "resolved/", cloud.ListOptions{}, func(path string) error {
		if !strings.HasSuffix(path, ".resolved.pb") {
			return nil
		}

		// Read the checkpoint file.
		reader, _, err := store.ReadFile(ctx, path, cloud.ReadOptions{})
		if err != nil {
			return errors.Wrapf(err, "reading checkpoint file %s", path)
		}
		defer reader.Close(ctx)

		data, err := ioctx.ReadAll(ctx, reader)
		if err != nil {
			return errors.Wrapf(err, "reading checkpoint data %s", path)
		}

		var cp continuouspb.ResolvedCheckpoint
		if err := protoutil.Unmarshal(data, &cp); err != nil {
			return errors.Wrapf(err, "unmarshaling checkpoint %s", path)
		}

		checkpoints = append(checkpoints, resolvedCheckpoint{
			ResolvedTs: cp.ResolvedTs,
			FileIDs:    cp.FileIds,
		})

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Sort by resolved timestamp ascending.
	sort.Slice(checkpoints, func(i, j int) bool {
		return checkpoints[i].ResolvedTs.Less(checkpoints[j].ResolvedTs)
	})

	return checkpoints, nil
}

// =============================================================================
// Simple in-memory replayer implementation
// =============================================================================

// dedupEntry represents a deduplicated event for a single key.
// We keep the latest event (by timestamp) for each key.
type dedupEntry struct {
	ts        hlc.Timestamp
	value     []byte // nil for tombstone
	isTombstone bool
}

// simpleReplayer is an in-memory implementation of logReplayer.
// It builds a map of key -> latest event, then applies all entries.
// This is correct but doesn't scale to very large logs.
type simpleReplayer struct {
	store cloud.ExternalStorage
	db    *kv.DB
}

func newSimpleReplayer(store cloud.ExternalStorage, db *kv.DB) *simpleReplayer {
	return &simpleReplayer{
		store: store,
		db:    db,
	}
}

var _ logReplayer = (*simpleReplayer)(nil)

// Replay implements logReplayer.
func (r *simpleReplayer) Replay(
	ctx context.Context, fileIDs []int64, startTS, endTS hlc.Timestamp,
) (*ReplayStats, error) {
	stats := &ReplayStats{}

	// Phase 1: Build deduplicated map of key -> latest event
	dedup := make(map[string]dedupEntry)

	for _, fileID := range fileIDs {
		if err := ctx.Err(); err != nil {
			return stats, errors.Wrap(err, "replay cancelled")
		}

		filePath := fmt.Sprintf("data/%d.sst", fileID)
		eventsRead, eventsKept, err := r.readFileIntoMap(ctx, filePath, startTS, endTS, dedup)
		if err != nil {
			return stats, errors.Wrapf(err, "reading file %s", filePath)
		}
		stats.FilesRead++
		stats.EventsRead += eventsRead
		stats.EventsSkipped += eventsRead - eventsKept
	}

	log.Dev.Infof(ctx, "deduplication complete: %d files, %d events read, %d unique keys",
		stats.FilesRead, stats.EventsRead, len(dedup))

	// Phase 2: Apply all deduplicated entries
	applied, err := r.applyDedup(ctx, dedup)
	if err != nil {
		return stats, errors.Wrap(err, "applying deduplicated events")
	}
	stats.EventsApplied = applied

	log.Dev.Infof(ctx, "log replay complete: %d events applied", applied)

	return stats, nil
}

// readFileIntoMap reads events from an SST file and updates the dedup map.
// Returns (events read, events kept in map).
func (r *simpleReplayer) readFileIntoMap(
	ctx context.Context,
	filePath string,
	startTS, endTS hlc.Timestamp,
	dedup map[string]dedupEntry,
) (int64, int64, error) {
	// Read the SST file.
	reader, _, err := r.store.ReadFile(ctx, filePath, cloud.ReadOptions{})
	if err != nil {
		return 0, 0, errors.Wrap(err, "opening SST file")
	}
	defer reader.Close(ctx)

	data, err := ioctx.ReadAll(ctx, reader)
	if err != nil {
		return 0, 0, errors.Wrap(err, "reading SST file")
	}

	// Create SST iterator.
	iter, err := storage.NewMemSSTIterator(data, false /* verify */, storage.IterOptions{})
	if err != nil {
		return 0, 0, errors.Wrap(err, "creating SST iterator")
	}
	defer iter.Close()

	var eventsRead, eventsKept int64

	// Iterate through all events in the SST.
	iter.SeekGE(storage.MVCCKey{Key: roachpb.KeyMin})
	for {
		ok, err := iter.Valid()
		if err != nil {
			return eventsRead, eventsKept, errors.Wrap(err, "iterating SST")
		}
		if !ok {
			break
		}

		eventsRead++

		// Decode the event key to get the timestamp.
		sstKey := iter.UnsafeKey()
		eventTS := decodeEventTimestamp(sstKey.Key)

		// Skip events outside our time range.
		if eventTS.LessEq(startTS) || endTS.Less(eventTS) {
			iter.Next()
			continue
		}

		// Decode the event value.
		sstValue, err := iter.UnsafeValue()
		if err != nil {
			return eventsRead, eventsKept, errors.Wrap(err, "reading SST value")
		}
		event, err := decodeEvent(sstValue)
		if err != nil {
			return eventsRead, eventsKept, errors.Wrap(err, "decoding event")
		}

		// Check if we should update the map (this event is newer).
		keyStr := string(event.key)
		existing, exists := dedup[keyStr]
		if !exists || existing.ts.Less(eventTS) {
			// This event is newer (or first for this key).
			var valueCopy []byte
			if event.eventType == 0 && len(event.value) > 0 {
				valueCopy = make([]byte, len(event.value))
				copy(valueCopy, event.value)
			}
			dedup[keyStr] = dedupEntry{
				ts:          eventTS,
				value:       valueCopy,
				isTombstone: event.eventType == 1, // delete
			}
			eventsKept++
		}

		iter.Next()
	}

	return eventsRead, eventsKept, nil
}

// applyDedup applies all deduplicated entries to KV.
// Returns the number of entries applied.
func (r *simpleReplayer) applyDedup(ctx context.Context, dedup map[string]dedupEntry) (int64, error) {
	var applied int64

	// Sort keys for deterministic ordering (helps with testing and debugging).
	keys := make([]string, 0, len(dedup))
	for k := range dedup {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Apply each entry.
	for _, keyStr := range keys {
		if err := ctx.Err(); err != nil {
			return applied, errors.Wrap(err, "apply cancelled")
		}

		entry := dedup[keyStr]
		key := roachpb.Key(keyStr)

		if entry.isTombstone {
			// Delete the key.
			if _, err := r.db.Del(ctx, key); err != nil {
				return applied, errors.Wrapf(err, "deleting key %s", key)
			}
		} else {
			// Put the value.
			if err := r.db.Put(ctx, key, entry.value); err != nil {
				return applied, errors.Wrapf(err, "putting key %s", key)
			}
		}
		applied++
	}

	return applied, nil
}

// =============================================================================
// Utility functions for finding and querying continuous logs
// =============================================================================

// FindContinuousLogFolder attempts to find a continuous log folder for PITR.
// Given a collection URI and backup end time, it checks if a continuous log
// exists that can cover the time range (backupEndTime, targetTime].
// Returns the log folder path and max resolved timestamp, or empty values if not found.
//
// The log folder is stored as a subfolder of the backup at <collection>/<backup_subdir>/log/,
// where backup_subdir is the date-based folder name for the backup (e.g., /2024/01/15-140000.00).
//
// mkStore is a function that creates external storage from a URI.
func FindContinuousLogFolder(
	ctx context.Context,
	mkStore cloud.ExternalStorageFromURIFactory,
	user username.SQLUsername,
	collectionURI string,
	backupEndTime hlc.Timestamp,
	targetTime hlc.Timestamp,
) (logFolder string, maxResolved hlc.Timestamp, err error) {
	// Compute the expected log folder path: <collection>/<backup_subdir>/log/
	// The backup subdir is date-based, matching how backups are organized.
	backupSubdir := backupEndTime.GoTime().Format(backupbase.DateBasedIntoFolderName)
	logFolder = fmt.Sprintf("%s%s/log", collectionURI, backupSubdir)

	// Try to open the storage to check if it exists.
	store, err := mkStore(ctx, logFolder, user)
	if err != nil {
		// Check if this is a "not found" error. If so, return empty (no log exists).
		// Otherwise, propagate the error as it indicates a real problem.
		// For now, we treat all errors as "not found" since we can't easily distinguish.
		// TODO(pitr): Distinguish between "not found" and other storage errors.
		log.Dev.Infof(ctx, "continuous log folder %s not accessible: %v", logFolder, err)
		return "", hlc.Timestamp{}, nil
	}
	defer store.Close()

	// Check for RESOLVED checkpoints.
	checkpoints, err := findResolvedCheckpoints(ctx, store)
	if err != nil {
		// Error reading checkpoints is a real error - propagate it.
		return "", hlc.Timestamp{}, errors.Wrap(err, "reading resolved checkpoints")
	}

	if len(checkpoints) == 0 {
		// No checkpoints found.
		return "", hlc.Timestamp{}, nil
	}

	// Get the max resolved timestamp.
	maxResolved = checkpoints[len(checkpoints)-1].ResolvedTs

	// Check if the log covers enough of the time range.
	// The log should have resolved at least up to the target time.
	if maxResolved.Less(targetTime) {
		// Log doesn't cover the full range, but we can still use what we have.
		// The restore will fail if we can't replay to the target time.
		log.Dev.Warningf(ctx, "continuous log only covers up to %s, target is %s",
			maxResolved, targetTime)
	}

	return logFolder, maxResolved, nil
}

// GetMaxResolvedTimestamp returns the maximum resolved timestamp from the log.
// This is used by SHOW BACKUPS to display the PITR_UP_TO time.
func GetMaxResolvedTimestamp(
	ctx context.Context, execCtx sql.JobExecContext, logFolder string,
) (hlc.Timestamp, error) {
	store, err := execCtx.ExecCfg().DistSQLSrv.ExternalStorageFromURI(
		ctx, logFolder, execCtx.User(),
	)
	if err != nil {
		return hlc.Timestamp{}, errors.Wrap(err, "creating storage")
	}
	defer store.Close()

	checkpoints, err := findResolvedCheckpoints(ctx, store)
	if err != nil {
		return hlc.Timestamp{}, err
	}

	if len(checkpoints) == 0 {
		return hlc.Timestamp{}, nil
	}

	// Return the highest resolved timestamp.
	return checkpoints[len(checkpoints)-1].ResolvedTs, nil
}
