// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package continuous

import (
	"context"
	"path"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// backupIndexStopChecker implements stopChecker by checking the backup index
// for newer backups and enforcing max duration.
type backupIndexStopChecker struct {
	collectionURI string
	startTime     hlc.Timestamp
	jobStartTime  time.Time
	execCtx       sql.JobExecContext
	settings      *cluster.Settings

	// Rate limiting for stop checks.
	lastCheck time.Time
}

var _ stopChecker = (*backupIndexStopChecker)(nil)

// newBackupIndexStopChecker creates a new stop checker.
func newBackupIndexStopChecker(
	collectionURI string,
	startTime hlc.Timestamp,
	execCtx sql.JobExecContext,
) *backupIndexStopChecker {
	return &backupIndexStopChecker{
		collectionURI: collectionURI,
		startTime:     startTime,
		jobStartTime:  timeutil.Now(),
		execCtx:       execCtx,
		settings:      execCtx.ExecCfg().Settings,
	}
}

// ShouldStop implements stopChecker.
func (c *backupIndexStopChecker) ShouldStop(ctx context.Context) (bool, error) {
	// Rate limit stop checks.
	if timeutil.Since(c.lastCheck) < StopCheckInterval.Get(&c.settings.SV) {
		return false, nil
	}
	c.lastCheck = timeutil.Now()

	// Check max duration guardrail.
	if timeutil.Since(c.jobStartTime) > MaxDuration.Get(&c.settings.SV) {
		return false, errors.Errorf(
			"continuous backup exceeded max duration of %s without detecting a newer backup",
			MaxDuration.Get(&c.settings.SV),
		)
	}

	// Check backup index for newer backup.
	hasNewer, err := c.hasNewerBackupInIndex(ctx)
	if err != nil {
		// Log but don't fail - we'll check again next time.
		log.Dev.Warningf(ctx, "error checking backup index: %v", err)
		return false, nil
	}

	return hasNewer, nil
}

// hasNewerBackupInIndex checks if a backup with end time > our start time exists.
func (c *backupIndexStopChecker) hasNewerBackupInIndex(ctx context.Context) (bool, error) {
	if c.collectionURI == "" {
		return false, nil
	}

	store, err := c.execCtx.ExecCfg().DistSQLSrv.ExternalStorageFromURI(
		ctx, c.collectionURI, c.execCtx.User(),
	)
	if err != nil {
		return false, errors.Wrap(err, "creating collection storage")
	}
	defer store.Close()

	// Get backup index metadata - returns in ascending end time order.
	indexes, err := backupinfo.GetBackupTreeIndexMetadata(ctx, store, path.Base(c.collectionURI))
	if err != nil {
		return false, errors.Wrap(err, "reading backup index")
	}

	if len(indexes) == 0 {
		return false, nil
	}

	// Check if the latest backup's end time is after our start time.
	latestEndTime := indexes[len(indexes)-1].EndTime
	return c.startTime.Less(latestEndTime), nil
}
