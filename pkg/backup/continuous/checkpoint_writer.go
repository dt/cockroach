// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package continuous

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/backup/continuous/continuouspb"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const (
	// maxTimestamp is used to invert timestamps for descending sort order.
	// Using max int64 value (about 9.2 quintillion nanoseconds, ~290 years from epoch).
	maxTimestamp int64 = 1<<63 - 1

	// invertedTimestampWidth is 19 digits to accommodate max int64 (2^63-1 = 9,223,372,036,854,775,807).
	invertedTimestampWidth = 19

	// logicalClockWidth is 4 digits to accommodate logical clock values (typically small).
	logicalClockWidth = 4
)

// cloudCheckpointWriter implements checkpointWriter using cloud storage.
type cloudCheckpointWriter struct {
	logFolder string
	execCtx   sql.JobExecContext
}

var _ checkpointWriter = (*cloudCheckpointWriter)(nil)

// newCloudCheckpointWriter creates a new checkpoint writer that writes
// RESOLVED checkpoint files to cloud storage.
func newCloudCheckpointWriter(logFolder string, execCtx sql.JobExecContext) *cloudCheckpointWriter {
	return &cloudCheckpointWriter{
		logFolder: logFolder,
		execCtx:   execCtx,
	}
}

// WriteCheckpoint implements checkpointWriter.
func (w *cloudCheckpointWriter) WriteCheckpoint(
	ctx context.Context, checkpoint *continuouspb.ResolvedCheckpoint,
) error {
	// Create storage for the log folder.
	store, err := w.execCtx.ExecCfg().DistSQLSrv.ExternalStorageFromURI(
		ctx, w.logFolder, w.execCtx.User(),
	)
	if err != nil {
		return errors.Wrap(err, "creating storage")
	}
	defer store.Close()

	// Filename format: <inverted_resolved_ts>-<logical>.resolved.pb
	// Inverted wall time so that listing returns newest first (descending order).
	// Include logical clock to handle multiple checkpoints at same wall time.
	invertedTS := maxTimestamp - checkpoint.ResolvedTs.WallTime
	filename := fmt.Sprintf("resolved/%0*d-%0*d.resolved.pb",
		invertedTimestampWidth, invertedTS,
		logicalClockWidth, checkpoint.ResolvedTs.Logical)

	data, err := protoutil.Marshal(checkpoint)
	if err != nil {
		return errors.Wrap(err, "marshaling checkpoint")
	}

	if err := cloud.WriteFile(ctx, store, filename, bytes.NewReader(data)); err != nil {
		return errors.Wrap(err, "writing checkpoint file")
	}

	return nil
}
