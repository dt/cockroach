// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package continuous

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

// frontierName is the key used to persist the span frontier in job info storage.
const frontierName = "continuous-backup"

// jobInfoPersister implements frontierPersister using job info storage.
type jobInfoPersister struct {
	job *jobs.Job
}

var _ frontierPersister = (*jobInfoPersister)(nil)

// newJobInfoPersister creates a new persister that stores frontier state
// in the job's info storage.
func newJobInfoPersister(job *jobs.Job) *jobInfoPersister {
	return &jobInfoPersister{job: job}
}

// Load implements frontierPersister.
func (p *jobInfoPersister) Load(ctx context.Context) (span.Frontier, bool, error) {
	var frontier span.Frontier
	var found bool

	if err := p.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		var err error
		frontier, found, err = jobfrontier.Get(ctx, txn, p.job.ID(), frontierName)
		return err
	}); err != nil {
		return nil, false, errors.Wrap(err, "loading frontier from job storage")
	}

	return frontier, found, nil
}

// Store implements frontierPersister.
func (p *jobInfoPersister) Store(ctx context.Context, frontier span.Frontier) error {
	return p.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		return jobfrontier.Store(ctx, txn, p.job.ID(), frontierName, frontier)
	})
}

// deleteFrontier removes the persisted frontier from job storage.
// This is called when the job completes successfully.
func (p *jobInfoPersister) deleteFrontier(ctx context.Context) error {
	return p.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		return jobfrontier.Delete(ctx, txn, p.job.ID(), frontierName)
	})
}

// updateJobProgress updates the job's progress fraction.
func updateJobProgress(ctx context.Context, job *jobs.Job) error {
	return job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		// For continuous backup, we don't have a meaningful progress fraction.
		// Just touching the job ensures the registry knows it's making progress.
		_ = md.Progress.GetBackup()
		return nil
	})
}

// loadResolvedSpans loads the persisted frontier as resolved spans.
// This is useful for inspecting the frontier without reconstructing it.
func loadResolvedSpans(
	ctx context.Context, job *jobs.Job,
) ([]jobspb.ResolvedSpan, bool, error) {
	var spans []jobspb.ResolvedSpan
	var found bool

	if err := job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		var err error
		spans, found, err = jobfrontier.GetResolvedSpans(ctx, txn, job.ID(), frontierName)
		return err
	}); err != nil {
		return nil, false, errors.Wrap(err, "loading resolved spans from job storage")
	}

	return spans, found, nil
}
