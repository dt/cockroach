// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package continuous

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// FlushInterval is the target interval between shard flushes. This determines
// the RPO (Recovery Point Objective) for continuous backup.
var FlushInterval = settings.RegisterDurationSettingWithExplicitUnit(
	settings.ApplicationLevel,
	"backup.continuous.flush_interval",
	"the target interval between shard flushes for continuous backup",
	10*time.Second,
	settings.WithPublic,
)

// MaxBufferSize is the maximum buffer size per shard before a flush is triggered.
var MaxBufferSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"backup.continuous.max_buffer_size",
	"the maximum buffer size per shard before a flush is triggered",
	128<<20, // 128 MiB
	settings.WithPublic,
)

// ResolvedCheckpointInterval is the interval between writing RESOLVED checkpoint files.
var ResolvedCheckpointInterval = settings.RegisterDurationSettingWithExplicitUnit(
	settings.ApplicationLevel,
	"backup.continuous.resolved_checkpoint_interval",
	"the interval between writing RESOLVED checkpoint files",
	15*time.Second,
	settings.WithPublic,
)

// MaxDuration is the maximum time a continuous backup job can run without
// seeing a newer backup in the index. This is a guardrail to prevent jobs
// from running indefinitely if backups are failing.
var MaxDuration = settings.RegisterDurationSettingWithExplicitUnit(
	settings.ApplicationLevel,
	"backup.continuous.max_duration",
	"the maximum time a continuous backup job can run without seeing a newer backup",
	72*time.Hour,
	settings.WithPublic,
)

// StopCheckInterval is the interval between checking if the job should stop
// (i.e., if a newer backup has appeared in the index).
var StopCheckInterval = settings.RegisterDurationSettingWithExplicitUnit(
	settings.ApplicationLevel,
	"backup.continuous.stop_check_interval",
	"the interval between checking if a newer backup exists in the index",
	1*time.Minute,
	settings.WithPublic,
)

// FrontierSyncInterval is the interval at which shards send their full span
// frontier to the coordinator. This is separate from flush reports which only
// send the min timestamp for fast-path RESOLVED file updates.
var FrontierSyncInterval = settings.RegisterDurationSettingWithExplicitUnit(
	settings.ApplicationLevel,
	"backup.continuous.frontier_sync_interval",
	"the interval at which shards send their full span frontier to the coordinator",
	30*time.Second,
	settings.WithPublic,
)

// FrontierPersistLag is the lag threshold at which the coordinator persists
// the frontier to job storage. If the resolved timestamp is more than this
// far behind wall clock time, the frontier is persisted for resumability.
// Otherwise, persisting is skipped since resuming from the last RESOLVED
// checkpoint is efficient enough.
var FrontierPersistLag = settings.RegisterDurationSettingWithExplicitUnit(
	settings.ApplicationLevel,
	"backup.continuous.frontier_persist_lag",
	"persist frontier to job storage when resolved timestamp lags wall clock by this amount",
	5*time.Minute,
	settings.WithPublic,
)
