// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// sqlInstancesAddDrainingMigration adds a new column `is_draining` to the
// system.sql_instances table.
func addJobsTables(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		if err := createSystemTable(
			ctx, d.DB, d.Settings, d.Codec,
			systemschema.SystemJobProgressTable,
			tree.LocalityLevelTable,
		); err != nil {
			return err
		}

		if err := createSystemTable(
			ctx, d.DB, d.Settings, d.Codec,
			systemschema.SystemJobStatusTable,
			tree.LocalityLevelTable,
		); err != nil {
			return err
		}

		if err := migrateTable(
			ctx, cs, d,
			operation{
				name:       "add-status-columns",
				schemaList: []string{"description", "paused", "finished", "error_msg", "pause_reason"},
				query: `ALTER TABLE system.jobs 
				ADD COLUMN IF NOT EXISTS description STRING FAMILY "fam_0_id_status_created_payload",
				ADD COLUMN IF NOT EXISTS owner STRING FAMILY "fam_0_id_status_created_payload",
				ADD COLUMN IF NOT EXISTS paused TIMESTAMPTZ FAMILY "fam_0_id_status_created_payload",
				ADD COLUMN IF NOT EXISTS finished TIMESTAMPTZ FAMILY "fam_0_id_status_created_payload",
				ADD COLUMN IF NOT EXISTS error_msg STRING FAMILY "fam_0_id_status_created_payload",
				ADD COLUMN IF NOT EXISTS pause_reason STRING FAMILY "fam_0_id_status_created_payload"
			`,
				schemaExistsFn: hasColumn,
			},
			keys.JobsTableID,
			systemschema.JobsTable,
		); err != nil {
			return err
		}

		return nil
	})
}
