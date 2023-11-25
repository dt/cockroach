// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/repstream"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionprotectedts"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

type streamIngestManagerImpl struct {
	evalCtx     *eval.Context
	jobRegistry *jobs.Registry
	txn         isql.Txn
	sessionID   clusterunique.ID
}

// GetReplicationStatsAndStatus implements streaming.StreamIngestManager interface.
func (r *streamIngestManagerImpl) GetReplicationStatsAndStatus(
	ctx context.Context, ingestionJobID jobspb.JobID,
) (*streampb.StreamIngestionStats, string, error) {
	return getReplicationStatsAndStatus(ctx, r.jobRegistry, r.txn, ingestionJobID)
}

// RevertTenantToTimestamp  implements streaming.StreamIngestManager interface.
func (r *streamIngestManagerImpl) RevertTenantToTimestamp(
	ctx context.Context, tenantName roachpb.TenantName, revertTo hlc.Timestamp,
) error {
	execCfg := r.evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)

	// These vars are set in Txn below. This transaction checks
	// the service state of the tenant record, moves the tenant's
	// data state to ADD, and installs a PTS for the revert
	// timestamp.
	//
	// NB: We do this using a different txn since we want to be
	// able to commit the state change during the
	// non-transactional RevertSpans below.
	var (
		originalDataState mtinfopb.TenantDataState
		tenantID          roachpb.TenantID
		ptsCleanup        func()
	)
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		tenantRecord, err := sql.GetTenantRecordByName(ctx, r.evalCtx.Settings, txn, tenantName)
		if err != nil {
			return err
		}
		tenantID, err = roachpb.MakeTenantID(tenantRecord.ID)
		if err != nil {
			return err
		}

		if tenantID.Equal(roachpb.SystemTenantID) {
			return errors.New("cannot revert the system tenant")
		}

		if tenantRecord.ServiceMode != mtinfopb.ServiceModeNone {
			return errors.Newf("cannot revert tenant %q (%d) in service mode %s; service mode must be %s",
				tenantRecord.Name,
				tenantRecord.ID,
				tenantRecord.ServiceMode,
				mtinfopb.ServiceModeNone,
			)
		}

		originalDataState = tenantRecord.DataState

		ptsCleanup, err = protectTenantSpanWithSession(ctx, r.evalCtx, txn, execCfg, tenantID, r.sessionID, revertTo)
		if err != nil {
			return errors.Wrap(err, "protecting revert timestamp")
		}

		// Set the data state to Add during the destructive operation.
		tenantRecord.LastRevertTenantTimestamp = revertTo
		tenantRecord.DataState = mtinfopb.DataStateAdd
		return sql.UpdateTenantRecord(ctx, r.evalCtx.Settings, txn, tenantRecord)
	}); err != nil {
		return err
	}
	defer ptsCleanup()

	spanToRevert := keys.MakeTenantSpan(tenantID)
	if err := sql.RevertSpansFanout(ctx, r.evalCtx.Txn.DB(), r.evalCtx.JobExecContext.(sql.RevertSpansContext),
		[]roachpb.Span{spanToRevert},
		revertTo,
		false, /* ignoreGCThreshold */
		int64(sql.RevertTableDefaultBatchSize),
		nil /* onCompletedCallback */); err != nil {
		return err
	}

	return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		tenantRecord, err := sql.GetTenantRecordByName(ctx, r.evalCtx.Settings, txn, tenantName)
		if err != nil {
			return err
		}
		tenantRecord.DataState = originalDataState
		return sql.UpdateTenantRecord(ctx, r.evalCtx.Settings, txn, tenantRecord)
	})
}

func protectTenantSpanWithSession(
	ctx context.Context,
	evalCtx *eval.Context,
	txn isql.Txn,
	execCfg *sql.ExecutorConfig,
	tenantID roachpb.TenantID,
	sessionID clusterunique.ID,
	timestamp hlc.Timestamp,
) (func(), error) {
	ptsRecordID := uuid.MakeV4()
	ptsRecord := sessionprotectedts.MakeRecord(
		ptsRecordID,
		[]byte(sessionID.String()),
		timestamp,
		ptpb.MakeTenantsTarget([]roachpb.TenantID{tenantID}),
	)
	log.Infof(ctx, "protecting timestamp: %#+v", ptsRecord)
	pts := execCfg.ProtectedTimestampProvider.WithTxn(txn)
	if err := pts.Protect(ctx, ptsRecord); err != nil {
		return nil, err
	}
	releasePTS := func() {
		if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			pts := execCfg.ProtectedTimestampProvider.WithTxn(txn)
			return pts.Release(ctx, ptsRecordID)
		}); err != nil {
			log.Warningf(ctx, "failed to release protected timestamp %s: %v", ptsRecordID, err)
		}
	}
	return releasePTS, nil
}

func newStreamIngestManagerWithPrivilegesCheck(
	ctx context.Context, evalCtx *eval.Context, txn isql.Txn, sessionID clusterunique.ID,
) (eval.StreamIngestManager, error) {
	execCfg := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	enterpriseCheckErr := utilccl.CheckEnterpriseEnabled(
		execCfg.Settings, execCfg.NodeInfo.LogicalClusterID(), "REPLICATION")
	if enterpriseCheckErr != nil {
		return nil, pgerror.Wrap(enterpriseCheckErr,
			pgcode.CCLValidLicenseRequired, "physical replication requires an enterprise license on the secondary (and primary) cluster")
	}

	isAdmin, err := evalCtx.SessionAccessor.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		if err := evalCtx.SessionAccessor.CheckPrivilege(ctx,
			syntheticprivilege.GlobalPrivilegeObject,
			privilege.MANAGEVIRTUALCLUSTER); err != nil {
			return nil, err
		}
	}

	return &streamIngestManagerImpl{
		evalCtx:     evalCtx,
		txn:         txn,
		jobRegistry: execCfg.JobRegistry,
		sessionID:   sessionID,
	}, nil
}

func getReplicationStatsAndStatus(
	ctx context.Context, jobRegistry *jobs.Registry, txn isql.Txn, ingestionJobID jobspb.JobID,
) (*streampb.StreamIngestionStats, string, error) {
	job, err := jobRegistry.LoadJobWithTxn(ctx, ingestionJobID, txn)
	if err != nil {
		return nil, jobspb.ReplicationError.String(), err
	}
	details, ok := job.Details().(jobspb.StreamIngestionDetails)
	if !ok {
		return nil, jobspb.ReplicationError.String(),
			errors.Newf("job with id %d is not a stream ingestion job", job.ID())
	}

	details.StreamAddress, err = redactSourceURI(details.StreamAddress)
	if err != nil {
		return nil, jobspb.ReplicationError.String(), err
	}

	stats, err := replicationutils.GetStreamIngestionStats(ctx, details, job.Progress())
	if err != nil {
		return nil, jobspb.ReplicationError.String(), err
	}
	if job.Status() == jobs.StatusPaused {
		return stats, jobspb.ReplicationPaused.String(), nil
	}
	return stats, stats.IngestionProgress.ReplicationStatus.String(), nil
}

// RevertTenantToTimestamp  implements streaming.StreamIngestManager interface.
func (r *streamIngestManagerImpl) SetupReaderCatalog(
	ctx context.Context, from, to roachpb.TenantName, asOf hlc.Timestamp,
) error {
	execCfg := r.evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	var fromID, toID roachpb.TenantID
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		fromTenant, err := sql.GetTenantRecordByName(ctx, r.evalCtx.Settings, txn, from)
		if err != nil {
			return err
		}
		fromID, err = roachpb.MakeTenantID(fromTenant.ID)
		if err != nil {
			return err
		}
		toTenant, err := sql.GetTenantRecordByName(ctx, r.evalCtx.Settings, txn, to)
		if err != nil {
			return err
		}
		toID, err = roachpb.MakeTenantID(toTenant.ID)
		if err != nil {
			return err
		}
		if fromTenant.DataState != mtinfopb.DataStateReady {
			if fromTenant.PhysicalReplicationConsumerJobID == 0 {
				return errors.Newf("cannot copy catalog from tenant %s in state %s", from, fromTenant.DataState)
			}
			job, err := r.jobRegistry.LoadJobWithTxn(ctx, fromTenant.PhysicalReplicationConsumerJobID, txn)
			if err != nil {
				return errors.Wrap(err, "loading tenant replication job")
			}
			progress := job.Progress()
			replicatedTime := replicationutils.ReplicatedTimeFromProgress(&progress)
			if asOf.IsEmpty() {
				asOf = replicatedTime
			} else if replicatedTime.Less(asOf) {
				return errors.Newf("timestamp is not replicated yet")
			}
		} else if asOf.IsEmpty() {
			asOf = execCfg.Clock.Now()
		}
		if toTenant.ServiceMode != mtinfopb.ServiceModeNone && false {
			return errors.Newf("tenant %s must have service stopped to enable reader mode")
		}

		return nil
	}); err != nil {
		return err
	}

	if fromID.Equal(roachpb.SystemTenantID) || toID.Equal(roachpb.SystemTenantID) {
		return errors.New("cannot revert the system tenant")
	}

	extracted, err := getCatalogForTenantAsOf(ctx, execCfg, fromID, asOf)
	if err != nil {
		return err
	}

	m := mon.NewUnlimitedMonitor(ctx, "tenant_reader", mon.MemoryResource, nil, nil, 0, execCfg.Settings)
	buf := descs.NewBareBonesCollectionFactory(execCfg.Settings, keys.MakeSQLCodec(toID)).
		NewCollection(ctx, descs.WithMonitor(m))
	buf.SkipValidationOnWrite()

	return execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		if err := extracted.ForEachDescriptor(func(desc catalog.Descriptor) error {
			if desc.GetID() <= 103 { // TODO(dt): Ignore parentID == 1 and handle defaultdb.
				return nil
			}
			var mut catalog.MutableDescriptor
			switch t := desc.DescriptorProto().GetUnion().(type) {
			case *descpb.Descriptor_Table:
				if t.Table.ViewQuery == "" {
					t.Table.IsMaterializedView = true
					t.Table.External = &descpb.ExternalRowData{TenantID: fromID.ToUint64(), TableID: desc.GetID(), AsOf: asOf}
				}
				t.Table.Version = 1
				mut = tabledesc.NewBuilder(t.Table).BuildCreatedMutable()
			case *descpb.Descriptor_Database:
				t.Database.Version = 1
				mut = dbdesc.NewBuilder(t.Database).BuildCreatedMutable()
			case *descpb.Descriptor_Schema:
				t.Schema.Version = 1
				mut = schemadesc.NewBuilder(t.Schema).BuildCreatedMutable()
			case *descpb.Descriptor_Function:
				t.Function.Version = 1
				mut = funcdesc.NewBuilder(t.Function).BuildCreatedMutable()
			case *descpb.Descriptor_Type:
				t.Type.Version = 1
				mut = typedesc.NewBuilder(t.Type).BuildCreatedMutable()
			}

			return errors.Wrapf(buf.WriteDescToBatch(ctx, true, mut, b), "desc %d %T", mut.GetID(), mut)
		}); err != nil {
			return err
		}
		if err := extracted.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
			if e.GetID() <= 103 {
				log.Infof(ctx, "skipping ns entry %v", e)
				return nil
			}
			log.Infof(ctx, "setting up ns entry %v", e)
			return errors.Wrapf(buf.UpsertNamespaceEntryToBatch(ctx, true, e, b), "namespace entry %v", e)
		}); err != nil {
			return err
		}
		return errors.Wrap(txn.Run(ctx, b), "running batch")
	})
}

func getCatalogForTenantAsOf(
	ctx context.Context, execCfg *sql.ExecutorConfig, tenantID roachpb.TenantID, asOf hlc.Timestamp,
) (all nstree.Catalog, _ error) {
	cf := descs.NewBareBonesCollectionFactory(execCfg.Settings, keys.MakeSQLCodec(tenantID))
	err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		err := txn.SetFixedTimestamp(ctx, asOf)
		if err != nil {
			return err
		}
		all, err = cf.NewCollection(ctx).GetAllFromStorageUnvalidated(ctx, txn)
		if err != nil {
			return err
		}

		return nil
	})
	return all, err
}

func init() {
	repstream.GetStreamIngestManagerHook = newStreamIngestManagerWithPrivilegesCheck
}
