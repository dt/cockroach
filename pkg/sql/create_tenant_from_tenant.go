// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterfork"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/zoneconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// createTenantFromTenantNode implements
//
//	CREATE VIRTUAL CLUSTER [IF NOT EXISTS] <dst> FROM <src>
//
// as a synchronous DDL: it allocates a fresh destination tenant by name,
// then invokes pkg/sql/clusterfork to clone the source tenant's keyspace
// into it as of the statement's read timestamp.
//
// Distinct from CREATE VIRTUAL CLUSTER ... FROM REPLICATION OF ..., which
// sets up an asynchronous replication stream from a remote cluster.
type createTenantFromTenantNode struct {
	zeroInputPlanNode
	ifNotExists bool
	dstSpec     tenantSpec
	srcSpec     tenantSpec
}

// CreateTenantFromTenantNode constructs the planNode for
// CREATE VIRTUAL CLUSTER ... FROM ... .
func (p *planner) CreateTenantFromTenantNode(
	ctx context.Context, n *tree.CreateTenantFromTenant,
) (planNode, error) {
	const op = "CREATE VIRTUAL CLUSTER FROM"
	dstSpec, err := p.planTenantSpec(ctx, n.TenantSpec, op)
	if err != nil {
		return nil, err
	}
	srcSpec, err := p.planTenantSpec(ctx, n.SourceTenantSpec, op)
	if err != nil {
		return nil, err
	}
	return &createTenantFromTenantNode{
		ifNotExists: n.IfNotExists,
		dstSpec:     dstSpec,
		srcSpec:     srcSpec,
	}, nil
}

func (n *createTenantFromTenantNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p

	// Privilege and system-tenant checks. ForkTenant performs the same
	// checks again, but we want to fail fast before allocating the dst
	// tenant.
	if err := CanManageTenant(ctx, p); err != nil {
		return err
	}
	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, "fork", p.execCfg.Settings); err != nil {
		return err
	}

	// Resolve the source tenant. The grammar pins this to a name; we
	// look it up in system.tenants.
	srcInfo, err := n.srcSpec.getTenantInfo(ctx, p)
	if err != nil {
		return errors.Wrap(err, "resolving source tenant")
	}
	srcID, err := roachpb.MakeTenantID(srcInfo.ID)
	if err != nil {
		return errors.Wrapf(err, "invalid source tenant ID %d", srcInfo.ID)
	}

	// Resolve the destination name. Grammar pins this to a name as well
	// (no by-ID destination since we're allocating a new tenant).
	_, dstName, err := n.dstSpec.getTenantParameters(ctx, p)
	if err != nil {
		return err
	}
	if dstName == "" {
		return pgerror.New(pgcode.Syntax,
			"destination tenant name is required for CREATE VIRTUAL CLUSTER FROM")
	}

	// Allocate the destination tenant the way PCR does: insert the row
	// directly with DataStateAdd so the destination keyspace is *not*
	// bootstrapped with system tables. Bootstrapping would lay down
	// data in the dst LSM that collides with what VirtualClone is
	// about to mount there.
	zcfg, err := zoneconfig.GetHydratedForTenantsRange(
		ctx, p.Txn(), p.ExtendedEvalContext().Descs)
	if err != nil {
		return errors.Wrap(err, "loading initial tenant zone config")
	}
	tenantInfo := &mtinfopb.TenantInfoWithUsage{
		SQLInfo: mtinfopb.SQLInfo{
			Name:      dstName,
			DataState: mtinfopb.DataStateAdd,
		},
	}
	dstID, err := CreateTenantRecord(
		ctx, p.execCfg.Codec, p.execCfg.Settings,
		p.InternalSQLTxn(),
		p.execCfg.SpanConfigKVAccessor.WithISQLTxn(ctx, p.InternalSQLTxn()),
		tenantInfo, zcfg,
		n.ifNotExists,
		p.execCfg.TenantTestingKnobs,
	)
	if err != nil {
		return errors.Wrapf(err, "allocating destination tenant %q", dstName)
	}
	if !dstID.IsSet() {
		// IF NOT EXISTS hit: tenant already existed. Don't fork into an
		// existing tenant; that's not a safe operation. Return without
		// error to match the IF NOT EXISTS contract.
		return nil
	}
	if dstID.Equal(srcID) {
		return errors.AssertionFailedf(
			"freshly allocated destination tenant %d collides with source", dstID)
	}

	// Fork. We use the planner's read timestamp as the fork point — the
	// source data is consistent at that time, and the planner already
	// holds it for snapshot isolation.
	t := hlc.Timestamp{WallTime: params.EvalContext().GetStmtTimestamp().UnixNano()}
	if err := clusterfork.ForkTenant(ctx, p.execCfg.DB, srcID, dstID, t); err != nil {
		return errors.Wrapf(err, "forking %d -> %d", srcID, dstID)
	}

	// Activate the destination: flip data_state from ADD to READY now that
	// the keyspace is populated. Service mode stays NONE — the user can
	// ALTER VIRTUAL CLUSTER ... START SERVICE later, matching the
	// no-bootstrap CREATE VIRTUAL CLUSTER baseline.
	dstInfo, err := GetTenantRecordByID(ctx, p.InternalSQLTxn(), dstID, p.execCfg.Settings)
	if err != nil {
		return errors.Wrap(err, "loading destination tenant after fork")
	}
	dstInfo.DataState = mtinfopb.DataStateReady
	if err := UpdateTenantRecord(ctx, p.execCfg.Settings, p.InternalSQLTxn(), dstInfo); err != nil {
		return errors.Wrap(err, "activating destination tenant after fork")
	}
	return nil
}

func (n *createTenantFromTenantNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *createTenantFromTenantNode) Values() tree.Datums            { return tree.Datums{} }
func (n *createTenantFromTenantNode) Close(_ context.Context)        {}
