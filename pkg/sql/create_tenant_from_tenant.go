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
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/zoneconfig"
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

	// CREATE VIRTUAL CLUSTER FROM commits side-effecting work (the dst
	// tenant record, ForkTenant's KV mutations, the activation flip)
	// across multiple independent transactions and runs each step on
	// the assumption that prior steps have committed. That model is
	// incompatible with an explicit BEGIN/COMMIT block where the
	// caller controls when the planner txn commits.
	if !p.ExtendedEvalContext().TxnIsSingleStmt {
		return pgerror.Newf(pgcode.InvalidTransactionState,
			"CREATE VIRTUAL CLUSTER FROM cannot be used inside a multi-statement transaction")
	}

	// Commit the planner txn now, before any of the side-effecting work
	// below runs. ForkTenant takes seconds to minutes; running it under
	// the planner txn would expose us to txn refresh errors, and more
	// importantly the orchestration's KV mutations auto-commit through
	// the raw DB independently of the planner txn. Committing here makes
	// each subsequent ISQL txn the unambiguous home for its own writes.
	if err := p.Txn().Commit(ctx); err != nil {
		return err
	}
	// Releasing descriptor leases held by the (now-committed) planner
	// txn. We're about to do schema-affecting work in fresh ISQL txns;
	// holding leases acquired under the old txn would interfere. Pattern
	// borrowed from CREATE LOGICAL REPLICATION STREAM (see
	// pkg/crosscluster/logical/create_logical_replication_stmt.go).
	p.InternalSQLTxn().Descriptors().ReleaseAll(ctx)

	// Allocate the destination tenant the way PCR does: insert the row
	// directly with DataStateAdd so the destination keyspace is *not*
	// bootstrapped with system tables. Bootstrapping would lay down data
	// in the dst LSM that collides with what VirtualClone is about to
	// mount there.
	//
	// The insert runs in its own auto-commit internal txn so the
	// system.tenants row is durable BEFORE ForkTenant starts mutating
	// /Tenant/<dstID>/... Otherwise: ForkTenant errors → tenant row
	// never committed → ForkTenant's KV side effects are orphaned under
	// a tenant ID nobody knows is in use; no DROP TENANT path can reach
	// them, and the ID can be reallocated to a future CREATE TENANT,
	// which will then bootstrap on top of garbage. With the standalone
	// commit, a half-baked fork is always visible in SHOW TENANTS as
	// DataStateAdd and droppable via DROP TENANT (modulo the force-drop
	// TODO in clusterfork.go for the case where dst ranges still hold
	// the InconsistencyLease at fail time).
	var dstID roachpb.TenantID
	if err := p.execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		zcfg, err := zoneconfig.GetHydratedForTenantsRange(
			ctx, txn.KV(), p.ExtendedEvalContext().Descs)
		if err != nil {
			return errors.Wrap(err, "loading initial tenant zone config")
		}
		tenantInfo := &mtinfopb.TenantInfoWithUsage{
			SQLInfo: mtinfopb.SQLInfo{
				Name:      dstName,
				DataState: mtinfopb.DataStateAdd,
			},
		}
		id, err := CreateTenantRecord(
			ctx, p.execCfg.Codec, p.execCfg.Settings,
			txn,
			p.execCfg.SpanConfigKVAccessor.WithISQLTxn(ctx, txn),
			tenantInfo, zcfg,
			n.ifNotExists,
			p.execCfg.TenantTestingKnobs,
		)
		if err != nil {
			return errors.Wrapf(err, "allocating destination tenant %q", dstName)
		}
		dstID = id
		return nil
	}); err != nil {
		return err
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

	// Use the HLC clock rather than wall-clock-derived stmtTimestamp:
	// the seed writes on the source tenant get HLC-issued timestamps
	// that can be ahead of any single node's wall clock, so a wall-clock
	// T may sit BELOW data we'd revert against.
	t := p.execCfg.Clock.Now()
	if err := clusterfork.ForkTenant(ctx, p.execCfg.DB, srcID, dstID, t); err != nil {
		return errors.Wrapf(err, "forking %d -> %d", srcID, dstID)
	}

	// Activate the destination: flip data_state from ADD to READY now
	// that the keyspace is populated.
	//
	// As with the create, this update runs in its own auto-commit
	// internal txn so the activation is durable as soon as the fork
	// succeeds. Keeping it in the planner txn would risk a successful
	// fork being silently reverted to ADD state if the planner txn later
	// rolls back (e.g., on client disconnect after startExec returns).
	var dstInfo *mtinfopb.TenantInfo
	if err := p.execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		info, err := GetTenantRecordByID(ctx, txn, dstID, p.execCfg.Settings)
		if err != nil {
			return errors.Wrap(err, "loading destination tenant after fork")
		}
		info.DataState = mtinfopb.DataStateReady
		if err := UpdateTenantRecord(ctx, p.execCfg.Settings, txn, info); err != nil {
			return errors.Wrap(err, "activating destination tenant after fork")
		}
		dstInfo = info
		return nil
	}); err != nil {
		return err
	}

	// Mirror the source's service mode onto the destination. setTenantService
	// drives its own transactions internally, so it must run after the
	// activation txn above commits. ServiceModeNone is the default for a
	// freshly-created tenant, so we only act if the source had a service.
	if srcInfo.ServiceMode == mtinfopb.ServiceModeNone {
		return nil
	}
	if err := p.setTenantService(ctx, dstInfo, srcInfo.ServiceMode); err != nil {
		return errors.Wrapf(err, "starting service %q on destination tenant", srcInfo.ServiceMode)
	}
	return nil
}

func (n *createTenantFromTenantNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *createTenantFromTenantNode) Values() tree.Datums            { return tree.Datums{} }
func (n *createTenantFromTenantNode) Close(_ context.Context)        {}
