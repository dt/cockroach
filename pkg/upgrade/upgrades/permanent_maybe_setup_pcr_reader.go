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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

func maybeSetupPCRStandbyReader(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if d.ReaderTenant == nil {
		return nil
	}
	id, ts, err := d.ReaderTenant.ReadFromTenantInfo(ctx)
	if err != nil {
		return err
	}
	if !id.IsSet() {
		return nil
	}

	log.Infof(ctx, "setting up read-only catalog as of %s reading from tenant %s", ts, id)
	// TODO(foundations): replace the rest of this with call to real API.
	if err := todoSetupCatalogPlaceholder(ctx, id, ts, d.DB.KV(), d.Settings, d.Codec); err != nil {
		return err
	}

	// TODO(DR team): create a job that will poll the ts and advance the catalog.
	return nil
}

// setupCatalogPlaceholder is a placeholder for a foundations-supplied API that
// sets up or updates an catalog.
func todoSetupCatalogPlaceholder(
	ctx context.Context,
	fromID roachpb.TenantID,
	asOf hlc.Timestamp,
	db *kv.DB,
	settings *cluster.Settings,
	codec keys.SQLCodec,
) error {
	var extracted nstree.Catalog
	cf := descs.NewBareBonesCollectionFactory(settings, keys.MakeSQLCodec(fromID))
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		err := txn.SetFixedTimestamp(ctx, asOf)
		if err != nil {
			return err
		}
		descs := cf.NewCollection(ctx)
		defer descs.ReleaseAll(ctx)
		extracted, err = descs.GetAllFromStorageUnvalidated(ctx, txn)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	m := mon.NewUnlimitedMonitor(ctx, mon.Options{
		Name:     "tenant_reader",
		Res:      mon.MemoryResource,
		Settings: settings,
	})
	// Inherit session data, so that we can run validation.
	writeDescs := descs.NewBareBonesCollectionFactory(settings, codec).
		NewCollection(ctx, descs.WithMonitor(m))
	writeDescs.SkipValidationOnWrite()
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Reset any state between txn retries.
		defer writeDescs.ReleaseAll(ctx)
		// Resolve any existing descriptors within the tenant, which
		// will be use to compute old values for writing.
		existingDescriptors, err := writeDescs.GetAllFromStorageUnvalidated(ctx, txn)
		if err != nil {
			return err
		}
		b := txn.NewBatch()
		if err := extracted.ForEachDescriptor(func(desc catalog.Descriptor) error {
			if desc.GetParentID() == keys.SystemDatabaseID ||
				desc.GetID() == keys.SystemDatabaseID {
				return nil
			}
			// If there is an existing descriptor with the same ID, we should
			// determine the old bytes in storage for the upsert.
			var existingRawBytes []byte
			if existingDesc := existingDescriptors.LookupDescriptor(desc.GetID()); existingDesc != nil {
				existingRawBytes = existingDesc.GetRawBytesInStorage()
			}
			var mut catalog.MutableDescriptor
			switch t := desc.DescriptorProto().GetUnion().(type) {
			case *descpb.Descriptor_Table:
				t.Table.Version = 1
				mutBuilder := tabledesc.NewBuilder(t.Table)
				if existingRawBytes != nil {
					mutBuilder.SetRawBytesInStorage(existingRawBytes)
				}
				mutTbl := mutBuilder.BuildCreatedMutable().(*tabledesc.Mutable)
				mut = mutTbl
				// Convert any physical tables into external row tables.
				// Note: Materialized views will be converted, but their
				// view definition will be wiped.
				if mutTbl.IsPhysicalTable() {
					mutTbl.ViewQuery = ""
					mutTbl.IsMaterializedView = !mutTbl.IsSequence()
					mutTbl.External = &descpb.ExternalRowData{TenantID: fromID, TableID: desc.GetID(), AsOf: asOf}
				}
			case *descpb.Descriptor_Database:
				t.Database.Version = 1
				mutBuilder := dbdesc.NewBuilder(t.Database)
				if existingRawBytes != nil {
					mutBuilder.SetRawBytesInStorage(existingRawBytes)
				}
				mut = mutBuilder.BuildCreatedMutable()
			case *descpb.Descriptor_Schema:
				t.Schema.Version = 1
				mutBuilder := schemadesc.NewBuilder(t.Schema)
				if existingRawBytes != nil {
					mutBuilder.SetRawBytesInStorage(existingRawBytes)
				}
				mut = mutBuilder.BuildCreatedMutable()
			case *descpb.Descriptor_Function:
				t.Function.Version = 1
				mutBuilder := funcdesc.NewBuilder(t.Function)
				if existingRawBytes != nil {
					mutBuilder.SetRawBytesInStorage(existingRawBytes)
				}
				mut = mutBuilder.BuildCreatedMutable()
			case *descpb.Descriptor_Type:
				t.Type.Version = 1
				mutBuilder := typedesc.NewBuilder(t.Type)
				if existingRawBytes != nil {
					mutBuilder.SetRawBytesInStorage(existingRawBytes)
				}
				mut = mutBuilder.BuildCreatedMutable()
			}
			return errors.Wrapf(writeDescs.WriteDescToBatch(ctx, true, mut, b),
				"unable to create replicated descriptor: %d %T", mut.GetID(), mut)
		}); err != nil {
			return err
		}
		if err := extracted.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
			if e.GetParentID() == keys.SystemDatabaseID ||
				e.GetID() == keys.SystemDatabaseID {
				return nil
			}
			return errors.Wrapf(writeDescs.UpsertNamespaceEntryToBatch(ctx, true, e, b), "namespace entry %v", e)
		}); err != nil {
			return err
		}
		return errors.Wrap(txn.Run(ctx, b), "running batch")
	})
}
