// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package splits

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// AdminSplitMany splits the range(s) covering keys at every key in keys.
// Keys must be sorted ascending. Idempotent: keys already at a range
// boundary are silently dropped server-side.
func AdminSplitMany(ctx context.Context, db *kv.DB, keys []roachpb.Key, exp hlc.Timestamp) error {
	if len(keys) == 0 {
		return nil
	}
	return splitManyRecurse(ctx, db, keys, exp)
}

func splitManyRecurse(ctx context.Context, db *kv.DB, keys []roachpb.Key, exp hlc.Timestamp) error {
	if len(keys) == 0 {
		return nil
	}
	req := &kvpb.AdminSplitRequest{
		RequestHeader:       kvpb.RequestHeader{Key: keys[0]},
		SplitKey:            keys[0],
		AdditionalSplitKeys: keys[1:],
		ExpirationTime:      exp,
	}
	_, pErr := kv.SendWrapped(ctx, db.NonTransactionalSender(), req)
	if pErr == nil {
		return nil
	}
	err := pErr.GoError()
	var mismatch *kvpb.RangeKeyMismatchError
	if !errors.As(err, &mismatch) {
		return err
	}
	ri, riErr := mismatch.MismatchedRange()
	if riErr != nil {
		return riErr
	}
	i := bisectFirstOutside(keys, &ri.Desc)
	if i == 0 || i == len(keys) {
		return err
	}
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return splitManyRecurse(gctx, db, keys[:i], exp) })
	g.Go(func() error { return splitManyRecurse(gctx, db, keys[i:], exp) })
	return g.Wait()
}

func bisectFirstOutside(keys []roachpb.Key, desc *roachpb.RangeDescriptor) int {
	return sort.Search(len(keys), func(i int) bool {
		return !desc.ContainsKey(roachpb.RKey(keys[i]))
	})
}
