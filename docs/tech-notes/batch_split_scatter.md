# Batched Split + Scatter Design

## Part 1: Batched Split (implemented, see dt/batch-split-poc)

See the PoC branch for the full implementation. Summary: extends
`AdminSplitRequest` with `additional_split_keys` to split a range at N
keys in a single raft proposal. Achieves 2500-3500 splits/sec on empty
ranges, 11-35x faster than parallel per-key `AdminSplit`.

## Part 2: Scatter-at-Split-Time

### Problem

After batched split creates 20k empty ranges, they all live on the same
3 stores (the LHS replicas). Scatter needs to spread them across a
100-node cluster. Today's `AdminScatter` sends one `AdminRelocateRange`
per range, each writing to meta2 via `AdminChangeReplicas`. That's ~3N
meta2 descriptor writes (add learner + promote + leave joint config per
replica change), all funneling through the meta2 leaseholder.

### Design: integrated scatter in AdminSplit

Fold replica placement into the split transaction. The split txn already
writes N RHS descriptors to meta2 — writing them with the target replica
sets instead of inheriting the LHS replicas costs zero additional meta2
writes.

### API

```proto
message AdminSplitRequest {
  // ... existing fields ...
  repeated bytes additional_split_keys = 6;

  // Per-RHS target replicas. len == len(additional_split_keys) + 1.
  // Each entry specifies the desired replica stores for that RHS.
  // If empty, all RHS inherit LHS replicas (existing behavior).
  repeated ReplicaTargets rhs_replica_targets = 7;
}

message ReplicaTargets {
  repeated roachpb.ReplicationTarget targets = 1;
}
```

### Proposer path

`adminBatchSplit` gains an optional `rhs_replica_targets` parameter.
When set:

1. Run the allocator N times to compute target replica sets. Each call
   tells the allocator to "pretend" ranges already allocated so far
   exist, preventing the allocator from stacking all 20k ranges on the
   same stores.
2. Build each RHS descriptor with its target replica set (not the LHS
   replicas).
3. Split txn writes all descriptors in one txn (same as today's batched
   split).

### Two apply-time behaviors based on LHS emptiness

The split trigger applies on all LHS stores. For each RHS, the LHS
store checks whether it's in that RHS's target replica set:

**Empty RHS** (LHS was empty at split time):

- LHS store is NOT in any RHS descriptor.
- Pre-apply: takes the `rhsDestroyed` path, clears replicated state.
- Post-apply (leaseholder only): sends `InitReplica` RPC to each target
  store. Payload is ~500 bytes of metadata (descriptor, lease, GC
  threshold, version), constructed from the trigger — no engine reads.
- LHS store holds zero extra replicas when `AdminSplit` returns.

**Non-empty RHS** (LHS had data):

- RHS descriptor written in joint configuration: target stores as
  `VOTER_INCOMING`, LHS stores not in target set as
  `VOTER_DEMOTING_LEARNER`.
- LHS stores apply the split trigger normally, creating the RHS with
  data.
- LHS stores (as demoting voters) send raft snapshots to incoming
  voters. Once incoming voters catch up, raft leaves joint config.
- LHS stores shed replicas asynchronously via replicate queue.

### New RPC: InitReplica

For the empty-range fast path. Sent by the leaseholder during
`batchSplitPostApply` to each target store.

```proto
service Internal {
  rpc InitReplica(InitReplicaRequest) returns (InitReplicaResponse);
}

message InitReplicaRequest {
  roachpb.RangeDescriptor desc = 1;
  roachpb.Lease lease = 2;
  hlc.Timestamp gc_threshold = 3;
  roachpb.GCHint gc_hint = 4;
  roachpb.Version replica_version = 5;
  hlc.Timestamp closed_timestamp = 6;
}
```

**Receiver behavior:**

1. Check for overlapping initialized ranges.
2. `getOrCreateReplica` (may find an existing uninitialized replica from
   pre-vote messages).
3. If already initialized: return success (idempotent).
4. `WriteInitialReplicaState` + `SynthesizeRaftState` + set closed
   timestamp. Same code paths as the split trigger.
5. Mark initialized, add to store's replica map.
6. Campaign to form raft group with the other target voters.

**Raft group formation:** Once 2 of 3 target voters are initialized,
they form quorum and elect a leader. The third catches up via raft
snapshot (trivially small for empty ranges).

### Shedding non-empty-range replicas

For non-empty ranges where LHS stores have demoting replicas:

After `AdminSplit` returns, the caller sends an RPC to each LHS store:
"check your replicas — if any are in ranges where you're
`VOTER_DEMOTING_LEARNER` or the range is over-replicated, enqueue them
in the replicate queue."

This is O(LHS stores) RPCs, not O(ranges). The replicate queue handles
the actual removal, writing one meta2 descriptor update per removal.
These writes are non-contending because each range has its own meta2
key (unlike pre-split scatter where all ranges contend on the same
meta2 range).

**Checking completion:** The caller polls each LHS store with "do you
still hold any replicas where you're `VOTER_DEMOTING_LEARNER`?" This is
one query per store, not per range. Once all stores return zero, the
anchor nodes are clean and data import can begin.

### Caller flow

```go
keys, targets := planSplitAndScatter(tableSpan, clusterStores, rf)

err := splits.AdminSplitMany(ctx, db, keys, exp,
    splits.WithReplicaTargets(targets))
// Returns when:
//   - All descriptors in meta2
//   - Empty ranges: InitReplica RPCs sent, LHS stores hold nothing
//   - Non-empty ranges: joint config in place, demoting voters active

// For empty ranges: safe to start import immediately.
// For non-empty ranges: trigger shedding, then wait.
if hasNonEmptyRanges {
    for _, store := range lhsStores {
        triggerShedding(ctx, store)
    }
    waitForCleanStores(ctx, lhsStores) // polls per-store, not per-range
}

startImport(ctx, tableSpan)
```

### Cost comparison

| Operation | Status quo (split + scatter) | Batched (empty ranges) | Batched (non-empty) |
|---|---|---|---|
| Meta2 writes | N (split) + ~3N (scatter) | N (split only) | N (split) + N (leave joint) |
| RPCs | N AdminScatter + ~3N ChangeReplicas | N×RF InitReplica (~500B each) | N×RF raft snapshots |
| Raft proposals | N (split) + ~3N (ChangeReplicas) | N (split only) | N (split) + N (leave joint) |
| Anchor store replicas to shed | N | 0 | N (via replicate queue) |
| Completion signal | poll N ranges | synchronous return | poll O(stores) |

For 20k ranges on a 100-node cluster:
- Status quo: ~80k meta2 writes, contending
- Empty batched: ~20k meta2 writes (split txn) + 60k tiny RPCs, non-contending
- Non-empty batched: ~40k meta2 writes, non-contending

### Failure modes

| Failure | Behavior |
|---|---|
| Target store down during InitReplica | Other 2 targets form quorum. Down store gets raft snapshot on recovery. |
| Leaseholder crashes after split txn, before InitReplica | Descriptors in meta2 point to uninitialized targets. Recovery: any store seeing an uninitialized range listed in meta2 can re-send InitReplica. The restore job coordinator can detect and retry. |
| InitReplica for already-initialized range | Idempotent, return success. |
| Allocator can't find targets | Fall back to LHS replicas (existing behavior). |

### What to build

1. `ReplicaTargets` proto + `rhs_replica_targets` field on `AdminSplitRequest`
2. Allocator integration in `adminBatchSplit` (N calls with "pretend"
   allocated set)
3. `InitReplicaRequest`/`InitReplicaResponse` protos
4. `Store.InitReplica` handler (~50 lines, reuses existing init paths)
5. `initRemoteReplica` sender in `batchSplitPostApply`
6. `WithReplicaTargets` option on `AdminSplitMany` caller helper
7. Joint-config descriptor writing for non-empty ranges
8. "Trigger shedding" + "check clean" store-level RPCs
9. Recovery path for leaseholder crash between split and InitReplica

### Out of scope (future work)

- Non-raft bootstrap for non-empty ranges (would eliminate all
  post-split meta2 writes for non-empty case too, but requires new
  snapshot-like data transfer mechanism).
- Allocator batching (making N allocator calls faster by batching the
  constraint evaluation).
- Lease placement optimization (round-robin initial lease holders
  across target stores to spread load even before replicas are fully
  placed).
