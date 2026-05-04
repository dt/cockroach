# Cluster Forking: Copy-on-Write Tenant Snapshots

## Motivation

Forking a cluster is an invaluable way to provide a low-risk sandbox to
test changes, check the performance of query plans with a new index, or
otherwise experiment. In an era where powerful AI agents are increasingly
being given such tasks but remain susceptible to mistakes from confusing
naming, missing context, or just hallucinations, sandboxes are more useful
than ever.

Such a sandbox must be cheap to create: restoring an entire copy of a
cluster to a test cluster is too slow and expensive to be a viable option
every time one needs to test or validate a change, particularly if many
agents are acting concurrently. At the same time, to deliver its core
value of being a *representative* experiment, a forked cluster must have
the same data and perform similarly to the original — copying only a
subset of the data, or proxying access through extra or more expensive
layers, would make such snapshots no longer usable for the experiments
they are intended to serve.

`CREATE VIRTUAL CLUSTER B FROM A` is a new feature that produces virtual
cluster B as a logical snapshot of A as of some chosen MVCC timestamp T,
with no full-data copy and no source-tenant downtime. It is cheap to
create because Pebble virtual SSTs reference the source's backing
physical SSTs until either a tenant write or replica movement overwrites
them, and the underlying virtual-SST machinery (developed for online
restore) is designed for minimal overhead.

## Design

The implementation has two stages: 
1) for each range of the source tenant, produce a "clone" of that range that has
the correct number of byte-consistent replicas using pebble's ability to alias
existing sstables via "virtual" sstables.
2) adjust the cloned ranges to be MVCC-consistent with each, other by truncating
them to a common timestamp. 

Stage 1 is the substantive new work including a new pebble API called via a new
raft command; Stage 2 is standard PCR-cutover (`RevertRange`).

### Stage 1: per-range cloning

Data in the source tenant lives in ranges whose replicas store their
underlying data in files on disparate per-store LSMs. The forked tenant's
ranges thus need their replicas on those same LSMs in order to share the
underlying files.

A Pebble-level `VirtualClone` API for cloning from one span to another
using virtual SSTs makes the per-store clone zero-copy. The wire-up
challenge is cross-replica: a single range's replicas live on different
nodes, and after the clone they must agree on the data they expose.
Naively invoking the clone once per store, or once per destination range,
captures each source replica at a different applied log index — leaving
the resulting clone replicas in disagreement. The source replicas
themselves would converge as they keep applying log entries, but the
*destination* replicas have their own raft log and would not.

So the LSM-level clone is applied by the source replicas themselves, via
a raft command at a specific applied index — `CloneData` rides the
source range's raft log. The apply hook on each source-range replica
calls `Engine.VirtualClone` on the local store, which mounts virtual
SSTs aliasing the source span under the destination prefix. Because the
command rides raft, every source replica applies it deterministically;
because the destination range's replicas are colocated with the source
range's replicas (same set of stores), the cloned data lands on every
store hosting a destination replica.

This implies that the *content* of the destination range changes out
from under its replicas as the source replicas apply `CloneData`. Since
source and destination replicas are independent, one destination
replica's local store may gain the cloned content before another's,
during which cross-replica consistency on the destination is violated.
The destination range tolerates this transient inconsistency via a new
`InconsistentReplicas` bit on its `RangeDescriptor`. While the bit is
set, the consistency checker is hands-off, non-admin requests are
rejected (so no caller can observe the inconsistency), and the merge
queue and replica GC queue leave the range alone. The bit is inherited
through splits.

### Stage 2: cross-range MVCC consistency

After each range has been cloned and arrived at a consistent set of
replicas, the destination tenant contains many ranges, but because they
cloned at slightly different wall-clock times some may have replicated
MVCC history to a higher timestamp than others. This is exactly the
state a tenant is in mid-replication by PCR: the same `RevertRange`
machinery used at PCR cutover truncates each range's MVCC history to a
common target timestamp T, ensuring the tenant has consistent MVCC
history up to T.

### Process timeline

1. Install a PTS at chosen target time T on the source tenant's
   keyspan, so MVCC history is preserved for the eventual revert while
   the operation runs.
2. Allocate destination tenant B with `data_state = ADD` (no
   bootstrap, so the destination keyspace is not seeded with system
   tables that would collide with the cloned data). This is the same
   primitive PCR uses for its own destination tenant.
3. Lock the destination tenant's parent range by setting
   `InconsistentReplicas` to a future timestamp via
   `AdminSetReplicaInconsistency`.
4. Pre-split the destination tenant to mirror the source range layout
   1:1 — one destination range per source range, bounds prefix-rewritten
   to B. The bit inherits through splits.
5. `AdminRelocateRange` each destination range so its replicas live on
   the same stores as the corresponding source range. Destination
   replicas are trivially byte-identical at this point because they're
   all empty.
6. For each source range R, propose a `CloneData(srcSpan, dstSpan)` raft
   command into R's raft log. Apply on each R replica:
   a. (Pebble internal) flush any memtable contents overlapping
      srcSpan;
   b. excise dstSpan and atomically install virtual SSTs aliasing
      srcSpan under dstPrefix on the local store.
   Once all R replicas have applied, the destination range's replicas
   are byte-identical from the point of view of read and write APIs the
   replica exposes (the underlying SST files and layouts may differ
   per store, but the LSM-visible content is the same).
7. Verify each destination replica reports complete coverage of its
   range, then clear `InconsistentReplicas` on its descriptor (a normal
   descriptor mutation through the existing pathway). The destination
   range is now a normal CRDB range.
8. Run `RevertRange` over the destination tenant span back to T to make
   all destination ranges cross-range MVCC-consistent. Release the PTS.
9. `ClearRange` the three system tables PCR's `KeyRewriter` elides
   during stream ingestion (`sql_instances`, `sqlliveness`, `lease`) —
   their contents from the source would otherwise spuriously appear as
   active SQL pods / sessions / leases on the destination.
10. Activate the destination by flipping `data_state` to `READY`.
    Service mode stays `NONE` until a subsequent
    `ALTER VIRTUAL CLUSTER ... START SERVICE`.

## Implementation

### `RangeDescriptor.InconsistentReplicas`

The bit is implemented as a `hlc.Timestamp` that doubles as an
inconsistency lease. Values:

- *Zero / empty.* Range is in the normal state.
- *Future timestamp.* Range is locked. The orchestration is expected to
  clear it before the timestamp passes.
- *Past timestamp ("expired").* The orchestration evidently failed; KV's
  deadman cleanup is authorized to ClearRange the keyspan and clear the
  bit. Until that cleanup runs, the range continues to be assumed
  inconsistent and remains locked — an expired lease is not a
  "safe to proceed" signal, only an authorization for KV to do the
  rollback. (Optionally encoded as a negative-WallTime sentinel
  meaning "aborted, awaiting acknowledgement"; the orchestration can
  detect this and clean up cooperatively.)

Guards on the bit:

- **Consistency queue:** skip ranges with `InconsistentReplicas` set
  (replicas legitimately disagree mid-clone).
- **Merge queue:** skip — merging out a locked range would lose the bit.
- **Replica GC queue:** skip — same reason.
- **Request dispatch:** in `Replica.SendWithWriteBytes`, reject any
  non-admin batch that touches user keys when the bit is set. Three
  exemptions:
  - Admin commands themselves (split, relocate, the clear of the bit
    itself, etc.).
  - Batches whose keys are entirely local — internal txn writes on the
    `RangeDescriptor` key, used by `AdminSetReplicaInconsistency` to
    clear the bit.
  - Lease-management batches (`LeaseInfoRequest`, `RequestLeaseRequest`,
    `TransferLeaseRequest`). `AdminChangeReplicas` and
    `AdminRelocateRange` issue these internally to discover the
    leaseholder and hand off the lease before removing a voter. Without
    this exemption an orchestration that has just locked a range can't
    move replicas on it, deadlocking against the bit it set itself.

The bit is set/cleared via `AdminSetReplicaInconsistency`, an admin
command that runs through a transaction CPut on the descriptor key. The
transaction includes an `InternalCommitTrigger`
(`InconsistencyLeaseTrigger`) modeled on `StickyBitTrigger` whose
handler in `cmd_end_transaction.go` emits
`Replicated.State{Desc: &newDesc}` so the apply path installs the
descriptor into the in-memory replica state. **Without this trigger,
the bit reads correctly out of meta2 but the queues / dispatch reject
lag** — caught in prototype.

The bit inherits through splits via the standard descriptor inheritance
in `splitTrigger`.

### `CloneData` raft command

`CloneData` is a range-scoped raft command. The request carries:

```
CloneDataRequest {
  Header (KeyRange) — the source span (within the source range)
  SrcPrefix []byte
  DstSpan KeyRange
  DstPrefix []byte
}
```

Eval (`batcheval.EvalCloneData`) validates the spans/prefixes and
records them in `Replicated.CloneData`. Apply
(`appBatch.runPostAddTriggers`) calls `Engine.VirtualClone(srcSpan,
srcPrefix, dstSpan, dstPrefix)` on the local store engine.

The apply path is meant to be idempotent on raft replay: re-applying
must detect that the destination already holds the cloned content and
no-op. Because the new Pebble contract is destructive (excises dstSpan
atomically with the install), re-applying after the destination range
has been unlocked would wipe whatever sits there. The apply hook
must consult the destination range descriptor at apply time and
no-op if `InconsistentReplicas` is empty or has lapsed — see the
"Replay safety" open issue below.

#### Wire-through gotchas (caught in prototype)

- `Engine.VirtualClone` requires `FormatPrefixSubstitution` (Pebble
  FMV 31). `pebbleFormatVersionMap` for the latest cluster version
  must include this so newly-created stores opt in.
- The source/destination *spans* passed to Pebble are wrapped in
  engine-key form (`EngineKey{Key: ...}.Encode()`); the source/destination
  *prefixes* are passed raw (no engine-key encoding). The substitution
  operates byte-for-byte on what is literally shared at the start of
  every in-block encoded key — for a tenant prefix that is the
  varint-encoded tenant ID with no MVCC sentinel. Wrapping the prefixes
  would append a 0x00 byte that doesn't exist at that offset in any data
  key, causing Pebble's per-block validation to (correctly) reject.
- The new `Replicated.CloneData` side-effect type needs a case in
  `result.MergeAndDestroy` or apply-time merging panics with
  "unhandled EvalResult". Easy to miss; it's a fatal panic, not a
  build error.
- The destination span is NOT derivable from `(srcSpan, srcPrefix,
  dstPrefix)` alone. CRDB's standard span shape is
  `[prefix, prefix.PrefixEnd())`, where `PrefixEnd()` is by
  construction *outside* the prefix — substitution doesn't apply to it.
  The caller (CRDB) constructs `dstSpan` explicitly and passes it.

### Pebble `VirtualClone`

API (Pebble side):

```go
func (d *DB) VirtualClone(
    ctx context.Context,
    srcSpan KeyRange,
    srcPrefix []byte,
    dstSpan KeyRange,
    dstPrefix []byte,
) error
```

Semantically: atomically excise dstSpan, then install virtual SSTs
referencing every source SST that intersects srcSpan, with each
virtual SST's keys appearing under `dstPrefix` instead of `srcPrefix`.
The mechanism is a single block-shared-prefix substitution applied at
key-reconstruction time inside the colblk data-block iterator (the
same architectural layer where `SyntheticPrefix` already lives).

Detailed Pebble-side design and implementation notes are in
**Appendix A**.

### Orchestration (`pkg/sql/clusterfork.ForkTenant`)

`ForkTenant(ctx, db, src, dst, t)` runs the steps above. Each phase is
wrapped in a `tracing.ChildSpan` for visibility:

```
clusterfork.ForkTenant
├── clusterfork.lock-dst-parent
├── clusterfork.pre-split-dst
├── clusterfork.relocate-dst
├── clusterfork.clone-data-all
│   └── clusterfork.clone-data-range  (one per src range)
├── clusterfork.unlock-dst
├── clusterfork.revert-dst-to-t
└── clusterfork.clear-elided-tables
```

The PTS install (step 1), lease heartbeat, deadman cleanup, and
unlock-time per-replica missing-spans verification (step 7's "verify
each destination replica reports complete coverage") are all currently
**TODO** — the prototype trusts the operator to keep durations short
enough that GC doesn't run on src below T and that the orchestration
finishes before its inconsistency-lease lapses.

### SQL surface

```
CREATE VIRTUAL CLUSTER [IF NOT EXISTS] <dst> FROM <src>
```

A synchronous DDL that runs through a `planNode` calling
`clusterfork.ForkTenant`. Sits next to the existing
`CREATE VIRTUAL CLUSTER ... FROM REPLICATION OF ...` (PCR) grammar.

The planNode allocates the destination tenant the way PCR does: insert
the row directly with `DataState=ADD` (no bootstrap), then after the
fork flip to `DataState=READY`. Service mode stays `NONE`; the user
runs `ALTER VIRTUAL CLUSTER ... START SERVICE` separately.

The fork timestamp T defaults to the planner's read timestamp.

## Open issues / required follow-ups

The prototype validates the architecture but leaves several real
correctness gaps that must be closed before this can ship.

### Value checksum incompatibility

`roachpb.Value` stores `[crc32(key || payload) | tag | payload]`. The
crc is computed at write time over key+payload. `VirtualClone` aliases
a value at one key (e.g. `/Tenant/3/foo`) into another (e.g.
`/Tenant/4/foo`). Pebble has no opinion about value content, so the
stored crc is preserved. Reads at the new key then see a value whose
stored crc was computed against the *old* key.

Production today: silently works — `Value.Verify` only runs in test
builds, so production reads succeed. But the cluster has effectively
disarmed line-level corruption detection on cloned ranges: any actual
corruption (bit flip in a value) goes undetected because every value
already mismatches its checksum by design.

Tests: noisy false positives on remote reads of cloned data. The
prototype disables the test-build verifier in
`kvclient/kvcoord/transport.go` to make progress.

The right answer is a per-range "values carry checksums computed
against original keys" bit on the descriptor (or possibly a per-tenant
bit on the tenant record). `Value.Verify` and the consistency checker
skip the checksum check on cloned ranges; cross-replica consistency
checks switch to a reduced check that doesn't require checksum equality
(e.g. decoded-payload comparison). Anything that requires a strict
checksum gets a clear error pointing to the cloned-range origin.

### Apply-time replay safety / non-fataling

Two related issues, both in the `CloneData` apply hook:

1. **Replay over an unlocked destination wipes data.** The new Pebble
   contract excises dstSpan atomically with install. If the source
   range's CloneData entry replays (e.g. crash-and-restart before the
   applied index advanced past the entry), and the destination range
   has already been unlocked + has user writes, the replay re-excises
   and re-installs, wiping the user writes. The fix is to look up the
   destination range descriptor at apply time and no-op if
   `InconsistentReplicas` is empty or has lapsed. The natural place is
   `runPostAddTriggersReplicaOnly` (where `b.r.store` is reachable);
   the prototype currently runs the hook in
   `runPostAddTriggers` (no replica access) with a TODO to relocate it.

2. **Apply errors fatal the node, hanging the client.**
   `runPostAddTriggers` returning a non-nil error → `maybeFatalOnRaftReadyErr`
   panics the node → all source-range replicas panic → DistSender on
   the orchestration sees connection errors → retries forever → test
   "hangs" (eventually times out). The structural fix is an apply-time
   error category that returns to the client as a non-retriable
   permanent failure rather than fataling. The prototype workaround:
   the apply hook catches Pebble's `ErrUnsupportedClone` and similar
   and logs-and-skips (apply succeeds with no work done); the
   orchestration's eventual completeness verification at unlock detects
   the missing data and fails cleanly. Tests today detect the gap via
   their own dst-key reads failing.

Both issues collapse into one cleanly when the apply hook lives in
`runPostAddTriggersReplicaOnly`: the descriptor lookup happens before
the VirtualClone call; an empty/lapsed bit means skip; a Pebble error
also means log-and-skip with the unlock-time verifier as the
detection layer.

### Per-replica missing-spans verification at unlock

Today the unlock path (`AdminSetReplicaInconsistency` clearing the
bit) trusts that the orchestration drove `CloneData` to completion on
every replica. It doesn't verify that the data actually landed on each
replica's local store.

The chosen design (deferred to a follow-up):

Each replica persists a small `[]Span` "what I'm still missing" in
range-id-local storage, **included in raft snapshots**:

- Initialized to `[range span]` by the apply path that observes the
  replica's `InconsistentReplicas` field transition empty → non-zero
  (the descriptor change drives it via the standard raft replication
  path; no separate fanout).
- Subtracted by the `CloneData` apply hook on the local store when a
  cloned span intersects this dst range.
- Included verbatim in raft snapshots: a replica created from a
  snapshot of a complete sender (missing-spans = `[]`) inherits
  completeness; one created from an incomplete sender inherits the
  truthful incompleteness.

Unlock fans out a `VerifyReplicaComplete` RPC asking each replica its
missing-spans; if any non-empty, returns an error listing the
incomplete replicas so the orchestration can `ChangeReplicas` to evict
them. Replacements catch up via standard raft snapshot from a complete
peer (missing-spans = `[]` propagates), and the orchestration retries
the unlock.

This was the chosen design after rejecting a simpler "marker-add"
approach: each `CloneData` apply writes a positive "I cloned this dst
span" marker on its store, and unlock requires gap-free coverage on
every replica. Problem: raft snapshots transmit the dst range's MVCC
content but not the per-store markers, so a replica acquired by raft
catch-up snapshot has the data but no marker → marker check fails on
a replica that's actually fine. Recovery requires conflating "no
marker" with "needs eviction" or carrying markers in snapshots, both
ugly. Inverting the polarity (track what's *missing*, propagate
through snapshots naturally) sidesteps both hazards.

### Snapshot-residue straddlers

`AdminRelocateRange` triggers raft snapshots to colocate destination
replicas. Snapshot recv writes range tombstones over the recipient
range's bounds (via `MultiSSTWriter.initSST` adding a `RangeDel` and
`RangeKeyDel` covering the SST's bounds, for non-MVCC spans). If the
recipient range's bounds were wider in the past (before later splits),
those tombstones live on with their original wide bounds.

When a subsequent `VirtualClone` operates on a source span that
intersects (or is intersected by) one of these wide-bounded tombstones,
Pebble hits unsupported cases:

1. A source SST whose *file bounds* extend past srcSpan into dstSpan.
2. A source SST containing a range-tombstone *fragment* whose interval
   straddles srcSpan or dstSpan boundaries.
3. (Variations involving range-key deletes.)

All three are functionally irrelevant to the clone (the tombstones are
older than any data we care about), but Pebble's bounds-based
preconditions trip on them. The right Pebble-side answer is to use
the existing ingest-split / fragmenter machinery to split tombstones
at the relevant boundaries before excising/cloning — see Appendix A.

The alternative ("don't relocate dst replicas via raft snapshot — born
on the right stores instead") is structurally cleaner but requires
inventing a new mechanism for placing fresh empty replicas on specific
stores at creation time. The Pebble-side fix is local and reuses
existing primitives.

### Smaller TODOs

- **PTS install on source span at T.** Today the orchestration trusts
  the operator to keep duration short enough that source-side GC
  doesn't eat the data we'd revert to.
- **Lease heartbeat / extend.** The orchestration takes a 30-minute
  inconsistency lease and trusts itself to finish before that. A
  heartbeat loop while the orchestration runs makes long forks safe.
- **KV deadman cleanup.** When `InconsistentReplicas` lapses with no
  orchestration alive, KV should `ClearRange` the keyspan and write the
  aborted sentinel. Removes the orchestration-crash-leaves-bit-set
  footgun.

## Out of scope

- **System tenant fork.** The system tenant's keyspace conflates user
  data with cluster infrastructure (meta2, range descriptors, settings,
  etc.); there's no clean "user data" span to clone. Forking the system
  tenant would require a much more invasive design.

---

# Appendix A: Pebble `VirtualClone` (for Pebble-side review)

This appendix describes the Pebble-side API and implementation,
including the open issues surfaced by the CRDB prototype that the
Pebble work needs to resolve. The Pebble session should review this
section against what's actually on the `clone` branch.

## Context

CRDB needs a copy-on-write clone of a key span from one prefix to
another. Motivating case: tenant clone `/Tenant/3/...` →
`/Tenant/4/...`; the API must also handle sub-ranges, e.g.
`[/Tenant/1/foobar, /Tenant/1/foobaz)`.

Pebble's only zero-copy cross-key-space mechanism prior to this work
was `SyntheticPrefix`, which *prepends* a prefix to keys whose
underlying SST was *written* with the prefix already stripped (the
backup-and-ingest workflow). It cannot operate on existing in-LSM
SSTs whose keys physically contain a source prefix that must be
*replaced*.

A previous attempt at general prefix replacement
(`sstable/prefix_replacing_iterator.go`, removed after PR #3344) lived
as a wrapper iterator above the SST iterator. It got tangled with
`singleLevelIterator.SetBounds` via a duck-typed
`SetBoundsWithSyntheticPrefix` interface and was abandoned as too hard
to maintain.

The current approach: a substitution applied to the *block-shared
prefix* stored in colblk data blocks, at key-reconstruction time
inside the block iterator — the same architectural layer where
`SyntheticPrefix` already sits cleanly.

## Public API

```go
// VirtualClone walks every SST in the current LSM that intersects
// srcSpan and creates virtual SSTs (sharing existing physical
// backings) that expose the intersected keys under dstPrefix instead
// of srcPrefix. It atomically excises dstSpan before installing the
// virtual SSTs, so any pre-existing data or tombstones in the
// destination region are removed.
//
// srcSpan must lie entirely within [srcPrefix, srcPrefix.ImmediateSuccessor).
// dstSpan must lie entirely within [dstPrefix, dstPrefix.ImmediateSuccessor)
// and must be the substitution image of srcSpan (the caller constructs it;
// Pebble cannot derive a destination exclusive upper bound from a prefix
// swap alone — see "Why dstSpan is a parameter" below).
//
// Atomicity: a single VersionEdit excises dstSpan, installs all
// virtual SSTs, and any small physical SSTs produced for boundary-
// block rewrites. On conflict with a concurrent compaction/excise on
// a referenced source SST, the operation restarts from scratch.
//
// Returns ErrUnsupportedClone (with details) for v1 unsupported cases:
//   - rowblk-format SSTs intersecting srcSpan
//   - source SSTs whose file bounds extend into dstSpan (case A
//     below; needs ingest-split-style fix to lift)
//   - source SSTs containing range-deletion or range-key fragments
//     whose [Start, End) interval straddles a srcSpan boundary
//     (case B below; needs keyspan-fragmenter-driven fix to lift)
func (d *DB) VirtualClone(
    ctx context.Context,
    srcSpan KeyRange,
    srcPrefix []byte,
    dstSpan KeyRange,
    dstPrefix []byte,
) error

// EstimateCloneCost returns the number of bytes VirtualClone would
// physically write (boundary-block rewrites only; virtually-cloned
// files contribute 0).
func (d *DB) EstimateCloneCost(
    ctx context.Context, srcSpan KeyRange,
) (estBytesWritten int64, err error)
```

### Why `dstSpan` is a parameter

CRDB's standard span shape is `[prefix, prefix.PrefixEnd())`, where
`PrefixEnd()` is by construction *outside* the prefix (it's the
lexicographically next prefix; the exclusive upper bound). Pebble
cannot derive `dstSpan.End` from `(srcSpan, srcPrefix, dstPrefix)`
alone via byte-level substitution because the substitution doesn't
apply to keys that aren't under srcPrefix.

Earlier API iterations tried to derive `dstSpan.End` via
`bytesPrefixEnd(dstPrefix)` — incrementing the last byte of dstPrefix.
For a CRDB tenant prefix like `\xfe\x8c` (tenant 4) this produces
`\xfe\x8d` (tenant 5 prefix). That byte sequence is *valid* as a
tenant prefix but is *invalid* as a Pebble engine key: the
cockroachkvs comparer reads the last byte as an MVCC suffix length,
and `\x8d` decodes as a 13-byte suffix length on a 2-byte key,
yielding `len(a) - aSuffixLen = -11` → slice-bound panic in
`Compare`.

The lesson is broader than tenant prefixes: any "prefix end"
computation requires comparer-semantic awareness Pebble can't get
without the caller's help. Asking the caller to pass `dstSpan`
explicitly is the cleanest fix:

- The caller's keys (`dstSpan.Key`, `dstSpan.End`) are well-formed
  by construction (built from CRDB-side primitives that know the
  encoding).
- Pebble never synthesizes a boundary key; `Compare` only ever sees
  keys the caller already passed in or that exist in source SSTs.
- The substitution is purely byte-level on data keys (which always
  start with srcPrefix); no encoding reasoning needed.
- It also supports cloning a sub-range cleanly (one table within a
  tenant, say) without needing additional parameters.

### Excise-on-clone semantic

The `VirtualClone` contract is that dstSpan is **excised atomically**
with the install. Earlier iterations had a softer "destination span
is expected to be empty; if not, files land at higher levels per
per-file conflict resolution" contract. That's fragile because
"empty" is hard to define when tombstone-only SSTs exist with bounds
covering the dst span (snapshot-recv residue is a common source).

With excise-on-clone:

- The caller commits to "I want the dst span replaced with the
  clone." Same shape as RESTORE.
- Any kind of leftover (tombstones, half-written data, snapshot
  residue) in dst gets wiped before the clone lands.
- Pebble doesn't have to reason about whether the obstacle is
  "really empty"; it just removes everything in the span.
- No more "find a level free of overlap" search; cloned virtual SSTs
  can land at the source level deterministically.

The caller is responsible for ensuring exclusivity over the dst span
during the operation (via `InconsistentReplicas` in our case);
without that exclusivity the excise loses concurrent writes.

## Decomposition (per source SST intersecting srcSpan)

1. **Fully contained.** Create a virtual `TableMetadata` referencing
   the existing `TableBacking` with `BlockPrefixSubstitution{Src:
   srcPrefix, Dst: dstPrefix}` and bounds rewritten into destination
   space. Zero data copy.

2. **Straddling colblk SST (file bounds extend past srcSpan).** Read
   the index block, find the contiguous run of data blocks whose key
   range lies entirely within srcSpan. Create a virtual
   `TableMetadata` whose bounds are *block-aligned* — chosen from
   index separators so iteration naturally skips the boundary blocks.
   For the (typically one) boundary block at each end whose key range
   straddles srcSpan, decode it once and write the in-span keys (with
   prefix substituted) into a small new physical SST.

3. **Straddling rowblk SST (pre-v5 format).** Error in v1
   (`ErrUnsupportedClone`). Rowblk has no per-block stored shared
   prefix.

## Open issues from CRDB prototype

The following cases are surfaced by real CRDB workloads (every fresh
fork in a non-empty cluster will hit at least one of them) and need
to be handled before the API is usable in production.

### Case A: source SST file bounds extend into dstSpan

**Symptom:**

```
source table 000008 at L0 has bounds overlapping the dst excise span
(smallest=/Tenant/3/0,0#43,RANGEKEYDEL
 largest=/Tenant/4"ccc"/0,0#inf,RANGEDEL)
```

**Cause.** A source SST holds tombstones whose bounds straddle the
src/dst tenant boundary. This is snapshot-recv residue: when an
earlier raft snapshot was applied to a recipient range whose bounds
covered both tenants (before later splits), `MultiSSTWriter.initSST`
wrote a range-deletion fragment over the recipient's bounds.

**Why it matters.** Pebble's dst-excise wants to remove everything in
dstSpan. The straddling source SST has bounds that overlap the
excise region; excising it would also remove the src-side portion of
the same physical file.

**Proposed fix.** Use existing `ingestSplit` machinery before the
excise: walk every local SST whose bounds intersect dstSpan but
extend outside it, and ingest-split at the dstSpan boundaries. After
this, every overlapping SST is either entirely inside dstSpan
(excisable) or entirely outside (untouched). Then dst-excise runs
over a clean dstSpan; then source-side cloning proceeds with the
existing boundary-block-rewrite for srcSpan straddlers (which file 8a
above will still trigger, and is already handled).

This reuses existing Pebble primitives: `ingestSplit` already exists
for the symmetric problem ("an existing SST straddles an ingest's
span") and is the same shape as what we need.

### Case B: range-tombstone fragments straddle srcSpan

**Symptom:**

```
source table 000009 at L0 contains a range deletion fragment
[/Table/80/0,0, /Max/0,0) that straddles srcSpan
[/Tenant/3/0,0, /Tenant/4/0,0)
```

**Cause.** Same as case A — snapshot-recv residue — but the straddler
is *inside* a source SST whose file bounds are otherwise clean. The
file's range-tombstone fragment block contains a single fragment
whose `[Start, End)` interval extends past srcSpan.

**Why it matters.** The boundary-block-rewrite path at the *file*
level doesn't fire (the file's bounds are inside srcSpan), so the
file gets virtually referenced. The keyspan iterator over the virtual
file then walks the rangedel block expecting all fragments to share
srcPrefix; the wide fragment doesn't, and the block-level
`sharedPrefixLen` calculation comes out as 0 — confusing because
the data-block straddler-rewrite path *guarantees* `sharedPrefixLen
>= len(srcPrefix)` for data blocks, but no such guarantee exists for
keyspan blocks because that path doesn't have a corresponding
straddler-rewrite step.

**Proposed fix.** Extend straddler-rewrite to cover range-tombstone
and range-key blocks. For each fragment whose interval extends beyond
srcSpan, truncate to the srcSpan intersection (the keyspan
fragmenter — `rangedel.Fragmenter` / `rangekey.Fragmenter` — already
supports truncation), then substitute the prefix on the (now in-srcSpan)
endpoints, then emit into the rewritten boundary block.

For our case the fragment `[/Table/80, /Max)` would be truncated to
`[/Tenant/3, /Tenant/4)` (the srcSpan intersection), then prefix-
substituted to `[/Tenant/4, /Tenant/5)` in the dst. Logically: the
fragment is useless tombstone history; physically: it gets carried
through with correct bounds.

### Case C: range keys (any presence)

`ErrUnsupportedClone` for any source SST that contains range keys
straddling srcSpan boundaries. Same family as case B but for the
range-key block instead of the range-tombstone block.

**Proposed fix.** Same mechanism as case B, applied to range-key
fragments via `rangekey.Fragmenter`.

## Concurrency, atomicity, refcounting

**Concurrency model: ingest-style.** All reads and boundary writes
happen upfront without locks; a single `UpdateVersionLocked` callback
re-snapshots, validates source backings still exist, and either
applies or returns an error that triggers a full restart. Matches
Pebble's `ingest.go` pattern. Single-VE atomicity, naturally
serialized.

**TableBacking refcounting.** Multiple virtual SSTs (one per source
file) share a backing. Refcounting must happen *inside*
`UpdateVersionLocked` to avoid racing with concurrent compaction
`Unref`. Reuse the existing `AttachVirtualBacking` flow used by
excise.

**Crash recovery for pre-VE boundary rewrites.** Boundary-block SSTs
are written to the object provider before the VE applies. On crash
they become orphans. Track them in the obsolete-file set if the VE
never applies (or rely on `pebble.checkConsistency`-style orphan
detection at startup — same flow ingest already uses for its own pre-
VE physical SST writes).

**EFOS interaction.** If an `EventuallyFileOnlySnapshot` covers
srcSpan or dstSpan at clone time, the dst-excise path needs to
register with `ongoingExcises` so a concurrent EFOS that subsequently
acquires `DB.mu` observes a `visibleSeqNum` strictly past the excise
(preserving its pre-excise view). This mirrors the existing excise
pattern.

## Bound-translation site checklist

Every place that does "strip `SyntheticPrefix` from a query key"
needs a parallel "strip `Dst`, prepend `Src`" branch via
`BlockPrefixSubstitution.Invert`. Sites:

| Site | File | v1 status |
|---|---|---|
| Data-block key reconstruct | `sstable/colblk/data_block.go` | Done |
| Data-block `seekGEInternal` | `sstable/colblk/data_block.go` | Done |
| Data-block `IsLowerBound` | `sstable/colblk/data_block.go` | Done |
| Index-block iter | `sstable/colblk/index_block.go` | Required v1 |
| Single-lvl `SeekPrefixGE` / bounds | `sstable/reader_iter_single_lvl.go` | Required v1 |
| Bloom filter probe | `sstable/reader_iter_single_lvl.go` | **Required v1, critical** (silent-wrong-data risk if missed) |
| Block-property filter | `sstable/block_property.go` | Required v1 (or disable on substituted virtual SSTs in v1) |
| Virtual reader bounds | `sstable/virtual/virtual_reader_params.go` (`ConstrainBounds`) | Required v1 |
| Range-key keyspan paths | `sstable/colblk/keyspan.go` | **Required for case B/C above** |
| Two-level iterator | `sstable/reader_iter_two_lvl.go` | Required v1 |
| Rowblk readers | `sstable/rowblk/...` | Out of scope (v1 errors on rowblk straddlers) |

## Probe inversion (silent-wrong-data risk)

Bloom filters and block-property filters are computed at write time
over storage-space (Src) keys. Any probe taking a user-space (Dst)
key must `Invert` the key (Dst→Src) before consulting the filter, or
probes return wrong answers (false negatives, possibly false
positives via aliasing). This is the highest-stakes correctness
concern in the implementation — silent wrong data, not loud failures.

## Format-major-version gate

`FormatPrefixSubstitution` (FMV 31). `VirtualClone` returns
`ErrFormatMajorVersionTooLow` when invoked below the floor. The new
manifest tag `customTagBlockPrefixSubstitution` sits in the non-safe-
ignore range so older Pebble binaries fatal during replay.

## Verification

Per-site unit tests:

1. `BlockPrefixSubstitution.Apply`/`Invert` round-trip + edge cases.
2. `IndexBlockIter` produces dst-space separators when carrying a
   substitution.
3. `singleLevelIterator.SetBounds` / `SeekPrefixGE` translate bounds
   correctly.
4. **Bloom-filter probe:** cloned virtual SST returns correct
   present/absent for keys that exist (must hit), don't exist (must
   miss), and don't start with `Dst` (definite miss).
5. **Block-property filter:** substituted virtual SST disables
   filtering and returns correct results.
6. `virtual.ConstrainBounds` correctly inverts.

End-to-end (datadriven):

7. Build small LSM with known tenant prefixes; `VirtualClone`; scan
   dst prefix; assert correctness at fully-contained, leading-
   straddler, trailing-straddler, and both-end-straddler cases.
8. `EstimateCloneCost` returns expected byte counts.

CRDB-realistic LSM fixture (lessons from prototype):

9. **Value separation enabled, value blocks enabled** (CRDB
    defaults). Pebble's `IterateDataBlock` must handle out-of-line
    value handles produced by separation; older code paths passed
    `nil` as the lazyValuer and crashed.
10. **Blob references present** on at least one in-span SST.
11. **Source SSTs containing MVCC-versioned keys** under a CRDB-style
    tenant prefix (so iterators see real timestamp suffixes during
    validation).
12. **Destination keyspace pre-populated** with overlapping data,
    not empty — exercise the dst-excise path against an occupied dst
    region (this is where snapshot-recv tombstones live in real CRDB
    LSMs).
13. **Prefixes passed in literal byte-prefix form** a real caller
    would build (raw tenant prefix, not engine-key-encoded).

Concurrency / correctness:

14. **Concurrent-compaction race.**
15. **Level-placement seqnum-ordering.**
16. **EFOS interaction** (EFOS over dst region; EFOS over src region).
17. **Crash recovery between boundary-write and VE apply.**
18. **Format-major-version gate.**

Metamorphic:

19. Extend the metamorphic test to randomly insert
    `BlockPrefixSubstitution` transforms during operations and
    validate equivalence against an oracle.

---

# Appendix B: Lessons from the prototype

A few non-obvious things the prototype surfaced that are worth
preserving as cautionary notes:

- **Tenant creation paths matter.** Using the standard
  `crdb_internal.create_tenant` path for the destination tenant
  bootstraps system tables into the dst keyspace, which then collide
  with cloned data. PCR's path (insert with `DataState=ADD`, no
  bootstrap, then activate post-fork) is the right shape and is what
  `CREATE VIRTUAL CLUSTER FROM` uses.
- **`AdminRelocateRange` issues `LeaseInfoRequest` /
  `TransferLeaseRequest`** internally as part of replica movement.
  These are *not* admin-flagged batches; the InconsistentReplicas
  dispatch reject must exempt them or the orchestration deadlocks
  against the bit it set itself.
- **The destination tenant's relocation snapshots are the source of
  every "straddler" Pebble error we hit.** Snapshot recv writes
  range tombstones over the recipient range's *current* bounds; after
  later splits those tombstones live on with their original wider
  bounds, intersecting boundaries the new layout doesn't have. Pebble
  needs to handle this (Appendix A cases A/B/C); CRDB can't avoid it
  without inventing a new "born on the right stores" replica
  placement primitive.
- **Apply-time errors are unconditionally fatal.** `runPostAddTriggers`
  returning a non-nil error → `maybeFatalOnRaftReadyErr` → node
  panic. There is no clean "this command can't apply, tell the
  client" path. The structural fix is bigger than this prototype;
  the prototype workaround is to make `CloneData` apply errors a
  no-op-and-detect-at-unlock.
- **Test-build response checksum verification (in `kvclient/kvcoord/
  transport.go`) trips on any prefix-substituted value.** `roachpb.
  Value` carries a crc over `(key || payload)`. The verifier's
  test-build-only nature means production isn't affected, but we've
  also lost line-level corruption detection on cloned ranges. Needs
  a per-range "values carry checksums against original keys" bit
  before this can ship — see "Value checksum incompatibility" in the
  open issues.
- **`tracing.ChildSpan` wrappers per orchestration phase** make
  multi-node debugging tractable. Without them every fork looks like
  one opaque RPC; with them you see lock / split / relocate / clone /
  unlock / revert / clear-elided as separate spans with timings.
