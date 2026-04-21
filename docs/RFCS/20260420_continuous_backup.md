# Continuous Backup / Rangefeed-to-S3

## Context / Motivation

CockroachDB today supports point-in-time backup/restore via discrete
full and incremental backups, with the "revision history" flavor of
incrementals capturing all MVCC versions in the window via polling
`ExportRequest(MVCCFilter_All)`. Recovery point objective (RPO) is
bounded below by how often you schedule incrementals, and the
operational cost of frequent incrementals is real.

Separately, rangefeed consumers (CDC, LDR, PCR, etc.) protect history
via per-job PTS records so their catch-up scans don't see GC errors.
PTS records can be scoped (per-table or per-database), so the issue
isn't keyspace breadth — it's *duration*. When the consuming job
stalls or falls behind for hours/days, MVCC garbage accumulates
within the protected scope and serving traffic on those same tables
pays the cost. The other awkwardness: catch-up is key-ordered (driven
by `MVCCIncrementalIterator`) rather than time-ordered.

This project introduces a **continuous backup** job that opens
rangefeeds across the cluster and streams events to external storage
(S3-style). The same external log then serves two purposes:

1. **Sub-minute RPO restore.** Restore replays the suffix of the log
   over the most recent discrete backup up to a chosen restore time.
   Inc backups themselves remain — they are load-bearing for RTO as
   the restore base on which the log is replayed — but the operational
   pressure to schedule them more frequently than ~hourly is relieved.
   Deprecates the "revision history" flavor of inc backup.
2. **External catch-up for rangefeed consumers.** A rangefeed client
   wrapper serves the catch-up phase from the log instead of from KV's
   MVCC history, transitioning to a live KV subscription once close to
   the present. Consumers covered by a continuous backup can then opt
   out of PTS, letting GC proceed on its normal schedule and shifting
   catch-up scans into time order (the natural order of the log).

---

## 1. Backup job / log production

### Concerns and constraints

- **Flush durability on the order of 5–10 seconds.** This is the
  RPO floor — events must be in durable external storage and visible
  to readers within that window of the write actually happening.
- **Resolved-timestamp / completeness markers are first-class.**
  Many readers can only operate on a time slice once they know it is
  *complete* — every change that occurred in the slice has been
  flushed. The log emits these as a primitive readers gate on, not
  as a derivation a reader has to compute.
- **Output organized so a reader can pick a subset.** Even if the
  job is logging an entire cluster, a reader rarely wants
  *everything* — it wants some subset of spans (could be all, could
  be one table's spans, could be a single index range) over some
  time slice. The two retrieval dimensions:
  - **Span subset.** Locate events for an arbitrary subset of spans
    (per-table restore "fish out widgets as of 30m ago after a bad
    UPDATE"; per-table changefeed catch-up; whole-cluster restore)
    without reading data for spans outside the subset.
  - **Time slice.** Locate a short window (e.g. 1h–6h) inside a
    long retention period (e.g. up to a year) without scanning the
    year.
- **The span set evolves under us.** A "cluster backup" today is a
  backup of every user table's every *index* span individually;
  nothing tells the backup job when tables or indexes are
  created/dropped. The log job watches the schema descriptor system
  table via its own rangefeed and replans on relevant changes —
  see "Span coverage tracking" below for the full mechanism. The
  earlier attempt to widen this to per-table spans hit a DistSQL
  planning snag (adjacent spans where `span1.EndKey ==
  span2.StartKey` get merged), so we stay at per-index span
  granularity.
- **PTS — v1 self-managed; future cluster-coordinated.**
  - **v1**: the log writer maintains its own PTS, advancing it
    whenever it falls more than ~10 minutes behind its own
    flushed frontier (target lag = 10 minutes). This bounds the
    history GC must hold while giving the log slack to recover
    from transient stalls without losing data — if the log
    writer falls further behind, its PTS slips with the
    frontier rather than racing ahead and orphaning data the
    log was supposed to capture. Inc backups run unchanged in
    v1.
  - **Future**: once the cluster exposes "is a log job
    configured for this scope?" via a system table or
    equivalent registry, the log writer no longer needs its
    own PTS. Other PTS-installing jobs become log-aware:
    - Rangefeed consumers (CDC, LDR, PCR) skip their own PTS
      when the log covers their span — they can serve catch-up
      from the log instead.
    - Inc backup adjusts its PTS advances to **not pass a
      lagging log writer**: it advances to
      `min(its_end_time, log_resolved_time)` so the inc
      backup chain doesn't move forward past data the log
      hasn't yet captured. (The phrasing the RFC originally
      had — but factored out to v1's simpler self-PTS for
      bring-up.)
  - The migration from v1 to the future model is additive:
    the registry can be added later without changing the
    on-disk format or the writer's job structure. v1's
    self-PTS just becomes redundant once the consumers
    coordinate.
- **Tiny files are expensive.** Cloud providers bill against a
  per-object size floor, and small objects also dominate the
  compute / IO / RPC cost of listing, opening, and reading — for
  both producers and downstream readers. Production is
  parallelized for throughput capacity, which means during
  low-traffic periods on a large cluster we risk producing an
  outsized number of tiny files. The job must avoid this; specific
  mitigation (batching across producers, idle-collapse, larger
  segments at the cost of latency, etc.) TBD in the file-
  organization session.
- **Storage is WORM / immutable.** Assume we cannot delete or
  rewrite files (anti-ransomware / compliance protection). The
  layout, segment sizing, and any post-hoc compaction strategy
  must work under this assumption — no after-the-fact merging of
  tiny files, no garbage collection of superseded segments.
- **No new persisted job type.** A flag check at the top of the
  existing BACKUP resumer (`pkg/backup/backup_job.go`) dispatches
  to the log-job branch. Same job table row, same job ID space.

### Design

#### File organization

This subsection describes the artifacts on disk and why they take
the shape they do. The pipeline that produces them — buffering,
sorting, coalescing, who-writes-what — lives in the next
subsection ("DistSQL flow").

The read constraints the layout serves:

- Reads pick an arbitrary subset of spans (one index, one table,
  many tables, the whole cluster) over a time slice (typically
  short — minutes to hours — inside a long retention period).
- Reads must be able to determine whether a chosen time slice is
  *complete* before consuming it.

##### Layout sketch

- **Tick.** A fixed wall-clock slice — working number 10s, may
  go to 5s for tighter RPO. Every change is assigned to exactly
  one tick by its MVCC timestamp.
- **Data files.** Internally **key-sorted**. Each file is
  written by a single PUT (no append). A tick contains zero or
  more data files. Files within a tick have **no global
  ordering across each other** — concurrent producers contribute
  concurrently.
- **Tick close marker.** A small object per tick, written once
  the tick is sealed. It records:
  - That the tick is **complete** (every change for the tick
    has been durably flushed somewhere).
  - The **list of data files** belonging to the tick, in
    **per-producer flush order** so readers can preserve KV
    rangefeed's per-key revision ordering (see "Per-key
    revision ordering" below). The marker does not carry
    per-file key bounds: in steady state every producer's spans
    interleave across the keyspace, so per-file bounds collapse
    to ~(start-of-keyspace, end-of-keyspace) and pay no
    span-pruning rent. A reader for a span-subset query opens
    every file in the tick and seeks within each.
  - **Optionally, a small inline tail of data** for the tick —
    if the residual data was too small to justify a separate
    file, it rides in the marker (analogous to inode inline
    data). A tick whose total residual is small enough has zero
    data files and carries everything in the marker.
- **Naming / prefix.** Tick close markers are named
  `…/YY-MMDD_HH/<tick-end-YYMMDD-HHMMSS>` — i.e. one
  object per closed tick, under an hour-granularity prefix,
  with the marker name itself being the tick's wall-clock end
  time (10s ticks are wall-clock-aligned, so end times are
  deterministic without an HLC tail). The hour-prefix folder
  name uses **distinct separators at each level** (`-` between
  year and day, `_` between day and hour, `/` between hour and
  contents) so a reader picks the LIST delimiter that matches
  the granularity it wants — `delim="-"` for year, `delim="_"`
  for day, `delim="/"` for hour. This gives:
  - **Hour-level discovery via LIST**: listing the parent prefix
    with a `/` delimiter enumerates the hours that contain at
    least one closed tick, in chronological order (lex-sorted
    `YY-MMDD_HH` is chronological).
  - **Within-hour iteration in chronological order**: listing a
    specific hour-prefix returns up to ~360 markers (for 10s
    ticks) named by HLC, again lex-sorted = chronological. A
    reader can find the most recent closed tick by listing the
    most recent hour and taking the lex-max entry.
  - **Bounded long-window LIST**: a 6h window LISTs ~6
    hour-prefixes and iterates within each, never the year of
    retention. Data file names are unconstrained beyond living
    somewhere reachable from the marker; the marker's file list
    is authoritative.

##### Data file format

Data files are **Pebble SSTables** keyed on
`(user_key, ascending mvcc_ts)`. We reuse Pebble for the writer,
reader, and block-level seek, but deviate from CockroachDB's
standard MVCC `EngineKey` encoding in one specific: the timestamp
sorts ascending rather than descending. Cockroach's default
descending encoding is tuned for "find the latest revision at
time T" via a forward seek; the log's read pattern is the
opposite — stream per-key revisions in commit order to a
downstream consumer — and ascending encoding makes a forward
iterator naturally emit them in that order, with no per-key
buffer-and-reverse. The tradeoff is a custom Pebble comparator
for this format and tooling that doesn't speak Cockroach's
standard MVCC encoding out of the box; both are mild.

The SSTable **value** is a small `revlogpb` proto carrying just
the `roachpb.Value` for the new value and an optional
`roachpb.Value` for the prior value (populated when the
producer's rangefeed subscription has `WithDiff` set and the
row existed prior — same semantics as `kvpb.RangeFeedValue`'s
`prev_value`). The user key is already in the SSTable key, so
this proto deliberately omits it. (The closest existing shape
is `streampb.StreamEvent.KV` — key_value + prev_value — but
that re-includes the key, so a fresh thin proto is cheaper
than reusing it.)

Why store `PrevValue` inline rather than derive it from earlier
ticks:

- **Catch-up with `WithDiff` needs it.** Today's catch-up scan
  (`pkg/kv/kvserver/rangefeed/catchup_scan.go`) populates
  `PrevValue` by walking the `MVCCIncrementalIterator` to the
  next-older version of each key. A log-served catch-up has to
  provide the same to be a drop-in replacement.
- **Cross-tick derivation is potentially unbounded.** A key that
  is updated rarely could have its previous version many ticks
  back — hours, days, retention-window-old. Walking tick markers
  + data files backward until the prior version turns up would
  dominate catch-up latency for any workload with a heavy tail.
- **Cost is acceptable.** Inserts carry no `PrevValue` (no
  inflation). For updates and deletes the per-event payload
  roughly doubles — real, but the log is write-bandwidth-bound
  either way.

The log job's producers therefore **subscribe with `WithDiff`
unconditionally**, so the log is always usable for any downstream
`WithDiff` consumer. Whether to expose a "no diff, smaller files"
mode later is a tunable, not a base-case concern.

##### Why files are key-sorted

1. **Reads target arbitrary key subspans.** A read may want one
   index, one table, many tables, or the whole cluster — and
   the choice is the reader's, not the writer's.
2. **The number of distinct logical subspans is unbounded.** A
   cluster can have millions of tables × several indexes each.
   Pre-bucketing files by key (one per index, per table, per
   range) would force producers to emit millions of files per
   tick. Coarser pre-bucketing schemes either lose span
   locality (hashing), can't adapt over time under WORM
   (adaptive bucketing can't rewrite history once written), or
   degrade exactly the per-table reads they were meant to
   optimize (lumping small spans together). **A single file
   must hold events spanning many key subspans.**
3. **In-file span-subset efficiency requires in-file key sort.**
   Given a file holds many subspans, the reader needs to seek
   to a specific key range and skip the rest. That demands the
   file be internally key-sorted — which in turn rules out
   append (a key-sorted file cannot grow with arbitrary new
   keys arriving later). Each file is one PUT.

##### Why time is divided into ticks

Once files contain mixed key ranges, a reader needs *some* way
to find files relevant to a time slice. Ticks provide that:
fixed wall-clock slices, each change assigned to exactly one
tick by MVCC timestamp, every file associated with exactly one
tick. A time-range read lists the relevant tick prefixes
(cheaply, via the hierarchical prefix) and reads markers / files
within them.

Ticks also provide the natural primitive for **completeness**.
Many readers can only operate on a slice once every change for
the slice has been flushed. The tick close marker says exactly
this, and pulls double duty as the per-tick file-pruning index.

##### Tick close markers in more detail

A reader catching up on a window:

1. **Discover ticks** by LISTing each `YY-MMDD_HH/` hour
   prefix that overlaps the window, with `/` as the delimiter
   for the parent step (which gives hours containing at least
   one closed tick) and a flat LIST inside each chosen hour
   (which gives the closed-tick markers, named by tick-end
   time, in chronological order). We can't avoid LIST here —
   under WORM there's no overwriteable manifest of "current
   state of all ticks" — but the hour granularity bounds it: a
   6h window LISTs ~6 hour-prefixes plus their contents, not
   the year.
2. **GET each tick's close marker** for the ticks of interest.
   The marker is authoritative for what's in the tick — readers
   never LIST inside a tick beyond the discovery step above.
3. **GET data files** named in the marker, ordered per-producer
   by the marker's flush-order, and seek within each file to
   the requested span subset. Read inline-tail data, if any,
   directly out of the marker.

Why the marker carries the explicit file list rather than
relying on implicit prefix-listing inside the tick:

- **Straggler safety under WORM.** If a buggy / re-running
  producer ever PUTs a file under the prefix after the marker
  is written, an implicit reader would pick it up and we can't
  delete it. With an explicit list, unsanctioned files are
  inert. Self-healing.
- **GET-of-known-key vs LIST-of-prefix.** Per-tick consumption
  doesn't pay for a second LIST inside the tick.
- **Flush-order has to be recorded somewhere anyway.** Per-key
  revision ordering depends on it; the marker is the natural
  place.
- **Coalescing makes the marker authoritative anyway.** A tick
  can have zero data files (data inline in marker), one file,
  or many — only the marker can authoritatively report which.

##### Per-key revision ordering

KV rangefeed's per-key guarantee is precise but limited: an
*initial* delivery of an older event never follows the initial
delivery of a newer event for the same key. Replays of
already-delivered events can come out of order — consumers must
already handle that — but the first time the consumer sees each
event, the per-key sequence is monotonic in commit time. There
is no cross-key ordering guarantee at all.

A log-served catch-up must preserve the same property. Two
invariants make this work without a true sort-merge across all
files in a tick:

- **Disjoint producer spans at any given flushorder.** At any
  instant the active producers cover disjoint span subsets, so
  files written by *different* producers within a single
  flushorder bucket cannot contain the same key. Readers
  parallelize across producers within a bucket freely. After a
  crash + rebalance the same span may be served by a different
  producer than before, and that new producer's files for an
  open tick may share keys with the prior incarnation's files
  for the same span — but the resume protocol bumps every
  producer's starting flushorder above the prior max (see
  "Progress checkpointing" below), so the prior files are at a
  strictly lower flushorder and read first. Per-key ordering
  holds across the rebalance because flushorder enforces it.
- **Flush-order within a single producer.** A single producer
  may write multiple files for the same tick (when its buffer
  fills before tick close). For a key that appears in more
  than one of that producer's files, revisions are split
  across files — but the producer never reorders its rangefeed
  inputs, so reading those files in **flush order** preserves
  whatever ordering the rangefeed delivered (i.e. monotonic in
  commit time for initial deliveries; any replays appear after
  the events they replay, which is fine). The marker records
  this flush-order, and readers consume each producer's files
  in that order.

Together these mean a log reader can offer downstream consumers
the same per-key initial-delivery monotonicity that KV rangefeed
does, by (a) interleaving across producers freely (disjoint
keysets), and (b) processing each producer's contributions in
the marker's flush-order. No external sort over all-of-tick is
required.

##### WORM-consistent

No file is modified after PUT. No tick is reopened after its
close marker is written. Tick discovery uses LIST (which
doesn't require mutation); per-tick reads use GET on the
known-keyed marker.

##### Still open

- **Coverage manifest** (which spans were in scope at time T,
  reflecting span-set evolution per §1's chosen policy). May
  live in tick markers, in periodic separate manifests, or
  inline in data files — TBD.
- **Where descriptor revisions live** (per §1 "Reflecting
  schema changes" / §2 "Schema").
- **Tick-size choice** (5s vs 10s) and its interaction with
  RPO, slowest-producer-couples-close effects, and small-file
  production rate.

#### DistSQL flow / production pipeline

The artifacts above describe what is written. This subsection
covers the pipeline that produces them — who buffers, who
sorts, who flushes, and how an idle cluster avoids producing
swarms of tiny files.

A large cluster (hundreds of nodes) cannot assume any single
node has the capacity to log all of its changes; production is
distributed across many producers. Region-pinning of logs (for
data sovereignty) further argues for keeping production work
near the data being logged.

Roles in the flow:

- **Producers.** Each producer subscribes to an assigned span
  subset, buffers incoming events, and key-sorts buffer
  contents. At each tick boundary it decides per tick: if its
  buffered contribution for that tick is large enough to
  justify a standalone file, it PUTs a key-sorted data file
  directly into the tick. If it's too small, it forwards the
  small sorted buffer to the coordinator instead.
- **Coordinator (per region in the future; flat for now).**
  Receives small forwarded buffers from low-volume producers,
  merges them into a single coalesced key-sorted file per
  tick, and PUTs that file. If the coalesced result is itself
  small enough, the coordinator hands it to the tick-closer
  for inclusion as the marker's inline tail rather than
  emitting a separate data file.
- **Tick closer / metadata writer.** Waits for both: (1) every
  producer's frontier has advanced past the tick end, AND (2)
  every contributed data file (producer-direct or
  coordinator-coalesced) is durably PUT. Then writes the close
  marker — the tick's data file list in per-producer flush
  order, plus any inline tail.

This pipeline lets an idle cluster collapse to a single
artifact per tick (the close marker, with inline tail), while
giving each node independence to flush its own files when it
has enough volume to justify them. Producers with steady
traffic write directly; quiet producers piggy-back on a shared
coalesced file or marker tail.

Two coupling notes worth flagging here, full design deferred to
the piece-(a) session:

- **Region-pinned coalescing.** A single global coordinator
  collecting inline payloads would shuttle EU data through US
  (or vice versa). Coalescing must therefore happen at region
  granularity — per-region coordinator (or sub-coordinator),
  not flat. The single-coordinator topology is fine for the
  initial implementation but the design must not preclude
  per-region.
- **Producer durability handshake.** "I forwarded my buffer to
  the coordinator" is *not* "the data is durable." A producer
  may only release a tick's buffer once it observes the tick's
  durable close marker exists (or some equivalent ack
  guaranteeing the coordinator's PUT has happened). If the
  coordinator dies mid-coalesce, producers must be able to
  re-supply.

TODO — exact "small enough to forward" and "small enough to
inline" thresholds; producer ↔ coordinator wire format;
coordinator topology (single, per-region, hierarchical);
resilience model (producer/coordinator failure recovery,
exactly-once-via-idempotent-PUT story); admission control.

#### Implementation sketch / package layout

A first cut at where the code lives. Three packages:

- **`revlog`** — library for interacting with the revision log
  in external storage. Used by the backup-side writer (§1),
  the restore-side reader (§2), and the rangefeed-client
  wrapper (§3); it is the shared "S3 API" surface for the log.
  Reader and writer surfaces (exact API TBD):
  - **Reader side:**
    - List closed-tick markers in a time window (the LIST walk
      over `YY-MMDD_HH/<tick-end-YYMMDD-HHMMSS>`).
    - GET and parse a close marker.
    - Open a data file as an iterator of decoded events.
  - **Writer side** — a thin wrapper around `sstable.Writer`
    that handles the log's key/value encoding and minimal
    bookkeeping. All buffer / sort / multi-tick / when-to-flush
    logic belongs to the processor; the writer just encodes
    one file. Sketch:
    ```go
    package revlog

    type Writer struct{ /* sstable.Writer + count */ }

    func NewWriter(w io.Writer) *Writer
    func (w *Writer) Add(key roachpb.Key, ts hlc.Timestamp,
                         value, prevValue *roachpb.Value) error
    func (w *Writer) Count() int
    func (w *Writer) Close() error  // finalizes sstable
    ```
    Tests can use `Writer` directly with an in-memory
    `io.Writer`, no DistSQL needed.
  - **Marker writer** — a function the coordinator calls with
    the tick id, file list (in per-producer flush order), and
    optional inline tail. Writes the close marker.
- **`revlog/revlogpb`** — proto definitions. The tick close
  marker (file list, per-producer flush order, optional inline
  tail) and the thin value-frame proto for the SSTable value
  (see §1 "Data file format").
- **`revlog/revlogjob`** — the DistSQL flow. Built on `revlog`.
  Not a standalone job — entry point is invoked from the
  top-level fork in the existing BACKUP resumer.

##### v1 flow (barebones, no coalescing)

Goal: get the file format / discovery / read path exercised
end-to-end with the simplest possible producer/coordinator
behavior.

- **Coordinator setup.** Calls partition-spans against the
  job's span set to assign ranges to producer processors. Each
  processor learns which spans it watches.
- **Producer processor.** Opens a `kvclient/rangefeed`
  subscription over its assigned spans (with `WithDiff`).
  Buffers KV events and checkpoint events arriving from the
  rangefeed. When either the buffer fills (working number 32MB)
  *or* the rangefeed's frontier crosses a tick boundary, the
  processor:
  1. Sorts the buffer by `(key, ts)`.
  2. Iterates the sorted buffer, grouping by tick (one buffer
     can span multiple ticks during quiet periods). For each
     group, opens a new SSTable file via `revlog.Writer`, adds
     the events, closes / syncs.
  3. Sends a message to the coordinator:
     `{ flushed: []{tick_id, filename}, checkpoints: []{span, ts} }`.
- **Coordinator.** Per processor it maintains a flushed
  frontier from the checkpoint events. Tick T becomes closable
  when `min(flushed_frontier) > T.end` across all processors and
  every contributing file is on the file list. Then the
  coordinator writes the close marker, listing files in
  per-producer flush order. Implicit FIFO of the DistSQL
  channel gives flush order; a per-processor monotonic
  sequence number is cheap insurance against reordering.
- **PTS maintenance** (per §1 Concerns "PTS — v1
  self-managed"): the coordinator owns a single PTS record
  for the log job. Whenever the global flushed frontier
  advances, if the PTS is more than ~10 minutes behind, the
  coordinator advances the PTS to `frontier - 10min`. If the
  log falls behind, the PTS slips with the frontier rather
  than racing ahead, so GC never removes data the log was
  supposed to capture but hasn't yet.

The processor → coordinator message deliberately carries no
per-file key bounds — in steady state each processor's spans
interleave across the keyspace (well-spread replicas), so
bounds collapse to ~(start-of-keyspace, end-of-keyspace) and
buy nothing for span-pruning.

##### Progress checkpointing

The coordinator holds two pieces of in-memory state:

- **Flushed frontier.** The min-across-producers timestamp up to
  which the coordinator has durably recorded data. Advances as
  producer checkpoint events arrive.
- **Open-tick file lists**, one per not-yet-closed tick. Each
  entry is `{filename, flushorder}`. Filename is whatever random
  identifier the producer chose (no need for predictability).
  Flushorder is normally a **per-producer-per-tick** monotonic
  counter — each producer increments its own counter as it
  flushes, so two producers can both have files at flushorder=0
  in the same tick. That's fine: they have disjoint spans by
  construction, so the files are spatially separable and
  interchangeable within the bucket. Readers consume all
  flushorder=0 files (in any interleaved order), then all
  flushorder=1 files, etc.

When the flushed frontier crosses a tick's end the coordinator
**closes the tick**: writes the close marker (file list in
flushorder) and drops the tick from in-memory state.

Every ~1 minute the coordinator **persists** both items to the
job's progress payload:

- The flushed frontier — so producers can resume their
  rangefeed subscriptions from the right point on restart.
- The open-tick file lists — so the eventual close marker
  includes files written before the crash, even if they were
  written by producers no longer on the new plan.

Closed ticks need nothing in the checkpoint — their markers are
already durable on S3 and discoverable via the LIST walk.

**On resume:**

1. Read the job's checkpoint.
2. Re-partition spans (the new plan may differ if nodes have
   changed).
3. Hand each producer:
   - Its assigned spans.
   - The flushed frontier (rangefeed subscription start time).
   - For each open tick, a **starting flushorder** = max
     persisted flushorder for that tick + 1 (across all
     producers). Every resumed producer starts its own
     per-tick counter at this value instead of 0, so any new
     flush is at a flushorder strictly above any prior
     incarnation's flushes — even if the post-resume span
     partitioning gives a span to a different producer than
     before.

Events flushed between the persisted frontier and the crash get
re-derived from the resumed rangefeed; the new files have new
random names and are recorded fresh. The narrow race where the
coordinator wrote a close marker but died before persisting
that fact is handled by the resume-time LIST walk — markers
that already exist on S3 win, and the coordinator skips
re-opening those ticks.

##### v2 add-on: coalescing for tiny files

Layered on v1: producers with too little data to justify a
file forward their small sorted buffer to the coordinator (or
a regional sub-coordinator) instead of flushing their own.
The coordinator merges forwarded buffers across producers into
one coalesced file per tick. If the coalesced result is
itself small enough, it inlines into the close marker as the
inline tail — collapsing an idle cluster to one artifact per
tick.

Region-pinning and producer-durability handshake notes (above,
in "DistSQL flow / production pipeline") apply to this layer.

##### Job creation

User-facing syntax: `BACKUP ... WITH REVISION STREAM` (the
SQL keyword is `STREAM` rather than `LOG` to avoid introducing
a new unreserved keyword; the internal subsystem is named
`revlog` regardless). Captured on the BACKUP job's details
proto as a `CreateRevlogJob bool`. The BACKUP runs as it
normally would (taking the inc backup the user asked for). At
some point during execution it also checks whether a log job
needs to exist for this destination:

1. **Look for an existing log job marker** at the
   destination via `LIST(prefix="log/job.latest-", limit=1)`
   on the destination's external storage. The presence of
   any such file means a log job has already been
   established here.
2. **If present, no-op.** BACKUP skips log-job creation and
   continues with its own work. This makes
   `WITH REVISION STREAM` idempotent across repeated /
   scheduled BACKUPs.
3. **If absent, create the log job.** Build a sibling job
   that's a copy of this BACKUP's details, with two
   modifications:
   - Flip the flags: `CreateRevlogJob=false`,
     `RevLogJob=true`. The resumer fork (next subsection)
     keys off `RevLogJob`.
   - **Clear PTS state from the copy.** The parent BACKUP
     may have its own `ProtectedTimestampRecord` set; if
     the sibling inherits it, the sibling's `OnFailOrCancel`
     would `Release` the parent BACKUP's PTS by ID — orphaning
     the parent. The sibling installs its own PTS during its
     own resumer execution (per the v1 self-managed PTS
     design above), so leaving the field zero on the copy is
     correct.
   After allocating the new job's ID, write the ID into
   `log/job.latest-1` on the destination so subsequent
   BACKUPs will find it.

**Multiple destinations**: log jobs are independent per
destination. Two BACKUPs to two different destinations, both
with `WITH REVISION STREAM`, produce two independent log jobs.
The marker dance enforces "at most one per destination" but
imposes no global cluster-wide limit.

**Where `log/` lives**: at the **backup collection root** —
the same URI the user supplied to `BACKUP ... INTO`, *not*
nested under any individual full-backup directory inside the
collection. A single, well-known location across the
collection's lifetime so rangefeed consumers (CDC, etc.) can
find the log without knowing about any particular full
backup, and so log playback that crosses full-backup
boundaries (e.g. CDC catching up across a fortnight that
spans two full backups) doesn't have to hop directories.

**No "log folder" to speak of**: object storage has no
folders, only files. The job-marker file *is* the
"log exists" indicator. Stranded artifacts from a previously
canceled job (data files, old tick close markers, old
coverage manifests) are inert under v1 — they exist on disk
but are never referenced from a new job's manifests, and
random file IDs / non-overlapping tick boundaries (the new
job's frontier starts at the parent BACKUP's end time, after
any prior run's frontier) mean they don't collide with new
writes.

The constant `1` suffix on the marker is intentional v1
simplicity: **at most one log job per destination ever** in
v1. **TODO** (post-v1): replace the constant suffix with a
descending `now()` encoding so multiple generations of log
jobs over the destination's lifetime are findable in
chronological-newest-first order; on existence-check, read
the latest file, parse the embedded job ID, and check its
status (running / paused / canceled / failed) — like a
pidfile. A dead prior generation allows takeover by a fresh
log job; a live one continues to no-op. Without this, in v1
a canceled log job permanently blocks the destination from
hosting a new one (since the marker file can't be deleted
under WORM); the workaround is manual cleanup / a fresh
destination URI.

A small race exists if two BACKUPs concurrently see no marker
and both create log jobs: both PUTs to `log/job.latest-1`
succeed (object stores allow PUT-overwrite by default; WORM
buckets configured with object-lock would fail one). Accepted
in v1; pre-v2 if it matters, use a CockroachDB-side lock
(system-table row) to serialize the create-or-noop check.

##### Resumer fork

The existing BACKUP resumer in `pkg/backup/backup_job.go` gains
a small fork at the top: if the job's payload says it's a log
job (the `RevLogJob` details flag set in the previous
subsection), hand off to `revlogjob`; otherwise the existing
incremental/full path runs unchanged. No new persisted job
type, no new system-table changes.

##### Lifecycle: pause / resume / replan / crash recovery

All non-running states (pause, resume, replan after a node
blip, full crash recovery) share a single mechanism: **resume
from the persisted frontier**. The Coordinator's
`Progress checkpointing` subsection covers what's persisted;
this one covers how the resumer uses it.

- **First run.** No persisted frontier yet. The Coordinator
  initializes the frontier to **the parent BACKUP's end
  time** — i.e. the timestamp the inc/full BACKUP that
  triggered `WITH REVISION STREAM` finished at. Restore from
  any time T ≥ parent.end_time is then served by
  `parent backup + log replay over (parent.end_time, T]`.
- **Subsequent runs.** Frontier comes from the most recent
  persisted checkpoint, exactly as documented in
  `Progress checkpointing`. Producers re-subscribe their
  rangefeeds from this frontier; events with ts ≤ frontier
  are not re-delivered, events with ts > frontier are
  (re-)captured. **No hole** — every MVCC timestamp in
  `(parent.end_time, current_frontier]` is durably in the
  log.
- **Coordinator retry policy.** Modeled on PCR / other
  long-running jobs: the Coordinator does *not* fail the
  job on transient errors. Instead it retries while making
  progress (frontier is advancing), and pauses the job if
  it's stuck (no progress for some threshold). The job sits
  in paused state until a user / cluster operator
  intervenes; resume proceeds via the standard resume-from-
  frontier path above.
- **Pause is a normal pause.** No special semantics — the
  job suspends, no rangefeeds are open, no PTS advances,
  the persisted frontier stays where it was. Resume picks
  up at that frontier. The window between pause and resume
  is *not* a hole in the log because the catch-up scan on
  resume will deliver every event with ts > frontier from
  KV, including all events committed during the paused
  period (assuming KV still has them — which the
  self-managed PTS at `frontier - 10min` is supposed to
  ensure, but a long pause beyond that 10-minute slack
  risks GC removing data the resume would need).
- **Resume avoids conflicting writes by construction, not by
  PUT semantics.** Conditional PUT (`If-None-Match: *`) is
  not portable across the cloud providers we target, so we
  don't rely on it. Instead: tick close markers are only
  written once the aggregate frontier crosses the tick end,
  the persisted checkpoint records which ticks have already
  been closed, and a resumed coordinator starts from the
  persisted frontier — so it never tries to re-close a tick
  whose marker is already on disk. Data files use random IDs
  so independent collisions are practically impossible. A
  duplicate write that does happen anyway (true coordinator
  race / bug) silently overwrites with byte-identical content
  in the common case; in the pathological case (different
  content) it's a bug we'd catch via downstream failures, not
  via the storage layer.

##### Telemetry / metrics

Deferred. Likely surface (post-v1): `flushed_frontier_lag`,
`descriptor_frontier_lag`, `pts_lag`, per-tick file count,
per-tick byte size. Not blocking for v1.

##### Package path

TBD: `pkg/backup/revlog/` keeps it adjacent to the existing
backup code; `pkg/revlog/` reflects that the package is
consumed by non-backup callers too (§2 restore, §3
rangefeed-client wrapper). Lean toward the latter so the
dependency direction stays clean.

#### Span coverage tracking

The log's covered spans evolve over time as schema changes
add and drop tables or indexes. Three pieces make this work:
an artifact (the coverage manifest), a control-plane signal
(the schema descriptor rangefeed), and a mid-tick transition
protocol that reuses the crash-recovery flushorder bump.

##### Coverage manifest (artifact)

A separate `log/coverage/<HLC>` file is written whenever the
covered spans change — initially at job-launch with the
starting span set, then again at each relevant descriptor
change. Each file is a small proto carrying the scope spec
(e.g. "cluster", "database:foo") and the expanded span set
effective from that HLC forward. Readers answer "which spans
were covered at time T?" by reading the latest entry whose
HLC ≤ T. Lookups distinguish "absent because no event
happened" from "absent because we weren't watching." See
`pkg/revlog/revlog-format.md` §5 for the on-disk format.

##### Schema descriptor rangefeed (control plane)

The log job's coordinator subscribes its own rangefeed to the
schema descriptor system table (or the relevant subset for
the log's scope). This rangefeed serves two purposes:

1. **Tick close gating.** A tick can only be sealed once the
   descriptor rangefeed's frontier has advanced past the
   tick's end. Even if every data producer's frontier is past
   the tick end, the coordinator must have evidence that no
   schema change happened during the tick before writing the
   close marker — otherwise a late-delivered descriptor
   change inside the tick would mean the recorded coverage
   was wrong for some events in the tick.
2. **Span-change detection.** When the descriptor rangefeed
   delivers any descriptor change at `T_change`, the
   coordinator **re-resolves the job's target spec** (cluster
   / database / table) into a fresh span set and **compares
   to the currently-covered span set**. If they differ,
   spans changed at `T_change` and the mid-tick transition
   protocol below kicks in. If they match (a descriptor
   mutation that doesn't affect the resolved spans — e.g. a
   column rename, an unrelated table elsewhere in the
   cluster), no replan is needed; the coordinator just
   records that the descriptor frontier has advanced past
   `T_change` and continues. Re-resolve-and-compare is
   simpler and more robust than maintaining a per-scope
   filter that tries to guess which descriptor mutations
   matter.

##### Mid-tick span change protocol

When a span change at `T_change` falls inside an open tick,
the coordinator orchestrates a transition that keeps the
wall-clock-aligned tick model intact:

1. Wait for the data producers' frontier to pass `T_change` —
   so all events with MVCC ts < `T_change` for the *old* span
   set are durably flushed and reported.
2. Cancel the existing flow.
3. Write `log/coverage/<T_change>` with the new span set.
4. Launch a new flow on the new spans, starting at
   `T_change`. Per-tick flushorder for any in-flight tick is
   bumped to `max(prior flushorder for the tick) + 1` —
   exactly the same machinery as crash-recovery resume (see
   "Progress checkpointing"). The new flow's files for the
   in-flight tick land at strictly higher flushorder than
   the old flow's contributions.
5. The tick eventually closes normally at its wall-clock
   boundary. Its marker contains both flows' files in the
   right order; per-key revision ordering is preserved by
   the standard read rule (lower flushorder first).

The reader has no special case: it sees a normal tick whose
marker happens to list files from two flows. Coverage is
looked up via the coverage manifest, not derived from tick
contents.

The alternative — closing a "stub" tick at `T_change` and
starting a new tick mid-second — would break the
wall-clock-aligned tick invariant, complicate marker
discovery with variable-length ticks, and duplicate
machinery already provided by the resume protocol. Rejected.

##### Edge case: chronically slow descriptor rangefeed

If the descriptor rangefeed lags behind the data rangefeeds,
ticks cannot close until it catches up — even when data is
fully flushed. This throttles RPO without bounding it. A
metric on `descriptor_frontier_lag` and an alert if it
exceeds a threshold is enough; remediation (cancel + restart
the descriptor sub, fall back to alternative detection) is
out of scope here.

#### Schema descriptor capture

Span coverage tracking (above) tells a reader **which key
spans were covered at time T**. That's enough for rangefeed
consumers — they only need to know which spans of KV pairs to
expect from the log. RESTORE needs more: to interpret the
encoded KV bytes back into rows, it needs the **table
descriptors** that were in effect when those bytes were
written.

This is a separate artifact stream from coverage, written by
the same control plane (the schema descriptor rangefeed) but
serving a different reader.

##### Schema manifest (artifact)

A separate `log/schema/<HLC>` file is written every time the
descriptor rangefeed delivers a relevant change. Each file
contains a **snapshot of the full descriptor set in scope at
that HLC** — every table descriptor, type descriptor, etc.
that the log job is covering.

For v1 the snapshot is the full set on every entry (simple,
typically a few KB to low MB depending on cluster size,
written infrequently — only on DDL events). A delta encoding
(only changed descriptors, periodic full snapshots) is a
post-v1 optimization if size becomes a problem.

Restore at time T reads the `log/schema/` entries up to T
into memory once, then for each KV event in the replay window
consults the descriptor in effect at the event's MVCC ts to
decode it. The schema entries are HLC-keyed and chronological
(lex sort), so this is a one-pass build of an HLC →
descriptor-set map.

See `pkg/revlog/revlog-format.md` §6 for the on-disk format.

##### Relationship to span coverage

The two artifacts are written by the same coordinator
response to a descriptor change, and they share a triggering
event, but they are independent:

- **Spans always change as a consequence of descriptor
  changes** (spans are derived from descriptors).
- **Descriptors can change without spans changing** (e.g.
  column rename, default value change, type change on an
  existing column). In that case only `log/schema/<HLC>` is
  written; `log/coverage/<HLC>` is not (its content would
  be identical to the prior entry).

Keeping them in separate folders preserves the
single-responsibility shape: rangefeed consumers walk
`log/coverage/`; restore walks both. Folding schema into
the Coverage proto would force rangefeed consumers to read
descriptors they don't need on every coverage lookup.

##### Joint concern with §2 "Schema"

The §2 Restore subsection's "Schema" subsubsection consumes
the schema artifact described here. Must stay coordinated:
the encoding chosen here determines the read API there.

#### (rest TODO — per-component session)

Processor count and span partitioning policy; processor restart
behavior vs. already-flushed segments; the durable handshake
between "S3 write succeeded" and "advance job's resolved time";
admission control / pacing. (The log-job-resolved-time signal
to inc backup is now post-v1 — see "PTS — v1 self-managed;
future cluster-coordinated" in §1 Concerns.)

---

## 2. Restore job and UX

### Concerns and constraints

- **Restorability check.** The chosen AOST must be ≤ the log's
  resolved time and the prior backup chain + log together must form
  a contiguous cover over the requested spans. Restore planning
  fails clearly otherwise.
- **Locate the base.** Find the most recent prior incremental (or
  full) backup whose end time is ≤ the requested AOST. Restore the
  base via the existing path (online restore is the expected
  default), then replay the log over `[base.end_time, AOST]`.
- **Time-ordered → key-ordered ingest.** The log is time-ordered;
  ingest needs key-ordered SSTs. Volume to rotate is **assumed >
  memory** — design must spill / stream, not buffer in RAM.
  DistMerge-style precedents: IMPORT (`pkg/sql/importer/`), index
  backfill (`pkg/sql/backfill/`).
- **Per-span lifecycle in the window.** A table that existed for
  only part of `[base.end, AOST]` must restore correctly. The log's
  coverage manifest entered-at / left-at metadata flows into restore
  planning so this is handled, not ignored.
- **`SHOW BACKUP` extended.** Surfaces the log's resolved time and
  the restorability window the log adds on top of the discrete
  backup chain. Operators need to be able to inspect "what is
  restorable right now."
- **Idempotency mid-replay.** A restore that fails part-way through
  log replay must be resumable / retryable without corrupting the
  partially-restored state.

### Design

#### Schema

Replay reads descriptor revisions from the log and applies them at
the right times across the replay window so that data ingested
between two DDL events is interpreted under the correct descriptor.
Must coordinate with §1 "Reflecting schema changes" — the encoding
chosen there determines the read API here.

TODO — separate session.

#### (rest TODO — per-component session)

Which DistMerge primitive(s) to reuse vs. fork; AddSSTable batching
and pacing; integration with `RESTORE ... AS OF SYSTEM TIME`
syntax; per-table point-in-time restore UX; resumability state and
on-disk staging area.

---

## 3. Rangefeed client catch-up extension

### Concerns and constraints

- **Status quo cost.** Today's rangefeed consumers (CDC, LDR, PCR)
  install per-job PTS records to protect their catch-up window.
  When a job stalls or falls behind, MVCC garbage accumulates in
  the protected scope and serving traffic on those same tables
  pays the cost.
- **Order awkwardness.** Catch-up today is key-ordered (driven by
  `MVCCIncrementalIterator` in
  `pkg/kv/kvserver/rangefeed/catchup_scan.go`). The log enables
  time-ordered catch-up, which is more natural for replay-style
  consumers and lets GC proceed sooner.
- **Drop-in API surface.** The wrapper must look like
  `pkg/kv/kvclient/rangefeed` to consumers — same event callback,
  same frontier, same resolved-timestamp semantics. Consumers
  shouldn't need consumer-specific changes beyond opt-in.
- **Two-phase with a clean seam.** Phase 1 reads from the log;
  phase 2 hands off to a live KV subscription. The seam must
  produce neither a gap nor a duplicate at the boundary.
- **Coverage gaps.** The wrapper must handle the case where the
  log doesn't cover the requested span or doesn't go back far
  enough to the requested start time. Either fail clearly or fall
  back to the existing PTS-protected path — choice deferred.
- **PTS opt-out.** Initially an opt-in flag (per-job option or
  cluster setting) on the consumer job. Only safe when the log
  demonstrably covers the consumer's span back far enough; opt-out
  is the consumer's assertion that the log will be there for it.
  Eventually this could be automatic; out of scope here.

### Design

#### (TODO — per-component session)

Phase-1 (log replay) ↔ phase-2 (live KV) wiring and exact handoff
criterion (lag threshold? caught-up signal from the log?); overlap
policy at the seam; coverage-gap handling and fallback policy;
schema / system-table event flow through the wrapper; knob shape
for the PTS opt-out (per-job CREATE-time option vs. cluster
setting vs. both); wiring at the `Manager.Protect` call sites in
CDC (`pkg/ccl/changefeedccl/`), LDR (`pkg/crosscluster/logical/`),
and PCR (`pkg/crosscluster/physical/`).
