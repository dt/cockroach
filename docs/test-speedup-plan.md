# Test Speedup Initiative

## Motivation

Slow CI is a tax on every engineer, every day. When test suites take 20+
minutes developer productivity costs of slow iteration cycles and infrastructure
cost are significant.

The goal of this initiative is to systematically reduce CI time by
identifying the slowest tests, fixing the root causes, and putting guardrails
in place to prevent regression.

## Plan of Attack

### Phase 1: Measure and Rank

Build tooling to identify the slowest tests across the codebase and maintain a
leaderboard that tracks progress over time.

**Tooling:**

- Script that runs `./dev test <package> -v` for target packages, parses
  `--- PASS: TestFoo (123.45s)` lines, and produces a sorted timing report.
- Leaderboard file (or dashboard) tracking the top N slowest tests across
  key packages, updated periodically as improvements land.
- Target packages for initial focus (ordered by wall-clock time):
  - `pkg/kv/kvserver` (~17 min)
  - `pkg/backup` (~15 min)
  - `pkg/crosscluster/physical` (~13 min)
  - `pkg/ccl/changefeedccl` (~13 min)

### Phase 2: Investigate and Fix

For each batch of slow tests, use AI coding tools to:

1. Read the test code and trace where time is spent.
2. Classify the root cause into one of the known anti-patterns (see below).
3. Propose concrete fixes with estimated savings.
4. Implement the fix, re-measure, and update the leaderboard.

This is an iterative process: fix a batch, measure the results, pick the next
batch, repeat. Prioritize by total time saved (duration x frequency).

### Phase 3: Backstop Against Regression

Once tests are fast, keep them fast. Two complementary mechanisms:

**Option A: Runtime enforcement via `leaktest`**

Extend `leaktest.AfterTest(t)` to start a timer goroutine that fails the test
if it exceeds a per-test time budget (e.g. 30 seconds). Provide an escape hatch
(`leaktest.AfterSlowTest(t)` or similar) for tests that legitimately need more
time. Apply a multiplier under race/stress conditions.

This catches regressions at the point of failure with a clear error message.

**Option B: CI post-processing**

Parse test output in CI to extract per-test durations. Fail the build (or post
a warning comment on the PR) if any test exceeds a threshold, with an allowlist
for known exceptions. This requires no code changes and can be deployed
incrementally.

**Option C: Lint pass**

Add a custom analyzer to `pkg/testutils/lint/passes/` that requires tests above
a duration threshold to carry an annotation (e.g. `skip.UnderShort`) or use the
slow-test escape hatch. Catches regressions at review time.

These options are not mutually exclusive. A reasonable rollout is: start with
Option B (cheapest to deploy), then add Option A for hard enforcement once the
test suite is in a healthy state.

## Anti-Pattern Catalog

This section catalogs the classes of test behavior that cause slowness and the
recommended fixes for each. This is a living document -- as we investigate more
tests, we will expand these entries with more detail, examples, and guidance.

### Combinatorial Parameter Explosion

**Symptom:** Test runs nested loops over multiple parameter dimensions, creating
hundreds or thousands of test cases.

**Example:** `TestRestoreEntryCover*` tests 7 span counts x 8 file counts x 2
modes x 5 sizes x 5 checkpoint iterations = ~6,720 calls per backup chain
length. Six variants of this test collectively consumed ~2,000 seconds.

**Fix strategies:**
- Reduce parameter dimensions to representative subsets rather than exhaustive
  enumeration.
- Use orthogonal sampling (e.g. pairwise testing) instead of full Cartesian
  products.
- Move exhaustive variants to a less-frequent test suite (nightly, weekly).

### Unnecessary Cluster Per Subtest

**Symptom:** Each subtest in a `t.Run()` loop creates and tears down a fresh
`TestCluster` or `TestServer`, when the subtests could share infrastructure.

**Example:** `TestMergeQueueSeesNonVoters` creates 13 separate 7-node clusters,
one per subtest. `TestChangefeedEnriched` spins up 32 separate servers (8 test
cases x 4 sink types).

**Fix strategies:**
- Create the cluster once and reuse across subtests, resetting state between
  iterations where needed.
- Where subtests genuinely need isolation, use `t.Parallel()` to overlap their
  execution.

### Over-Sized Test Data

**Symptom:** Tests load far more data than needed to exercise the behavior
under test.

**Example:** `TestBackupCompactionLocAware` creates 1,000 accounts when 100
would suffice. `TestBackpressureNotAppliedWhenReducingRangeSize` loads 200 MiB
via 40 individual UPSERT statements.

**Fix strategies:**
- Reduce row counts to the minimum needed for correctness.
- Batch data loading (bulk INSERT instead of individual UPSERTs).
- Use smaller row sizes where the test doesn't depend on data volume.

### Excessive Range/Split Counts

**Symptom:** Tests create hundreds or thousands of ranges via SPLIT AT when a
few dozen would exercise the same code paths.

**Example:** `TestChangefeedTimelyResolvedTimestampUpdatePostRollingRestart`
creates 1,000 ranges. The test only needs enough ranges to verify fan-out
behavior, which 100-200 ranges would cover.

**Fix strategies:**
- Reduce range counts to the minimum needed to trigger the behavior under test.
- Document why a specific range count is necessary when it must be large.

### Sink/Backend Iteration in Changefeed Tests

**Symptom:** Changefeed tests iterate over 4-6 sink types (kafka, webhook,
pubsub, cloudstorage, sinkless, pulsar), spinning up a fresh test server for
each sink, when the test logic is sink-independent.

**Example:** `TestDatabaseLevelChangefeedWithInitialScanOptions` runs 7 test
cases x 6 sinks = 42 server instantiations.

**Fix strategies:**
- Decouple feature testing from sink coverage: test the feature with one sink,
  test sink compatibility separately.
- Use random sink selection for feature tests instead of exhaustive iteration.
- Pre-check sink applicability before incurring setup costs.

### Cluster Over-Provisioning

**Symptom:** Tests create more nodes than the test actually requires.

**Example:** Lease transfer tests create 3-node clusters for operations that
only involve 2 nodes (source and target of the transfer).

**Fix strategies:**
- Right-size clusters to the minimum node count needed.
- Document why a specific node count is required when it must be large.

### Polling with Generous Timeouts

**Symptom:** Tests use `testutils.SucceedsSoon()` (default 45 seconds) or
similar polling loops where a tighter timeout would be appropriate.

**Fix strategies:**
- Use `testutils.SucceedsWithin()` with a duration appropriate to the
  operation, rather than the default.
- Replace polling with event-driven synchronization (channels, condition
  variables) where possible.

### (More patterns to be added)

As we investigate more slow tests, additional anti-patterns will be documented
here with examples, fix strategies, and before/after measurements.

## Leaderboard

_To be populated and maintained as fixes land._

| Date | Package | Test | Before | After | Savings | PR |
|------|---------|------|--------|-------|---------|----|
| | | | | | | |

## Progress Log

_Track batches of work and cumulative savings._

| Batch | Tests Fixed | Total Savings (s) | Cumulative Savings (s) |
|-------|-------------|-------------------|------------------------|
| | | | |
