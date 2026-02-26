- Feature Name: Colocated Design Documentation
- Status: draft
- Start Date: 2026-02-26
- Authors: David Taylor
- RFC PR: (pending)

# RFC: Colocating Design Documents with Code

## Summary

Move design documents from google docs and/or `docs/RFCS/` into the packages
they describe, using the naming convention `packagename.md` for the primary 
design doc and `packagename-topic.md` for detailed sub-documents. This keeps 
design context next to the code it describes, making it more likely to be read, 
referenced, and maintained.

## Motivation

Design documentation for CockroachDB subsystems has drifted increasingly far
from the code it describes — first into `docs/RFCS/`, then into Google Docs and
Confluence. Documents outside the repo can't be found by browsing code, can't be
updated in the same PR as the code they describe, and are invisible to AI coding
agents. The result is that design context — the "why" behind the code, the
tradeoffs considered, the invariants that must hold — is effectively
inaccessible during day-to-day development.

The goal is to establish a convention where technical design documentation lives
in the repository next to the code it describes, as **living reference
documentation** that stays accurate as the code evolves.

## Detailed Design

### Lifecycle of a Design Document

Design discussion can start anywhere — a Google Doc, a PR creating a file in
`docs/RFCS/`, a Confluence page, a conversation. The format and venue for
*drafting and reviewing* a design are not prescribed here.

However, when implementation begins, the design document should move into the
package it describes, as `packagename.md` or `packagename-topic.md`. This is
the point at which the document transitions from a proposal being discussed into
reference documentation for the code being written. The implementer should be
able to point at the document next to their code and say "this is what I'm
building and why."

For new packages, the design becomes `packagename.md`. For new features added
to an existing package that warrant discussion of their design, that design can
either be documented as an addition/update to the existing `packagename.md` or,
for major/significant areas of functionality, a `packagename-feature.md`
sub-document. For example, `kvserver-queues.md` might make sense as a
sub-document describing specifically the various background queues in kvserver.

This means the in-package document is present *during* implementation, not just
after — reviewers of the code can read the design doc in the same tree.

### Naming Convention

Each package that has design documentation gets:

- **`packagename.md`**: The primary design document for the package. This is the
  top-level architectural overview—what the package does, how it works, key data
  structures and flows. Equivalent to the original RFC that introduced the
  feature.

- **`packagename-topic.md`**: Detailed documents on specific aspects of the
  package. These cover sub-features, specific mechanisms, or design decisions
  that warrant their own document.

For example, in `pkg/backup/`:

```
pkg/backup/
├── backup.md                    # core design, reason for logical reads/writes, data format, etc.
├── backup-online-restore.md     # "online restore"; pebble virtual files, prefix/ts synthesis, downloading, etc.
├── backup_job.go
└── ...
```

This convention has several properties:

- **Flat and greppable**: Docs live in a package root, as close to code as possible.

- **Self-identifying**: `backup.md` is unambiguous regardless of where you
  encounter it (search results, file listings, grep output). Unlike
  `architecture.md` or `design.md`, you don't need to look at the parent
  directory to know what it describes.

- **Extensible**: Adding a new sub-document is just adding
  `packagename-newtopic.md`. No directory restructuring needed.

- **Consistent**: The same convention works across the codebase:
  `pkg/jobs/jobs.md`, `pkg/cloud/cloud.md`,
  `pkg/sql/importer/importer.md`, etc.

### Initial Migration

As a first step, on a package by package bases, move the related RFCs, as-is, to their package.
For example, for `BACKUP` three RFCs move into `pkg/backup/`:

| Source | Destination |
|--------|-------------|
| `docs/RFCS/20160720_backup_restore.md` | `pkg/backup/backup.md` |
| `docs/RFCS/20191202_full_cluster_backup_restore.md` | `pkg/backup/backup-full-cluster.md` |
| `docs/RFCS/20260129_restore_unification.md` | `pkg/backup/backup-online-restore.md` |

These moves are done via `git mv` with no content changes. 
Content modernization (rewriting to reflect current state, removing obsolete sections), 
merging separate expansion/revision RFCs into sections/modifications of other documents
can happen in subsequent changes as people work in the package. 

### Special Cases

Some documents might not have clear homes:

- **Cross-cutting infrastructure**: `20210825_mvcc_bulk_ops.md` affects backup,
  import, schema changes, and replication. It could live in pkg/kv/kv-mvcc-history.md.

- **Fully obsolete**: `20160418_dump.md` describes the removed `cockroach dump`
  command. It has no living code to be colocated with and can simply be removed,
  or could be moved to `pkg/backup/backup-deprecated-dump.md`.

- **Process/policy docs**: Documents about development process, not code design,
  can live directly under docs/.

### Incremental Adoption

This convention is adopted incrementally, package by package, to allow maintainers
of each package a chance to review how their docs get restructured.

### Content Evolution

The initial move is a pure `git mv` — no content changes. After a document
lands in its package, it transitions from a point-in-time proposal into living
reference documentation through incremental improvements:

1. **Review for accuracy**: Teams (or AI agents assisting them) read the moved
   document against the current code and flag sections that have drifted.

2. **Update or annotate**: Outdated sections are either updated to reflect
   current behavior or annotated with notes like "This section describes the
   original design; the current implementation differs in X." Wholesale rewrites
   are not required.

3. **Merge related documents**: Where multiple RFCs describe aspects of the same
   system (e.g., backup and full-cluster backup), they can be consolidated into
   a single `packagename.md` once someone has reviewed both for coherence. This
   is optional and happens when it's useful, not on a schedule.

4. **Add new content**: As features are added to a package, their design context
   goes into the existing doc or a new `packagename-topic.md`, rather than back
   into `docs/RFCS/`.

This is intentionally low-ceremony. There is no requirement to modernize a
document upon moving it. The move itself provides the primary value (discovery,
proximity to code). Content improvements happen organically as people work in
the package.


## Alternatives

### `docs/` subdirectory per package

```
pkg/backup/docs/backup.md
pkg/backup/docs/backup-online-restore.md
```

It isn't clear this adds much value but could be considered if docs, or their
supporting asseets, start to clutter up a package root.

### `README.md` per package

Using `README.md` for the primary doc has the advantage of being rendered
automatically by GitHub when browsing the directory. However, it's not
self-identifying in search results, and there's no natural naming convention for
sub-documents (`README-online-restore.md` is awkward).

### `doc.go` (Go package doc comments)

Go's `doc.go` convention puts package documentation in Go source files. This
works well for API documentation but is poorly suited for design documents with
diagrams, tables, and multi-page explanations. The two are complementary:
`doc.go` describes the package API, `packagename.md` describes the design.

### Keep everything in `docs/RFCS/`

The status quo. Documents continue to accumulate in a flat directory far from
the code, read primarily by people who already know they exist.

## Future Work: Collaborative Review of In-Repo Design Docs

A common objection to moving design documents into the repo is that Google Docs
provides a much better experience for drafting and reviewing: inline comments
on arbitrary text selections, threaded discussions, resolution, and real-time
collaboration. GitHub PR review comes close but operates on raw markdown lines,
not rendered output.

A purpose-built web app could bridge this gap. Hosted at e.g. `doc.crdb.dev`,
it would serve URLs like `doc.crdb.dev/branch/pkg/backup/backup.md` and
provide:

- **Rendered markdown** with full control over presentation (diagrams,
  animations, custom blocks — whatever we want to support).
- **A comment sidebar** where reviewers select text in the rendered document and
  leave threaded, resolvable comments.
- **Branch-aware URLs** so a doc on a feature branch can be reviewed before
  merge, mapping naturally to the PR workflow.

Comments would be stored via the GitHub API rather than in a separate system:

- **On a branch with an open PR**: the app maps text selections back to source
  line numbers and posts PR review comments. Reviewers can use either the app or
  GitHub's PR view interchangeably — they see the same comments.
- **On main (post-merge)**: comments go to a GitHub Discussion linked to the
  document, for long-lived discussion on merged docs.

The text-selection-to-line-number mapping is the main technical challenge, but
it's solvable: the markdown renderer knows which source lines produced which
DOM nodes, so rendered elements can carry `data-source-line` attributes for
reverse mapping.

This would let teams draft and review design docs with a Google Docs-like
experience while keeping the source of truth in the repository.
