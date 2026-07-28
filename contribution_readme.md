# Contribution 2: Let taps continue to next partitions and/or streams upon certain errors #280

**Contribution Number:** 2  
**Student:** Daria Hrabar  
**Issue:** [Issue #280](https://github.com/meltano/sdk/issues/280)  
**Status:** Phase II In Progress  

---

## Why I Chose This Issue

By taking on this issue, I hope to expand my knowledge of writing adaptable and reliable Python programs, with emphasis on comprehensive error handling. I believe my applied AI engineering skills, enhanced by developing [a solution to the nested replication keys issue in the Meltano SDK repository](https://github.com/meltano/sdk/pull/3690), meet the demand of this issue to introduce new error-handling behaviors.

---

## Understanding the Issue

### Problem Description

The Meltano Singer SDK lacks a mechanism for taps to gracefully continue syncing when a non-fatal error occurs mid-run. While the SDK already defines `RetriableAPIError` and `FatalAPIError` for HTTP-level error classification, there is no equivalent signal that tells the tap to skip the current partition or stream and move on to the next one. As a result, any unhandled error during a partitioned sync aborts the entire tap run — all remaining partitions and streams are abandoned, and their data is never extracted.

### Expected Behavior

When a tap encounters a recoverable or partition-scoped error during sync, it should:

1. **At the partition level:** catch the error in `Stream._sync_records` (in `singer_sdk/streams/core.py`), log or record it, preserve whatever state has been written so far, and `continue` to the next partition context in the loop.
2. **At the stream level:** catch the error in `Tap.sync_all` (in `singer_sdk/tap_base.py`), and similarly `continue` to the next stream rather than crashing the process.
3. Optionally return a non-zero exit code to signal partial failure to the calling process, while still emitting all successfully retrieved records and state.

As proposed by maintainer [@edgarrmondragon](https://github.com/meltano/sdk/issues/280), this would be implemented via a new `EndOfStreamError` exception class that tap developers can raise to explicitly signal "skip this partition/stream and keep going."

### Current Behavior

In `Stream._sync_records`, the code iterates through a list of partition contexts with no surrounding error handling. If an exception is raised for any partition — even a persistent, partition-specific API error — the exception propagates up uncaught, halting the entire tap process. All subsequent partitions and streams in the run are silently skipped. Because the tap runs as a separate subprocess, the calling process (e.g., Meltano) cannot inspect the exception details; it only sees a failed exit code with no partial results or state.

A concrete example is `tap-github`, which fetches data for a list of repositories (partitions). If one repository returns a persistent error, no data is extracted for any repo further down the list.

### Affected Components

| File | Relevant Location | What Changes |
|---|---|---|
| `singer_sdk/streams/core.py` | `Stream._sync_records` — the `for context_element in context_list or [{}]:` loop | Wrap loop body in `try/except EndOfStreamError: continue` |
| `singer_sdk/tap_base.py` | `Tap.sync_all` — the stream iteration loop | Wrap each stream sync in `try/except EndOfStreamError: continue` |
| `singer_sdk/exceptions.py` | Top-level exception definitions | Add new `EndOfStreamError` exception class alongside `RetriableAPIError` and `FatalAPIError` |

### Solution Acceptance Criteria
- A new `EndOfStreamError` (or equivalent) exception is importable from `singer_sdk.exceptions`.
- Raising `EndOfStreamError` inside `get_records()` for a given partition causes that partition to be skipped; remaining partitions in the same stream continue to sync normally.
- Raising `EndOfStreamError` at the stream level causes that stream to be skipped; remaining streams in the tap continue to sync.
- State is correctly written for all successfully completed partitions/streams before the error.
- Existing behavior for `FatalAPIError` (halt everything) and `RetriableAPIError` (retry with backoff) is unchanged.
- Unit tests cover both the partition-level and stream-level skip scenarios.

---

## Reproduction Process

### Environment Setup

**Step 1:** Install project prerequisites on your local device. More details can be found on [the Prerequisites page](https://docs.meltano.com/contribute/prerequisites/) within the Meltano Documentation.
  1. [Python 3.10+](https://www.python.org/downloads/)
  2. [uv](https://docs.astral.sh/uv/)
  3. [Node 18+](https://nodejs.org/)
  4. [Yarn](https://yarnpkg.com/)
  5. Although not listed as an official prerequisite, it is also recommended to install [Visual Studio Build Tools for C++](https://visualstudio.microsoft.com/visual-cpp-build-tools/) to avoid the `ModuleNotFoundError`.

**Step 2:** Complete the setup of your local development environment.
  1. Clone the forked repository to your local device by either:
     - Running `git clone git@github.com:[your/github/file/path].git` and `cd sdk` in your terminal, or
     - Completing the process directly through GitHub Desktop.
  2. Install Nox and pre-commit by running `uv tool install nox` and `uv tool install pre-commit`.
  3. If not already there, navigate to the cloned local repository and install dependencies by running `uv sync`.
  4. Install pre-commit hooks by running `pre-commit install --install-hooks`. 

When working in VS Code, the virtual environment should become activated automatically. If not, run the `.venv\Scripts\Activate.ps1` command in your terminal. Your `.venv` is activated if you can see `(singer-sdk)` displayed at the beginning of your file path in the terminal window.

### Steps to Reproduce

1. Create a reproduction script (`reproduce_issue_280.py`) at the root of the forked SDK repository defining a `BrokenStream` with three partitions, `good-repo`, `broken-repo`, and `another-good-repo`, where `get_records()` raises a `RuntimeError` on the second partition, and a `BrokenTap` that registers it.
2. Run the script from the terminal:
```powershell
   python reproduce_issue_280.py --config '{}' sync
```
3. **Observed result:** The tap successfully emits a record for `good-repo`, then crashes on `broken-repo` with an unhandled `RuntimeError`. `another-good-repo` is never reached and produces no output. The process exits with a non-zero error code.

### Reproduction Evidence

- **Link to the working branch:** [daria-hrabar/sdk/fix-issue-280](https://github.com/daria-hrabar/sdk/tree/fix-issue-280)
- **Commit showing reproduction:** [Add reproduce_issue_280.py](https://github.com/meltano/sdk/commit/9e9d0f9c92468a96d3168ecc37d3afb33522ac3c)
- **Terminal output after running reproduction script:**
```
2026-07-28 13:40:18,562 | INFO | tap-broken | tap-broken v[could not be detected], Meltano SDK v0.54.3.dev51+g1b205c028
2026-07-28 13:40:18,568 | INFO | tap-broken | Skipping parse of env var settings...
2026-07-28 13:40:18,585 | INFO | tap-broken.broken_stream | Beginning sync of 'broken_stream' in full_table mode
{"type":"SCHEMA","stream":"broken_stream","schema":{"properties":{"id":{"type":["integer","null"]}},"type":"object","$schema":"https://json-schema.org/draft/2020-12/schema"},"key_properties":["id"]}
2026-07-28 13:40:18,598 | WARNING | tap-broken.broken_stream | Properties ('repo',) were present in the 'broken_stream' stream but not found in catalog schema. Ignoring.
{"type":"RECORD","stream":"broken_stream","record":{"id":1},"time_extracted":"2026-07-28T17:40:18.599427+00:00"}
2026-07-28 13:40:18,600 | INFO | singer_sdk.metrics | METRIC: {"type":"timer","metric":"sync_duration","value":0.0014007091522216797,"tags":{"stream":"broken_stream","pid":26348,"context":{"repo":"broken-repo"},"status":"failed"}}
2026-07-28 13:40:18,606 | INFO | singer_sdk.metrics | METRIC: {"type":"counter","metric":"record_count","value":1,"tags":{"stream":"broken_stream","pid":26348,"context":{"repo":"broken-repo"}}}
2026-07-28 13:40:18,612 | ERROR | tap-broken.broken_stream | An unhandled error occurred while syncing 'broken_stream'
2026-07-28 13:40:18,619 | ERROR | tap-broken | Simulated persistent error for repo: broken-repo
Traceback (most recent call last):
File "/file_path/reproduce_issue_280.py", line 49, in <module>
BrokenTap.cli()
~~~~~~~~~~~~~^^
File "/file_path/.venv/Lib/site-packages/click/core.py", line 1524, in call
return self.main(*args, **kwargs)
~~~~~~~~~^^^^^^^^^^^^^^^^^
File "/file_path/.venv/Lib/site-packages/click/core.py", line 1445, in main
rv = self.invoke(ctx)
File "/file_path/singer_sdk/plugin_base.py", line 148, in invoke
return super().invoke(ctx)
~~~~~~~~~~~~~~^^^^^
File "/file_path/.venv/Lib/site-packages/click/core.py", line 1308, in invoke
return ctx.invoke(self.callback, **ctx.params)
~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/file_path/.venv/Lib/site-packages/click\core.py", line 877, in invoke
return callback(*args, **kwargs)
File "/file_path/singer_sdk/tap_base.py", line 574, in invoke
tap.sync_all()
~~~~~~~~~~~~^^
File "/file_path/singer_sdk/tap_base.py", line 513, in sync_all
stream.sync()
~~~~~~~~~~~^^
File "/file_path/singer_sdk/streams/core.py", line 1302, in sync
for _ in self._sync_records(context=context):
~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^
File "/file_path/singer_sdk/streams/core.py", line 1174, in _sync_records
for idx, record_result in enumerate(self.get_records(current_context)):
~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/file_path/reproduce_issue_280.py", line 34, in get_records
raise RuntimeError(msg)
RuntimeError: Simulated persistent error for repo: broken-repo
```
- **My findings:** The SDK's metrics logger does record the failure — `"context":{"repo":"broken-repo"},"status":"failed"}` appears in the output, meaning the infrastructure to detect a partition failure already exists. What is missing is the recovery logic: after detecting the failure, the SDK has no mechanism to log it, preserve state, and continue to the next partition.

---

## Solution Approach

### Analysis

The root cause is the absence of a `try/except` block inside the partition loop in `Stream._sync_records` (`singer_sdk/streams/core.py`), and the absence of equivalent error handling in the stream loop in `Tap.sync_all` (`singer_sdk/tap_base.py`). When `get_records()` raises any exception for a given partition, it propagates upward uncaught, terminating the entire tap process. No recovery, state preservation, or continuation to the next partition or stream occurs. The SDK already has the vocabulary for error classification (`RetriableAPIError`, `FatalAPIError`, `IgnorableAPIError` in `singer_sdk/exceptions.py`) but no exception type that signals "skip this partition/stream and keep going."

### Proposed Solution

Introduce a new `EndOfStreamError` exception class in `singer_sdk/exceptions.py`, inheriting from the appropriate SDK base. Then wrap the partition loop in `Stream._sync_records` and the stream loop in `Tap.sync_all` with `try/except EndOfStreamError` blocks that log the skipped context and continue to the next iteration. Tap developers can then raise `EndOfStreamError` inside `get_records()` to signal a clean, intentional skip, without crashing the tap.

### Implementation Plan Using UMPIRE Framework:

**Understand:** The SDK provides no way for a tap to recover from a partition- or stream-level error and continue syncing. Any unhandled exception in `get_records()` crashes the entire run, leaving all subsequent partitions and streams unprocessed.

**Match:** The existing `RetriableAPIError` and `FatalAPIError` pattern in `singer_sdk/exceptions.py` is the direct precedent: new exceptions are defined there, inherit from intermediate SDK base classes, and are caught at specific points in the sync loop. The CLAUDE.md exception table also lists `IgnorableAPIError` as the model for "expected / skip silently" behavior, which is closest to what `EndOfStreamError` needs to do.

**Plan:**
1. Add `EndOfStreamError` to `singer_sdk/exceptions.py`, inheriting from the appropriate intermediate base, with an entry in `__all__`
2. Wrap the `for context_element in context_list` loop body in `Stream._sync_records` (`singer_sdk/streams/core.py`) with `try/except EndOfStreamError`, logging the skipped partition and calling `continue`
3. Wrap the stream iteration in `Tap.sync_all` (`singer_sdk/tap_base.py`) with `try/except EndOfStreamError`, logging the skipped stream and calling `continue`
4. Add `issubclass` assertions for `EndOfStreamError` to `tests/core/test_exceptions.py`
5. Add unit tests covering partition-level and stream-level skip scenarios, verifying that records from non-failing partitions/streams are still emitted

**Implement:**
- *Link to the working branch:* https://github.com/daria-hrabar/sdk/tree/fix-issue-280

**Review:**
- [ ] `from __future__ import annotations` present in every modified file
- [ ] `import typing as t` used for type imports
- [ ] New exception placed in `singer_sdk/exceptions.py` and added to `__all__`
- [ ] New exception inherits from an appropriate SDK intermediate base class, not directly from `Exception`
- [ ] Google-style docstrings on all new/modified methods
- [ ] `ruff check` and `ruff format` pass with no errors
- [ ] `pre-commit run --all` passes

**Evaluate:** Run the reproduction script `reproduce_issue_280.py` after the fix and confirm that all three partitions produce output, including `another-good-repo`, with the error for `broken-repo` logged but the tap continuing to completion with exit code `0`.

---

## Testing Strategy

### Unit Tests

- [ ] Test case 1: [Description]
- [ ] Test case 2: [Description]
- [ ] Test case 3: [Description]

### Integration Tests

- [ ] Integration scenario 1
- [ ] Integration scenario 2

### Manual Testing

[What you tested manually and results]

---

## Implementation Notes

### Week [X] Progress

[What you built this week, challenges faced, decisions made]

### Week [Y] Progress

[Continue documenting as you work]

### Code Changes

- **Files modified:** [List]
- **Key commits:** [Links to important commits]
- **Approach decisions:** [Why you chose certain approaches]

---

## Pull Request

**PR Link:** [GitHub PR URL when submitted]

**PR Description:** [Draft or final PR description - much of the content above can be adapted]

**Maintainer Feedback:**
- [Date]: [Summary of feedback received]
- [Date]: [How you addressed it]

**Status:** [Awaiting review / Iterating / Approved / Merged]

---

## Learnings & Reflections

### Technical Skills Gained

[What you learned technically]

### Challenges Overcome

[What was hard and how you solved it]

### What I'd Do Differently Next Time

[Reflection on your process]

---

## Resources Used

- [Link to helpful documentation]
- [Tutorial or Stack Overflow post that helped]
- [GitHub issues or discussions that helped]
