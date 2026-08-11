# Contribution 2: Let taps continue to next partitions and/or streams upon certain errors #280

**Contribution Number:** 2  
**Student:** Daria Hrabar  
**Issue:** [Issue #280](https://github.com/meltano/sdk/issues/280)  
**Status:** Phase III Completed  

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

**Match:** The existing `RetriableAPIError` and `FatalAPIError` patterns in `singer_sdk/exceptions.py` are the direct precedent: new exceptions are defined, inherit from intermediate SDK base classes, and are caught at specific points in the sync loop. The CLAUDE.md exception table also lists `IgnorableAPIError` as the model for "expected / skip silently" behavior, which is closest to what `EndOfStreamError` needs to do.

**Plan:**
1. Add `EndOfStreamError` to `singer_sdk/exceptions.py`, inheriting from the appropriate intermediate base, with an entry in `__all__`
2. Wrap the `for context_element in context_list` loop body in `Stream._sync_records` (`singer_sdk/streams/core.py`) with `try/except EndOfStreamError`, logging the skipped partition and calling `continue`
3. Wrap the stream iteration in `Tap.sync_all` (`singer_sdk/tap_base.py`) with `try/except EndOfStreamError`, logging the skipped stream and calling `continue`
4. Add `EndOfStreamError` to the existing import block in `tests/core/test_streams.py`, and add a `test_end_of_stream_error_hierarchy` test asserting the correct inheritance chain (`SkippableSyncError` → `SyncError` → `SingerSDKError`)
5. Add unit tests to `tests/core/test_streams.py` covering: partition-level skip (middle, first, last, only, and all partitions), stream-level skip, empty partition list, empty stream list, exception hierarchy, and verification that plain `RuntimeError` still propagates uncaught at both the partition and stream level

**Implement:**
- *Link to the working branch:* https://github.com/daria-hrabar/sdk/tree/fix-issue-280

**Review:** Stepped through `_sync_records` and `sync_all` manually, tracing the partition loop with the three-partition scenario (`good-repo`, `broken-repo`, `another-good-repo`) as the happy path, and verified edge cases, including empty partition/stream list, all partitions failing, first/last/only partition failing, and non-`EndOfStreamError` propagation at both levels.

*Happy path trace:*
- `context_list = [{"repo": "good-repo"}, {"repo": "broken-repo"}, {"repo": "another-good-repo"}]`
- Iteration 1: `context_element = {"repo": "good-repo"}` → `get_records()` yields one record → record emitted ✓
- Iteration 2: `context_element = {"repo": "broken-repo"}` → `get_records()` raises `EndOfStreamError` → caught by `except EndOfStreamError` → warning logged → `continue` ✓
- Iteration 3: `context_element = {"repo": "another-good-repo"}` → `get_records()` yields one record → record emitted ✓

*Edge case traces:*
- **Empty partition list:** `context_list = []` → `context_list or [{}]` evaluates to `[{}]` → loop runs once with `context_element = {}` → `get_records({})` does not raise → record emitted normally → no regression ✓
- **Empty stream list:** `discover_streams()` returns `[]` → `sync_all` iterates over nothing → exits cleanly with no records and no errors ✓
- **All partitions fail:** all three iterations raise `EndOfStreamError` → all caught → zero records emitted → tap exits cleanly ✓
- **First partition fails:** iteration 1 raises → caught → `continue` → iterations 2 and 3 emit records normally ✓
- **Last partition fails:** iterations 1 and 2 emit records normally → iteration 3 raises → caught → `continue` → loop exits cleanly with no off-by-one issues ✓
- **Only partition fails:** single iteration raises → caught → zero records → tap exits cleanly ✓
- **Non-`EndOfStreamError` at partition level:** `RuntimeError` raised in `get_records()` → not caught by `except EndOfStreamError` → propagates up and crashes the tap → confirms the catch is precise ✓
- **Non-`EndOfStreamError` at stream level:** `RuntimeError` raised in `stream.sync()` → not caught by `except EndOfStreamError` in `sync_all` → propagates up and crashes the tap → confirms the stream-level catch is equally precise ✓

*Stream-level trace:*
- `sync_all` iterates over `[GoodStream, BrokenStream, AnotherGoodStream]`
- `GoodStream.sync()` completes normally ✓
- `BrokenStream.sync()` raises `EndOfStreamError` → caught → warning logged → `continue` ✓
- `AnotherGoodStream.sync()` completes normally ✓

*Exception hierarchy verified:*
- `EndOfStreamError` confirmed as a subclass of `SkippableSyncError` → `SyncError` → `SingerSDKError` → exception is correctly plugged into the SDK hierarchy and will not be accidentally swallowed or missed by broader SDK catches ✓

Pre-commit checks passed with no errors:
- [x] `ruff check` — no violations
- [x] `ruff format` — no formatting changes
- [x] `pre-commit run --all` — all hooks passed

**Evaluate:** Run the `verify_fix_280.py` script, directly adapted from `reproduce_issue_280.py`, and confirm that all valid partitions produce output, including `another-good-repo`, with the error for `broken-repo` logged and the tap continuing to completion with exit code `0`.

---

## Testing Strategy

### Unit Tests

- *Test case 1 — Partition-level skip:* Verifies that raising `EndOfStreamError` inside `get_records()` for a specific partition skips that partition only, while records from the remaining partitions are still emitted. Directly tests the `try/except EndOfStreamError` block added to the `for context_element in context_list` loop in `Stream._sync_records`.
- *Test case 2 — Stream-level skip:* Verifies that raising `EndOfStreamError` inside `stream.sync()` skips that stream entirely, while all other streams in the tap continue to sync and emit records. Directly tests the `try/except EndOfStreamError` block added to `Tap.sync_all`.
- *Test case 3 — Exception hierarchy:* Verifies that `EndOfStreamError` correctly inherits from `SkippableSyncError`, `SyncError`, and `SingerSDKError`, confirming the new exception is wired into the right place in the SDK exception hierarchy.
- *Test case 4 — Empty partition list:* Verifies that when `partitions` returns an empty list, the SDK falls back to treating the stream as unpartitioned, running `get_records` once with an empty context and emitting records normally without crashing.
- *Test case 5 — Empty stream list:* Verifies that when `discover_streams()` returns an empty list, `sync_all` exits cleanly with no records and no errors.
- *Test case 6 — All partitions fail:* Verifies that when every partition raises `EndOfStreamError`, the tap exits cleanly with zero records emitted.
- *Test case 7 — First partition fails:* Verifies that `continue` correctly skips to the next iteration rather than breaking out of the loop entirely, with remaining partitions still emitting records.
- *Test case 8 — Last partition fails:* Verifies that a `continue` on the final loop iteration exits cleanly with no off-by-one issues and records from earlier partitions preserved.
- *Test case 9 — Only partition fails:* Verifies that a single-partition stream that raises `EndOfStreamError` exits cleanly with zero records emitted.
- *Test case 10 — Non-`EndOfStreamError` propagates at partition level:* Verifies that a plain `RuntimeError` raised in `get_records()` is not caught by the `except EndOfStreamError` block and still crashes the tap, confirming the catch is precise.
- *Test case 11 — Non-`EndOfStreamError` propagates at stream level:* Verifies that a plain `RuntimeError` raised in `stream.sync()` is not caught by the stream-level `except EndOfStreamError` block in `sync_all` and still crashes the tap.

### Integration Tests

- *Integration scenario 1 — Full nox test suite:* `nox -s tests` was run across all supported Python versions (3.10–3.14) after implementing the fix. All 835–836 tests passed per session with 0 failures, confirming no existing stream, state, partition, or replication behavior was broken by changes to `exceptions.py`, `core.py`, and `tap_base.py`.
- *Integration scenario 2 — Coverage session:* `nox -s coverage` combined data from all 5 Python version sessions and confirmed `singer_sdk/exceptions.py` at **100% coverage**, verifying that all lines of the new `EndOfStreamError` class are exercised by the tests.

### Manual Testing

Adapted `verify_fix_280.py` from the original reproduction script `reproduce_issue_280.py` to confirm the favorable behavior change after introducing `EndOfStreamError`.

*Before the fix:* tap crashed at `broken-repo` with an unhandled `RuntimeError`, and `another-good-repo` produced no output.

*After the fix:* all three partitions were processed within `verify_fix_280.py`. The `EndOfStreamError` for `broken-repo` was logged as a `WARNING`, and the tap continued to completion:

```
2026-08-05 00:08:56,973 | INFO | tap-broken | tap-broken v[could not be detected], Meltano SDK v0.54.3.dev51+g1b205c028
2026-08-05 00:08:56,974 | INFO | tap-broken | Skipping parse of env var settings...
2026-08-05 00:08:56,975 | INFO | tap-broken.broken_stream | Beginning sync of 'broken_stream' in full_table mode
{"type":"SCHEMA","stream":"broken_stream","schema":{"properties":{"id":{"type":["integer","null"]}},"type":"object","$schema":"https://json-schema.org/draft/2020-12/schema"},"key_properties":["id"]}
2026-08-05 00:08:56,976 | WARNING | tap-broken.broken_stream | Properties ('repo',) were present in the 'broken_stream' stream but not found in catalog schema. Ignoring.
{"type":"RECORD","stream":"broken_stream","record":{"id":1},"time_extracted":"2026-08-05T04:08:56.976974+00:00"}
2026-08-05 00:08:56,977 | WARNING | tap-broken.broken_stream | Skipping partition '{'repo': 'broken-repo'}' in stream 'broken_stream': Simulated persistent error for repo: broken-repo
{"type":"RECORD","stream":"broken_stream","record":{"id":1},"time_extracted":"2026-08-05T04:08:56.977408+00:00"}
2026-08-05 00:08:56,977 | INFO | singer_sdk.metrics | METRIC: {"type":"timer","metric":"sync_duration","value":0.0008358955383300781,"tags":{"stream":"broken_stream","pid":7436,"context":{"repo":"another-good-repo"},"status":"succeeded"}}
2026-08-05 00:08:56,977 | INFO | singer_sdk.metrics | METRIC: {"type":"counter","metric":"record_count","value":2,"tags":{"stream":"broken_stream","pid":7436,"context":{"repo":"another-good-repo"}}}
{"type":"STATE","value":{"bookmarks":{"broken_stream":{"partitions":[{"context":{"repo":"good-repo"}},{"context":{"repo":"another-good-repo"}}]}}}}
{"type":"STATE","value":{"bookmarks":{"broken_stream":{"partitions":[{"context":{"repo":"good-repo"}},{"context":{"repo":"another-good-repo"}},{"context":{"repo":"broken-repo"}}]}}}}
```

---

## Implementation Notes

### Week 1 Progress

**What Was Built:**

- Added `EndOfStreamError` to `singer_sdk/exceptions.py`, inheriting from `SkippableSyncError`, and registered it in `__all__`
- Added a `try/except EndOfStreamError` block inside the `for context_element in context_list` loop in `Stream._sync_records` (`singer_sdk/streams/core.py`) to catch partition-level signals, log a warning with the partition context and error message, and `continue` to the next partition
- Added a `try/except EndOfStreamError` block around `stream.sync()` in `Tap.sync_all` (`singer_sdk/tap_base.py`) to catch stream-level signals, log a warning with the stream name, and `continue` to the next stream
- Created `verify_fix_280.py`, derived from `reproduce_issue_280.py`, at the root of the repository to verify the fix, using three partitions where the middle one raises `EndOfStreamError`
- Added three unit tests to `tests/core/test_streams.py` covering partition-level skip, stream-level skip, and exception hierarchy verification

**Challenges Faced:**

- Running `nox -s tests` after committing produced an `Access Denied (os error 5)` failure in the `coverage` session, caused by Windows locking files inside the `.nox\coverage` environment. Resolved by deleting all stale nox environments and re-running:
```powershell
  Remove-Item -Recurse -Force .nox\tests-3-10, .nox\tests-3-11, .nox\tests-3-12, .nox\tests-3-13, .nox\tests-3-14, .nox\coverage
```

**Decisions Made:**

- `reproduce_issue_280.py` raised a plain `RuntimeError` to simulate a partition failure. After implementing the fix, the script was updated to `verify_fix_280.py` to raise `EndOfStreamError` instead. `EndOfStreamError` is an explicit, opt-in signal for tap developers; they must deliberately raise it to indicate a partition is skippable. A plain `RuntimeError` should still crash the tap, as it may indicate a genuine bug rather than an expected, recoverable condition.

**Test Coverage After Week 1:**

- `singer_sdk/exceptions.py` — **100%**, confirming all lines of `EndOfStreamError` are exercised by tests.
- `singer_sdk/streams/core.py` — **90%**, consistent with the project baseline; uncovered lines are pre-existing edge cases unrelated to this fix.
- `singer_sdk/tap_base.py` — **85%**, consistent with the project baseline.
- Overall SDK coverage — **83%**, unchanged from baseline, confirming the fix introduced no regressions.

**Windows PowerShell Output After the Nox Test Suite Run:**

```
nox > Ran 6 sessions in 2 minutes:
nox > * tests-3.10: success, took 22 seconds
nox > * tests-3.11: success, took 24 seconds
nox > * tests-3.12: success, took 22 seconds
nox > * tests-3.13: success, took 20 seconds
nox > * tests-3.14: success, took 15 seconds
nox > * coverage: success, took 10 seconds
```

**Windows PowerShell Commands Used:**

- `git add <file>` — stages specific files for commit
- `git commit -m "title" -m "description"` — commits with both title and body
- `git push origin <branch-name>` — pushes local commits to the remote fork
- `git pull origin <branch-name> --rebase` — syncs local branch with remote without creating a merge commit
- `nox -s tests` — runs the full test suite across all supported Python versions
- `nox -s coverage` — combines per-version coverage data and generates a full coverage report

### Week 2 Progress

**What Was Built:**

- Added 8 additional unit tests to `tests/core/test_streams.py` covering: empty partition list fallback, empty stream list, all partitions failing, first/last/only partition failing, and non-`EndOfStreamError` propagation at both partition and stream level.

**Challenges Faced:**

- After adding new unit tests, `nox -s tests` failed across all Python versions due to stale cached environments. Resolved by restarting Windows PowerShell as an Administrator, clearing cached environments, and reinstalling the hooks:
```powershell
  Remove-Item -Recurse -Force "$env:USERPROFILE\.cache\pre-commit"
  pre-commit install --install-hooks
```

**Findings & Decisions Made:**

- *Empty partition fallback behavior:* Investigation of the `test_end_of_stream_error_empty_partition_list` failure revealed that an empty `partitions` list does not mean "sync nothing" — the SDK falls back to `context_list or [{}]`, treating the stream as unpartitioned and running `get_records` once with an empty context. The test assertion was corrected from `records == []` to `len(records) == 1`.

- *Misleading record emission in `reproduce_issue_280.py`:* The reproduction script emits one record before crashing, but the metrics tag it under `"context":{"repo":"broken-repo"}`:

```
{"type":"counter","metric":"record_count","value":1,"tags":
{"stream":"broken_stream","pid":14408,"context":{"repo":"broken-repo"}}}
```

The record was actually emitted by `good-repo` — the SDK associates the final metrics with the last active context at the time of the crash, which is `broken-repo`. No change was made to the reproduction script.

**Test Coverage Report:**

No change observed from Week 1. All three modified files remain at the same percentages: `singer_sdk/exceptions.py` at 100%, `singer_sdk/streams/core.py` at 90%, and `singer_sdk/tap_base.py` at 85%. The 8 new tests cover additional edge cases but exercise code paths already counted as covered by the original 3 tests, so no new lines are unlocked.

**Windows PowerShell Output After the Latest Nox Test Suite Run:**

```
nox > Session coverage was successful in 3 seconds.
nox > Ran 6 sessions in 4 minutes:
nox > * tests-3.10: success, took 50 seconds
nox > * tests-3.11: success, took 45 seconds
nox > * tests-3.12: success, took 45 seconds
nox > * tests-3.13: success, took 40 seconds
nox > * tests-3.14: success, took 35 seconds
nox > * coverage: success, took 3 seconds
```

---

### Code Changes

**Files Modified:**

- `singer_sdk/exceptions.py` — added `EndOfStreamError` class inheriting from `SkippableSyncError`, registered in `__all__`
- `singer_sdk/streams/core.py` — imported `EndOfStreamError`; added `try/except EndOfStreamError` block inside the partition loop in `_sync_records`
- `singer_sdk/tap_base.py` — imported `EndOfStreamError`; added `try/except EndOfStreamError` block around `stream.sync()` in `sync_all`
- `tests/core/test_streams.py` — added 11 unit tests and updated the `singer_sdk.exceptions` import block

**Key Commits:**
- [Add a new non-fatal error class EndOfStreamError](https://github.com/meltano/sdk/commit/0df3405f663c97bdae0acf4ad21c239c90161968)
- [Add the partition-level EndOfStreamError catch in core.py](https://github.com/meltano/sdk/commit/fcdc18fadbd6dd6fe82f4cdcdcc2658a673a651d)
- [Add the stream-level EndOfStreamError catch in tap_base.py](https://github.com/meltano/sdk/commit/c279f11d34e8310cf40415836715758dc4ccc101)
- [test: add EndOfStreamError partition- and stream-level skip tests](https://github.com/meltano/sdk/commit/635effb6b16a678f828bdbe9d6a8e5f3db8200f2)
- [Create a script verifying fix of issue 280](https://github.com/meltano/sdk/commit/44482954663e3ec41d854ec1955813d2c1bb8762)
- [Added new edge cases, including empty partition/stream list, all partitions failing, first/last/only partition failing, and `non-EndOfStreamError` propagation at both levels](https://github.com/meltano/sdk/commit/3064682714f38c0a9172f470eeb41d0ffe2118bc)
- [Modify `test_end_of_stream_error_empty_partition_list` test to treat an empty partition as an unpartitioned stream instead of an empty list
](https://github.com/meltano/sdk/commit/088510920137be8d99e77f96f8d6f7b86d289531)

**Approach Decisions:**

- Placed `EndOfStreamError` in `singer_sdk/exceptions.py` rather than defining it inline in `core.py` or `tap_base.py`, consistent with the project's rule that all public exceptions must live in that file
- Chose `SkippableSyncError` as the base class because it already carries the semantic meaning of "log, skip, and continue" — matching exactly what `EndOfStreamError` needs to signal
- Kept `EndOfStreamError` as a named subclass rather than reusing `SkippableSyncError` directly, so the SDK's `try/except` blocks can catch it precisely without accidentally swallowing other skippable errors that were never meant to trigger partition or stream continuation
- Added the stream-level catch in `Tap.sync_all` in addition to the partition-level catch in `_sync_records`, because a stream can raise `EndOfStreamError` before partition iteration even begins. Both levels are needed to cover different failure points in the sync lifecycle
- Corrected the empty partition test assertion from `records == []` to `len(records) == 1` after discovering the SDK's fallback behavior

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
