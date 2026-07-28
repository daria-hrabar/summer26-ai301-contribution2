# Contribution 2: Let taps continue to next partitions and/or streams upon certain errors #280

**Contribution Number:** 2  
**Student:** Daria Hrabar  
**Issue:** [Issue #280](https://github.com/meltano/sdk/issues/280)  
**Status:** Phase I Completed  

---

## Why I Chose This Issue

By taking on this issue, I hope to expand my knowledge of writing adaptable and reliable Python programs, with emphasis on comprehensive error handling. I believe my applied AI engineering skills, enhanced by developing [a solution to the nested replication keys issue in the Meltano SDK repository](https://github.com/meltano/sdk/pull/3690), meet the demand of this issue to introduce new error-handling behaviors.

---

## Understanding the Issue

### Problem Description

The Meltano Singer SDK lacks a mechanism for taps to gracefully continue syncing when a non-fatal error occurs mid-run. While the SDK already defines `RetriableAPIError` and `FatalAPIError` for HTTP-level error classification, there is no equivalent signal that tells the tap to skip the current partition or stream and move on to the next one. As a result, any unhandled error during a partitioned sync aborts the entire tap run — all remaining partitions and streams are abandoned, and their data is never extracted.

### Expected Behavior

When a tap encounters a recoverable or partition-scoped error during sync, it should:

1. *At the partition level:* catch the error in `Stream._sync_records` (in `singer_sdk/streams/core.py`), log or record it, preserve whatever state has been written so far, and `continue` to the next partition context in the loop.
2. *At the stream level:* catch the error in `Tap.sync_all` (in `singer_sdk/tap_base.py`), and similarly `continue` to the next stream rather than crashing the process.
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

*Step 1:* Install project prerequisites on your local device. More details can be found on [the Prerequisites page](https://docs.meltano.com/contribute/prerequisites/) within the Meltano Documentation.
  1. [Python 3.10+](https://www.python.org/downloads/)
  2. [uv](https://docs.astral.sh/uv/)
  3. [Node 18+](https://nodejs.org/)
  4. [Yarn](https://yarnpkg.com/)
  5. Although not listed as an official prerequisite, it is also recommended to install [Visual Studio Build Tools for C++](https://visualstudio.microsoft.com/visual-cpp-build-tools/) to avoid the `ModuleNotFoundError`.

*Step 2:* Complete the setup of your local development environment.
  1. Clone the forked repository to your local device by either:
     - Running `git clone git@github.com:[your/github/file/path].git` and `cd sdk` in your terminal, or
     - Completing the process directly through GitHub Desktop.
  2. Install Nox and pre-commit by running `uv tool install nox` and `uv tool install pre-commit`.
  3. If not already there, navigate to the cloned local repository and install dependencies by running `uv sync`.
  4. Install pre-commit hooks by running `pre-commit install --install-hooks`. 

When working in VS Code, the virtual environment should become activated automatically. If not, run the `.venv\Scripts\Activate.ps1` command in your terminal. Your `.venv` is activated if you can see `(singer-sdk)` displayed at the beginning of your file path in the terminal window.

### Steps to Reproduce

1. [Step 1]
2. [Step 2]
3. [Observed result]

### Reproduction Evidence

- **Commit showing reproduction:** [Link to commit in your fork]
- **Screenshots/logs:** [If applicable]
- **My findings:** [What you discovered during reproduction]

---

## Solution Approach

### Analysis

[Your analysis of the root cause - what's causing the issue?]

### Proposed Solution

[High-level description of your fix approach]

### Implementation Plan

Using UMPIRE framework (adapted):

**Understand:** [Restate the problem]

**Match:** [What similar patterns/solutions exist in the codebase?]

**Plan:** [Step-by-step implementation plan]
1. [Modify file X to do Y]
2. [Add function Z]
3. [Update tests]

**Implement:** [Link to your branch/commits as you work]  
- *Link to the working branch:* https://github.com/daria-hrabar/sdk/tree/fix-issue-280

**Review:** [Self-review checklist - does it follow the project's contribution guidelines?]

**Evaluate:** [How will you verify it works?]

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
