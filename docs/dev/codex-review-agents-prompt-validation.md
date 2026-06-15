# Codex Review AGENTS Prompt Validation Notes

## Background

The Doris AI review workflow uses `.github/workflows/code-review-comment.yml` to
dispatch `/review` comments into `.github/workflows/code-review-runner.yml`.
The runner checks out the PR head, prepares a repository-local review context
directory, builds `review_prompt.txt`, runs `codex exec`, and records the prompt,
event stream, and final output to Litefuse.

The code-review skill already tells the model to read module-specific
`AGENTS.md` files, but recent Litefuse traces showed that this instruction was
not reliable enough. The model often read the main code-review skill and some
nearby guides, but skipped ancestor `AGENTS.md` files that should have applied
to the changed files.

The optimization was to make the workflow compute the exact required
`AGENTS.md` files from the PR changed-file list and inject those paths directly
into the first Codex prompt.

No Litefuse secrets are recorded in this document.

## Initial Trace Audit

Data source:

- Litefuse project: `code-review-test`
- Repository in traces: `apache/doris`
- Sample: latest 30 `doris-ai-review` traces at the time of audit
- Local artifact with full matrix: `/tmp/litefuse_latest30_agents_analysis.json`

Audit method:

1. Read the latest 30 Litefuse traces.
2. For each trace, get the reviewed PR from trace metadata.
3. Fetch every changed file from the PR.
4. In the current checkout, compute expected `AGENTS.md` files by walking each
   changed file's ancestor directories and keeping existing `AGENTS.md` files.
5. Parse trace observations for commands that actually read `AGENTS.md` content.
   Plain path discovery, such as `rg --files` or `find ... AGENTS.md`, was not
   counted as reading.

Observed coverage:

- Complete coverage: 8 / 30 traces
- Missing `AGENTS.md`: 16 traces
- Missing `fe/AGENTS.md`: 9 traces
- Missing `be/test/AGENTS.md`: 6 traces
- Missing `be/src/common/AGENTS.md`: 3 traces
- Missing `be/src/io/AGENTS.md`: 3 traces

Representative misses:

| Source PR | Expected | Actual | Missing |
|---|---|---|---|
| `apache/doris#64496` | `AGENTS.md`, `be/src/common/AGENTS.md`, `be/src/io/AGENTS.md`, `be/test/AGENTS.md` | unrelated `fe/.../nereids/trees/expressions/AGENTS.md` | all expected |
| `apache/doris#64478` | `AGENTS.md`, `fe/AGENTS.md`, `fe/fe-core/AGENTS.md` | `AGENTS.md`, `fe/fe-core/AGENTS.md` | `fe/AGENTS.md` |
| `apache/doris#64458` | `AGENTS.md`, `fe/AGENTS.md`, `fe/fe-core/AGENTS.md`, `fe/.../nereids/AGENTS.md` | `fe/fe-core/AGENTS.md`, `fe/.../nereids/AGENTS.md` | `AGENTS.md`, `fe/AGENTS.md` |
| `apache/doris#64489` | `AGENTS.md`, `be/test/AGENTS.md` | `be/src/common/AGENTS.md`, `be/src/core/AGENTS.md`, `be/test/AGENTS.md` | `AGENTS.md` |

## Implementation

Branch:

- `origin/dynamic_prompt`

Commits:

- `7cf67eb90a7` `[improvement](build) Inject required AGENTS guides into review prompt`
- `9192fd62f69` `[improvement](build) Allow dispatch validation for review runner`

Changed files:

- `.github/scripts/prepare_review_agents.py`
- `.github/workflows/code-review-runner.yml`

### Helper Script

`prepare_review_agents.py` takes a newline-delimited PR changed-file list and
generates:

- `required_agents.txt`: one required `AGENTS.md` path per line
- `required_agents_prompt.txt`: bullet list inserted into `review_prompt.txt`

The helper:

- Always includes root `AGENTS.md` when it exists in the checked-out tree.
- Walks each changed file's ancestor directories.
- Includes only `AGENTS.md` files that exist in the checked-out PR tree.
- Rejects absolute paths or paths with `..`.

The "exists in checked-out tree" rule matters for old validation branches. Some
module-level `AGENTS.md` files exist in the current tree but did not exist in
the historical base used by older PRs. The workflow should not ask the model to
read files that are absent from the checkout being reviewed.

### Runner Workflow

The runner now adds a `Prepare required AGENTS guides` step before building the
review prompt:

1. Fetch changed files through GitHub PR files API:
   `gh api --paginate repos/${REPO}/pulls/${PR_NUMBER}/files --jq '.[].filename'`
2. Download `prepare_review_agents.py` from the workflow ref.
3. Generate `pr_changed_files.txt`, `required_agents.txt`, and
   `required_agents_prompt.txt`.
4. Insert the required path list into the first Codex prompt.

Prompt text added:

```text
Before inspecting the PR diff or related code, you MUST read the contents of every AGENTS.md file listed below. These paths are computed from the PR changed file ancestors in this checkout. Searching for or listing paths is not sufficient; read each listed file directly.
Required AGENTS.md files for this PR:
- ...
```

The runner also has `workflow_dispatch` inputs matching `workflow_call`. This was
added for branch-level validation. The normal `/review` path still goes through
`workflow_call`.

## Validation Notes

### Why `workflow_dispatch`

`issue_comment` workflows are evaluated from the repository default branch, so a
plain `/review` comment cannot validate a workflow patch that only exists on a
test base branch. For branch-level validation, each test PR was reviewed through
manual dispatch of `code-review-runner.yml` from that test PR's base branch.

This still exercises the real runner: it checks out the PR head, submits a
GitHub PR review, and writes Litefuse trace data.

### Branch Layout

For each validation PR:

- Base branch = original source PR pre-merge base commit plus the dynamic prompt
  patch and the Litefuse upload helper.
- Head branch = base branch plus the original source PR changes.

The Litefuse helper had to be copied into the base branch as well. Older base
commits did not have `.github/scripts/emit_litefuse_otel_io.py`; without that
file, review could succeed but Litefuse upload failed with `gh: Not Found (HTTP
404)`.

### Final Effective Validation Set

Final evidence file:

- `/tmp/doris_agents_validate_final_results.json`

| Source PR | Validation PR | Run | Litefuse Trace | Expected `AGENTS.md` | Result |
|---|---:|---:|---|---|---|
| `apache/doris#64478` | `zclllyybb/doris#36` | `27536249602` | `d76b77ac4e0d52d96f413b154c9f2571` | `AGENTS.md`, `fe/AGENTS.md`, `fe/fe-core/AGENTS.md` | all read |
| `apache/doris#64458` | `zclllyybb/doris#37` | `27536258049` | `8db34388a8379acdcd9e48720f11225f` | `AGENTS.md`, `fe/AGENTS.md`, `fe/fe-core/AGENTS.md`, `fe/fe-core/src/main/java/org/apache/doris/nereids/AGENTS.md` | all read |
| `apache/doris#64392` | `zclllyybb/doris#38` | `27536266784` | `a5e37fba03375a9517f7a67e33f70d39` | `AGENTS.md`, `fe/AGENTS.md`, `fe/fe-core/AGENTS.md` | all read |
| `apache/doris#64489` | `zclllyybb/doris#39` | `27536275680` | `22ace18d70ece96b0ca7fa73123b62da` | `AGENTS.md`, `be/test/AGENTS.md` | all read |
| `apache/doris#64419` | `zclllyybb/doris#41` | `27538239601` | `1803c040e4140d61e840e4e1526190e7` | `AGENTS.md` | all read |

Concrete read commands observed in Litefuse included:

```text
cat AGENTS.md
cat fe/AGENTS.md
cat fe/fe-core/AGENTS.md
cat fe/fe-core/src/main/java/org/apache/doris/nereids/AGENTS.md
cat be/test/AGENTS.md
sed -n '1,260p' AGENTS.md
sed -n '1,260p' fe/AGENTS.md
sed -n '1,260p' fe/fe-core/AGENTS.md
```

All five final validation runs:

- had the expected paths in the first trace input prompt,
- had no prompt-missing paths,
- had actual command evidence that all expected paths were read,
- completed successfully in GitHub Actions,
- uploaded Litefuse trace data successfully.

### Exploratory Samples Not Counted

Two exploratory validation PRs were intentionally closed and excluded from the
final five-sample result:

- `zclllyybb/doris#35`, based on `apache/doris#64496`
- `zclllyybb/doris#40`, based on `apache/doris#64413`

Reason:

- The expected-path calculation used during the first audit was based on the
  current checkout's `AGENTS.md` inventory.
- Their historical validation checkout did not contain some module-level
  `AGENTS.md` files that exist in the current tree.
- The new helper correctly included only files present in the historical
  checkout, so those runs were useful for finding the historical-tree caveat but
  not suitable as "model skipped an existing required file" samples.

## Key Observations

1. The model follows explicit per-path prompt requirements much more reliably
   than a general instruction inside the code-review skill.
2. The required guide list should be generated by workflow code, not by model
   inference.
3. Required paths must be computed from the checked-out tree. Current-tree
   expectations can be wrong for historical PR replay.
4. Validation through `/review` is not sufficient for branch-only workflow
   changes because `issue_comment` uses default-branch workflow files.
5. Litefuse upload helpers also need to exist on the workflow ref used for
   branch-level validation.
6. Litefuse traces are the right evidence source because they show both:
   - the first model input prompt,
   - the actual shell commands executed by Codex.

## Reusable Validation Recipe

1. Pick source PRs with known missing `AGENTS.md` coverage from Litefuse.
2. Fetch each source PR head:

   ```bash
   git fetch upstream pull/<pr>/head:refs/remotes/upstream/pr-<pr>
   ```

3. Create base branch from the source PR's historical base SHA.
4. Copy in the current review runner, AGENTS helper, and Litefuse helper.
5. Create head branch by cherry-picking the source PR range onto that base.
6. Open a validation PR in `zclllyybb/doris` from head to base.
7. Run:

   ```bash
   gh workflow run code-review-runner.yml \
     --repo zclllyybb/doris \
     --ref <validation-base-branch> \
     -f pr_number=<validation-pr> \
     -f head_sha=<validation-head-sha> \
     -f base_sha=<validation-base-sha> \
     -f review_focus='Validation run for dynamic AGENTS prompt injection.'
   ```

8. Use the run log to extract Litefuse `trace_id`.
9. Verify both prompt content and actual read commands through Litefuse API.

## Current State After Validation

Open validation PRs kept for review:

- `zclllyybb/doris#36`
- `zclllyybb/doris#37`
- `zclllyybb/doris#38`
- `zclllyybb/doris#39`
- `zclllyybb/doris#41`

Exploratory PRs closed:

- `zclllyybb/doris#35`
- `zclllyybb/doris#40`

Local full result artifacts:

- `/tmp/litefuse_latest30_agents_analysis.json`
- `/tmp/doris_agents_validate_final_results.json`

