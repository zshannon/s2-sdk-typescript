# Development

Internal notes for maintainers.

## Development setup

Install the pre-commit hook to automatically format staged files before each commit:

```sh
ln -sf "$(pwd)/scripts/pre-commit" .git/hooks/pre-commit
```

This runs `biome check --write` on staged files. If biome finds issues it cannot fix, the commit will be blocked.

## Maintaining documentation snippets

- Run `bun run snippets` whenever you touch `README.md` or the snippet source files under `examples/`.
- `bun run check:snippets` (also part of `bun run check`) type-checks every example so regressions are caught in CI.
- Snippet blocks in markdown are delimited by `<!-- snippet:start NAME -->` / `<!-- snippet:end NAME -->`; never edit the generated code directly – update the matching file in `examples/` instead.
- To keep snippets small, add region markers to example files: `snippet-region REGION start` / `snippet-region REGION end`.

## Fork releases to npm

The fork publishes `@zshannon/streamstore`, `@zshannon/streamstore-patterns`
and `@zshannon/resumable-stream` as public packages. The core PKI entry is
`@zshannon/streamstore/pki`. Use Bun for repository development; the SDK packages
remain local workspaces. The core uses the published `@flickfyi/biscuit@0.0.2`
package, including the schema-6 and verified-signature metadata corrections.
The Bun and npm lockfiles record its registry dependency and integrity.
`bun run release:check` and each package's `prepublishOnly` hook reject local
links and upstream scopes before anything is uploaded.

Configure npm trusted publishing for each package with GitHub user `zshannon`,
repository `s2-sdk-typescript`, workflow filename `publish.yml`, no environment,
and permission to publish directly. The core package already exists. The two
new add-on packages need their first authenticated publish before npm permits
registering a trusted publisher. With npm >= 11.15 and an authenticated account:

```sh
npm trust github @zshannon/streamstore --repo zshannon/s2-sdk-typescript --file publish.yml --allow-publish
```

The same command applies to each add-on after its first publish. npm may request
interactive account verification. Trusted publishers require npm-side setup;
`id-token: write` alone does not register the repository.

The existing Changesets workflow runs on `main` and can be dispatched manually
on `main`. Changesets opens a version PR for pending changes, then publishes
after that version PR merges. Builds, checks and local tests run before the
publish step. Node 24 supplies npm with OIDC support, so no npm write token is
stored in GitHub. The initial PKI changeset is included in this branch.

## Weekly upstream checks

`.github/workflows/sync-upstream.yml` checks `s2-streamstore/s2-sdk-typescript:main`
every Tuesday at 09:00 America/Los_Angeles, matching the S2 fork's weekly check.
It can also be dispatched manually on `main`. When upstream has new commits,
the workflow opens one PR directly from upstream main, or reuses the existing
open PR. It does not reset the fork, merge changes, or publish packages.

Review the PR, including workflow changes. GitHub creates approval-required CI
runs for PRs opened or updated with `GITHUB_TOKEN`. A maintainer with write access
must select **Approve workflows to run** in the PR merge box when prompted, then
wait for all required checks to pass. See
[GitHub's token event rules](https://docs.github.com/en/actions/concepts/security/github_token#when-github_token-triggers-workflow-runs).
Keep the fork's PKI behavior, published Biscuit dependency, `@zshannon`
package names, and release configuration. Merge upstream PRs with a merge commit
to preserve ancestry and avoid presenting the same upstream commits again. The
check uses `GITHUB_TOKEN` with read access to contents and write access to PRs;
it adds no external token or npm permission.

### Conflicting upstream changes

The automated PR's head is upstream `main`, which fork maintainers cannot edit.
Resolve conflicts on a new branch in this fork and open a replacement integration
PR. Use a separate worktree to preserve the main checkout. With `origin` pointing
to `zshannon/s2-sdk-typescript` and `upstream` to `s2-streamstore/s2-sdk-typescript`,
choose an unused branch name and worktree path:

```sh
git fetch origin main
git fetch upstream main
git worktree add -b sync/upstream-main ../s2-sdk-upstream origin/main
cd ../s2-sdk-upstream
git merge --no-ff upstream/main
```

Resolve the conflicts while retaining the fork behavior and package names above.
Stage the resolved files and complete the merge with `git commit`, then push the
integration branch to the fork:

```sh
git push -u origin sync/upstream-main
```

Open a PR from `sync/upstream-main` to this fork's `main` and link it from the
automated PR. Keep the automated PR open until the integration PR merges, so the
weekly check reuses the tracking PR. Review and pass required CI on the integration
PR, merge it with a merge commit, then close the tracking PR. Both histories remain
ancestors of the result; neither main branch is reset or pushed during preparation.
