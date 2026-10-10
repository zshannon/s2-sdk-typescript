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
`@zshannon/streamstore/pki`. Use Bun for repository development; the temporary
ENG-716 Biscuit link is not an installable npm dependency.

Before the first PKI release, publish the standalone `@flickfyi/biscuit` package
with the changes from Crayon PR #336, replace the link in the core manifest
with that exact registry version, and regenerate the Bun and npm lockfiles.
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
