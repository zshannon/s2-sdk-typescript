import { execFileSync, spawnSync } from 'node:child_process'
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { join } from 'node:path'
import { runInNewContext } from 'node:vm'
import { expect, test } from 'vitest'

test('upstream PR provides an editable draft and manual review instructions', async () => {
  const candidate = 'a'.repeat(40)
  const workflow = readFileSync(new URL('../.github/workflows/sync-upstream.yml', import.meta.url), 'utf8')
  const script = workflow.split('          script: |\n')[1].replace(/^            /gm, '')
  let body = ''
  await runInNewContext(`(async () => { ${script}\n })()`, {
    context: { repo: { owner: 'zshannon', repo: 's2-sdk-typescript' } },
    core: { info() {} },
    github: {
      paginate: async () => [],
      rest: {
        git: {
          createRef: async (request: { ref: string; sha: string }) => {
            expect(request.ref).toBe(`refs/heads/automation/upstream-${candidate}`)
            expect(request.sha).toBe(candidate)
          },
          getRef: async () => { throw Object.assign(new Error('Not Found'), { status: 404 }) },
        },
        pulls: {
          create: async (request: { body: string; draft: boolean; head: string; maintainer_can_modify: boolean }) => {
            expect(request.draft).toBe(true)
            expect(request.head).toBe(`zshannon:automation/upstream-${candidate}`)
            expect(request.maintainer_can_modify).toBe(true)
            body = request.body
            return { data: { html_url: 'https://example.test/pr/1' } }
          },
          list() {},
        },
        repos: {
          compareCommitsWithBasehead: async () => ({ data: { ahead_by: 1 } }),
          getBranch: async ({ owner }: { owner: string }) => ({ data: { commit: { sha: owner === 's2-streamstore' ? candidate : 'b'.repeat(40) } } }),
        },
      },
    },
    require: createRequire(new URL('../package.json', import.meta.url)),
  })
  expect(body).toContain('Approve workflows to run')
  expect(body).toContain('Resolve conflicts on this branch')
  expect(body).toContain('merge commit')
  expect(body).toContain('published Biscuit dependency')
  expect(body).toContain('@zshannon package names')
})

test('documented conflict recipe preserves fork main and upstream ancestry on a writable branch', () => {
  const documentation = readFileSync(new URL('../.github/UPSTREAM_SYNC.md', import.meta.url), 'utf8')
  const section = documentation.split('## Resolve and merge manually\n')[1]?.split('## Local regression tests')[0]
  expect(section, 'maintainers need a supported conflict-resolution recipe').toBeDefined()
  const blocks = [...section.matchAll(/```sh\n([\s\S]*?)```/g)].map(match => match[1])
  expect(blocks).toHaveLength(2)

  const cache = join(process.cwd(), 'node_modules/.cache')
  mkdirSync(cache, { recursive: true })
  const directory = mkdtempSync(join(cache, 'upstream-sync-'))
  const environment = {
    ...process.env,
    GIT_CONFIG_COUNT: '3',
    GIT_CONFIG_KEY_0: 'commit.gpgsign',
    GIT_CONFIG_KEY_1: 'user.email',
    GIT_CONFIG_KEY_2: 'user.name',
    GIT_CONFIG_VALUE_0: 'false',
    GIT_CONFIG_VALUE_1: 'upstream-sync@example.test',
    GIT_CONFIG_VALUE_2: 'Upstream sync fixture',
  }
  const git = (cwd: string, ...args: string[]) => execFileSync('git', args, { cwd, env: environment, stdio: ['ignore', 'pipe', 'pipe'] }).toString().trim()
  const upstream = join(directory, 'upstream')
  const fork = join(directory, 'fork.git')
  const checkout = join(directory, 'checkout')
  const integration = join(directory, 'upstream-integration')
  try {
    git(directory, 'init', '--initial-branch=main', upstream)
    writeFileSync(join(upstream, 'package.json'), '{"name":"@s2-dev/streamstore"}\n')
    git(upstream, 'add', '.')
    git(upstream, 'commit', '-m', 'Common base')
    git(directory, 'clone', '--bare', upstream, fork)
    git(directory, 'clone', fork, checkout)
    git(checkout, 'remote', 'add', 'upstream', upstream)
    writeFileSync(join(checkout, 'package.json'), '{"name":"@zshannon/streamstore","dependencies":{"@flickfyi/biscuit":"0.0.2"}}\n')
    writeFileSync(join(checkout, 'pki.ts'), 'export const pki = true\n')
    git(checkout, 'add', '.')
    git(checkout, 'commit', '-m', 'Fork PKI and package scope')
    git(checkout, 'push', 'origin', 'main')
    const forkMain = git(checkout, 'rev-parse', 'main')
    writeFileSync(join(upstream, 'package.json'), '{"name":"@s2-dev/renamed-streamstore"}\n')
    writeFileSync(join(upstream, 'feature.ts'), 'export const upstreamFeature = true\n')
    git(upstream, 'add', '.')
    git(upstream, 'commit', '-m', 'Conflicting upstream update')
    const upstreamMain = git(upstream, 'rev-parse', 'main')
    const proposalBranch = `automation/upstream-${upstreamMain}`
    git(checkout, 'fetch', 'upstream', 'main')
    git(checkout, 'push', 'origin', `${upstreamMain}:refs/heads/${proposalBranch}`)

    const merge = spawnSync('/bin/sh', ['-eu', '-c', blocks[0].replaceAll('REPLACE_WITH_SHA', upstreamMain)], { cwd: checkout, encoding: 'utf8', env: environment })
    expect(merge.status, merge.stderr).toBe(1)
    expect(merge.stdout).toContain('CONFLICT')
    expect(git(integration, 'branch', '--show-current')).toBe('sync/upstream-review')
    expect(git(checkout, 'rev-parse', 'main')).toBe(forkMain)
    expect(git(upstream, 'rev-parse', 'main')).toBe(upstreamMain)
    writeFileSync(join(integration, 'package.json'), '{"name":"@zshannon/streamstore","dependencies":{"@flickfyi/biscuit":"0.0.2"}}\n')
    git(integration, 'add', 'package.json')
    git(integration, 'commit', '--no-edit')
    expect(git(integration, 'merge-base', '--is-ancestor', forkMain, 'HEAD')).toBe('')
    expect(git(integration, 'merge-base', '--is-ancestor', upstreamMain, 'HEAD')).toBe('')
    expect(readFileSync(join(integration, 'pki.ts'), 'utf8')).toContain('pki = true')
    expect(readFileSync(join(integration, 'feature.ts'), 'utf8')).toContain('upstreamFeature = true')
    execFileSync('/bin/sh', ['-eu', '-c', blocks[1].replaceAll('REPLACE_WITH_SHA', upstreamMain)], { cwd: integration, env: environment, stdio: ['ignore', 'pipe', 'pipe'] })
    expect(git(fork, 'rev-parse', 'main')).toBe(forkMain)
    expect(git(fork, 'rev-parse', proposalBranch)).toBe(git(integration, 'rev-parse', 'HEAD'))
    expect(git(integration, 'rev-list', '--count', 'HEAD..upstream/main')).toBe('0')
    expect(git(checkout, 'status', '--porcelain')).toBe('')
  } finally {
    rmSync(directory, { force: true, recursive: true })
  }
})
