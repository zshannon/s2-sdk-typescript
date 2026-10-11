import { execFileSync, spawnSync } from 'node:child_process'
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { runInNewContext } from 'node:vm'
import { expect, test } from 'vitest'

test('upstream PR tells maintainers how to approve CI and handle its read-only head', async () => {
  const workflow = readFileSync(new URL('../.github/workflows/sync-upstream.yml', import.meta.url), 'utf8')
  const script = workflow.split('          script: |\n')[1].replace(/^            /gm, '')
  let body = ''
  await runInNewContext(`(async () => { ${script}\n })()`, {
    context: { repo: { owner: 'zshannon', repo: 's2-sdk-typescript' } },
    core: { info() {} },
    github: {
      paginate: async () => [],
      rest: {
        pulls: {
          create: async (request: { body: string; head: string; maintainer_can_modify: boolean }) => {
            expect(request.head).toBe('s2-streamstore:main')
            expect(request.maintainer_can_modify).toBe(false)
            body = request.body
            return { data: { html_url: 'https://example.test/pr/1' } }
          },
          list() {},
        },
        repos: { compareCommitsWithBasehead: async () => ({ data: { ahead_by: 1 } }) },
      },
    },
  })
  expect(body).toContain('Approve workflows to run')
  expect(body).toContain('DEVELOPMENT.md#conflicting-upstream-changes')
})

test('documented conflict recipe preserves fork main and upstream ancestry on a writable branch', () => {
  const documentation = readFileSync(new URL('../DEVELOPMENT.md', import.meta.url), 'utf8')
  const section = documentation.split('### Conflicting upstream changes\n')[1]
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
  const integration = join(directory, 's2-sdk-upstream')
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

    const merge = spawnSync('/bin/sh', ['-eu', '-c', blocks[0]], { cwd: checkout, encoding: 'utf8', env: environment })
    expect(merge.status, merge.stderr).toBe(1)
    expect(merge.stdout).toContain('CONFLICT')
    expect(git(integration, 'branch', '--show-current')).toBe('sync/upstream-main')
    expect(git(checkout, 'rev-parse', 'main')).toBe(forkMain)
    expect(git(upstream, 'rev-parse', 'main')).toBe(upstreamMain)
    writeFileSync(join(integration, 'package.json'), '{"name":"@zshannon/streamstore","dependencies":{"@flickfyi/biscuit":"0.0.2"}}\n')
    git(integration, 'add', 'package.json')
    git(integration, 'commit', '--no-edit')
    expect(git(integration, 'merge-base', '--is-ancestor', forkMain, 'HEAD')).toBe('')
    expect(git(integration, 'merge-base', '--is-ancestor', upstreamMain, 'HEAD')).toBe('')
    expect(readFileSync(join(integration, 'pki.ts'), 'utf8')).toContain('pki = true')
    expect(readFileSync(join(integration, 'feature.ts'), 'utf8')).toContain('upstreamFeature = true')
    execFileSync('/bin/sh', ['-eu', '-c', blocks[1]], { cwd: integration, env: environment, stdio: ['ignore', 'pipe', 'pipe'] })
    expect(git(fork, 'rev-parse', 'main')).toBe(forkMain)
    expect(git(fork, 'rev-parse', 'sync/upstream-main')).toBe(git(integration, 'rev-parse', 'HEAD'))
    expect(git(integration, 'rev-list', '--count', 'HEAD..upstream/main')).toBe('0')
    expect(git(checkout, 'status', '--porcelain')).toBe('')
  } finally {
    rmSync(directory, { force: true, recursive: true })
  }
})
