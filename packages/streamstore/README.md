# @zshannon/streamstore

TypeScript SDK for [S2](https://s2.dev), a serverless data store for streams.

This package is the core client for S2's [REST API](https://s2.dev/docs/rest/protocol), providing an ergonomic interface for appending to streams and reading data from them.

## Installation

```bash
npm add @zshannon/streamstore
# or
yarn add @zshannon/streamstore
# or
bun add @zshannon/streamstore
```

## Quick start

Generate an access token from the S2 console at <https://s2.dev/dashboard>, then:

<!-- snippet:start quick-start -->
```ts
import {
	AppendAck,
	AppendInput,
	AppendRecord,
	S2,
	S2Environment,
} from "@zshannon/streamstore";

const basinName = process.env.S2_BASIN ?? "my-existing-basin";
const streamName = process.env.S2_STREAM ?? "my-new-stream";

const s2 = new S2({
	...S2Environment.parse(),
	accessToken: process.env.S2_ACCESS_TOKEN ?? "my-access-token",
});

// Create a basin (namespace) client for basin-level operations.
const basin = s2.basin(basinName);

// Make a new stream within the basin, using the default configuration.
const streamResponse = await basin.streams.create({ stream: streamName });
console.dir(streamResponse, { depth: null });

// Create a stream client on our new stream.
const stream = basin.stream(streamName);

// Make a single append call.
const append: Promise<AppendAck> = stream.append(
	// `append` expects an input batch of one or many records.
	AppendInput.create([
		// Records can use a string encoding...
		AppendRecord.string({
			body: "Hello from the docs snippet!",
			headers: [["content-type", "text/plain"]],
		}),
		// ...or contain raw binary data.
		AppendRecord.bytes({
			body: new TextEncoder().encode("Bytes payload"),
		}),
	]),
);

// When the promise resolves, the data is fully durable and present on the stream.
const ack = await append;
console.log(
	`Appended records ${ack.start.seqNum} through ${ack.end.seqNum} (exclusive).`,
);
console.dir(ack, { depth: null });

// Read the two records back as binary.
const batch = await stream.read(
	{
		start: { from: { seqNum: ack.start.seqNum } },
		stop: { limits: { count: 2 } },
	},
	{ as: "bytes" },
);

for (const record of batch.records) {
	console.dir(record, { depth: null });
	console.log("decoded body: %s", new TextDecoder().decode(record.body));
}
```
<!-- snippet:end quick-start -->

> Tip: snippets look for `S2_ACCESS_TOKEN`, `S2_BASIN`, and `S2_STREAM` so you can run them with `npx tsx examples/<file>.ts`.

## More documentation

- Full SDK overview and additional examples: see the root repo README at <https://github.com/s2-streamstore/s2-sdk-typescript>.
- S2 service docs: <https://s2.dev/docs>.

## PKI v2 authentication

The PKI entry supports Node.js 20.19+ (including CommonJS), Bun, and browser
bundlers. Node 24 ESM/CommonJS and browser bundles are verified locally.

For an S2 Lite server configured with `S2LITE_PKI_ISSUER` and
`S2LITE_PKI_AUDIENCE`, import credentials from `@zshannon/streamstore/pki`:

```ts
import { S2 } from "@zshannon/streamstore";
import { ClientKey, Grant, PkiCredentials } from "@zshannon/streamstore/pki";

// The grant document is compatible with `s2 auth pki issue` / `delegate`.
const grant = Grant.fromJSON({ issuer: issuerPublicKey, grant: biscuitBase64url });
const key = ClientKey.fromBase58(clientPrivateKey);
const s2 = new S2({ pki: new PkiCredentials(key, grant) });
```

PKI replaces the access token entirely. Endpoints default to the grant's exact
HTTP audience; explicit endpoints must share that origin. Every request attempt
and session opening gets a fresh RFC 9421 P-256 proof. Unary proofs include a
SHA-256 digest of the serialized body; HTTP/2 append sessions sign their opening
in streaming mode. Redirects are rejected. The server evaluates cumulative native
Biscuit facts and checks, replay state, revocation and session leases.

Create and delegate grants locally:

```ts
import { delegateGrant, issueGrant } from "@zshannon/streamstore/pki";

const grant = issueGrant({
  audience: "http://localhost:8080",
  expiresAt: Math.floor(Date.now() / 1000) + 3600,
  holder: alice.publicKey,
  issuerKey: issuer,
  scope: {
    basins: { kind: "exact", value: "my-basin" },
    operations: ["Append", "Read"],
    streams: { kind: "prefix", value: "events/" },
  },
});
const bobGrant = delegateGrant({
  delegatorKey: alice,
  expiresAt: grant.expiresAt,
  holder: bob.publicKey,
  parent: grant,
  scope: { ...grant.scope, operations: ["Read"] },
});
await s2.revokeGrant(bobGrant.toBase64());
```

Here `issuer`, `alice` and `bob` are `ClientKey` instances. Delegation requires the
current holder's private key and can only narrow permissions, resource scopes
and expiry. `restrictGrant` uses the same signed delegation with the same holder.
`ClientKey.generate()` creates keys; explicit `toBase58()` exports private keys.
`Grant.toJSON()` exports the CLI-compatible grant document. Store private keys
securely. `accessTokens.list/issue/revoke` are unavailable in PKI mode.

The profile is v2 only. Native `pki_*` facts and five mandatory checks live in each
signed block; there is no JSON policy inside the Biscuit. The SDK verifies the
chain and supports the crate's bounded scalar custom checks, while conservatively
rejecting block-level global trust scopes. SDK admission checks the decoded AST;
the server additionally applies raw wire/schema and interned-symbol bounds.
Authorization remains on the server.

### Published Biscuit dependency

The generic `@flickfyi/biscuit@0.0.2` package is installed from npm. Its root export
supplies Biscuit cryptography and serialization; S2's profile and HTTP signatures
belong to this SDK. The SDK packages remain local workspaces for development.

```sh
bun install --frozen-lockfile
```

The published package includes two interoperability corrections: verified block
signing-algorithm metadata for stable revocation IDs, and schema version 6 for
authority, attenuation and third-party blocks (including native Reject checks
and null terms). The Bun and npm lockfiles pin the registry package, so clean CI
runners and npm consumers do not need a Crayon checkout or a global Bun link.

Run focused regressions against the actual Rust CLI and an isolated in-memory
Lite server with:

```sh
S2_PKI_RUST_BIN=/path/to/s2 bun run test:core -- \
  src/tests/pki-v2.test.ts src/tests/pki-v2-rust.integration.test.ts \
  src/tests/pki-biscuit-link.test.ts
```
