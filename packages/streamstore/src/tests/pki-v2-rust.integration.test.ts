import { type ChildProcess, execFileSync, spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	Biscuit,
	BlockBuilder,
	type Check,
	type Fact,
	PrivateKey,
	PublicKey,
} from "@flickfyi/biscuit";
import { base64urlnopad } from "@scure/base";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { AppendInput, AppendRecord, S2 } from "../index.js";
import {
	ClientKey,
	delegateGrant,
	Grant,
	issueGrant,
	PKI_OPERATIONS,
	PkiCredentials,
} from "../pki/index.js";

const binary = process.env.S2_PKI_RUST_BIN;
const root = ClientKey.fromBytes(
	Uint8Array.from({ length: 32 }, (_, i) => (i === 31 ? 1 : 0)),
);
const alice = ClientKey.fromBytes(
	Uint8Array.from({ length: 32 }, (_, i) => (i === 31 ? 2 : 0)),
);
const bob = ClientKey.fromBytes(
	Uint8Array.from({ length: 32 }, (_, i) => (i === 31 ? 3 : 0)),
);
const expiresAt = 2_000_000_000;
const fullScope = {
	basins: { kind: "any" },
	operations: PKI_OPERATIONS,
	streams: { kind: "any" },
} as const;

describe.skipIf(!binary)(
	"PKI v2 interoperability with the actual Rust crate and Lite",
	() => {
		let directory: string;
		let server: ChildProcess;
		let audience: string;
		let s2: S2;
		let credentials: PkiCredentials;
		let rootGrant: Grant;
		beforeAll(async () => {
			directory = mkdtempSync(join(tmpdir(), "s2-pki-interop-"));
			const listener = createServer();
			listener.listen(0, "127.0.0.1");
			await once(listener, "listening");
			const port = (listener.address() as { port: number }).port;
			await new Promise<void>((resolve) => listener.close(() => resolve()));
			audience = `http://127.0.0.1:${port}`;
			rootGrant = issueGrant({
				audience,
				expiresAt,
				holder: alice.publicKey,
				issuerKey: root,
				scope: fullScope,
			});
			credentials = new PkiCredentials(alice, rootGrant);
			server = spawn(binary!, ["lite", "--port", String(port)], {
				env: {
					...process.env,
					S2LITE_PKI_ISSUER: root.publicKey,
					S2LITE_PKI_AUDIENCE: audience,
				},
				stdio: ["ignore", "ignore", "pipe"],
			});
			let error = "";
			server.stderr!.on("data", (chunk) => {
				error += String(chunk).slice(0, 2048);
			});
			const deadline = Date.now() + 15_000;
			for (;;) {
				if (server.exitCode !== null)
					throw new Error(`Rust Lite exited: ${error}`);
				try {
					if ((await fetch(audience + "/health")).ok) break;
				} catch {}
				if (Date.now() > deadline)
					throw new Error(`Rust Lite readiness timed out: ${error}`);
				await new Promise((resolve) => setTimeout(resolve, 25));
			}
			s2 = new S2({ pki: credentials, retry: { maxAttempts: 1 } });
			await s2.basins.create({ basin: "pki-test" });
			await s2.basin("pki-test").streams.create({ stream: "events/unary" });
		}, 20_000);
		afterAll(async () => {
			if (server && server.exitCode === null) {
				const exit = once(server, "exit");
				server.kill("SIGTERM");
				await exit;
			}
			if (directory) rmSync(directory, { recursive: true, force: true });
		});
		it("Rust verifies TypeScript issuance and two holder-signed delegation hops", () => {
			const first = delegateGrant({
				delegatorKey: alice,
				expiresAt,
				holder: bob.publicKey,
				parent: rootGrant,
				scope: {
					basins: { kind: "exact", value: "pki-test" },
					operations: ["Read"],
					streams: { kind: "prefix", value: "events/" },
				},
			});
			const second = delegateGrant({
				delegatorKey: bob,
				expiresAt: expiresAt - 1,
				holder: alice.publicKey,
				parent: first,
				scope: first.scope,
			});
			const path = join(directory, "typescript.grant.json");
			writeFileSync(path, JSON.stringify(second.toJSON()), { mode: 0o600 });
			const inspected = JSON.parse(
				execFileSync(
					binary!,
					["auth", "pki", "inspect", "--grant-file", path],
					{ encoding: "utf8" },
				),
			);
			expect(inspected.client).toBe(alice.publicKey);
			expect(inspected.scope.operations).toEqual([16]);
		});
		it("TypeScript verifies and delegates a Rust-issued grant with Ed25519 internal blocks", () => {
			const source = JSON.parse(
				readFileSync(
					new URL("./fixtures/pki-v2-rust.json", import.meta.url),
					"utf8",
				),
			);
			const imported = Grant.fromJSON(source);
			const child = delegateGrant({
				delegatorKey: alice,
				expiresAt,
				holder: bob.publicKey,
				parent: imported,
				scope: imported.scope,
			});
			const grandchild = delegateGrant({
				delegatorKey: bob,
				expiresAt,
				holder: alice.publicKey,
				parent: child,
				scope: child.scope,
			});
			const path = join(directory, "mixed.grant.json");
			writeFileSync(path, JSON.stringify(grandchild.toJSON()), { mode: 0o600 });
			const inspected = JSON.parse(
				execFileSync(
					binary!,
					["auth", "pki", "inspect", "--grant-file", path],
					{ encoding: "utf8" },
				),
			);
			expect(inspected.client).toBe(alice.publicKey);
		});
		it("loads a Rust-signed delegation of a TypeScript authority", () => {
			const parentPath = join(directory, "reverse-parent.json"),
				keyPath = join(directory, "alice.key"),
				scopePath = join(directory, "read-scope.json"),
				outPath = join(directory, "reverse-child.json");
			writeFileSync(parentPath, JSON.stringify(rootGrant.toJSON()));
			writeFileSync(keyPath, alice.toBase58(), { mode: 0o600 });
			writeFileSync(
				scopePath,
				JSON.stringify({
					basins: { Exact: "pki-test" },
					operations: [16],
					streams: { Prefix: "events/" },
				}),
			);
			execFileSync(binary!, [
				"auth",
				"pki",
				"delegate",
				"--key-file",
				keyPath,
				"--grant-file",
				parentPath,
				"--client-key",
				bob.publicKey,
				"--scope-file",
				scopePath,
				"--expires-at",
				String(expiresAt),
				"--out",
				outPath,
			]);
			const child = Grant.fromJSON(JSON.parse(readFileSync(outPath, "utf8")));
			expect(child.holder).toBe(bob.publicKey);
			expect(child.scope.operations).toEqual(["Read"]);
			expect(child.revocationIds[0]).toEqual(rootGrant.revocationIds[0]);
		});

		it.each([
			["authority", "null"],
			["authority", "reject"],
			["delegated", "null"],
			["delegated", "reject"],
		] as const)(
			"Rust accepts schema-6 %s %s grants and preserves their restrictions",
			async (mode, feature) => {
				const parse = (grant: Grant) =>
					Biscuit.fromBytes(
						base64urlnopad.decode(grant.toBase64()),
						PublicKey.fromBase58(root.publicKey, "secp256r1"),
					);
				const parent = parse(rootGrant);
				const template =
					mode === "authority"
						? parent
						: parse(
								delegateGrant({
									delegatorKey: alice,
									expiresAt,
									holder: bob.publicKey,
									parent: rootGrant,
									scope: fullScope,
								}),
							);
				const build = (facts: Fact[], checks: Check[]) => {
					const index = mode === "authority" ? 0 : 1;
					facts.push(...template.blockFacts(index));
					checks.push(...template.blockChecks(index));
					if (feature === "null")
						facts.push({
							predicate: { name: "nullable", terms: [{ type: "null" }] },
						});
					if (feature === "reject")
						checks.push({
							kind: "reject",
							queries: [
								{
									body: [
										{
											name: "operation",
											terms: [{ type: "string", value: "Append" }],
										},
									],
									expressions: [],
									head: { name: "query", terms: [] },
									scope: [],
								},
							],
						});
				};
				const token =
					mode === "authority"
						? Biscuit.create(
								PrivateKey.fromBytes(root.toBytes(), "secp256r1"),
								build,
							)
						: parent.appendThirdPartyBlock({
								block: parent.thirdPartyRequest().createBlock({
									build: ({ checks, facts }) => build(facts, checks),
									thirdPartyKey: PrivateKey.fromBytes(
										alice.toBytes(),
										"secp256r1",
									),
								}),
							});
				const grant = Grant.fromBase64(
					base64urlnopad.encode(token.toBytes()),
					root.publicKey,
				);
				const path = join(directory, `schema6-${mode}-${feature}.json`);
				writeFileSync(path, JSON.stringify(grant.toJSON()));
				expect(
					JSON.parse(
						execFileSync(
							binary!,
							["auth", "pki", "inspect", "--grant-file", path],
							{ encoding: "utf8" },
						),
					).client,
				).toBe(grant.holder);
				const holder = mode === "authority" ? alice : bob;
				const child = delegateGrant({
					delegatorKey: holder,
					expiresAt,
					holder: holder.publicKey,
					parent: grant,
					scope: fullScope,
				});
				const client = new S2({
					pki: new PkiCredentials(holder, child),
					retry: { maxAttempts: 1 },
				});
				const stream = client.basin("pki-test").stream("events/unary");
				try {
					await stream.checkTail();
					if (feature === "reject")
						await expect(
							stream.append(
								AppendInput.create([
									AppendRecord.string({ body: "ancestor reject must deny" }),
								]),
							),
						).rejects.toMatchObject({ status: 403 });
				} finally {
					await stream.close();
				}
			},
		);
		it("accepts the same native null fixture as the Rust verifier", () => {
			const source = JSON.parse(
				readFileSync(
					new URL("./fixtures/pki-v2-null.json", import.meta.url),
					"utf8",
				),
			);
			const grant = Grant.fromJSON(source);
			const path = join(directory, "native-null.json");
			writeFileSync(path, JSON.stringify(grant.toJSON()));
			const inspected = JSON.parse(
				execFileSync(
					binary!,
					["auth", "pki", "inspect", "--grant-file", path],
					{ encoding: "utf8" },
				),
			);
			expect(inspected.client).toBe(grant.holder);
		});
		it.each(["authority", "delegation"] as const)(
			"Rust accepts TypeScript-generated %s Reject/null checks and enforces them after delegation",
			async (placement) => {
				const original = Biscuit.fromBytes(
					base64urlnopad.decode(rootGrant.toBase64()),
					PublicKey.fromBase58(root.publicKey, "secp256r1"),
				);
				const custom = new BlockBuilder().fact("nullable", null).addCheck({
					kind: "reject",
					queries: [
						{
							body: [
								{
									name: "operation",
									terms: [{ type: "string", value: "Append" }],
								},
							],
							expressions: [],
							head: { name: "query", terms: [] },
							scope: [],
						},
					],
				});
				let token: Biscuit;
				if (placement === "authority") {
					token = Biscuit.create(
						PrivateKey.fromBytes(root.toBytes(), "secp256r1"),
						(facts, checks) => {
							facts.push(...original.blockFacts(0), ...custom.facts);
							checks.push(...original.blockChecks(0), ...custom.checks);
						},
					);
				} else {
					const template = delegateGrant({
						delegatorKey: alice,
						expiresAt,
						holder: bob.publicKey,
						parent: rootGrant,
						scope: fullScope,
					});
					const next = Biscuit.fromBytes(
						base64urlnopad.decode(template.toBase64()),
						PublicKey.fromBase58(root.publicKey, "secp256r1"),
					);
					const block = original.thirdPartyRequest().createBlock({
						build({ checks, facts }) {
							facts.push(...next.blockFacts(1), ...custom.facts);
							checks.push(...next.blockChecks(1), ...custom.checks);
						},
						thirdPartyKey: PrivateKey.fromBytes(alice.toBytes(), "secp256r1"),
					});
					token = original.appendThirdPartyBlock({ block });
				}
				const parent = Grant.fromBase64(
					base64urlnopad.encode(token.toBytes()),
					root.publicKey,
				);
				const child = delegateGrant({
					delegatorKey: placement === "authority" ? alice : bob,
					expiresAt: expiresAt - 1,
					holder: alice.publicKey,
					parent,
					scope: fullScope,
				});
				const path = join(directory, `native-${placement}-reject-null.json`);
				writeFileSync(path, JSON.stringify(child.toJSON()));
				const inspected = JSON.parse(
					execFileSync(
						binary!,
						["auth", "pki", "inspect", "--grant-file", path],
						{ encoding: "utf8" },
					),
				);
				expect(inspected.client).toBe(alice.publicKey);
				const client = new S2({
					pki: new PkiCredentials(alice, child),
					retry: { maxAttempts: 1 },
				});
				const stream = client.basin("pki-test").stream("events/unary");
				try {
					await stream.checkTail();
					await expect(
						stream.append(
							AppendInput.create([AppendRecord.string({ body: "denied" })]),
						),
					).rejects.toMatchObject({ status: 403 });
				} finally {
					await stream.close();
				}
			},
		);
		it("authenticates management and unary read/append requests", async () => {
			expect((await s2.basins.list()).basins.map((b) => b.name)).toContain(
				"pki-test",
			);
			const stream = s2.basin("pki-test").stream("events/unary");
			try {
				await stream.append(
					AppendInput.create([AppendRecord.string({ body: "unary" })]),
				);
				const batch = await stream.read({
					start: { from: { seqNum: 0 } },
					stop: { limits: { count: 1 } },
				});
				expect(batch.records[0]?.body).toBe("unary");
			} finally {
				await stream.close();
			}
		});
		it.each(["fetch", "s2s"] as const)(
			"authenticates append and read sessions over %s",
			async (forceTransport) => {
				const name = "events/" + forceTransport;
				await s2.basin("pki-test").streams.create({ stream: name });
				const stream = s2.basin("pki-test").stream(name, { forceTransport });
				try {
					const append = await stream.appendSession();
					try {
						const ticket = await append.submit(
							AppendInput.create([
								AppendRecord.string({ body: forceTransport }),
							]),
						);
						expect((await ticket.ack()).end.seqNum).toBe(1);
					} finally {
						await append.close();
					}
					const read = await stream.readSession({
						start: { from: { seqNum: 0 } },
						stop: { limits: { count: 1 } },
					});
					try {
						const reader = read.getReader();
						try {
							expect((await reader.read()).value?.body).toBe(forceTransport);
						} finally {
							reader.releaseLock();
						}
					} finally {
						await read.cancel();
					}
				} finally {
					await stream.close();
				}
			},
			15_000,
		);
		it("rejects replay and tampered URI, resource header, body, and injected s2 header", async () => {
			const proof = await credentials.signRequest(
				new Request(audience + "/v1/basins"),
			);
			expect((await fetch(proof.clone())).ok).toBe(true);
			expect((await fetch(proof.clone())).status).toBe(401);
			for (const mutation of ["query", "basin", "body", "s2-header"]) {
				const signed = await credentials.signRequest(
					new Request(audience + "/v1/basins", {
						method: "POST",
						headers: { "content-type": "application/json" },
						body: JSON.stringify({ basin: "tampered" }),
					}),
				);
				const headers = new Headers(signed.headers);
				let url = signed.url;
				let body = await signed.text();
				if (mutation === "query") url += "?injected=1";
				if (mutation === "basin") headers.set("s2-basin", "different");
				if (mutation === "body") body = JSON.stringify({ basin: "different" });
				if (mutation === "s2-header") headers.set("s2-injected", "yes");
				expect(
					(await fetch(url, { body, headers, method: "POST" })).status,
				).toBe(401);
			}
		});
		it("enforces the delegated operation and resource scopes, then durable revocation", async () => {
			const child = delegateGrant({
				delegatorKey: alice,
				expiresAt,
				holder: bob.publicKey,
				parent: rootGrant,
				scope: {
					basins: { kind: "exact", value: "pki-test" },
					operations: ["Read"],
					streams: { kind: "exact", value: "events/unary" },
				},
			});
			const delegated = new S2({
				pki: new PkiCredentials(bob, child),
				retry: { maxAttempts: 1 },
			});
			const allowed = delegated.basin("pki-test").stream("events/unary");
			try {
				expect(
					(
						await allowed.read({
							start: { from: { seqNum: 0 } },
							stop: { limits: { count: 1 } },
						})
					).records[0]?.body,
				).toBe("unary");
				await expect(
					allowed.append(
						AppendInput.create([AppendRecord.string({ body: "denied" })]),
					),
				).rejects.toMatchObject({ status: 403 });
				const denied = delegated.basin("pki-test").stream("events/other");
				try {
					await expect(denied.read()).rejects.toMatchObject({ status: 403 });
				} finally {
					await denied.close();
				}
				await s2.revokeGrant(child.toBase64());
				await expect(allowed.read()).rejects.toMatchObject({ status: 401 });
			} finally {
				await allowed.close();
			}
		});
	},
);
