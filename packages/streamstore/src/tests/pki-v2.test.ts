import { readFileSync } from "node:fs";
import {
	Biscuit,
	BlockBuilder,
	type Check,
	PrivateKey,
	PublicKey,
} from "@flickfyi/biscuit";
import { p256 } from "@noble/curves/nist.js";
import { sha256 } from "@noble/hashes/sha2.js";
import { base64, base64urlnopad } from "@scure/base";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createClient, createConfig } from "../generated/client/index.js";
import { configurePkiClient } from "../internal/pki-client.js";
import {
	ClientKey,
	delegateGrant,
	Grant,
	issueGrant,
	PkiCredentials,
	restrictGrant,
} from "../pki/index.js";
import { S2 } from "../s2.js";

const audience = "http://localhost:8765";
const expiresAt = 2_000_000_000;
const scope = {
	basins: { kind: "exact", value: "test-basin" },
	operations: ["Append", "Read", "ListStreams"],
	streams: { kind: "prefix", value: "events/" },
} as const;
function setup() {
	const issuer = ClientKey.generate();
	const alice = ClientKey.generate();
	const grant = issueGrant({
		audience,
		expiresAt,
		holder: alice.publicKey,
		issuerKey: issuer,
		scope,
	});
	return {
		alice,
		credentials: new PkiCredentials(alice, grant),
		grant,
		issuer,
	};
}
afterEach(() => vi.unstubAllGlobals());

describe("PKI v2 grants", () => {
	it("accepts the native null scalar in a Rust-compatible v6 grant", () => {
		const document = JSON.parse(
			readFileSync(
				new URL("./fixtures/pki-v2-null.json", import.meta.url),
				"utf8",
			),
		);
		expect(Grant.fromJSON(document).toJSON()).toEqual(document);
	});
	it("rejects aggregate policy materialization work exceeding Rust's limit", () => {
		const { grant, issuer } = setup();
		const original = Biscuit.fromBytes(
			base64urlnopad.decode(grant.toBase64()),
			PublicKey.fromBase58(issuer.publicKey, "secp256r1"),
		);
		const token = Biscuit.create(
			PrivateKey.fromBytes(issuer.toBytes(), "secp256r1"),
			(facts, checks) => {
				facts.push(
					...original.blockFacts(0),
					...Array.from({ length: 500 }, () => ({
						predicate: {
							name: "extra",
							terms: Array.from({ length: 8 }, () => ({
								type: "integer" as const,
								value: 0n,
							})),
						},
					})),
				);
				checks.push(...original.blockChecks(0));
			},
		);
		expect(() =>
			Grant.fromBase64(
				base64urlnopad.encode(token.toBytes()),
				issuer.publicKey,
			),
		).toThrow(/budget/);
	});

	it.each<Check>([
		{
			kind: "one",
			queries: [
				{
					head: { name: "query", terms: [] },
					body: [],
					scope: [
						{ type: "authority" },
						{ type: "authority" },
						{ type: "authority" },
					],
					expressions: [],
				},
			],
		},
		{
			kind: "one",
			queries: [
				{
					head: { name: "query", terms: [] },
					body: [],
					scope: [],
					expressions: Array.from({ length: 17 }, () => []),
				},
			],
		},
		{ kind: "one", queries: [] },
		{ kind: "all", queries: [] },
		{ kind: "reject", queries: [] },
		{
			kind: "one",
			queries: [
				{
					head: { name: "query", terms: [] },
					body: [],
					scope: [],
					expressions: [
						[
							{ type: "value", term: { type: "string", value: "events/x" } },
							{ type: "value", term: { type: "string", value: "events" } },
							{ type: "binary", kind: 5 },
						],
					],
				},
			],
		},
	])(
		"rejects custom checks outside the Rust admission profile: %j",
		(extra) => {
			const { grant, issuer } = setup();
			const original = Biscuit.fromBytes(
				base64urlnopad.decode(grant.toBase64()),
				PublicKey.fromBase58(issuer.publicKey, "secp256r1"),
			);
			const token = Biscuit.create(
				PrivateKey.fromBytes(issuer.toBytes(), "secp256r1"),
				(facts, checks) => {
					facts.push(...original.blockFacts(0));
					checks.push(...original.blockChecks(0), extra);
				},
			);
			expect(() =>
				Grant.fromBase64(
					base64urlnopad.encode(token.toBytes()),
					issuer.publicKey,
				),
			).toThrow();
		},
	);

	it("uses only native indexed facts and the five cumulative checks", () => {
		const { grant, issuer } = setup();
		const biscuit = Biscuit.fromBytes(
			base64urlnopad.decode(grant.toBase64()),
			PublicKey.fromBase58(issuer.publicKey, "secp256r1"),
		);
		expect(biscuit.blockFacts(0).map((f) => f.predicate.name)).toContain(
			"pki_version",
		);
		expect(biscuit.blockFacts(0).map((f) => f.predicate.name)).not.toContain(
			"scope",
		);
		expect(biscuit.blockChecks(0)).toHaveLength(5);
		expect(grant.scope).toEqual(scope);
		expect(grant.toJSON()).toEqual({
			grant: grant.toBase64(),
			issuer: issuer.publicKey,
		});
	});
	it("requires Alice's private key for delegation and lets Bob sign", async () => {
		const { grant, alice } = setup();
		const bob = ClientKey.generate();
		const child = delegateGrant({
			delegatorKey: alice,
			expiresAt: expiresAt - 1,
			holder: bob.publicKey,
			parent: grant,
			scope,
		});
		expect(() => new PkiCredentials(alice, child)).toThrow();
		const signed = await new PkiCredentials(bob, child).signRequest(
			new Request(`${audience}/v1/basins`),
		);
		expect(signed.headers.get("authorization")).toBe(
			`Biscuit ${child.toBase64()}`,
		);
		expect(() =>
			delegateGrant({
				delegatorKey: bob,
				expiresAt,
				holder: bob.publicKey,
				parent: grant,
				scope,
			}),
		).toThrow();
	});
	it.each([
		{ ...scope, operations: [...scope.operations, "Trim"] },
		{ ...scope, basins: { kind: "any" } },
		{ ...scope, streams: { kind: "prefix", value: "" } },
	])("rejects scope expansion: %j", (expanded) => {
		const { grant, alice } = setup();
		expect(() =>
			restrictGrant({
				expiresAt,
				holderKey: alice,
				parent: grant,
				scope: expanded as typeof scope,
			}),
		).toThrow();
	});
	it("rejects audience, expiry, unsigned attenuation and missing checks", () => {
		const { grant, issuer, alice } = setup();
		expect(() =>
			delegateGrant({
				delegatorKey: alice,
				expiresAt: expiresAt + 1,
				holder: alice.publicKey,
				parent: grant,
				scope,
			}),
		).toThrow();
		const root = PublicKey.fromBase58(issuer.publicKey, "secp256r1");
		const biscuit = Biscuit.fromBytes(
			base64urlnopad.decode(grant.toBase64()),
			root,
		);
		expect(() =>
			Grant.fromBase64(
				base64urlnopad.encode(biscuit.attenuate(() => {}).toBytes()),
				issuer.publicKey,
			),
		).toThrow();
		const forgedProfile = Biscuit.create(
			PrivateKey.fromBase58(issuer.toBase58(), "secp256r1"),
			(facts) => facts.push(...biscuit.blockFacts(0)),
		);
		expect(() =>
			Grant.fromBase64(
				base64urlnopad.encode(forgedProfile.toBytes()),
				issuer.publicKey,
			),
		).toThrow();
		expect(() =>
			issueGrant({
				audience: audience + "/",
				expiresAt,
				holder: alice.publicKey,
				issuerKey: issuer,
				scope,
			}),
		).toThrow();
	});
	it("rejects cross-block metadata and ambient fact injection", () => {
		const { grant, issuer } = setup();
		const root = PublicKey.fromBase58(issuer.publicKey, "secp256r1");
		const original = Biscuit.fromBytes(
			base64urlnopad.decode(grant.toBase64()),
			root,
		);
		for (const fact of [
			new BlockBuilder().fact("operation", "Trim").facts[0]!,
			new BlockBuilder().fact("pki_holder", 1n, issuer.publicKey).facts[0]!,
		]) {
			const bad = Biscuit.create(
				PrivateKey.fromBase58(issuer.toBase58(), "secp256r1"),
				(facts, checks) => {
					facts.push(...original.blockFacts(0), fact);
					checks.push(...original.blockChecks(0));
				},
			);
			expect(() =>
				Grant.fromBase64(
					base64urlnopad.encode(bad.toBytes()),
					issuer.publicKey,
				),
			).toThrow();
		}
	});
});

describe("PKI v2 HTTP proofs", () => {
	it("binds exact URI, body, Authorization and all present policy headers", async () => {
		const { credentials, alice } = setup();
		const bytes = new TextEncoder().encode("hello");
		const request = new Request(
			`${audience}/v1/streams/events%2Fx/records?q=1`,
			{
				method: "POST",
				body: bytes,
				headers: {
					"s2-basin": "test-basin",
					"content-type": "application/octet-stream",
					"idempotency-key": "id1",
					"if-match": "tag",
					"last-event-id": "42",
				},
			},
		);
		const signed = await credentials.signRequest(request);
		expect(signed.headers.get("s2-pki-body")).toBe("unary");
		expect(signed.headers.get("content-digest")).toBe(
			`sha-256=:${base64.encode(sha256(bytes))}:`,
		);
		expect(signed.redirect).toBe("error");
		const input = signed.headers.get("signature-input")!;
		expect(input).toMatch(/^s2=\("@method" "@target-uri"/);
		expect(input).toContain('"last-event-id"');
		expect(input).toContain(';tag="s2-pki-v2"');
		expect(input).toMatch(
			/;created=\d+;expires=\d+;nonce="[A-Za-z0-9+/]{43}=";alg="ecdsa-p256-sha256";keyid=/,
		);
		const components = [...input.matchAll(/"([^"@][^"]*)"/g)];
		const list = input.slice(3, input.indexOf(")") + 1);
		const names = [...list.matchAll(/"([^"]+)"/g)].map((m) => m[1]!);
		const params = input.slice(3);
		const sig = base64.decode(signed.headers.get("signature")!.slice(4, -1));
		const signatureBase =
			names
				.map(
					(n) =>
						`"${n}": ${n === "@method" ? "POST" : n === "@target-uri" ? signed.url : signed.headers.get(n)}`,
				)
				.join("\n") + `\n"@signature-params": ${params}`;
		expect(
			p256.verify(
				sig,
				new TextEncoder().encode(signatureBase),
				p256.getPublicKey(alice.toBytes()),
				{ format: "compact", lowS: false },
			),
		).toBe(true);
		expect(components.length).toBeGreaterThan(0);
	});
	it("signs empty unary bodies and fresh proofs on each attempt", async () => {
		const { credentials } = setup();
		const a = await credentials.signRequest(
			new Request(`${audience}/v1/basins`),
		);
		const b = await credentials.signRequest(
			new Request(`${audience}/v1/basins`),
		);
		expect(a.headers.get("accept")).toBe("*/*");
		expect(a.headers.get("content-digest")).toBe(
			`sha-256=:${base64.encode(sha256(new Uint8Array()))}:`,
		);
		expect(a.headers.get("signature-input")).not.toBe(
			b.headers.get("signature-input"),
		);
	});
	it("streaming openings omit the digest and reject another origin", async () => {
		const { credentials } = setup();
		const headers = new Headers({
			"content-type": "s2s/proto",
			"content-digest": "stale",
		});
		credentials.signHeaders({
			body: "streaming",
			headers,
			method: "POST",
			url: `${audience}/v1/streams/x/records`,
		});
		expect(headers.get("s2-pki-body")).toBe("streaming");
		expect(headers.has("content-digest")).toBe(false);
		await expect(
			credentials.signRequest(new Request("http://different/v1/basins")),
		).rejects.toThrow();
	});
	it("signs management retries anew after a transient failure", async () => {
		const { credentials } = setup();
		const requests: Request[] = [];
		vi.stubGlobal(
			"fetch",
			vi.fn(async (request: Request) => {
				requests.push(request);
				return requests.length === 1
					? Response.json(
							{ message: "temporary", code: "unavailable" },
							{ status: 503 },
						)
					: Response.json({ basins: [], has_more: false });
			}),
		);
		const s2 = new S2({
			pki: credentials,
			retry: { maxAttempts: 2, minBaseDelayMillis: 1, maxBaseDelayMillis: 1 },
		});
		await s2.basins.list();
		expect(requests).toHaveLength(2);
		expect(requests[0]!.headers.get("signature-input")).not.toBe(
			requests[1]!.headers.get("signature-input"),
		);
		for (const request of requests) {
			expect(request.headers.get("authorization")).toMatch(/^Biscuit /);
			expect(request.redirect).toBe("error");
		}
	});
	it("signs the SSE cursor and a fresh nonce on reconnect", async () => {
		const { credentials } = setup();
		const requests: Request[] = [];
		const client = createClient(
			createConfig({
				baseUrl: audience,
				fetch: Object.assign(
					async (request: RequestInfo | URL) => {
						if (!(request instanceof Request))
							throw new Error("Expected serialized request");
						requests.push(request);
						if (requests.length === 1) {
							let pulled = false;
							return new Response(
								new ReadableStream<Uint8Array>({
									pull(controller) {
										if (!pulled) {
											pulled = true;
											controller.enqueue(
												new TextEncoder().encode(
													'id: 42\ndata: {"value":1}\n\n',
												),
											);
										} else controller.error(new TypeError("network error"));
									},
								}),
							);
						}
						return new Response('data: {"value":2}\n\n');
					},
					{ preconnect: vi.fn() },
				),
			}),
		);
		configurePkiClient(client, credentials);
		const { stream } = await client.sse.get<{ value: number }>({
			url: "/v1/streams/x/records",
			sseMaxRetryAttempts: 2,
			sseDefaultRetryDelay: 1,
			sseMaxRetryDelay: 1,
		});
		const values = [];
		for await (const value of stream) values.push(value);
		expect(values).toEqual([{ value: 1 }, { value: 2 }]);
		expect(requests).toHaveLength(2);
		expect(requests[1]!.headers.get("last-event-id")).toBe("42");
		expect(requests[1]!.headers.get("signature-input")).toContain(
			'"last-event-id"',
		);
		expect(requests[0]!.headers.get("signature-input")).not.toBe(
			requests[1]!.headers.get("signature-input"),
		);
	});
	it("PKI never selects an access token and blocks ordinary token management locally", async () => {
		const { credentials } = setup();
		const fetchMock = vi.fn();
		vi.stubGlobal("fetch", fetchMock);
		const s2 = new S2({
			pki: credentials,
			endpoints: { account: audience, basin: audience },
		});
		await expect(s2.accessTokens.list()).rejects.toThrow(/PKI/);
		await expect(s2.accessTokens.revoke({ id: "token" })).rejects.toThrow(
			/PKI/,
		);
		await expect(
			s2.accessTokens.issue({ id: "token", scope: {} }),
		).rejects.toThrow(/PKI/);
		expect(fetchMock).not.toHaveBeenCalled();
		expect(
			() => new S2({ accessToken: "bearer", pki: credentials } as never),
		).toThrow();
	});
});
