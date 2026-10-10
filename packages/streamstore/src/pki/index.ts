/** S2 PKI v2. Generic Biscuit cryptography and wire encoding come from ENG-716. */
import {
	Biscuit,
	BlockBuilder,
	type Check,
	type Fact,
	type Op,
	type Predicate,
	PrivateKey,
	PublicKey,
	type Rule,
	type Term,
} from "@flickfyi/biscuit";
import { p256 } from "@noble/curves/nist.js";
import { sha256 } from "@noble/hashes/sha2.js";
import { base58, base64, base64urlnopad } from "@scure/base";

export const PKI_OPERATIONS = [
	"AccountMetrics",
	"Append",
	"BasinMetrics",
	"CheckTail",
	"CreateBasin",
	"CreateStream",
	"DeleteBasin",
	"DeleteStream",
	"Fence",
	"GetBasinConfig",
	"GetDefaultLocation",
	"GetStreamConfig",
	"ListBasins",
	"ListLocations",
	"ListStreams",
	"Read",
	"ReconfigureBasin",
	"ReconfigureStream",
	"SetDefaultLocation",
	"StreamMetrics",
	"Trim",
] as const;
export type PkiOperation = (typeof PKI_OPERATIONS)[number];
export type ResourceScope =
	| { readonly kind: "any" }
	| { readonly kind: "none" }
	| { readonly kind: "exact" | "prefix"; readonly value: string };
export type GrantScope = {
	readonly basins: ResourceScope;
	readonly operations: readonly PkiOperation[];
	readonly streams: ResourceScope;
};
export type GrantDocument = { grant: string; issuer: string };
type Metadata = {
	audience: string;
	expiresAt: number;
	holder: string;
	parent?: Uint8Array;
	scope: GrantScope;
};

/** P-256 key; private bytes are kept out of enumerable properties and JSON. */
export class ClientKey {
	readonly #bytes: Uint8Array;
	private constructor(bytes: Uint8Array) {
		p256.getPublicKey(bytes);
		this.#bytes = Uint8Array.from(bytes);
	}
	static fromBase58(value: string): ClientKey {
		return new ClientKey(base58.decode(value));
	}
	static fromBytes(value: Uint8Array): ClientKey {
		return new ClientKey(value);
	}
	static generate(): ClientKey {
		return new ClientKey(p256.utils.randomSecretKey());
	}
	get publicKey(): string {
		return base58.encode(p256.getPublicKey(this.#bytes, true));
	}
	toBase58(): string {
		return base58.encode(this.#bytes);
	}
	toBytes(): Uint8Array {
		return Uint8Array.from(this.#bytes);
	}
	toJSON(): string {
		return "<redacted>";
	}
}

/** Verified issuer chain and S2 profile. Permissions remain server-side Biscuit checks. */
export class Grant {
	readonly #biscuit: Biscuit;
	readonly #bytes: Uint8Array;
	readonly #issuer: string;
	readonly #metadata: Metadata;
	readonly #ids: Uint8Array[];
	private constructor(bytes: Uint8Array, issuer: string) {
		if (bytes.length > 64 * 1024) throw new Error("PKI grant exceeds 64 KiB");
		validatePublicKey(issuer);
		this.#biscuit = Biscuit.fromBytes(
			bytes,
			PublicKey.fromBase58(issuer, "secp256r1"),
		);
		const { ids, metadata } = validateProfile(this.#biscuit);
		this.#bytes = Uint8Array.from(bytes);
		this.#issuer = issuer;
		this.#metadata = metadata;
		this.#ids = ids;
	}
	static fromBase64(value: string, issuer: string): Grant {
		if (value.length > 128 * 1024 || !/^[A-Za-z0-9_-]+$/.test(value))
			throw new Error("Invalid PKI grant encoding");
		return new Grant(base64urlnopad.decode(value), issuer);
	}
	static fromJSON(document: GrantDocument): Grant {
		return Grant.fromBase64(document.grant, document.issuer);
	}
	get audience(): string {
		return this.#metadata.audience;
	}
	get expiresAt(): number {
		return this.#metadata.expiresAt;
	}
	get holder(): string {
		return this.#metadata.holder;
	}
	get issuer(): string {
		return this.#issuer;
	}
	get scope(): GrantScope {
		return structuredClone(this.#metadata.scope);
	}
	get revocationIds(): Uint8Array[] {
		return this.#ids.map((id) => Uint8Array.from(id));
	}
	toBase64(): string {
		return base64urlnopad.encode(this.#bytes);
	}
	toJSON(): GrantDocument {
		return { grant: this.toBase64(), issuer: this.#issuer };
	}
	/** @internal Kept here so no caller can mutate the verified Biscuit AST. */
	static issue(options: IssueGrantOptions): Grant {
		validateMetadata(options);
		const block = nativeBlock(0, options);
		const biscuit = Biscuit.create(
			PrivateKey.fromBytes(options.issuerKey.toBytes(), "secp256r1"),
			(facts, checks) => {
				facts.push(...block.facts);
				checks.push(...block.checks);
			},
		);
		return new Grant(biscuit.toBytes(), options.issuerKey.publicKey);
	}
	/** @internal Requires the leaf holder's key, not the Biscuit attenuation secret. */
	delegate(options: Omit<DelegateGrantOptions, "parent">): Grant {
		const { delegatorKey, expiresAt, holder, scope } = options;
		if (delegatorKey.publicKey !== this.holder)
			throw new Error("PKI delegation requires the current holder's key");
		const metadata: Metadata = {
			audience: this.audience,
			expiresAt,
			holder,
			parent: this.#ids.at(-1),
			scope,
		};
		validateMetadata(metadata);
		if (expiresAt > this.expiresAt || !scopeSubset(scope, this.#metadata.scope))
			throw new Error("PKI delegation cannot expand scope or expiry");
		const block = nativeBlock(this.#biscuit.blockCount, metadata);
		const external = this.#biscuit.thirdPartyRequest().createBlock({
			build: (content) => {
				content.facts.push(...block.facts);
				content.checks.push(...block.checks);
			},
			thirdPartyKey: PrivateKey.fromBytes(delegatorKey.toBytes(), "secp256r1"),
		});
		return new Grant(
			this.#biscuit.appendThirdPartyBlock({ block: external }).toBytes(),
			this.#issuer,
		);
	}
}
export type IssueGrantOptions = {
	audience: string;
	expiresAt: number;
	holder: string;
	issuerKey: ClientKey;
	scope: GrantScope;
};
export type DelegateGrantOptions = {
	delegatorKey: ClientKey;
	expiresAt: number;
	holder: string;
	parent: Grant;
	scope: GrantScope;
};
export function issueGrant(options: IssueGrantOptions): Grant {
	return Grant.issue(options);
}
export function delegateGrant({
	parent,
	...options
}: DelegateGrantOptions): Grant {
	return parent.delegate(options);
}
export function restrictGrant({
	expiresAt,
	holderKey,
	parent,
	scope,
}: {
	expiresAt: number;
	holderKey: ClientKey;
	parent: Grant;
	scope: GrantScope;
}): Grant {
	return delegateGrant({
		delegatorKey: holderKey,
		expiresAt,
		holder: holderKey.publicKey,
		parent,
		scope,
	});
}

/** Exclusive PKI credentials. The signer never reads a bearer credential provider. */
export class PkiCredentials {
	readonly #key: ClientKey;
	readonly #grant: Grant;
	constructor(key: ClientKey, grant: Grant) {
		if (key.publicKey !== grant.holder)
			throw new Error("PKI client key does not match the leaf holder");
		this.#key = key;
		this.#grant = grant;
	}
	get audience(): string {
		return this.#grant.audience;
	}
	get grant(): Grant {
		return this.#grant;
	}
	signHeaders({
		body,
		headers,
		method,
		now = Math.floor(Date.now() / 1000),
		url,
	}: {
		body: Uint8Array | "streaming";
		headers: Headers;
		method: string;
		now?: number;
		url: string;
	}): void {
		const target = new URL(url);
		if (
			target.origin !== this.audience ||
			target.username ||
			target.password ||
			target.hash
		)
			throw new Error("PKI request origin does not match the grant audience");
		if (!Number.isSafeInteger(now) || now < 0 || now >= this.#grant.expiresAt)
			throw new Error("PKI grant expired");
		if (!headers.has("accept")) headers.set("accept", "*/*");
		headers.set("authorization", `Biscuit ${this.#grant.toBase64()}`);
		headers.delete("signature");
		headers.delete("signature-input");
		if (body === "streaming") {
			headers.set("s2-pki-body", "streaming");
			headers.delete("content-digest");
		} else {
			headers.set("s2-pki-body", "unary");
			headers.set("content-digest", `sha-256=:${base64.encode(sha256(body))}:`);
		}
		const headerNames: string[] = [];
		headers.forEach((_, name) => headerNames.push(name));
		const names = [
			"@method",
			"@target-uri",
			...headerNames
				.filter(
					(name) =>
						name.startsWith("s2-") ||
						[
							"authorization",
							"content-digest",
							"content-type",
							"content-encoding",
							"idempotency-key",
							"range",
							"accept",
							"last-event-id",
							"if-match",
							"if-none-match",
						].includes(name),
				)
				.sort(),
		];
		const nonce = base64.encode(crypto.getRandomValues(new Uint8Array(32)));
		const params = `(${names.map((name) => `"${name}"`).join(" ")});created=${now};expires=${now + 60};nonce="${nonce}";alg="ecdsa-p256-sha256";keyid="${this.#key.publicKey}";tag="s2-pki-v2"`;
		const signatureBase =
			names
				.map(
					(name) =>
						`"${name}": ${name === "@method" ? method : name === "@target-uri" ? url : headers.get(name)}`,
				)
				.join("\n") + `\n"@signature-params": ${params}`;
		const signature = p256.sign(
			new TextEncoder().encode(signatureBase),
			this.#key.toBytes(),
			{ format: "compact", lowS: true },
		);
		headers.set("signature", `s2=:${base64.encode(signature)}:`);
		headers.set("signature-input", `s2=${params}`);
	}
	async signRequest(request: Request): Promise<Request> {
		const headers = new Headers(request.headers);
		this.signHeaders({
			body: new Uint8Array(await request.clone().arrayBuffer()),
			headers,
			method: request.method,
			url: request.url,
		});
		return new Request(request, {
			cache: "no-store",
			headers,
			redirect: "error",
		});
	}
	toJSON(): string {
		return "<redacted>";
	}
}

function nativeBlock(index: number, metadata: Metadata): BlockBuilder {
	const block = new BlockBuilder()
		.fact("pki_version", BigInt(index), 2n)
		.fact("pki_holder", BigInt(index), metadata.holder)
		.fact("pki_audience", BigInt(index), metadata.audience)
		.fact("pki_expires", BigInt(index), BigInt(metadata.expiresAt));
	for (const [name, scope] of [
		["pki_basin", metadata.scope.basins],
		["pki_stream", metadata.scope.streams],
	] as const)
		block.fact(
			name,
			BigInt(index),
			scope.kind,
			"value" in scope ? scope.value : "",
		);
	for (const operation of metadata.scope.operations)
		block.fact("pki_operation", BigInt(index), operation);
	if (metadata.parent) block.fact("pki_parent", BigInt(index), metadata.parent);
	for (const check of nativeChecks(index)) block.addCheck(check);
	return block;
}
function nativeChecks(index: number): Check[] {
	const i = integer(index);
	const checks: Check[] = [
		check(
			query([
				predicate("operation", variable("op")),
				predicate("pki_operation", i, variable("op")),
			]),
		),
		check(
			query([
				predicate("audience", variable("a")),
				predicate("pki_audience", i, variable("a")),
			]),
		),
		check(
			query(
				[
					predicate("time", variable("now")),
					predicate("pki_expires", i, variable("expiry")),
				],
				[
					[
						value(variable("now")),
						value(variable("expiry")),
						{ type: "binary", kind: 0 },
					],
				],
			),
		),
	];
	for (const [resource, v] of [
		["basin", "b"],
		["stream", "s"],
	] as const)
		checks.push(
			check(
				query([
					predicate(`${resource}_present`, { type: "bool", value: false }),
				]),
				query([
					predicate(resource, variable(v)),
					predicate(`pki_${resource}`, i, string("any"), string("")),
				]),
				query([
					predicate(resource, variable(v)),
					predicate(`pki_${resource}`, i, string("exact"), variable(v)),
				]),
				query(
					[
						predicate(resource, variable(v)),
						predicate(`pki_${resource}`, i, string("prefix"), variable("p")),
					],
					[
						[
							value(variable(v)),
							value(variable("p")),
							{ type: "binary", kind: 6 },
						],
					],
				),
			),
		);
	return checks;
}
function validateProfile(biscuit: Biscuit): {
	ids: Uint8Array[];
	metadata: Metadata;
} {
	if (biscuit.blockCount > 16)
		throw new Error("PKI grants support at most 16 blocks");
	const ids = biscuit.blockMetadata.map(
		({ revocationId, signatureAlgorithm }) => {
			if (signatureAlgorithm === "ed25519")
				return Uint8Array.from(revocationId);
			if (signatureAlgorithm !== "secp256r1")
				throw new Error(
					"Biscuit package must expose verified signature algorithms",
				);
			const signature = p256.Signature.fromBytes(revocationId, "der");
			return new p256.Signature(
				signature.r,
				signature.hasHighS() ? p256.Point.Fn.ORDER - signature.s : signature.s,
			).toBytes("der");
		},
	);
	let previous: Metadata | undefined;
	const totalFacts = Array.from(
		{ length: biscuit.blockCount },
		(_, i) => biscuit.blockFacts(i).length,
	).reduce((a, b) => a + b, 0);
	if (totalFacts > 512) throw new Error("PKI grant fact budget exceeded");
	let customQueries = 0,
		customWork = 0,
		conversionItems = 0;
	for (let index = 0; index < biscuit.blockCount; index++) {
		if (biscuit.blockRules(index).length || biscuit.blockScopeCount(index))
			throw new Error("Unsupported PKI rules or global trust scopes");
		const facts = biscuit.blockFacts(index);
		const checks = biscuit.blockChecks(index);
		const required = nativeChecks(index);
		// Mirror Rust's aggregate conversion-item charge using the generic AST.
		conversionItems += facts.reduce(
			(sum, fact) => sum + 1 + fact.predicate.terms.length,
			0,
		);
		for (const c of checks) {
			conversionItems++;
			for (const q of c.queries) {
				conversionItems +=
					2 +
					q.head.terms.length +
					q.body.reduce((sum, p) => sum + 1 + p.terms.length, 0) +
					2 * q.expressions.flat().length +
					q.expressions.length +
					q.scope.length;
			}
		}
		if (conversionItems > 4096)
			throw new Error("PKI policy conversion budget exceeded");

		for (const expected of required)
			if (checks.filter((c) => astEqual(c, expected)).length !== 1)
				throw new Error("Missing or duplicate PKI native check");
		for (const fact of facts) {
			validatePredicate(fact.predicate, false);
			const { name, terms } = fact.predicate;
			if (
				[
					"time",
					"operation",
					"audience",
					"basin",
					"stream",
					"basin_present",
					"stream_present",
				].includes(name)
			)
				throw new Error("PKI ambient facts are server-owned");
			if (
				name.startsWith("pki_") &&
				(![
					"pki_version",
					"pki_holder",
					"pki_audience",
					"pki_expires",
					"pki_basin",
					"pki_stream",
					"pki_operation",
					"pki_parent",
				].includes(name) ||
					!astEqual(terms[0], integer(index)))
			)
				throw new Error("Invalid PKI metadata index");
		}
		for (const c of checks) {
			if (required.some((r) => astEqual(c, r))) continue;
			if (!c.queries.length)
				throw new Error("Unsupported empty PKI custom check");
			for (const q of c.queries) {
				customQueries++;
				if (
					q.scope.some(
						(s) => s.type !== "authority" && s.type !== "previous",
					) ||
					q.body.length > 1 ||
					q.scope.length > 2 ||
					q.expressions.length > 16
				)
					throw new Error("Unsupported PKI custom check");
				const ops = q.expressions.flat();
				if (ops.length > 16)
					throw new Error("PKI check expression budget exceeded");
				for (const p of [q.head, ...q.body]) validatePredicate(p, true);
				for (const op of ops) {
					if (op.type === "value") validateTerm(op.term, true);
					else if (
						op.type === "closure" ||
						op.ffiName !== undefined ||
						(op.type === "binary" &&
							![0, 1, 2, 3, 4, 6, 7, 13, 14, 20, 21, 22].includes(op.kind)) ||
						(op.type === "unary" && ![0, 1, 2, 3].includes(op.kind))
					)
						throw new Error("Unsupported PKI expression");
				}
				customWork +=
					(q.body.length ? totalFacts + 7 : 1) *
					(1 + (q.body[0]?.terms.length ?? 0) + ops.length);
			}
		}
		if (customQueries > 64 || customWork > 4096)
			throw new Error("PKI custom policy budget exceeded");
		const terms = (name: string): Term[] => {
			const matches = facts.filter((f) => f.predicate.name === name);
			if (matches.length !== 1) throw new Error(`Invalid PKI ${name} fact`);
			return matches[0]!.predicate.terms.slice(1);
		};
		const scalar = (name: string, kind: "string" | "integer") => {
			const t = terms(name);
			if (t.length !== 1 || t[0]!.type !== kind)
				throw new Error(`Invalid PKI ${name} value`);
			return (t[0] as { value: string | bigint }).value;
		};
		if (scalar("pki_version", "integer") !== 2n)
			throw new Error("Unsupported PKI protocol version");
		const operations = facts
			.filter((f) => f.predicate.name === "pki_operation")
			.map((f) => {
				const t = f.predicate.terms;
				if (
					t.length !== 2 ||
					t[1]?.type !== "string" ||
					!PKI_OPERATIONS.includes(t[1].value as PkiOperation)
				)
					throw new Error("Invalid PKI operation");
				return t[1].value as PkiOperation;
			});
		const readScope = (name: string): ResourceScope => {
			const t = terms(name);
			if (t.length !== 2 || t[0]?.type !== "string" || t[1]?.type !== "string")
				throw new Error("Invalid PKI scope");
			const kind = t[0].value;
			if (kind === "any" || kind === "none") {
				if (t[1].value !== "") throw new Error("Invalid PKI scope value");
				return { kind };
			}
			if (kind !== "exact" && kind !== "prefix")
				throw new Error("Invalid PKI scope kind");
			return { kind, value: t[1].value };
		};
		const expiry = scalar("pki_expires", "integer") as bigint;
		if (expiry > BigInt(Number.MAX_SAFE_INTEGER))
			throw new Error("Invalid PKI expiry");
		const current: Metadata = {
			audience: scalar("pki_audience", "string") as string,
			expiresAt: Number(expiry),
			holder: scalar("pki_holder", "string") as string,
			scope: {
				basins: readScope("pki_basin"),
				operations,
				streams: readScope("pki_stream"),
			},
		};
		validateMetadata(current, false);
		const parent = facts.filter((f) => f.predicate.name === "pki_parent");
		if (index === 0) {
			if (parent.length) throw new Error("Authority cannot have a parent");
		} else {
			const t = terms("pki_parent");
			const external = biscuit.blockThirdPartyKey(index);
			if (
				t.length !== 1 ||
				t[0]?.type !== "bytes" ||
				!astEqual(t[0].value, ids[index - 1]) ||
				external?.algorithm !== "secp256r1" ||
				external.toBase58() !== previous!.holder ||
				current.audience !== previous!.audience ||
				current.expiresAt > previous!.expiresAt ||
				!scopeSubset(current.scope, previous!.scope)
			)
				throw new Error("Invalid PKI delegation or scope expansion");
		}
		previous = current;
	}
	return { ids, metadata: previous! };
}
function validateMetadata(metadata: Metadata, future = true): void {
	const origin = new URL(metadata.audience);
	if (
		!["http:", "https:"].includes(origin.protocol) ||
		origin.origin !== metadata.audience ||
		origin.username ||
		origin.password
	)
		throw new Error("PKI audience must be an exact HTTP origin");
	validatePublicKey(metadata.holder);
	if (
		!Number.isSafeInteger(metadata.expiresAt) ||
		metadata.expiresAt <= 0 ||
		(future && metadata.expiresAt <= Math.floor(Date.now() / 1000))
	)
		throw new Error("Invalid PKI grant expiry");
	for (const scope of [metadata.scope.basins, metadata.scope.streams]) {
		if (
			!["any", "none", "exact", "prefix"].includes(scope.kind) ||
			((scope.kind === "exact" || scope.kind === "prefix") &&
				(typeof scope.value !== "string" ||
					new TextEncoder().encode(scope.value).length > 1024)) ||
			((scope.kind === "any" || scope.kind === "none") &&
				"value" in scope &&
				scope.value !== "")
		)
			throw new Error("Invalid PKI resource scope");
	}
	if (
		!metadata.scope.operations.length ||
		metadata.scope.operations.some((op) => !PKI_OPERATIONS.includes(op))
	)
		throw new Error(
			"Invalid PKI operation; bearer token management is unavailable",
		);
}
function validatePublicKey(key: string): void {
	const bytes = base58.decode(key);
	if (bytes.length !== 33) throw new Error("PKI keys use compressed P-256");
	p256.Point.fromBytes(bytes).assertValidity();
}
function scopeSubset(child: GrantScope, parent: GrantScope): boolean {
	return (
		resourceSubset(child.basins, parent.basins) &&
		resourceSubset(child.streams, parent.streams) &&
		child.operations.every((op) => parent.operations.includes(op))
	);
}
function resourceSubset(child: ResourceScope, parent: ResourceScope): boolean {
	if (child.kind === "none" || parent.kind === "any") return true;
	if (child.kind === "any" || parent.kind === "none") return false;
	return parent.kind === "prefix"
		? child.value.startsWith(parent.value)
		: child.kind === "exact" && child.value === parent.value;
}
function astEqual(a: unknown, b: unknown): boolean {
	return (
		JSON.stringify(a, (_, v) => (typeof v === "bigint" ? `${v}n` : v)) ===
		JSON.stringify(b, (_, v) => (typeof v === "bigint" ? `${v}n` : v))
	);
}
function validatePredicate(p: Predicate, variables: boolean): void {
	if (new TextEncoder().encode(p.name).length > 128 || p.terms.length > 8)
		throw new Error("Invalid PKI predicate");
	for (const term of p.terms) validateTerm(term, variables);
}
function validateTerm(term: Term, variables: boolean): void {
	if (
		term.type === "string" &&
		new TextEncoder().encode(term.value).length <= 1024
	)
		return;
	if (term.type === "bytes" && term.value.length <= 1024) return;
	if (
		term.type === "variable" &&
		variables &&
		new TextEncoder().encode(term.name).length <= 128
	)
		return;
	if (
		term.type === "integer" ||
		term.type === "bool" ||
		term.type === "date" ||
		term.type === "null"
	)
		return;
	throw new Error("Unsupported PKI term");
}
function check(...queries: Rule[]): Check {
	return { kind: "one", queries };
}
function query(body: Predicate[], expressions: Op[][] = []): Rule {
	return {
		body,
		expressions,
		head: predicate("query"),
		scope: [{ type: "authority" }],
	};
}
function predicate(name: string, ...terms: Term[]): Predicate {
	return { name, terms };
}
function variable(name: string): Term {
	return { type: "variable", name };
}
function string(value: string): Term {
	return { type: "string", value };
}
function integer(value: number): Term {
	return { type: "integer", value: BigInt(value) };
}
function value(term: Term): Op {
	return { type: "value", term };
}
