/**
 * Native TypeScript Biscuit token creation using @bufbuild/protobuf + @noble/curves.
 * No WASM dependency.
 */

import { create, toBinary } from "@bufbuild/protobuf";
import { p256 } from "@noble/curves/nist.js";
import { base64 } from "@scure/base";
import {
	BiscuitSchema,
	BlockSchema,
	CheckSchema,
	Check_Kind,
	ExpressionSchema,
	FactSchema,
	OpBinary_Kind,
	OpSchema,
	PredicateSchema,
	ProofSchema,
	PublicKeySchema,
	PublicKey_Algorithm,
	RuleSchema,
	SignedBlockSchema,
	TermSchema,
} from "./proto/schema_pb.js";
import { SigningKey } from "./signing-key.js";

// ─── Symbol table ───

const DEFAULT_SYMBOLS = [
	"read", "write", "resource", "operation", "right", "time", "role", "owner",
	"tenant", "namespace", "user", "team", "service", "admin", "email", "group",
	"member", "ip_address", "client", "client_ip", "domain", "path", "version",
	"cluster", "node", "hostname", "nonce", "query",
];

const CUSTOM_SYMBOL_START = 1024;

class SymbolTable {
	#symbols = [...DEFAULT_SYMBOLS];
	#nextCustom = CUSTOM_SYMBOL_START;

	intern(name: string): number {
		const defaultIdx = DEFAULT_SYMBOLS.indexOf(name);
		if (defaultIdx !== -1) return defaultIdx;
		for (let i = DEFAULT_SYMBOLS.length; i < this.#symbols.length; i++) {
			if (this.#symbols[i] === name) return CUSTOM_SYMBOL_START + (i - DEFAULT_SYMBOLS.length);
		}
		this.#symbols.push(name);
		return this.#nextCustom++;
	}

	customSymbols(): string[] {
		return this.#symbols.slice(DEFAULT_SYMBOLS.length);
	}
}

// ─── Signature payload (V1 tagged format) ───

const encoder = new TextEncoder();

function tagged(tag: string, data: Uint8Array): Uint8Array {
	const tagBytes = encoder.encode(`\0${tag}\0`);
	const result = new Uint8Array(tagBytes.length + data.length);
	result.set(tagBytes);
	result.set(data, tagBytes.length);
	return result;
}

function u32le(n: number): Uint8Array {
	const buf = new Uint8Array(4);
	new DataView(buf.buffer).setUint32(0, n, true);
	return buf;
}

function concat(...arrays: Uint8Array[]): Uint8Array {
	let length = 0;
	for (const arr of arrays) length += arr.length;
	const result = new Uint8Array(length);
	let offset = 0;
	for (const arr of arrays) { result.set(arr, offset); offset += arr.length; }
	return result;
}

function signaturePayloadV1(
	blockData: Uint8Array,
	nextKeyAlgorithm: number,
	nextKeyBytes: Uint8Array,
): Uint8Array {
	return concat(
		tagged("BLOCK", tagged("VERSION", u32le(1))),
		tagged("PAYLOAD", blockData),
		tagged("ALGORITHM", u32le(nextKeyAlgorithm)),
		tagged("NEXTKEY", nextKeyBytes),
	);
}

// ─── Token creation ───

export type BiscuitTokenOptions = {
	/** P256 private key as base58-encoded 32 bytes */
	privateKey: string;
	/** Public key to bind the token to (base58). If not provided, derives from privateKey. */
	publicKey?: string;
	/** Token expiry in seconds from now (default: 3600 = 1 hour) */
	expiresIn?: number;
	/** Operation groups to grant (default: all read+write) */
	opGroups?: Array<{ access: string; level: string }>;
	/** Basin scope (default: prefix "" = all basins) */
	basinScope?: { type: "prefix" | "exact" | "none"; value: string };
	/** Stream scope (default: prefix "" = all streams) */
	streamScope?: { type: "prefix" | "exact" | "none"; value: string };
	/** Access token scope (default: prefix "" = all tokens) */
	accessTokenScope?: { type: "prefix" | "exact" | "none"; value: string };
};

/**
 * Creates a Biscuit authority token signed with P-256.
 * @returns Base64-encoded Biscuit token
 */
export function createBiscuitToken(options: BiscuitTokenOptions): string {
	const {
		privateKey,
		publicKey: providedPublicKey,
		expiresIn = 3600,
		opGroups = [
			{ level: "account", access: "read" },
			{ level: "account", access: "write" },
			{ level: "basin", access: "read" },
			{ level: "basin", access: "write" },
			{ level: "stream", access: "read" },
			{ level: "stream", access: "write" },
		],
		basinScope = { type: "prefix", value: "" },
		streamScope = { type: "prefix", value: "" },
		accessTokenScope = { type: "prefix", value: "" },
	} = options;

	const signingKey = SigningKey.fromBase58(privateKey);
	const rootKeyBytes = signingKey.getPrivateKeyBytes();
	const publicKeyBase58 = providedPublicKey ?? signingKey.publicKeyBase58();
	const expiresTs = Math.floor(Date.now() / 1000) + expiresIn;

	// Build authority block
	const symbols = new SymbolTable();

	const facts = [
		makeFact(symbols, "public_key", [{ type: "string", value: publicKeyBase58 }]),
		makeFact(symbols, "expires", [{ type: "integer", value: expiresTs }]),
		...opGroups.map(({ level, access }) =>
			makeFact(symbols, "op_group", [{ type: "string", value: level }, { type: "string", value: access }]),
		),
		makeFact(symbols, "basin_scope", [{ type: "string", value: basinScope.type }, { type: "string", value: basinScope.value }]),
		makeFact(symbols, "stream_scope", [{ type: "string", value: streamScope.type }, { type: "string", value: streamScope.value }]),
		makeFact(symbols, "access_token_scope", [{ type: "string", value: accessTokenScope.type }, { type: "string", value: accessTokenScope.value }]),
	];

	// check if time($t), $t < expiresTs
	const tVar = symbols.intern("t");
	const checks = [create(CheckSchema, {
		kind: Check_Kind.One,
		queries: [create(RuleSchema, {
			body: [create(PredicateSchema, {
				name: BigInt(symbols.intern("time")),
				terms: [create(TermSchema, { Content: { case: "variable", value: tVar } })],
			})],
			expressions: [create(ExpressionSchema, {
				ops: [
					create(OpSchema, { Content: { case: "value", value: create(TermSchema, { Content: { case: "variable", value: tVar } }) } }),
					create(OpSchema, { Content: { case: "value", value: create(TermSchema, { Content: { case: "integer", value: BigInt(expiresTs) } }) } }),
					create(OpSchema, { Content: { case: "Binary", value: { kind: OpBinary_Kind.LessThan, ffiName: 0n, $typeName: "biscuit.format.schema.OpBinary" } } }),
				],
			})],
			head: create(PredicateSchema, { name: BigInt(symbols.intern("query")), terms: [] }),
			scope: [],
		})],
	})];

	const blockData = toBinary(BlockSchema, create(BlockSchema, {
		checks,
		facts,
		publicKeys: [],
		rules: [],
		scope: [],
		symbols: symbols.customSymbols(),
		version: 3,
	}));

	// Ephemeral next key
	const nextPrivKey = p256.utils.randomSecretKey();
	const nextPubKey = p256.getPublicKey(nextPrivKey, true);

	// Sign
	const payload = signaturePayloadV1(blockData, PublicKey_Algorithm.SECP256R1, nextPubKey);
	const signature = p256.sign(payload, rootKeyBytes, { format: "der", lowS: true });

	// Assemble
	const signedBlock = create(SignedBlockSchema, {
		block: blockData,
		nextKey: create(PublicKeySchema, { algorithm: PublicKey_Algorithm.SECP256R1, key: nextPubKey }),
		signature,
		version: 1,
	});

	const proof = create(ProofSchema, {
		Content: { case: "nextSecret", value: nextPrivKey },
	});

	const biscuit = create(BiscuitSchema, {
		authority: signedBlock,
		blocks: [],
		proof,
	});

	return base64.encode(toBinary(BiscuitSchema, biscuit));
}

/**
 * Derives the public key from a private key.
 * @param privateKey Base58-encoded P256 private key (32 bytes)
 * @returns Base58-encoded compressed public key (33 bytes)
 */
export function derivePublicKey(privateKey: string): string {
	return SigningKey.fromBase58(privateKey).publicKeyBase58();
}

// ─── Helpers ───

type TermValue =
	| { type: "string"; value: string }
	| { type: "integer"; value: number };

function makeFact(symbols: SymbolTable, name: string, terms: TermValue[]) {
	return create(FactSchema, {
		predicate: create(PredicateSchema, {
			name: BigInt(symbols.intern(name)),
			terms: terms.map((t) => {
				if (t.type === "string") {
					return create(TermSchema, { Content: { case: "string", value: BigInt(symbols.intern(t.value)) } });
				}
				return create(TermSchema, { Content: { case: "integer", value: BigInt(t.value) } });
			}),
		}),
	});
}
