import { base64 } from "@scure/base";
import { SigningKey } from "./signing-key.js";

// Dynamic import for biscuit-wasm to handle WASM initialization
let biscuitModule: typeof import("@biscuit-auth/biscuit-wasm") | null = null;
let initPromise: Promise<void> | null = null;

async function getBiscuit() {
	if (!biscuitModule) {
		biscuitModule = await import("@biscuit-auth/biscuit-wasm");
		if (!initPromise) {
			initPromise = Promise.resolve(biscuitModule.init());
		}
		await initPromise;
	}
	return biscuitModule;
}

export type BiscuitTokenOptions = {
	/** P256 private key as base58-encoded 32 bytes */
	privateKey: string;
	/** Public key to bind the token to (base58). If not provided, derives from privateKey. */
	publicKey?: string;
	/** Token expiry in seconds from now (default: 3600 = 1 hour) */
	expiresIn?: number;
	/** Operation groups to grant (default: all read+write) */
	opGroups?: Array<{ level: string; access: string }>;
	/** Basin scope (default: prefix "" = all basins) */
	basinScope?: { type: "prefix" | "exact" | "none"; value: string };
	/** Stream scope (default: prefix "" = all streams) */
	streamScope?: { type: "prefix" | "exact" | "none"; value: string };
	/** Access token scope (default: prefix "" = all tokens) */
	accessTokenScope?: { type: "prefix" | "exact" | "none"; value: string };
};

/**
 * Creates an admin Biscuit token from a root private key.
 * This enables bootstrap mode where the admin can operate without a pre-existing token.
 *
 * @returns Base64-encoded Biscuit token
 */
export async function createBiscuitToken(
	options: BiscuitTokenOptions,
): Promise<string> {
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

	const biscuit = await getBiscuit();

	// Use SigningKey for validation and key derivation
	const signingKey = SigningKey.fromBase58(privateKey);
	const privateKeyBytes = signingKey.getPrivateKeyBytes();
	const publicKeyBase58 = providedPublicKey ?? signingKey.publicKeyBase58();

	// Create Biscuit private key (P256 = secp256r1)
	const biscuitPrivateKey = biscuit.PrivateKey.fromBytes(
		privateKeyBytes,
		biscuit.SignatureAlgorithm.Secp256r1,
	);

	// Build the token
	const expiresTs = Math.floor(Date.now() / 1000) + expiresIn;

	const builder = biscuit.biscuit`
		public_key(${publicKeyBase58});
		expires(${expiresTs});
	`;

	builder.merge(biscuit.block`
		check if time($t), $t < ${expiresTs};
	`);

	for (const { level, access } of opGroups) {
		builder.merge(biscuit.block`
			op_group(${level}, ${access});
		`);
	}

	builder.merge(biscuit.block`
		basin_scope(${basinScope.type}, ${basinScope.value});
		stream_scope(${streamScope.type}, ${streamScope.value});
		access_token_scope(${accessTokenScope.type}, ${accessTokenScope.value});
	`);

	const token = builder.build(biscuitPrivateKey);
	return base64.encode(token.toBytes());
}

/**
 * Derives the public key from a private key.
 * @param privateKey Base58-encoded P256 private key (32 bytes)
 * @returns Base58-encoded compressed public key (33 bytes)
 */
export function derivePublicKey(privateKey: string): string {
	return SigningKey.fromBase58(privateKey).publicKeyBase58();
}
