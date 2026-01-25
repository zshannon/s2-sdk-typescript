import { createBiscuitToken } from "./biscuit.js";
import { signRequest, signHeaders } from "./sign.js";
import { SigningKey } from "./signing-key.js";
import type { SignHeadersOptions } from "./sign.js";

/**
 * Configuration for PKI authentication.
 *
 * Two modes are supported:
 *
 * 1. **Token + SigningKey mode** (for regular operations): Provide `token` and `signingKey`.
 *    Use this for basins, streams, and other regular operations.
 *
 * 2. **Root key mode** (for all operations): Provide only `rootKey`.
 *    The SDK generates tokens locally (signed by root key, bound to a generated client key).
 */
export type PkiAuthConfig = {
	/** Biscuit token issued with the signing key's public key */
	token?: string;
	/** P256 signing key that matches the token's bound public key */
	signingKey?: SigningKey;
	/** P256 private key as base58-encoded 32 bytes (root key) */
	rootKey?: string;
	/** Token expiry in seconds (default: 3600 = 1 hour) */
	tokenExpiresIn?: number;
	/** Signature expiry in seconds (default: 300 = 5 minutes) */
	signatureExpiresIn?: number;
};

/**
 * Auth mode indicates what operations are allowed.
 */
export type PkiAuthMode = "root" | "token";

export type PkiAuthContext = {
	/** The auth mode */
	mode: PkiAuthMode;
	/** The signing key's public key (base58) */
	publicKey: string;
	/** Get current token */
	getToken: () => Promise<string>;
	/** Sign a request with RFC 9421 HTTP message signatures */
	signRequest: (request: Request) => Promise<Request>;
	/** Sign headers with RFC 9421 HTTP message signatures */
	signHeaders: (options: Omit<SignHeadersOptions, "signingKey" | "expiresIn">) => Promise<void>;
	/** The signing key (for token mode, can be used to issue sub-tokens) */
	signingKey: SigningKey;
};

/**
 * Creates a PKI auth context.
 *
 * @example Token + SigningKey mode (for regular operations)
 * ```typescript
 * const auth = createPkiAuth({ token: accessToken, signingKey });
 * const s2 = new S2({ authContext: auth, endpoints: { ... } });
 * ```
 *
 * @example Root key mode (for all operations)
 * ```typescript
 * const auth = createPkiAuth({ rootKey: "..." });
 * const s2 = new S2({ authContext: auth, endpoints: { ... } });
 * ```
 */
export function createPkiAuth(config: PkiAuthConfig): PkiAuthContext {
	const { signatureExpiresIn = 300 } = config;

	if (config.token && config.signingKey) {
		return createTokenAuth(config.token, config.signingKey, signatureExpiresIn);
	} else if (config.rootKey) {
		return createRootKeyAuth(config.rootKey, config.tokenExpiresIn ?? 3600, signatureExpiresIn);
	} else if (config.token && !config.signingKey) {
		throw new Error("PKI auth: token provided without signingKey");
	} else if (!config.token && config.signingKey) {
		throw new Error("PKI auth: signingKey provided without token");
	} else {
		throw new Error("PKI auth: either (token + signingKey) or rootKey required");
	}
}

function createTokenAuth(
	token: string,
	signingKey: SigningKey,
	signatureExpiresIn: number,
): PkiAuthContext {
	const publicKey = signingKey.publicKeyBase58();

	function isAccessTokenEndpoint(path: string): boolean {
		return /^\/v\d+\/access-tokens(\/[^\/]+)?$/.test(path);
	}

	function extractPath(urlOrRequest: string | Request): string {
		const url = typeof urlOrRequest === "string" ? new URL(urlOrRequest) : new URL(urlOrRequest.url);
		return url.pathname;
	}

	return {
		mode: "token",
		publicKey,
		signingKey,

		async getToken(): Promise<string> {
			return token;
		},

		async signRequest(request: Request): Promise<Request> {
			const path = extractPath(request);
			if (isAccessTokenEndpoint(path)) {
				throw new Error("Token mode cannot be used for access token endpoints. Use root key mode instead.");
			}
			return signRequest({ request, signingKey, expiresIn: signatureExpiresIn });
		},

		async signHeaders(options: Omit<SignHeadersOptions, "signingKey" | "expiresIn">): Promise<void> {
			const path = extractPath(options.url);
			if (isAccessTokenEndpoint(path)) {
				throw new Error("Token mode cannot be used for access token endpoints. Use root key mode instead.");
			}
			return signHeaders({ ...options, signingKey, expiresIn: signatureExpiresIn });
		},
	};
}

function createRootKeyAuth(
	rootKey: string,
	tokenExpiresIn: number,
	signatureExpiresIn: number,
): PkiAuthContext {
	const clientKey = SigningKey.generate();
	const clientPublicKey = clientKey.publicKeyBase58();

	let cachedToken: string | null = null;
	let tokenExpiry: number = 0;

	async function getToken(): Promise<string> {
		const now = Date.now();
		if (!cachedToken || now >= tokenExpiry - 60000) {
			cachedToken = await createBiscuitToken({
				privateKey: rootKey,
				publicKey: clientPublicKey,
				expiresIn: tokenExpiresIn,
			});
			tokenExpiry = now + tokenExpiresIn * 1000;
		}
		return cachedToken;
	}

	return {
		mode: "root",
		publicKey: clientPublicKey,
		signingKey: clientKey,
		getToken,

		async signRequest(request: Request): Promise<Request> {
			return signRequest({ request, signingKey: clientKey, expiresIn: signatureExpiresIn });
		},

		async signHeaders(options: Omit<SignHeadersOptions, "signingKey" | "expiresIn">): Promise<void> {
			return signHeaders({ ...options, signingKey: clientKey, expiresIn: signatureExpiresIn });
		},
	};
}
