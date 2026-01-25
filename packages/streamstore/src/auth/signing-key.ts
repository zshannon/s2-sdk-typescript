import { p256 } from "@noble/curves/nist.js";
import { base58 } from "@scure/base";

/**
 * P-256 signing key for RFC 9421 HTTP message signatures.
 *
 * When configured, the SDK will sign every HTTP request per RFC 9421,
 * binding the request to both the bearer token and this cryptographic key.
 *
 * @example
 * ```typescript
 * // Generate a new key
 * const key = SigningKey.generate();
 * console.log("Public key:", key.publicKeyBase58());
 *
 * // Import from base58
 * const key = SigningKey.fromBase58("your-base58-encoded-private-key");
 * ```
 */
export class SigningKey {
	private readonly privateKeyBytes: Uint8Array;

	private constructor(privateKeyBytes: Uint8Array) {
		this.privateKeyBytes = privateKeyBytes;
	}

	/**
	 * Generate a new random P-256 signing key.
	 */
	static generate(): SigningKey {
		const privateKey = p256.utils.randomSecretKey();
		return new SigningKey(privateKey);
	}

	/**
	 * Create a signing key from a base58-encoded 32-byte private key.
	 */
	static fromBase58(encoded: string): SigningKey {
		const privateKey = base58.decode(encoded);
		if (privateKey.length !== 32) {
			throw new Error(
				`Invalid signing key: expected 32 bytes, got ${privateKey.length}`,
			);
		}
		return new SigningKey(privateKey);
	}

	/**
	 * Export the private key as a base58-encoded string.
	 */
	toBase58(): string {
		return base58.encode(this.privateKeyBytes);
	}

	/**
	 * Get the public key as a base58-encoded compressed point (33 bytes).
	 */
	publicKeyBase58(): string {
		const publicKey = p256.getPublicKey(this.privateKeyBytes, true);
		return base58.encode(publicKey);
	}

	/**
	 * Get the raw private key bytes (for internal use).
	 * @internal
	 */
	getPrivateKeyBytes(): Uint8Array {
		return this.privateKeyBytes;
	}
}
