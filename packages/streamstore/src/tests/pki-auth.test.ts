import { describe, expect, it } from "vitest";
import { createBiscuitToken, derivePublicKey } from "../auth/biscuit.js";
import { createPkiAuth } from "../auth/pki-auth.js";
import { SigningKey } from "../auth/signing-key.js";
import { signRequest, signHeaders } from "../auth/sign.js";

// Test key pair (P256)
const TEST_PRIVATE_KEY = "ByDGSRM82bqEVQoGYpZzvmmHujrB32UN1sr7WbKN6TPQ";
const TEST_PUBLIC_KEY = "pTGh6RCaGt5PcA3evMKB6ZZmsYfALRSPhCH9tq3xzEsW";

describe("derivePublicKey", () => {
	it("derives correct public key from private key", () => {
		const publicKey = derivePublicKey(TEST_PRIVATE_KEY);
		expect(publicKey).toBe(TEST_PUBLIC_KEY);
	});

	it("throws on invalid private key length", () => {
		expect(() => derivePublicKey("short")).toThrow("expected 32 bytes");
	});
});

describe("createBiscuitToken", () => {
	it("creates a valid base64-encoded token", async () => {
		const token = await createBiscuitToken({
			privateKey: TEST_PRIVATE_KEY,
			expiresIn: 3600,
		});

		expect(token).toBeTruthy();
		expect(typeof token).toBe("string");
		// Base64 encoded token should be decodable
		expect(() => atob(token)).not.toThrow();
	});

	it("creates token with custom op groups", async () => {
		const token = await createBiscuitToken({
			privateKey: TEST_PRIVATE_KEY,
			opGroups: [{ level: "stream", access: "read" }],
		});

		expect(token).toBeTruthy();
	});

	it("creates token with custom scopes", async () => {
		const token = await createBiscuitToken({
			privateKey: TEST_PRIVATE_KEY,
			basinScope: { type: "exact", value: "my-basin" },
			streamScope: { type: "prefix", value: "logs-" },
		});

		expect(token).toBeTruthy();
	});
});

describe("signRequest", () => {
	const signingKey = SigningKey.fromBase58(TEST_PRIVATE_KEY);

	it("adds signature headers to request", async () => {
		const request = new Request("https://example.com/v1/test", {
			method: "GET",
			headers: {
				authorization: "Bearer test-token",
			},
		});

		const signedRequest = await signRequest({ request, signingKey });

		expect(signedRequest.headers.get("signature")).toBeTruthy();
		expect(signedRequest.headers.get("signature-input")).toBeTruthy();
	});

	it("adds content-digest for requests with body", async () => {
		const request = new Request("https://example.com/v1/test", {
			method: "POST",
			headers: {
				authorization: "Bearer test-token",
				"content-type": "application/json",
			},
			body: JSON.stringify({ foo: "bar" }),
		});

		const signedRequest = await signRequest({ request, signingKey });

		expect(signedRequest.headers.get("content-digest")).toMatch(/^sha-256=:/);
		expect(signedRequest.headers.get("signature")).toBeTruthy();
	});

	it("preserves original request properties", async () => {
		const request = new Request("https://example.com/v1/test?foo=bar", {
			method: "POST",
			headers: {
				authorization: "Bearer test-token",
				"x-custom": "value",
			},
			body: "test body",
		});

		const signedRequest = await signRequest({ request, signingKey });

		expect(signedRequest.url).toBe("https://example.com/v1/test?foo=bar");
		expect(signedRequest.method).toBe("POST");
		expect(signedRequest.headers.get("x-custom")).toBe("value");
	});
});

describe("signHeaders", () => {
	const signingKey = SigningKey.fromBase58(TEST_PRIVATE_KEY);

	it("adds signature headers to header object", async () => {
		const headers: Record<string, string> = {
			":method": "GET",
			":path": "/v1/test",
			":scheme": "https",
			":authority": "example.com",
			authorization: "Bearer test-token",
		};

		await signHeaders({
			method: "GET",
			url: "https://example.com/v1/test",
			headers,
			signingKey,
		});

		expect(headers["signature"]).toBeTruthy();
		expect(headers["signature-input"]).toBeTruthy();
	});

	it("adds content-digest when body is provided", async () => {
		const headers: Record<string, string> = {
			":method": "POST",
			":path": "/v1/test",
			":scheme": "https",
			":authority": "example.com",
			authorization: "Bearer test-token",
		};

		const body = new TextEncoder().encode('{"foo":"bar"}');

		await signHeaders({
			method: "POST",
			url: "https://example.com/v1/test",
			headers,
			signingKey,
			body,
		});

		expect(headers["content-digest"]).toMatch(/^sha-256=:/);
		expect(headers["signature"]).toBeTruthy();
	});
});

describe("SigningKey", () => {
	it("generates a new random key", () => {
		const key = SigningKey.generate();
		expect(key.publicKeyBase58()).toBeTruthy();
		expect(key.publicKeyBase58().length).toBeGreaterThan(40);
	});

	it("imports from base58", () => {
		const key = SigningKey.fromBase58(TEST_PRIVATE_KEY);
		expect(key.publicKeyBase58()).toBe(TEST_PUBLIC_KEY);
	});

	it("exports to base58", () => {
		const key = SigningKey.fromBase58(TEST_PRIVATE_KEY);
		expect(key.toBase58()).toBe(TEST_PRIVATE_KEY);
	});

	it("throws on invalid key length", () => {
		expect(() => SigningKey.fromBase58("short")).toThrow("expected 32 bytes");
	});
});

describe("createPkiAuth", () => {
	describe("root key mode", () => {
		it("creates auth context with mode=root", () => {
			const auth = createPkiAuth({ rootKey: TEST_PRIVATE_KEY });

			expect(auth.mode).toBe("root");
			// publicKey is the generated client key (not the root key)
			expect(auth.publicKey).toBeTruthy();
			expect(auth.publicKey.length).toBeGreaterThan(40); // base58 compressed pubkey
			expect(auth.publicKey).not.toBe(TEST_PUBLIC_KEY); // NOT the root key
		});

		it("generates and caches tokens", async () => {
			const auth = createPkiAuth({ rootKey: TEST_PRIVATE_KEY });

			const token1 = await auth.getToken();
			const token2 = await auth.getToken();

			expect(token1).toBeTruthy();
			expect(token1).toBe(token2); // Should be cached
		});

		it("signs requests with client key", async () => {
			const auth = createPkiAuth({ rootKey: TEST_PRIVATE_KEY });

			const request = new Request("https://example.com/v1/access-tokens", {
				method: "POST",
				headers: {
					authorization: "Bearer test-token",
				},
			});

			const signedRequest = await auth.signRequest(request);

			expect(signedRequest.headers.get("signature")).toBeTruthy();
			expect(signedRequest.headers.get("signature-input")).toBeTruthy();
			// Signature should use CLIENT key's public key as keyid (not root key)
			expect(signedRequest.headers.get("signature-input")).toContain(auth.publicKey);
			expect(signedRequest.headers.get("signature-input")).not.toContain(TEST_PUBLIC_KEY);
		});
	});

	describe("token + signingKey mode", () => {
		it("creates auth context with mode=token", () => {
			const signingKey = SigningKey.generate();
			const auth = createPkiAuth({
				token: "test-token",
				signingKey,
			});

			expect(auth.mode).toBe("token");
			expect(auth.publicKey).toBe(signingKey.publicKeyBase58());
		});

		it("returns the provided token", async () => {
			const signingKey = SigningKey.generate();
			const auth = createPkiAuth({
				token: "my-access-token",
				signingKey,
			});

			const token = await auth.getToken();
			expect(token).toBe("my-access-token");
		});

		it("signs requests with the signing key", async () => {
			const signingKey = SigningKey.generate();
			const auth = createPkiAuth({
				token: "my-access-token",
				signingKey,
			});

			const request = new Request("https://example.com/v1/basins", {
				method: "GET",
				headers: {
					authorization: "Bearer my-access-token",
				},
			});

			const signedRequest = await auth.signRequest(request);

			expect(signedRequest.headers.get("signature")).toBeTruthy();
			expect(signedRequest.headers.get("signature-input")).toBeTruthy();
			// Signature should use signing key's public key as keyid
			expect(signedRequest.headers.get("signature-input")).toContain(signingKey.publicKeyBase58());
		});

		it("signs headers with the signing key", async () => {
			const signingKey = SigningKey.generate();
			const auth = createPkiAuth({
				token: "my-access-token",
				signingKey,
			});

			const headers: Record<string, string> = {
				":method": "GET",
				":path": "/v1/streams/test/records",
				":scheme": "https",
				":authority": "example.com",
				authorization: "Bearer my-access-token",
			};

			await auth.signHeaders({
				method: "GET",
				url: "https://example.com/v1/streams/test/records",
				headers,
			});

			expect(headers["signature"]).toBeTruthy();
			expect(headers["signature-input"]).toBeTruthy();
			// Signature should use signing key's public key as keyid
			expect(headers["signature-input"]).toContain(signingKey.publicKeyBase58());
		});
	});

	describe("validation", () => {
		it("throws when token provided without signingKey", () => {
			expect(() => createPkiAuth({ token: "test-token" })).toThrow(
				"token provided without signingKey"
			);
		});

		it("throws when signingKey provided without token", () => {
			const signingKey = SigningKey.generate();
			expect(() => createPkiAuth({ signingKey })).toThrow(
				"signingKey provided without token"
			);
		});

		it("throws when neither rootKey nor token+signingKey provided", () => {
			expect(() => createPkiAuth({})).toThrow(
				"either (token + signingKey) or rootKey required"
			);
		});
	});
});
