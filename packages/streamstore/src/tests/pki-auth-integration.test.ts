/**
 * Integration tests for PKI auth behavior.
 *
 * Key behavior:
 * - Root key mode: Uses bootstrap token (signed by root, public_key=root) for all operations
 * - Token + SigningKey mode: For all endpoints except /access-tokens
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { createPkiAuth } from "../auth/pki-auth.js";
import { SigningKey } from "../auth/signing-key.js";
import { S2 } from "../s2.js";

const TEST_ROOT_KEY = "ByDGSRM82bqEVQoGYpZzvmmHujrB32UN1sr7WbKN6TPQ";

describe("PKI Auth Integration", () => {
	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it("signs fetch requests made by the SDK client", async () => {
		let request: Request | undefined;
		vi.stubGlobal(
			"fetch",
			async (input: RequestInfo | URL, init?: RequestInit) => {
				request = new Request(input, init);
				return Response.json({ basins: [], has_more: false });
			},
		);

		const s2 = new S2({
			rootKey: TEST_ROOT_KEY,
			endpoints: {
				account: "https://example.com/v1",
				basin: "https://example.com/v1",
			},
		});
		await s2.basins.list();

		expect(request?.headers.get("authorization")).toMatch(/^Bearer /);
		expect(request?.headers.get("signature")).toBeTruthy();
		expect(request?.headers.get("signature-input")).toBeTruthy();
	});

	describe("root key mode signs all endpoints with root key", () => {
		it("allows signing requests to /access-tokens endpoints", async () => {
			const auth = createPkiAuth({ rootKey: TEST_ROOT_KEY });

			const request = new Request("https://example.com/v1/access-tokens", {
				method: "POST",
				headers: { authorization: "Bearer test" },
			});

			const signed = await auth.signRequest(request);
			expect(signed.headers.get("signature")).toBeTruthy();
		});

		it("allows signing requests to /access-tokens/{id} endpoints", async () => {
			const auth = createPkiAuth({ rootKey: TEST_ROOT_KEY });

			const request = new Request(
				"https://example.com/v1/access-tokens/token-123",
				{
					method: "DELETE",
					headers: { authorization: "Bearer test" },
				},
			);

			const signed = await auth.signRequest(request);
			expect(signed.headers.get("signature")).toBeTruthy();
		});

		it("signs /basins endpoints with root key", async () => {
			const auth = createPkiAuth({ rootKey: TEST_ROOT_KEY });

			const request = new Request("https://example.com/v1/basins", {
				method: "GET",
				headers: { authorization: "Bearer test" },
			});

			const signed = await auth.signRequest(request);
			expect(signed.headers.get("signature")).toBeTruthy();
		});

		it("signs /basins/{name} endpoints with root key", async () => {
			const auth = createPkiAuth({ rootKey: TEST_ROOT_KEY });

			const request = new Request("https://example.com/v1/basins/my-basin", {
				method: "GET",
				headers: { authorization: "Bearer test" },
			});

			const signed = await auth.signRequest(request);
			expect(signed.headers.get("signature")).toBeTruthy();
		});

		it("signs stream endpoints with root key", async () => {
			const auth = createPkiAuth({ rootKey: TEST_ROOT_KEY });

			const request = new Request(
				"https://example.com/v1/streams/my-stream/records",
				{
					method: "GET",
					headers: { authorization: "Bearer test" },
				},
			);

			const signed = await auth.signRequest(request);
			expect(signed.headers.get("signature")).toBeTruthy();
		});

		it("signs headers on stream endpoints with root key", async () => {
			const auth = createPkiAuth({ rootKey: TEST_ROOT_KEY });

			const headers: Record<string, string> = {
				":method": "GET",
				":path": "/v1/streams/my-stream/records",
				authorization: "Bearer test",
			};

			await auth.signHeaders({
				method: "GET",
				url: "https://example.com/v1/streams/my-stream/records",
				headers,
			});

			expect(headers["signature"]).toBeTruthy();
		});
	});

	describe("token + signingKey mode allows all endpoints", () => {
		it("signs requests to /basins endpoints", async () => {
			const signingKey = SigningKey.generate();
			const auth = createPkiAuth({
				token: "access-token",
				signingKey,
			});

			const request = new Request("https://example.com/v1/basins", {
				method: "GET",
				headers: { authorization: "Bearer access-token" },
			});

			const signed = await auth.signRequest(request);
			expect(signed.headers.get("signature")).toBeTruthy();
		});

		it("signs requests to stream endpoints", async () => {
			const signingKey = SigningKey.generate();
			const auth = createPkiAuth({
				token: "access-token",
				signingKey,
			});

			const request = new Request(
				"https://example.com/v1/streams/my-stream/records",
				{
					method: "GET",
					headers: { authorization: "Bearer access-token" },
				},
			);

			const signed = await auth.signRequest(request);
			expect(signed.headers.get("signature")).toBeTruthy();
		});

		it("signs headers for stream endpoints", async () => {
			const signingKey = SigningKey.generate();
			const auth = createPkiAuth({
				token: "access-token",
				signingKey,
			});

			const headers: Record<string, string> = {
				":method": "GET",
				":path": "/v1/streams/my-stream/records",
				authorization: "Bearer access-token",
			};

			await auth.signHeaders({
				method: "GET",
				url: "https://example.com/v1/streams/my-stream/records",
				headers,
			});

			expect(headers["signature"]).toBeTruthy();
		});

		it("rejects signing requests to /access-tokens endpoints (token mode cannot issue tokens)", async () => {
			const signingKey = SigningKey.generate();
			const auth = createPkiAuth({
				token: "access-token",
				signingKey,
			});

			const request = new Request("https://example.com/v1/access-tokens", {
				method: "POST",
				headers: { authorization: "Bearer access-token" },
			});

			// Token mode should NOT be able to issue new access tokens
			await expect(auth.signRequest(request)).rejects.toThrow(
				"Token mode cannot be used for access token endpoints",
			);
		});
	});
});
