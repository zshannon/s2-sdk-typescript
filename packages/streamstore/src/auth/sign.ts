import { p256 } from "@noble/curves/nist.js";
import { sha256 } from "@noble/hashes/sha256";
import { base64 } from "@scure/base";
import { SigningKey } from "./signing-key.js";

export type SignRequestOptions = {
	/** Request to sign */
	request: Request;
	/** P256 signing key */
	signingKey: SigningKey;
	/** Duration until signature expires (default: 300 seconds) */
	expiresIn?: number;
};

/**
 * Build the signature base string per RFC 9421.
 * The @signature-params line uses the same parameter order as the Rust httpsig crate:
 * created, alg, keyid (to ensure compatibility with server verification).
 */
function buildSignatureBase(
	method: string,
	path: string,
	authority: string,
	headers: Headers,
	components: string[],
	created: number,
	keyid: string,
): string {
	const lines: string[] = [];

	for (const component of components) {
		let value: string;
		if (component === "@method") {
			value = method.toUpperCase();
		} else if (component === "@path") {
			value = path;
		} else if (component === "@authority") {
			value = authority;
		} else {
			// Regular header
			value = headers.get(component) ?? "";
		}
		lines.push(`"${component}": ${value}`);
	}

	// Build signature-params with Rust httpsig order: created, alg, keyid
	const componentsList = components.map((c) => `"${c}"`).join(" ");
	const signatureParams = `(${componentsList});created=${created};alg="ecdsa-p256-sha256";keyid="${keyid}"`;
	lines.push(`"@signature-params": ${signatureParams}`);

	return lines.join("\n");
}

/**
 * Extract authority from URL, stripping default ports per RFC 9421.
 */
function getAuthority(url: URL): string {
	const port = url.port ? parseInt(url.port, 10) : null;
	if (port && ![80, 443].includes(port)) {
		return `${url.hostname}:${port}`;
	}
	return url.hostname;
}

/**
 * Signs a Request using RFC 9421 HTTP Message Signatures with a P256 key.
 *
 * Components signed: @method, @path, @authority, authorization, content-digest (if body present)
 *
 * @returns A new Request with signature headers added
 */
export async function signRequest(
	options: SignRequestOptions,
): Promise<Request> {
	const { request, signingKey } = options;

	// Clone request headers
	const headers = new Headers(request.headers);

	// Determine components to sign
	const components: string[] = [
		"@method",
		"@path",
		"@authority",
		"authorization",
	];

	// Compute content-digest if body exists
	const body = await request.clone().arrayBuffer();
	if (body.byteLength > 0) {
		const hash = sha256(new Uint8Array(body));
		const hashBase64 = base64.encode(hash);
		headers.set("Content-Digest", `sha-256=:${hashBase64}:`);
		components.push("content-digest");
	}

	// Extract URL components
	// Per RFC 9421: @path is the path without query string
	const url = new URL(request.url);
	const path = url.pathname;
	const authority = getAuthority(url);

	// Get signing parameters
	const created = Math.floor(Date.now() / 1000);
	const keyid = signingKey.publicKeyBase58();
	const privateKeyBytes = signingKey.getPrivateKeyBytes();

	// Build signature base (RFC 9421 format, Rust httpsig-compatible param order)
	const signatureBase = buildSignatureBase(
		request.method,
		path,
		authority,
		headers,
		components,
		created,
		keyid,
	);

	// Sign the signature base
	const signatureBytes = p256.sign(
		new TextEncoder().encode(signatureBase),
		privateKeyBytes,
		{ lowS: true },
	);
	const signatureB64 = base64.encode(signatureBytes);

	// Build signature-input header (same param order as signature base)
	const componentsList = components.map((c) => `"${c}"`).join(" ");
	const signatureInput = `sig1=(${componentsList});created=${created};alg="ecdsa-p256-sha256";keyid="${keyid}"`;

	// Apply signature headers
	headers.set("Signature", `sig1=:${signatureB64}:`);
	headers.set("Signature-Input", signatureInput);

	// Create new request with signed headers
	return new Request(request.url, {
		body: body.byteLength > 0 ? body : undefined,
		headers,
		method: request.method,
	});
}

export type SignHeadersOptions = {
	/** HTTP method */
	method: string;
	/** Full URL */
	url: string;
	/** Headers to sign (will be mutated to add signature headers) */
	headers: Record<string, string>;
	/** P256 signing key */
	signingKey: SigningKey;
	/** Request body (optional, for content-digest) */
	body?: Uint8Array;
	/** Duration until signature expires (default: 300 seconds) */
	expiresIn?: number;
};

/**
 * Signs HTTP headers using RFC 9421 HTTP Message Signatures with a P256 key.
 * Mutates the headers object to add signature headers.
 *
 * Components signed: @method, @path, @authority, authorization, content-digest (if body present)
 */
export async function signHeaders(options: SignHeadersOptions): Promise<void> {
	const { method, url, headers, signingKey, body, expiresIn = 300 } = options;

	// Build headers (skip pseudo-headers for HTTP/2)
	const requestHeaders = new Headers();
	for (const [key, value] of Object.entries(headers)) {
		if (!key.startsWith(":")) {
			requestHeaders.set(key, value);
		}
	}

	const request = new Request(url, {
		method,
		headers: requestHeaders,
		body: body && body.byteLength > 0 ? body.slice().buffer : undefined,
	});

	const signedRequest = await signRequest({ request, signingKey, expiresIn });

	// Extract signature headers and apply to original headers object
	const signature = signedRequest.headers.get("Signature");
	const signatureInput = signedRequest.headers.get("Signature-Input");
	const contentDigest = signedRequest.headers.get("Content-Digest");

	if (signature) headers["signature"] = signature;
	if (signatureInput) headers["signature-input"] = signatureInput;
	if (contentDigest) headers["content-digest"] = contentDigest;
}
