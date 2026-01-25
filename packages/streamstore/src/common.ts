import type { PkiAuthContext } from "./auth/pki-auth.js";
import { S2Endpoints, type S2EndpointsInit } from "./endpoints.js";
import { createClient, createConfig } from "./generated/client/index.js";
import type { Client } from "./generated/client/types.gen.js";

/**
 * Policy for retrying append operations.
 *
 * - `all`: Retry all append operations, including those that may have side effects (default)
 * - `noSideEffects`: Only retry append operations that are guaranteed to have no side effects
 */
export type AppendRetryPolicy = "all" | "noSideEffects";

/**
 * Auth provider that abstracts token-based and PKI-based authentication.
 */
export type AuthProvider =
	| { type: "token"; token: string }
	| { type: "pki"; context: PkiAuthContext };

/**
 * Retry configuration for handling transient failures.
 */
export type RetryConfig = {
	/**
	 * Total number of attempts, including the initial try.
	 * Must be >= 1. A value of 1 means no retries.
	 * @default 3
	 */
	maxAttempts?: number;

	/**
	 * Minimum delay in milliseconds for exponential backoff.
	 * The first retry will have a delay in the range [minBaseDelayMillis, 2*minBaseDelayMillis).
	 * @default 100
	 */
	minBaseDelayMillis?: number;

	/**
	 * Maximum base delay in milliseconds for exponential backoff.
	 * Once the exponential backoff reaches this value, it stays capped here.
	 * Note: actual delay with jitter can be up to 2*maxBaseDelayMillis.
	 * @default 1000
	 */
	maxBaseDelayMillis?: number;

	/**
	 * Policy for retrying append operations.
	 * @default "all"
	 */
	appendRetryPolicy?: AppendRetryPolicy;

	/**
	 * Maximum time in milliseconds to wait for an append ack before considering
	 * the attempt timed out and applying retry logic.
	 *
	 * Used by retrying append sessions. When unset, defaults to 5000ms.
	 *
	 * @deprecated Use `requestTimeoutMillis` on {@link S2ClientOptions} instead.
	 */
	requestTimeoutMillis?: number;

	/**
	 * Maximum time in milliseconds to wait for connection establishment.
	 * This is a "fail fast" timeout that aborts slow connections early.
	 * Connection time counts toward requestTimeoutMillis.
	 *
	 * Only applies to S2S (HTTP/2) transport when establishing new connections.
	 * Reused pooled connections are not subject to this timeout.
	 *
	 * @default 3000
	 * @deprecated Use `connectionTimeoutMillis` on {@link S2ClientOptions} instead.
	 */
	connectionTimeoutMillis?: number;
};

export type S2EnvironmentConfig = Partial<S2ClientOptions>;

export class S2Environment {
	public static parse(): S2EnvironmentConfig {
		const config: S2EnvironmentConfig = {};

		const token = process.env.S2_ACCESS_TOKEN;
		if (token) {
			config.accessToken = token;
		}

		const rootKey = process.env.S2_ROOT_KEY;
		if (rootKey) {
			config.rootKey = rootKey;
		}

		const accountEndpoint = process.env.S2_ACCOUNT_ENDPOINT;
		const basinEndpoint = process.env.S2_BASIN_ENDPOINT;
		if (accountEndpoint || basinEndpoint) {
			const endpointsInit: S2EndpointsInit = {
				account: accountEndpoint || undefined,
				basin: basinEndpoint || undefined,
			};
			config.endpoints = new S2Endpoints(endpointsInit);
		}

		return config;
	}
}

/**
 * Configuration for constructing the top-level `S2` client.
 *
 * Provide one of:
 * - `accessToken` for Bearer token auth (legacy)
 * - `rootKey` for PKI bootstrap mode (limited to admin operations)
 * - `authContext` for PKI proper auth (full access)
 */
export type S2ClientOptions = {
	/**
	 * Access token used for HTTP Bearer authentication.
	 * Typically obtained via your S2 account or created using `s2.accessTokens.issue`.
	 *
	 * Mutually exclusive with `rootKey` and `authContext`.
	 */
	accessToken?: string;
	/**
	 * P256 private key as base58-encoded 32 bytes for PKI bootstrap mode.
	 * When provided, the SDK generates short-lived Biscuit tokens on-the-fly
	 * and signs all requests with RFC 9421 HTTP message signatures.
	 *
	 * Bootstrap mode is limited to admin operations (list basins, issue tokens).
	 * For full access including stream operations, use `authContext` instead.
	 *
	 * Mutually exclusive with `accessToken` and `authContext`.
	 */
	rootKey?: string;
	/**
	 * Pre-created PKI auth context for proper auth mode.
	 * Create with `createPkiAuth({ token, signingKey })`.
	 *
	 * This is the recommended mode for full access to all operations including streams.
	 *
	 * Mutually exclusive with `accessToken` and `rootKey`.
	 */
	authContext?: PkiAuthContext;
	/**
	 * Endpoint configuration for the S2 environment.
	 *
	 * Defaults to AWS (`aws.s2.dev` and `{basin}.b.aws.s2.dev`) with the API base path inferred as `/v1`.
	 */
	endpoints?: S2Endpoints | S2EndpointsInit;
	/**
	 * Maximum time in milliseconds to wait for an append ack before considering
	 * the attempt timed out and applying retry logic.
	 *
	 * Used by retrying append sessions. When unset, defaults to 5000ms.
	 */
	requestTimeoutMillis?: number;
	/**
	 * Maximum time in milliseconds to wait for connection establishment.
	 * This is a "fail fast" timeout that aborts slow connections early.
	 * Connection time counts toward requestTimeoutMillis.
	 *
	 * Only applies to S2S (HTTP/2) transport when establishing new connections.
	 * Reused pooled connections are not subject to this timeout.
	 *
	 * @default 3000
	 */
	connectionTimeoutMillis?: number;
	/**
	 * Retry configuration for handling transient failures.
	 * Applies to management operations (basins, streams, tokens) and stream operations (read, append).
	 * @default { maxAttempts: 3, minBaseDelayMillis: 100, maxBaseDelayMillis: 1000, appendRetryPolicy: "all" }
	 */
	retry?: RetryConfig;
};

/**
 * Per-request options that apply to all SDK operations.
 */
export type S2RequestOptions = {
	/**
	 * Optional abort signal to cancel the underlying HTTP request.
	 */
	signal?: AbortSignal;
};

/**
 * Creates a client configured with the given auth provider.
 * Adds request interceptor for PKI signing when using PKI auth.
 */
export function createAuthenticatedClient(
	baseUrl: string,
	authProvider: AuthProvider,
	headers: Record<string, string>,
): Client {
	const client = createClient(
		createConfig({
			baseUrl,
			auth: async () => {
				if (authProvider.type === "pki") {
					return authProvider.context.getToken();
				}
				return authProvider.token;
			},
			headers,
		}),
	);

	if (authProvider.type === "pki") {
		const pkiContext = authProvider.context;
		client.interceptors.request.use(async (request) => {
			return pkiContext.signRequest(request);
		});
	}

	return client;
}
