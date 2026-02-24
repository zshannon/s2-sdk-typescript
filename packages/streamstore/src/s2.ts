import { S2AccessTokens } from "./accessTokens.js";
import { createPkiAuth } from "./auth/pki-auth.js";
import { S2Basin } from "./basin.js";
import { S2Basins } from "./basins.js";
import {
	type AuthProvider,
	createAuthenticatedClient,
	type RetryConfig,
	type S2ClientOptions,
} from "./common.js";
import { S2Endpoints } from "./endpoints.js";
import { makeServerError, S2Error } from "./error.js";
import type { Client } from "./generated/client/types.gen.js";
import {
	canSetUserAgentHeader,
	DEFAULT_USER_AGENT,
} from "./lib/stream/runtime.js";
import { S2Metrics } from "./metrics.js";

/**
 * Basin names must be 8-48 characters, lowercase alphanumeric and hyphens,
 * cannot start or end with a hyphen.
 */
const BASIN_NAME_REGEX = /^[a-z0-9][a-z0-9-]{6,46}[a-z0-9]$/;

/**
 * Top-level S2 SDK client.
 *
 * - Authenticates with an access token or PKI root key.
 * - Exposes account-scoped helpers for basins, streams, access tokens and metrics.
 */
export class S2 {
	private readonly authProvider: AuthProvider;
	private readonly client: Client;
	private readonly endpoints: S2Endpoints;
	private readonly retryConfig: RetryConfig;

	/**
	 * Account-scoped basin management operations.
	 *
	 * - List, create, delete and reconfigure basins.
	 */
	public readonly basins: S2Basins;
	/** Manage access tokens for the account (list, issue, revoke). */
	public readonly accessTokens: S2AccessTokens;
	/** Account, basin and stream level metrics. */
	public readonly metrics: S2Metrics;

	/**
	 * Create a new S2 client.
	 *
	 * @param options Auth configuration (either accessToken or rootKey).
	 */
	constructor(options: S2ClientOptions) {
		// Validate auth options - exactly one must be specified
		const authCount = [
			options.accessToken,
			options.rootKey,
			options.authContext,
		].filter(Boolean).length;
		if (authCount === 0) {
			throw new S2Error({
				message:
					"Must specify one of: accessToken, rootKey, or authContext for authentication.",
				origin: "sdk",
			});
		}
		if (authCount > 1) {
			throw new S2Error({
				message: "Specify only one of: accessToken, rootKey, or authContext.",
				origin: "sdk",
			});
		}

		// Create auth provider
		if (options.authContext) {
			this.authProvider = { type: "pki", context: options.authContext };
		} else if (options.rootKey) {
			const pkiAuth = createPkiAuth({ rootKey: options.rootKey });
			this.authProvider = { type: "pki", context: pkiAuth };
		} else {
			this.authProvider = { type: "token", token: options.accessToken! };
		}

		// Merge timeout config: top-level options take precedence over retry.* for backwards compatibility
		this.retryConfig = {
			...options.retry,
			...(options.requestTimeoutMillis !== undefined && {
				requestTimeoutMillis: options.requestTimeoutMillis,
			}),
			...(options.connectionTimeoutMillis !== undefined && {
				connectionTimeoutMillis: options.connectionTimeoutMillis,
			}),
		};
		this.endpoints =
			options.endpoints instanceof S2Endpoints
				? options.endpoints
				: new S2Endpoints(options.endpoints);
		const headers: Record<string, string> = {};
		if (canSetUserAgentHeader()) {
			headers["user-agent"] = DEFAULT_USER_AGENT;
		}
		this.client = createAuthenticatedClient(
			this.endpoints.accountBaseUrl(),
			this.authProvider,
			headers,
		);
		this.client.interceptors.error.use((err, res) => {
			return makeServerError(res, err);
		});

		this.basins = new S2Basins(this.client, this.retryConfig);
		this.accessTokens = new S2AccessTokens(this.client, this.retryConfig);
		this.metrics = new S2Metrics(this.client, this.retryConfig);
	}

	/**
	 * Create a basin-scoped client bound to a specific basin name.
	 *
	 * @param name Basin name (8-48 characters, lowercase alphanumeric and hyphens, no leading/trailing hyphens).
	 * @throws {S2Error} If the basin name is invalid.
	 */
	public basin(name: string) {
		if (!BASIN_NAME_REGEX.test(name)) {
			throw new S2Error({
				message:
					`Invalid basin name: "${name}". Basin names must be 8-48 characters, ` +
					`contain only lowercase letters, numbers, and hyphens, and cannot start or end with a hyphen.`,
				origin: "sdk",
			});
		}
		return new S2Basin(name, {
			authProvider: this.authProvider,
			baseUrl: this.endpoints.basinBaseUrl(name),
			includeBasinHeader: this.endpoints.includeBasinHeader,
			retryConfig: this.retryConfig,
		});
	}
}
