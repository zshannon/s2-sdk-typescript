import { S2AccessTokens } from "./accessTokens.js";
import { S2Basin } from "./basin.js";
import { S2Basins } from "./basins.js";
import type {
	RetryConfig,
	S2ClientOptions,
	S2Compression,
	S2RequestOptions,
} from "./common.js";
import { S2Endpoints } from "./endpoints.js";
import { makeServerError, S2Error, withS2Data } from "./error.js";
import { createClient, createConfig } from "./generated/client/index.js";
import type { Client } from "./generated/client/types.gen.js";
import { configurePkiClient } from "./internal/pki-client.js";
import * as Redacted from "./lib/redacted.js";
import { withRetries } from "./lib/retry.js";
import {
	canSetUserAgentHeader,
	DEFAULT_USER_AGENT,
} from "./lib/stream/runtime.js";
import type { Http2Settings } from "./lib/stream/types.js";
import { S2Locations } from "./locations.js";
import { S2Metrics } from "./metrics.js";
import type { PkiCredentials } from "./pki/index.js";

/**
 * Basin names must be 8-48 characters, lowercase alphanumeric and hyphens,
 * cannot start or end with a hyphen.
 */
const BASIN_NAME_REGEX = /^[a-z0-9][a-z0-9-]{6,46}[a-z0-9]$/;

/**
 * Top-level S2 SDK client.
 *
 * Authenticates with a Bearer access token or PKI v2 credentials and exposes
 * account-scoped helpers for basins, streams and metrics.
 */
export class S2 {
	private readonly accessToken?: Redacted.Redacted;
	private readonly pki?: PkiCredentials;
	private readonly client: Client;
	private readonly endpoints: S2Endpoints;
	private readonly retryConfig: RetryConfig;
	private readonly compression?: S2Compression;
	private readonly http2?: Http2Settings;

	/**
	 * Account-scoped basin management operations.
	 *
	 * - List, create, ensure, delete and reconfigure basins.
	 */
	public readonly basins: S2Basins;
	/** Manage access tokens for the account (list, issue, revoke). */
	public readonly accessTokens: S2AccessTokens;
	/** Account location operations. */
	public readonly locations: S2Locations;
	/** Account, basin and stream level metrics. */
	public readonly metrics: S2Metrics;

	/**
	 * Create a new S2 client.
	 *
	 * @param options Authentication and connection configuration.
	 */
	constructor(options: S2ClientOptions) {
		if ((options.accessToken !== undefined) === (options.pki !== undefined))
			throw new S2Error({
				message: "Choose exactly one of accessToken or PKI credentials",
				origin: "sdk",
			});
		this.pki = options.pki;
		this.accessToken =
			options.accessToken !== undefined
				? Redacted.make(options.accessToken)
				: undefined;
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
				: new S2Endpoints(
						options.endpoints ??
							(this.pki
								? { account: this.pki.audience, basin: this.pki.audience }
								: undefined),
					);
		if (
			this.pki &&
			[
				this.endpoints.accountBaseUrl(),
				this.endpoints.basinBaseUrl("example-basin"),
			].some((endpoint) => new URL(endpoint).origin !== this.pki!.audience)
		)
			throw new S2Error({
				message: "PKI endpoints must match the grant audience",
				origin: "sdk",
			});
		this.compression = options.compression;
		// Copy so later mutation of the caller's object cannot skew pool keying.
		this.http2 = options.http2 ? { ...options.http2 } : undefined;
		const headers: Record<string, string> = {};
		if (canSetUserAgentHeader()) {
			headers["user-agent"] = DEFAULT_USER_AGENT;
		}
		this.client = createClient(
			createConfig({
				baseUrl: this.endpoints.accountBaseUrl(),
				auth: this.accessToken
					? () => Redacted.value(this.accessToken!)
					: undefined,
				headers: headers,
			}),
		);

		configurePkiClient(this.client, this.pki);

		this.client.interceptors.error.use((err, res) => {
			return makeServerError(res, err);
		});

		this.basins = new S2Basins(this.client, this.retryConfig);
		this.accessTokens = new S2AccessTokens(
			this.client,
			this.retryConfig,
			!!this.pki,
		);
		this.locations = new S2Locations(this.client, this.retryConfig);
		this.metrics = new S2Metrics(this.client, this.retryConfig);
	}

	/** Revoke a Biscuit grant and its descendants as its issuer or ancestor holder. */
	public async revokeGrant(
		grant: string,
		options?: S2RequestOptions,
	): Promise<void> {
		if (!this.pki)
			throw new S2Error({
				message: "Grant revocation requires PKI authentication",
				origin: "sdk",
			});
		await withRetries(this.retryConfig, () =>
			withS2Data(() =>
				this.client.post({
					url: "/pki/revocations",
					body: { grant },
					headers: { "Content-Type": "application/json" },
					...options,
				}),
			),
		);
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
			accessToken: this.accessToken,
			pki: this.pki,
			baseUrl: this.endpoints.basinBaseUrl(name),
			includeBasinHeader: this.endpoints.includeBasinHeader,
			retryConfig: this.retryConfig,
			compression: this.compression,
			http2: this.http2,
		});
	}
}
