import type { Client } from "../generated/client/types.gen.js";
import type { PkiCredentials } from "../pki/index.js";

/** Sign final serialized bytes anew for every generated-client invocation. */
export function configurePkiClient(
	client: Client,
	credentials?: PkiCredentials,
): void {
	if (credentials)
		client.interceptors.request.use((request) =>
			credentials.signRequest(request),
		);
}
