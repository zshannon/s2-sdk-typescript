# PKI Auth Fix: Access Token Issuance Flow

> **Status:** Planning
> **Date:** 2025-01-24

## Problem Summary

HTTP/1.1 operations (list basins, list streams, checkTail) succeed with PKI auth, but HTTP/2 operations (readSession, appendSession) fail with:

```
S2Error: Invalid signature: signature verification failed: no allowed public key verified signature
```

The root cause is that the TypeScript SDK's PKI auth implementation is incomplete. It uses "bootstrap mode" (root key signs everything) which only works for certain operations, not stream-level HTTP/2 operations.

## Files Consulted

### Server Implementation (s2/lite)

1. **`s2/docs/plans/2025-01-23-authorization-design.md`** - The authoritative design document explaining:
   - Biscuit tokens contain `public_key("...")` facts binding to client keys
   - Root key signs the Biscuit token (authority block)
   - **Client key** (separate from root) signs HTTP requests via RFC 9421
   - Access token endpoints accept root key signing directly
   - Regular endpoints require token + matching client key signature

2. **`s2/docs/plans/2025-01-23-authorization-impl.md`** - Implementation plan showing:
   - Token contains `public_key("<client-pubkey>")` - the **client's** key, not root
   - Server extracts `allowed_public_keys` from token and verifies RFC 9421 signature against them

3. **`s2/lite/docs/authentication.md`** - Usage documentation confirming:
   - "The client's public key must be embedded in the token"
   - Issue token endpoint requires `public_key` in request body
   - Example: `"public_key": "2NEpo7TZRRrLZSi2U8FxKaAqV3FJ8MFmCxLBqQMZxBGZ"`

4. **`s2/lite/src/auth/verify.rs`** - Server verification logic:
   ```rust
   // Extract all public keys from facts (supports delegation)
   let allowed_public_keys = extract_client_public_keys(&biscuit)?;

   // Later, verifies signature against these keys
   if !token.allowed_public_keys.contains(signer_public_key) {
       return Err(AuthorizeError::UnauthorizedSigner);
   }
   ```

5. **`s2/lite/src/handlers/v1/access_tokens.rs`** - Token issuance handler:
   ```rust
   // Require public_key for new auth tokens
   let public_key_str = request
       .public_key
       .ok_or_else(|| ServiceError::Validation("public_key is required".into()))?;
   ```

### CLI Auth Design (s2-cli)

6. **`s2-cli/docs/plans/2025-01-23-cli-auth-design.md`** - CLI auth flow:
   - `signing_key` + `token` are separate config items
   - `signing_key` requires `token` (and vice versa)
   - Bootstrap mode: `root_key` alone creates admin Biscuit on-the-fly
   - Normal mode: issue access token with new keypair, use that for operations

7. **`s2-cli/docs/plans/2025-01-23-sdk-auth-requirements.md`** - SDK requirements:
   ```rust
   let config = S2Config::new(token)
       .with_signing_key(key);  // ← SEPARATE from token
   ```

### Rust SDK (s2-sdk-rust)

8. **`s2-sdk-rust/src/types.rs`** - Correct implementation:
   ```rust
   pub struct S2Config {
       pub(crate) access_token: SecretString,      // The Biscuit token
       pub(crate) signing_key: Option<SigningKey>, // SEPARATE signing key
       // ...
   }

   pub struct IssueAccessTokenInput {
       pub id: Option<AccessTokenId>,      // Legacy
       pub public_key: Option<String>,     // NEW AUTH - binds token to this key
       // ...
   }
   ```

9. **`s2-sdk-rust/src/signing.rs`** - Request signing:
   - Uses `signing_key` from config (not token)
   - Signs with `keyid` = signing key's public key

### TypeScript SDK (Current - Broken)

10. **`packages/streamstore/src/auth/pki-auth.ts`** - Current implementation:
    ```typescript
    // PROBLEM: Uses rootKey for BOTH token creation AND signing
    async signRequest(request: Request): Promise<Request> {
        return signRequest({
            request,
            privateKey: rootKey,  // ← Should be a SEPARATE client key
            expiresIn: signatureExpiresIn,
        });
    }
    ```

11. **`packages/streamstore/src/types.ts`** - Missing field:
    ```typescript
    export interface IssueAccessTokenInput {
        id: string;                    // Legacy only!
        scope: AccessTokenScope;
        // MISSING: publicKey: string  ← Required for new auth!
    }
    ```

12. **`packages/streamstore/src/auth/biscuit.ts`** - Token creation:
    ```typescript
    // Creates token with ROOT key's public key
    const publicKeyBase58 = base58.encode(publicKeyBytes);
    const builder = biscuit.biscuit`
        public_key(${publicKeyBase58});  // ← Root's public key
    `;
    ```

## The Auth Model (Correct Understanding)

### Two Auth Modes

1. **Bootstrap Mode** (Admin only):
   - User has root key
   - Creates admin Biscuit on-the-fly with root's public key
   - Signs requests with root key
   - Works for: Token management, account operations
   - **Limited scope** - not intended for stream operations

2. **Normal Mode** (Proper client auth):
   - Generate NEW client keypair
   - Issue access token via API with new public key
   - Token contains `public_key("<client-pubkey>")`
   - Sign requests with client's private key
   - Works for: ALL operations including streams

### Why HTTP/1.1 Works But HTTP/2 Fails

This was a red herring. The actual issue is:

- HTTP/1.1 operations that work: `listBasins`, `listStreams`, `checkTail`
- HTTP/2 operations that fail: `readSession`, `appendSession`

The difference is NOT HTTP/1.1 vs HTTP/2 protocol. The difference is that certain operations (likely stream read/write) have stricter auth requirements that bootstrap mode doesn't satisfy.

The server likely has authorization rules that:
- Allow bootstrap mode for account/basin management
- Require proper issued tokens for stream operations

## Required Changes

### 1. Add `publicKey` to Types

```typescript
// src/types.ts
export interface IssueAccessTokenInput {
    /** Access token ID (legacy auth). */
    id?: string;
    /** Client's P-256 public key for request signing (new auth). Base58-encoded. */
    publicKey?: string;
    /** Access token scope. */
    scope: AccessTokenScope;
    // ... rest unchanged
}
```

### 2. Create Separate SigningKey Type

```typescript
// src/auth/signing-key.ts
import { p256 } from "@noble/curves/nist.js";
import { base58 } from "@scure/base";

export class SigningKey {
    private privateKey: Uint8Array;

    private constructor(privateKey: Uint8Array) {
        this.privateKey = privateKey;
    }

    static generate(): SigningKey {
        const privateKey = p256.utils.randomPrivateKey();
        return new SigningKey(privateKey);
    }

    static fromBase58(encoded: string): SigningKey {
        const privateKey = base58.decode(encoded);
        if (privateKey.length !== 32) {
            throw new Error(`Invalid key length: ${privateKey.length}`);
        }
        return new SigningKey(privateKey);
    }

    toBase58(): string {
        return base58.encode(this.privateKey);
    }

    publicKeyBase58(): string {
        const publicKey = p256.getPublicKey(this.privateKey, true);
        return base58.encode(publicKey);
    }

    getPrivateKeyBytes(): Uint8Array {
        return this.privateKey;
    }
}
```

### 3. Update S2 Config

```typescript
// src/index.ts - S2Config changes
export interface S2Config {
    // Existing
    accessToken?: string;
    endpoints?: { account: string; basin: string };

    // New PKI auth (proper flow)
    token?: string;           // Biscuit token
    signingKey?: SigningKey;  // Key to sign requests (matches token's public_key)

    // Bootstrap mode (admin only)
    rootKey?: string;         // Creates admin token on-the-fly, limited operations
}
```

### 4. Update Auth Provider Interface

```typescript
// src/lib/auth/provider.ts
export interface AuthProvider {
    getToken(): Promise<string>;
    signRequest(request: Request): Promise<Request>;
    signHttp2Headers(options: SignHttp2HeadersOptions): Promise<void>;

    // NEW: Get the signing key (for proper auth flow)
    getSigningKey?(): SigningKey;
}
```

### 5. Create Proper PKI Auth Flow

```typescript
// src/auth/pki-auth.ts - Updated
export type PkiAuthConfig = {
    // Option 1: Bootstrap mode (limited)
    rootKey?: string;

    // Option 2: Proper auth (full access)
    token?: string;
    signingKey?: SigningKey;
};

export function createPkiAuth(config: PkiAuthConfig): PkiAuthContext {
    if (config.token && config.signingKey) {
        // Proper auth: use provided token + signing key
        return createProperAuth(config.token, config.signingKey);
    } else if (config.rootKey) {
        // Bootstrap mode: limited operations only
        return createBootstrapAuth(config.rootKey);
    } else {
        throw new Error("Either (token + signingKey) or rootKey required");
    }
}
```

### 6. Add Helper to Issue Token with New Key

```typescript
// src/auth/helpers.ts
export async function issueAccessTokenWithNewKey(
    s2: S2,
    scope: AccessTokenScope,
    expiresAt?: Date,
): Promise<{ token: string; signingKey: SigningKey }> {
    // Generate new client keypair
    const signingKey = SigningKey.generate();
    const publicKey = signingKey.publicKeyBase58();

    // Issue token bound to the new key
    const response = await s2.accessTokens.issue({
        publicKey,
        scope,
        expiresAt,
    });

    return {
        token: response.accessToken,
        signingKey,
    };
}
```

### 7. Update accessTokens.issue()

```typescript
// src/accessTokens.ts
async issue(input: IssueAccessTokenInput): Promise<IssueAccessTokenResponse> {
    const body: API.AccessTokenInfo = {
        // Legacy
        id: input.id,
        // New auth
        public_key: input.publicKey,  // ADD THIS
        // ... rest
    };
    // ... rest unchanged
}
```

## Migration Path

### For Users

**Before (broken for streams):**
```typescript
const s2 = new S2({
    rootKey: "...",  // Bootstrap mode - limited
    endpoints: { ... },
});
```

**After (full access):**
```typescript
// Step 1: Bootstrap with root key to issue token
const bootstrap = new S2({
    rootKey: "...",
    endpoints: { ... },
});

// Step 2: Issue access token with new keypair
const { token, signingKey } = await issueAccessTokenWithNewKey(bootstrap, {
    basins: { prefix: "my-app/" },
    streams: { prefix: "my-app/" },
    opGroups: { stream: { read: true, write: true } },
});

// Step 3: Create client with proper auth
const s2 = new S2({
    token,
    signingKey,
    endpoints: { ... },
});

// Now all operations work
const session = await s2.basin("my-app").stream("logs").readSession({ ... });
```

## Verification Test

After implementing:

```typescript
// This should work with proper auth
const s2 = new S2({ token, signingKey, endpoints });
const basin = s2.basin("bench-test");
const stream = basin.stream("bench/test-stream");

// HTTP/1.1 operations
const tail = await stream.checkTail();  // Should work

// HTTP/2 operations (currently failing)
const session = await stream.readSession({ start: { seqNum: 0 }, limit: 5 });
for await (const record of session) {
    console.log(record);  // Should work now!
}
```

## Files to Modify

1. `src/types.ts` - Add `publicKey` to `IssueAccessTokenInput`
2. `src/auth/signing-key.ts` - NEW: SigningKey class
3. `src/auth/pki-auth.ts` - Update to support proper auth flow
4. `src/auth/sign.ts` - Accept SigningKey instead of raw string
5. `src/accessTokens.ts` - Pass `public_key` to API
6. `src/index.ts` - Update S2Config types
7. `src/lib/auth/provider.ts` - Update interface

## Open Questions

1. Should bootstrap mode be deprecated entirely, or kept for admin operations?
2. Should we auto-issue a token when rootKey is provided but token is not?
3. How should we handle token refresh when using proper auth?
