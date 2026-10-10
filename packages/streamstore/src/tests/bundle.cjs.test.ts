import { execFileSync } from "node:child_process";
import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	symlinkSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";

it("loads the built CommonJS PKI entry with only declared runtime dependencies", () => {
	const pkgRoot = fileURLToPath(new URL("../..", import.meta.url));
	const repoRoot = join(pkgRoot, "../..");
	const directory = mkdtempSync(join(tmpdir(), "streamstore-cjs-"));
	try {
		const manifest = JSON.parse(
			readFileSync(join(pkgRoot, "package.json"), "utf8"),
		);
		mkdirSync(join(directory, "dist/cjs"), { recursive: true });
		writeFileSync(join(directory, "package.json"), JSON.stringify(manifest));
		symlinkSync(join(pkgRoot, "src"), join(directory, "src"), "dir");
		execFileSync("bun", [join(repoRoot, "scripts/postbuild.ts")], {
			cwd: directory,
		});
		for (const name of Object.keys(manifest.dependencies)) {
			const local = join(pkgRoot, "node_modules", name);
			const dependency = existsSync(local)
				? local
				: join(repoRoot, "node_modules", name);
			const destination = join(directory, "node_modules", name);
			mkdirSync(join(destination, ".."), { recursive: true });
			symlinkSync(dependency, destination, "dir");
		}
		expect(
			execFileSync(
				process.execPath,
				[
					"-e",
					`
            const { ClientKey, delegateGrant, issueGrant, PkiCredentials } = require('./dist/cjs/pki/index.js');
            const issuer = ClientKey.generate();
            const client = ClientKey.generate();
            const scope = { basins: { kind: 'any' }, operations: ['Read'], streams: { kind: 'any' } };
            const parent = issueGrant({ audience: 'https://example.com', expiresAt: 2000000000,
                holder: issuer.publicKey, issuerKey: issuer, scope });
            const grant = delegateGrant({ delegatorKey: issuer, expiresAt: 2000000000,
                holder: client.publicKey, parent, scope });
            console.log(new PkiCredentials(client, grant).grant.holder === client.publicKey);
        `,
				],
				{ cwd: directory, encoding: "utf8" },
			).trim(),
		).toBe("true");
	} finally {
		rmSync(directory, { force: true, recursive: true });
	}
});
