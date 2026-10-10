import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const root = fileURLToPath(new URL("../", import.meta.url));
const errors = [];
for (const directory of readdirSync(join(root, "packages"))) {
	const pkg = JSON.parse(readFileSync(join(root, "packages", directory, "package.json"), "utf8"));
	if (pkg.private) continue;
	if (!pkg.name.startsWith("@zshannon/")) errors.push(`${pkg.name}: expected the @zshannon scope`);
	if (pkg.publishConfig?.access !== "public") errors.push(`${pkg.name}: expected public npm access`);
	if (pkg.publishConfig?.registry !== "https://registry.npmjs.org") errors.push(`${pkg.name}: expected the npm registry`);
	if (pkg.repository?.url !== "git+https://github.com/zshannon/s2-sdk-typescript.git") errors.push(`${pkg.name}: repository must match this fork for npm provenance`);
	for (const field of ["dependencies", "optionalDependencies", "peerDependencies"]) {
		for (const [name, version] of Object.entries(pkg[field] ?? {})) {
			if (/^(link:|file:|workspace:)/.test(version)) errors.push(`${pkg.name}: ${name} uses ${version}; replace it with a published npm version and regenerate the lockfiles before releasing`);
		}
	}
}
if (errors.length) {
	console.error(errors.join("\n"));
	process.exitCode = 1;
} else {
	console.log("Public @zshannon packages are configured for npm publishing.");
}
