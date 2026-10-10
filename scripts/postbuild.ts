#!/usr/bin/env bun

/**
 * Post-build script:
 * - Add package.json files to dist directories so Node.js interprets module formats correctly
 */

import { writeFileSync } from "node:fs";
import { join } from "node:path";

// Add package.json to CJS dist to mark it as CommonJS
const cjsPackageJson = {
	type: "commonjs",
};

writeFileSync(
	join(process.cwd(), "dist", "cjs", "package.json"),
	JSON.stringify(cjsPackageJson, null, 2),
	"utf-8",
);

console.log("✓ Created dist/cjs/package.json");

// The generic Biscuit package is ESM-only. Bundle it and its protobuf dependency
// into the CJS PKI entry. Only the SDK's declared crypto dependencies stay external.
// The main bearer entry stays independent of this optional PKI entry point.
const pkiBuild = await Bun.build({
	entrypoints: [join(process.cwd(), "src/pki/index.ts")],
	external: ["@noble/*", "@scure/*"],
	format: "cjs",
	naming: "index.js",
	outdir: join(process.cwd(), "dist/cjs/pki"),
	target: "node",
});
if (!pkiBuild.success) {
	throw new AggregateError(pkiBuild.logs, "CommonJS PKI build failed");
}
