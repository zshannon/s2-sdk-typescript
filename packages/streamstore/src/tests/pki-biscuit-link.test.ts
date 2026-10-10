import { readFileSync } from "node:fs";
import { Biscuit, PublicKey } from "@flickfyi/biscuit";
import { base64urlnopad } from "@scure/base";
import { expect, it } from "vitest";

it("links the generic ENG-716 package and identifies the signer of each verified block", () => {
	const document = JSON.parse(
		readFileSync(
			new URL("./fixtures/pki-v2-rust.json", import.meta.url),
			"utf8",
		),
	);
	const biscuit = Biscuit.fromBytes(
		base64urlnopad.decode(document.grant),
		PublicKey.fromBase58(document.issuer, "secp256r1"),
	);
	expect(biscuit.blockMetadata[0]).toHaveProperty(
		"signatureAlgorithm",
		"secp256r1",
	);
});
