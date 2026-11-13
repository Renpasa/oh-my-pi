import { describe, expect, it } from "bun:test";
import { parseArgs } from "@oh-my-pi/pi-coding-agent/cli/args";

describe("parseArgs — --strict-tools flag", () => {
	it("is off by default so --tools keeps its additive behavior", () => {
		const result = parseArgs(["--tools", "read,grep"]);
		expect(result.strictTools).toBeUndefined();
		expect(result.tools).toEqual(["read", "grep"]);
	});

	it("parses alongside --tools without consuming a value", () => {
		const result = parseArgs(["--tools", "read,grep", "--strict-tools", "do the thing"]);
		expect(result.strictTools).toBe(true);
		expect(result.tools).toEqual(["read", "grep"]);
		expect(result.messages).toEqual(["do the thing"]);
	});

	it("requires an explicit --tools list", () => {
		expect(() => parseArgs(["--strict-tools"])).toThrow("--strict-tools requires --tools");
	});
});
