import { expect, test } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { getBundledModel } from "@oh-my-pi/pi-catalog/models";
import { parseArgs } from "../src/cli/args";
import { buildSessionOptions } from "../src/main";
import { createAgentSession, loadSessionExtensions } from "../src/sdk";
import { AuthStorage } from "../src/session/auth-storage";
import { SessionManager } from "../src/session/session-manager";
import { Settings } from "../src/config/settings";
import { ModelRegistry } from "../src/config/model-registry";
import { EventBus } from "../src/utils/event-bus";

interface StrictFixture {
  root: string;
  auth: AuthStorage;
  registry: ModelRegistry;
  settings: Settings;
  manager: SessionManager;
  close(): Promise<void>;
}
async function fixture(): Promise<StrictFixture> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "s-"));
  const auth = await AuthStorage.create(":memory:");
  const registry = new ModelRegistry(auth, path.join(root, "models.yml"));
  const settings = Settings.isolated({ "checkpoint.enabled": true });
  const manager = SessionManager.inMemory(root);
  return { root, auth, registry, settings, manager, async close() { auth.close(); await fs.rm(root, { recursive: true, force: true }); } };
}
async function options(f: StrictFixture, names = "read", strict = true) {
  const args = parseArgs(["--cwd", f.root, "--tools", names, ...(strict ? ["--strict-tools"] : [])]);
  const built = await buildSessionOptions(args, [], f.manager, f.registry, f.settings);
  return { ...built, agentDir: path.join(f.root, "agent"), authStorage: f.auth, modelRegistry: f.registry,
    settings: f.settings, model: getBundledModel("openai", "gpt-4o-mini")!,
    skills: [], contextFiles: [], promptTemplates: [], slashCommands: [], rules: [], skipPythonPreflight: true,
    workspaceTree: { rootPath: f.root, rendered: "", truncated: false, totalLines: 0, agentsMdFiles: [] },
  };
}

test("strict CLI-to-SDK path excludes real project MCP and ambient .omp/tools", async () => {
  const f = await fixture();
  const marker = path.join(f.root, "custom-loaded");
  try {
    await fs.mkdir(path.join(f.root, ".omp/tools"), { recursive: true });
    await fs.writeFile(path.join(f.root, ".omp/tools/ambient.ts"), `import { appendFileSync } from 'node:fs';\nexport default function(api) { appendFileSync(${JSON.stringify(marker)}, 'loaded\\n'); return { name: 'ambient_review_tool', description: 'Review-only ambient tool', parameters: api.typebox.Type.Object({}), async execute() { return { content: [{type:'text', text:'ok'}] }; } }; }`);
    const server = path.resolve(import.meta.dir, "fixtures/instructions-mcp.ts");
    await fs.writeFile(path.join(f.root, ".mcp.json"), JSON.stringify({ mcpServers: { instr: { type: "stdio", command: process.execPath, args: [server] } } }));
    const strictOptions = await options(f);
    expect(strictOptions.restrictToolNames).toBe(true);
    const { session: strict, mcpManager: strictMcp } = await createAgentSession(strictOptions);
    try {
      const names = strict.getAllToolNames();
      console.log("STRICT_ACTUAL_TOOLS", JSON.stringify({ names, mcpManagerPresent: strictMcp !== undefined, customFactoryEvaluated: await fs.exists(marker) }));
      expect(names).toEqual(["read"]); expect(strictMcp).toBeUndefined(); expect(await fs.exists(marker)).toBe(false);
      expect(strict.getXdevToolEntries()).toEqual([]);
    } finally { await strict.dispose(); }
    // Positive control proves the exact same fixtures are discoverable, rather than vacuous absence.
    const normalOptions = await options(f, "read", false);
    const { session: normal, mcpManager } = await createAgentSession({ ...normalOptions, sessionManager: SessionManager.inMemory(f.root), enableLsp: false });
    try {
      const names = normal.getAllToolNames();
      console.log("NORMAL_ACTUAL_TOOLS", JSON.stringify({ names, customFactoryEvaluated: await fs.exists(marker), mcpManagerPresent: mcpManager !== undefined }));
      expect(names).toContain("ambient_review_tool"); expect(names).toContain("mcp__instr_do_thing"); expect(await fs.exists(marker)).toBe(true);
    } finally { await normal.dispose(); }
  } finally { await f.close(); }
}, 30000);

test("strict CLI startup must not evaluate ambient extension factories", async () => {
  const f = await fixture(); const marker = path.join(f.root, "extension-loaded");
  try {
    await fs.mkdir(path.join(f.root, ".omp/extensions"), { recursive: true });
    await fs.writeFile(path.join(f.root, ".omp/extensions/ambient.ts"), `import { appendFileSync } from 'node:fs';\nexport default function(pi) { appendFileSync(${JSON.stringify(marker)}, 'loaded\\n'); pi.registerFlag('ambient-review', {type:'boolean', description:'review sentinel'}); }`);
    const built = await options(f);
    // This is the exact main.ts pre-session loader call, not the SDK-only path.
    const loaded = await loadSessionExtensions(built, f.root, f.settings, new EventBus(), { deferStartupSafe: false });
    console.log("STRICT_AMBIENT_EXTENSION", JSON.stringify({ restrict: built.restrictToolNames, sources: loaded.extensions.map(e => e.path), evaluated: await fs.exists(marker), errors: loaded.errors }));
    expect(await fs.exists(marker)).toBe(false);
  } finally { await f.close(); }
}, 15000);

test("strict --tools naming only one of checkpoint/rewind is refused, not silently widened", async () => {
  const f = await fixture();
  try {
    for (const one of ["checkpoint", "rewind"]) {
      const args = parseArgs(["--cwd", f.root, "--tools", one, "--strict-tools"]);
      await expect(buildSessionOptions(args, [], f.manager, f.registry, f.settings)).rejects.toThrow(
        "both `checkpoint` and `rewind`",
      );
    }
    const { session } = await createAgentSession(await options(f, "checkpoint,rewind"));
    try {
      expect([...session.getAllToolNames()].sort()).toEqual(["checkpoint", "rewind"]);
    } finally { await session.dispose(); }
  } finally { await f.close(); }
}, 30000);
