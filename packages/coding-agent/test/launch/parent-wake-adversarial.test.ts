import { expect, test } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { Agent } from "@oh-my-pi/pi-agent-core";
import { createMockModel } from "@oh-my-pi/pi-ai/providers/mock";
import { getBundledModel } from "@oh-my-pi/pi-catalog/models";
import { getDaemonRuntimeRoot } from "@oh-my-pi/pi-utils";
import { startDaemonBrokerFromEnvironment } from "../../src/launch/broker";
import { createDaemonBrokerClient, type DaemonBrokerClient } from "../../src/launch/client";
import { resolvePrivateRuntimeDir } from "../../src/launch/paths";
import { startParentWakeListenerFromEnvironment } from "../../src/launch/parent-wake-listener";
import { type ParentWakeTarget, type ParentWakeEvent } from "../../src/launch/protocol";
import { AgentSession } from "../../src/session/agent-session";
import { SessionManager } from "../../src/session/session-manager";
import { AuthStorage } from "../../src/session/auth-storage";
import { ModelRegistry } from "../../src/config/model-registry";
import { Settings } from "../../src/config/settings";
import { convertToLlm } from "../../src/session/messages";

async function until(predicate: () => boolean | Promise<boolean>, timeout = 5000) {
  // Real native sockets and broker disk publication are cross-runtime effects; fake timers cannot drive them.
  const deadline = Date.now() + timeout;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error("condition timed out");
    await Bun.sleep(5);
  }
}
function wake(target: ParentWakeTarget, eventId = "probe-event"): ParentWakeEvent {
  return { target, eventId, kind: "probe", pointers: [{ kind: "artifact", ref: "local://review.md" }], createdAt: 1 };
}
async function fixture() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "r-"));
  const projectDir = path.join(root, "p"), runtimeDir = path.join(root, "r");
  await fs.mkdir(projectDir);
  const clients: DaemonBrokerClient[] = [];
  async function client() {
    const c = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 1000 });
    clients.push(c); return c;
  }
  const caller = await client();
  let finished: Promise<void>;
  async function start() {
    const keys = ["OMP_DAEMON_PROJECT_DIR", "OMP_DAEMON_RUNTIME_DIR", "OMP_DAEMON_IDLE_GRACE_MS"];
    const old = keys.map(k => process.env[k]);
    process.env[keys[0]!] = projectDir; process.env[keys[1]!] = runtimeDir; process.env[keys[2]!] = "1000";
    const listening = Promise.withResolvers<void>();
    finished = startDaemonBrokerFromEnvironment({ onListening: listening.resolve });
    keys.forEach((k, i) => old[i] === undefined ? delete process.env[k] : process.env[k] = old[i]);
    await Promise.race([listening.promise, finished.then(() => { throw new Error("broker failed to acquire lease"); })]);
  }
  await start();
  async function stop(c = caller) {
    await c.request({ op: "shutdown" });
    for (const c of clients) c.close();
    await finished;
  }
  const target: ParentWakeTarget = { projectDir, sessionId: "parent", runtimeScope: "candidate" };
  const eventFile = (id = "probe-event") => path.join(runtimeDir, "parent-wake/events", `${id}.json`);
  return { root, projectDir, runtimeDir, caller, client, target, eventFile, start, stop,
    async cleanup(c = caller) { try { await stop(c); } finally { await fs.rm(root, { recursive: true, force: true }); } } };
}
async function sessionFixture() {
  const firstCall = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
  let calls = 0;
  const mock = createMockModel({ handler: async () => {
    if (++calls === 1) { firstCall.resolve(); await release.promise; }
    return { content: ["Done"] };
  } });
  const agent = new Agent({ getApiKey: () => "test-key", initialState: {
    model: getBundledModel("anthropic", "claude-sonnet-4-5")!, systemPrompt: ["Test"], tools: [],
  }, convertToLlm, streamFn: mock.stream });
  const auth = await AuthStorage.create(":memory:");
  auth.keys.setRuntime("anthropic", "test-key");
  const manager = SessionManager.inMemory();
  const active = new AgentSession({ agent, sessionManager: manager, settings: Settings.isolated(), modelRegistry: new ModelRegistry(auth) });
  return { active, agent, manager, firstCall, release, mock, async close() { release.resolve(); await active.dispose(); auth.close(); } };
}

test("FALSIFIER: another token-authenticated client cannot forge ACK before any session commit", async () => {
  const f = await fixture();
  try {
    const owner = await f.client();
    await owner.request({ op: "wake-register", target: f.target });
    const submitted = await f.caller.request({ op: "wake-submit", event: wake(f.target), timeoutMs: 0 });
    let rejected = false, result: unknown;
    try { result = await f.caller.request({ op: "wake-ack", eventId: "probe-event", target: f.target }); } catch { rejected = true; }
    console.log("FORGED_ACK", JSON.stringify({ submitted, result, rejected, sessionCommits: 0 }));
    expect(rejected).toBe(true);
  } finally { await f.cleanup(); }
}, 15000);

test("FALSIFIER: another client cannot unregister a live owner", async () => {
  const f = await fixture();
  try {
    const owner = await f.client();
    await owner.request({ op: "wake-register", target: f.target });
    let rejected = false;
    try { await f.caller.request({ op: "wake-unregister", target: f.target }); } catch { rejected = true; }
    const probe = await f.caller.request({ op: "wake-submit", event: wake(f.target), timeoutMs: 0 });
    console.log("FORGED_UNREGISTER", JSON.stringify({ rejected, probe }));
    expect(rejected).toBe(true);
  } finally { await f.cleanup(); }
}, 15000);

test("FALSIFIER: another socket cannot replace a live registration using the same scope", async () => {
  const f = await fixture();
  try {
    const owner = await f.client();
    await owner.request({ op: "wake-register", target: f.target });
    let rejected = false;
    try { await f.caller.request({ op: "wake-register", target: f.target }); } catch { rejected = true; }
    console.log("SAME_SCOPE_HIJACK", JSON.stringify({ rejected }));
    expect(rejected).toBe(true);
  } finally { await f.cleanup(); }
}, 15000);

test("a symlink alias into the synthetic shared root is not private", async () => {
  const alias = path.join(os.tmpdir(), `alias-${crypto.randomUUID()}`);
  // This is the profile-local daemon root of the isolated test env, never the real profile or a live broker.
  await fs.mkdir(getDaemonRuntimeRoot(), { recursive: true });
  await fs.symlink(getDaemonRuntimeRoot(), alias);
  try {
    await expect(resolvePrivateRuntimeDir(path.join(alias, "scope"), "probe")).rejects.toThrow("symlinked");
  } finally { await fs.unlink(alias); }
});

test("a shared-root child named ..escape is inside the root, not outside it", async () => {
  const candidate = path.join(getDaemonRuntimeRoot(), "..escape", "scope");
  await expect(resolvePrivateRuntimeDir(candidate, "probe")).rejects.toThrow("shared daemon runtime root");
});

test("CONTROL: relative isolation paths reject and unset opt-in never connects or enqueues", async () => {
  await expect(resolvePrivateRuntimeDir("relative/runtime", "probe")).rejects.toThrow("absolute");
  const handle = await startParentWakeListenerFromEnvironment("/unreachable-review-project", {
    queueParentWake: async () => { throw new Error("unset opt-in must not deliver"); },
  });
  expect(handle).toBeUndefined();
});

test("CONTROL: genuine session receipt stays unacked until queue injection commits", async () => {
  const s = await sessionFixture(); const f = await fixture();
  let running: Promise<unknown> | undefined;
  try {
    running = s.active.prompt("hold first turn"); await s.firstCall.promise;
    const owner = await f.client(); const target = { ...f.target, sessionId: s.manager.getSessionId() };
    owner.onParentWake(target, event => s.active.queueParentWake(event));
    await owner.request({ op: "wake-register", target });
    const submitted = f.caller.request({ op: "wake-submit", event: wake(target), timeoutMs: 3000 });
    await until(async () => { try { return JSON.parse(await fs.readFile(f.eventFile(), "utf8")).status === "dispatched" && s.active.yieldQueue.has("parent-wake"); } catch { return false; } });
    const before = JSON.parse(await fs.readFile(f.eventFile(), "utf8"));
    console.log("BEFORE_QUEUE_COMMIT", JSON.stringify({ status: before.status, ackedAt: before.ackedAt, queueHasWake: s.active.yieldQueue.has("parent-wake") }));
    expect(before.status).toBe("dispatched"); expect(before.ackedAt).toBeUndefined();
    s.release.resolve();
    const result = await submitted; await running; await s.active.waitForIdle();
    expect(result.op === "wake-submit" && result.result.status).toBe("acked");
    expect(s.agent.state.messages.some(m => m.role === "custom" && m.customType === "parent-wake")).toBe(true);
  } finally { s.release.resolve(); await running?.catch(() => {}); await f.cleanup(); await s.close(); }
}, 15000);

test("CONTROL: a queued wake becomes stale when the real session identity changes", async () => {
  const s = await sessionFixture(); const f = await fixture();
  let running: Promise<unknown> | undefined;
  try {
    running = s.active.prompt("hold old session"); await s.firstCall.promise;
    const owner = await f.client(); const target = { ...f.target, sessionId: s.manager.getSessionId() };
    owner.onParentWake(target, event => s.active.queueParentWake(event));
    await owner.request({ op: "wake-register", target });
    const submitted = f.caller.request({ op: "wake-submit", event: wake(target), timeoutMs: 500 });
    await until(() => s.active.yieldQueue.has("parent-wake"));
    s.manager.newSession(); s.release.resolve();
    const result = await submitted; await running; await s.active.waitForIdle();
    console.log("STALE_SESSION", JSON.stringify(result));
    expect(result.op === "wake-submit" && result.result.status).not.toBe("acked");
    expect(s.agent.state.messages.some(m => m.role === "custom" && m.customType === "parent-wake")).toBe(false);
    await expect(s.active.queueParentWake(wake(target, "wrong-session"))).rejects.toThrow("does not match");
  } finally { s.release.resolve(); await running?.catch(() => {}); await f.cleanup(); await s.close(); }
}, 15000);

test("CONTROL: successfully acked event dedupes after a genuine broker restart", async () => {
  const f = await fixture(); let current = f.caller;
  try {
    const owner = await f.client(); let delivered = 0;
    owner.onParentWake(f.target, () => { delivered++; });
    await owner.request({ op: "wake-register", target: f.target });
    const first = await current.request({ op: "wake-submit", event: wake(f.target), timeoutMs: 1000 });
    await f.stop(current); current = await f.client(); await f.start();
    const after = await f.client(); after.onParentWake(f.target, () => { delivered++; });
    await after.request({ op: "wake-register", target: f.target });
    const replay = await current.request({ op: "wake-submit", event: wake(f.target), timeoutMs: 1000 });
    console.log("RESTART_ACK_DEDUPE", JSON.stringify({ first, replay, delivered }));
    expect(replay).toEqual(first); expect(delivered).toBe(1);
  } finally { await f.cleanup(current); }
}, 15000);

test("FALSIFIER: failed durable ACK write cannot be reported as durable acked", async () => {
  const f = await fixture();
  try {
    const owner = await f.client(); await owner.request({ op: "wake-register", target: f.target });
    await f.caller.request({ op: "wake-submit", event: wake(f.target), timeoutMs: 0 });
    // Occupy the atomic publication destination. This deterministically fails rename without mocking fs.
    await fs.unlink(f.eventFile()); await fs.mkdir(f.eventFile());
    let rejected = false, result: unknown;
    try { result = await owner.request({ op: "wake-ack", eventId: "probe-event", target: f.target }); } catch { rejected = true; }
    console.log("ACK_PERSIST_FAILURE", JSON.stringify({ rejected, result, durableFileIsDirectory: (await fs.stat(f.eventFile())).isDirectory() }));
    expect(rejected).toBe(true);
  } finally { await f.cleanup(); }
}, 15000);

test("FALSIFIER: persisted pending crash-window event resumes rather than remaining pending forever", async () => {
  const f = await fixture(); let current = f.caller;
  try {
    await f.stop();
    await fs.mkdir(path.dirname(f.eventFile()), { recursive: true });
    await fs.writeFile(f.eventFile(), JSON.stringify({ event: wake(f.target), status: "pending" }));
    current = await f.client(); await f.start();
    const owner = await f.client(); let delivered = 0;
    owner.onParentWake(f.target, () => { delivered++; });
    await owner.request({ op: "wake-register", target: f.target });
    const replay = await current.request({ op: "wake-submit", event: wake(f.target), timeoutMs: 1000 });
    console.log("PENDING_RESTART", JSON.stringify({ replay, delivered }));
    expect(replay.op === "wake-submit" && replay.result.status).not.toBe("pending");
  } finally { await f.cleanup(current); }
}, 15000);

test("retention: records past the horizon are pruned, recent replay identities stay durable beyond the resident bound", async () => {
  const f = await fixture(); let current = f.caller;
  const day = 24 * 60 * 60_000, old = new Date(Date.now() - 20 * day);
  const total = 300, backdated = 40;
  try {
    for (let i = 0; i < total; i++) {
      const target = { ...f.target, sessionId: `session-${i}` };
      if (i < backdated) {
        await current.request({ op: "wake-register", target });
        await current.request({ op: "wake-unregister", target });
      }
      await current.request({ op: "wake-submit", event: wake(target, `growth-${i}`), timeoutMs: 0 });
    }
    await f.stop(current); current = await f.client();
    // Backdate the first `backdated` events and tombstones beyond the 14-day horizon while the broker is down.
    for (let i = 0; i < backdated; i++) {
      await fs.utimes(f.eventFile(`growth-${i}`), old, old);
      const registration = path.join(f.runtimeDir, "parent-wake/registrations", `session-${i}.json`);
      const decoded = JSON.parse(await fs.readFile(registration, "utf8"));
      await fs.writeFile(registration, JSON.stringify({ ...decoded, updatedAt: old.getTime() }));
    }
    await f.start();
    const events = await fs.readdir(path.dirname(f.eventFile()));
    const registrations = await fs.readdir(path.join(f.runtimeDir, "parent-wake/registrations"));
    expect(events.length).toBe(total - backdated);
    expect(registrations.length).toBe(0);
    // A recent event beyond the resident bound is still deduped from its durable record (no second dispatch).
    const sessionId = `session-${backdated}`;
    const replay = await current.request({ op: "wake-submit", event: wake({ ...f.target, sessionId }, `growth-${backdated}`), timeoutMs: 0 });
    expect(replay.op === "wake-submit" && replay.result.failureCode).toBe("target-missing");
    expect(await fs.readdir(path.dirname(f.eventFile()))).toHaveLength(total - backdated);
  } finally { await f.cleanup(current); }
}, 60000);
