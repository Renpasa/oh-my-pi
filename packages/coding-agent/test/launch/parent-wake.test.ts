import { describe, expect, it, vi } from "bun:test";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { getDaemonRuntimeRoot, TempDir } from "@oh-my-pi/pi-utils";
import { startDaemonBrokerFromEnvironment } from "../../src/launch/broker";
import { createDaemonBrokerClient, type DaemonBrokerClient } from "../../src/launch/client";
import * as daemonClient from "../../src/launch/client";
import { assertPrivateRuntimeDir } from "../../src/launch/paths";
import {
	resolveParentWakeTargetFromEnvironment,
	startParentWakeListenerFromEnvironment,
	stopParentWakeListenerOnSessionChange,
} from "../../src/launch/parent-wake-listener";
import {
	DAEMON_IDLE_GRACE_ENV,
	DAEMON_PROJECT_DIR_ENV,
	DAEMON_RUNTIME_DIR_ENV,
	PARENT_WAKE_BROKER_RUNTIME_DIR_ENV,
	PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV,
	PARENT_WAKE_TARGET_SESSION_ID_ENV,
	type ParentWakeEvent,
	type ParentWakeTarget,
} from "../../src/launch/protocol";

function restoreEnv(name: string, value: string | undefined): void {
	if (value === undefined) delete process.env[name];
	else process.env[name] = value;
}

function startBroker(projectDir: string, runtimeDir: string): Promise<void> {
	const previousProjectDir = process.env[DAEMON_PROJECT_DIR_ENV];
	const previousRuntimeDir = process.env[DAEMON_RUNTIME_DIR_ENV];
	const previousGrace = process.env[DAEMON_IDLE_GRACE_ENV];
	process.env[DAEMON_PROJECT_DIR_ENV] = projectDir;
	process.env[DAEMON_RUNTIME_DIR_ENV] = runtimeDir;
	process.env[DAEMON_IDLE_GRACE_ENV] = "5000";
	const broker = startDaemonBrokerFromEnvironment();
	restoreEnv(DAEMON_PROJECT_DIR_ENV, previousProjectDir);
	restoreEnv(DAEMON_RUNTIME_DIR_ENV, previousRuntimeDir);
	restoreEnv(DAEMON_IDLE_GRACE_ENV, previousGrace);
	return broker;
}

async function stopBroker(
	clients: DaemonBrokerClient[],
	anyClient: DaemonBrokerClient,
	broker: Promise<void>,
): Promise<void> {
	await anyClient.request({ op: "shutdown" }).catch(() => undefined);
	for (const client of clients) client.close();
	await broker;
}

/** Poll a real observed condition until it holds or the bound elapses — never a guessed fixed delay. */
async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 5_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		if (await predicate()) return;
		if (Date.now() >= deadline) throw new Error(`Condition did not become true within ${timeoutMs}ms`);
		await Bun.sleep(5);
	}
}

function event(target: ParentWakeTarget, eventId: string, overrides: Partial<ParentWakeEvent> = {}): ParentWakeEvent {
	return {
		eventId,
		kind: "candidate-ready",
		pointers: [{ kind: "artifact", ref: "local://result.md" }],
		createdAt: Date.now(),
		target,
		...overrides,
	};
}

describe("parent wake transport", () => {
	it("routes an event from an unregistered caller to exactly the registered target, dispatch+ack succeeds", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-route-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const caller = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const target = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const wakeTarget: ParentWakeTarget = { projectDir, sessionId: "parent-1", runtimeScope: "candidate-a" };
			const received: ParentWakeEvent[] = [];
			// Registration happens on the target's own connection; the caller below
			// never registers itself and cannot be mistaken for the target it addresses.
			await target.request({ op: "wake-register", target: wakeTarget });
			target.onParentWake(wakeTarget, wakeEvent => {
				received.push(wakeEvent);
			});
			const result = await caller.request({
				op: "wake-submit",
				event: event(wakeTarget, "evt-route-1"),
				timeoutMs: 5_000,
			});
			expect(result.op).toBe("wake-submit");
			if (result.op !== "wake-submit") throw new Error("unexpected op");
			expect(result.result.status).toBe("acked");
			expect(result.result.eventId).toBe("evt-route-1");
			expect(result.result.target).toEqual(wakeTarget);
			expect(result.result.ackedAt).toBeDefined();
			expect(received).toHaveLength(1);
			expect(received[0]?.eventId).toBe("evt-route-1");
			expect(received[0]?.pointers).toEqual([{ kind: "artifact", ref: "local://result.md" }]);
		} finally {
			await stopBroker([caller, target], caller, broker);
		}
	}, 20_000);

	it("fails closed with target-missing when no target ever registered", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-missing-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const caller = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const wakeTarget: ParentWakeTarget = { projectDir, sessionId: "ghost", runtimeScope: "candidate-a" };
			const result = await caller.request({
				op: "wake-submit",
				event: event(wakeTarget, "evt-missing-1"),
				timeoutMs: 5_000,
			});
			if (result.op !== "wake-submit") throw new Error("unexpected op");
			expect(result.result.status).toBe("failed");
			expect(result.result.failureCode).toBe("target-missing");
		} finally {
			await stopBroker([caller], caller, broker);
		}
	}, 20_000);

	it("fails closed with target-dead after the registered socket disconnects without unregistering", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-dead-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const caller = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const target = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const wakeTarget: ParentWakeTarget = { projectDir, sessionId: "parent-dead", runtimeScope: "candidate-a" };
			await target.request({ op: "wake-register", target: wakeTarget });
			target.close(); // abrupt disconnect, no wake-unregister
			// The broker clears the registration's live socket ref asynchronously off
			// the TCP close callback; poll the real observable outcome (a distinct
			// probe event's failure code) instead of guessing how long that takes.
			let attempt = 0;
			await waitFor(async () => {
				attempt++;
				const result = await caller.request({
					op: "wake-submit",
					event: event(wakeTarget, `evt-dead-probe-${attempt}`),
					timeoutMs: 5_000,
				});
				return result.op === "wake-submit" && result.result.failureCode === "target-dead";
			});
		} finally {
			await stopBroker([caller], caller, broker);
		}
	}, 20_000);

	it("rejects a different runtime scope hijacking a session id whose socket disconnected without unregistering", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-hijack-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const owner = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const impostor = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const ownerTarget: ParentWakeTarget = { projectDir, sessionId: "parent-hijack", runtimeScope: "candidate-owner" };
			await owner.request({ op: "wake-register", target: ownerTarget });
			owner.close(); // abrupt disconnect, no wake-unregister — the registration stays live (not stale)

			// Poll until the broker has actually observed the socket close (mirrors
			// the target-dead test above), so the hijack attempt below races a real
			// dead-but-non-stale registration instead of a still-live one.
			let attempt = 0;
			await waitFor(async () => {
				attempt++;
				const probe = await impostor.request({
					op: "wake-submit",
					event: event(ownerTarget, `evt-hijack-probe-${attempt}`),
					timeoutMs: 2_000,
				});
				return probe.op === "wake-submit" && probe.result.failureCode === "target-dead";
			});

			const impostorTarget: ParentWakeTarget = { ...ownerTarget, runtimeScope: "candidate-impostor" };
			let rejected: unknown;
			try {
				await impostor.request({ op: "wake-register", target: impostorTarget });
			} catch (error) {
				rejected = error;
			}
			expect(rejected).toBeInstanceOf(Error);

			// The original runtime scope must still own the session id: a fresh
			// submit under the owner's scope still resolves target-dead (not
			// wrong-target), proving the impostor's registration never took.
			const stillOwner = await impostor.request({
				op: "wake-submit",
				event: event(ownerTarget, "evt-hijack-still-owner"),
				timeoutMs: 2_000,
			});
			if (stillOwner.op !== "wake-submit") throw new Error("unexpected op");
			expect(stillOwner.result.failureCode).toBe("target-dead");
		} finally {
			await stopBroker([impostor], impostor, broker);
		}
	}, 20_000);

	it("fails closed with wrong-target when the registered runtime scope differs from the requested one", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-wrong-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const caller = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const target = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const registered: ParentWakeTarget = { projectDir, sessionId: "parent-wrong", runtimeScope: "candidate-a" };
			await target.request({ op: "wake-register", target: registered });
			const requested: ParentWakeTarget = { ...registered, runtimeScope: "candidate-b" };
			const result = await caller.request({
				op: "wake-submit",
				event: event(requested, "evt-wrong-1"),
				timeoutMs: 5_000,
			});
			if (result.op !== "wake-submit") throw new Error("unexpected op");
			expect(result.result.status).toBe("failed");
			expect(result.result.failureCode).toBe("wrong-target");
		} finally {
			await stopBroker([caller, target], caller, broker);
		}
	}, 20_000);

	it("fails closed with target-stale after an explicit graceful unregister", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-stale-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const caller = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const target = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const wakeTarget: ParentWakeTarget = { projectDir, sessionId: "parent-stale", runtimeScope: "candidate-a" };
			await target.request({ op: "wake-register", target: wakeTarget });
			await target.request({ op: "wake-unregister", target: wakeTarget });
			const result = await caller.request({
				op: "wake-submit",
				event: event(wakeTarget, "evt-stale-1"),
				timeoutMs: 5_000,
			});
			if (result.op !== "wake-submit") throw new Error("unexpected op");
			expect(result.result.status).toBe("failed");
			expect(result.result.failureCode).toBe("target-stale");
		} finally {
			await stopBroker([caller, target], caller, broker);
		}
	}, 20_000);

	it("fails closed with timeout when the target registers but never acknowledges", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-timeout-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const caller = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const target = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const wakeTarget: ParentWakeTarget = { projectDir, sessionId: "parent-timeout", runtimeScope: "candidate-a" };
			// Raw wake-register with no onParentWake sink: the delivery arrives on the
			// wire but nothing ever acks it — this exercises the real bounded ack wait.
			await target.request({ op: "wake-register", target: wakeTarget });
			const start = Date.now();
			const result = await caller.request({
				op: "wake-submit",
				event: event(wakeTarget, "evt-timeout-1"),
				timeoutMs: 300,
			});
			if (result.op !== "wake-submit") throw new Error("unexpected op");
			expect(result.result.status).toBe("failed");
			expect(result.result.failureCode).toBe("timeout");
			expect(Date.now() - start).toBeGreaterThanOrEqual(280);
		} finally {
			await stopBroker([caller, target], caller, broker);
		}
	}, 20_000);

	it("dedupes a replayed eventId: exactly one wake reaches the target, second submit returns the same result", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-replay-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const caller = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const target = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const wakeTarget: ParentWakeTarget = { projectDir, sessionId: "parent-replay", runtimeScope: "candidate-a" };
			let deliveries = 0;
			await target.request({ op: "wake-register", target: wakeTarget });
			target.onParentWake(wakeTarget, () => {
				deliveries++;
			});
			const first = await caller.request({
				op: "wake-submit",
				event: event(wakeTarget, "evt-replay-1"),
				timeoutMs: 5_000,
			});
			const second = await caller.request({
				op: "wake-submit",
				event: event(wakeTarget, "evt-replay-1"),
				timeoutMs: 5_000,
			});
			if (first.op !== "wake-submit" || second.op !== "wake-submit") throw new Error("unexpected op");
			expect(first.result.status).toBe("acked");
			expect(second.result).toEqual(first.result);
			expect(deliveries).toBe(1);
		} finally {
			await stopBroker([caller, target], caller, broker);
		}
	}, 20_000);

	it("persists dispatch state across a broker restart and redelivers exactly once on target reconnect", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-restart-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const caller = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const targetBefore = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		let broker = startBroker(projectDir, runtimeDir);
		const wakeTarget: ParentWakeTarget = { projectDir, sessionId: "parent-restart", runtimeScope: "candidate-a" };
		const eventFile = path.join(runtimeDir, "parent-wake", "events", "evt-restart-1.json");
		try {
			// Register with no sink so the dispatched event is never acked before restart.
			await targetBefore.request({ op: "wake-register", target: wakeTarget });
			const submitPromise = caller
				.request({ op: "wake-submit", event: event(wakeTarget, "evt-restart-1"), timeoutMs: 20_000 })
				.catch(() => undefined);
			// Wait for the real durable side effect (the persisted event file showing
			// "dispatched") rather than guessing how long dispatch takes, before killing
			// the broker mid-flight.
			await waitFor(async () => {
				try {
					const raw = await fs.readFile(eventFile, "utf8");
					const decoded: unknown = JSON.parse(raw);
					return typeof decoded === "object" && decoded !== null && "status" in decoded && decoded.status === "dispatched";
				} catch {
					return false;
				}
			});
			caller.close();
			targetBefore.close();
			await submitPromise;
			await broker;
		} finally {
			// broker already stopped above via caller.close()/targetBefore.close() + await broker
		}

		const targetAfter = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const callerAfter = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		broker = startBroker(projectDir, runtimeDir);
		try {
			let deliveries = 0;
			targetAfter.onParentWake(wakeTarget, () => {
				deliveries++;
			});
			await waitFor(() => deliveries === 1);
			// Give the transport a moment past delivery to also complete the
			// auto-ack round trip, observed through the durable replay result below
			// (no fixed sleep needed: the replay request itself blocks on the real
			// broker-side record, which only settles once the ack lands).
			await waitFor(async () => {
				const replay = await callerAfter.request({
					op: "wake-submit",
					event: event(wakeTarget, "evt-restart-1"),
					timeoutMs: 5_000,
				});
				return replay.op === "wake-submit" && replay.result.status === "acked";
			});
			expect(deliveries).toBe(1);
		} finally {
			await stopBroker([targetAfter, callerAfter], callerAfter, broker);
		}
	}, 20_000);

	it("leaves ordinary daemon supervision (start/list/stop) unaffected by the wake transport", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-legacy-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const client = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const started = await client.request({
				op: "start",
				spec: {
					name: "legacy-daemon",
					application: process.execPath,
					args: ["-e", "process.stdin.resume()"],
					env: {},
					cwd: projectDir,
					pty: false,
					restart: "no",
					persist: false,
					detached: false,
				},
			});
			expect(started.op).toBe("start");
			if (started.op !== "start") throw new Error("unexpected op");
			expect(started.daemon.state).not.toBe("failed");
			const listed = await client.request({ op: "list" });
			if (listed.op !== "list") throw new Error("unexpected op");
			expect(listed.daemons.map(daemon => daemon.name)).toContain("legacy-daemon");
			await client.request({ op: "stop", name: "legacy-daemon", timeoutMs: 2_000 });
		} finally {
			await stopBroker([client], client, broker);
		}
	}, 20_000);


	it("rejects a resubmitted eventId addressed to a different target instead of inheriting the prior result", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-id-collision-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const caller = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const targetA = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const wakeTargetA: ParentWakeTarget = { projectDir, sessionId: "parent-a", runtimeScope: "candidate-a" };
			targetA.onParentWake(wakeTargetA, () => {});
			const first = await caller.request({
				op: "wake-submit",
				event: event(wakeTargetA, "evt-collision-1"),
				timeoutMs: 5_000,
			});
			if (first.op !== "wake-submit") throw new Error("unexpected op");
			expect(first.result.status).toBe("acked");

			// Same eventId, different session — must never dispatch against
			// targetA's identity nor return targetA's already-acked result.
			const wakeTargetB: ParentWakeTarget = { projectDir, sessionId: "parent-b", runtimeScope: "candidate-a" };
			let rejected: unknown;
			try {
				await caller.request({ op: "wake-submit", event: event(wakeTargetB, "evt-collision-1"), timeoutMs: 5_000 });
			} catch (error) {
				rejected = error;
			}
			expect(rejected).toBeInstanceOf(Error);
		} finally {
			await stopBroker([caller, targetA], caller, broker);
		}
	}, 20_000);

	it("rejects an ack whose target.projectDir does not match the broker's own scope", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-forged-scope-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const caller = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const target = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const wakeTarget: ParentWakeTarget = { projectDir, sessionId: "parent-forge", runtimeScope: "candidate-a" };
			// Raw registration with no sink: the real target never acks itself, so a
			// forged ack sent over a *different* connection (the caller) is the only
			// thing that could settle the event — and must fail closed.
			await target.request({ op: "wake-register", target: wakeTarget });
			const submitted = await caller.request({
				op: "wake-submit",
				event: event(wakeTarget, "evt-forge-1"),
				timeoutMs: 200,
			});
			if (submitted.op !== "wake-submit") throw new Error("unexpected op");
			expect(submitted.result.status).toBe("failed");
			expect(submitted.result.failureCode).toBe("timeout");

			const forgedTarget: ParentWakeTarget = { ...wakeTarget, projectDir: `${projectDir}-not-this-scope` };
			let rejected: unknown;
			try {
				await caller.request({ op: "wake-ack", eventId: "evt-forge-1", target: forgedTarget });
			} catch (error) {
				rejected = error;
			}
			expect(rejected).toBeInstanceOf(Error);
		} finally {
			await stopBroker([caller, target], caller, broker);
		}
	}, 20_000);

	it("accepts a valid late ack after wake-submit already returned timeout and persists the true acked outcome", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-late-ack-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const caller = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const target = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const wakeTarget: ParentWakeTarget = { projectDir, sessionId: "parent-late-ack", runtimeScope: "candidate-a" };
			// Raw registration with no sink: the real target never auto-acks, so the
			// submitter's bounded wait genuinely times out while the event is still
			// "dispatched" broker-side.
			await target.request({ op: "wake-register", target: wakeTarget });
			const submitted = await caller.request({
				op: "wake-submit",
				event: event(wakeTarget, "evt-late-ack-1"),
				timeoutMs: 200,
			});
			if (submitted.op !== "wake-submit") throw new Error("unexpected op");
			expect(submitted.result.status).toBe("failed");
			expect(submitted.result.failureCode).toBe("timeout");

			// The real target's sink finishes processing after the submitter already
			// gave up; its ack must still land and overwrite the stale timeout.
			const lateAck = await target.request({ op: "wake-ack", eventId: "evt-late-ack-1", target: wakeTarget });
			if (lateAck.op !== "wake-ack") throw new Error("unexpected op");
			expect(lateAck.result.status).toBe("acked");
			expect(lateAck.result.failureCode).toBeUndefined();

			// A replay of the same eventId must report the durable acked outcome,
			// not the discarded timeout.
			const replay = await caller.request({
				op: "wake-submit",
				event: event(wakeTarget, "evt-late-ack-1"),
				timeoutMs: 5_000,
			});
			if (replay.op !== "wake-submit") throw new Error("unexpected op");
			expect(replay.result.status).toBe("acked");
			expect(replay.result.failureCode).toBeUndefined();
		} finally {
			await stopBroker([caller, target], caller, broker);
		}
	}, 20_000);

	it("resolveParentWakeTargetFromEnvironment is a no-op unless both opt-in env vars are set", () => {
		const previousSessionId = process.env[PARENT_WAKE_TARGET_SESSION_ID_ENV];
		const previousRuntimeScope = process.env[PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV];
		try {
			delete process.env[PARENT_WAKE_TARGET_SESSION_ID_ENV];
			delete process.env[PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV];
			expect(resolveParentWakeTargetFromEnvironment("/some/project")).toBeUndefined();
			process.env[PARENT_WAKE_TARGET_SESSION_ID_ENV] = "parent-x";
			expect(resolveParentWakeTargetFromEnvironment("/some/project")).toBeUndefined();
			process.env[PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV] = "candidate-x";
			expect(resolveParentWakeTargetFromEnvironment("/some/project")).toEqual({
				projectDir: "/some/project",
				sessionId: "parent-x",
				runtimeScope: "candidate-x",
			});
		} finally {
			restoreEnv(PARENT_WAKE_TARGET_SESSION_ID_ENV, previousSessionId);
			restoreEnv(PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV, previousRuntimeScope);
		}
	});

	it("startParentWakeListenerFromEnvironment never connects to a broker when the opt-in env vars are unset", async () => {
		const previousSessionId = process.env[PARENT_WAKE_TARGET_SESSION_ID_ENV];
		const previousRuntimeScope = process.env[PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV];
		delete process.env[PARENT_WAKE_TARGET_SESSION_ID_ENV];
		delete process.env[PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV];
		try {
			const handle = await startParentWakeListenerFromEnvironment("/nonexistent/project/path", {
				queueParentWake: async () => {},
			});
			expect(handle).toBeUndefined();
		} finally {
			restoreEnv(PARENT_WAKE_TARGET_SESSION_ID_ENV, previousSessionId);
			restoreEnv(PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV, previousRuntimeScope);
		}
	});

	it("registers under the connected client's canonical projectDir when started from a symlinked session cwd", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-symlink-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		const symlinkDir = path.join(tempDir.path(), "project-link");
		await fs.mkdir(projectDir);
		await fs.symlink(projectDir, symlinkDir, "dir");
		const prober = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const listenerClient = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);

		const previousSessionId = process.env[PARENT_WAKE_TARGET_SESSION_ID_ENV];
		const previousRuntimeScope = process.env[PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV];
		process.env[PARENT_WAKE_TARGET_SESSION_ID_ENV] = "parent-symlink";
		process.env[PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV] = "candidate-symlink";
		// Only the broker-connection lookup is faked, to the real, already-connected
		// `listenerClient` created above; registration, dispatch, and ack still
		// travel over the real socket to the real broker started above, so this
		// still proves real end-to-end target identity, not a mocked assertion.
		const daemonClientForProjectSpy = vi
			.spyOn(daemonClient, "daemonClientForProject")
			.mockResolvedValue(listenerClient);
		try {
			const received: ParentWakeEvent[] = [];
			const handle = await startParentWakeListenerFromEnvironment(symlinkDir, {
				queueParentWake: async wakeEvent => {
					received.push(wakeEvent);
				},
			});
			expect(handle).toBeDefined();

			// Submitted under the connected client's own canonical projectDir — the
			// identity a real caller (e.g. the `omp parent-wake` CLI, which also
			// canonicalizes via the same helper) would use. Before the fix, the
			// listener registered target.projectDir as the raw symlinked session
			// cwd, which the broker's #validateWakeTarget rejects outright, so this
			// would never dispatch (registration silently failed, target-missing
			// forever).
			const wakeTarget: ParentWakeTarget = {
				projectDir: listenerClient.projectDir,
				sessionId: "parent-symlink",
				runtimeScope: "candidate-symlink",
			};
			let ackedAttemptId: string | undefined;
			await waitFor(async () => {
				const attemptId = `evt-symlink-${crypto.randomUUID()}`;
				const attempt = await prober.request({
					op: "wake-submit",
					event: event(wakeTarget, attemptId),
					timeoutMs: 2_000,
				});
				if (attempt.op === "wake-submit" && attempt.result.status === "acked") {
					ackedAttemptId = attemptId;
					return true;
				}
				return false;
			});
			if (!ackedAttemptId) throw new Error("expected an acked attempt");
			expect(received).toHaveLength(1);
			expect(received[0]?.eventId).toBe(ackedAttemptId);
			handle?.stop();
		} finally {
			daemonClientForProjectSpy.mockRestore();
			restoreEnv(PARENT_WAKE_TARGET_SESSION_ID_ENV, previousSessionId);
			restoreEnv(PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV, previousRuntimeScope);
			await stopBroker([prober, listenerClient], prober, broker);
		}
	}, 20_000);

	it("tears down the wake registration when the owning session's identity is replaced", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-lifecycle-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const prober = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const listenerClient = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const wakeTarget: ParentWakeTarget = { projectDir, sessionId: "parent-lifecycle", runtimeScope: "candidate-a" };
			const received: ParentWakeEvent[] = [];
			const unregister = listenerClient.onParentWake(wakeTarget, async wakeEvent => {
				received.push(wakeEvent);
			});
			let sessionChangeCallback: (() => void) | undefined;
			stopParentWakeListenerOnSessionChange({ stop: unregister }, callback => {
				sessionChangeCallback = callback;
			});

			// The registration RPC is fire-and-forget (matches onCompletion's owner
			// registration semantics): poll with fresh eventIds until it has actually
			// landed broker-side and dispatch succeeds.
			let ackedAttemptId: string | undefined;
			await waitFor(async () => {
				const attemptId = `evt-lifecycle-${crypto.randomUUID()}`;
				const attempt = await prober.request({
					op: "wake-submit",
					event: event(wakeTarget, attemptId),
					timeoutMs: 2_000,
				});
				if (attempt.op === "wake-submit" && attempt.result.status === "acked") {
					ackedAttemptId = attemptId;
					return true;
				}
				return false;
			});
			expect(ackedAttemptId).toBeDefined();
			expect(received).toHaveLength(1);

			// Simulate this AgentSession adopting a different session ID (fork/resume/move).
			expect(sessionChangeCallback).toBeDefined();
			sessionChangeCallback?.();
			await waitFor(async () => {
				const probe = await prober.request({
					op: "wake-submit",
					event: event(wakeTarget, "evt-lifecycle-probe"),
					timeoutMs: 5_000,
				});
				return probe.op === "wake-submit" && probe.result.failureCode === "target-stale";
			});
			expect(received).toHaveLength(1);
		} finally {
			await stopBroker([prober, listenerClient], prober, broker);
		}
	}, 20_000);
	it("joins an isolated broker named by the runtime-dir env and never touches the shared project broker", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-isolated-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const prober = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);

		const previousSessionId = process.env[PARENT_WAKE_TARGET_SESSION_ID_ENV];
		const previousRuntimeScope = process.env[PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV];
		const previousRuntimeDir = process.env[PARENT_WAKE_BROKER_RUNTIME_DIR_ENV];
		process.env[PARENT_WAKE_TARGET_SESSION_ID_ENV] = "parent-isolated";
		process.env[PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV] = "candidate-isolated";
		process.env[PARENT_WAKE_BROKER_RUNTIME_DIR_ENV] = runtimeDir;
		// Pass-through observer: the shared project broker lookup must never be reached.
		const sharedLookup = vi.spyOn(daemonClient, "daemonClientForProject");
		try {
			const received: ParentWakeEvent[] = [];
			const handle = await startParentWakeListenerFromEnvironment(projectDir, {
				queueParentWake: async wakeEvent => {
					received.push(wakeEvent);
				},
			});
			expect(handle).toBeDefined();
			const wakeTarget: ParentWakeTarget = {
				projectDir: prober.projectDir,
				sessionId: "parent-isolated",
				runtimeScope: "candidate-isolated",
			};
			let ackedAttemptId: string | undefined;
			await waitFor(async () => {
				const attemptId = `evt-isolated-${crypto.randomUUID()}`;
				const attempt = await prober.request({
					op: "wake-submit",
					event: event(wakeTarget, attemptId),
					timeoutMs: 2_000,
				});
				if (attempt.op === "wake-submit" && attempt.result.status === "acked") {
					ackedAttemptId = attemptId;
					return true;
				}
				return false;
			});
			if (!ackedAttemptId) throw new Error("expected an acked attempt");
			expect(received.map(item => item.eventId)).toEqual([ackedAttemptId]);
			expect(sharedLookup).not.toHaveBeenCalled();

			// Stopping tombstones the registration on the isolated broker and closes the owned client.
			handle?.stop();
			await waitFor(async () => {
				const probe = await prober.request({
					op: "wake-submit",
					event: event(wakeTarget, `evt-isolated-stale-${crypto.randomUUID()}`),
					timeoutMs: 2_000,
				});
				return probe.op === "wake-submit" && probe.result.failureCode === "target-stale";
			});
		} finally {
			sharedLookup.mockRestore();
			restoreEnv(PARENT_WAKE_TARGET_SESSION_ID_ENV, previousSessionId);
			restoreEnv(PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV, previousRuntimeScope);
			restoreEnv(PARENT_WAKE_BROKER_RUNTIME_DIR_ENV, previousRuntimeDir);
			await stopBroker([prober], prober, broker);
		}
	}, 20_000);

	it("refuses a relative or shared-root runtime dir for the isolated broker", async () => {
		expect(() => assertPrivateRuntimeDir("relative/run", "runtime dir")).toThrow("absolute");
		expect(() => assertPrivateRuntimeDir(path.join(getDaemonRuntimeRoot(), "0123456789abcdef"), "runtime dir")).toThrow(
			"shared daemon runtime root",
		);
		using tempDir = TempDir.createSync("@omp-parent-wake-private-dir-");
		expect(() => assertPrivateRuntimeDir(path.join(tempDir.path(), "run"), "runtime dir")).not.toThrow();

		const previousSessionId = process.env[PARENT_WAKE_TARGET_SESSION_ID_ENV];
		const previousRuntimeScope = process.env[PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV];
		const previousRuntimeDir = process.env[PARENT_WAKE_BROKER_RUNTIME_DIR_ENV];
		process.env[PARENT_WAKE_TARGET_SESSION_ID_ENV] = "parent-refused";
		process.env[PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV] = "candidate-refused";
		process.env[PARENT_WAKE_BROKER_RUNTIME_DIR_ENV] = path.join(getDaemonRuntimeRoot(), "0123456789abcdef");
		try {
			let rejected: unknown;
			try {
				await startParentWakeListenerFromEnvironment(tempDir.path(), { queueParentWake: async () => {} });
			} catch (error) {
				rejected = error;
			}
			expect(rejected).toBeInstanceOf(Error);
		} finally {
			restoreEnv(PARENT_WAKE_TARGET_SESSION_ID_ENV, previousSessionId);
			restoreEnv(PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV, previousRuntimeScope);
			restoreEnv(PARENT_WAKE_BROKER_RUNTIME_DIR_ENV, previousRuntimeDir);
		}
	});

	it("does not acknowledge when the target sink rejects the wake", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-sink-reject-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const caller = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const target = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const wakeTarget: ParentWakeTarget = { projectDir, sessionId: "parent-reject", runtimeScope: "candidate-a" };
			target.onParentWake(wakeTarget, () => {
				throw new Error("session refused the wake");
			});
			const result = await caller.request({
				op: "wake-submit",
				event: event(wakeTarget, "evt-reject-1"),
				timeoutMs: 400,
			});
			if (result.op !== "wake-submit") throw new Error("unexpected op");
			expect(result.result.status).not.toBe("acked");
			expect(result.result.ackedAt).toBeUndefined();
		} finally {
			await stopBroker([caller, target], caller, broker);
		}
	}, 20_000);
	it("fails closed without dispatching when the event cannot be durably recorded", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-persist-fail-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const caller = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const target = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const wakeTarget: ParentWakeTarget = { projectDir, sessionId: "parent-persist", runtimeScope: "candidate-a" };
			const received: ParentWakeEvent[] = [];
			target.onParentWake(wakeTarget, wakeEvent => {
				received.push(wakeEvent);
			});
			// Registration is durable first (creates parent-wake/registrations); then the events
			// directory is made unwritable by occupying its path with a regular file.
			await waitFor(async () => {
				const probe = await caller.request({
					op: "wake-submit",
					event: event(wakeTarget, `evt-persist-probe-${crypto.randomUUID()}`),
					timeoutMs: 2_000,
				});
				return probe.op === "wake-submit" && probe.result.status === "acked";
			});
			const eventsDir = path.join(runtimeDir, "parent-wake", "events");
			await fs.rm(eventsDir, { recursive: true, force: true });
			await fs.writeFile(eventsDir, "not a directory");
			const deliveredBefore = received.length;

			let rejected: unknown;
			try {
				await caller.request({ op: "wake-submit", event: event(wakeTarget, "evt-persist-fail"), timeoutMs: 1_000 });
			} catch (error) {
				rejected = error;
			}
			expect(rejected).toBeInstanceOf(Error);
			expect((rejected as Error).message).toContain("durably recorded");
			expect(received).toHaveLength(deliveredBefore);
		} finally {
			await stopBroker([caller, target], caller, broker);
		}
	}, 20_000);
});
