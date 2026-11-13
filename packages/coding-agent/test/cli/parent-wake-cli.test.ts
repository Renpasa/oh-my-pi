import { describe, expect, it } from "bun:test";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { TempDir } from "@oh-my-pi/pi-utils";
import { startDaemonBrokerFromEnvironment } from "../../src/launch/broker";
import { createDaemonBrokerClient, type DaemonBrokerClient } from "../../src/launch/client";
import { DAEMON_IDLE_GRACE_ENV, DAEMON_PROJECT_DIR_ENV, DAEMON_RUNTIME_DIR_ENV } from "../../src/launch/protocol";

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

async function stopBroker(client: DaemonBrokerClient, broker: Promise<void>): Promise<void> {
	await client.request({ op: "shutdown" }).catch(() => undefined);
	client.close();
	await broker;
}

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 5_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		if (await predicate()) return;
		if (Date.now() >= deadline) throw new Error(`Condition did not become true within ${timeoutMs}ms`);
		await Bun.sleep(5);
	}
}


const CLI_ENTRY = path.resolve(import.meta.dir, "../../src/cli.ts");

async function runCli(stdin: string, args: string[]): Promise<{ exitCode: number; stdout: string; stderr: string }> {
	const proc = Bun.spawn(["bun", "run", CLI_ENTRY, "parent-wake", ...args], {
		stdin: new TextEncoder().encode(stdin),
		stdout: "pipe",
		stderr: "pipe",
	});
	const [stdout, stderr, exitCode] = await Promise.all([
		new Response(proc.stdout).text(),
		new Response(proc.stderr).text(),
		proc.exited,
	]);
	return { exitCode, stdout, stderr };
}
describe("omp parent-wake CLI", () => {
	it("submits an adapter-schema event over stdin and prints the adapter-schema result, acked", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-cli-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const target = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const wakeTarget = { projectDir, sessionId: "parent-cli", runtimeScope: "candidate-cli" };
			target.onParentWake(wakeTarget, () => {});
			// The registration RPC is fire-and-forget; wait for it to actually land
			// before invoking the CLI, using a real probe rather than a guessed delay.
			await waitFor(async () => {
				const probe = await target.request({
					op: "wake-submit",
					event: {
						eventId: `probe-${crypto.randomUUID()}`,
						kind: "probe",
						pointers: [],
						createdAt: Date.now(),
						target: wakeTarget,
					},
					timeoutMs: 2_000,
				});
				return probe.op === "wake-submit" && probe.result.status === "acked";
			});

			const adapterEvent = {
				event_id: "evt-cli-1",
				event_kind: "candidate-ready",
				canonical_pointers: [{ kind: "artifact", ref: "local://report.md" }],
				created_at: 1_700_000_000_000,
				target: { scope: projectDir, parent_session_id: "parent-cli", candidate_runtime_id: "candidate-cli" },
				producer_session_id: "orchestrator-1",
			};
			const { exitCode, stdout, stderr } = await runCli(JSON.stringify(adapterEvent), [
				"--project-dir",
				projectDir,
				"--runtime-dir",
				runtimeDir,
				"--timeout",
				"5000",
			]);
			expect(stderr).toBe("");
			const lines = stdout.trim().split("\n");
			expect(lines).toHaveLength(1);
			const result: unknown = JSON.parse(lines[0]);
			expect(result).toMatchObject({
				event_id: "evt-cli-1",
				target: { scope: projectDir, parent_session_id: "parent-cli", candidate_runtime_id: "candidate-cli" },
				dispatch_status: "dispatched",
				ack_status: "acked",
				timestamps: { created_at: 1_700_000_000_000 },
			});
			expect(exitCode).toBe(0);
		} finally {
			await stopBroker(target, broker);
		}
	}, 30_000);

	it("exits non-zero with a structured target-missing result when no target is registered", async () => {
		using tempDir = TempDir.createSync("@omp-parent-wake-cli-missing-");
		const projectDir = path.join(tempDir.path(), "project");
		const runtimeDir = path.join(tempDir.path(), "runtime");
		await fs.mkdir(projectDir);
		const prober = await createDaemonBrokerClient(projectDir, { runtimeDir, idleGraceMs: 5_000 });
		const broker = startBroker(projectDir, runtimeDir);
		try {
			const adapterEvent = {
				event_id: "evt-cli-missing-1",
				event_kind: "candidate-ready",
				canonical_pointers: [],
				created_at: 1_700_000_000_000,
				target: { scope: projectDir, parent_session_id: "ghost", candidate_runtime_id: "candidate-cli" },
			};
			const { exitCode, stdout } = await runCli(JSON.stringify(adapterEvent), [
				"--project-dir",
				projectDir,
				"--runtime-dir",
				runtimeDir,
			]);
			const result: unknown = JSON.parse(stdout.trim());
			expect(result).toMatchObject({
				dispatch_status: "failed",
				ack_status: "pending",
				failure_code: "target-missing",
			});
			expect(exitCode).toBe(1);
		} finally {
			await stopBroker(prober, broker);
		}
	}, 30_000);

	it("exits non-zero and prints no stdout result for malformed stdin JSON", async () => {
		const { exitCode, stdout, stderr } = await runCli("not json", ["--project-dir", "/tmp"]);
		expect(stdout).toBe("");
		expect(stderr).toContain("invalid JSON");
		expect(exitCode).toBe(1);
	}, 15_000);
});
