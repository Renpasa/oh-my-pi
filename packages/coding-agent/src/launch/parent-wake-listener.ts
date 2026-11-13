/**
 * Parent Wake Transport v1 — opt-in target listener.
 *
 * Wires a live AgentSession as the Parent Wake target for one project/runtime
 * scope, over a daemon broker connection. Gated entirely by
 * {@link PARENT_WAKE_TARGET_SESSION_ID_ENV} and
 * {@link PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV}: either unset (the default) is a
 * no-op, so ordinary/global OMP behavior is unchanged unless a caller (e.g.
 * an external orchestrator) deliberately opts one launch in.
 *
 * Broker isolation: by default the target joins the project's shared daemon
 * broker. Setting {@link PARENT_WAKE_BROKER_RUNTIME_DIR_ENV} instead connects an
 * independent client to a broker owned by that private runtime directory (the
 * same directory `omp parent-wake --runtime-dir` submits to), so a candidate can
 * be exercised without touching, replacing, or shutting down the shared broker.
 */
import { logger, postmortem } from "@oh-my-pi/pi-utils";
import { createDaemonBrokerClient, type DaemonBrokerClient, daemonClientForProject } from "./client";
import { assertPrivateRuntimeDir } from "./paths";
import {
	PARENT_WAKE_BROKER_RUNTIME_DIR_ENV,
	PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV,
	PARENT_WAKE_TARGET_SESSION_ID_ENV,
	type ParentWakeEvent,
	type ParentWakeTarget,
} from "./protocol";

export interface ParentWakeListenerHandle {
	stop(): void;
}

/** The subset of `AgentSession` the listener needs — accepts one delivered wake event. */
export interface ParentWakeListenerHost {
	queueParentWake(event: ParentWakeEvent): Promise<void>;
}

/** Read the opt-in env pair. Returns undefined when either is unset — the default, no-op case. */
export function resolveParentWakeTargetFromEnvironment(projectDir: string): ParentWakeTarget | undefined {
	const sessionId = process.env[PARENT_WAKE_TARGET_SESSION_ID_ENV];
	const runtimeScope = process.env[PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV];
	if (!sessionId || !runtimeScope) return undefined;
	return { projectDir, sessionId, runtimeScope };
}

/**
 * Connect the broker client this target registers on: the shared project
 * broker by default, or an independent client on the private runtime directory
 * named by {@link PARENT_WAKE_BROKER_RUNTIME_DIR_ENV}. `owned` is true for the
 * independent client, which this process must close itself.
 */
async function connectWakeBroker(projectDir: string): Promise<{ client: DaemonBrokerClient; owned: boolean }> {
	const runtimeDir = process.env[PARENT_WAKE_BROKER_RUNTIME_DIR_ENV];
	if (!runtimeDir) return { client: await daemonClientForProject(projectDir), owned: false };
	assertPrivateRuntimeDir(runtimeDir, PARENT_WAKE_BROKER_RUNTIME_DIR_ENV);
	return { client: await createDaemonBrokerClient(projectDir, { runtimeDir }), owned: true };
}

/**
 * Register `host` as the live Parent Wake target for this process. No-op
 * (resolves to undefined) unless both opt-in env vars are set. Each delivered
 * event is queued as one bounded hidden wake message via
 * {@link ParentWakeListenerHost.queueParentWake}; the broker ACKs the wake
 * only once that promise resolves, so a durable ACK means the target session
 * actually accepted it. Duplicate delivery of the same `eventId` is a
 * transport-level no-op handled by the daemon broker client.
 *
 * `target.projectDir` is taken from the connected client's canonical
 * `projectDir`, never the raw `projectDir` argument: the broker validates
 * `wake-register`/`wake-submit`/`wake-ack` targets against its own
 * canonicalized scope, so registering under an uncanonicalized session cwd
 * (e.g. one reached through a symlink) would otherwise be rejected outright.
 */
export async function startParentWakeListenerFromEnvironment(
	projectDir: string,
	host: ParentWakeListenerHost,
): Promise<ParentWakeListenerHandle | undefined> {
	const resolved = resolveParentWakeTargetFromEnvironment(projectDir);
	if (!resolved) return undefined;
	const { client, owned } = await connectWakeBroker(projectDir);
	const target: ParentWakeTarget = { ...resolved, projectDir: client.projectDir };
	const unregister = client.onParentWake(target, event =>
		host.queueParentWake(event).catch(error => {
			logger.warn("Parent wake delivery rejected by target session", {
				sessionId: target.sessionId,
				eventId: event.eventId,
				error: error instanceof Error ? error.message : String(error),
			});
			throw error;
		}),
	);
	// An independent client is not tracked by the shared-client registry, so close
	// it with the process; the shared project client is closed by `closeDaemonClients`.
	const cancelExitCleanup = owned
		? postmortem.register(`parent-wake-listener:${target.sessionId}`, () => client.close())
		: undefined;
	logger.info("Parent wake target registered", { sessionId: target.sessionId, runtimeScope: target.runtimeScope });
	let stopped = false;
	return {
		stop: () => {
			if (stopped) return;
			stopped = true;
			const unregistered = unregister();
			if (owned) {
				cancelExitCleanup?.();
				// Close only after the graceful wake-unregister settled, or it would be dropped.
				void unregistered.finally(() => client.close());
			}
		},
	};
}

/**
 * Tie a Parent Wake listener handle to session-identity replacement: once the
 * owning AgentSession adopts a different session ID (fork/resume/move), the
 * registration this handle owns is torn down (a graceful `wake-unregister`,
 * tombstoning it `target-stale`) so it cannot survive past the identity it
 * was registered under — otherwise a stale live connection could keep
 * answering wakes addressed to a session identity this process no longer is.
 */
export function stopParentWakeListenerOnSessionChange(
	handle: ParentWakeListenerHandle,
	registerSessionChangeCallback: (callback: () => void) => void,
): void {
	registerSessionChangeCallback(() => handle.stop());
}
