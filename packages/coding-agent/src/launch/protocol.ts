import { isRecord } from "@oh-my-pi/pi-utils";
import {
	type DaemonState,
	type DaemonRestartPolicy,
	type DaemonReadySpec,
	type DaemonSpec,
	type DaemonSnapshot,
} from "@oh-my-pi/pi-tui/tools/daemon";
/**
 * Cross-process daemon broker protocol shared by the tool, client, and broker.
 */
export { DAEMON_BROKER_WORKER_ARG } from "../cli/worker-selectors";

/** Fixed dimensions negotiated with every supervised PTY. */
export const DAEMON_PTY_COLUMNS = 120;
export const DAEMON_PTY_ROWS = 40;

/** Environment key carrying the broker's canonical project or synthetic global scope directory. */
export const DAEMON_PROJECT_DIR_ENV = "OMP_DAEMON_PROJECT_DIR";

/** Environment key carrying the broker's private runtime directory. */
export const DAEMON_RUNTIME_DIR_ENV = "OMP_DAEMON_RUNTIME_DIR";

/** Optional environment key overriding last-client shutdown grace. */
export const DAEMON_IDLE_GRACE_ENV = "OMP_DAEMON_IDLE_GRACE_MS";

/**
 * Parent Wake Transport v1 — opt-in environment keys.
 *
 * Unset (the default): the launching AgentSession never registers as a
 * Parent Wake target and ordinary/global OMP behavior is unchanged. Both
 * keys must be set together to opt in; see {@link ParentWakeTarget}.
 */
/** Stable identity of the target Parent AgentSession to register under. */
export const PARENT_WAKE_TARGET_SESSION_ID_ENV = "OMP_PARENT_WAKE_TARGET_SESSION_ID";
/** Candidate/runtime provenance accompanying {@link PARENT_WAKE_TARGET_SESSION_ID_ENV}. */
export const PARENT_WAKE_TARGET_RUNTIME_SCOPE_ENV = "OMP_PARENT_WAKE_RUNTIME_SCOPE";
/**
 * Optional absolute private runtime directory of an isolated broker. When set, the target registers
 * with the broker owned by that directory instead of the shared project broker (the counterpart of
 * `omp parent-wake --runtime-dir`).
 */
export const PARENT_WAKE_BROKER_RUNTIME_DIR_ENV = "OMP_PARENT_WAKE_BROKER_RUNTIME_DIR";

/** Signals accepted by daemon input operations. */
export type DaemonSignal = "SIGINT" | "SIGTERM" | "SIGHUP" | "SIGQUIT" | "SIGKILL";

/**
 * Parent Wake Transport v1 — cross-process wake delivery to one explicitly
 * registered Parent AgentSession, routed through the existing daemon broker
 * socket for one project/runtime scope.
 *
 * Field names below are the wire contract the `omp parent-wake` CLI and any
 * external adapter (e.g. an orchestrator) read/write as JSON — keep them
 * stable.
 */

/**
 * Explicit identity of a Parent Wake target.
 *
 * - `projectDir`: canonical project directory (or synthetic global scope
 *   directory) that must match the broker's own scope — a target cannot
 *   register with, and an event cannot dispatch across, a different scope.
 * - `sessionId`: stable identity of the target Parent AgentSession. This is
 *   the routing key; distinct from any caller/submitter identity.
 * - `runtimeScope`: candidate/runtime provenance (e.g. a worktree or branch
 *   label) bound to `sessionId` at registration time, so a caller supplying
 *   the right `sessionId` under the wrong `runtimeScope` cannot be mistaken
 *   for the real target (`wrong-target` failure).
 */
export interface ParentWakeTarget {
	projectDir: string;
	sessionId: string;
	runtimeScope: string;
}

/** One bounded, canonical pointer the target dereferences itself — never inline content. */
export interface ParentWakePointer {
	kind: string;
	ref: string;
}

/** Max length for `sessionId`, `runtimeScope`, and `eventId` (filesystem-safe charset). */
export const PARENT_WAKE_ID_MAX_LENGTH = 128;
/** Max pointers carried by one {@link ParentWakeEvent}. */
export const PARENT_WAKE_MAX_POINTERS = 8;
/** Max length of one pointer's `kind` field. */
export const PARENT_WAKE_POINTER_KIND_MAX_LENGTH = 64;
/** Max length of one pointer's `ref` field. */
export const PARENT_WAKE_POINTER_REF_MAX_LENGTH = 512;
/** Max length of the event `kind` field. */
export const PARENT_WAKE_KIND_MAX_LENGTH = 64;
/** Filesystem-safe identity charset shared by `sessionId`, `runtimeScope`, and `eventId`. */
export const PARENT_WAKE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

/**
 * One durable wake event. `eventId` is caller-assigned and is the dedupe key:
 * resubmitting the same `eventId` (retry, replay, or after a broker restart)
 * never dispatches a second wake — it returns the persisted {@link ParentWakeResult}.
 */
export interface ParentWakeEvent {
	eventId: string;
	kind: string;
	pointers: ParentWakePointer[];
	createdAt: number;
	target: ParentWakeTarget;
	/** Optional submitter identity for audit/logging — never used for routing or fail-closed decisions. */
	producerSessionId?: string;
}

/** Terminal reasons a Parent Wake event failed to reach an ACK. */
export type ParentWakeFailureCode =
	| "target-missing"
	| "target-dead"
	| "target-stale"
	| "wrong-target"
	| "timeout"
	| "invalid-payload";

/** Durable dispatch/ACK lifecycle of one {@link ParentWakeEvent}. */
export type ParentWakeDispatchStatus = "pending" | "dispatched" | "acked" | "failed";

/** Structured, durable outcome of one Parent Wake submission. */
export interface ParentWakeResult {
	eventId: string;
	target: ParentWakeTarget;
	status: ParentWakeDispatchStatus;
	dispatchedAt?: number;
	ackedAt?: number;
	failureCode?: ParentWakeFailureCode;
	failureMessage?: string;
}

/** Unsolicited push delivering one wake event to its registered target socket. */
export interface ParentWakeDelivery {
	event: "parent-wake";
	wakeEvent: ParentWakeEvent;
}

/** Typed broker operation sent over the authenticated socket. */
export type DaemonOperation =
	| { op: "ping" }
	| {
			op: "start";
			spec: DaemonSpec;
			owner?: string;
			replace?: boolean;
			/** `false` answers once the process launches; follow with a `wait` for readiness. Legacy brokers ignore it and wait. */
			awaitReady?: boolean;
	  }
	| { op: "list" }
	| {
			op: "logs";
			name: string;
			lines: number;
			head: boolean;
			grep?: string;
			follow: boolean;
			cursor?: number;
			/** Ask an upgraded broker to replay PTY output; absent preserves legacy raw-text responses. */
			renderTerminalRows?: boolean;
			timeoutMs: number;
	  }
	| { op: "wait"; name: string; for: "ready" | "exit"; pattern?: string; timeoutMs: number }
	| { op: "send"; name: string; data?: string; signal?: DaemonSignal }
	| {
			op: "stop";
			name: string;
			timeoutMs: number;
			/** Stop only this generation (`DaemonSnapshot.id`); another one is left running. Legacy brokers ignore it. */
			id?: string;
	  }
	| { op: "restart"; name: string }
	| { op: "mode"; name: string; mode: "persist" | "session" | "detached" }
	| { op: "describe"; name: string }
	/** Abort this connection's in-flight `wait`/follow-`logs` request `requestId`. Legacy brokers reject it. */
	| { op: "cancel"; requestId: string }
	| { op: "shutdown" }
	| { op: "wake-register"; target: ParentWakeTarget }
	| { op: "wake-unregister"; target: ParentWakeTarget }
	| { op: "wake-submit"; event: ParentWakeEvent; timeoutMs: number }
	| { op: "wake-ack"; eventId: string; target: ParentWakeTarget };

/** Typed broker result decoded before it reaches tool code. */
export type DaemonRpcResult =
	| { op: "ping"; projectDir: string }
	| { op: "start"; daemon: DaemonSnapshot; readyTimedOut: boolean }
	| { op: "list"; daemons: DaemonSnapshot[] }
	| {
			op: "logs";
			name: string;
			text: string;
			/** Virtual PTY rows reconstructed by the broker for terminal display. */
			terminalRows?: string[];
			/** Raw PTY bytes returned by legacy brokers and to clients that did not request rendered rows. */
			terminalText?: string;
			cursor: number;
			timedOut: boolean;
			state: DaemonState;
	  }
	| { op: "wait"; daemon: DaemonSnapshot; matched?: string; timedOut: boolean }
	| { op: "send"; daemon: DaemonSnapshot }
	| { op: "stop"; daemon: DaemonSnapshot }
	| { op: "restart"; daemon: DaemonSnapshot }
	| { op: "mode"; daemon: DaemonSnapshot }
	| { op: "describe"; daemon: DaemonSnapshot; spec: DaemonSpec }
	| { op: "cancel"; cancelled: boolean }
	| { op: "shutdown" }
	| { op: "wake-register"; target: ParentWakeTarget }
	| { op: "wake-unregister"; target: ParentWakeTarget }
	| { op: "wake-submit"; result: ParentWakeResult }
	| { op: "wake-ack"; result: ParentWakeResult };

/** Authenticated request envelope used by socket clients. */
export interface DaemonWireRequest {
	id: string;
	token: string;
	owners?: string[];
	detachedOwners?: string[];
	completionEvents?: boolean;
	completionAcks?: string[];
	completionUnsubscribes?: string[];
	completionReplays?: string[];
	completionSubscriptionId?: string;
	operation: DaemonOperation;
}

/** Response envelope kept raw until matched with its pending operation. */
export type DaemonWireResponse = { id: string; ok: true; result: unknown } | { id: string; ok: false; error: string };

/** Unsolicited terminal completion sent to the socket that owns a daemon. */
export interface DaemonCompletionNotification {
	event: "daemon-completed";
	completionId: string;
	owner: string;
	daemon: DaemonSnapshot;
}

export type DaemonWireMessage = DaemonWireResponse | DaemonCompletionNotification | ParentWakeDelivery;

function record(value: unknown, label: string): Record<string, unknown> {
	if (!isRecord(value)) throw new Error(`${label} must be an object`);
	return value;
}

function stringValue(value: unknown, label: string): string {
	if (typeof value !== "string" || value.length === 0) throw new Error(`${label} must be a non-empty string`);
	return value;
}
function rawString(value: unknown, label: string): string {
	if (typeof value !== "string") throw new Error(`${label} must be a string`);
	return value;
}

function optionalString(value: unknown, label: string): string | undefined {
	if (value === undefined) return undefined;
	return stringValue(value, label);
}

function optionalRawString(value: unknown, label: string): string | undefined {
	if (value === undefined) return undefined;
	return rawString(value, label);
}

function booleanValue(value: unknown, label: string): boolean {
	if (typeof value !== "boolean") throw new Error(`${label} must be a boolean`);
	return value;
}

function numberValue(value: unknown, label: string): number {
	if (typeof value !== "number" || !Number.isFinite(value)) throw new Error(`${label} must be a finite number`);
	return value;
}

function optionalNumber(value: unknown, label: string): number | undefined {
	if (value === undefined) return undefined;
	return numberValue(value, label);
}

function stringArray(value: unknown, label: string): string[] {
	if (!Array.isArray(value)) throw new Error(`${label} must be an array of strings`);
	const result: string[] = [];
	for (const item of value) result.push(rawString(item, `${label} item`));
	return result;
}

function stringRecord(value: unknown, label: string): Record<string, string> {
	const source = record(value, label);
	const result: Record<string, string> = {};
	for (const key in source) result[key] = rawString(source[key], `${label}.${key}`);
	return result;
}

function daemonState(value: unknown): DaemonState {
	const state = stringValue(value, "daemon state");
	if (state === "starting" || state === "running" || state === "ready" || state === "restarting") return state;
	if (state === "stopping" || state === "exited" || state === "failed") return state;
	throw new Error(`Unknown daemon state: ${state}`);
}

function restartPolicy(value: unknown): DaemonRestartPolicy {
	const policy = stringValue(value, "restart policy");
	if (policy === "no" || policy === "on-failure" || policy === "always") return policy;
	throw new Error(`Unknown restart policy: ${policy}`);
}

function daemonSignal(value: unknown): DaemonSignal {
	const signal = stringValue(value, "signal");
	if (signal === "SIGINT" || signal === "SIGTERM" || signal === "SIGHUP") return signal;
	if (signal === "SIGQUIT" || signal === "SIGKILL") return signal;
	throw new Error(`Unknown daemon signal: ${signal}`);
}

function readyPendingList(value: unknown): ("log" | "port")[] {
	if (!Array.isArray(value)) throw new Error("daemon.readyPending must be an array");
	const result: ("log" | "port")[] = [];
	for (const item of value) {
		if (item !== "log" && item !== "port") throw new Error(`Unknown readiness condition: ${String(item)}`);
		result.push(item);
	}
	return result;
}

function readySpec(value: unknown): DaemonReadySpec {
	const source = record(value, "ready");
	const log = optionalString(source.log, "ready.log");
	const port = optionalNumber(source.port, "ready.port");
	const host = optionalString(source.host, "ready.host");
	const timeoutMs = numberValue(source.timeoutMs, "ready.timeoutMs");
	if (!log && port === undefined) throw new Error("ready requires log or port");
	return { log, port, host, timeoutMs };
}

/** Decode and validate a daemon launch specification. */
export function parseDaemonSpec(value: unknown): DaemonSpec {
	const source = record(value, "daemon spec");
	const detached = source.detached === undefined ? false : booleanValue(source.detached, "spec.detached");
	return {
		name: stringValue(source.name, "spec.name"),
		application: stringValue(source.application, "spec.application"),
		args: stringArray(source.args, "spec.args"),
		env: stringRecord(source.env, "spec.env"),
		cwd: stringValue(source.cwd, "spec.cwd"),
		pty: booleanValue(source.pty, "spec.pty"),
		ready: source.ready === undefined ? undefined : readySpec(source.ready),
		restart: restartPolicy(source.restart),
		persist: booleanValue(source.persist, "spec.persist") || detached,
		detached,
	};
}

/** Decode and validate one daemon snapshot. */
export function parseDaemonSnapshot(value: unknown): DaemonSnapshot {
	const source = record(value, "daemon snapshot");
	return {
		name: stringValue(source.name, "daemon.name"),
		id: stringValue(source.id, "daemon.id"),
		state: daemonState(source.state),
		pid: optionalNumber(source.pid, "daemon.pid"),
		createdAt: numberValue(source.createdAt, "daemon.createdAt"),
		startedAt: numberValue(source.startedAt, "daemon.startedAt"),
		readyAt: optionalNumber(source.readyAt, "daemon.readyAt"),
		exitedAt: optionalNumber(source.exitedAt, "daemon.exitedAt"),
		exitCode: optionalNumber(source.exitCode, "daemon.exitCode"),
		exitReason: optionalString(source.exitReason, "daemon.exitReason"),
		restartCount: numberValue(source.restartCount, "daemon.restartCount"),
		outputBytes: numberValue(source.outputBytes, "daemon.outputBytes"),
		owner: optionalString(source.owner, "daemon.owner"),
		readyMatch: optionalRawString(source.readyMatch, "daemon.readyMatch"),
		readyPending: source.readyPending === undefined ? undefined : readyPendingList(source.readyPending),
		persist: booleanValue(source.persist, "daemon.persist"),
		detached: source.detached === undefined ? false : booleanValue(source.detached, "daemon.detached"),
	};
}

/** Decode a socket request before the broker acts on it. */
export function parseDaemonWireRequest(value: unknown): DaemonWireRequest {
	const source = record(value, "daemon request");
	return {
		id: stringValue(source.id, "request.id"),
		token: stringValue(source.token, "request.token"),
		owners: source.owners === undefined ? undefined : stringArray(source.owners, "request.owners"),
		detachedOwners:
			source.detachedOwners === undefined ? undefined : stringArray(source.detachedOwners, "request.detachedOwners"),
		completionEvents:
			source.completionEvents === undefined
				? undefined
				: booleanValue(source.completionEvents, "request.completionEvents"),
		completionAcks:
			source.completionAcks === undefined ? undefined : stringArray(source.completionAcks, "request.completionAcks"),
		completionUnsubscribes:
			source.completionUnsubscribes === undefined
				? undefined
				: stringArray(source.completionUnsubscribes, "request.completionUnsubscribes"),
		completionReplays:
			source.completionReplays === undefined
				? undefined
				: stringArray(source.completionReplays, "request.completionReplays"),
		completionSubscriptionId:
			source.completionSubscriptionId === undefined
				? undefined
				: stringValue(source.completionSubscriptionId, "request.completionSubscriptionId"),
		operation: parseDaemonOperation(source.operation),
	};
}

/** Decode a socket response envelope before resolving a pending call. */
export function parseDaemonWireResponse(value: unknown): DaemonWireResponse {
	const source = record(value, "daemon response");
	const id = stringValue(source.id, "response.id");
	if (source.ok === true) return { id, ok: true, result: source.result };
	if (source.ok === false) return { id, ok: false, error: stringValue(source.error, "response.error") };
	throw new Error("response.ok must be a boolean");
}

/** Decode one broker response or unsolicited completion notification. */
export function parseDaemonWireMessage(value: unknown): DaemonWireMessage {
	const source = record(value, "daemon message");
	if (source.event === "daemon-completed") {
		return {
			event: "daemon-completed",
			completionId: stringValue(source.completionId, "completion.id"),
			owner: stringValue(source.owner, "completion.owner"),
			daemon: parseDaemonSnapshot(source.daemon),
		};
	}
	if (source.event === "parent-wake") return parseParentWakeDelivery(value);
	return parseDaemonWireResponse(value);
}

function parseDaemonOperation(value: unknown): DaemonOperation {
	const source = record(value, "daemon operation");
	const op = stringValue(source.op, "operation.op");
	switch (op) {
		case "ping":
		case "list":
		case "shutdown":
			return { op };
		case "start":
			return {
				op,
				spec: parseDaemonSpec(source.spec),
				owner: optionalString(source.owner, "operation.owner"),
				replace: source.replace === undefined ? undefined : booleanValue(source.replace, "operation.replace"),
				awaitReady:
					source.awaitReady === undefined ? undefined : booleanValue(source.awaitReady, "operation.awaitReady"),
			};
		case "logs":
			return {
				op,
				name: stringValue(source.name, "operation.name"),
				lines: numberValue(source.lines, "operation.lines"),
				head: booleanValue(source.head, "operation.head"),
				grep: optionalString(source.grep, "operation.grep"),
				follow: booleanValue(source.follow, "operation.follow"),
				cursor: optionalNumber(source.cursor, "operation.cursor"),
				renderTerminalRows:
					source.renderTerminalRows === undefined
						? undefined
						: booleanValue(source.renderTerminalRows, "operation.renderTerminalRows"),
				timeoutMs: numberValue(source.timeoutMs, "operation.timeoutMs"),
			};
		case "wait": {
			const target = stringValue(source.for, "operation.for");
			if (target !== "ready" && target !== "exit") throw new Error("operation.for must be ready or exit");
			return {
				op,
				name: stringValue(source.name, "operation.name"),
				for: target,
				pattern: optionalString(source.pattern, "operation.pattern"),
				timeoutMs: numberValue(source.timeoutMs, "operation.timeoutMs"),
			};
		}
		case "send":
			return {
				op,
				name: stringValue(source.name, "operation.name"),
				data: optionalString(source.data, "operation.data"),
				signal: source.signal === undefined ? undefined : daemonSignal(source.signal),
			};
		case "stop":
			return {
				op,
				name: stringValue(source.name, "operation.name"),
				timeoutMs: numberValue(source.timeoutMs, "operation.timeoutMs"),
				id: optionalString(source.id, "operation.id"),
			};
		case "restart":
		case "describe":
			return { op, name: stringValue(source.name, "operation.name") };
		case "cancel":
			return { op, requestId: stringValue(source.requestId, "operation.requestId") };
		case "mode": {
			const mode = stringValue(source.mode, "operation.mode");
			if (mode !== "persist" && mode !== "session" && mode !== "detached")
				throw new Error("operation.mode must be persist, session, or detached");
			return { op, name: stringValue(source.name, "operation.name"), mode };
		}
		case "wake-register":
		case "wake-unregister":
			return { op, target: parseParentWakeTarget(source.target) };
		case "wake-submit":
			return {
				op,
				event: parseParentWakeEvent(source.event),
				timeoutMs: numberValue(source.timeoutMs, "operation.timeoutMs"),
			};
		case "wake-ack":
			return {
				op,
				eventId: parentWakeId(source.eventId, "operation.eventId"),
				target: parseParentWakeTarget(source.target),
			};
		default:
			throw new Error(`Unknown daemon operation: ${op}`);
	}
}

/** Decode a broker result using its pending operation as the discriminator. */
export function parseDaemonRpcResult(operation: DaemonOperation, value: unknown): DaemonRpcResult {
	const source = record(value, `${operation.op} result`);
	switch (operation.op) {
		case "ping":
			return { op: "ping", projectDir: stringValue(source.projectDir, "result.projectDir") };
		case "start":
			return {
				op: "start",
				daemon: parseDaemonSnapshot(source.daemon),
				readyTimedOut: booleanValue(source.readyTimedOut, "result.readyTimedOut"),
			};
		case "list": {
			if (!Array.isArray(source.daemons)) throw new Error("result.daemons must be an array");
			return { op: "list", daemons: source.daemons.map(parseDaemonSnapshot) };
		}
		case "logs":
			return {
				op: "logs",
				name: stringValue(source.name, "result.name"),
				text: typeof source.text === "string" ? source.text : "",
				terminalRows:
					source.terminalRows === undefined ? undefined : stringArray(source.terminalRows, "result.terminalRows"),
				terminalText:
					source.terminalText === undefined ? undefined : rawString(source.terminalText, "result.terminalText"),
				cursor: numberValue(source.cursor, "result.cursor"),
				timedOut: booleanValue(source.timedOut, "result.timedOut"),
				state: daemonState(source.state),
			};
		case "wait":
			return {
				op: "wait",
				daemon: parseDaemonSnapshot(source.daemon),
				matched: optionalRawString(source.matched, "result.matched"),
				timedOut: booleanValue(source.timedOut, "result.timedOut"),
			};
		case "send":
			return { op: "send", daemon: parseDaemonSnapshot(source.daemon) };
		case "stop":
			return { op: "stop", daemon: parseDaemonSnapshot(source.daemon) };
		case "restart":
			return { op: "restart", daemon: parseDaemonSnapshot(source.daemon) };
		case "mode":
			return { op: "mode", daemon: parseDaemonSnapshot(source.daemon) };
		case "describe":
			return {
				op: "describe",
				daemon: parseDaemonSnapshot(source.daemon),
				spec: parseDaemonSpec(source.spec),
			};
		case "cancel":
			return { op: "cancel", cancelled: booleanValue(source.cancelled, "result.cancelled") };
		case "shutdown":
			return { op: "shutdown" };
		case "wake-register":
		case "wake-unregister":
			return { op: operation.op, target: parseParentWakeTarget(source.target) };
		case "wake-submit":
			return { op: "wake-submit", result: parseParentWakeResult(source.result) };
		case "wake-ack":
			return { op: "wake-ack", result: parseParentWakeResult(source.result) };
	}
}

function parentWakeId(value: unknown, label: string): string {
	const id = stringValue(value, label);
	if (!PARENT_WAKE_ID_PATTERN.test(id)) {
		throw new Error(`${label} must be 1-${PARENT_WAKE_ID_MAX_LENGTH} letters, numbers, dots, underscores, or hyphens`);
	}
	return id;
}

/** Decode and validate a Parent Wake target identity. */
export function parseParentWakeTarget(value: unknown): ParentWakeTarget {
	const source = record(value, "wake target");
	return {
		projectDir: stringValue(source.projectDir, "target.projectDir"),
		sessionId: parentWakeId(source.sessionId, "target.sessionId"),
		runtimeScope: parentWakeId(source.runtimeScope, "target.runtimeScope"),
	};
}

function parseParentWakePointer(value: unknown, label: string): ParentWakePointer {
	const source = record(value, label);
	const kind = stringValue(source.kind, `${label}.kind`);
	const ref = stringValue(source.ref, `${label}.ref`);
	if (kind.length > PARENT_WAKE_POINTER_KIND_MAX_LENGTH) throw new Error(`${label}.kind is too long`);
	if (ref.length > PARENT_WAKE_POINTER_REF_MAX_LENGTH) throw new Error(`${label}.ref is too long`);
	return { kind, ref };
}

function parseParentWakePointers(value: unknown): ParentWakePointer[] {
	if (!Array.isArray(value)) throw new Error("event.pointers must be an array");
	if (value.length > PARENT_WAKE_MAX_POINTERS) {
		throw new Error(`event.pointers must have at most ${PARENT_WAKE_MAX_POINTERS} entries`);
	}
	return value.map((item, index) => parseParentWakePointer(item, `event.pointers[${index}]`));
}

/** Decode and validate one Parent Wake event, including its bounded pointer payload. */
export function parseParentWakeEvent(value: unknown): ParentWakeEvent {
	const source = record(value, "wake event");
	const kind = stringValue(source.kind, "event.kind");
	if (kind.length > PARENT_WAKE_KIND_MAX_LENGTH) throw new Error("event.kind is too long");
	return {
		eventId: parentWakeId(source.eventId, "event.eventId"),
		kind,
		pointers: parseParentWakePointers(source.pointers),
		createdAt: numberValue(source.createdAt, "event.createdAt"),
		target: parseParentWakeTarget(source.target),
		producerSessionId:
			source.producerSessionId === undefined ? undefined : parentWakeId(source.producerSessionId, "event.producerSessionId"),
	};
}

function parentWakeFailureCode(value: unknown): ParentWakeFailureCode {
	const code = stringValue(value, "result.failureCode");
	if (code === "target-missing" || code === "target-dead" || code === "target-stale") return code;
	if (code === "wrong-target" || code === "timeout" || code === "invalid-payload") return code;
	throw new Error(`Unknown parent wake failure code: ${code}`);
}

function parentWakeDispatchStatus(value: unknown): ParentWakeDispatchStatus {
	const status = stringValue(value, "result.status");
	if (status === "pending" || status === "dispatched" || status === "acked" || status === "failed") return status;
	throw new Error(`Unknown parent wake status: ${status}`);
}

/** Decode and validate one durable Parent Wake result. */
export function parseParentWakeResult(value: unknown): ParentWakeResult {
	const source = record(value, "wake result");
	return {
		eventId: parentWakeId(source.eventId, "result.eventId"),
		target: parseParentWakeTarget(source.target),
		status: parentWakeDispatchStatus(source.status),
		dispatchedAt: optionalNumber(source.dispatchedAt, "result.dispatchedAt"),
		ackedAt: optionalNumber(source.ackedAt, "result.ackedAt"),
		failureCode: source.failureCode === undefined ? undefined : parentWakeFailureCode(source.failureCode),
		failureMessage: optionalString(source.failureMessage, "result.failureMessage"),
	};
}

/** Decode and validate one unsolicited Parent Wake delivery. */
export function parseParentWakeDelivery(value: unknown): ParentWakeDelivery {
	const source = record(value, "parent wake delivery");
	if (source.event !== "parent-wake") throw new Error("delivery.event must be parent-wake");
	return { event: "parent-wake", wakeEvent: parseParentWakeEvent(source.wakeEvent) };
}
