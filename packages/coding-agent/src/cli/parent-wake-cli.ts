/**
 * `omp parent-wake` — submit one Parent Wake Transport v1 event to the daemon
 * broker and print the durable result.
 *
 * This is the stable external-adapter boundary: stdin carries exactly one
 * JSON object in the adapter's schema (below), stdout carries exactly one
 * JSON result object in the adapter's schema. Internal candidate field names
 * (`eventId`, `kind`, `pointers`, `createdAt`, `target.projectDir`,
 * `target.sessionId`, `target.runtimeScope`, `status`, `failureCode` — see
 * `../launch/protocol.ts`) are translated here and never exposed on the wire
 * to external callers.
 *
 * Adapter input schema (stdin):
 *   {
 *     "event_id": string,                       // dedupe key; stable across retries
 *     "event_kind": string,
 *     "canonical_pointers": [{ "kind": string, "ref": string }, ...],
 *     "created_at": number,                      // epoch ms
 *     "target": {
 *       "scope": string,                         // matches --project-dir (canonicalized)
 *       "parent_session_id": string,
 *       "candidate_runtime_id": string
 *     },
 *     "producer_session_id"?: string              // informational only, never used for routing
 *   }
 *
 * Adapter output schema (stdout, always exactly one line):
 *   {
 *     "event_id": string,
 *     "target": { "scope": string, "parent_session_id": string, "candidate_runtime_id": string },
 *     "dispatch_status": "pending" | "dispatched" | "failed",
 *     "ack_status": "pending" | "acked" | "timeout",
 *     "timestamps": { "created_at": number, "dispatched_at"?: number, "acked_at"?: number },
 *     "failure_code"?: string                     // present only when dispatch_status is "failed" or ack_status is "timeout"
 *   }
 *
 * Exit code 0 iff ack_status is "acked"; 1 for any other outcome — including a
 * structured failure/timeout result, which is still printed on stdout. A
 * transport-level failure (unreachable broker, malformed stdin) prints to
 * stderr instead and never fabricates a result line.
 */

import { getProjectDir } from "@oh-my-pi/pi-utils";
import { createDaemonBrokerClient, type DaemonBrokerClient } from "../launch/client";
import { canonicalProjectDir, resolvePrivateRuntimeDir } from "../launch/paths";
import type { ParentWakeEvent, ParentWakePointer, ParentWakeResult } from "../launch/protocol";

export interface ParentWakeCommandArgs {
	flags: {
		projectDir?: string;
		runtimeDir?: string;
		timeout?: number;
	};
}

const DEFAULT_WAKE_TIMEOUT_MS = 30_000;

interface AdapterParentWakePointer {
	kind: string;
	ref: string;
}

interface AdapterParentWakeTarget {
	scope: string;
	parent_session_id: string;
	candidate_runtime_id: string;
}

interface AdapterParentWakeEvent {
	event_id: string;
	event_kind: string;
	canonical_pointers: AdapterParentWakePointer[];
	created_at: number;
	target: AdapterParentWakeTarget;
	producer_session_id?: string;
}

interface AdapterParentWakeResult {
	event_id: string;
	target: AdapterParentWakeTarget;
	dispatch_status: "pending" | "dispatched" | "failed";
	ack_status: "pending" | "acked" | "timeout";
	timestamps: { created_at: number; dispatched_at?: number; acked_at?: number };
	failure_code?: string;
}

function requireNonEmptyString(value: unknown, label: string): string {
	if (typeof value !== "string" || value.length === 0) throw new Error(`${label} must be a non-empty string`);
	return value;
}

function parseAdapterPointer(value: unknown, index: number): AdapterParentWakePointer {
	if (typeof value !== "object" || value === null) throw new Error(`canonical_pointers[${index}] must be an object`);
	const source = value as Record<string, unknown>;
	return {
		kind: requireNonEmptyString(source.kind, `canonical_pointers[${index}].kind`),
		ref: requireNonEmptyString(source.ref, `canonical_pointers[${index}].ref`),
	};
}

function parseAdapterTarget(value: unknown): AdapterParentWakeTarget {
	if (typeof value !== "object" || value === null) throw new Error("target must be an object");
	const source = value as Record<string, unknown>;
	return {
		scope: requireNonEmptyString(source.scope, "target.scope"),
		parent_session_id: requireNonEmptyString(source.parent_session_id, "target.parent_session_id"),
		candidate_runtime_id: requireNonEmptyString(source.candidate_runtime_id, "target.candidate_runtime_id"),
	};
}

/** Decode and validate one adapter-schema event read from stdin. */
export function parseAdapterParentWakeEvent(value: unknown): AdapterParentWakeEvent {
	if (typeof value !== "object" || value === null) throw new Error("stdin must contain one JSON object");
	const source = value as Record<string, unknown>;
	if (typeof source.created_at !== "number" || !Number.isFinite(source.created_at)) {
		throw new Error("created_at must be a finite number");
	}
	if (!Array.isArray(source.canonical_pointers)) throw new Error("canonical_pointers must be an array");
	if (source.producer_session_id !== undefined && typeof source.producer_session_id !== "string") {
		throw new Error("producer_session_id must be a string");
	}
	return {
		event_id: requireNonEmptyString(source.event_id, "event_id"),
		event_kind: requireNonEmptyString(source.event_kind, "event_kind"),
		canonical_pointers: source.canonical_pointers.map((item, index) => parseAdapterPointer(item, index)),
		created_at: source.created_at,
		target: parseAdapterTarget(source.target),
		producer_session_id: source.producer_session_id,
	};
}

/** Translate one adapter-schema event into the internal candidate transport shape. */
async function toInternalEvent(adapterEvent: AdapterParentWakeEvent): Promise<ParentWakeEvent> {
	const pointers: ParentWakePointer[] = adapterEvent.canonical_pointers.map(pointer => ({
		kind: pointer.kind,
		ref: pointer.ref,
	}));
	return {
		eventId: adapterEvent.event_id,
		kind: adapterEvent.event_kind,
		pointers,
		createdAt: adapterEvent.created_at,
		target: {
			projectDir: await canonicalProjectDir(adapterEvent.target.scope),
			sessionId: adapterEvent.target.parent_session_id,
			runtimeScope: adapterEvent.target.candidate_runtime_id,
		},
		producerSessionId: adapterEvent.producer_session_id,
	};
}

/**
 * Translate one durable candidate `ParentWakeResult` into the adapter's
 * two-axis (`dispatch_status`, `ack_status`) schema. The candidate side
 * tracks a single unified `status` + `failureCode`; this is the lossless
 * decomposition:
 *   - "acked"                              -> dispatched / acked
 *   - "dispatched"                         -> dispatched / pending
 *   - "failed" with failureCode "timeout"  -> dispatched / timeout (it *was* dispatched)
 *   - "failed" with any other failureCode  -> failed / pending (dispatch itself never succeeded)
 *   - "pending" (should never be externally observable)  -> pending / pending
 */
export function toAdapterParentWakeResult(
	result: ParentWakeResult,
	createdAt: number,
): AdapterParentWakeResult {
	let dispatchStatus: AdapterParentWakeResult["dispatch_status"];
	let ackStatus: AdapterParentWakeResult["ack_status"];
	if (result.status === "acked") {
		dispatchStatus = "dispatched";
		ackStatus = "acked";
	} else if (result.status === "dispatched") {
		dispatchStatus = "dispatched";
		ackStatus = "pending";
	} else if (result.status === "failed" && result.failureCode === "timeout") {
		dispatchStatus = "dispatched";
		ackStatus = "timeout";
	} else if (result.status === "failed") {
		dispatchStatus = "failed";
		ackStatus = "pending";
	} else {
		dispatchStatus = "pending";
		ackStatus = "pending";
	}
	return {
		event_id: result.eventId,
		target: {
			scope: result.target.projectDir,
			parent_session_id: result.target.sessionId,
			candidate_runtime_id: result.target.runtimeScope,
		},
		dispatch_status: dispatchStatus,
		ack_status: ackStatus,
		timestamps: { created_at: createdAt, dispatched_at: result.dispatchedAt, acked_at: result.ackedAt },
		failure_code: result.failureCode,
	};
}

export async function runParentWakeCommand(cmd: ParentWakeCommandArgs): Promise<void> {
	const raw = await Bun.stdin.text();
	let decoded: unknown;
	try {
		decoded = JSON.parse(raw);
	} catch (error) {
		process.stderr.write(`omp parent-wake: invalid JSON on stdin: ${error instanceof Error ? error.message : String(error)}\n`);
		process.exitCode = 1;
		return;
	}
	let adapterEvent: AdapterParentWakeEvent;
	try {
		adapterEvent = parseAdapterParentWakeEvent(decoded);
	} catch (error) {
		process.stderr.write(
			`omp parent-wake: invalid parent wake event: ${error instanceof Error ? error.message : String(error)}\n`,
		);
		process.exitCode = 1;
		return;
	}
	const projectDir = cmd.flags.projectDir ?? getProjectDir();
	const timeoutMs = cmd.flags.timeout ?? DEFAULT_WAKE_TIMEOUT_MS;
	let client: DaemonBrokerClient;
	try {
		const runtimeDir =
			cmd.flags.runtimeDir === undefined
				? undefined
				: await resolvePrivateRuntimeDir(cmd.flags.runtimeDir, "--runtime-dir");
		client = await createDaemonBrokerClient(projectDir, runtimeDir === undefined ? {} : { runtimeDir });
	} catch (error) {
		process.stderr.write(
			`omp parent-wake: failed to connect to the daemon broker: ${error instanceof Error ? error.message : String(error)}\n`,
		);
		process.exitCode = 1;
		return;
	}
	try {
		const event = await toInternalEvent(adapterEvent);
		const response = await client.request({ op: "wake-submit", event, timeoutMs });
		if (response.op !== "wake-submit") throw new Error(`Unexpected broker response op ${response.op}`);
		const output = toAdapterParentWakeResult(response.result, adapterEvent.created_at);
		process.stdout.write(`${JSON.stringify(output)}\n`);
		process.exitCode = output.ack_status === "acked" ? 0 : 1;
	} catch (error) {
		process.stderr.write(
			`omp parent-wake: submission failed: ${error instanceof Error ? error.message : String(error)}\n`,
		);
		process.exitCode = 1;
	} finally {
		client.close();
	}
}
