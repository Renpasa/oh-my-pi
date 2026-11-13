import { prompt } from "@oh-my-pi/pi-utils";
import { parseParentWakeEvent, type ParentWakeEvent } from "../launch/protocol";
import parentWakeTemplate from "../prompts/session/parent-wake.md" with { type: "text" };
import type { CustomMessage } from "./messages";

/** Yield-queue kind for broker-delivered Parent Wake events. */
export const PARENT_WAKE_MESSAGE_TYPE = "parent-wake";

/** One durable wake event awaiting injection into its target session. */
export type ParentWakeEntry = ParentWakeEvent;

/** Whether a wake event's target session identity matches this session. */
export function isParentWakeTargetSession(target: ParentWakeEvent["target"], sessionId: string): boolean {
	return target.sessionId === sessionId;
}

/**
 * Build one hidden, model-visible message per queued wake event. The message
 * carries only the bounded canonical pointers — the target session fetches
 * canonical state itself; no inline content is ever transported.
 */
export function buildParentWakeBatchMessage(entries: ParentWakeEntry[]): CustomMessage<{ events: ParentWakeEvent[] }> {
	const validatedEntries = entries.map(entry => parseParentWakeEvent(entry));
	return {
		role: "custom",
		customType: PARENT_WAKE_MESSAGE_TYPE,
		content: validatedEntries
			.map(entry =>
				prompt.render(parentWakeTemplate, {
					kind: entry.kind,
					eventId: entry.eventId,
					pointerCount: entry.pointers.length,
					pointers: entry.pointers,
				}),
			)
			.join("\n"),
		display: true,
		attribution: "agent",
		details: { events: validatedEntries },
		timestamp: Date.now(),
	};
}
