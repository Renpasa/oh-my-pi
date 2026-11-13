/**
 * Parent Wake session sink: `queueParentWake` is the acceptance point the
 * broker ACK depends on. Its promise must settle only once the target session
 * has actually injected the wake, and must reject — never resolve — for a wake
 * that is invalid, addressed to another session, or arrives after disposal.
 */
import { afterEach, describe, expect, it } from "bun:test";
import { Agent } from "@oh-my-pi/pi-agent-core";
import { createMockModel } from "@oh-my-pi/pi-ai/providers/mock";
import { getBundledModel } from "@oh-my-pi/pi-catalog/models";
import { ModelRegistry } from "@oh-my-pi/pi-coding-agent/config/model-registry";
import { Settings } from "@oh-my-pi/pi-coding-agent/config/settings";
import {
	PARENT_WAKE_MAX_POINTERS,
	type ParentWakeEvent,
} from "@oh-my-pi/pi-coding-agent/launch/protocol";
import { AgentSession } from "@oh-my-pi/pi-coding-agent/session/agent-session";
import { AuthStorage } from "@oh-my-pi/pi-coding-agent/session/auth-storage";
import { convertToLlm } from "@oh-my-pi/pi-coding-agent/session/messages";
import { SessionManager } from "@oh-my-pi/pi-coding-agent/session/session-manager";

function textOf(message: { content: unknown }): string {
	if (typeof message.content === "string") return message.content;
	if (!Array.isArray(message.content)) return "";
	return message.content
		.map(part => (typeof part === "object" && part !== null && "text" in part ? String(part.text) : ""))
		.join("");
}

describe("AgentSession parent wake sink", () => {
	let session: AgentSession | undefined;
	const authStorages: AuthStorage[] = [];

	afterEach(async () => {
		await session?.dispose();
		session = undefined;
		for (const authStorage of authStorages.splice(0)) authStorage.close();
	});

	async function createSession() {
		const model = getBundledModel("anthropic", "claude-sonnet-4-5")!;
		const mock = createMockModel({ handler: () => ({ content: ["Done"] }) });
		const agent = new Agent({
			getApiKey: () => "test-key",
			initialState: { model, systemPrompt: ["Test"], tools: [] },
			convertToLlm,
			streamFn: mock.stream,
		});
		const authStorage = await AuthStorage.create(":memory:");
		authStorages.push(authStorage);
		authStorage.keys.setRuntime("anthropic", "test-key");
		const sessionManager = SessionManager.inMemory();
		session = new AgentSession({
			agent,
			sessionManager,
			settings: Settings.isolated(),
			modelRegistry: new ModelRegistry(authStorage),
		});
		return { active: session, mock, sessionId: sessionManager.getSessionId() };
	}

	function wake(sessionId: string, overrides: Partial<ParentWakeEvent> = {}): ParentWakeEvent {
		return {
			eventId: "evt-session-1",
			kind: "candidate-ready",
			pointers: [{ kind: "artifact", ref: "local://result.md" }],
			createdAt: 1,
			target: { projectDir: "/project", sessionId, runtimeScope: "candidate-a" },
			...overrides,
		};
	}

	it("settles after injecting the wake and runs one turn that carries only the pointers", async () => {
		const { active, mock, sessionId } = await createSession();

		await active.queueParentWake(wake(sessionId));
		await active.waitForIdle();

		const wakeTurns = mock.calls.filter(call =>
			call.context.messages.some(message => textOf(message).includes("evt-session-1")),
		);
		expect(wakeTurns).toHaveLength(1);
		const rendered = wakeTurns[0]!.context.messages.map(textOf).join("\n");
		expect(rendered).toContain("candidate-ready");
		expect(rendered).toContain("artifact=local://result.md");
	});

	it("rejects a wake addressed to another session without running a turn", async () => {
		const { active, mock } = await createSession();

		await expect(active.queueParentWake(wake("some-other-session"))).rejects.toThrow("does not match this session");
		await active.waitForIdle();

		expect(mock.calls).toHaveLength(0);
	});

	it("rejects an oversized pointer set", async () => {
		const { active, mock, sessionId } = await createSession();
		const pointers = Array.from({ length: PARENT_WAKE_MAX_POINTERS + 1 }, (_, index) => ({
			kind: "artifact",
			ref: `local://result-${index}.md`,
		}));

		await expect(active.queueParentWake(wake(sessionId, { pointers }))).rejects.toThrow("at most");
		await active.waitForIdle();

		expect(mock.calls).toHaveLength(0);
	});

	it("rejects a wake delivered after the session was disposed", async () => {
		const { active, sessionId } = await createSession();
		await active.dispose();

		await expect(active.queueParentWake(wake(sessionId))).rejects.toThrow("disposed");
	});
});
