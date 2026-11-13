import { test, expect } from 'bun:test';
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import { Agent } from '@oh-my-pi/pi-agent-core';
import { createMockModel } from '@oh-my-pi/pi-ai/providers/mock';
import { getBundledModel } from '@oh-my-pi/pi-catalog/models';
import { startDaemonBrokerFromEnvironment } from '../../src/launch/broker';
import { createDaemonBrokerClient, type DaemonBrokerClient } from '../../src/launch/client';
import { AgentSession } from '../../src/session/agent-session';
import { SessionManager } from '../../src/session/session-manager';
import { AuthStorage } from '../../src/session/auth-storage';
import { ModelRegistry } from '../../src/config/model-registry';
import { Settings } from '../../src/config/settings';
import { convertToLlm } from '../../src/session/messages';

test('queue-committed but unacked wake must not commit twice after broker/client restart', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'a-'));
  const projectDir = path.join(root, 'p'), runtimeDir = path.join(root, 'r');
  await fs.mkdir(projectDir);
  const auth = await AuthStorage.create(':memory:'); auth.keys.setRuntime('anthropic', 'test-key');
  const model = getBundledModel('anthropic', 'claude-sonnet-4-5')!;
  const mock = createMockModel({ handler: () => ({ content: ['Done'] }) });
  const agent = new Agent({ getApiKey: () => 'test-key', initialState: { model, systemPrompt: ['Test'], tools: [] }, convertToLlm, streamFn: mock.stream });
  const manager = SessionManager.inMemory();
  const session = new AgentSession({ agent, sessionManager: manager, settings: Settings.isolated(), modelRegistry: new ModelRegistry(auth) });
  const clients: DaemonBrokerClient[] = [];
  async function client() { const c = await createDaemonBrokerClient(projectDir, { runtimeDir }); clients.push(c); return c; }
  let caller = await client(), finished: Promise<void> | undefined;
  async function start() {
    const keys = ['OMP_DAEMON_PROJECT_DIR', 'OMP_DAEMON_RUNTIME_DIR', 'OMP_DAEMON_IDLE_GRACE_MS'];
    const old = keys.map(k => process.env[k]);
    process.env[keys[0]!] = projectDir; process.env[keys[1]!] = runtimeDir; process.env[keys[2]!] = '1000';
    const listening = Promise.withResolvers<void>();
    finished = startDaemonBrokerFromEnvironment({ onListening: listening.resolve });
    keys.forEach((k, i) => old[i] === undefined ? delete process.env[k] : process.env[k] = old[i]);
    await Promise.race([listening.promise, finished.then(() => { throw new Error('broker failed to claim'); })]);
  }
  async function stop() { await caller.request({op:'shutdown'}); for (const c of clients.splice(0)) c.close(); await finished; finished = undefined; }
  const target = { projectDir, sessionId: manager.getSessionId(), runtimeScope: 'candidate' };
  const event = { target, eventId: 'accepted-unacked', kind: 'probe', pointers: [], createdAt: 1 };
  const eventFile = path.join(runtimeDir, 'parent-wake/events/accepted-unacked.json');
  const firstCommitted = Promise.withResolvers<void>(), secondCommitted = Promise.withResolvers<void>(), withholdAck = Promise.withResolvers<void>();
  try {
    // Seed the exact persisted crash-window state produced before socket delivery. No live submit poller.
    await fs.mkdir(path.dirname(eventFile), { recursive: true });
    await fs.writeFile(eventFile, JSON.stringify({ event, status: 'dispatched', dispatchedAt: 1 }));
    await start();
    const firstOwner = await client();
    firstOwner.onParentWake(target, async e => { await session.queueParentWake(e); firstCommitted.resolve(); await withholdAck.promise; });
    await firstOwner.request({op:'wake-register', target});
    await firstCommitted.promise; await session.waitForIdle();
    const before = agent.state.messages.filter(m => m.role === 'custom' && m.customType === 'parent-wake').length;
    expect(before).toBe(1);
    expect(JSON.parse(await fs.readFile(eventFile, 'utf8')).status).toBe('dispatched');
    firstOwner.close(); await stop();
    caller = await client(); await start();
    const secondOwner = await client();
    secondOwner.onParentWake(target, async e => { await session.queueParentWake(e); secondCommitted.resolve(); });
    await secondOwner.request({op:'wake-register', target});
    await secondCommitted.promise; await session.waitForIdle();
    const after = agent.state.messages.filter(m => m.role === 'custom' && m.customType === 'parent-wake').length;
    console.log('ACCEPTED_UNACKED_REPLAY', JSON.stringify({ eventId: event.eventId, before, after, sameSessionId: manager.getSessionId(), providerCalls: mock.calls.length }));
    expect(after).toBe(1);
  } finally {
    withholdAck.resolve();
    if (finished) await stop();
    for (const c of clients) c.close();
    await session.dispose(); auth.close(); await fs.rm(root, {recursive:true, force:true});
  }
}, 15000);
