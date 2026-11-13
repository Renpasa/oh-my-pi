import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import { startDaemonBrokerFromEnvironment } from '../../src/launch/broker';
import { createDaemonBrokerClient, type DaemonBrokerClient } from '../../src/launch/client';
import type { ParentWakeTarget, ParentWakeEvent } from '../../src/launch/protocol';

export async function fixture(autoStart = true) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'p95-'));
  const projectDir = path.join(root, 'p'), runtimeDir = path.join(root, 'r');
  await fs.mkdir(projectDir);
  const clients: DaemonBrokerClient[] = [];
  async function client() {
    const c = await createDaemonBrokerClient(projectDir, {runtimeDir, idleGraceMs:1000}); clients.push(c); return c;
  }
  let caller = await client(), finished: Promise<void> | undefined;
  async function start() {
    const keys = ['OMP_DAEMON_PROJECT_DIR', 'OMP_DAEMON_RUNTIME_DIR', 'OMP_DAEMON_IDLE_GRACE_MS'];
    const old = keys.map(k => process.env[k]);
    process.env[keys[0]!] = projectDir; process.env[keys[1]!] = runtimeDir; process.env[keys[2]!] = '1000';
    const listening = Promise.withResolvers<void>();
    finished = startDaemonBrokerFromEnvironment({onListening:listening.resolve});
    keys.forEach((k,i) => old[i] === undefined ? delete process.env[k] : process.env[k] = old[i]);
    await Promise.race([listening.promise, finished.then(() => { throw new Error('broker lease failed'); })]);
  }
  async function stop() {
    if (finished) { await caller.request({op:'shutdown'}); for (const c of clients.splice(0)) c.close(); await finished; finished=undefined; }
  }
  async function restart() { await stop(); caller = await client(); await start(); }
  const target: ParentWakeTarget = {projectDir, sessionId:'parent95', runtimeScope:'candidate95'};
  function event(id: string, addressed = target): ParentWakeEvent { return {target:addressed, eventId:id, kind:'probe', pointers:[], createdAt:1}; }
  const eventFile = (id:string) => path.join(runtimeDir,'parent-wake/events',`${id}.json`);
  const registrationFile = (sid:string) => path.join(runtimeDir,'parent-wake/registrations',`${sid}.json`);
  async function seed(id:string, status='acked', ageMs=0) {
    const file = eventFile(id); await fs.mkdir(path.dirname(file),{recursive:true});
    await fs.writeFile(file, JSON.stringify({event:event(id),status,dispatchedAt:1,...(status==='acked'?{ackedAt:2}:{})}));
    const timestamp = new Date(Date.now()-ageMs); await fs.utimes(file,timestamp,timestamp);
  }
  if(autoStart) await start();
  return {root, projectDir, runtimeDir, target, event, eventFile, registrationFile, seed, client, start, restart,
    get caller(){ return caller; },
    async cleanup(){try{await stop();}finally{for(const c of clients)c.close();await fs.rm(root,{recursive:true,force:true});}}
  };
}
