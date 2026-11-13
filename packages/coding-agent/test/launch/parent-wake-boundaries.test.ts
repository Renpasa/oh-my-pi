import { test, expect } from 'bun:test';
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import { fixture } from '../helpers/parent-wake-fixture';
import { Agent } from '@oh-my-pi/pi-agent-core';
import { createMockModel } from '@oh-my-pi/pi-ai/providers/mock';
import { getBundledModel } from '@oh-my-pi/pi-catalog/models';
import { AgentSession } from '../../src/session/agent-session';
import { SessionManager } from '../../src/session/session-manager';
import { AuthStorage } from '../../src/session/auth-storage';
import { ModelRegistry } from '../../src/config/model-registry';
import { Settings } from '../../src/config/settings';
import { convertToLlm } from '../../src/session/messages';
import { parseArgs } from '../../src/cli/args';
import { buildSessionOptions } from '../../src/main';
import { loadSessionExtensions } from '../../src/sdk';
import { EventBus } from '../../src/utils/event-bus';

// Every socket, broker record and session file lives below the fresh TMPDIR.
test('unowned reconnect retries cannot ACK/unregister; same-scope takeover succeeds only after old owner disconnects', async () => {
  const f=await fixture();
  try {
    const owner=await f.client(), reconnect=await f.client();
    await owner.request({op:'wake-register',target:f.target});
    await f.caller.request({op:'wake-submit',event:f.event('old-delivery'),timeoutMs:0});
    const refused=Promise.withResolvers<void>(), acquired=Promise.withResolvers<void>();
    const request=reconnect.request.bind(reconnect); let refusals=0;
    // Pass-through observation of real RPCs, preserving their results and errors.
    reconnect.request=async (op,signal) => {
      try {const result=await request(op,signal);if(op.op==='wake-register')acquired.resolve();return result;}
      catch(error){if(op.op==='wake-register'){refusals++;refused.resolve();}throw error;}
    };
    const received:string[]=[];
    reconnect.onParentWake(f.target,e=>{received.push(e.eventId);});
    await refused.promise;
    await expect(reconnect.request({op:'wake-ack',target:f.target,eventId:'old-delivery'})).rejects.toThrow('does not own');
    await expect(reconnect.request({op:'wake-unregister',target:f.target})).rejects.toThrow('does not own');
    owner.close(); await acquired.promise;
    const outcome=await f.caller.request({op:'wake-submit',event:f.event('after-reconnect'),timeoutMs:2000});
    console.log('OWNER_RETRY_BOUNDARY',JSON.stringify({refusals,received,outcome}));
    expect(outcome.op==='wake-submit'&&outcome.result.status).toBe('acked');
    expect(received).toEqual(['after-reconnect']);
    await expect(f.caller.request({op:'wake-unregister',target:f.target})).rejects.toThrow('does not own');
  }finally{await f.cleanup();}
},10000);

test('retention disk fallback preserves a nonresident replay inside 14 days and removes expired terminal records',async()=>{
  const f=await fixture(false);
  try{
    for(let i=0;i<300;i++)await f.seed(`retained-${i}`,'acked',3600000+i);
    await f.seed('expired','acked',15*24*3600000);
    await f.start();
    const replay=await f.caller.request({op:'wake-submit',event:f.event('retained-299'),timeoutMs:0});
    const expiredExists=await fs.exists(f.eventFile('expired'));
    console.log('RETENTION_REPLAY',JSON.stringify({replay,expiredExists}));
    expect(replay.op==='wake-submit'&&replay.result.status).toBe('acked');expect(expiredExists).toBe(false);
  }finally{await f.cleanup();}
},10000);

test('disk-loaded terminal records do not exceed the advertised 256 resident bound',async()=>{
  const f=await fixture(false);
  try{
    for(let i=0;i<600;i++)await f.seed(`resident-${i}`,'acked',3600000+i);
    await f.start();
    for(let i=300;i<600;i++)await f.caller.request({op:'wake-submit',event:f.event(`resident-${i}`),timeoutMs:0});
    // Remove only this fixture's durable records. An ACK returned afterward proves the identity was resident.
    await fs.rm(path.dirname(f.eventFile('x')),{recursive:true,force:true});
    let residentAcked=0;
    for(let i=300;i<600;i++){
      const replay=await f.caller.request({op:'wake-submit',event:f.event(`resident-${i}`),timeoutMs:0});
      if(replay.op==='wake-submit'&&replay.result.status==='acked')residentAcked++;
    }
    console.log('RESIDENT_TERMINAL_BOUND',JSON.stringify({probed:300,residentAckedAfterDiskRemoval:residentAcked,advertisedCap:256}));
    expect(residentAcked).toBeLessThanOrEqual(256);
  }finally{await f.cleanup();}
},15000);

test('online stale-registration residency is bounded, not only sliced during startup',async()=>{
  const f=await fixture();
  try{
    for(let i=0;i<300;i++){
      const target={...f.target,sessionId:`stale-${i}`};
      await f.caller.request({op:'wake-register',target});await f.caller.request({op:'wake-unregister',target});
    }
    await fs.rm(path.dirname(f.registrationFile('x')),{recursive:true,force:true});
    let residentStale=0;
    for(let i=0;i<300;i++){
      const replay=await f.caller.request({op:'wake-submit',event:f.event(`stale-probe-${i}`,{...f.target,sessionId:`stale-${i}`}),timeoutMs:0});
      if(replay.op==='wake-submit'&&replay.result.failureCode==='target-stale')residentStale++;
    }
    console.log('RESIDENT_TOMBSTONE_BOUND',JSON.stringify({probed:300,residentStaleAfterDiskRemoval:residentStale,advertisedCap:256}));
    expect(residentStale).toBeLessThanOrEqual(256);
  }finally{await f.cleanup();}
},15000);

test('eviction must not invalidate a genuine late ACK on the same live owner socket',async()=>{
  const f=await fixture();const realNow=Date.now;
  try{
    const owner=await f.client();await owner.request({op:'wake-register',target:f.target});
    await f.caller.request({op:'wake-submit',event:f.event('late-retention'),timeoutMs:0});
    // Advance only the broker's timestamp calculations. No wall-clock wait and no socket timer interception.
    const advanced=realNow()+61000;Date.now=()=>advanced;
    for(let i=0;i<256;i++)await f.caller.request({op:'wake-submit',event:f.event(`eviction-${i}`,{...f.target,sessionId:'missing'}),timeoutMs:0});
    let ack:unknown,error:string|undefined;
    try{ack=await owner.request({op:'wake-ack',eventId:'late-retention',target:f.target});}catch(e){error=String(e);}
    console.log('LATE_ACK_EVICTION',JSON.stringify({ack,error,sameOwnerSocket:true,elapsedLogicalMs:61000}));
    expect(error).toBeUndefined();
  }finally{Date.now=realNow;await f.cleanup();}
},15000);

test('failed ACK publication must not leak acked to concurrent replay',async()=>{
  const f=await fixture();
  try{
    const owner=await f.client();await owner.request({op:'wake-register',target:f.target});
    await f.caller.request({op:'wake-submit',event:f.event('ack-race'),timeoutMs:0});
    await fs.unlink(f.eventFile('ack-race'));await fs.mkdir(f.eventFile('ack-race'));
    const acknowledgement=owner.request({op:'wake-ack',eventId:'ack-race',target:f.target});
    const replay=owner.request({op:'wake-submit',event:f.event('ack-race'),timeoutMs:0});
    const [ackResult,replayResult]=await Promise.allSettled([acknowledgement,replay]);
    console.log('CONCURRENT_FAILED_ACK',JSON.stringify({ackResult,replayResult}));
    expect(ackResult.status).toBe('rejected');
    expect(replayResult.status==='fulfilled'&&replayResult.value.op==='wake-submit'&&replayResult.value.result.status).not.toBe('acked');
  }finally{await f.cleanup();}
},10000);

async function sessionParts(manager:SessionManager,auth:AuthStorage,holdFirst=false){
  const entered=Promise.withResolvers<void>(),release=Promise.withResolvers<void>();let calls=0;
  const mock=createMockModel({handler:async()=>{if(++calls===1&&holdFirst){entered.resolve();await release.promise;}return{content:['Done']};}});
  const agent=new Agent({getApiKey:()=> 'test-key',initialState:{model:getBundledModel('anthropic','claude-sonnet-4-5')!,systemPrompt:['Test'],tools:[]},convertToLlm,streamFn:mock.stream});
  const session=new AgentSession({agent,sessionManager:manager,settings:Settings.isolated(),modelRegistry:new ModelRegistry(auth)});
  return{session,agent,mock,entered,release};
}

test('persisted wake dedupe survives a new AgentSession and reopened real transcript',async()=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'session95-'));
  const auth=await AuthStorage.create(':memory:');auth.keys.setRuntime('anthropic','test-key');
  const manager=SessionManager.create(root,path.join(root,'sessions'));
  let active:AgentSession|undefined;
  try{
    const first=await sessionParts(manager,auth);active=first.session;
    const event={target:{projectDir:root,sessionId:manager.getSessionId(),runtimeScope:'candidate'},eventId:'durable-session-event',kind:'probe',pointers:[],createdAt:1};
    await active.queueParentWake(event);await active.waitForIdle();await manager.flush();
    const file=manager.getSessionFile()!;await active.dispose();active=undefined;await manager.close();
    const reopened=await SessionManager.open(file);const second=await sessionParts(reopened,auth);active=second.session;
    await active.queueParentWake(event);await active.waitForIdle();
    console.log('REOPENED_SESSION_DEDUPE',JSON.stringify({firstCalls:first.mock.calls.length,secondCalls:second.mock.calls.length,entryCount:reopened.getEntries().length}));
    expect(first.mock.calls.length).toBe(1);expect(second.mock.calls.length).toBe(0);
  }finally{await active?.dispose();auth.close();await fs.rm(root,{recursive:true,force:true});}
},10000);

test('513 in-flight wake receipts do not evict dedupe for the oldest still-queued event',async()=>{
  const auth=await AuthStorage.create(':memory:');auth.keys.setRuntime('anthropic','test-key');
  const manager=SessionManager.inMemory();const s=await sessionParts(manager,auth,true);let running:Promise<unknown>|undefined;
  try{
    running=s.session.prompt('hold');await s.entered.promise;
    const make=(i:number)=>({target:{projectDir:'/probe',sessionId:manager.getSessionId(),runtimeScope:'candidate'},eventId:`burst-${i}`,kind:'probe',pointers:[],createdAt:1});
    const receipts=Array.from({length:513},(_,i)=>s.session.queueParentWake(make(i)));for(const p of receipts)p.catch(()=>{});
    const replay=s.session.queueParentWake(make(0));replay.catch(()=>{});
    const sameReceipt=replay===receipts[0];s.release.resolve();const settled=await Promise.allSettled([...receipts,replay]);await running;await s.session.waitForIdle();
    // Backpressure, not eviction: the wake beyond the unresolved bound is refused (never ACKed); the rest inject once.
    expect(settled.filter(r=>r.status==='rejected')).toHaveLength(1);expect(settled[512]?.status).toBe('rejected');expect(sameReceipt).toBe(true);
    let firstEventOccurrences=0;
    for(const message of s.agent.state.messages){if(message.role==='custom'&&message.customType==='parent-wake'){
      const details=message.details as {events?:{eventId:string}[]}|undefined;firstEventOccurrences+=details?.events?.filter(e=>e.eventId==='burst-0').length??0;
    }}
    console.log('INFLIGHT_RECEIPT_BOUNDARY',JSON.stringify({queuedUnique:513,sameReceipt,firstEventOccurrences}));
    expect(firstEventOccurrences).toBe(1);
  }finally{s.release.resolve();await running?.catch(()=>{});await s.session.dispose();auth.close();}
},15000);

test('strict extension bootstrap excludes ambient, explicit, hooks and trusted paths',async()=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'strict95-'));const auth=await AuthStorage.create(':memory:');
  try{
    const marker=path.join(root,'evaluated');const extension=path.join(root,'extra.ts');
    const source=`import {appendFileSync} from 'node:fs';export default function(){appendFileSync(${JSON.stringify(marker)},'loaded');}`;
    await fs.writeFile(extension,source);await fs.mkdir(path.join(root,'.omp/extensions'),{recursive:true});await fs.writeFile(path.join(root,'.omp/extensions/ambient.ts'),source);
    const settings=Settings.isolated();const registry=new ModelRegistry(auth,path.join(root,'models.yml'));
    for(const extra of [[],['--extension',extension],['--hook',extension],['--trusted-extension',extension]]){
      const args=parseArgs(['--cwd',root,'--tools','read','--strict-tools',...extra]);
      const options=await buildSessionOptions(args,[],SessionManager.inMemory(root),registry,settings);
      const loaded=await loadSessionExtensions(options,root,settings,new EventBus());
      expect(options.additionalExtensionPaths).toEqual([]);expect(loaded.extensions).toEqual([]);expect(loaded.errors).toEqual([]);
    }
    console.log('STRICT_ALL_EXTENSION_PATHS',JSON.stringify({factoryEvaluated:await fs.exists(marker)}));expect(await fs.exists(marker)).toBe(false);
  }finally{auth.close();await fs.rm(root,{recursive:true,force:true});}
},10000);
