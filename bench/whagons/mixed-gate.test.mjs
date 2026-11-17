import test from 'node:test';
import assert from 'node:assert/strict';
import { mixedPlan, qualifyMixed } from './mixed-gate.mjs';

test('mixed cohort IDs are unique and deterministic with exact offered windows', () => {
 const p=mixedPlan();assert.deepEqual(p,mixedPlan());
 assert.equal(p.length,1000);assert.equal(new Set(p.map(x=>x.user)).size,1000);
 assert.deepEqual(p.map(x=>x.user),Array.from({length:1000},(_,i)=>i));
 for(const [cohort,count,start,end] of [['spread',700,0,900000],['burst-75-a',75,180000,185000],['burst-75-b',75,450000,455000],['burst-150',150,720000,725000]]){
  const xs=p.filter(x=>x.cohort===cohort);assert.equal(xs.length,count);assert.ok(xs.every(x=>x.at>=start&&x.at<end));
 }
 // Every ID keeps the original production tenant skew assignment.
 const weights=[39,39,11,6,2,2,1],counts=weights.map(()=>0);
 for(const x of p){let r=((x.user*.61803398875)%1)*100;counts[weights.findIndex(w=>(r-=w)<0)]++;}
 assert.ok(counts.every((n,i)=>Math.abs(n-weights[i]*10)<=2));
 assert.throws(()=>mixedPlan(999));
});

const good=()=>{
 const started=1000000;
 const signIns=mixedPlan().map(x=>({user:x.user,cohort:x.cohort,plannedAt:started+x.at,attemptedAt:started+x.at,authAt:started+x.at+50,readyAt:started+x.at+100}));
 return {started,signIns,samples:{login:signIns.map(s=>({at:s.plannedAt,ms:50})),reducer:[{at:started+180100,ms:50}]},errors:[]};
};
test('each burst qualifies independently and late completions stay in their start window', () => {
 const x=good();assert.ok(qualifyMixed(x).every(w=>w.passed));
 x.samples.save=[{at:x.started+184999,ms:2000}];
 let q=qualifyMixed(x);
 assert.equal(q[0].passed,false);assert.ok(q[0].failingKinds.includes('save'));
 assert.equal(q[1].passed,true);assert.equal(q[2].passed,true);
 assert.equal(q[3].passed,false);
 x.samples.save=[];
 x.signIns.find(s=>s.user===774).readyAt=x.started+190000;
 q=qualifyMixed(x);assert.equal(q[0].passed,false);assert.ok(q[0].latencyMs.readiness.p99>1000);
});
test('missing readiness, failed operations and unfinished operations fail their own burst', () => {
 for(const fault of ['missing','error','pending']){
  const x=good();
  if(fault==='missing')delete x.signIns.find(s=>s.user===850).readyAt;
  if(fault==='error')x.errors=[{at:x.started+721000,key:'refused'},{at:x.started+721100,key:'refused'}];
  if(fault==='pending')x.attempts=[{at:x.started+721000,kind:'save',done:false}];
  const q=qualifyMixed(x);assert.equal(q[2].passed,false,fault);assert.equal(q[0].passed,true);
 }
});
test('retry recovery after a burst is reported against its originating refusal', () => {
 const x=good();x.retries=[{type:'refused',at:x.started+184000},{type:'retry',at:x.started+184250},{type:'recovered',failedAt:x.started+184000,at:x.started+186000}];
 const q=qualifyMixed(x)[0];assert.equal(q.retryCounts.refused,1);assert.equal(q.retryCounts.retry,1);assert.equal(q.retryCounts.recovered,0);assert.equal(q.recoveredAfterWindow,1);
});
