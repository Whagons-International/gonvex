import {expect,it,vi} from 'vitest';
import {TransactionEvidenceCall} from './transaction-evidence.js';
it('synchronous ownership invalidation cannot send after an unsent rejection',async()=>{
  let call:TransactionEvidenceCall; const write=vi.fn();
  call=new TransactionEvidenceCall('command','receipt','board.rename',{epoch:'db',revision:3,accountId:'a',memberId:'m'},1000,
    ()=>{call.fail();return true;},()=>3,()=>{},()=>{},()=>{});
  const result=call.promise.catch(error=>error);call.send(write);
  expect(await result).toMatchObject({possibleWriteApplied:false,outcome:'unknown'});expect(write).not.toHaveBeenCalled();
});
