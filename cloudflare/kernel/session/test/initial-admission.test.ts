import {describe,expect,it} from 'vitest';
import {InitialAdmission,initialPressure,MAX_PENDING_INITIALS} from '../initial-admission';
describe('0065 unsettled initial admission (provisional envelope)',()=>{
 it('keeps4095/4096 and refuses4097 without evicting active generations',()=>{
  const admission=new InitialAdmission();
  try{for(let n=1;n<=4095;n++)expect(admission.reserve(n,128)).toBe(true);
   expect(admission.reserve(4096,128)).toBe(true);
   expect(admission.reserve(4097,128)).toBe(false);
   expect(()=>admission.reserve(4096,128)).toThrow('generation reused');
   admission.release(1);expect(admission.reserve(4097,128)).toBe(true);
   expect(MAX_PENDING_INITIALS).toBe(4096);
  }finally{admission.clear();}
 });
 it('shares estimated bytes across socket owners and retains a sole legal large request',()=>{
  const a=new InitialAdmission(),b=new InitialAdmission(),before=initialPressure();
  try{
   expect(a.reserve(1,40*1024*1024)).toBe(true);
   expect(b.reserve(1,1024)).toBe(false);
   a.clear();expect(b.reserve(1,1024)).toBe(true);
  }finally{a.clear();b.clear();}
  expect(initialPressure()).toEqual(before);
 });
});
