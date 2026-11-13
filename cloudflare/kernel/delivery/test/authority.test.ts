import {expect,it} from 'vitest';
import {AuthorityAdmission,authorityPressure,TENANT_AUTHORITY_COUNT} from '../authority';
it('bounds historical UNKNOWN authority without eviction, and keeps existing owners working',()=>{
 const admission=new AuthorityAdmission();
 const before=authorityPressure();
 for(let n=0;n<TENANT_AUTHORITY_COUNT;n++)admission.set(`physical-${n}`,600);
 expect(()=>admission.set('new-physical',600)).toThrow('tenant subscription authority admission exhausted');
 expect(()=>admission.set('physical-0',600)).not.toThrow();
 // A reset/reduction never frees an uncertain old owner or its peak copies.
 admission.set('physical-0',512);
 expect(authorityPressure().count-before.count).toBe(TENANT_AUTHORITY_COUNT);
 expect(authorityPressure().bytes-before.bytes).toBe(TENANT_AUTHORITY_COUNT*600);
});
