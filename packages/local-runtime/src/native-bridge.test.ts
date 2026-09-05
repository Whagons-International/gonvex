import { describe, expect, it, vi } from 'vitest';
import { nativeReducerHosts, nativeMessageScript, installNativeRuntimeGlobals } from './native-bridge.js';
describe('native execution host lifecycle', () => {
  it('serializes requests only after readiness and releases the host on close', async () => {
    const hosts = nativeReducerHosts(); const executor = hosts.create(); const bridge = hosts.snapshot()[0]!;
    const messages: any[] = []; bridge.attach(message => messages.push(message));
    const result = executor.replay({scope:'scope',tables:{}}, []);
    await Promise.resolve(); expect(messages).toEqual([]);
    bridge.receive(JSON.stringify({id:0})); await new Promise(resolve => setTimeout(resolve,0));
    expect(messages[0].method).toBe('replay');
    bridge.receive(JSON.stringify({id:messages[0].id,result:{transactions:[],rejected:[{commandId:'c',error:{name:'ValidationError',message:'rejected'}}]}}));
    expect((await result).rejected[0]!.error).toBeInstanceOf(Error);
    executor.close(); expect(hosts.snapshot()).toEqual([]);
    await expect(executor.ready).resolves.toBeUndefined();
  });
  it('rejects readiness on a native renderer failure without dropping queued application data', async () => {
    const hosts = nativeReducerHosts(); const executor = hosts.create();
    hosts.snapshot()[0]!.fail('Renderer terminated');
    await expect(executor.ready).rejects.toThrow('Renderer terminated');
    await expect(executor.replay({scope:'scope',tables:{}},[])).rejects.toThrow('Renderer terminated');
    executor.close();
  });
  it('keeps hostile strings as message data instead of executable source', () => {
    const value = {id:1,args:['</script>\";throw Error(1);//\u2028']};
    let observed: unknown;
    const window = {dispatchEvent(event: MessageEvent){observed=event.data;}};
    new Function('window','MessageEvent',nativeMessageScript(value))(window,MessageEvent);
    expect(observed).toEqual(value);
  });
});

it('installs native platform primitives while preserving structured replica values', () => {
  vi.stubGlobal('structuredClone',undefined);
  vi.stubGlobal('crypto',undefined);
  try {
    installNativeRuntimeGlobals({getRandomValues:array=>{array.fill(3);return array;},randomUUID:()=> 'test-id'});
    const value: any = {missing:undefined,rows:new Map([['id',{count:1}]])}; value.self=value;
    const restored=structuredClone(value);
    expect(restored).not.toBe(value);expect(restored.self).toBe(restored);expect(restored.rows.get('id')).toEqual({count:1});expect('missing' in restored).toBe(true);
    expect(crypto.getRandomValues(new Uint8Array(2))).toEqual(new Uint8Array([3,3]));
  } finally {vi.unstubAllGlobals();}
});
