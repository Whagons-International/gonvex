/** Fault injection only: the real SDK still owns every protocol message and
 * reconnect. Track sockets constructed by its public connect() entry point. */
export function installTransportFaults() {
  const NativeWebSocket=globalThis.WebSocket;
  if(typeof NativeWebSocket!=='function')throw Error('Native WebSocket is required');
  const sockets=new WeakMap(),attached=new WeakSet();let owner;
  class TrackedWebSocket extends NativeWebSocket {
    constructor(...args){super(...args);if(owner)sockets.set(owner,this);}
  }
  globalThis.WebSocket=TrackedWebSocket;
  return {
    attach(client){
      if(attached.has(client))return;
      attached.add(client);const connect=client.connect.bind(client);
      client.connect=(...args)=>{const previous=owner;owner=client;try{return connect(...args);}finally{owner=previous;}};
    },
    drop(client,timeoutMs=60000){
      const socket=sockets.get(client);
      if(!socket||socket.readyState!==NativeWebSocket.OPEN)return Promise.reject(Error('SDK transport was not connected before the fault'));
      return new Promise((resolve,reject)=>{
        const closed=()=>{clearTimeout(timer);resolve();};
        const timer=setTimeout(()=>{socket.removeEventListener('close',closed);reject(Error('SDK transport did not close within the diagnostic timeout'));},timeoutMs);
        socket.addEventListener('close',closed,{once:true});
        try{socket.close(4001,'synthetic network reconnect');}
        catch(error){clearTimeout(timer);socket.removeEventListener('close',closed);reject(error);}
      });
    },
    restore(){if(globalThis.WebSocket===TrackedWebSocket)globalThis.WebSocket=NativeWebSocket;},
  };
}
