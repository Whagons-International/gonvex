// Compile-only consumer of genuine generated refs/binding. Never executed.
import {GonvexClient} from './explicit-server-fixture/gonvex/_generated/client.web.js';
import {api} from './explicit-server-fixture/gonvex/_generated/api.js';
function publicConsumer(client:GonvexClient) {
 const created=client.reducerWithTransactionEvidence(api.board.add,{id:'synthetic',title:'synthetic'},{execution:'server',offline:'reject',optimistic:[],timeoutMs:1000});
 void created.then(value=>{const source:'server-durable'=value.confirmation.source;const basis:'matching-replica-transaction'=value.confirmation.basis;const epoch:string=value.confirmation.cursor.epoch;const revision:number=value.confirmation.cursor.revision;void [source,basis,epoch,revision];});
 // The ordinary public reducer remains available with its existing result contract.
 void client.reducer(api.board.rename,{id:'synthetic',title:'renamed'});
 // @ts-expect-error only the additive explicit server value exists
 void client.reducerWithTransactionEvidence(api.board.add,{id:'synthetic',title:'synthetic'},{execution:'local',offline:'reject',optimistic:[],timeoutMs:1000});
 // @ts-expect-error caller artifact authority is not an option
 void client.reducerWithTransactionEvidence(api.board.add,{id:'synthetic',title:'synthetic'},{execution:'server',artifactHash:'caller',offline:'reject',optimistic:[],timeoutMs:1000});
}
void publicConsumer;
