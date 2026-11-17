// Additive SYNTHETIC fixture operation only. Never a substitute for Whagons tasks.remove.
// Retain the existing board row with a fixed archive marker through the normal db API.
// Actual reducer transaction/capture/feed/visibility remain production implementations.
import {reducer,schema} from '@gonvex/module-sdk';
export const archive=reducer({
  args:schema.object({id:schema.string()}),result:schema.any(),
  offline:{mode:'onlineOnly',reason:'synthetic transaction-evidence fixture'},
  run:async({db},args)=>db.update('board',args.id,{title:'[synthetic-archived]'}),
});
