// A two-function Query/Reducer bundle with K04's prelude, as the release builder emits one.
import { Date } from "gonvex:module-globals";
const ping = () => "pong";
const stamp = (ctx) => ({ at: new Date(ctx.now).toISOString() });
export { ping as "notes.ping", stamp as "notes.stamp" };
