// Synthetic fixture module; executed by the actual K04/K12 hosts and K22
// tenant adapter, never substituted for a deployed application proof.
export async function publicTool(ctx, args) {
  try { await ctx.tools.write(args); return { denied: false }; }
  catch (error) { return { denied: true, error: String(error) }; }
}
export async function capabilityWrite(ctx, args) {
  await ctx.db.query('INSERT INTO board (id,title,position,bumps) VALUES ($1,$1,0,0)', [args.id]);
  const attempt = () => args.kind === 'outbox' ? ctx.actions.enqueue('probe.noop', {}) : ctx.scheduler.runAfter(3600000, 'probe.noop', {});
  if (args.catchMissing) {
    // Surface omission throws JS TypeError, not a dispatched host denial.
    // Existing JS catch semantics must not be replaced with a fake host latch.
    try { await attempt(); } catch { /* intentionally caught */ }
    return { wrote: true, actionsAbsent: !ctx.actions, schedulerAbsent: !ctx.scheduler };
  }
  await attempt();
  return { wrote: true };
}
export async function write(ctx, args) {
  await ctx.db.query('INSERT INTO board (id,title,position,bumps) VALUES ($1,$1,0,0)', [args.id]);
  return { id: args.id };
}
export async function service(ctx, args) {
  if (args.fail) throw new Error('service Action failed');
  const i = ctx.invocation;
  return { value: 42, channel: i.channel, rootChannel: i.rootChannel, command: i.commandId, root: i.rootCommandId, account: ctx.auth.account?.id };
}
export async function noop() { return null; }

export async function resultSize(_ctx,args){return "x".repeat(args.bytes);}

export async function invite(ctx,args){
  await ctx.db.query('INSERT INTO board (id,title,position,bumps) VALUES ($1,$1,0,0)',['invite-'+args.invitationId]);
  if(args.payload?.fail)throw new Error('invitation hook refused');
  return {memberId:args.memberId};
}
