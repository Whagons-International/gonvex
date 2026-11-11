// Provenance recorded with each revision (feed.rs `Provenance`, which replaced
// the Postgres `gonvex.*` GUCs). The defaulting rules are Rust's:
//   origin_command_id       = commandId, or "" when absent
//   root_command_id         = rootCommandId if non-empty, else origin
//   invocation_channel      = channel if non-empty, else "system"
//   root_invocation_channel = rootChannel if non-empty, else invocation_channel
//   actor/on-behalf/agent   = the value if non-empty, else NULL
// `_gonvex_sync_changes.command_id` is commandId if non-empty, else NULL.

export interface Provenance {
  readonly commandId?: string | null | undefined;
  readonly rootCommandId?: string | null | undefined;
  readonly rootChannel?: string | null | undefined;
  readonly channel?: string | null | undefined;
  readonly actorAccountId?: string | null | undefined;
  readonly actorMemberId?: string | null | undefined;
  readonly onBehalfOfMemberId?: string | null | undefined;
  readonly agentExecutionId?: string | null | undefined;
}

/** The `_gonvex_sync_transactions` row values (without revision and transaction_id). */
export interface TransactionRow {
  root_command_id: string;
  origin_command_id: string;
  root_invocation_channel: string;
  invocation_channel: string;
  actor_account_id: string | null;
  actor_member_id: string | null;
  on_behalf_of_member_id: string | null;
  agent_execution_id: string | null;
}

const nonEmpty = (value: string | null | undefined): string | null => (value ? value : null);

export function transactionRow(provenance: Provenance): TransactionRow {
  const command = provenance.commandId ?? "";
  const channel = nonEmpty(provenance.channel) ?? "system";
  return {
    root_command_id: nonEmpty(provenance.rootCommandId) ?? command,
    origin_command_id: command,
    root_invocation_channel: nonEmpty(provenance.rootChannel) ?? channel,
    invocation_channel: channel,
    actor_account_id: nonEmpty(provenance.actorAccountId),
    actor_member_id: nonEmpty(provenance.actorMemberId),
    on_behalf_of_member_id: nonEmpty(provenance.onBehalfOfMemberId),
    agent_execution_id: nonEmpty(provenance.agentExecutionId),
  };
}

/** `command_id` stamped on the revision's change rows. */
export function changeCommandId(provenance: Provenance): string | null {
  return nonEmpty(provenance.commandId);
}
