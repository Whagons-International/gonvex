// The two placements of a socket's holder (brief: one interface, two
// placements). `GatewayOutlet` sends to gateway objects over RPC; `LocalOutlet`
// hands payloads to sockets in this object. The engine cannot tell them apart.
//
// Payloads to one holder go one at a time, in order: Workers RPC gives no
// order across separate calls, and a socket must see a snapshot before the
// deltas that follow it. A payload stays under the 32 MiB RPC argument limit
// because snapshot pages are posted chunk by chunk (0020 §6, 0022 §4.4).

import type { DeliveryOutlet } from "./engine.ts";
import type { ReplicaDelivery, ReplicaReceipt } from "./protocol.ts";

interface Queue {
  tail: Promise<unknown>;
  count: number;
  generation: number;
}

export interface ReplicaHolderRpc {
  deliverReplica(tenantId: string, payload: ReplicaDelivery): Promise<ReplicaReceipt>;
}

abstract class OrderedOutlet implements DeliveryOutlet {
  private readonly queues = new Map<string, Queue>();

  protected abstract deliver(host: string, payload: ReplicaDelivery): Promise<ReplicaReceipt>;
  abstract hosts(): Promise<string[]>;

  private queue(host: string): Queue {
    let queue = this.queues.get(host);
    if (!queue) this.queues.set(host, (queue = { tail: Promise.resolve(), count: 0, generation: 0 }));
    return queue;
  }

  send(host: string, payload: ReplicaDelivery): Promise<ReplicaReceipt | undefined> {
    const queue = this.queue(host);
    const generation = queue.generation;
    queue.count += 1;
    const run = async (): Promise<ReplicaReceipt | undefined> => {
      try {
        // Discarded (lag): its sockets were reset by a later payload.
        if (generation !== queue.generation) return undefined;
        return await this.deliver(host, payload);
      } finally {
        queue.count -= 1;
      }
    };
    const next = queue.tail.then(run, run);
    queue.tail = next.catch(() => undefined);
    return next;
  }

  queued(host: string): number {
    return this.queues.get(host)?.count ?? 0;
  }

  discard(host: string): void {
    this.queue(host).generation += 1;
  }

  async idle(host: string): Promise<void> {
    await this.queues.get(host)?.tail;
  }
}

/** Gateway placement (0016 G0-1): one RPC per payload to the gateway holding the sockets. */
export class GatewayOutlet extends OrderedOutlet {
  constructor(
    private readonly tenantId: string,
    private readonly gateway: (name: string) => ReplicaHolderRpc,
    private readonly registered: () => Promise<string[]>,
  ) {
    super();
  }

  protected deliver(host: string, payload: ReplicaDelivery): Promise<ReplicaReceipt> {
    return this.gateway(host).deliverReplica(this.tenantId, payload);
  }

  hosts(): Promise<string[]> {
    return this.registered();
  }
}

/** Tenant placement: the sockets live in this object; `deliver` is a local call. */
export class LocalOutlet extends OrderedOutlet {
  constructor(private readonly local: (host: string, payload: ReplicaDelivery) => ReplicaReceipt, private readonly names: () => string[]) {
    super();
  }

  protected async deliver(host: string, payload: ReplicaDelivery): Promise<ReplicaReceipt> {
    return this.local(host, payload);
  }

  async hosts(): Promise<string[]> {
    return this.names();
  }
}
