// Everything visibility keeps per tenant object, in one place: the parent ACL
// indexes, the principal cache and the permission bits. One instance per
// tenant object; nothing here is per connection.

import type { FeedTransaction } from "./feed.ts";
import { PermissionBits, type PermissionGrantSource } from "./permission-bits.ts";
import { PrincipalCache, type PrincipalCacheConfig } from "./principal-cache.ts";
import { ParentAclRegistry } from "./registry.ts";

export class VisibilityRegistry {
  readonly acl = new ParentAclRegistry();
  readonly principals: PrincipalCache;
  private permissionBits: PermissionBits | undefined;

  constructor(options: { principalCache?: Partial<PrincipalCacheConfig> } = {}) {
    this.principals = new PrincipalCache(options.principalCache);
  }

  /** The module's `permissionGrants` (undefined when it declares none). A changed declaration rebuilds the bits. */
  setPermissionGrants(source: PermissionGrantSource | undefined): void {
    if (!source) {
      this.permissionBits = undefined;
      return;
    }
    const current = this.permissionBits?.source;
    if (current && JSON.stringify(current) === JSON.stringify(source)) return;
    this.permissionBits = new PermissionBits(source);
  }

  get permissions(): PermissionBits | undefined {
    return this.permissionBits;
  }

  /** One committed transaction, in revision order (delivery's push path). */
  apply(transaction: FeedTransaction, previousRevision?: number): void {
    this.acl.apply(transaction, previousRevision);
    this.permissionBits?.apply(transaction);
  }

  /** Schema change (migration, new generation): capture classes and projections may differ. */
  invalidateSchema(): void {
    this.principals.invalidateSchema();
    this.acl.poisonAll();
    this.permissionBits?.poison();
  }
}
