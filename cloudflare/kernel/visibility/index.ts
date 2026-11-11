// Kernel visibility (package K09): masks v2 as SQL, interpreter and compiled
// class programs; permission grants; the principal cache. See README.md.

export { compareText, debugString, field, get, isObject, jsonEqual, scalar, serdeJson, sortedText, truthy, type JsonObject, type JsonValue } from "./json.ts";
export {
  aclColumns,
  activeSets,
  dependencyColumns,
  dependencyColumnsForSets,
  forgetPlanCaches,
  identifier,
  indexColumns,
  inheritedColumns,
  inheritedTables,
  inheritsAny,
  parseConstraint,
  parseExpression,
  parsePlan,
  parseSet,
  planToJson,
  principalAnchors,
  principalChangePossible,
  principalInputs,
  quote,
  requiredIndexes,
  roleOnlyValue,
  rowIdColumn,
  setEntries,
  validateManifest,
  validatePlan,
  viaIndexName,
  VisibilityDependencies,
  VisibilityError,
  type VisibilityConstraint,
  type VisibilityErrorKind,
  type VisibilityExpression,
  type VisibilityJoin,
  type VisibilityPlan,
  type VisibilitySet,
  type VisibilityVia,
} from "./plan.ts";
export { exactlyAt, GrantIndex, grantPair, ParentAclIndex, readyAt, readyNow, usableRevision, type IndexSignal } from "./acl-index.ts";
export { emptyResolution, InheritedParent, withParentOverlay, type DirectContext, type ParentOverlay, type ResolvedVisibility, type TokenSets } from "./resolved.ts";
export { literalConstraintsHold, matchExpression, rowMatches } from "./mask.ts";
export { compileLiteralComparison, compilePredicate, compileSet, compileSetWithParameters } from "./sql.ts";
export {
  activeGrantSets,
  ancestorDerivedChangeConcerns,
  ancestorRowChangeConcerns,
  boundIndexes,
  changeConcernsMember,
  derivedChangeConcerns,
  GRANT_JOIN_SUFFIX,
  grantChangeConcerns,
  grantContribution,
  grantJoinSet,
  indexesAhead,
  needsMaterializedSets,
  parentAdmits,
  principalChangeConcerns,
  recomputeDerived,
  resolveDerived,
  resolvedDependencyColumns,
  resolveMember,
  resolveMemberSql,
  resolveRelatedQuery,
  resolveWithParents,
  visibilityFingerprint,
  type DerivedChange,
  type GrantContribution,
  type ParentAclSource,
  type ResolutionMetrics,
  type SharedResolution,
  type VisibilitySession,
  type VisibilitySql,
} from "./principal.ts";
export {
  AclRow,
  ClassScope,
  compiled,
  CompiledPlan,
  EvalMemo,
  HandleCache,
  memberVisibility,
  MemberVisibility,
  rowJson,
  rowValue,
  VisibilityClass,
  type Candidates,
  type EvalStats,
  type MemberHandle,
  type RowView,
  type ScopeStats,
  type TokenSet,
} from "./class.ts";
export { CHANGE_PAGE_ROWS, readChanges, readClock, type FeedTransaction, type LogChange, type TenantClock } from "./feed.ts";
export { jsonValueExpr, loadIndex, ParentAclRegistry, PreparedParentAcl, projectChanges, type CatalogSql, type ColumnKind, type IndexShape, type RegistryStats } from "./registry.ts";
export { capturedTables, changed, inputTables, PrincipalCache, probeStatement, type PrincipalCacheConfig, type PrincipalCacheStats, type PrincipalClock } from "./principal-cache.ts";
export { loadPermissionIndex, parsePermissionGrants, PermissionBits, PermissionIndex, validatePermissionGrants, type PermissionGrantSource } from "./permission-bits.ts";
export { createQueryVisibility, dependencyTables, maskChangeRelevant, planOf, plansOf, type QueryVisibilityContext } from "./query-port.ts";
export { VisibilityRegistry } from "./tenant.ts";
