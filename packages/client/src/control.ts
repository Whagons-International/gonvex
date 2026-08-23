import type { JsonValue } from "@gonvex/protocol";
import type { FunctionReference } from "./index.js";

type JSONObject = { [key: string]: JsonValue };
type Authorization = "public" | "account" | "tenantAdmin" | "projectAdmin";
const stringSchema = { kind: "string" } as const;
const optionalStringSchema = { kind: "string", optional: true } as const;
const booleanSchema = { kind: "boolean" } as const;
const numberSchema = { kind: "number" } as const;
const anySchema = { kind: "any" } as const;
const emptySchema = { kind: "object", fields: {}, allowUnknown: false } as const;
const objectSchema = (fields: Record<string, JsonValue>) => ({ kind: "object", fields, allowUnknown: false } as const);
const arraySchema = (items: JsonValue = anySchema) => ({ kind: "array", items } as const);
const accountSchema = objectSchema({ id:stringSchema,email:stringSchema,name:stringSchema,avatarUrl:stringSchema });
const authAccountSchema = objectSchema({
  id:stringSchema,email:stringSchema,emailVerified:booleanSchema,name:stringSchema,picture:stringSchema,provider:stringSchema,
});
const tenantSchema = objectSchema({ id:stringSchema,name:stringSchema,role:stringSchema,permissions:anySchema });
const directoryTenantSchema = objectSchema({
  id:stringSchema,name:stringSchema,role:stringSchema,permissions:anySchema,
  domain:stringSchema,timezone:stringSchema,profile:anySchema,
});
const sessionSchema = objectSchema({
  accessToken:stringSchema,tokenType:stringSchema,expiresIn:numberSchema,expiresAt:numberSchema,
  refreshToken:stringSchema,refreshExpiresAt:numberSchema,account:authAccountSchema,
  tenants:arraySchema(tenantSchema),activeTenantId:stringSchema,
});
const updatedSchema = objectSchema({updated:booleanSchema});
const idSchema = objectSchema({ id: stringSchema });
const tokenSchema = objectSchema({ id: stringSchema, token: stringSchema });
const invitationLookupSchema = objectSchema({ tenantName: stringSchema, role: stringSchema, expiresAt: stringSchema });
const invitationAcceptanceSchema = objectSchema({ tenantId: stringSchema, memberId: stringSchema });
const developerSchema = objectSchema({ email: stringSchema, name: stringSchema, role: stringSchema });
const settingSchema = objectSchema({ kind: stringSchema, scopeId: stringSchema, value: anySchema });
const supportSessionSchema = objectSchema({
  id: stringSchema, tenantId: stringSchema, accountId: stringSchema,
  release: stringSchema, environment: stringSchema, lastSeenAt: stringSchema,
});
const supportTenantSchema = objectSchema({
  id: stringSchema, name: stringSchema, domain: stringSchema, status: stringSchema,
  timezone: stringSchema, seatLimit: anySchema, createdAt: stringSchema,
});
const supportErrorGroupSchema = objectSchema({
  fingerprint: stringSchema, project: stringSchema, title: stringSchema, level: stringSchema,
  culprit: optionalStringSchema, status: stringSchema, priority: stringSchema, assignee: optionalStringSchema,
  firstSeen: stringSchema, lastSeen: stringSchema, count: numberSchema,
  tenants: anySchema, releases: anySchema, environments: anySchema,
  accounts: anySchema, devices: anySchema, regression: booleanSchema, latest: anySchema,
});
const supportErrorsSchema = objectSchema({ groups: arraySchema(supportErrorGroupSchema), releases: arraySchema(stringSchema) });
const impersonationSchema = objectSchema({ id: stringSchema, token: stringSchema, expiresAt: stringSchema });
const memberResultSchema = objectSchema({ accountId: stringSchema, memberId: stringSchema });

export const controlFunctionSchemas: Readonly<Record<string, { args: JsonValue; result: JsonValue }>> = Object.freeze({
  "control.accounts.me": { args: emptySchema, result: accountSchema },
  "control.accounts.updatePassword": { args: objectSchema({ currentPassword:stringSchema,newPassword:stringSchema }), result: updatedSchema },
  "control.accounts.provisionMemberLogin": { args: objectSchema({ email:stringSchema,name:stringSchema,password:stringSchema,role:stringSchema,permissions:anySchema }), result:objectSchema({updated:booleanSchema,accountId:stringSchema,memberId:stringSchema}) },
  "control.auth.passwordLogin": { args: objectSchema({ email:stringSchema,password:stringSchema }), result:sessionSchema },
  "control.auth.refreshSession": { args: objectSchema({ refreshToken:stringSchema }), result:sessionSchema },
  "control.auth.logout": { args: objectSchema({ refreshToken:stringSchema,all:booleanSchema }), result:updatedSchema },
  "control.auth.publicSettings": { args:emptySchema,result:objectSchema({providers:arraySchema(stringSchema)}) },
  "control.auth.realms.list": { args:emptySchema,result:arraySchema(objectSchema({provider:stringSchema,enabled:booleanSchema,signupMode:stringSchema})) },
  "control.auth.realms.configure": { args:objectSchema({provider:stringSchema,enabled:booleanSchema,signupMode:stringSchema}),result:updatedSchema },
  "control.tenants.mine": { args:emptySchema,result:arraySchema(directoryTenantSchema) },
  "control.tenants.create": { args:objectSchema({name:stringSchema}),result:tenantSchema },
  "control.tenants.getByDomain": { args:objectSchema({domain:stringSchema}),result:objectSchema({id:stringSchema,name:stringSchema,domain:stringSchema}) },
  "control.tenants.updateProfile": { args:objectSchema({name:stringSchema,domain:stringSchema,description:stringSchema}),result:updatedSchema },
  "control.tenants.updateTimezone": { args:objectSchema({timezone:stringSchema}),result:updatedSchema },
  "control.tenants.delete": { args:emptySchema,result:updatedSchema },
  "control.tenants.setException": { args:objectSchema({tenantId:stringSchema,value:anySchema}),result:objectSchema({updated:booleanSchema}) },
  "control.tenants.setSeatLimit": { args:objectSchema({tenantId:stringSchema,seatLimit:anySchema}),result:objectSchema({updated:booleanSchema}) },
  "control.invitations.lookup": { args:objectSchema({token:stringSchema}),result:invitationLookupSchema },
  "control.invitations.create": { args:objectSchema({email:stringSchema,role:stringSchema,permissions:anySchema}),result:tokenSchema },
  "control.invitations.accept": { args:objectSchema({token:stringSchema}),result:invitationAcceptanceSchema },
  "control.invitations.revoke": { args:objectSchema({id:stringSchema,email:stringSchema}),result:objectSchema({updated:booleanSchema}) },
  "control.agentAuth.issue": { args:objectSchema({permissions:arraySchema(stringSchema),expiresInSeconds:numberSchema}),result:tokenSchema },
  "control.agentAuth.claim": { args:objectSchema({token:stringSchema}),result:objectSchema({id:stringSchema,permissions:arraySchema(stringSchema)}) },
  "control.agentAuth.revoke": { args:objectSchema({id:stringSchema}),result:objectSchema({updated:booleanSchema}) },
  "control.project.developers.list": { args:emptySchema,result:arraySchema(developerSchema) },
  "control.project.developers.invite": { args:objectSchema({email:stringSchema,name:stringSchema,role:stringSchema}),result:objectSchema({updated:booleanSchema}) },
  "control.project.developers.remove": { args:objectSchema({email:stringSchema}),result:objectSchema({updated:booleanSchema}) },
  "control.assistant.getDefaults": { args:emptySchema,result:anySchema },
  "control.assistant.setDefaults": { args:objectSchema({scopeId:stringSchema,value:anySchema}),result:objectSchema({updated:booleanSchema}) },
  "control.voice.getConfiguration": { args:emptySchema,result:arraySchema(settingSchema) },
  "control.voice.setRateCard": { args:objectSchema({scopeId:stringSchema,value:anySchema}),result:objectSchema({updated:booleanSchema}) },
  "control.voice.setTenantEntitlement": { args:objectSchema({scopeId:stringSchema,value:anySchema}),result:objectSchema({updated:booleanSchema}) },
  "control.voice.setUserOverride": { args:objectSchema({scopeId:stringSchema,value:anySchema}),result:objectSchema({updated:booleanSchema}) },
  "control.support.listSessions": { args:emptySchema,result:arraySchema(supportSessionSchema) },
  "control.support.listTenants": { args:emptySchema,result:arraySchema(supportTenantSchema) },
  "control.support.listErrors": { args:emptySchema,result:supportErrorsSchema },
  "control.support.heartbeat": { args:objectSchema({release:stringSchema,environment:stringSchema}),result:objectSchema({sessionId:stringSchema}) },
  "control.support.sendCommand": { args:objectSchema({sessionId:stringSchema,kind:stringSchema,payload:anySchema}),result:idSchema },
  "control.support.ackCommand": { args:objectSchema({id:stringSchema}),result:objectSchema({updated:booleanSchema}) },
  "control.support.createImpersonation": { args:objectSchema({accountId:stringSchema,tenantId:stringSchema,reason:stringSchema}),result:impersonationSchema },
  "control.demos.create": { args:objectSchema({tenantId:stringSchema,email:stringSchema,name:stringSchema,password:stringSchema,label:stringSchema}),result:memberResultSchema },
  "control.demos.resetPassword": { args:objectSchema({accountId:stringSchema,password:stringSchema}),result:objectSchema({updated:booleanSchema}) },
  "control.demos.delete": { args:objectSchema({accountId:stringSchema}),result:objectSchema({updated:booleanSchema}) },
});

function ref<A extends JsonValue, R extends JsonValue>(kind: "query" | "reducer" | "action", path: string, authorization: Authorization) {
  const schemas = controlFunctionSchemas[path];
  if (!schemas) throw new Error(`Missing Control Plane schema for ${path}`);
  return Object.freeze({ kind, path, scope: "control", delivery: "oneShot", authorization, argsSchema:schemas.args, resultSchema:schemas.result }) as FunctionReference<A, R>;
}

export type ControlAccount = JSONObject & { id: string; email: string; name: string; avatarUrl: string };
export type ControlTenant = JSONObject & {
  id: string; name: string; role: string; permissions: JSONObject;
  domain: string; timezone: string; profile: JsonValue;
};
export type ControlTenantCreated = JSONObject & { id: string; name: string; role: string; permissions: JSONObject };
export type ControlSession = JSONObject & {
  accessToken: string; tokenType: string; expiresIn: number; expiresAt: number;
  refreshToken: string; refreshExpiresAt: number; account: JSONObject;
  tenants: ControlTenantCreated[]; activeTenantId: string;
};
export type ControlAuthRealm = JSONObject & { provider: string; enabled: boolean; signupMode: string };
export type ControlUpdated = JSONObject & { updated: boolean };
export type ControlMemberProvisioned = ControlUpdated & { accountId: string; memberId: string };
export type ControlInvitation = JSONObject & { tenantName: string; role: string; expiresAt: string };
export type ControlToken = JSONObject & { id: string; token: string };
export type ControlInvitationAcceptance = JSONObject & { tenantId: string; memberId: string };
export type ControlAgentClaim = JSONObject & { id: string; permissions: string[] };
export type ControlDeveloper = JSONObject & { email: string; name: string; role: string };
export type ControlSetting = JSONObject & { kind: string; scopeId: string; value: JsonValue };
export type ControlSupportSession = JSONObject & {
  id: string; tenantId: string; accountId: string; release: string;
  environment: string; lastSeenAt: string;
};
export type ControlSupportTenant = JSONObject & {
  id: string; name: string; domain: string; status: string; timezone: string;
  seatLimit: number | null; createdAt: string;
};
export type ControlSupportErrorGroup = JSONObject & {
  fingerprint: string; project: string; title: string; level: string; culprit?: string;
  status: string; priority: string; assignee?: string; firstSeen: string; lastSeen: string;
  count: number; tenants: JSONObject; releases: JSONObject; environments: JSONObject;
  accounts: JSONObject; devices: JSONObject; regression: boolean; latest: JsonValue;
};
export type ControlSupportErrors = JSONObject & { groups: ControlSupportErrorGroup[]; releases: string[] };
export type ControlImpersonation = JSONObject & { id: string; token: string; expiresAt: string };
export type ControlMemberResult = JSONObject & { accountId: string; memberId: string };

/** Typed references for Gonvex host-owned Control Plane functions. */
export const control = Object.freeze({
  accounts: Object.freeze({
    me: ref<JSONObject, ControlAccount>("query", "control.accounts.me", "account"),
    updatePassword: ref<JSONObject & { currentPassword: string; newPassword: string }, ControlUpdated>("reducer", "control.accounts.updatePassword", "account"),
    provisionMemberLogin: ref<JSONObject & { email: string; name: string; password: string; role: string; permissions: JSONObject }, ControlMemberProvisioned>("reducer", "control.accounts.provisionMemberLogin", "tenantAdmin"),
  }),
  auth: Object.freeze({
    publicSettings: ref<JSONObject, JSONObject & { providers: string[] }>("query", "control.auth.publicSettings", "public"),
    passwordLogin: ref<JSONObject & { email: string; password: string }, ControlSession>("action", "control.auth.passwordLogin", "public"),
    refreshSession: ref<JSONObject & { refreshToken: string }, ControlSession>("action", "control.auth.refreshSession", "public"),
    logout: ref<JSONObject & { refreshToken: string; all: boolean }, ControlUpdated>("reducer", "control.auth.logout", "account"),
    realms: Object.freeze({
      list: ref<JSONObject, ControlAuthRealm[]>("query", "control.auth.realms.list", "projectAdmin"),
      configure: ref<JSONObject & { provider: string; enabled: boolean; signupMode: string }, ControlUpdated>("reducer", "control.auth.realms.configure", "projectAdmin"),
    }),
  }),
  tenants: Object.freeze({
    mine: ref<JSONObject, ControlTenant[]>("query", "control.tenants.mine", "account"),
    create: ref<JSONObject & { name: string }, ControlTenantCreated>("reducer", "control.tenants.create", "account"),
    getByDomain: ref<JSONObject & { domain: string }, JSONObject & { id: string; name: string; domain: string }>("query", "control.tenants.getByDomain", "public"),
    updateProfile: ref<JSONObject & { name: string; domain: string; description: string }, ControlUpdated>("reducer", "control.tenants.updateProfile", "tenantAdmin"),
    updateTimezone: ref<JSONObject & { timezone: string }, ControlUpdated>("reducer", "control.tenants.updateTimezone", "tenantAdmin"),
    delete: ref<JSONObject, ControlUpdated>("reducer", "control.tenants.delete", "tenantAdmin"),
    setException: ref<JSONObject & { tenantId: string; value: JsonValue }, ControlUpdated>("reducer", "control.tenants.setException", "projectAdmin"),
    setSeatLimit: ref<JSONObject & { tenantId: string; seatLimit: number | null }, ControlUpdated>("reducer", "control.tenants.setSeatLimit", "projectAdmin"),
  }),
  invitations: Object.freeze({
    lookup: ref<JSONObject & { token: string }, ControlInvitation>("query", "control.invitations.lookup", "public"),
    create: ref<JSONObject & { email: string; role: string; permissions: JSONObject }, ControlToken>("reducer", "control.invitations.create", "tenantAdmin"),
    accept: ref<JSONObject & { token: string }, ControlInvitationAcceptance>("reducer", "control.invitations.accept", "account"),
    revoke: ref<JSONObject & { id: string; email: string }, ControlUpdated>("reducer", "control.invitations.revoke", "tenantAdmin"),
  }),
  agentAuth: Object.freeze({
    issue: ref<JSONObject & { permissions: string[]; expiresInSeconds: number }, ControlToken>("reducer", "control.agentAuth.issue", "projectAdmin"),
    claim: ref<JSONObject & { token: string }, ControlAgentClaim>("reducer", "control.agentAuth.claim", "account"),
    revoke: ref<JSONObject & { id: string }, ControlUpdated>("reducer", "control.agentAuth.revoke", "projectAdmin"),
  }),
  project: Object.freeze({ developers: Object.freeze({
    list: ref<JSONObject, ControlDeveloper[]>("query", "control.project.developers.list", "projectAdmin"),
    invite: ref<JSONObject & { email: string; name: string; role: string }, ControlUpdated>("reducer", "control.project.developers.invite", "projectAdmin"),
    remove: ref<JSONObject & { email: string }, ControlUpdated>("reducer", "control.project.developers.remove", "projectAdmin"),
  }) }),
  assistant: Object.freeze({
    getDefaults: ref<JSONObject, JSONObject>("query", "control.assistant.getDefaults", "projectAdmin"),
    setDefaults: ref<JSONObject & { scopeId: string; value: JsonValue }, ControlUpdated>("reducer", "control.assistant.setDefaults", "projectAdmin"),
  }),
  voice: Object.freeze({
    getConfiguration: ref<JSONObject, ControlSetting[]>("query", "control.voice.getConfiguration", "projectAdmin"),
    setRateCard: ref<JSONObject & { scopeId: string; value: JsonValue }, ControlUpdated>("reducer", "control.voice.setRateCard", "projectAdmin"),
    setTenantEntitlement: ref<JSONObject & { scopeId: string; value: JsonValue }, ControlUpdated>("reducer", "control.voice.setTenantEntitlement", "projectAdmin"),
    setUserOverride: ref<JSONObject & { scopeId: string; value: JsonValue }, ControlUpdated>("reducer", "control.voice.setUserOverride", "projectAdmin"),
  }),
  support: Object.freeze({
    listTenants: ref<JSONObject, ControlSupportTenant[]>("query", "control.support.listTenants", "projectAdmin"),
    listSessions: ref<JSONObject, ControlSupportSession[]>("query", "control.support.listSessions", "projectAdmin"),
    listErrors: ref<JSONObject, ControlSupportErrors>("query", "control.support.listErrors", "projectAdmin"),
    heartbeat: ref<JSONObject & { release: string; environment: string }, JSONObject & { sessionId: string }>("reducer", "control.support.heartbeat", "account"),
    sendCommand: ref<JSONObject & { sessionId: string; kind: string; payload: JsonValue }, JSONObject & { id: string }>("reducer", "control.support.sendCommand", "projectAdmin"),
    ackCommand: ref<JSONObject & { id: string }, ControlUpdated>("reducer", "control.support.ackCommand", "account"),
    createImpersonation: ref<JSONObject & { accountId: string; tenantId: string; reason: string }, ControlImpersonation>("reducer", "control.support.createImpersonation", "projectAdmin"),
  }),
  demos: Object.freeze({
    create: ref<JSONObject & { tenantId: string; email: string; name: string; password: string; label: string }, ControlMemberResult>("reducer", "control.demos.create", "projectAdmin"),
    resetPassword: ref<JSONObject & { accountId: string; password: string }, ControlUpdated>("reducer", "control.demos.resetPassword", "projectAdmin"),
    delete: ref<JSONObject & { accountId: string }, ControlUpdated>("reducer", "control.demos.delete", "projectAdmin"),
  }),
});
