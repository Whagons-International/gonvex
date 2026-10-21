export type RuntimeInvocation = { kind: "query" | "reducer" | "action"; now: number; intentEntropy?: string };
export function createRuntimeGlobals(current: () => RuntimeInvocation | undefined): { Date: DateConstructor; Math: Math };
