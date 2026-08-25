/** Non-secret identity hint used to partition persisted Local Replica state. */
export type GonvexExternalIdentityHint = {
  uid: string;
  issuer?: string;
};

/**
 * Host-auth adapter consumed by @gonvex/react. It deliberately mirrors the
 * small Firebase SDK boundary Gonvex needs and does not import Firebase.
 */
export type GonvexExternalAuthAdapter = {
  readonly provider: "firebase" | "external-oidc";
  getIdToken(forceRefresh: boolean): Promise<string | null>;
  onIdTokenChanged(listener: (identity: GonvexExternalIdentityHint | null) => void): () => void;
  signOut?: () => Promise<void>;
};

export type GonvexFirebaseAuthAdapterOptions = Omit<GonvexExternalAuthAdapter, "provider">;

/** Create a Firebase adapter without adding Firebase to Gonvex's dependency graph. */
export function createFirebaseAuthAdapter(options: GonvexFirebaseAuthAdapterOptions): GonvexExternalAuthAdapter {
  if (typeof options.getIdToken !== "function" || typeof options.onIdTokenChanged !== "function") {
    throw new TypeError("Firebase auth adapter requires getIdToken and onIdTokenChanged");
  }
  return Object.freeze({
    provider: "firebase" as const,
    getIdToken: options.getIdToken,
    onIdTokenChanged: options.onIdTokenChanged,
    ...(options.signOut ? { signOut: options.signOut } : {}),
  });
}
