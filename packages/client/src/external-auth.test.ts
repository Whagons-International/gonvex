import { describe, expect, it, vi } from "vitest";
import { createFirebaseAuthAdapter } from "./external-auth";

describe("createFirebaseAuthAdapter", () => {
  it("keeps Firebase outside the package and forwards token lifecycle callbacks", async () => {
    const getIdToken = vi.fn(async (forceRefresh: boolean) => forceRefresh ? "fresh" : "cached");
    let listener: ((identity: { uid: string } | null) => void) | undefined;
    const unsubscribe = vi.fn();
    const adapter = createFirebaseAuthAdapter({
      getIdToken,
      onIdTokenChanged(next) { listener = next; return unsubscribe; },
    });
    const changed = vi.fn();
    const stop = adapter.onIdTokenChanged(changed);
    listener?.({ uid: "firebase-uid" });
    expect(changed).toHaveBeenCalledWith({ uid: "firebase-uid" });
    expect(await adapter.getIdToken(true)).toBe("fresh");
    expect(getIdToken).toHaveBeenCalledWith(true);
    stop();
    expect(unsubscribe).toHaveBeenCalledOnce();
    expect(adapter.provider).toBe("firebase");
  });
});
