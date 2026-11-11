// `importFile`'s storage read against K13's tenant files (#354), streaming the
// object from R2 instead of buffering it. K13's `ActionFiles.readFile` is an
// equivalent `SandboxFileSource` that buffers; either can be passed.
import type { SandboxFileSource, SandboxSession } from "./service";

/** K13's `FilesTenantRpc.filesReadTarget`: finalize, owner check (IB1), `uploaded` required. */
export interface FilesReadTarget {
  filesReadTarget(session: never, fileId: string): Promise<{ objectKey: string }>;
}

export function r2FileSource(options: { tenant: (session: SandboxSession) => FilesReadTarget; bucket: R2Bucket | undefined }): SandboxFileSource {
  return {
    async readFile(session, fileId, maxBytes) {
      if (!options.bucket) throw new Error("storage is not configured");
      const target = await options.tenant(session).filesReadTarget(session as never, fileId);
      const object = await options.bucket.get(target.objectKey);
      if (!object) throw new Error("object storage returned 404 Not Found");
      if (object.size > maxBytes) {
        await object.body.cancel();
        throw new Error("stored file exceeds the sandbox import limit");
      }
      return { stream: object.body, size: object.size };
    },
  };
}
