// Bindings of test/wrangler.jsonc.
declare namespace Cloudflare {
  interface Env {
    TENANT: DurableObjectNamespace<import("./worker").TestFilesTenant>;
    FILES: R2Bucket;
    GONVEX_PUBLIC_URL: string;
    GONVEX_STORAGE_SECRET: string;
    GONVEX_STORAGE_BUCKET: string;
    GONVEX_S3_ENDPOINT: string;
    GONVEX_S3_ACCESS_KEY_ID: string;
    GONVEX_S3_SECRET_ACCESS_KEY: string;
  }
}
interface Env extends Cloudflare.Env {}
