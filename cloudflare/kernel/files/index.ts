// Kernel files (K13): `_gonvex_files` in the tenant object, R2 objects, signed
// proxy URLs and the IB1 owner checks. See README.md.
export { FILE_NOT_FOUND, type FilesSession, isAnonymous, mayAccess, OWNER_REFUSED, ownerFor, visibilityFor } from "./access";
export {
  DEFAULT_DOWNLOAD_SECONDS, DEFAULT_PROXY_MAX_BYTES, DEFAULT_UPLOAD_SECONDS, type FilesConfig, filesConfigFromEnv,
  MAX_STORE_BYTES, MAX_UPLOAD_BYTES, ttlSeconds,
} from "./config";
export { awsEncode, encodeKey, newFileId, objectKey, type ParsedKey, parseObjectKey, safeSegment } from "./keys";
export { INLINE_MEDIA_TYPES, protectStorageResponse, storageCors } from "./media";
export { reducerFiles, type ReducerFilesOptions, reducerSession } from "./reducer";
export { parseStorageRequest, type StorageRequest } from "./request";
export { ActionFiles, type ActionFilesOptions, type FilesTenantRpc, filesRpc } from "./rpc";
export { FILES_SCHEMA, type FileMetadata, type FileRow, metadataOf, rfc3339 } from "./schema";
export { constantTimeEqual, proxySignature, proxyUrl, type SignatureScope, verifyProxy } from "./signing";
export { objectUrl, presignUrl, type S3Credentials } from "./sigv4";
export { decodeStoreContent, INVALID_BASE64, storeLimitError } from "./store";
export {
  type ClaimResult, type DownloadTarget, NOT_CONFIGURED, type ReadTarget, type StoredObject, TenantFiles, type TenantFilesOptions,
  UPLOAD_CONSUMED,
} from "./tenant-files";
