/** Keep Dexie's VersionError retry from opening a newer format without a version. */
export function versionedIndexedDB(factory: IDBFactory | undefined, schemaVersion: number): IDBFactory | undefined {
  if (!factory) return undefined;
  return new Proxy(factory, {
    get(target, property) {
      if (property === "open") {
        // Dexie maps its declared schema version to native version * 10.
        // Keep that bound even when Dexie retries without a version after a
        // VersionError. Native IndexedDB then rejects before opening any data.
        return (name: string, version?: number) => target.open(name, version ?? schemaVersion * 10);
      }
      // Native IDBFactory methods require their original receiver.
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}
