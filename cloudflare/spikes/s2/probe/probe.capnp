using Workerd = import "/workerd/workerd.capnp";
const config :Workerd.Config = (
  services = [ (name = "main", worker = .probe), (name = "disk", disk = (path = "../.storage/probe", writable = true)) ],
  sockets = [ (name = "http", address = "127.0.0.1:28787", http = (), service = "main") ],
);
const probe :Workerd.Worker = (
  modules = [ (name = "probe.js", esModule = embed "probe.js") ],
  compatibilityDate = "2026-10-01",
  compatibilityFlags = ["nodejs_als"],
  durableObjectNamespaces = [ (className = "Probe", uniqueKey = "s2-probe", enableSql = true) ],
  durableObjectStorage = (localDisk = "disk"),
  bindings = [ (name = "PROBE", durableObjectNamespace = "Probe") ],
);
