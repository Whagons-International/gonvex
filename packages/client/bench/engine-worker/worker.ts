import { installWorkerProfile } from "./profile.js";
import { serveEngineWorker } from "../../src/engine/worker.js";
import { binding } from "./fixture.js";
if (location.search.includes("io")) installWorkerProfile();
serveEngineWorker({ localRuntime: binding });
