import { serveEngineWorker } from "../../src/engine/worker.js";
import { binding } from "./fixture.js";
serveEngineWorker({ localRuntime: binding });
