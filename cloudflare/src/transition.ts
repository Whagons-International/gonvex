// One-time namespace handoff only; never an app-publication target.
export {default} from "./index";
export * from "./index";
export {AssemblyTenant} from "../kernel/assembly/tenant";
export {TransitionControl as SessionControlObject} from '../kernel/assembly/transition-control';
