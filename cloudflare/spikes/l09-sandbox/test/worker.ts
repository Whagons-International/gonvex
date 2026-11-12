// The test Worker: the sandbox classes and nothing else.
export { RunSlots, SandboxAnalysis, SandboxRegistry, SandboxWorkspace } from "../src/index";

export default {
  fetch(): Response {
    return new Response("k20 sandbox test worker");
  },
};
