// The test Worker: the three sandbox classes and nothing else.
export { SandboxAnalysis, SandboxRegistry, SandboxWorkspace } from "../src/index";

export default {
  fetch(): Response {
    return new Response("k20 sandbox test worker");
  },
};
