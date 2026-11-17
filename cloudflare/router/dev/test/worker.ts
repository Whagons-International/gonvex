// The MemoryVersionTarget does not replace this test Worker's baked catalog.
// Real catalog/tenant activation is exercised by the assembled runtime test.
import { OperatorControlObject as Control } from "../../../control/operator/object";
export { default, LocalKernelTenant } from "../local/worker";
export class OperatorControlObject extends Control {
  protected override async activatePublicationTenant(): Promise<boolean> { return true; }
}
