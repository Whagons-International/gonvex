// One-time framework bridge. It still carries the captured monolithic catalog,
// so an already-staged SAME-ID publication can finish before the durable freeze.
// The steady-state stable Worker has no baked-version fallback.
import {SessionControlObject as StableControl} from './control';
// @ts-expect-error Migration alias is supplied with the captured whole catalog.
import * as generated from 'gonvex:releases';
export class TransitionControl extends StableControl {
 protected override publicationVersionReady(version:string):Promise<boolean>{
  return Promise.resolve((generated as {version?:string}).version===version);
 }
}
