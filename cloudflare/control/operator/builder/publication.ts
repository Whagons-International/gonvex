import type { ActiveRelease, DeploymentGrant, DeploymentRef, ReleaseBuilder } from "../release.ts";

export interface DeploymentAuthority {
  authorize(ref: DeploymentRef): Promise<DeploymentGrant>;
  settle(ref: DeploymentRef, result: {ok:boolean;error?:string;retain?:boolean}): Promise<void>;
  abort(ref: DeploymentRef, error: string): Promise<void>;
}

/** Runs in the real builder, where target execution (including delayed local
 * swaps) finishes before another deployment or abort can pass this barrier.
 * Authorization is checked at execution time, never when an HTTP call queues. */
export class PublicationExecutor {
  private tail: Promise<unknown> = Promise.resolve();
  private builder:ReleaseBuilder;
  private authority:DeploymentAuthority;
  private restore:(ref:DeploymentRef,catalog:ActiveRelease[])=>Promise<void>;
  constructor(builder: ReleaseBuilder, authority: DeploymentAuthority, restore: (ref: DeploymentRef, catalog: ActiveRelease[]) => Promise<void>) {
    this.builder=builder;this.authority=authority;this.restore=restore;
  }

  private serial<T>(run:()=>Promise<T>):Promise<T> {
    const work=this.tail.then(run,run);this.tail=work.catch(()=>undefined);return work;
  }
  deploy(ref: DeploymentRef): Promise<{ok:true}|{ok:false;error:string;retain?:true}> {
    return this.serial(async()=>{
      const grant=await this.authority.authorize(ref);
      if (!grant.allowed) {
        if (grant.completed || grant.serving) return {ok:true};
        return {ok:false,error:"deployment fence is not current"};
      }
      let result:{ok:true}|{ok:false;error:string;retain?:true};
      try {
        await this.restore(ref,grant.catalog);
        result=await this.builder.deploy(ref.version,undefined,grant.writeDeadline===undefined?undefined:{deadline:grant.writeDeadline});
      } catch (cause) { result={ok:false,error:(cause as Error).message}; }
      if(!result.ok&&grant.recovery)result={...result,retain:true};
      await this.authority.settle(ref,result);
      return result;
    });
  }
  abort(ref: DeploymentRef, error: string): Promise<{ok:true}> {
    return this.serial(async()=>{await this.authority.abort(ref,error);return {ok:true};});
  }
}
