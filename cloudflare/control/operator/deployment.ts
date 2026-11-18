/** The sync row also owns the whole Worker catalog. Its SQLite rowid is a
 * durable fence: terminal request IDs cannot be reused. Only preparation may
 * expire; a staged owner survives until publication or the builder's barrier. */
import { NOW_MS, num, str, type Sql, type Row } from "../src/sql";
import { HttpError } from "./http";
import type { ActiveRelease, DeploymentRef, DeploymentGrant } from "./release";

export const DEPLOYMENT_SCHEMA = `
DROP INDEX IF EXISTS gonvex_cf_syncs_running;
CREATE UNIQUE INDEX IF NOT EXISTS gonvex_cf_one_deployment
  ON gonvex_cf_syncs((1)) WHERE state IN ('running','publishing');
CREATE TABLE IF NOT EXISTS gonvex_cf_topology (singleton INTEGER PRIMARY KEY CHECK(singleton=1), frozen INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS gonvex_cf_target_attempts (sync_id TEXT PRIMARY KEY, uncertain INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS gonvex_cf_publication_catalogs (
  sync_id TEXT PRIMARY KEY, catalog JSON NOT NULL
);
`;

/** The pre-fence runtime can leave multiple running preparations after reset.
 * They never handed a deployment to the router (its finally first finished the
 * sync). Retire those once before installing the global index. Modern owners
 * must survive every subsequent constructor/code update. */
export function installDeploymentSchema(sql:Sql):void {
  sql.tx(()=>{
    if(sql.value("SELECT 1 FROM sqlite_master WHERE type='index' AND name='gonvex_cf_one_deployment'")===undefined){
      sql.run(`UPDATE gonvex_cf_syncs SET state='abandoned',finished_at=${NOW_MS} WHERE state='running'`);
    }
    sql.script(DEPLOYMENT_SCHEMA);
  });
}

export class DeploymentCoordinator {
  constructor(private sql: Sql, private versionReady: (version: string) => boolean | Promise<boolean>, private preparationMs: number,
    private versionAbsent: (version: string) => boolean | Promise<boolean> = () => false) {}

  private owner():Row|undefined {
    return this.sql.get(`SELECT s.rowid AS fence,s.sync_id,s.project_id,s.state,s.expires_at,p.version
      FROM gonvex_cf_syncs s LEFT JOIN gonvex_cf_publications p ON p.sync_id=s.sync_id
      WHERE s.state IN ('running','publishing')`);
  }
  topologyFrozen():boolean { return this.sql.value("SELECT frozen FROM gonvex_cf_topology WHERE singleton=1")===1; }
  freezeTopology():void {
    this.sql.tx(()=>{
      const owner=this.owner();
      // Preparation may expire just as it does during normal acquisition. A
      // staged publication never expires while a target write may still run.
      if(owner?.state==='running'&&num(owner.expires_at)<=Date.now())
        this.sql.run(`UPDATE gonvex_cf_syncs SET state='abandoned',finished_at=${NOW_MS} WHERE sync_id=$1`,owner.sync_id);
      else if(owner)throw new HttpError(409,"complete the current publication before topology migration");
      this.sql.run("INSERT INTO gonvex_cf_topology VALUES(1,1) ON CONFLICT(singleton) DO UPDATE SET frozen=1");
    });
  }
  thawTopology():void {this.sql.run("UPDATE gonvex_cf_topology SET frozen=0 WHERE singleton=1");}
  /** Called in the same transaction as the per-project refusal. */
  acquire(project:string,id:string):{acquired:true;fence:number}|{acquired:false;sameProject:boolean;recovery?:DeploymentRef} {
    if(this.topologyFrozen())throw new HttpError(503,"runtime topology migration in progress");
    let owner=this.owner();
    if(owner?.state==='running'&&num(owner.expires_at)<=Date.now()){
      this.sql.run(`UPDATE gonvex_cf_syncs SET state='abandoned',finished_at=${NOW_MS} WHERE sync_id=$1`,owner.sync_id);
      owner=undefined;
    }
    if(owner){
      if(owner.sync_id===id&&owner.state==='running')return {acquired:true,fence:num(owner.fence)};
      return {acquired:false,sameProject:owner.project_id===project,...(owner.version?{recovery:{id:str(owner.sync_id),fence:num(owner.fence),version:str(owner.version)}}:{})};
    }
    if(this.sql.value("SELECT 1 FROM gonvex_cf_syncs WHERE sync_id=$1",id)!==undefined)throw new HttpError(409,"sync deployment is no longer current; use a new request id");
    this.sql.run(`INSERT INTO gonvex_cf_syncs(sync_id,project_id,state,expires_at)
      VALUES($1,$2,'running',${NOW_MS}+$3)`,id,project,this.preparationMs);
    const fence=this.sql.value<number>("SELECT last_insert_rowid() AS fence");
    return {acquired:true,fence:num(fence)};
  }
  /** A conditional write combines fencing with the required catalog write. */
  catalog(id:string,fence:number,releases:ActiveRelease[]):void {
    this.sql.run(`INSERT INTO gonvex_cf_publication_catalogs(sync_id,catalog)
      SELECT sync_id,$3 FROM gonvex_cf_syncs WHERE sync_id=$1 AND rowid=$2 AND state='running'
      ON CONFLICT(sync_id) DO UPDATE SET catalog=excluded.catalog`,id,fence,JSON.stringify(releases));
    if(this.sql.value("SELECT changes()")!==1)throw new HttpError(409,"deployment fence is no longer current");
  }
  staged(id:string,fence:number,_version:string):void {
    this.sql.run("UPDATE gonvex_cf_syncs SET state='publishing' WHERE sync_id=$1 AND rowid=$2 AND state='running'",id,fence);
    if(this.sql.value("SELECT changes()")!==1)throw new HttpError(409,"deployment fence is no longer current");
  }
  reference(id:string):DeploymentRef {
    const row=this.sql.get("SELECT s.rowid AS fence,p.version FROM gonvex_cf_syncs s JOIN gonvex_cf_publications p ON p.sync_id=s.sync_id WHERE s.sync_id=$1",id);
    if(!row?.version)throw new HttpError(409,"publication has no deployment fence");
    return {id,fence:num(row.fence),version:str(row.version)};
  }
  current():DeploymentRef|null {
    const owner=this.owner();return owner?.version?{id:str(owner.sync_id),fence:num(owner.fence),version:str(owner.version)}:null;
  }
  assertCurrent(id:string,fence:number):void {
    if(this.sql.value("SELECT 1 FROM gonvex_cf_syncs WHERE sync_id=$1 AND rowid=$2 AND state IN ('running','publishing')",id,fence)===undefined)throw new HttpError(409,"deployment fence is no longer current");
  }
  complete(id:string,fence:number):void {
    this.sql.run(`UPDATE gonvex_cf_syncs SET state='finished',finished_at=${NOW_MS}
      WHERE sync_id=$1 AND rowid=$2 AND state='publishing'`,id,fence);
    if(this.sql.value("SELECT changes()")!==1)throw new HttpError(409,"deployment fence is no longer current");
    this.sql.run("DELETE FROM gonvex_cf_target_attempts WHERE sync_id=$1",id);
  }
  private publication(ref:DeploymentRef):Row {
    const row=this.sql.get(`SELECT s.rowid AS fence,s.state,p.version,p.completed FROM gonvex_cf_syncs s
      JOIN gonvex_cf_publications p ON p.sync_id=s.sync_id WHERE s.sync_id=$1`,ref.id);
    if(!row||num(row.fence)!==ref.fence||row.version!==ref.version)throw new HttpError(409,"deployment reference does not match its fence");
    return row;
  }
  /** Checked inside the builder queue immediately before a target write. */
  async authorize(ref:DeploymentRef):Promise<DeploymentGrant> {
    const initial=this.publication(ref);
    if(num(initial.completed))return {allowed:false,completed:true};
    if(initial.state!=="publishing")throw new HttpError(409,"deployment fence is no longer current");
    const serving=await this.versionReady(ref.version);
    const row=this.publication(ref);
    if(num(row.completed))return {allowed:false,completed:true};
    if(row.state!=='publishing')throw new HttpError(409,"deployment fence is no longer current");
    if(serving)return {allowed:false,serving:true};
    const catalog=this.sql.value<string>("SELECT catalog FROM gonvex_cf_publication_catalogs WHERE sync_id=$1",ref.id);
    if(catalog===undefined)throw new HttpError(409,"publication catalog is missing");
    // Persist before the builder can attempt either component. Re-authorization
    // after a reset cannot assume a previous target write never happened.
    const recovery=this.sql.value("SELECT 1 FROM gonvex_cf_target_attempts WHERE sync_id=$1",ref.id)!==undefined;
    this.sql.run(`INSERT INTO gonvex_cf_target_attempts(sync_id,uncertain) VALUES($1,0)
      ON CONFLICT(sync_id) DO UPDATE SET uncertain=1`,ref.id);
    return {allowed:true,catalog:JSON.parse(catalog) as ActiveRelease[],...(recovery?{recovery:true as const}:{})};
  }
  /** Caller/RPC failure alone never unlocks a target operation still running. */
  async settle(ref:DeploymentRef,result:{ok:boolean;error?:string;retain?:boolean}):Promise<void> {
    const initial=this.publication(ref);
    if(num(initial.completed)||initial.state==='failed')return;
    if(initial.state!=='publishing')throw new HttpError(409,"deployment fence is no longer current");
    const serving=result.ok?false:await this.versionReady(ref.version);
    const row=this.publication(ref);
    if(num(row.completed)||row.state==='failed')return;
    if(row.state!=='publishing')throw new HttpError(409,"deployment fence is no longer current");
    if(result.ok||serving){this.sql.run("DELETE FROM gonvex_cf_target_attempts WHERE sync_id=$1",ref.id);return;}
    if(result.retain||this.sql.value("SELECT uncertain FROM gonvex_cf_target_attempts WHERE sync_id=$1",ref.id)===1){
      this.sql.run("UPDATE gonvex_cf_target_attempts SET uncertain=1 WHERE sync_id=$1",ref.id);
      this.sql.run("UPDATE gonvex_cf_syncs SET error=$2 WHERE sync_id=$1",ref.id,(result.error??'component deployment uncertain').slice(0,1500));return;
    }
    this.sql.run("DELETE FROM gonvex_cf_target_attempts WHERE sync_id=$1",ref.id);
    await this.abort(ref,result.error??'deployment failed');
  }
  /**
   * Only invoked from the real builder's serialized target barrier. An uncertain component attempt normally
   * needs same-ID recovery. Recovery can be impossible: e.g. a later builder emits a different catalog format,
   * so the publication's exact version can never be reproduced. A caller may then abort it as `superseded`,
   * which is accepted only when neither runtime nor Actions serves that version.
   */
  async abort(ref:DeploymentRef,error:string,{superseded=false}:{superseded?:boolean}={}):Promise<void> {
    const uncertain=()=>this.sql.value("SELECT 1 FROM gonvex_cf_target_attempts WHERE sync_id=$1",ref.id)!==undefined;
    const absent=superseded&&uncertain()?await this.versionAbsent(ref.version):false;
    this.sql.tx(()=>{
      const row=this.publication(ref);
      if(num(row.completed)||row.state==='failed')return;
      if(row.state!=='publishing')throw new HttpError(409,"deployment fence is no longer current");
      if(uncertain()){
        if(!absent)throw new HttpError(409,"component deployment requires same-ID recovery before abort");
        this.sql.run("DELETE FROM gonvex_cf_target_attempts WHERE sync_id=$1",ref.id);
      }
      this.sql.run(`UPDATE gonvex_cf_syncs SET state='failed',error=$2,finished_at=${NOW_MS} WHERE sync_id=$1`,ref.id,error.slice(0,1500));
    });
  }
}
