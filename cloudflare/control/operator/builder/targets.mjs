import {mkdir,writeFile,rename} from 'node:fs/promises';
import {join,dirname,relative} from 'node:path';
import {RELEASES_ENTRY,ACTION_RELEASES_ENTRY} from './core.ts';
import {servingConfigs} from '../../../kernel/assembly/deploy.mjs';
const log=()=>{};
export class LocalTarget {
  constructor(dir, deferMs, stagingDir = dir) {
    this.dir = dir;
    this.stagingDir = stagingDir;
    this.deferMs = deferMs;
    this.staged = new Map();
  }
  async stage(files, version) {
    for (const [path, content] of files) {
      if (path === RELEASES_ENTRY || path === ACTION_RELEASES_ENTRY) continue;
      const file = join(this.stagingDir, path);
      await mkdir(dirname(file), { recursive: true });
      await writeFile(file, content);
    }
    // The entry imports `./releases/...`; point it at the staging directory.
    const base = relative(this.dir, this.stagingDir).split('\\').join('/') || '.';
    const prefix = base.startsWith('.') ? base : `./${base}`;
    const rebase = (text) => text?.replaceAll('from "./releases/', `from "${prefix}/releases/`).replaceAll('require("./releases/', `require("${prefix}/releases/`);
    const entry = rebase(files.get(RELEASES_ENTRY));
    const actions = rebase(files.get(ACTION_RELEASES_ENTRY));
    this.staged.set(version, { kernel: entry, actions });
  }
  async deploy(version) {
    const entry = this.staged.get(version);
    if (entry === undefined) throw new Error(`version ${version} was not staged`);
    // workerd restarts on the swap and cuts every request in flight, including
    // the /dev/sync response; swap after it has gone out.
    await new Promise(resolveSwap => setTimeout(resolveSwap,this.deferMs));
    const file = join(this.dir, RELEASES_ENTRY);
    if (entry.actions) {
      const actions = join(this.dir, ACTION_RELEASES_ENTRY);
      await writeFile(`${actions}.tmp`, entry.actions);
      await rename(`${actions}.tmp`, actions);
    }
    await writeFile(`${file}.tmp`, entry.kernel);
    await rename(`${file}.tmp`, file);
    log(`version ${version} is live (workerd reloads)`);
  }
}


/**
 * The release source label (a full commit SHA). The builder deploys runtime and Actions from its own
 * baked source, so an app publication must keep that label. If it is not a SHA, the config omits the
 * var and `--keep-vars` keeps the deployed label. The committed `development` default never overwrites it.
 */
export function releaseSourceLabel(value){return /^[0-9a-f]{40}$/.test(value??'')?value:null;}

/** Only the socket-free app runtime and Actions are application deployment targets. */
export class WranglerTarget extends LocalTarget {
 constructor(dir,script,deploy,source){super(dir,0);this.script=script;this.execute=deploy;this.source=releaseSourceLabel(source);}
 async deploy(version){
  const entry=this.staged.get(version);if(!entry)throw new Error(`version ${version} was not staged`);
  if(!/^gonvex-cf-dev-[a-z0-9-]+$/.test(this.script))throw new Error('development script name required');
  await writeFile(join(this.dir,RELEASES_ENTRY),entry.kernel);
  if(entry.actions)await writeFile(join(this.dir,ACTION_RELEASES_ENTRY),entry.actions);
  const configs=await servingConfigs(this.script,this.dir);
  if(configs.runtime.config.durable_objects.code_update_strategy?.mode!=='immediate'||Object.keys(configs.runtime.config.exports).join(',')!=='AssemblyTenant')throw new Error('runtime isolation configuration differs');
  const targets=[];
  for(const[kind,alias,file]of[['actions','gonvex:action-releases',ACTION_RELEASES_ENTRY],['runtime','gonvex:releases',RELEASES_ENTRY]]){
   const {config,path}=configs[kind];config.alias[alias]=join(this.dir,file);
   if(this.source)config.vars={...config.vars,GONVEX_RUNTIME_VERSION:this.source};else if(config.vars)delete config.vars.GONVEX_RUNTIME_VERSION;
   await writeFile(path,JSON.stringify(config),{mode:0o600});
   targets.push({path,name:config.name});
  }
  for(const {path,name} of targets){
   try{await this.execute(path,name);}
   catch(cause){throw Object.assign(new Error(`component deployment failed: ${cause.message}`),{retainPublication:true});}
  }
 }
}
