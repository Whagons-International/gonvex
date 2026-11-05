import {createRequire} from 'node:module';
import {createInterface} from 'node:readline';
import fs from 'node:fs';import http from 'node:http';
const root=process.env.STARTER_BROWSER_ROOT, modulePath=process.env.STARTER_BROWSER_PLAYWRIGHT;
if(!root||!modulePath)throw new Error('explicit browser root and existing Playwright module required');
const {chromium}=createRequire(import.meta.url)(modulePath);
const lines=[],waiting=[],rl=createInterface({input:process.stdin});rl.on('line',s=>waiting.length?waiting.shift()(s):lines.push(s));
const server=http.createServer((req,res)=>{res.setHeader('Content-Type',req.url==='/live.mjs'?'text/javascript':'text/html');res.end(req.url==='/live.mjs'?fs.readFileSync(root+'/live.mjs'):'<!doctype html><title>Synthetic Rust starter fixture</title>');});
let browser;
try {
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 browser=await chromium.launch({args:['--disable-gpu'],executablePath:process.env.STARTER_BROWSER_EXECUTABLE,downloadsPath:root+'/downloads'});
 const context=await browser.newContext();const page=await context.newPage();
 await page.exposeFunction('fixtureEmit',s=>console.log(s));
 await page.exposeFunction('fixtureLine',()=>lines.length?Promise.resolve(lines.shift()):new Promise(r=>waiting.push(r)));
 await page.goto('http://127.0.0.1:'+server.address().port);
 await page.evaluate(async args=>{const {run}=await import('/live.mjs');await run(...args,globalThis.fixtureEmit,globalThis.fixtureLine);},process.argv.slice(2));
}finally{await browser?.close();rl.close();await new Promise(r=>server.close(r));}
