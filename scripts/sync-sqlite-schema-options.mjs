import{readFileSync,writeFileSync}from'node:fs';
const sql=readFileSync(new URL('../rust/crates/store/src/schema_options.sql',import.meta.url),'utf8');
const file=new URL('../packages/local-runtime/src/sqlite-schema-options.ts',import.meta.url);
const source=`// Generated from rust/crates/store/src/schema_options.sql. Do not edit.\nexport const sqliteSchemaBootstrap=${JSON.stringify(sql)};\n`;
if(process.argv.includes('--check')){if(readFileSync(file,'utf8')!==source)throw new Error('Run node scripts/sync-sqlite-schema-options.mjs');}else writeFileSync(file,source);
