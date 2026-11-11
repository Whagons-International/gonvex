declare module "*.sql" { const text: string; export default text; }
declare module "*.wasm" {
  const module: WebAssembly.Module;
  export default module;
}
