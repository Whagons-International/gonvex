//! `k20-ingest <target.duckdb> <filename>`: reads the uploaded bytes on stdin,
//! runs Rust's `data_ingest::ingest` unchanged, and prints the imported tables
//! as JSON (`[{tableName,rowCount,columns}]`). On failure it prints Rust's error
//! text on stderr and exits 2. The sandbox manager (../manager.mjs) calls it for
//! `importFile`, as `SandboxManager::import_file` calls `ingest` in-process.
use std::io::Read;
use std::path::Path;

#[path = "../../../../../rust/crates/runtime/src/data_ingest.rs"]
mod data_ingest;

fn main() {
    let mut args = std::env::args().skip(1);
    let (Some(target), Some(filename)) = (args.next(), args.next()) else {
        eprintln!("usage: k20-ingest <target.duckdb> <filename>");
        std::process::exit(64);
    };
    let mut bytes = Vec::new();
    if let Err(error) = std::io::stdin().read_to_end(&mut bytes) {
        eprint!("{error}");
        std::process::exit(2);
    }
    match data_ingest::ingest(&bytes, &filename, Path::new(&target)) {
        Ok(tables) => println!("{}", serde_json::to_string(&tables).expect("tables serialize")),
        Err(error) => {
            eprint!("{error}");
            std::process::exit(2);
        }
    }
}
