// Standalone qualification harness linked to the already built, exact runtime
// websocket crate. No new SDK dependency. Output flag 0 means uncompressed.
use std::io::{Read, Write};
fn main() {
    let mut raw=Vec::new();std::io::stdin().read_to_end(&mut raw).unwrap();
    assert!(raw.len()<=256*1024);
    let encoded=gonvex_websocket::bench_compress_text(&raw,12).unwrap();
    let mut out=std::io::stdout();
    match encoded {Some(bytes)=>{out.write_all(&[1]).unwrap();out.write_all(&bytes).unwrap();},None=>{out.write_all(&[0]).unwrap();out.write_all(&raw).unwrap();}}
}
