//! Isolated experiment. None of these codecs are production dependencies.
use axum::serve::ListenerExt;
use axum::{
    extract::{Request, State},
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use flate2::{Compress, Compression, FlushCompress};
use serde_json::{json, Value};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    io::Write,
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc,
    },
    time::Instant,
};

struct Allocator;
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            ALLOCATIONS.fetch_add(1, Relaxed);
            let n = LIVE.fetch_add(layout.size(), Relaxed) + layout.size();
            PEAK.fetch_max(n, Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Relaxed);
        System.dealloc(ptr, layout);
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

fn deflate(input: &[u8], level: u32) -> Vec<u8> {
    let mut codec = Compress::new_with_window_bits(Compression::new(level), false, 12);
    let mut output = Vec::with_capacity(input.len() / 4);
    let mut scratch = [0; 8192];
    loop {
        let before = codec.total_out();
        codec
            .compress(
                &input[codec.total_in() as usize..],
                &mut scratch,
                FlushCompress::Sync,
            )
            .unwrap();
        let written = (codec.total_out() - before) as usize;
        output.extend_from_slice(&scratch[..written]);
        if codec.total_in() as usize == input.len() && written < scratch.len() {
            break;
        }
    }
    assert!(output.ends_with(&[0, 0, 255, 255]));
    output.truncate(output.len() - 4);
    output
}
fn encode(value: &Value, codec: &str) -> Vec<u8> {
    match codec {
        "json" => serde_json::to_vec(value).unwrap(),
        "msgpack" => rmp_serde::to_vec_named(value).unwrap(),
        _ => panic!("codec"),
    }
}
fn compress(bytes: &[u8], method: &str, dictionary: &[u8]) -> Vec<u8> {
    match method {
        "none" => bytes.to_vec(),
        "zstd3" => zstd::bulk::compress(bytes, 3).unwrap(),
        "zstd3-dict" => zstd::bulk::Compressor::with_dictionary(3, dictionary)
            .unwrap()
            .compress(bytes)
            .unwrap(),
        "brotli4" => {
            let mut output = Vec::new();
            {
                let mut writer = brotli::CompressorWriter::new(&mut output, 8192, 4, 22);
                writer.write_all(bytes).unwrap();
            }
            output
        }
        method => {
            let parts: Vec<_> = method.split('-').collect();
            let level = parts[0].trim_start_matches("deflate").parse().unwrap();
            let threshold: usize = parts[1].parse().unwrap();
            if bytes.len() < threshold {
                return bytes.to_vec();
            }
            let compressed = deflate(bytes, level);
            if compressed.len() < bytes.len() {
                compressed
            } else {
                bytes.to_vec()
            }
        }
    }
}
fn stats(mut samples: Vec<f64>) -> Value {
    samples.sort_by(f64::total_cmp);
    let p = |percent: f64| {
        samples[((samples.len() as f64 * percent).ceil() as usize).saturating_sub(1)]
    };
    json!({"samples":samples.len(),"p50":p(0.5),"p95":p(0.95),"p99":p(0.99),"mean":samples.iter().sum::<f64>()/samples.len() as f64})
}
fn measure(mut run: impl FnMut(), samples: usize) -> Value {
    for _ in 0..5 {
        run();
    }
    let mut times = Vec::new();
    let mut counts = Vec::new();
    let mut peaks = Vec::new();
    for _ in 0..samples {
        let count = ALLOCATIONS.load(Relaxed);
        let live = LIVE.load(Relaxed);
        PEAK.store(live, Relaxed);
        let start = Instant::now();
        run();
        let micros = start.elapsed().as_secs_f64() * 1e6;
        counts.push((ALLOCATIONS.load(Relaxed) - count) as f64);
        peaks.push(PEAK.load(Relaxed).saturating_sub(live) as f64);
        times.push(micros);
    }
    json!({"us":stats(times),"allocations":stats(counts),"peakAdditionalBytes":stats(peaks)})
}
fn matrix(source: &Value, output: &str) {
    let training: Vec<Vec<u8>> = source["training"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| serde_json::to_vec(v).unwrap())
        .collect();
    let dictionary = zstd::dict::from_samples(&training, 4096).unwrap();
    let methods = [
        "none",
        "deflate1-1024",
        "deflate1-256",
        "deflate1-4096",
        "deflate3-1024",
        "deflate6-1024",
        "zstd3",
        "zstd3-dict",
        "brotli4",
    ];
    let mut results = Vec::new();
    for fixture in source["fixtures"].as_array().unwrap() {
        for (variant, frames) in fixture["variants"].as_object().unwrap() {
            for codec in ["json", "msgpack"] {
                let frames = frames.as_array().unwrap();
                let bytes: Vec<_> = frames.iter().map(|f| encode(f, codec)).collect();
                let raw: usize = bytes.iter().map(Vec::len).sum();
                let samples = if raw > 1_000_000 {
                    8
                } else if raw > 100_000 {
                    30
                } else {
                    100
                };
                let encoding = measure(
                    || {
                        for frame in frames {
                            black_box(encode(frame, codec));
                        }
                    },
                    samples,
                );
                let decoding = measure(
                    || {
                        for bytes in &bytes {
                            let value: Value = if codec == "json" {
                                serde_json::from_slice(bytes).unwrap()
                            } else {
                                rmp_serde::from_slice(bytes).unwrap()
                            };
                            black_box(value);
                        }
                    },
                    samples,
                );
                for method in methods {
                    let compressed: Vec<_> = bytes
                        .iter()
                        .map(|b| compress(b, method, &dictionary))
                        .collect();
                    let payload: usize = compressed.iter().map(Vec::len).sum();
                    let wire: usize = compressed
                        .iter()
                        .map(|b| {
                            b.len()
                                + if b.len() < 126 {
                                    2
                                } else if b.len() < 65536 {
                                    4
                                } else {
                                    10
                                }
                        })
                        .sum();
                    let compression = measure(
                        || {
                            for b in &bytes {
                                black_box(compress(b, method, &dictionary));
                            }
                        },
                        samples,
                    );
                    results.push(json!({"workload":fixture["name"],"variant":variant,"codec":codec,"compression":method,"frames":frames.len(),"rawBytes":raw,"compressedPayloadBytes":payload,"serverWebSocketBytes":wire,"encode":encoding,"rustDecode":decoding,"compress":compression}));
                }
            }
        }
        eprintln!("matrix {} complete", fixture["name"]);
    }
    std::fs::write(output, serde_json::to_vec_pretty(&json!({"allocator":"counting System allocator, release build; includes codec state","matrix":results})).unwrap()).unwrap();
}

struct Bench {
    source: Value,
    javascript: String,
}

fn boundaries(source: &Value, output: &str) {
    let mut results = Vec::new();
    for fixture in source["fixtures"].as_array().unwrap() {
        let frames = fixture["variants"]["baseline"].as_array().unwrap();
        if frames[0]["type"] != "replica.transaction" {
            continue;
        }
        let transaction: gonvex_protocol::ServerMessage =
            serde_json::from_value(frames[0].clone()).unwrap();
        let legacy = serde_json::to_vec(&transaction).unwrap();
        let naive =
            gonvex_protocol::encode_compact_replica_transaction(&mut transaction.clone(), None)
                .unwrap()
                .unwrap()
                .into_bytes();
        let guarded = gonvex_protocol::encode_compact_replica_transaction(
            &mut transaction.clone(),
            Some(1024),
        )
        .unwrap()
        .unwrap()
        .into_bytes();
        let size = |first: &[u8]| {
            std::iter::once(first.to_vec())
                .chain(frames[1..].iter().map(|f| encode(f, "json")))
                .map(|bytes| {
                    let c = compress(&bytes, "deflate1-1024", &[]);
                    c.len()
                        + if c.len() < 126 {
                            2
                        } else if c.len() < 65536 {
                            4
                        } else {
                            10
                        }
                })
                .sum::<usize>()
        };
        let mut times = Vec::new();
        let mut legacy_times = Vec::new();
        for i in 0..105 {
            // Runtime already owns the parsed transaction. Fixture preparation
            // is outside timing; measure moves, guard and encoding together.
            let mut input = transaction.clone();
            let start = Instant::now();
            let encoded =
                gonvex_protocol::encode_compact_replica_transaction(&mut input, Some(1024))
                    .unwrap();
            let elapsed = start.elapsed().as_secs_f64() * 1e6;
            black_box(encoded);
            if i >= 5 {
                times.push(elapsed);
            }
            let input = transaction.clone();
            let start = Instant::now();
            black_box(serde_json::to_string(&input).unwrap());
            if i >= 5 {
                legacy_times.push(start.elapsed().as_secs_f64() * 1e6);
            }
        }
        results.push(json!({"workload":fixture["name"],"legacyWireBytes":size(&legacy),"naiveWireBytes":size(&naive),"guardedWireBytes":size(&guarded),"legacyEncodeUs":stats(legacy_times),"guardedEncodeUs":stats(times)}));
    }
    std::fs::write(output, serde_json::to_vec_pretty(&results).unwrap()).unwrap();
}
async fn websocket(State(state): State<Arc<Bench>>, request: Request) -> Response {
    let pmd = request.uri().query() == Some("pmd=1");
    gonvex_websocket::upgrade(
        request,
        pmd,
        1024,
        64 * 1024 * 1024,
        64 * 1024 * 1024,
        move |mut socket| async move {
            while let Some(Ok(axum::extract::ws::Message::Text(text))) = socket.next().await {
                let request: Value = serde_json::from_str(&text).unwrap();
                let fixture = state.source["fixtures"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|v| v["name"] == request["workload"])
                    .unwrap();
                let frames = fixture["variants"][request["variant"].as_str().unwrap()]
                    .as_array()
                    .unwrap();
                let codec = request["codec"].as_str().unwrap();
                let compression = request["compression"].as_str().unwrap();
                let mut encode_us = 0.0;
                let mut send_us = 0.0;
                for frame in frames {
                    let start = Instant::now();
                    let bytes = encode(frame, codec);
                    encode_us += start.elapsed().as_secs_f64() * 1e6;
                    let start = Instant::now();
                    let message = if compression == "zstd3" || compression == "brotli4" {
                        axum::extract::ws::Message::Binary(
                            compress(&bytes, compression, &[]).into(),
                        )
                    } else if codec == "json" {
                        axum::extract::ws::Message::Text(String::from_utf8(bytes).unwrap().into())
                    } else {
                        axum::extract::ws::Message::Binary(bytes.into())
                    };
                    if socket.send(message).await.is_err() {
                        return;
                    }
                    send_us += start.elapsed().as_secs_f64() * 1e6;
                }
                let trailer =
                    json!({"type":"bench.done","encodeUs":encode_us,"compressSendUs":send_us})
                        .to_string();
                if socket
                    .send(axum::extract::ws::Message::Text(trailer.into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        },
    )
    .await
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<_> = std::env::args().collect();
    let source: Value = serde_json::from_slice(&std::fs::read(&args[2]).unwrap()).unwrap();
    if args[1] == "matrix" {
        matrix(&source, &args[3]);
        return;
    }
    if args[1] == "boundaries" {
        boundaries(&source, &args[3]);
        return;
    }
    assert_eq!(args[1], "serve");
    let state = Arc::new(Bench {
        source,
        javascript: std::fs::read_to_string(&args[3]).unwrap(),
    });
    let app = Router::new().route("/ws", get(websocket))
        .route("/contract.msgpack", get(|| async {
            encode(&json!({"signed":i64::MIN,"unsigned":u64::MAX,"unsafeInteger":9007199254740993u64,"nullable":null,"type":"contract"}), "msgpack")
        }))
        .route("/benchmark.js", get(|State(s): State<Arc<Bench>>| async move { ([("content-type", "application/javascript")], s.javascript.clone()).into_response() }))
        .route("/fixtures.json", get(|State(s): State<Arc<Bench>>| async move { axum::Json(s.source.clone()) }))
        .route("/", get(|| async { ([("cross-origin-opener-policy", "same-origin"), ("cross-origin-embedder-policy", "require-corp")], Html("<!doctype html><title>Gonvex protocol benchmark</title><script type=module src=/benchmark.js></script>")) })).with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    println!("http://{}", listener.local_addr().unwrap());
    let nodelay = std::env::var("GONVEX_BENCH_NODELAY").as_deref() == Ok("1");
    axum::serve(
        listener.tap_io(move |stream| {
            stream.set_nodelay(nodelay).unwrap();
        }),
        app,
    )
    .await
    .unwrap();
}
