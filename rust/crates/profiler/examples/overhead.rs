//! Microbenchmark of the instrumentation itself.
//!
//! cargo run --release -p gonvex-profiler --example overhead
//!
//! Prints ns per operation for: a disabled record, an enabled record (value
//! already measured), a `Timer::lap` (clock read + record), an instrumented
//! poll, and contended records from 1..N threads hitting the same stage/label,
//! plus the CPU cost at 50k stage boundaries per second.
use std::future::Future;
use std::hint::black_box;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use gonvex_profiler::{instrument, record, set_enabled, Label, Stage, TaskKind, Timer};

fn per_op(iterations: u64, f: impl FnOnce()) -> f64 {
    let start = Instant::now();
    f();
    start.elapsed().as_nanos() as f64 / iterations as f64
}

struct Ready;
impl Future for Ready {
    type Output = u64;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<u64> {
        Poll::Ready(black_box(1))
    }
}

fn main() {
    const N: u64 = 20_000_000;
    let clock = per_op(N, || {
        for _ in 0..N {
            black_box(gonvex_profiler::now_ns());
        }
    });
    set_enabled(false);
    let disabled = per_op(N, || {
        for i in 0..N {
            record(Stage::Sql, Label::Heartbeat, black_box(i));
        }
    });
    let disabled_lap = per_op(N, || {
        let mut timer = Timer::start();
        for _ in 0..N {
            timer.lap(Stage::Sql, Label::Heartbeat);
        }
    });
    set_enabled(true);
    let enabled = per_op(N, || {
        for i in 0..N {
            record(Stage::Sql, Label::Heartbeat, black_box(i * 37 % 10_000_000));
        }
    });
    let lap = per_op(N, || {
        let mut timer = Timer::start();
        for _ in 0..N {
            timer.lap(Stage::Sql, Label::Heartbeat);
        }
    });
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    let bare_poll = per_op(N, || {
        for _ in 0..N {
            let mut future = std::pin::pin!(Ready);
            black_box(future.as_mut().poll(&mut context));
        }
    });
    let instrumented_poll = per_op(N, || {
        for _ in 0..N {
            let mut future = std::pin::pin!(instrument(TaskKind::Other, Ready));
            black_box(future.as_mut().poll(&mut context));
        }
    });
    println!("clock read (now_ns)              {clock:>7.1} ns/op");
    println!("record, profiler disabled        {disabled:>7.1} ns/op");
    println!("Timer::lap, profiler disabled    {disabled_lap:>7.1} ns/op");
    println!("record (value given)             {enabled:>7.1} ns/op");
    println!("Timer::lap (clock + record)      {lap:>7.1} ns/op");
    println!(
        "instrumented poll overhead       {:>7.1} ns/op  (bare {bare_poll:.1}, wrapped {instrumented_poll:.1})",
        instrumented_poll - bare_poll
    );
    let threads_max = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(16);
    let mut threads = 1;
    while threads <= threads_max {
        const M: u64 = 5_000_000;
        let start = Instant::now();
        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| {
                    let mut timer = Timer::start();
                    for _ in 0..M {
                        timer.lap(Stage::WsWrite, Label::None);
                    }
                });
            }
        });
        let per = start.elapsed().as_nanos() as f64 / M as f64;
        println!(
            "contended Timer::lap, {threads:>2} threads  {per:>7.1} ns/op per thread (same stage)"
        );
        threads *= 2;
    }
    let rate = 50_000.0;
    println!(
        "CPU at 50k boundaries/s: {:.4}% of one core (lap), {:.4}% for 50k instrumented polls/s",
        lap * rate / 1e9 * 100.0,
        (instrumented_poll - bare_poll).max(0.0) * rate / 1e9 * 100.0
    );
}
