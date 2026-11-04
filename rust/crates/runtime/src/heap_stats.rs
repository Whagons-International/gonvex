//! Live Rust heap bytes and allocator state (`/dev/metrics` `memory.allocator`).
//!
//! Staging (2026-11-04) showed ~955 MiB of anonymous memory that no owner in
//! `/dev/metrics` explained, most of it resident in one mimalloc arena. RSS
//! alone cannot say whether that is live data (a leak or an unaccounted
//! owner) or memory the allocator freed but kept (retention). [`Counting`]
//! wraps the global allocator and keeps the bytes currently allocated through
//! it, in 16 cache-line-padded shards picked per thread (one relaxed add per
//! allocation and free, no shared cache line between busy threads). The
//! binary registers its allocator's own view (mimalloc commit and RSS,
//! jemalloc allocated/resident) with [`set_allocator_info`].

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicIsize, AtomicU64, Ordering::Relaxed};
use std::sync::OnceLock;

use serde_json::{json, Value};

#[repr(align(128))]
struct Shard {
    live: AtomicIsize,
    allocations: AtomicU64,
}

const SHARDS: usize = 16;

static LIVE: [Shard; SHARDS] = [const {
    Shard {
        live: AtomicIsize::new(0),
        allocations: AtomicU64::new(0),
    }
}; SHARDS];

#[inline]
fn shard() -> &'static Shard {
    thread_local! {
        // const-initialized, no destructor: usable while a thread exits and
        // never allocates.
        static ANCHOR: u8 = const { 0 };
    }
    let address = ANCHOR.with(|anchor| anchor as *const u8 as usize);
    // Thread-local blocks are page-aligned per thread: mix the high bits.
    let mixed = (address >> 12) ^ (address >> 20) ^ (address >> 28);
    &LIVE[mixed % SHARDS]
}

#[inline]
fn add(bytes: isize) {
    shard().live.fetch_add(bytes, Relaxed);
}

/// Bytes currently allocated through [`Counting`] (requested sizes).
pub fn live_bytes() -> i64 {
    LIVE.iter()
        .map(|shard| shard.live.load(Relaxed) as i64)
        .sum()
}

/// Allocations made through [`Counting`] since start (reallocations count).
pub fn allocations() -> u64 {
    LIVE.iter()
        .map(|shard| shard.allocations.load(Relaxed))
        .sum()
}

/// A global allocator that counts the live bytes of `A`.
pub struct Counting<A>(pub A);

unsafe impl<A: GlobalAlloc> GlobalAlloc for Counting<A> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged.
        let pointer = unsafe { self.0.alloc(layout) };
        if !pointer.is_null() {
            let shard = shard();
            shard.live.fetch_add(layout.size() as isize, Relaxed);
            shard.allocations.fetch_add(1, Relaxed);
        }
        pointer
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged.
        let pointer = unsafe { self.0.alloc_zeroed(layout) };
        if !pointer.is_null() {
            let shard = shard();
            shard.live.fetch_add(layout.size() as isize, Relaxed);
            shard.allocations.fetch_add(1, Relaxed);
        }
        pointer
    }

    #[inline]
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged.
        unsafe { self.0.dealloc(pointer, layout) };
        add(-(layout.size() as isize));
    }

    #[inline]
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded unchanged.
        let moved = unsafe { self.0.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            let shard = shard();
            shard
                .live
                .fetch_add(new_size as isize - layout.size() as isize, Relaxed);
            shard.allocations.fetch_add(1, Relaxed);
        }
        moved
    }
}

type Info = fn() -> Value;
static INFO: OnceLock<(&'static str, Info)> = OnceLock::new();

/// The binary's allocator name and a function reporting its own state.
pub fn set_allocator_info(name: &'static str, info: Info) {
    let _ = INFO.set((name, info));
}

/// `/dev/metrics` `memory.allocator`.
pub fn json() -> Value {
    let (name, info) = INFO
        .get()
        .map_or(("unknown", Value::Null), |(name, info)| (*name, info()));
    json!({
        "kind": name,
        "liveBytes": live_bytes(),
        "allocations": allocations(),
        "state": info,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_live_bytes_through_alloc_realloc_and_free() {
        let counting = Counting(std::alloc::System);
        let before = live_bytes();
        unsafe {
            let layout = Layout::from_size_align(1000, 8).unwrap();
            let pointer = counting.alloc(layout);
            assert_eq!(live_bytes() - before, 1000);
            let grown = counting.realloc(pointer, layout, 5000);
            assert_eq!(live_bytes() - before, 5000);
            counting.dealloc(grown, Layout::from_size_align(5000, 8).unwrap());
        }
        assert_eq!(live_bytes(), before);
    }
}
