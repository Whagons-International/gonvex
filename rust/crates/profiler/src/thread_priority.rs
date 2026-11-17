//! Per-thread CPU priority (nice) that does not depend on who created the thread.
//!
//! On Linux a new thread inherits its creator's nice value, and `setpriority`
//! on a thread id sets that thread only. Two consequences the runtime must
//! handle:
//!
//! * A niced pool that sets "current + n" on every thread start stacks when a
//!   thread of the pool starts another (tokio blocking threads of the offload
//!   runtime came up at 19 instead of 10). Use [`set_current_target`]: an
//!   absolute target, so applying it twice is harmless.
//! * A thread that must run at the process's normal priority (writer
//!   connection workers, the checkpoint thread, module isolates, network
//!   workers) inherits a niced creator's value, and without CAP_SYS_NICE (the
//!   default in containers) it can never lower it again. Create such threads
//!   with [`spawn_at`]: a spawner thread started from `main` (nice = base)
//!   creates them, so they start at the base value whoever asked.
//!
//! Targets are relative to the process base, the main thread's nice value
//! (0 in production; a whole process run under `systemd Nice=10` keeps its
//! relative ordering): target = clamp(base + offset, base, 19).
//!
//! Non-Linux targets: every call is a no-op and [`current`] returns 0.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Mutex, OnceLock};

/// Raises that failed (EACCES/EPERM) because the thread was already above its
/// target and lowering needs CAP_SYS_NICE.
static RESET_FAILURES: AtomicU64 = AtomicU64::new(0);

#[cfg(target_os = "linux")]
mod sys {
    extern "C" {
        fn getpriority(which: i32, who: u32) -> i32;
        fn setpriority(which: i32, who: u32, prio: i32) -> i32;
        fn gettid() -> i32;
        fn getpid() -> i32;
    }
    const PRIO_PROCESS: i32 = 0;
    pub fn tid() -> u32 {
        // SAFETY: plain syscall wrapper.
        unsafe { gettid() as u32 }
    }
    pub fn pid() -> u32 {
        // SAFETY: plain syscall wrapper.
        unsafe { getpid() as u32 }
    }
    pub fn get(who: u32) -> i32 {
        // SAFETY: plain syscall wrapper; nice values here are never -1 in practice.
        unsafe { getpriority(PRIO_PROCESS, who) }
    }
    pub fn set(who: u32, value: i32) -> bool {
        // SAFETY: plain syscall wrapper on a thread id.
        unsafe { setpriority(PRIO_PROCESS, who, value) == 0 }
    }
}

/// The calling thread's nice value.
pub fn current() -> i32 {
    #[cfg(target_os = "linux")]
    {
        sys::get(sys::tid())
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// The process base: the main thread's nice value (its tid is the pid),
/// remembered on first use (call [`init`] from `main`).
pub fn base() -> i32 {
    static BASE: OnceLock<i32> = OnceLock::new();
    *BASE.get_or_init(|| {
        #[cfg(target_os = "linux")]
        {
            sys::get(sys::pid())
        }
        #[cfg(not(target_os = "linux"))]
        {
            0
        }
    })
}

/// base + offset, clamped to [base, 19].
pub fn target(offset: i32) -> i32 {
    let base = base();
    (base + offset.max(0)).clamp(base, 19)
}

/// Sets the calling thread to `target(offset)` (absolute, idempotent).
/// Returns false when the thread stays above the target (lowering a nice
/// value needs CAP_SYS_NICE); such failures are counted.
pub fn set_current_target(offset: i32) -> bool {
    #[cfg(target_os = "linux")]
    {
        let wanted = target(offset);
        let tid = sys::tid();
        if sys::get(tid) == wanted {
            return true;
        }
        if sys::set(tid, wanted) {
            return true;
        }
        RESET_FAILURES.fetch_add(1, Ordering::Relaxed);
        false
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = offset;
        true
    }
}

/// Threads that could not reach their target (see [`set_current_target`]).
pub fn reset_failures() -> u64 {
    RESET_FAILURES.load(Ordering::Relaxed)
}

type Spawn = Box<dyn FnOnce() + Send + 'static>;

fn spawner() -> &'static Mutex<mpsc::Sender<Spawn>> {
    static SPAWNER: OnceLock<Mutex<mpsc::Sender<Spawn>>> = OnceLock::new();
    SPAWNER.get_or_init(|| {
        let _ = base();
        let (sender, receiver) = mpsc::channel::<Spawn>();
        std::thread::Builder::new()
            .name("gonvex-spawner".to_owned())
            .spawn(move || {
                // Started from `main` (see `init`): already at base.
                set_current_target(0);
                for request in receiver {
                    request();
                }
            })
            .expect("spawn the thread spawner");
        Mutex::new(sender)
    })
}

/// Records the process base and starts the spawner thread. Call first thing
/// in `main`, on the main thread, before any thread is niced.
pub fn init() {
    let _ = base();
    let _ = spawner();
}

/// Spawns a thread named `name` that runs `f` at `target(offset)`, created by
/// the base-priority spawner thread so it does not inherit its caller's nice
/// value. Errors if the spawner thread is gone.
pub fn spawn_at<F, T>(
    name: impl Into<String>,
    offset: i32,
    f: F,
) -> std::io::Result<std::thread::JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let name = name.into();
    let (reply, handle) = mpsc::sync_channel(1);
    let request_name = name.clone();
    let request: Spawn = Box::new(move || {
        let spawned = std::thread::Builder::new()
            .name(request_name)
            .spawn(move || {
                set_current_target(offset);
                f()
            });
        let _ = reply.send(spawned);
    });
    let sent = spawner()
        .lock()
        .map(|sender| sender.send(request).is_ok())
        .unwrap_or(false);
    if sent {
        if let Ok(spawned) = handle.recv() {
            return spawned;
        }
    }
    Err(std::io::Error::other(format!(
        "thread spawner unavailable for {name}"
    )))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    fn nice_of_new_thread<F: FnOnce() -> std::thread::JoinHandle<i32> + Send + 'static>(
        from_offset: i32,
        spawn: F,
    ) -> i32 {
        std::thread::spawn(move || {
            assert!(set_current_target(from_offset));
            spawn().join().unwrap()
        })
        .join()
        .unwrap()
    }

    #[test]
    fn nice_targets_are_absolute_and_spawn_at_ignores_the_creator() {
        init();
        let base = base();
        if base > 14 {
            eprintln!("process base nice {base}: no room to test");
            return;
        }
        // Inheritance: a plain thread from a niced (base+5) thread runs at base+5.
        let inherited = nice_of_new_thread(5, || std::thread::spawn(current));
        assert_eq!(inherited, base + 5);
        // Absolute target: applying base+5 again on an inherited base+5 thread stays base+5
        // (the old relative "current + n" gave base+10).
        let twice = nice_of_new_thread(5, || {
            std::thread::spawn(|| {
                set_current_target(5);
                current()
            })
        });
        assert_eq!(twice, base + 5);
        // spawn_at from a niced creator: base for unniced threads, base+3 when asked.
        let unniced = nice_of_new_thread(5, || spawn_at("t-unniced", 0, current).unwrap());
        assert_eq!(unniced, base);
        let niced = nice_of_new_thread(5, || spawn_at("t-niced", 3, current).unwrap());
        assert_eq!(niced, base + 3);
        // A thread above its target cannot lower itself without CAP_SYS_NICE; it says so.
        let lowered = std::thread::spawn(move || {
            set_current_target(5);
            (set_current_target(0), current())
        })
        .join()
        .unwrap();
        assert!(
            (lowered.0 && lowered.1 == base) || (!lowered.0 && lowered.1 == base + 5),
            "a reset either works (CAP_SYS_NICE) or reports false: {lowered:?}"
        );
    }
}
