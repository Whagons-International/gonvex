//! `pbkdf2(password_len, salt_len, rounds, out_len)` reads the password then
//! the salt from `buffer()` and writes the derived key at the start of the
//! same buffer. No allocation, no imports.
#![no_std]

use sha2::Sha256;

const CAPACITY: usize = 4096;
static mut BUFFER: [u8; CAPACITY] = [0; CAPACITY];

#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

#[no_mangle]
pub extern "C" fn buffer() -> *mut u8 {
    core::ptr::addr_of_mut!(BUFFER) as *mut u8
}

#[no_mangle]
pub extern "C" fn capacity() -> u32 {
    CAPACITY as u32
}

/// Returns 0 on success, 1 when the inputs do not fit the buffer.
#[no_mangle]
pub extern "C" fn pbkdf2(password_len: u32, salt_len: u32, rounds: u32, out_len: u32) -> u32 {
    let (password_len, salt_len, out_len) = (password_len as usize, salt_len as usize, out_len as usize);
    if rounds == 0 || out_len == 0 || password_len + salt_len > CAPACITY / 2 || out_len > CAPACITY / 2 {
        return 1;
    }
    // SAFETY: single-threaded wasm; the buffer is only touched through these exports.
    let buffer = unsafe { &mut *core::ptr::addr_of_mut!(BUFFER) };
    let (input, output) = buffer.split_at_mut(CAPACITY / 2);
    let (password, rest) = input.split_at(password_len);
    let salt = &rest[..salt_len];
    pbkdf2::pbkdf2_hmac::<Sha256>(password, salt, rounds, &mut output[..out_len]);
    buffer.copy_within(CAPACITY / 2..CAPACITY / 2 + out_len, 0);
    0
}
