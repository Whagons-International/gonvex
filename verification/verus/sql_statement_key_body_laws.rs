// These functions call the extracted executable bodies, then inspect real bytes.
fn actual_execution_identity(s: &str, t: &str, a: &[u32], b: &[u32]) {
    let mut x = String::new();
    let mut y = String::new();
    append_execution(&mut x, s, a);
    append_execution(&mut y, t, b);
    let xb = x.as_str().as_bytes();
    let yb = y.as_str().as_bytes();
    assert(xb@ == vstd::utf8::encode_utf8(execution(s@, a@)));
    assert(yb@ == vstd::utf8::encode_utf8(execution(t@, b@)));
    proof {
        execution_identity(s@, t@, a@, b@);
        vstd::utf8::encode_utf8_decode_utf8(x@);
        vstd::utf8::encode_utf8_decode_utf8(y@);
    }
    assert((xb@ == yb@) <==> (s@ == t@ && a@ == b@));
}
fn actual_description_identity(s: &str, t: &str) {
    let mut x = String::new();
    let mut y = String::new();
    append_description(&mut x, s);
    append_description(&mut y, t);
    let xb = x.as_str().as_bytes();
    let yb = y.as_str().as_bytes();
    assert(xb@ == vstd::utf8::encode_utf8(description_spec(s@)));
    proof {
        description_identity(s@, t@);
        vstd::utf8::encode_utf8_decode_utf8(x@);
        vstd::utf8::encode_utf8_decode_utf8(y@);
    }
    assert((xb@ == yb@) <==> (s@ == t@));
}
fn actual_namespace_separation(s: &str, t: &str, a: &[u32]) {
    let mut x = String::new();
    let mut y = String::new();
    append_execution(&mut x, s, a);
    append_description(&mut y, t);
    let xb = x.as_str().as_bytes();
    let yb = y.as_str().as_bytes();
    proof {
        namespace_separation(s@, t@, a@);
        vstd::utf8::encode_utf8_decode_utf8(x@);
        vstd::utf8::encode_utf8_decode_utf8(y@);
    }
    assert(xb@ != yb@);
}
// Empty, zero, maximum, duplicate and order witnesses supplement universal laws.
fn actual_boundaries(sql: &str) {
    let a = [0u32, u32::MAX, 0u32];
    let b = [u32::MAX, 0u32, 0u32];
    actual_execution_identity(sql, sql, &a, &b);
    let empty: [u32; 0] = [];
    actual_namespace_separation(sql, sql, &empty);
    actual_execution_identity(sql, sql, &empty, &a);
}
