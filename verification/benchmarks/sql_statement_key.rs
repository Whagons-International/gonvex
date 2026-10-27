// Linux native helper benchmark. Compile through sql_statement_key.py.
mod original {
    include!(env!("GONVEX_SQL_KEY_BEFORE"));
}
mod candidate {
    include!(env!("GONVEX_SQL_KEY_AFTER"));
}
use std::hint::black_box;
use std::time::Instant;
#[repr(C)]
struct Timespec {
    sec: i64,
    nsec: i64,
}
unsafe extern "C" {
    fn clock_gettime(clock: i32, value: *mut Timespec) -> i32;
}
fn thread_ns() -> u64 {
    let mut t = Timespec { sec: 0, nsec: 0 };
    assert_eq!(unsafe { clock_gettime(3, &mut t) }, 0);
    t.sec as u64 * 1_000_000_000 + t.nsec as u64
}
fn main() {
    let long = "SELECT 'é漢🙂' -- tail\n".repeat(1024);
    let many = vec![u32::MAX; 1024];
    let cases: Vec<(&str, &str, &[u32])> = vec![
        ("empty", "", &[]),
        ("zero", "SELECT 1", &[]),
        ("scalar", "SELECT $1", &[20]),
        (
            "types",
            "SELECT $1",
            &[
                25, 16, 20, 701, 3802, 1009, 1005, 1007, 1016, 1021, 1022, 1000, 2951,
            ],
        ),
        (
            "decimal",
            "SELECT $1",
            &[0, 9, 10, 99, 100, u32::MAX, u32::MAX],
        ),
        ("long_sql", &long, &[0, u32::MAX]),
        ("many_oids", "SELECT $1", &many),
    ];
    println!("operation,case,round,implementation,iterations,cpu_ns,wall_ns,bytes,capacity");
    for operation in ["statement", "description"] {
        for (name, sql, oids) in &cases {
            if operation == "description" && ["types", "decimal", "many_oids"].contains(name) {
                continue;
            }
            assert_eq!(
                original::statement(sql, oids),
                candidate::statement(sql, oids)
            );
            assert_eq!(original::description(sql), candidate::description(sql));
            let scale = (sql.len() / 128)
                .max(if operation == "statement" {
                    oids.len() / 4
                } else {
                    0
                })
                .max(1);
            let iterations = (1_000_000 / scale).max(200);
            for round in 0..8 {
                for old in [round % 2 == 0, round % 2 != 0] {
                    // Warm both code/allocation paths before each timed block.
                    for _ in 0..1000 {
                        black_box(original::statement(sql, oids));
                        black_box(candidate::statement(sql, oids));
                        black_box(original::description(sql));
                        black_box(candidate::description(sql));
                    }
                    let wall = Instant::now();
                    let cpu = thread_ns();
                    for _ in 0..iterations {
                        if operation == "statement" {
                            if old {
                                black_box(original::statement(black_box(sql), black_box(oids)));
                            } else {
                                black_box(candidate::statement(black_box(sql), black_box(oids)));
                            }
                        } else if old {
                            black_box(original::description(black_box(sql)));
                        } else {
                            black_box(candidate::description(black_box(sql)));
                        }
                    }
                    let cpu = thread_ns() - cpu;
                    let wall = wall.elapsed().as_nanos();
                    let output = if operation == "statement" {
                        if old {
                            original::statement(sql, oids).into_owned()
                        } else {
                            candidate::statement(sql, oids).into_owned()
                        }
                    } else if old {
                        original::description(sql)
                    } else {
                        candidate::description(sql)
                    };
                    println!(
                        "{operation},{name},{round},{},{iterations},{cpu},{wall},{},{}",
                        if old { "original" } else { "candidate" },
                        output.len(),
                        output.capacity()
                    );
                }
            }
        }
    }
}
