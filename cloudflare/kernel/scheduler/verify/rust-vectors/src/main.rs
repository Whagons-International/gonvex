//! Prints the K11 golden vectors as JSON (see ../README.md in the scheduler
//! package): cron parsing and iteration with the `cron` 0.15 crate, staggered
//! next runs, the simulated day of the 14 Whagons crons, job ids, wake keys and
//! the outbox backoff, all computed by the Rust runtime's own functions.
//!
//!   cargo run --release > ../../test/golden/rust-vectors.json

mod reference;

use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use serde_json::{json, Value};

use reference::*;

fn at(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text).unwrap().with_timezone(&Utc)
}

fn ms(value: Option<DateTime<Utc>>) -> Value {
    value.map_or(Value::Null, |value| json!(value.timestamp_millis()))
}

fn spec(name: &str, interval_ms: Option<u64>, expression: Option<&str>) -> CronSpec {
    CronSpec {
        name: name.to_owned(),
        interval_ms,
        expression: expression.map(ToOwned::to_owned),
    }
}

const STAGGER: Stagger = Stagger {
    enabled: true,
    spread_ms: Stagger::DEFAULT_SPREAD_MS,
};
const OFF: Stagger = Stagger {
    enabled: false,
    spread_ms: 0,
};

/// Expressions for parsing and iteration: Whagons' own, the crate's quirks
/// (Sunday = 1, AND of day-of-month and day-of-week, seconds and year fields,
/// shorthands, names, `?`, steps on ranges) and invalid ones.
const EXPRESSIONS: &[&str] = &[
    "* * * * *",
    "*/15 * * * *",
    "0 * * * *",
    "7 * * * *",
    "0 */6 * * *",
    "0 8 * * *",
    "0 0 * * 1",
    "0 9 * * MON-FRI",
    "0 9 * * mon,Wed,FRI",
    "0 0 1,15 * *",
    "0 0 31 * *",
    "0 0 29 2 *",
    "30 2 * * 7",
    "0 0 13 * 6",
    "0 12 * JAN,jul ?",
    "15 10 ? * *",
    "5-10/2 * * * *",
    "1/10 * * * *",
    "5,*/20 * * * *",
    "*/7 * * * *",
    "0 0 */3 * *",
    "0 0 1 */2 *",
    "0 0 * * 1,7",
    "0 0 12 1 * *",
    "*/30 * * * * *",
    "0 0 0 * * * 2030",
    "0 0 0 1 1 * 2027-2028",
    "0 30 9 * * Mon-Fri",
    "@daily",
    "@hourly",
    "@weekly",
    "@monthly",
    "@yearly",
    "  0 8 * * *  ",
    "0  8  *  *  *",
    "59 23 31 12 *",
    "0 0 * * 0",
    "*/0 * * * *",
    "10-5 * * * *",
    "60 * * * *",
    "* 24 * * *",
    "* * 32 * *",
    "* * * 13 *",
    "* * * *",
    "0 0 L * *",
    "0 0 * * SUN#2",
    "0 0 * * Funday",
    "0 0 * Smarch *",
    "*/61 * * * *",
    "",
    "0 0 0 * * * 2101",
    "0 0 0 * * * 1969",
    "* * * * * * * *",
    "a b c d e",
    "1,,2 * * * *",
    "@reboot",
];

const INSTANTS: &[&str] = &[
    "2026-11-11T00:00:00Z",
    "2026-11-11T07:59:59.999Z",
    "2026-11-11T08:00:00Z",
    "2026-11-11T08:00:00.500Z",
    "2026-11-11T23:59:59Z",
    "2026-12-31T23:59:59.250Z",
    "2027-02-28T12:00:00Z",
    "2028-02-28T23:59:59Z",
    "2028-02-29T00:00:00Z",
    "2026-11-29T23:30:00Z",
    "2026-03-08T02:30:00Z",
    "2099-12-31T23:59:59Z",
    "2100-12-31T23:59:59Z",
    "1970-01-01T00:00:00Z",
];

fn expressions() -> Value {
    let mut out = Vec::new();
    for expression in EXPRESSIONS {
        let cron = spec("c", None, Some(expression));
        let parsed = parse_schedule(&cron);
        let mut entry = json!({ "expression": expression, "valid": parsed.is_ok() });
        if let Err(error) = &parsed {
            entry["error"] = json!(error);
        }
        if let Ok(schedule) = parsed {
            let mut next = Vec::new();
            for instant in INSTANTS {
                let after = at(instant);
                let mut chain = Vec::new();
                let mut cursor = schedule.after(&after);
                for _ in 0..4 {
                    chain.push(ms(cursor.next()));
                }
                next.push(json!({
                    "after": after.timestamp_millis(),
                    "next": chain,
                    "latestAtOrBefore": ms(latest_occurrence_at_or_before(&schedule, after)),
                }));
            }
            entry["occurrences"] = json!(next);
        }
        out.push(entry);
    }
    json!(out)
}

/// next_run / next_cron_run with and without stagger, for several tenants.
fn staggered_runs() -> Value {
    let specs = [
        spec("every-minute", None, Some("* * * * *")),
        spec("quarter", None, Some("*/15 * * * *")),
        spec("hourly", None, Some("0 * * * *")),
        spec("hourly-7", None, Some("7 * * * *")),
        spec("six-hours", None, Some("0 */6 * * *")),
        spec("daily-8", None, Some("0 8 * * *")),
        spec("weekly", None, Some("0 9 * * 2")),
        spec("seconds", None, Some("*/20 * * * * *")),
        spec("year-end", None, Some("0 0 0 31 12 * 2026")),
        spec("interval-minute", Some(60_000), None),
        spec("interval-quarter", Some(900_000), None),
        spec("interval-odd", Some(7_777), None),
        spec("interval-day", Some(86_400_000), None),
    ];
    let mut out = Vec::new();
    for tenant in ["tenant-a", "tenant-b", "t_0001", "租户"] {
        for cron in &specs {
            let key = stagger_key("project-x", tenant, &cron.name);
            for instant in [
                "2026-11-11T00:00:00Z",
                "2026-11-11T12:05:00Z",
                "2026-11-11T12:59:59.500Z",
                "2026-11-11T23:58:00Z",
                "2026-12-31T23:55:00Z",
            ] {
                let after = at(instant);
                let staggered = next_run(cron, after, key, STAGGER).unwrap();
                let plain = next_run(cron, after, key, OFF).unwrap();
                let late = next_cron_run(
                    cron,
                    after - ChronoDuration::days(3),
                    after,
                    key,
                    STAGGER,
                )
                .unwrap();
                let narrow = next_run(
                    cron,
                    after,
                    key,
                    Stagger {
                        enabled: true,
                        spread_ms: 1_000,
                    },
                )
                .unwrap();
                out.push(json!({
                    "tenant": tenant,
                    "cron": cron.name,
                    "intervalMs": cron.interval_ms,
                    "expression": cron.expression,
                    "key": key.to_string(),
                    "after": after.timestamp_millis(),
                    "staggered": ms(staggered),
                    "plain": ms(plain),
                    "lateFromThreeDaysAgo": ms(late),
                    "spread1000": ms(narrow),
                }));
            }
        }
    }
    json!(out)
}

/// The 14 tenant crons of whagons5-client 2457bb65f (gonvex/_generated/manifest.json).
fn whagons_specs() -> Vec<CronSpec> {
    vec![
        spec("agents.dispatch-every-minute", Some(60_000), None),
        spec("analyticsDelivery.process-every-minute", None, Some("* * * * *")),
        spec("billing.synchronize-every-fifteen-minutes", Some(900_000), None),
        spec("boards.generate-birthday-messages", None, Some("*/15 * * * *")),
        spec("centerClassification.queue-every-fifteen-minutes", None, Some("*/15 * * * *")),
        spec("cleaning.tick-hourly", None, Some("0 * * * *")),
        spec("deepResearch.tick-every-minute", Some(60_000), None),
        spec("grantProjection.reconcile-hourly", None, Some("7 * * * *")),
        spec("journeySync.process-every-minute", Some(60_000), None),
        spec("slaExecution.evaluate-hourly", None, Some("0 * * * *")),
        spec("taskRecurrences.reconcile-recurring-schedules", None, Some("0 */6 * * *")),
        spec("tools.check-overdue-items", None, Some("0 8 * * *")),
        spec("workflowRuntime.resume-due-delays-hourly", None, Some("0 * * * *")),
        spec("workplans.reconcile-schedules", None, Some("0 */6 * * *")),
    ]
}

/// A day of the Rust scheduler with every run on time: sync at `start`
/// (sync_project: next_run from now), then after each run finish() inserts
/// next_cron_run(scheduled_for, now = scheduled_for). Runs inside
/// [start, start + 24 h) are listed with their job ids.
fn whagons_day(project: &str, tenant: &str, start: DateTime<Utc>) -> Value {
    let end = start + ChronoDuration::hours(24);
    let mut crons = Vec::new();
    for cron in whagons_specs() {
        let key = stagger_key(project, tenant, &cron.name);
        let mut runs = Vec::new();
        let mut next = next_run(&cron, start, key, STAGGER).unwrap();
        while let Some(run) = next {
            if run >= end {
                break;
            }
            runs.push(json!([run.timestamp_millis(), cron_id(project, tenant, &cron.name, run)]));
            next = next_cron_run(&cron, run, run, key, STAGGER).unwrap();
        }
        crons.push(json!({ "name": cron.name, "runs": runs, "pendingAfter": ms(next) }));
    }
    json!({
        "project": project,
        "tenant": tenant,
        "start": start.timestamp_millis(),
        "end": end.timestamp_millis(),
        "crons": crons,
    })
}

fn ids() -> Value {
    let mut out = Vec::new();
    for (name, instant) in [
        ("slas", "2026-11-11T08:00:00Z"),
        ("slas", "2026-11-11T08:00:00.123Z"),
        ("slas", "2026-11-11T08:00:00.100Z"),
        ("x", "1970-01-01T00:00:00.001Z"),
        ("cleaning.tick-hourly", "2099-12-31T23:59:59.999Z"),
    ] {
        let when = at(instant);
        out.push(json!({
            "project": "p",
            "tenant": "t",
            "name": name,
            "at": when.timestamp_millis(),
            "rfc3339": when.to_rfc3339(),
            "rfc3339AutoSi": when.to_rfc3339_opts(SecondsFormat::AutoSi, false),
            "id": cron_id("p", "t", name, when),
        }));
    }
    json!(out)
}

/// Args texts are what JS `JSON.stringify` writes (the kernel stores args
/// from JS), so serde_json reads the same numbers the kernel holds. Floats
/// with 16 or more significant digits are left out: serde_json without
/// `float_roundtrip` parses them lossily (1000000000000000.5 reads as
/// ...0.4), which the kernel does not reproduce (README, "Differences").
fn wake_keys() -> Value {
    let cases: Vec<(&str, &str)> = vec![
        ("cleaning.tick", r#"{}"#),
        ("cleaning.tick", r#"null"#),
        ("cleaning.tick", r#"{"tenant":"t","ids":[1,2]}"#),
        ("cleaning.tick", r#"{"ids":[1,2],"tenant":"t"}"#),
        ("cleaning.tick", r#"{"ids":[2,1],"tenant":"t"}"#),
        ("agents.dispatch", r#"{"ids":[1,2],"tenant":"t"}"#),
        ("f", r#"{"b":{"z":1,"a":[true,false,null]},"a":"x"}"#),
        ("f", r#"{"n":1.5,"m":-0.25,"e":1e21,"s":1e-7,"big":20000000000000000000,"i":-9223372036854775808,"u":18446744073709552000,"g":10000000000000000,"h":0.00001}"#),
        ("f", r#"{"s":"quote\" back\\ nl\n tab\t ctl\u0001 del\u007f é 😀  "}"#),
        ("f", r#"{"é":1,"z":2,"Z":3,"😀":4,"￿":5,"aa":6,"a":7}"#),
        ("f", r#"[]"#),
        ("f", r#"[{"b":1,"a":2},"x",3]"#),
        ("f", r#""text""#),
        ("f", r#"42"#),
        ("f", r#"0.1"#),
    ];
    json!(cases
        .into_iter()
        .map(|(function, args)| {
            let value: Value = serde_json::from_str(args).unwrap();
            json!({ "function": function, "args": args, "key": wake_key(function, &value) })
        })
        .collect::<Vec<_>>())
}

fn main() {
    let keys: Vec<Value> = [
        ("project", "tenant-a", "jobs.every-minute"),
        ("conformance-fixture", "tenant-a", "agents.dispatch-every-minute"),
        ("", "", ""),
    ]
    .into_iter()
    .map(|(p, t, c)| json!({ "project": p, "tenant": t, "cron": c, "key": stagger_key(p, t, c).to_string() }))
    .collect();
    let vectors = json!({
        "source": "cloudflare/kernel/scheduler/verify/rust-vectors (functions copied from rust/crates/runtime/src/scheduler.rs at 0bf3ad13; cron 0.15.0)",
        "staggerKeys": keys,
        "expressions": expressions(),
        "staggered": staggered_runs(),
        "whagonsDays": [
            whagons_day("whagons", "tenant-a", at("2026-11-11T00:00:00Z")),
            whagons_day("whagons", "t_k9x2", at("2026-11-11T13:17:42.250Z")),
        ],
        "cronIds": ids(),
        "wakeKeys": wake_keys(),
        "retryDelayMs": (-1..=33).map(|attempts| json!([attempts, retry_delay_ms(attempts)])).collect::<Vec<_>>(),
    });
    println!("{}", serde_json::to_string(&vectors).unwrap());
}
