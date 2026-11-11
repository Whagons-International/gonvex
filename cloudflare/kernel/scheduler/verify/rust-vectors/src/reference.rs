//! Copied from the Rust runtime at 0bf3ad13 without behavior changes:
//! rust/crates/runtime/src/scheduler.rs (Stagger, stagger_key, next_cron_run,
//! next_run, latest_occurrence_at_or_before, staggered, next_occurrence,
//! parse_schedule, cron_id, wake_key) and rust/crates/store/src/lib.rs
//! (retry_delay_ms). Only `CronSpec` is reduced to the fields they read.

use std::str::FromStr;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use cron::Schedule;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub struct CronSpec {
    pub name: String,
    pub interval_ms: Option<u64>,
    pub expression: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stagger {
    pub enabled: bool,
    pub spread_ms: i64,
}

impl Stagger {
    pub const DEFAULT_SPREAD_MS: i64 = 600_000;
}

fn millis_to_datetime(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(millis).unwrap_or_default()
}

pub fn stagger_key(project: &str, tenant: &str, cron: &str) -> u64 {
    let digest = Sha256::digest(format!("{project}\0{tenant}\0{cron}").as_bytes());
    u64::from_be_bytes(digest[..8].try_into().unwrap_or_default())
}

pub fn next_cron_run(
    spec: &CronSpec,
    scheduled_for: DateTime<Utc>,
    now: DateTime<Utc>,
    key: u64,
    stagger: Stagger,
) -> Result<Option<DateTime<Utc>>, String> {
    match next_run(spec, scheduled_for, key, stagger)? {
        Some(next) if next <= now => next_run(spec, now, key, stagger),
        next => Ok(next),
    }
}

pub fn next_run(
    spec: &CronSpec,
    after: DateTime<Utc>,
    key: u64,
    stagger: Stagger,
) -> Result<Option<DateTime<Utc>>, String> {
    if !stagger.enabled {
        return next_occurrence(spec, after);
    }
    if let Some(interval_ms) = spec.interval_ms {
        let interval = i64::try_from(interval_ms)
            .map_err(|_| format!("cron {:?} interval is too large", spec.name))?;
        let phase = (key % interval_ms) as i64;
        let after_ms = after.timestamp_millis();
        let next = after_ms - (after_ms - phase).rem_euclid(interval) + interval;
        return Ok(Some(millis_to_datetime(next)));
    }
    let schedule = parse_schedule(spec)?;
    if let Some(previous) = latest_occurrence_at_or_before(&schedule, after) {
        if let Some(at) = staggered(&schedule, previous, key, stagger.spread_ms) {
            if at > after {
                return Ok(Some(at));
            }
        }
    }
    let Some(nominal) = schedule.after(&after).next() else {
        return Ok(None);
    };
    Ok(staggered(&schedule, nominal, key, stagger.spread_ms))
}

pub fn latest_occurrence_at_or_before(
    schedule: &Schedule,
    at: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let mut earlier = schedule.after(&(at + ChronoDuration::seconds(1)));
    let mut previous = earlier.next_back();
    while previous.is_some_and(|occurrence| occurrence > at) {
        previous = earlier.next_back();
    }
    previous
}

pub fn staggered(
    schedule: &Schedule,
    nominal: DateTime<Utc>,
    key: u64,
    spread_ms: i64,
) -> Option<DateTime<Utc>> {
    let gap = schedule
        .after(&nominal)
        .next()
        .map_or(spread_ms, |next| (next - nominal).num_milliseconds());
    let window = gap.min(spread_ms);
    if window <= 1 {
        return Some(nominal);
    }
    let offset = (key % window as u64) as i64;
    nominal.checked_add_signed(ChronoDuration::milliseconds(offset))
}

pub fn next_occurrence(
    spec: &CronSpec,
    after: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, String> {
    if let Some(interval_ms) = spec.interval_ms {
        let interval = i64::try_from(interval_ms)
            .map_err(|_| format!("cron {:?} interval is too large", spec.name))?;
        return Ok(Some(after + ChronoDuration::milliseconds(interval)));
    }
    Ok(parse_schedule(spec)?.after(&after).next())
}

pub fn parse_schedule(spec: &CronSpec) -> Result<Schedule, String> {
    let expression = spec.expression.as_deref().unwrap_or("").trim();
    let fields = expression.split_whitespace().count();
    let normalized = if fields == 5 {
        format!("0 {expression}")
    } else {
        expression.to_owned()
    };
    Schedule::from_str(&normalized)
        .map_err(|error| format!("cron {:?} has invalid expression: {error}", spec.name))
}

pub fn cron_id(project: &str, tenant: &str, name: &str, at: DateTime<Utc>) -> String {
    let source = format!("{project}\0{tenant}\0{name}\0{}", at.to_rfc3339());
    let digest = Sha256::digest(source.as_bytes());
    let suffix = digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("cron_{suffix}")
}

pub fn wake_key(function: &str, args: &Value) -> String {
    fn canonical(value: &Value, out: &mut String) {
        match value {
            Value::Object(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                out.push('{');
                for (index, key) in keys.into_iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str(&Value::String(key.clone()).to_string());
                    out.push(':');
                    canonical(&map[key], out);
                }
                out.push('}');
            }
            Value::Array(items) => {
                out.push('[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    canonical(item, out);
                }
                out.push(']');
            }
            other => out.push_str(&other.to_string()),
        }
    }
    let mut source = format!("{function}\0");
    canonical(args, &mut source);
    let digest = Sha256::digest(source.as_bytes());
    let suffix = digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("wake_{suffix}")
}

pub fn retry_delay_ms(attempts: i32) -> i64 {
    const CAP_MS: i64 = 15 * 60 * 1000;
    let exponent = attempts.clamp(1, 30) - 1;
    1000i64
        .checked_shl(exponent as u32)
        .unwrap_or(CAP_MS)
        .min(CAP_MS)
}
