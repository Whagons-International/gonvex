// The scheduler's time arithmetic against the Rust runtime's own functions.
// golden/rust-vectors.json is printed by verify/rust-vectors, which compiles
// the functions of RT/scheduler.rs (and ST/lib.rs retry_delay_ms) unchanged
// against the `cron` 0.15.0 crate Rust uses. Regenerate:
//   cargo run --release --manifest-path verify/rust-vectors/Cargo.toml > test/golden/rust-vectors.json
import { describe, expect, it } from "vitest";
import { CronSchedule, normalizeExpression } from "../cron.ts";
import {
  canonicalJson,
  cronJobId,
  CronSpecError,
  DEFAULT_STAGGER,
  NO_STAGGER,
  nextCronRun,
  nextRun,
  parseSchedule,
  retryDelayMs,
  rfc3339,
  staggerKey,
  wakeKey,
} from "../timing.ts";
import type { CronSpec } from "../specs.ts";
import vectors from "./golden/rust-vectors.json";

const spec = (name: string, intervalMs: number | null, expression: string | null): CronSpec => ({
  name, function: "jobs.run", args: null, scope: "tenant",
  ...(intervalMs !== null ? { intervalMs } : {}), ...(expression !== null ? { expression } : {}),
});

describe("cron expressions (cron 0.15)", () => {
  it(`parse like the crate: ${vectors.expressions.length} expressions, valid and invalid, with Rust's error text`, () => {
    for (const vector of vectors.expressions) {
      let error: string | undefined;
      try {
        parseSchedule({ name: "c", expression: vector.expression });
      } catch (thrown) {
        if (!(thrown instanceof CronSpecError)) throw thrown;
        error = thrown.message;
      }
      expect({ expression: vector.expression, valid: error === undefined, error }).toEqual({
        expression: vector.expression, valid: vector.valid, error: vector.valid ? undefined : vector.error,
      });
    }
  });

  it("iterate like the crate: next occurrences and the latest occurrence at or before, across month ends, leap days and the year 2100", () => {
    let compared = 0;
    for (const vector of vectors.expressions) {
      if (!vector.valid) continue;
      const schedule = CronSchedule.parse(normalizeExpression(vector.expression));
      for (const occurrence of vector.occurrences ?? []) {
        const chain: (number | null)[] = [];
        let cursor: number | null = occurrence.after;
        for (let i = 0; i < 4; i++) {
          cursor = cursor === null ? null : schedule.nextAfter(cursor);
          chain.push(cursor);
        }
        expect({ expression: vector.expression, after: occurrence.after, next: chain, latest: schedule.latestAtOrBefore(occurrence.after) })
          .toEqual({ expression: vector.expression, after: occurrence.after, next: occurrence.next, latest: occurrence.latestAtOrBefore });
        compared++;
      }
    }
    expect(compared).toBeGreaterThan(400);
  });
});

describe("stagger, next runs, ids and keys (RT/scheduler.rs)", () => {
  it("stagger keys are the first 8 bytes of sha256(project\\0tenant\\0cron)", () => {
    for (const vector of vectors.staggerKeys) {
      expect(staggerKey(vector.project, vector.tenant, vector.cron).toString()).toBe(vector.key);
    }
  });

  it(`next_run with and without stagger, next_cron_run after falling behind, and a narrow spread: ${vectors.staggered.length} cases`, () => {
    for (const v of vectors.staggered) {
      const cron = spec(v.cron, v.intervalMs ?? null, v.expression ?? null);
      const key = staggerKey("project-x", v.tenant, v.cron);
      expect(key.toString()).toBe(v.key);
      expect({
        case: `${v.tenant} ${v.cron} ${v.after}`,
        staggered: nextRun(cron, v.after, key, DEFAULT_STAGGER),
        plain: nextRun(cron, v.after, key, NO_STAGGER),
        late: nextCronRun(cron, v.after - 3 * 86_400_000, v.after, key, DEFAULT_STAGGER),
        narrow: nextRun(cron, v.after, key, { enabled: true, spreadMs: 1_000 }),
      }).toEqual({ case: `${v.tenant} ${v.cron} ${v.after}`, staggered: v.staggered, plain: v.plain, late: v.lateFromThreeDaysAgo, narrow: v.spread1000 });
    }
  });

  it("cron job ids hash chrono's RFC 3339 text (fraction only when non-zero)", () => {
    for (const v of vectors.cronIds) {
      expect(rfc3339(v.at)).toBe(v.rfc3339);
      expect(cronJobId(v.project, v.tenant, v.name, v.at)).toBe(v.id);
    }
  });

  it("wake keys sort object keys by UTF-8 bytes and print numbers as serde_json does", () => {
    for (const v of vectors.wakeKeys) {
      expect({ args: v.args, key: wakeKey(v.function, JSON.parse(v.args)) }).toEqual({ args: v.args, key: v.key });
    }
    expect(canonicalJson({ b: 1, a: [1.5, 1e21, null] })).toBe('{"a":[1.5,1e+21,null],"b":1}');
  });

  it("outbox backoff: 1 s doubling, capped at 15 minutes", () => {
    for (const [attempts, delay] of vectors.retryDelayMs as [number, number][]) expect([attempts, retryDelayMs(attempts)]).toEqual([attempts, delay]);
  });
});
