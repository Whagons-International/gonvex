use super::*;

#[cfg(test)]
mod codec_tests {
    use super::*;
    #[test]
    fn canonical_microseconds_offsets_negatives_and_unqualified_precision() {
        for value in [
            -1_000_001,
            -1_000_000,
            -1,
            0,
            1,
            999,
            1_001,
            1_234_567,
            1_794_321_123_456_789,
        ] {
            assert_eq!(parse_rfc3339(&encode(value).unwrap()).unwrap(), value);
        }
        assert_eq!(encode(-1).unwrap(), "1969-12-31T23:59:59.999999Z");
        assert_eq!(
            parse_rfc3339("1970-01-01T01:00:00.000001+01:00").unwrap(),
            1
        );
        for text in [
            "1970-01-01T00:00:00.0000001Z",
            "1970-01-01T00:00:00.1234560Z",
            "2016-12-31T23:59:60Z",
            "1970-01-01T00:00:00",
            "1",
        ] {
            assert!(parse_rfc3339(text).is_err(), "{text}");
        }
        assert!(encode(i64::MAX).is_err());
        assert!(encode(i64::MIN).is_err());
    }
}

#[cfg(test)]
mod sql_tests {
    use super::*;
    use sqlx::{Connection, SqliteConnection};
    use std::time::Duration;
    async fn connection() -> SqliteConnection {
        install();
        SqliteConnection::connect("sqlite::memory:").await.unwrap()
    }
    async fn stamp(c: &mut SqliteConnection) -> i64 {
        sqlx::query_scalar("SELECT pg_now_micros()")
            .fetch_one(c)
            .await
            .unwrap()
    }
    #[tokio::test]
    async fn native_timestamp_clock_transactions_savepoints_errors_triggers_and_reuse() {
        let mut c = connection().await;
        sqlx::raw_sql("CREATE TABLE t(id INTEGER PRIMARY KEY,at TIMESTAMP_MICROS DEFAULT(pg_now_micros()));CREATE TABLE audit(at TIMESTAMP_MICROS DEFAULT(pg_now_micros()));CREATE TRIGGER log_insert AFTER INSERT ON t BEGIN INSERT INTO audit(at) VALUES(pg_now_micros()); END;").execute(&mut c).await.unwrap();
        // Trigger syntax uses SELECT to keep this valid in SQLite triggers.
        sqlx::query("BEGIN").execute(&mut c).await.unwrap();
        let begin = stamp(&mut c).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        sqlx::query("SAVEPOINT nested")
            .execute(&mut c)
            .await
            .unwrap();
        assert!(sqlx::query("BEGIN").execute(&mut c).await.is_err());
        assert_eq!(stamp(&mut c).await, begin);
        sqlx::query("ROLLBACK TO nested")
            .execute(&mut c)
            .await
            .unwrap();
        sqlx::query("RELEASE nested").execute(&mut c).await.unwrap();
        sqlx::query("INSERT INTO t(id) VALUES(1),(2)")
            .execute(&mut c)
            .await
            .unwrap();
        let dates = sqlx::query_scalar::<_, i64>("SELECT at FROM t UNION ALL SELECT at FROM audit")
            .fetch_all(&mut c)
            .await
            .unwrap();
        assert_eq!(dates, vec![begin; 4]);
        assert!(sqlx::query("SELECT nonexistent_function()")
            .execute(&mut c)
            .await
            .is_err());
        assert_eq!(stamp(&mut c).await, begin);
        let wall: i64 = sqlx::query_scalar("SELECT pg_clock_timestamp_micros()")
            .fetch_one(&mut c)
            .await
            .unwrap();
        assert!(wall > begin);
        sqlx::query("COMMIT").execute(&mut c).await.unwrap();
        assert!(stamp(&mut c).await > begin);
        sqlx::query("BEGIN").execute(&mut c).await.unwrap();
        let second = stamp(&mut c).await;
        assert!(second > begin);
        sqlx::query("ROLLBACK").execute(&mut c).await.unwrap();
        sqlx::query("BEGIN").execute(&mut c).await.unwrap();
        let third = stamp(&mut c).await;
        assert!(sqlx::query("INSERT OR ROLLBACK INTO t(id) VALUES(1)")
            .execute(&mut c)
            .await
            .is_err());
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(stamp(&mut c).await > third);
        sqlx::query("INSERT INTO t(id) VALUES(3),(4)")
            .execute(&mut c)
            .await
            .unwrap();
        let counts: (i64, i64) =
            sqlx::query_as("SELECT count(DISTINCT at),count(*) FROM t WHERE id IN(3,4)")
                .fetch_one(&mut c)
                .await
                .unwrap();
        assert_eq!(counts, (1, 2));
        // Dropping an active stream must reset its statement; a new transaction
        // on the reused connection must not inherit the cancelled query clock.
        c.close().await.unwrap();
    }
    #[tokio::test]
    async fn timestamp_sql_projection_sqlx_direct_and_capture_match_exact_wire_values() {
        let mut c = connection().await;
        sqlx::raw_sql(crate::feed::SYNC_INFRASTRUCTURE_SQL)
            .execute(&mut c)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE dates(id TEXT PRIMARY KEY, at TIMESTAMP_MICROS CHECK(at IS NULL OR typeof(at)='integer'))").execute(&mut c).await.unwrap();
        let columns = crate::rows::table_columns(&mut c, "dates").await.unwrap();
        sqlx::raw_sql(&crate::feed::capture_triggers_sql("dates", "id", &columns))
            .execute(&mut c)
            .await
            .unwrap();
        crate::capture::install(&mut c).await.unwrap();
        for (i, value) in [Some(-1), Some(0), Some(1), Some(1234567), None]
            .into_iter()
            .enumerate()
        {
            sqlx::query("INSERT INTO dates VALUES(?1,?2)")
                .bind(i.to_string())
                .bind(value)
                .execute(&mut c)
                .await
                .unwrap();
        }
        let kinds = columns
            .iter()
            .map(|c| (c.name.clone(), c.kind()))
            .collect::<Vec<_>>();
        let statement = format!(
            "SELECT {} FROM dates r ORDER BY id",
            crate::rows::json_object_expr("r", &kinds)
        );
        let encoded = sqlx::query_scalar::<_, String>(&statement)
            .fetch_all(&mut c)
            .await
            .unwrap()
            .into_iter()
            .map(|s| serde_json::from_str::<serde_json::Value>(&s).unwrap())
            .collect::<Vec<_>>();
        let raw = sqlx::query("SELECT * FROM dates ORDER BY id")
            .fetch_all(&mut c)
            .await
            .unwrap();
        for (i, row) in raw.iter().enumerate() {
            let got = crate::rows::row_to_object(row, |column| {
                Some(if column == "at" {
                    crate::rows::JsonKind::TimestampMicros
                } else {
                    crate::rows::JsonKind::Text
                })
            })
            .unwrap();
            assert_eq!(serde_json::Value::Object(got), encoded[i]);
        }
        crate::feed::finalize(&mut c, &crate::feed::Provenance::default())
            .await
            .unwrap();
        let captured = sqlx::query_scalar::<_, String>(
            "SELECT new_value FROM _gonvex_sync_changes ORDER BY event_id",
        )
        .fetch_all(&mut c)
        .await
        .unwrap()
        .into_iter()
        .map(|s| serde_json::from_str::<serde_json::Value>(&s).unwrap())
        .collect::<Vec<_>>();
        assert_eq!(captured, encoded);
        for literal in ["'not a timestamp'", "9223372036854775807", "1.5"] {
            assert!(
                sqlx::query(&format!("INSERT INTO dates VALUES('bad',{literal})"))
                    .execute(&mut c)
                    .await
                    .is_err()
            );
        }
        assert!(
            sqlx::query("SELECT gonvex_timestamp_micros_rfc3339(9223372036854775807)")
                .fetch_all(&mut c)
                .await
                .is_err()
        );
        let filtered = sqlx::query_scalar::<_, String>(
            "SELECT id FROM dates WHERE at>=?1 AND at<?2 ORDER BY at DESC,id DESC LIMIT 2",
        )
        .bind(parse_rfc3339("1970-01-01T00:00:00Z").unwrap())
        .bind(parse_rfc3339("1970-01-01T00:00:01.234568Z").unwrap())
        .fetch_all(&mut c)
        .await
        .unwrap();
        assert_eq!(filtered, vec!["3", "2"]);
        // Actual native writer encoding shares the same declared-kind path.
        let mut handle = c.lock_handle().await.unwrap();
        let db = handle.as_raw_handle();
        let mut stmt = std::ptr::null_mut();
        unsafe {
            assert_eq!(
                ffi::sqlite3_prepare_v2(
                    db.as_ptr(),
                    c"SELECT at FROM dates ORDER BY id".as_ptr(),
                    -1,
                    &mut stmt,
                    std::ptr::null_mut()
                ),
                ffi::SQLITE_OK
            );
        }
        let mut native = Vec::new();
        unsafe {
            while ffi::sqlite3_step(stmt) == ffi::SQLITE_ROW {
                if ffi::sqlite3_column_type(stmt, 0) == ffi::SQLITE_NULL {
                    native.push(serde_json::Value::Null)
                } else {
                    native.push(serde_json::Value::String(
                        encode(ffi::sqlite3_column_int64(stmt, 0)).unwrap(),
                    ));
                }
            }
            ffi::sqlite3_finalize(stmt);
        }
        assert_eq!(
            native,
            encoded.iter().map(|r| r["at"].clone()).collect::<Vec<_>>()
        );
        drop(handle);
        c.close().await.unwrap();
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use sqlx::{Connection, Row, SqliteConnection};
    use std::time::Duration;
    #[tokio::test]
    async fn native_timestamp_failed_begin_reset_reuse_and_overlapping_statements() {
        install();
        let dir = std::env::temp_dir().join(format!("gonvex-clock-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let url = crate::database_url(&dir.join("clock.db"));
        let options = url
            .parse::<sqlx::sqlite::SqliteConnectOptions>()
            .unwrap()
            .create_if_missing(true)
            .busy_timeout(Duration::ZERO);
        let mut a = SqliteConnection::connect_with(&options).await.unwrap();
        let mut b = SqliteConnection::connect_with(&options).await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut a)
            .await
            .unwrap();
        assert!(sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut b)
            .await
            .is_err());
        sqlx::query("ROLLBACK").execute(&mut a).await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut b)
            .await
            .unwrap();
        let begin: i64 = sqlx::query_scalar("SELECT pg_now_micros()")
            .fetch_one(&mut b)
            .await
            .unwrap();
        let mut handle = b.lock_handle().await.unwrap();
        let db = handle.as_raw_handle();
        let mut one = std::ptr::null_mut();
        let mut two = std::ptr::null_mut();
        unsafe {
            assert_eq!(
                ffi::sqlite3_prepare_v2(
                    db.as_ptr(),
                    c"SELECT pg_now_micros() UNION ALL SELECT pg_now_micros()".as_ptr(),
                    -1,
                    &mut one,
                    std::ptr::null_mut()
                ),
                ffi::SQLITE_OK
            );
            assert_eq!(
                ffi::sqlite3_prepare_v2(
                    db.as_ptr(),
                    c"SELECT pg_now_micros() UNION ALL SELECT pg_now_micros()".as_ptr(),
                    -1,
                    &mut two,
                    std::ptr::null_mut()
                ),
                ffi::SQLITE_OK
            );
            for stmt in [one, two, one, two] {
                assert_eq!(ffi::sqlite3_step(stmt), ffi::SQLITE_ROW);
                assert_eq!(ffi::sqlite3_column_int64(stmt, 0), begin);
            }
            // Cancel/reset one active cursor while another is still alive.
            assert_eq!(ffi::sqlite3_reset(one), ffi::SQLITE_OK);
            ffi::sqlite3_finalize(one);
            ffi::sqlite3_finalize(two);
        }
        drop(handle);
        sqlx::query("ROLLBACK").execute(&mut b).await.unwrap();
        let row = sqlx::query("SELECT pg_now_micros(),pg_clock_timestamp_micros()")
            .fetch_one(&mut b)
            .await
            .unwrap();
        assert!(row.get::<i64, _>(0) >= begin);
        assert!(row.get::<i64, _>(1) >= row.get(0));
        // Abandoned SQLx transactions rollback before this connection is reused.
        {
            let mut tx = b.begin().await.unwrap();
            sqlx::query("SELECT pg_now_micros()")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        }
        let mut tx = b.begin().await.unwrap();
        let reused: i64 = sqlx::query_scalar("SELECT pg_now_micros()")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert!(reused >= begin);
        tx.rollback().await.unwrap();
        a.close().await.unwrap();
        b.close().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod registration_tests {
    use super::*;
    use sqlx::{Connection, SqliteConnection};
    #[tokio::test]
    async fn timestamp_registration_failure_keeps_old_context_and_close_is_safe() {
        install();
        let mut c = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        let mut handle = c.lock_handle().await.unwrap();
        let db = handle.as_raw_handle();
        let mut statement = std::ptr::null_mut();
        unsafe {
            assert_eq!(
                ffi::sqlite3_prepare_v2(
                    db.as_ptr(),
                    c"SELECT pg_now_micros()".as_ptr(),
                    -1,
                    &mut statement,
                    std::ptr::null_mut()
                ),
                ffi::SQLITE_OK
            );
            assert_eq!(ffi::sqlite3_step(statement), ffi::SQLITE_ROW);
            assert_eq!(register(db.as_ptr()), ffi::SQLITE_BUSY);
            ffi::sqlite3_finalize(statement);
        }
        drop(handle);
        let first: i64 = sqlx::query_scalar("SELECT pg_now_micros()")
            .fetch_one(&mut c)
            .await
            .unwrap();
        let second: i64 = sqlx::query_scalar("SELECT pg_now_micros()")
            .fetch_one(&mut c)
            .await
            .unwrap();
        assert!(second >= first);
        c.close().await.unwrap();
    }
    #[test]
    fn timestamp_profile_delegation_runs_in_fresh_process() {
        if std::env::var_os("GONVEX_TIMESTAMP_PROFILE_CHILD").is_some() {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                install();
                let mut c = SqliteConnection::connect("sqlite::memory:").await.unwrap();
                sqlx::query("SELECT pg_now_micros(),pg_clock_timestamp_micros()")
                    .fetch_all(&mut c)
                    .await
                    .unwrap();
                c.close().await.unwrap();
            });
            assert!(crate::statement_profile::snapshot()
                .iter()
                .any(|(sql, stats)| sql.contains("pg_now_micros") && stats.count > 0));
            return;
        }
        let status=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","timestamp_micros::tests::registration_tests::timestamp_profile_delegation_runs_in_fresh_process","--nocapture"]).env("GONVEX_TIMESTAMP_PROFILE_CHILD","1").env("GONVEX_SQLITE_STATEMENT_PROFILE","1").status().unwrap();
        assert!(status.success());
    }
}

#[cfg(test)]
mod pg_oracle_tests {
    use super::*;
    use serde_json::Value;
    use sqlx::{Connection, SqliteConnection};
    #[tokio::test]
    async fn timestamp_native_sql_matches_postgres_filters_order_nulls_and_canonical_wire() {
        install();
        let oracle: Value =
            serde_json::from_str(include_str!("../testdata/timestamp-micros-pg-oracle.json"))
                .unwrap();
        let mut c = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "CREATE TABLE synthetic_time(id TEXT PRIMARY KEY,at TIMESTAMP_MICROS,aligned BOOLEAN)",
        )
        .execute(&mut c)
        .await
        .unwrap();
        for case in oracle["cases"].as_array().unwrap() {
            let typed = parse_rfc3339(case["wire"].as_str().unwrap()).unwrap();
            assert_eq!(typed.to_string(), case["micros"].as_str().unwrap());
            sqlx::query("INSERT INTO synthetic_time VALUES(?1,?2,?3)")
                .bind(case["id"].as_str().unwrap())
                .bind(typed)
                .bind(typed % 1000 == 0)
                .execute(&mut c)
                .await
                .unwrap();
            assert_eq!(encode(typed).unwrap(), case["wire"].as_str().unwrap());
        }
        sqlx::query("INSERT INTO synthetic_time VALUES('synthetic-null',NULL,1)")
            .execute(&mut c)
            .await
            .unwrap();
        for query in oracle["queries"].as_array().unwrap() {
            let op = query["operator"].as_str().unwrap();
            assert!(["=", ">", "<", ">=", "<="].contains(&op));
            let statement = format!(
                "SELECT id FROM synthetic_time WHERE at {op} ?1 {} ORDER BY id",
                if query["aligned"] == true {
                    "AND aligned=1"
                } else {
                    ""
                }
            );
            let actual = sqlx::query_scalar::<_, String>(&statement)
                .bind(parse_rfc3339(query["cutoffWire"].as_str().unwrap()).unwrap())
                .fetch_all(&mut c)
                .await
                .unwrap();
            assert_eq!(serde_json::to_value(actual).unwrap(), query["expectedIds"]);
        }
        for query in oracle["orders"].as_array().unwrap() {
            let direction = query["direction"].as_str().unwrap();
            assert!(["ASC", "DESC"].contains(&direction));
            let statement = format!(
                "SELECT id FROM synthetic_time ORDER BY at {direction},id {direction} LIMIT ?1"
            );
            let actual = sqlx::query_scalar::<_, String>(&statement)
                .bind(query["limit"].as_i64().unwrap())
                .fetch_all(&mut c)
                .await
                .unwrap();
            assert_eq!(serde_json::to_value(actual).unwrap(), query["expectedIds"]);
        }
        let actual = sqlx::query_scalar::<_, String>(
            "SELECT id FROM synthetic_time WHERE at IS NULL ORDER BY id",
        )
        .fetch_all(&mut c)
        .await
        .unwrap();
        assert_eq!(serde_json::to_value(actual).unwrap(), oracle["nullIds"]);
        c.close().await.unwrap();
    }
    #[tokio::test]
    async fn timestamp_pool_reader_writer_and_actual_direct_json_encoding_agree() {
        let dir =
            std::env::temp_dir().join(format!("gonvex-direct-clock-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let url = crate::database_url(&dir.join("clock.db"));
        let pools = crate::PoolRegistry::new(crate::PoolLimits::default());
        let mut tx = crate::begin_transaction(&pools, &url, false).await.unwrap();
        sqlx::query(
            "CREATE TABLE d(id TEXT PRIMARY KEY,at TIMESTAMP_MICROS DEFAULT(pg_now_micros()))",
        )
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
        sqlx::query("INSERT INTO d(id,at) VALUES('a',-1),('b',1)")
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        let direct = tx.direct().await.unwrap();
        let encoded = direct
            .run(|conn| {
                let mut stmt = conn.prepare("SELECT at FROM d ORDER BY id")?;
                let mut rows = Vec::new();
                while stmt.step()? {
                    let mut bytes = Vec::new();
                    stmt.write_json(0, crate::rows::JsonKind::TimestampMicros, &mut bytes)
                        .map_err(sqlx::Error::Protocol)?;
                    rows.push(serde_json::from_slice::<Value>(&bytes).unwrap());
                }
                Ok::<_, sqlx::Error>(rows)
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            encoded,
            vec![
                Value::String(encode(-1).unwrap()),
                Value::String(encode(1).unwrap())
            ]
        );
        tx.commit().await.unwrap();
        let mut tx = crate::begin_transaction(&pools, &url, true).await.unwrap();
        let before: i64 = sqlx::query_scalar("SELECT pg_now_micros()")
            .fetch_one(&mut **tx.transaction())
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let after: i64 = sqlx::query_scalar("SELECT pg_now_micros()")
            .fetch_one(&mut **tx.transaction())
            .await
            .unwrap();
        assert_eq!(before, after);
        tx.commit().await.unwrap();
        pools.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod overhead_observation {
    use super::*;
    use sqlx::{Connection, SqliteConnection};
    fn cpu_ns() -> i128 {
        let mut t = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        assert_eq!(
            unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut t) },
            0
        );
        i128::from(t.tv_sec) * 1_000_000_000 + i128::from(t.tv_nsec)
    }
    #[tokio::test]
    #[ignore = "explicit local synthetic clock-hook CPU observation; not a capacity gate"]
    async fn timestamp_clock_hook_select_one_cpu_observation() {
        install();
        assert!(!crate::statement_profile::enabled());
        let mut c = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        let mut handle = c.lock_handle().await.unwrap();
        let db = handle.as_raw_handle();
        let mut statement = std::ptr::null_mut();
        unsafe {
            assert_eq!(
                ffi::sqlite3_prepare_v2(
                    db.as_ptr(),
                    c"SELECT 1".as_ptr(),
                    -1,
                    &mut statement,
                    std::ptr::null_mut()
                ),
                ffi::SQLITE_OK
            );
        }
        let mut samples = Vec::new();
        for pair in 0..12 {
            for enabled in if pair % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                unsafe {
                    if enabled {
                        assert_eq!(register(db.as_ptr()), ffi::SQLITE_OK);
                    } else {
                        ffi::sqlite3_trace_v2(db.as_ptr(), 0, None, std::ptr::null_mut());
                    }
                }
                let start = cpu_ns();
                for _ in 0..10000 {
                    unsafe {
                        assert_eq!(ffi::sqlite3_step(statement), ffi::SQLITE_ROW);
                        assert_eq!(ffi::sqlite3_step(statement), ffi::SQLITE_DONE);
                        assert_eq!(ffi::sqlite3_reset(statement), ffi::SQLITE_OK);
                    }
                }
                let cpu = cpu_ns() - start;
                samples.push(serde_json::json!({"pair":pair,"clockHook":enabled,"statements":10000,"threadCpuNs":cpu}));
            }
        }
        unsafe {
            ffi::sqlite3_finalize(statement);
        }
        let record = serde_json::json!({"qualification":"synthetic SELECT 1 hook overhead only; other host lanes uncontrolled, no app/capacity claim; default feature OFF installs no clock hook","samples":samples});
        if let Ok(path) = std::env::var("GONVEX_TIMESTAMP_OVERHEAD_OUTPUT") {
            std::fs::write(path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();
        }
        eprintln!("TIMESTAMP_HOOK_OBSERVATION {record}");
        drop(handle);
        c.close().await.unwrap();
    }
}

#[cfg(test)]
mod domain_tests {
    use super::*;
    use sqlx::{Connection, SqliteConnection};
    #[tokio::test]
    async fn timestamp_domain_is_fresh_range_checked_and_requires_native_registration() {
        install();
        let mut c = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::query("CREATE TABLE d(at TIMESTAMP_MICROS)")
            .execute(&mut c)
            .await
            .unwrap();
        sqlx::query("INSERT INTO d VALUES(-1),(NULL)")
            .execute(&mut c)
            .await
            .unwrap();
        let cols = vec!["at".to_owned()];
        assert!(source_domain(&mut c, "d", &cols).await.unwrap());
        sqlx::query("BEGIN").execute(&mut c).await.unwrap();
        sqlx::query("INSERT INTO d VALUES(9223372036854775807)")
            .execute(&mut c)
            .await
            .unwrap();
        assert!(!source_domain(&mut c, "d", &cols).await.unwrap());
        sqlx::query("ROLLBACK").execute(&mut c).await.unwrap();
        assert!(source_domain(&mut c, "d", &cols).await.unwrap());
        sqlx::query("INSERT INTO d VALUES('invalid')")
            .execute(&mut c)
            .await
            .unwrap();
        assert!(!source_domain(&mut c, "d", &cols).await.unwrap());
        sqlx::query("DELETE FROM d WHERE typeof(at)='text'")
            .execute(&mut c)
            .await
            .unwrap();
        {
            let mut handle = c.lock_handle().await.unwrap();
            unsafe {
                assert_eq!(
                    ffi::sqlite3_set_clientdata(
                        handle.as_raw_handle().as_ptr(),
                        c"gonvex.timestamp-micros.clock-v1".as_ptr(),
                        std::ptr::null_mut(),
                        None
                    ),
                    ffi::SQLITE_OK
                );
            }
        }
        assert!(!source_domain(&mut c, "d", &cols).await.unwrap());
        c.close().await.unwrap();
    }
}
