"""Validate observations, not just a fixture's self-reported pass count."""
import json

CASES = {
    ("baseline", "replicaOutbox"), ("baseline", "twoOutboxes"),
    ("baseline", "reader"), ("fixed", "replicaOutbox"),
    ("fixed", "twoOutboxes"), ("fixed", "reader"),
    ("fixed", "rejectedOwner"), ("fixed", "migrations"),
    ("fixed", "independent"),
}
TERMINAL = {
    "all": "complete", "crash-seed": "write-held-for-process-crash",
    "recover": "recovered-and-recommitted",
    "reopen-committed": "process-restart-complete",
    "journal-corrupt-seed": "corrupt-journal-seeded",
    "journal-corrupt-recover": "corrupt-journal-refused",
    "historical-seed": "historical-seeded",
    "historical-replica-held": "historical-replica-write-held",
    "historical-journal-held": "historical-journal-write-held",
    "historical-recover": "historical-recovered",
    "historical-reopen": "historical-reopened",
}


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def rows(snapshot, revision, changed):
    require(snapshot["cursor"] == {"epoch": "e", "revision": revision}, "wrong durable checkpoint")
    tasks = snapshot["entities"]["tasks"]
    require(tasks["a"] == {"id": "a", "name": "Changed" if changed else "Original"}, "wrong original row")
    require(tasks.get("c") == ({"id": "c", "name": "New"} if changed else None), "checkpoint passed missing/unexpected row")


def intent(entry, state, number=1, key="native-intent"):
    require(entry["id"] == number and entry["idempotencyKey"] == key, "intent identity changed")
    require(entry["state"] == state and entry["args"] == {"key": key}, "intent state or arguments changed")


def durable(event):
    require(event["sync"]["synchronous"] == 2, "SQLite FULL synchronization required")
    require(event["journal"]["journal_mode"] in ("delete", "wal"), "disk journal required")


def corrupt_result(result, phase):
    require(len(result["rows"]) == 2, "corrupt journal history lost or new work admitted")
    first, second = result["rows"]
    require(first == {"id": 1, "scope": "account", "idempotency_key": "native-corrupt", "state": "committed", "value": "{invalid accepted bytes"}, "corrupt accepted bytes changed")
    require(second["id"] == 2 and second["scope"] == "account" and second["idempotency_key"] == "native-good" and second["state"] == "committed", "valid accepted identity changed")
    intent(json.loads(second["value"]), "committed", 2, "native-good")
    if phase == "journal-corrupt-recover":
        require(set(result["errors"]) == {"load", "update", "restore", "enqueue"}, "missing blocked operation")
        for operation, error in result["errors"].items():
            require(error and error["name"] == "OutboxRecoveryError", operation + " did not report recovery requirement")
    durable(result)



def historical_entries(raw):
    entries = json.loads(raw)
    require(len(entries) == 6, "historical source must contain six intents")
    for i, entry in enumerate(entries, 1):
        require(entry["id"] == i and entry["idempotencyKey"] == f"historical-{i}", "historical source identity changed")
        require(entry["scope"] == ("bob" if i == 6 else "alice") and entry["receiptPath"] == "tasks.oldWrite", "historical source owner or receipt path changed")
        require(entry["args"] == {"key": f"historical-{i}", "text": "preserve\x00suffix"}, "historical source arguments changed")
    require([e["state"] for e in entries] == ["pending", "inflight", "committed", "failed", "rejected", "committed"], "historical source delivery states missing")
    return [dict(entry, state="pending" if entry["state"] == "inflight" else entry["state"], knownUnsent=False) for entry in entries]


def window_binding_result(value):
    signatures = ["tasks\x00a", "tasks\x00b"]
    require([v["mode"] for v in value["modes"]] == ["replaceWindow", "replaceSnapshot", "applyTransaction", "advanceWatermark"], "missing native key writer")
    for mode in value["modes"]:
        require(mode["full"] == mode["working"] == signatures, "native key writer lost NUL suffix or aliased its peer")
        require(mode["indexed"] == [{"bytes": s.encode().hex().upper()} for s in signatures], "native indexed key bytes were truncated")
        require(len(mode["points"]) == 2, "missing native point lookup")
        for i, point in enumerate(mode["points"]):
            row_id = "bind-a" if i == 0 else "bind-b"
            require(point and point["window"]["signature"] == signatures[i] and point["window"]["ids"] == [row_id]
                    and point["rows"] == [{"id": row_id}], "native point lookup crossed its indexed key")
    before, after = value["beforeRemove"], value["afterRemove"]
    require(before["liveQueries"][signatures[0]]["ids"] == [] and before["liveQueries"][signatures[1]]["ids"] == ["bind-b"], "native membership delete aliased its peer")
    require(list(after["liveQueries"]) == [signatures[1]] and after["liveQueries"][signatures[1]]["ids"] == ["bind-b"], "native window removal aliased its peer")
    require(before["cursor"] == after["cursor"] == {"epoch": "bindings", "revision": 6}, "native key mutation lost its checkpoint")


def historical_result(result, phase):
    if phase in ("historical-replica-held", "historical-journal-held"):
        require(result["variant"] == "unscoped", "held historical database changed")
        durable(result)
        if phase == "historical-replica-held":
            require(result["successfulEntityStatement"] is True, "historical native row copy did not execute")
            require(result["checkpointInsideTransaction"] == [], "historical checkpoint already published")
            copied = result["migratedRowsInsideTransaction"]
            require(len(copied) == 2 and all(row["scope"] == "default" for row in copied)
                    and sorted(row["id"] for row in copied) == ["a", "gone"]
                    and next(row for row in copied if row["id"] == "gone")["value"] == "null", "historical native row/tombstone copy missing")
        else:
            require(result["successfulAcceptedStatement"] is True, "historical acceptance SQL did not execute")
            require(result["beforeMigration"] == result["original"], "interrupted historical replica migration changed raw state")
            imported = result["importedRowsInsideTransaction"]
            require([row["id"] for row in imported] == [1, 2, 3] and json.loads(imported[-1]["value"])["idempotencyKey"] == "historical-3", "held import lacks its real accepted row")
            historical_entries(result["legacy"])
        return
    cases = result["cases"]
    require(len(cases) == 2 and [c["variant"] for c in cases] == ["unscoped", "scoped"], "historical schema matrix incomplete")
    for case in cases:
        durable(case)
        expected = historical_entries(case["legacy"])
        if phase == "historical-seed":
            raw = case["raw"]
            require(raw["appVersion"] == {"user_version": 700}, "historical application version missing")
            require(set(raw["tables"]) == {"entities", "queries", "meta"}, "historical raw schema incomplete")
            require(len(raw["schema"]) == 6, "historical tables or primary-key indexes missing")
            signature = 'tasks.all\x00{"parentId":"p"}'.encode().hex().upper()
            require(len(raw["tables"]["queries"]) == (1 if case["variant"] == "unscoped" else 2)
                    and all(row["signature"] == signature for row in raw["tables"]["queries"]), "historical seed key bytes were truncated")
            require(len(raw["tables"]["entities"]) == (2 if case["variant"] == "unscoped" else 4), "historical owner rows or tombstones missing")
            continue
        require(case["appVersion"] == {"user_version": 700} and case["checkpoint"] == [{"id": 1, "version": 3}], "historical upgrade changed application version or replica checkpoint")
        require(case["integrity"] == {"integrity_check": "ok"}, "historical SQLite integrity failed")
        window_binding_result(case["windowBindings"])
        require(case["tombstones"] == [{"scope": owner, "entity": "tasks", "id": "gone", "value": "null"}
                                       for owner in (["default"] if case["variant"] == "unscoped" else ["alice", "bob"])], "historical tombstones were lost or crossed owner scope")
        require(case["entries"] == expected, "historical import changed accepted intent identity or state")
        require(case["restored"] == {owner: [e for e in expected if e["scope"] == owner] for owner in ("alice", "bob")}, "historical queue restore changed delivery state or ordering")
        require(case["nextId"] == (7 if phase == "historical-recover" else 8), "historical allocator reused a consumed position")
        require(case["migrated"] == {"imported": 6 if phase == "historical-recover" else 0, "skipped": 0, "invalid": 0}, "historical migration duplicated or dropped intents")
        require(case["imported"] == {"imported": 0, "skipped": 6, "invalid": 0}, "historical retry duplicated or dropped intents")
        require(case["sourceBefore"] == (case["legacy"] if phase == "historical-recover" else None)
                and case["sourceAfter"] is None, "historical legacy cleanup crossed durable import or failed to complete")
        require(case["beforeImport"] == ([] if phase == "historical-recover" else expected), "interrupted historical journal import left partial accepted work")
        require(set(case["owners"]) == {"default", "alice", "bob"}, "historical owner matrix incomplete")
        for owner, view in case["owners"].items():
            authorized = owner == "default" if case["variant"] == "unscoped" else owner in ("alice", "bob")
            if not authorized:
                require(view["snapshot"] is None and view["selected"]["rows"] == [], "historical data crossed owner scope")
                continue
            row_id = "b" if owner == "bob" else "a"
            row = {"id": row_id, "parentId": "p", "title": owner + "\x00private"}
            snapshot = view["snapshot"]
            cursor = {"epoch": "historical-" + owner, "revision": 7}
            require(snapshot["entities"] == {"tasks": {row_id: row}} and snapshot["cursor"] == cursor, "historical rows or checkpoint changed")
            signature = 'tasks.all\x00{"parentId":"p"}'
            require(list(snapshot["liveQueries"]) == [signature], "historical NUL signature was truncated")
            window = snapshot["liveQueries"][signature]
            require(window["ids"] == [row_id] and window["completeness"] == "complete" and window["cursor"] == cursor, "historical window lost completeness, membership or cursor")
            require(view["selected"] == {"rows": [row], "complete": True}, "historical indexed predicate lost completeness")


def historical_transition(previous, phase, result):
    """Compare separate processes' observations, including raw rollback bytes."""
    if phase == "historical-seed":
        return
    seed = {case["variant"]: case for case in previous["historical-seed"]["cases"]}
    if phase in ("historical-replica-held", "historical-journal-held"):
        require(result["original"] == seed["unscoped"]["raw"], "historical migration changed its original durable witness")
        return
    for case in result["cases"]:
        require(case["legacy"] == seed[case["variant"]]["legacy"], "historical migration changed original journal bytes")
        if phase == "historical-reopen":
            recovered = next(c for c in previous["historical-recover"]["cases"] if c["variant"] == case["variant"])
            require(case["owners"] == recovered["owners"] and case["entries"] == recovered["entries"], "historical restart changed committed replica or intent state")


def validate_phase(events, phase, run_id, bundle, tool):
    require(phase in TERMINAL, "unknown phase")
    require(events, "no native observations")
    allowed = {"startup", "environment", TERMINAL[phase]}
    allowed.update({"running", "case"} if phase == "all" else {"committed-before-crash"} if phase == "crash-seed" else set())
    for event in events:
        require(event.get("type") != "fatal", "native fixture failed: " + event.get("message", ""))
        require(event.get("type") in allowed, "unknown native event")
        require(event.get("runId") == run_id and event.get("phase") == phase, "stale or foreign fixture event")
        require(event.get("bundleSha256") == bundle, "wrong source bundle")
    startup = [e for e in events if e["type"] == "startup"]
    require(len(startup) <= 1 and (not startup or events[0] == startup[0]), "repeated or misplaced native startup")
    environment = [e for e in events if e["type"] == "environment"]
    require(len(environment) == 1, "one native environment required per phase")
    env = environment[0]
    require(env["platform"] == "android" and env["android"] == tool["androidApi"], "wrong native platform")
    require(env["hermes"] is True and env["sqlite"] == tool["sqliteVersion"], "wrong native engine")
    terminal = [e for e in events if e["type"] == TERMINAL[phase]]
    require(len(terminal) == 1 and events[-1] == terminal[0], "incomplete or repeated native phase")
    result = terminal[0]
    if phase.startswith("historical-"):
        historical_result(result, phase)
    elif phase == "all":
        cases = [e for e in events if e["type"] == "case"]
        require(len(cases) == len(CASES), "missing native cases")
        require({(e["variant"], e["case"]) for e in cases} == CASES, "duplicate or unknown native case")
        require(result["passed"] is True and result["baselineCounterexamples"] == 3 and result["fixedCases"] == 6, "wrong completion declaration")
        for event in cases:
            value, name, baseline = event["result"], event["case"], event["variant"] == "baseline"
            if name == "replicaOutbox":
                rows(value["saved"], 8, not baseline)
                if baseline:
                    require(value["outcomes"] == ["rejected", "rejected"] and value["entries"] == [], "baseline did not lose both operations")
                    require("BEGIN" in value["during"]["statements"] and "ROLLBACK" in value["during"]["statements"], "baseline ownership conflict not witnessed")
                else:
                    require(value["outcomes"] == ["fulfilled", "fulfilled"], "fixed operations failed")
                    require(value["during"] == {"settled": False, "statements": []}, "second operation entered owned handle")
                    require(len(value["entries"]) == 1 and value["entries"][0]["idempotencyKey"] == "intent", "outbox write missing")
            elif name == "twoOutboxes":
                require(value["outcomes"] == (["rejected", "rejected"] if baseline else ["fulfilled", "fulfilled"]), "wrong outbox outcomes")
                require(value["identities"] == ([] if baseline else [[1, "first"], [2, "second"]]), "wrong outbox identities")
            elif name == "reader":
                require(value["before"]["published"] is baseline, "read escaped ownership")
                require(value["saved"]["cursor"]["revision"] == (7 if baseline else 8), "wrong reader checkpoint")
                require(value["saved"]["entities"]["tasks"]["a"]["name"] == "Changed", "read counterexample missing")
                if not baseline:
                    require(value["before"]["reads"] == 0 and value["write"] == "fulfilled", "read ran before commit")
            elif name == "rejectedOwner":
                require(value == {"write": "rejected", "next": "fulfilled", "revision": 8}, "rejection poisoned handle")
            elif name == "migrations":
                require(value == {"initialized": 4, "ids": [1, 2], "revision": 7}, "shared migration failed")
            elif name == "independent":
                require(value == {"independentCommitted": True}, "independent handle blocked")
    elif phase in ("journal-corrupt-seed", "journal-corrupt-recover"):
        corrupt_result(result, phase)
    elif phase == "crash-seed":
        committed = [e for e in events if e["type"] == "committed-before-crash"]
        require(len(committed) == 1, "committed intent witness missing")
        durable(committed[0])
        require(committed[0]["id"] == result["id"] == 1 and committed[0]["key"] == result["key"] == "native-intent", "wrong crash identity")
        require(result["successfulEntityStatement"] is True, "no successful native write before crash")
    elif phase == "recover":
        rows(result["saved"], 7, False)
        require(len(result["recovered"]) == 1, "recovered queue missing")
        intent(result["recovered"][0], "pending")
        require(result["nextId"] == 2 and result["committedRevision"] == 8, "sequence or retry commit failed")
        durable(result)
    else:
        require(result["passed"] is True, "restart failed")
        rows(result["saved"], 8, True)
        require(len(result["entries"]) == 2, "committed intents lost after restart")
        intent(result["entries"][0], "pending")
        intent(result["entries"][1], "pending", 2, "after-crash")
        durable(result)
    return result
