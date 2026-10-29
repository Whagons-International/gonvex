"""Validate observations, not just a fixture's self-reported pass count."""

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


def validate_phase(events, phase, run_id, bundle, tool):
    require(phase in TERMINAL, "unknown phase")
    require(events, "no native observations")
    allowed = {"environment", TERMINAL[phase]}
    allowed.update({"running", "case"} if phase == "all" else {"committed-before-crash"} if phase == "crash-seed" else set())
    for event in events:
        require(event.get("type") != "fatal", "native fixture failed: " + event.get("message", ""))
        require(event.get("type") in allowed, "unknown native event")
        require(event.get("runId") == run_id and event.get("phase") == phase, "stale or foreign fixture event")
        require(event.get("bundleSha256") == bundle, "wrong source bundle")
    environment = [e for e in events if e["type"] == "environment"]
    require(len(environment) == 1, "one native environment required per phase")
    env = environment[0]
    require(env["platform"] == "android" and env["android"] == tool["androidApi"], "wrong native platform")
    require(env["hermes"] is True and env["sqlite"] == tool["sqliteVersion"], "wrong native engine")
    terminal = [e for e in events if e["type"] == TERMINAL[phase]]
    require(len(terminal) == 1 and events[-1] == terminal[0], "incomplete or repeated native phase")
    result = terminal[0]
    if phase == "all":
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
