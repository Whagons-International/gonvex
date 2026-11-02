import copy
import json
from pathlib import Path
import unittest

from contract import TERMINAL, historical_result, historical_transition, validate_phase

HERE = Path(__file__).resolve().parent
VECTORS = json.loads((HERE / "historical-node-vectors.json").read_text())
PHASES = VECTORS["phases"]
TOOL = json.loads((HERE / "toolchain.json").read_text())


class HistoricalContractTests(unittest.TestCase):
    def test_required_native_history_inventory(self):
        self.assertEqual([name for name in TERMINAL if name.startswith("historical-")], [
            "historical-seed", "historical-replica-held", "historical-journal-held", "historical-recover", "historical-reopen"])

    def test_observed_node_postimages_and_synthetic_native_envelopes(self):
        # These are actual Node SQLite post-images. Only the validator envelope
        # is synthetic. This test is not evidence of Android/Hermes execution.
        self.assertIn("not Expo/Hermes", VECTORS["origin"]["driver"])
        previous = {}
        for phase, result in PHASES.items():
            historical_result(result, phase)
            historical_transition(previous, phase, result)
            environment = {"type": "environment", "runId": "unit", "phase": phase, "bundleSha256": "unit",
                           "platform": "android", "android": TOOL["androidApi"], "hermes": True, "sqlite": TOOL["sqliteVersion"]}
            terminal = dict(copy.deepcopy(result), runId="unit", phase=phase, bundleSha256="unit")
            validate_phase([environment, terminal], phase, "unit", "unit", TOOL)
            previous[phase] = result

    def test_recovery_rejects_identity_order_acceptance_and_replica_loss(self):
        for mutate in [
            lambda c: c["entries"][2].update(state="pending"),
            lambda c: c["entries"][2].update(idempotencyKey="new"),
            lambda c: c["entries"][2].pop("awaitingReplica"),
            lambda c: c["entries"].reverse(),
            lambda c: c["restored"]["alice"].pop(),
            lambda c: c["tombstones"].clear(),
            lambda c: c.update(nextId=6),
            lambda c: c["beforeImport"].append(c["entries"][0]),
            lambda c: c["appVersion"].update(user_version=3),
            lambda c: c["owners"]["default"]["snapshot"]["entities"]["tasks"].clear(),
            lambda c: next(iter(c["owners"]["default"]["snapshot"]["liveQueries"].values())).update(ids=[]),
            lambda c: next(iter(c["owners"]["default"]["snapshot"]["liveQueries"].values())).update(completeness="partial"),
            lambda c: c["owners"]["default"]["selected"].update(complete=False),
            lambda c: c["owners"]["alice"].update(snapshot=c["owners"]["default"]["snapshot"]),
            lambda c: c["owners"].pop("alice"),
            lambda c: c.update(sourceBefore=None),
            lambda c: c.update(sourceAfter=c["legacy"]),
            lambda c: c["sync"].update(synchronous=0),
        ]:
            result = copy.deepcopy(PHASES["historical-recover"])
            mutate(result["cases"][0])
            with self.subTest(mutation=mutate), self.assertRaises(AssertionError):
                historical_result(result, "historical-recover")

    def test_both_native_write_boundaries_are_required(self):
        for phase, flag in [("historical-replica-held", "successfulEntityStatement"),
                            ("historical-journal-held", "successfulAcceptedStatement")]:
            result = copy.deepcopy(PHASES[phase])
            result[flag] = False
            with self.subTest(phase=phase), self.assertRaises(AssertionError):
                historical_result(result, phase)

    def test_independent_process_witnesses_detect_raw_rollback_and_source_changes(self):
        result = copy.deepcopy(PHASES["historical-journal-held"])
        result["beforeMigration"]["tables"]["entities"] = []
        with self.assertRaisesRegex(AssertionError, "interrupted historical replica"):
            historical_result(result, "historical-journal-held")
        result["original"] = result["beforeMigration"]
        with self.assertRaisesRegex(AssertionError, "original durable witness"):
            historical_transition(PHASES, "historical-journal-held", result)
        result = copy.deepcopy(PHASES["historical-recover"])
        result["cases"][0]["legacy"] += " "
        result["cases"][0]["sourceBefore"] = result["cases"][0]["legacy"]
        historical_result(result, "historical-recover")
        with self.assertRaisesRegex(AssertionError, "original journal bytes"):
            historical_transition(PHASES, "historical-recover", result)

    def test_native_key_writer_observations_cannot_drop_suffixes_or_alias_peers(self):
        for mutate in [
            lambda value: value["modes"].pop(),
            lambda value: value["modes"][0]["full"].__setitem__(0, "tasks"),
            lambda value: value["modes"][1]["indexed"][0].update(bytes="7461736B73"),
            lambda value: value["modes"][2]["points"][1]["window"].update(signature="tasks"),
            lambda value: value["beforeRemove"]["liveQueries"]["tasks\x00b"].update(ids=[]),
            lambda value: value["afterRemove"].update(liveQueries={}),
        ]:
            result = copy.deepcopy(PHASES["historical-recover"])
            mutate(result["cases"][0]["windowBindings"])
            with self.assertRaises(AssertionError):
                historical_result(result, "historical-recover")
        result = copy.deepcopy(PHASES["historical-seed"])
        result["cases"][0]["raw"]["tables"]["queries"][0]["signature"] = "7461736B732E616C6C"
        with self.assertRaisesRegex(AssertionError, "seed key bytes"):
            historical_result(result, "historical-seed")

    def test_fresh_reopen_cannot_silently_reinitialize_import_or_allocation(self):
        for mutate in [lambda c: c.update(imported={"imported": 6, "skipped": 0, "invalid": 0}),
                       lambda c: c.update(nextId=7)]:
            result = copy.deepcopy(PHASES["historical-reopen"])
            mutate(result["cases"][0])
            with self.assertRaises(AssertionError):
                historical_result(result, "historical-reopen")


if __name__ == "__main__":
    unittest.main()
