import copy
import json
from pathlib import Path
import unittest

from contract import validate_phase

HERE = Path(__file__).resolve().parent
VECTORS = json.loads((HERE / "test-vectors.json").read_text())["phases"]
TOOL = json.loads((HERE / "toolchain.json").read_text())
RUN, BUNDLE = "contract-unit", "b" * 64


def events(phase):
    return [dict(copy.deepcopy(event), runId=RUN, phase=phase, bundleSha256=BUNDLE) for event in VECTORS[phase]]


class NativeContractTests(unittest.TestCase):
    def verify(self, phase, values):
        return validate_phase(values, phase, RUN, BUNDLE, TOOL)

    def test_observed_native_postconditions_pass_each_phase(self):
        for phase in VECTORS:
            with self.subTest(phase=phase):
                self.verify(phase, events(phase))

    def test_empty_truncated_and_duplicate_cases_fail(self):
        for values in [[], events("all")[:-1], events("all")[:2] + events("all")[1:]]:
            with self.subTest(values=len(values)), self.assertRaises(AssertionError):
                self.verify("all", values)

    def test_wrong_owner_phase_bundle_and_engine_fail(self):
        for field, value in [("runId", "old"), ("phase", "recover"), ("bundleSha256", "old"), ("hermes", False), ("sqlite", "unknown"), ("android", 35)]:
            values = events("all")
            values[0][field] = value
            with self.subTest(field=field), self.assertRaises(AssertionError):
                self.verify("all", values)

    def test_generic_tool_failure_is_not_a_counterexample(self):
        values = events("all")
        values.insert(1, dict(values[0], type="fatal", message="tool exited"))
        with self.assertRaises(AssertionError):
            self.verify("all", values)
        values = events("all")
        values[1]["result"]["saved"]["cursor"]["revision"] = 7
        with self.assertRaises(AssertionError):
            self.verify("all", values)

    def test_wrong_recovery_rows_identity_arguments_or_durability_fail(self):
        for mutate in [
            lambda result: result["saved"]["cursor"].update(revision=8),
            lambda result: result["recovered"][0].update(idempotencyKey="new-key"),
            lambda result: result["recovered"][0].update(args={"key": "changed"}),
            lambda result: result["sync"].update(synchronous=0),
        ]:
            values = events("recover")
            mutate(values[-1])
            with self.assertRaises(AssertionError):
                self.verify("recover", values)

    def test_missing_crash_write_witness_or_committed_intent_fails(self):
        values = events("crash-seed")
        values[-1]["successfulEntityStatement"] = False
        with self.assertRaises(AssertionError):
            self.verify("crash-seed", values)
        with self.assertRaises(AssertionError):
            self.verify("crash-seed", events("crash-seed")[::2])


if __name__ == "__main__":
    unittest.main()
