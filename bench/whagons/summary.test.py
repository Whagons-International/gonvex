"""Capacity qualification must fail closed even when the client reports success."""
import contextlib
import datetime
import io
import json
import pathlib
import runpy
import tempfile
import types
import unittest
from unittest.mock import patch

class Qualification(unittest.TestCase):
    def summarize(self, flags=None, telemetry=True):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            case = root / 'case'
            case.mkdir()
            start = 1700000000000
            (root / 'case.json').write_text(json.dumps({
                'clientSloPassed': True, 'targetUsers': 1, 'connectedUsers': 1,
                'steadyStartMs': start, 'steadyEndMs': start + 180000,
                'steadySeconds': 180, **(flags or {}),
            }))
            if telemetry:
                pg, host, metrics = [], [], []
                for seconds in [0, 179]:
                    at = start / 1000 + seconds
                    pg.append({'at': datetime.datetime.fromtimestamp(at, datetime.timezone.utc).isoformat(), 'connections': 1, 'sessions': 1})
                    host.append({'at': at, 'memory_full_avg10': 0,
                                 **{name: 'user_usec 1\nsystem_usec 1\n1024' for name in ['pg', 'runtime', 'generator']}})
                    metrics.append({'at': at * 1000, 'metrics': {'websocket': {'accounts': 1}, 'database': {'waitCount': 1, 'waitDurationMs': 1}}})
                for path, rows in [(case / 'postgres.jsonl', pg), (case / 'host.jsonl', host), (root / 'case.json.metrics.jsonl', metrics)]:
                    path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            output = io.StringIO()
            clean_logs = types.SimpleNamespace(stdout='', stderr='', returncode=0)
            with patch('sys.argv', ['summarize.py', directory, 'case']), patch('subprocess.run', return_value=clean_logs), contextlib.redirect_stdout(output):
                runpy.run_path(str(pathlib.Path(__file__).with_name('summarize.py')), run_name='__main__')
            return json.loads(output.getvalue())[0]['capacityPassed']

    def test_complete_case_with_telemetry_passes(self):
        self.assertTrue(self.summarize())

    def test_missing_telemetry_never_passes(self):
        self.assertFalse(self.summarize(telemetry=False))

    def test_partial_aborted_and_short_plateau_never_pass(self):
        for flags in [{'partial': True}, {'aborted': True}, {'steadyEndMs': 1700000010000}]:
            with self.subTest(flags=flags):
                self.assertFalse(self.summarize(flags))

if __name__ == '__main__':
    unittest.main()
