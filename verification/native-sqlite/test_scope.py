import unittest
from scope import relevant


class NativeScopeTests(unittest.TestCase):
    def test_native_execution_dependencies_require_native_checks(self):
        for path in ["packages/expo-sqlite/src/index.ts", "packages/client/src/outbox.ts",
                     "packages/local-runtime/src/read-view.ts", "packages/protocol/src/index.ts",
                     "packages/module-sdk/package.json", "pnpm-lock.yaml", "package.json",
                     "verification/native-sqlite/run.py", ".github/workflows/runtime-regression.yml"]:
            with self.subTest(path=path):
                self.assertTrue(relevant(path))

    def test_unrelated_server_and_product_ui_do_not_require_storage_fixture(self):
        for path in ["rust/crates/runtime/src/lib.rs", "apps/dashboard/src/App.tsx", "docs/architecture.md"]:
            with self.subTest(path=path):
                self.assertFalse(relevant(path))


if __name__ == "__main__":
    unittest.main()
