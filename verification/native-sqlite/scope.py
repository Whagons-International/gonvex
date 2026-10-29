"""Fail closed when native execution inputs change; preserve rename originals."""
import os
from pathlib import Path
import re
import subprocess


def relevant(path):
    return bool(
        re.match(r"^packages/(client|expo-sqlite|protocol|local-runtime|module-sdk)/(src/|package\.json$|tsconfig)", path)
        or path.startswith("verification/native-sqlite/")
        or path in ("package.json", "pnpm-lock.yaml", "pnpm-workspace.yaml",
                    ".github/workflows/runtime-regression.yml",
                    ".github/workflows/native-sqlite-verification.yml")
    )


def main():
    required = True
    if os.environ.get("NATIVE_EVENT") == "pull_request":
        base, head = os.environ["NATIVE_BASE"], os.environ["NATIVE_HEAD"]
        assert re.fullmatch(r"[a-f0-9]{40}", base) and re.fullmatch(r"[a-f0-9]{40}", head), "invalid immutable diff endpoints"
        # Disabling rename detection produces both deletion and addition paths.
        files = subprocess.check_output(["git", "diff", "--no-renames", "--name-only", "-z", f"{base}...{head}"]).decode().split("\0")
        required = any(relevant(path) for path in files if path)
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        output.write(f"required={str(required).lower()}\n")
    print(f"Native SQLite execution required: {required}")


if __name__ == "__main__":
    main()
