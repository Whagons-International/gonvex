#!/usr/bin/env python3
"""Derive upgrade fixtures from pinned Rust source without running Rust or SQL."""

import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

PIN = "8bae698880d6a7e63472596b1a22c5dc1ced0ebc"
SOURCE_PATH = "rust/crates/postgres/src/provision.rs"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def rust_format(source, marker):
    """Read an ordinary Rust string literal used by the pinned format! call."""
    tail = source.split(marker, 1)[1]
    token = re.search(r'format!\(\s*("(?:\\.|[^"\\])*")', tail)[1]
    # These pinned literals use only escapes shared by Rust and JSON.
    return json.loads(token)


def main():
    checkout = Path(sys.argv[1]).resolve()
    destination = Path(__file__).resolve().parent
    source_bytes = subprocess.check_output(
        ["git", "show", f"{PIN}:{SOURCE_PATH}"], cwd=checkout
    )
    source = source_bytes.decode("utf-8")
    infrastructure = source.split(
        'const SYNC_INFRASTRUCTURE_SQL: &str = r#"', 1
    )[1].split('"#;', 1)[0]
    installer = source.split("async fn install_table_trigger(", 1)[1].split(
        "\nfn bootstrap_digest(", 1
    )[0]
    template = installer.split('let statement = format!(\n        r#"', 1)[1].split(
        '"#,', 1
    )[0]

    table, key, columns = "tasks", "id", ["id", "title"]
    chunk_size = int(re.search(r"const JSONB_BUILD_OBJECT_COLUMNS: usize = (\d+);", source)[1])
    artifact_format = rust_format(installer, "let artifact = |suffix: &str| {")
    quote_format = rust_format(installer, "let quote =")
    literal_format = rust_format(installer, "let literal =")
    pair_format = rust_format(installer, ".map(|column|")
    # The next format! after the pair join wraps it in jsonb_build_object.
    object_format = rust_format(installer, '.join(",");')

    def quote(identifier):
        return quote_format.format(identifier=identifier)

    def literal(value):
        return literal_format.format(value.replace("'", "''"))

    def artifact(suffix):
        name = artifact_format.format(table=table, suffix=suffix)
        if len(name.encode("utf-8")) > 63:
            raise ValueError("This derivation only covers the pinned short artifact names")
        return quote(name)

    def projection(alias):
        objects = []
        for offset in range(0, len(columns), chunk_size):
            pairs = ",".join(
                pair_format.format(literal(column), alias, quote(column))
                for column in columns[offset : offset + chunk_size]
            )
            objects.append(object_format.format(pairs=pairs))
        return "||".join(objects) if objects else "jsonb_build_object()"

    arguments = {
        "stage_function": artifact("stage"),
        "stage_trigger": artifact("stage_trigger"),
        "finalize_trigger": artifact("finalize_trigger"),
        "table_ident": quote(table),
        "key_ident": quote(key),
        "old_projection": projection("OLD"),
        "new_projection": projection("NEW"),
        "table_literal": literal(table),
    }
    # This pinned Rust raw template has named substitutions and no other
    # formatting specifiers. Python preserves all surrounding bytes here.
    tasks = template.format_map(arguments)
    outputs = {
        "infrastructure.sql": infrastructure.encode("utf-8"),
        "tasks.sql": tasks.encode("utf-8"),
    }
    manifest = {
        "source_commit": PIN,
        "source_path": SOURCE_PATH,
        "source_sha256": digest(source_bytes),
        "derivation_script_sha256": digest(Path(__file__).read_bytes()),
        "stage_installer_sha256": digest(
            ("async fn install_table_trigger(" + installer).encode("utf-8")
        ),
        "stage_template_sha256": digest(template.encode("utf-8")),
        "inputs": {"table": table, "key": key, "columns": columns},
        "format_arguments": arguments,
        "fixtures": {
            name: {"bytes": len(data), "sha256": digest(data)}
            for name, data in outputs.items()
        },
        "digest_contract": "lowercase SHA-256 hex over the exact UTF-8 SQL bytes, with no trimming",
    }
    for name, data in outputs.items():
        (destination / name).write_bytes(data)
    (destination / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
