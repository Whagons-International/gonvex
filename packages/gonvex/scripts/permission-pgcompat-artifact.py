"""Compose an offline experimental SDK extension from an integrity-verified PG SDK.

No PG SDK implementation is rebuilt from the SQLite branch. The only existing
file changes are version and literal index export appends. Requires a published
baseline tarball, its frozen-lock SRI, and the frozen compiled extension pair.
"""
import argparse, base64, hashlib, io, json, pathlib, re, tarfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--baseline', required=True)
p.add_argument('--integrity', required=True)
p.add_argument('--compiled-sdk', required=True)
p.add_argument('--output', required=True)
p.add_argument('--version', required=True)
a = p.parse_args()
sha = lambda b: hashlib.sha256(b).hexdigest()
raw = pathlib.Path(a.baseline).read_bytes()
assert a.integrity == 'sha512-' + base64.b64encode(hashlib.sha512(raw).digest()).decode(), 'baseline SRI mismatch'
assert re.fullmatch(r'0\.5\.2-permission-pgcompat\.[0-9]+', a.version), 'explicit experimental version required'
with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as t:
    baseline = {}
    for member in t.getmembers():
        if member.isdir(): continue
        assert member.isfile() and member.name.startswith('package/') and '..' not in pathlib.PurePosixPath(member.name).parts
        baseline[member.name[8:]] = t.extractfile(member).read()
meta = json.loads(baseline['package.json'])
assert meta['name'] == '@gonvex/module-sdk' and meta['version'] == '0.5.2-staging.31'
assert b'jsonb_populate_recordset' in baseline['dist/data-write.js'] and b'= ANY(' in baseline['dist/data.js']
files = dict(baseline)
compiled = pathlib.Path(a.compiled_sdk)
extensions = {}
for name in ['permission-primitives.js', 'permission-primitives.d.ts', 'permission-primitives.js.map', 'permission-primitives.d.ts.map']:
    src = compiled / name
    if src.exists(): extensions['dist/' + name] = src.read_bytes()
assert {'dist/permission-primitives.js', 'dist/permission-primitives.d.ts'} <= extensions.keys()
# The literal helper has no imports, re-exports, dynamic loading or require.
assert not re.search(rb'\b(?:import|require)\s*(?:[({]|[\"\'])|\bfrom\s*[\"\']', extensions['dist/permission-primitives.js'])
appends = {}
for name in ['index.js', 'index.d.ts']:
    lines = [line for line in (compiled / name).read_bytes().splitlines(keepends=True) if b'from "./permission-primitives.js"' in line]
    assert len(lines) == (1 if name.endswith('.js') else 2)
    append = b'\n' + b''.join(lines)
    files['dist/' + name] += append
    appends['dist/' + name] = append.decode()
meta['version'] = a.version
old_version = b'"version": "0.5.2-staging.31"'
assert baseline['package.json'].count(old_version) == 1
# Preserve even the baseline's lack of a final newline. Parsed field equality
# alone would miss an additional, unauthorized package-manifest byte change.
files['package.json'] = baseline['package.json'].replace(old_version, ('"version": ' + json.dumps(a.version)).encode())
files.update(extensions)
changed = [name for name in baseline if baseline[name] != files[name]]
assert sorted(changed) == ['dist/index.d.ts', 'dist/index.js', 'package.json']
oldmeta = json.loads(baseline['package.json']); oldmeta['version'] = a.version
assert oldmeta == meta  # exports/dependencies/scripts/files are preserved exactly
out = pathlib.Path(a.output); out.mkdir(parents=True, exist_ok=False)
stage = out / 'sdk-stage' / 'package'
for name, data in files.items():
    target = stage / name; target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(data)
tar = out / f'gonvex-module-sdk-{a.version}.tgz'
with tarfile.open(tar, 'w:gz') as t: t.add(stage, arcname='package')
receipt = {'schemaVersion': 1, 'sdkVersion': a.version, 'sdkTarball': str(tar.resolve()), 'sdkSHA256': sha(tar.read_bytes()),
 'baselineVersion': '0.5.2-staging.31', 'baselineIntegrity': a.integrity, 'baselineTarSHA256': sha(raw),
 'baselineFilesSHA256': {n: sha(b) for n,b in sorted(baseline.items())},
 'sdkFilesSHA256': {n: sha(b) for n,b in sorted(files.items())},
 'exactAllowedChanges': {'package.json': {'versionOnly': a.version, 'byteReplacementOnly': True, 'baselineHasFinalNewline': baseline['package.json'].endswith(b'\n')}, 'indexExportAppends': appends},
 'unchangedBaselineFiles': sorted(set(baseline) - set(changed)), 'addedFiles': sorted(extensions),
 'helperClosure': 'compiled literal helper has no imports or runtime dependencies', 'published': False}
(out / 'sdk-receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps({k: receipt[k] for k in ['sdkVersion','sdkTarball','sdkSHA256','baselineIntegrity']}))
