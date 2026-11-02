#!/usr/bin/env python3
"""Require SDK recovery through production backend contract and SQL replacement."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import uuid

from check_restart import command, postgres_ready, require
from check_sdk import digest, ROOT

TEST = 'tests::receipt_guarantee_tests::authenticated_sdk_module_upgrade_recovery'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--build-metadata', type=Path, required=True)
    parser.add_argument('--module-host', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    artifacts = set()
    for line in args.build_metadata.read_text().splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if record.get('reason') == 'compiler-artifact' and record.get('target', {}).get('name') == 'gonvex_runtime' and record.get('profile', {}).get('test') and record.get('executable'):
            artifacts.add(Path(record['executable']).resolve())
    require(len(artifacts) == 1, 'one exact runtime test artifact required')
    binary, host = artifacts.pop(), args.module_host.resolve()
    require(binary.is_file() and host.is_file(), 'native artifacts missing')
    native = {'runtime': digest(binary), 'moduleHost': digest(host)}
    paths = ['verification/backend-receipts/check_module_upgrade.py', 'verification/backend-receipts/check_restart.py',
             'verification/backend-receipts/check_sdk.py', 'verification/backend-receipts/sdk_module_upgrade.mjs',
             'verification/backend-receipts/browser_module_upgrade_entry.mjs', '.github/workflows/runtime-regression.yml',
             'rust/crates/runtime/src/tests/receipt_guarantee_tests.rs', 'rust/crates/runtime/src/lib.rs',
             'rust/crates/runtime/src/execution.rs', 'rust/crates/runtime/src/modules.rs', 'rust/crates/postgres/src/lib.rs',
             'rust/Cargo.lock', 'pnpm-lock.yaml']
    paths += [str(path.relative_to(ROOT)) for path in (ROOT / 'packages/client/src').glob('*.ts') if not path.name.endswith('.test.ts')]
    paths += [str(path.relative_to(ROOT)) for package in ['client', 'protocol', 'module-sdk', 'local-runtime']
              for path in ROOT.glob(f'packages/{package}/dist/**/*.js')]
    sources = {name: digest(ROOT / name) for name in paths}
    env = dict(os.environ)
    env.pop('GONVEX_SDK_MODULE_UPGRADE_SCRIPT', None)
    env.pop('GONVEX_SDK_MODULE_UPGRADE_DAMAGE', None)
    env.pop('GONVEX_SDK_MODULE_UPGRADE_SCENARIO', None)
    missing = subprocess.run([str(binary), TEST, '--ignored', '--exact', '--nocapture'], env=env,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=20)
    (output / 'missing-coordinator.log').write_text(missing.stdout)
    require(missing.returncode != 0 and 'module upgrade requires its coordinator' in missing.stdout,
            'missing coordinator must fail explicitly')
    container = process = None
    try:
        container = command(['docker', 'run', '--detach', '--name', 'gonvex-module-upgrade-' + uuid.uuid4().hex[:12],
                             '--memory', '512m', '--cpus', '1', '--pids-limit', '128', '--shm-size', '64m',
                             '-p', '127.0.0.1::5432', '-e', 'POSTGRES_PASSWORD=fixture', '-e', 'POSTGRES_DB=postgres', 'postgres:16-alpine'])
        postgres_ready(container)
        port = command(['docker', 'port', container, '5432/tcp']).rsplit(':', 1)[1]
        settings = command(['docker', 'exec', container, 'psql', '-h', '127.0.0.1', '-U', 'postgres', '-Atc',
                            "SELECT name || '=' || setting FROM pg_settings WHERE name IN ('fsync','full_page_writes','synchronous_commit') ORDER BY name"]).splitlines()
        require(settings == ['fsync=on', 'full_page_writes=on', 'synchronous_commit=on'], 'durable PostgreSQL settings required')
        env.update(GONVEX_RECEIPT_TEST_REQUIRE_NATIVE='1', GONVEX_TEST_MODULE_HOST_BINARY=str(host),
                   GONVEX_TEST_POSTGRES_URL=f'postgres://postgres:fixture@127.0.0.1:{port}/postgres?sslmode=disable',
                   GONVEX_SDK_MODULE_UPGRADE_SCRIPT=str(ROOT / 'verification/backend-receipts/sdk_module_upgrade.mjs'))
        cases = [
            ('positive', 'lost-reply', None),
            ('counter-failure', 'counter-failure', None),
            ('missing-original-receipt-control', 'lost-reply', 'omit-original-receipt'),
            ('counter-barrier-source-control', 'counter-failure', 'omit-replica-barrier'),
        ]
        for name, scenario, damage in cases:
            env['GONVEX_SDK_RECEIPT_OUTPUT'] = str(output / name)
            env['GONVEX_SDK_MODULE_UPGRADE_SCENARIO'] = scenario
            env.pop('GONVEX_SDK_MODULE_UPGRADE_DAMAGE', None)
            if damage:
                env['GONVEX_SDK_MODULE_UPGRADE_DAMAGE'] = damage
            with (output / (name + '.log')).open('w') as log:
                process = subprocess.Popen([str(binary), TEST, '--ignored', '--exact', '--nocapture', '--test-threads=1'],
                                           env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                code = process.wait(timeout=120)
            log = (output / (name + '.log')).read_text()
            if not damage:
                require(code == 0 and '1 passed; 0 failed; 0 ignored;' in log, 'backend module upgrade composition failed')
                result = json.loads((output / name / 'receipt.json').read_text())
                require(result['scenario'] == scenario and result['killSignal'] == 'SIGKILL'
                        and result['committedIntentOrdinaryRedeliveries'] == 0
                        and result['orderedNewIntentDeliveries'] == 1 and result['downgradeRefused'],
                        'module upgrade omitted a required recovery boundary')
                require(result['replacement']['after']['clientContract'] == 2
                        and result['replacement']['before']['clientContract'] == 1
                        and 'write' not in result['replacement']['after']['functions']
                        and result['replacement']['renamedColumnValue'] == 7, 'actual backend replacement missing')
                if scenario == 'lost-reply':
                    positive = result
                else:
                    counter_failure = result
                    fault = result['counterFailure']
                    held, failed = fault['accepted'], fault['failed']
                    require(fault['independentJournalReader'] and fault['actualAcceptanceDelivered'],
                            'counter case omitted the actual durable received acceptance')
                    require(held['entries'][0]['state'] == failed['entries'][0]['state'] == 'committed'
                            and held['entries'][0]['awaitingReplica'] and failed['entries'][0]['awaitingReplica'],
                            'counter failure did not retain its durable unresolved acceptance')
                    require(any(row['key'] == 'sequence' and row['value'] == '0' for row in failed['rawReplica']['meta'])
                            and failed['rawReplica'] == held['counterFault']['corruptedReplica'],
                            'counter failure changed the original replica scope or omitted its actual fault')
                    require(any(failure['method'] != 'advanceWatermark' and 'Invalid replica storage sequence' in failure['error']
                                for failure in failed['storageFailures']), 'counter case did not exercise a data persistence rejection')
                    require(not failed['acknowledgements'] and not failed['retirementAttempts'],
                            'counter case acknowledged or retired unpersisted data')
                    require(fault['repaired'] == held['counterFault']['beforeReplica'], 'counter repair changed more than the injected fault')
                    require(result['upgraded']['entries'][0]['state'] == 'committed'
                            and result['upgraded']['entries'][0]['awaitingReplica'], 'upgrade rearmed accepted counter-failure intent')
            elif damage == 'omit-original-receipt':
                require(code != 0 and 'backend module upgrade lost original receipt identity' in log,
                        'receipt-identity source control did not fail its intended backend assertion')
                failure = json.loads((output / name / 'failure.json').read_text())
                mutation = failure['mutation']
                require(mutation['active'] and mutation['mutations'] == 1 and mutation['function'] == 'migrateClientData'
                        and mutation['originalSha256'] == digest(ROOT / 'packages/client/src/client-upgrades.ts')
                        and mutation['mutatedSha256'] != mutation['originalSha256'], 'wrong migration source mutation')
                upgraded = json.loads((output / name / 'upgraded.json').read_text())
                require(len(upgraded['entries']) == 2 and all(not entry.get('receiptPath') for entry in upgraded['entries']),
                        'receipt namespace control did not reach the durable migrated journal')
                state = failure['lastState']
                lookups = [call['message'] for call in state['calls'] if call['owner'] == 'main'
                           and call['message']['type'] == 'query.call' and call['message']['path'] == '__gonvex.reducerReceipt']
                require(any(call['args']['receiptPath'] == 'writeV2' for call in lookups), 'wrong lookup namespace was not exercised')
                require(any(event['owner'] == 'main' and event['message']['type'] == 'query.error'
                            and 'refusing to replay' in event['message']['error']
                            and any(call['id'] == event['message']['id'] for call in lookups) for event in state['events']),
                        'negative control lacks actual backend namespace rejection')
            else:
                require(code != 0 and 'accepted browser intent passed durable replica data' in log,
                        'counter barrier source control did not fail the actual data-ordering oracle')
                failure = json.loads((output / name / 'failure.json').read_text())
                barrier_mutation = failure['mutation']
                require(barrier_mutation['active'] and barrier_mutation['mutations'] == 1
                        and barrier_mutation['method'] == 'registerCall'
                        and barrier_mutation['originalSha256'] == digest(ROOT / 'packages/client/src/index.ts')
                        and barrier_mutation['originalSha256'] != barrier_mutation['mutatedSha256'],
                        'counter source control compiled the wrong watermark mutation')
                state = failure['lastState']
                original = json.loads((output / name / 'counter-fault-armed.json').read_text())
                require(any(row['key'] == 'sequence' and row['value'] == '0' for row in original['corruptedReplica']['meta']),
                        'barrier control did not commit the counter fault')
                require(state['lostResult']['committedRevision'] > 0 and state['rawReplica'] == original['corruptedReplica']
                        and not state.get('row') and (state['acknowledgements'] or state['retirementAttempts']),
                        'barrier control lacks actual acknowledgement past missing durable data')
        for name, expected in sources.items():
            require(digest(ROOT / name) == expected, 'source changed during module upgrade: ' + name)
        require(digest(binary) == native['runtime'] and digest(host) == native['moduleHost'], 'native artifacts changed')
        receipt = {'nativeArtifacts': native, 'sources': sources, 'postgresSettings': settings,
                   'postgresImage': command(['docker', 'inspect', container, '--format', '{{.Image}}']),
                   'positive': positive, 'counterFailure': counter_failure,
                   'sourceReceiptIdentityControlRejected': True, 'sourceMutation': mutation,
                   'sourceReplicaBarrierControlRejected': True, 'replicaBarrierMutation': barrier_mutation,
                   'missingCoordinatorRejected': True,
                   'boundary': 'One authenticated project and tenant, real production sync/SQL/V8 contract replacement and Chromium persisted application contract migration. Native app driver and arbitrary upgrade policy remain separate.'}
        (output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
        print('Backend contract/SQL replacement, durable accepted counter failure, browser crash, ordered recovery and both source controls passed')
    finally:
        if process:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)
        if container:
            subprocess.run(['docker', 'rm', '--force', '--volumes', container], check=True, stdout=subprocess.DEVNULL, timeout=20)


if __name__ == '__main__':
    main()
