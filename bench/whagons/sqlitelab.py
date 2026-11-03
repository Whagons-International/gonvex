"""SQLite access for the disposable load lab. Only opens files under
$LOAD_LAB_ROOT/runtime/data, and only while the runtime container is stopped
(the runtime holds the data-directory lock and owns the files while running)."""
import json, os, sqlite3, subprocess, urllib.parse

LAB = os.path.realpath(os.environ['LOAD_LAB_ROOT'])
assert LAB.startswith('/mnt/data/'), 'LOAD_LAB_ROOT must be on /mnt/data'
HOST_STATE = os.path.join(LAB, 'runtime')        # mounted at /state in the runtime
CONTAINER_STATE = '/state'
RUNTIME = os.environ.get('LOAD_RUNTIME_CONTAINER', 'gvsq-runtime')


def runtime_running():
    out = subprocess.run(['docker', 'inspect', '-f', '{{.State.Running}}', RUNTIME],
                         capture_output=True, text=True)
    return out.stdout.strip() == 'true'


def require_stopped():
    if runtime_running():
        raise SystemExit(f'stop {RUNTIME} first: the runtime owns the SQLite files while running')


def host_path(url):
    path = urllib.parse.urlparse(url).path if url.startswith('sqlite:') else url
    if url.startswith('sqlite://'):
        path = url[len('sqlite://'):]
    if not path.startswith(CONTAINER_STATE + '/'):
        raise ValueError(f'unexpected database path {path}')
    host = os.path.realpath(os.path.join(HOST_STATE, path[len(CONTAINER_STATE) + 1:]))
    if not host.startswith(HOST_STATE + '/'):
        raise ValueError('database outside the lab')
    return host


def connect(path):
    db = sqlite3.connect(path, isolation_level=None, timeout=30)
    db.execute('PRAGMA foreign_keys=ON')
    db.execute('PRAGMA busy_timeout=30000')
    return db


def control():
    return connect(os.path.join(HOST_STATE, 'data', 'control.db'))


def tenant_db(tenant_id):
    url = control().execute(
        'SELECT database_url FROM gonvex_runtime_tenants WHERE tenant_id = ?', (tenant_id,)).fetchone()
    if not url:
        raise ValueError(f'tenant {tenant_id} not provisioned')
    return connect(host_path(url[0]))


def finalize(db, command='load-lab'):
    """Assigns one revision to changes staged by this script, exactly as the
    runtime's commit would, so they are not adopted by an unrelated commit."""
    if not db.execute("SELECT 1 FROM _gonvex_sync_changes WHERE revision IS NULL LIMIT 1").fetchone():
        return None
    revision = db.execute(
        "UPDATE _gonvex_sync_clock SET revision = revision + 1 WHERE singleton = 1 RETURNING revision").fetchone()[0]
    db.execute("""UPDATE _gonvex_sync_changes SET revision = ?1, transaction_id = ?1, ordinal = r.n, command_id = ?2
                  FROM (SELECT event_id, row_number() OVER (ORDER BY event_id) AS n
                        FROM _gonvex_sync_changes WHERE revision IS NULL) AS r
                  WHERE _gonvex_sync_changes.event_id = r.event_id""", (revision, command))
    db.execute("""INSERT INTO _gonvex_sync_transactions (revision, transaction_id, root_command_id,
                  origin_command_id, invocation_channel) VALUES (?1, ?1, ?2, ?2, 'system')""", (revision, command))
    return revision


def columns(db, table):
    return [row[1] for row in db.execute('SELECT cid, name FROM pragma_table_info(?) ORDER BY cid', (table,))]


def ident(name):
    return '"' + name.replace('"', '""') + '"'
