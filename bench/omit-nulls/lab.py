import sqlite3, json, sys, zlib, time
db = sys.argv[1]; out = sys.argv[2]
c = sqlite3.connect(f'file:{db}?mode=ro', uri=True)
cols = [(r[1], (r[2] or '').upper()) for r in c.execute('PRAGMA table_info("tasks")')]
def expr(n, t):
    q = f'r."{n}"'
    if 'JSON' in t: return f"CASE WHEN {q} IS NULL THEN NULL WHEN json_valid({q}) THEN json({q}) ELSE {q} END"
    if 'BOOL' in t: return f"CASE WHEN {q} IS NULL THEN NULL WHEN {q} THEN json('true') ELSE json('false') END"
    return q
pairs = [f"'{n}', {expr(n,t)}" for n, t in cols]
chunks = [pairs[i:i+60] for i in range(0, len(pairs), 60)]
if len(chunks) == 1: sel = f"json_object({', '.join(chunks[0])})"
else: sel = "json('{' || " + " || ',' || ".join(f"substr(json_object({', '.join(ch)}), 2, length(json_object({', '.join(ch)})) - 2)" for ch in chunks) + " || '}')"
rows = [r[0] for r in c.execute(f'SELECT {sel} FROM "tasks" AS r ORDER BY r."id" DESC')]
def strip(t):
    o = json.loads(t)
    return json.dumps({k: v for k, v in o.items() if v is not None}, separators=(',', ':'), ensure_ascii=False)
stripped = [strip(r) for r in rows]
def pages(rs, limit=256*1024):
    out, cur, n = [], [], 0
    for r in rs:
        if cur and n + len(r) > limit: out.append('[' + ','.join(cur) + ']'); cur, n = [], 0
        cur.append(r); n += len(r) + 1
    if cur: out.append('[' + ','.join(cur) + ']')
    return out
def deflate(s):
    co = zlib.compressobj(6, zlib.DEFLATED, -15); return len(co.compress(s.encode()) + co.flush(zlib.Z_SYNC_FLUSH)) - 4
res = {'rows': len(rows), 'columns': len(cols)}
for name, rs in [('legacy', rows), ('omit', stripped)]:
    ps = pages(rs)
    res[name] = {'row_bytes': sum(map(len, rs)), 'bytes_per_row': sum(map(len, rs)) / len(rs), 'pages': len(ps), 'deflated_bytes': sum(deflate(p) for p in ps)}
always_null = sum(1 for i, (n, _) in enumerate(cols) if all(json.loads(r).get(n) is None for r in rows[:2000]))
res['always_null_columns_first_2000'] = always_null
print(json.dumps(res))
# Page texts for the decode bench (shape of replica.snapshotPage: only rows matter).
colnames = [n for n, _ in cols]
json.dump([json.dumps({'type':'replica.snapshotPage','id':'t','result':json.loads(p)}, separators=(',',':'), ensure_ascii=False) for p in pages(rows)], open(f'{out}/legacy.json','w'))
op = pages(stripped)
json.dump([json.dumps({'type':'replica.snapshotPage','id':'t','result':json.loads(p), **({'columns': colnames} if i == 0 else {})}, separators=(',',':'), ensure_ascii=False) for i, p in enumerate(op)], open(f'{out}/omit.json','w'))
