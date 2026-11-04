#!/usr/bin/env python3
"""Expose the lab's loopback-only runtime and auth emulator to a remote generator
without publishing ports. Runs inside the lab's network namespace
(network_mode: service:namespace) and listens on Unix sockets in a bind-mounted
directory; the host forwards them over SSH, for example
  ssh -N -R 18080:$DIR/runtime.sock -R 18099:$DIR/auth.sock mac
so the generator uses loopback URLs (http://127.0.0.1:18080, :18099).
Usage: remote-proxy.py DIR [name=port ...] (default runtime=8080 auth=9099)."""
import asyncio, os, signal, socket, sys

async def pipe(reader, writer):
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        try:
            writer.close()
        except OSError:
            pass

def bind(path):
    if os.path.exists(path):
        os.unlink(path)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(path)
    os.chmod(path, 0o600)
    sock.listen(4096)
    return sock

def handler(port):
    async def handle(client_reader, client_writer):
        try:
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
        except OSError:
            client_writer.close()
            return
        await asyncio.gather(pipe(client_reader, writer), pipe(reader, client_writer))
    return handle

async def worker(listeners):
    servers = [await asyncio.start_unix_server(handler(port), sock=sock) for sock, port in listeners]
    await asyncio.gather(*(s.serve_forever() for s in servers))

def main():
    directory = sys.argv[1]
    targets = dict(a.split('=') for a in sys.argv[2:]) or {'runtime': '8080', 'auth': '9099'}
    listeners = [(bind(os.path.join(directory, name + '.sock')), int(port)) for name, port in targets.items()]
    # One asyncio loop tops out near one core (~1k thin users through the
    # tunnel): PROXY_WORKERS processes accept on the same listening sockets.
    workers = max(1, int(os.environ.get('PROXY_WORKERS', '4')))
    children = []
    for _ in range(workers - 1):
        pid = os.fork()
        if pid == 0:
            asyncio.run(worker(listeners))
            os._exit(0)
        children.append(pid)
    print('listening', {name: os.path.join(directory, name + '.sock') for name in targets}, 'workers', workers, flush=True)
    try:
        asyncio.run(worker(listeners))
    finally:
        for pid in children:
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                pass

main()
