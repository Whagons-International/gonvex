#!/usr/bin/env python3
"""Expose the lab's loopback-only runtime and auth emulator to a remote generator
without publishing ports. Runs inside the lab's network namespace
(network_mode: service:namespace) and listens on Unix sockets in a bind-mounted
directory; the host forwards them over SSH, for example
  ssh -N -R 18080:$DIR/runtime.sock -R 18099:$DIR/auth.sock mac
so the generator uses loopback URLs (http://127.0.0.1:18080, :18099).
Usage: remote-proxy.py DIR [name=port ...] (default runtime=8080 auth=9099)."""
import asyncio, os, sys

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

async def serve(path, port):
    async def handle(client_reader, client_writer):
        try:
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
        except OSError:
            client_writer.close()
            return
        await asyncio.gather(pipe(client_reader, writer), pipe(reader, client_writer))
    if os.path.exists(path):
        os.unlink(path)
    server = await asyncio.start_unix_server(handle, path=path)
    os.chmod(path, 0o600)
    return server

async def main():
    directory = sys.argv[1]
    targets = dict(a.split('=') for a in sys.argv[2:]) or {'runtime': '8080', 'auth': '9099'}
    servers = [await serve(os.path.join(directory, name + '.sock'), int(port)) for name, port in targets.items()]
    print('listening', {name: os.path.join(directory, name + '.sock') for name in targets}, flush=True)
    await asyncio.gather(*(s.serve_forever() for s in servers))

asyncio.run(main())
