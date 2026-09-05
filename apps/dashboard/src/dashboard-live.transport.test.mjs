import { afterEach, describe, expect, it, vi } from "vitest";
import { WebSocket, WebSocketServer } from "ws";
import { closeDashboardConnections, watchDashboard } from "./dashboard-live";
const servers = [];
afterEach(async () => {
    closeDashboardConnections();
    for (const server of servers.splice(0)) {
        for (const socket of server.clients)
            socket.terminate();
        await new Promise((resolve) => server.close(() => resolve()));
    }
    vi.unstubAllGlobals();
});
describe("dashboard SDK connection", () => {
    it.each(["operator-token", ""])("streams and caches SDK results with token %s", async (token) => {
        const server = new WebSocketServer({ port: 0, host: "127.0.0.1" });
        servers.push(server);
        await new Promise((resolve) => server.once("listening", resolve));
        const address = server.address();
        if (typeof address === "string")
            throw new Error("Expected TCP server");
        vi.stubGlobal("WebSocket", WebSocket);
        const frames = [];
        let publish;
        server.on("connection", (socket, request) => {
            expect(request.url).toBe("/dev/dashboard/ws");
            socket.send(JSON.stringify({ type: "session.ready", capabilities: { protocolVersion: 2 } }));
            socket.on("message", (raw) => {
                const frame = JSON.parse(String(raw));
                frames.push(frame);
                if (frame.type === "auth")
                    socket.send(JSON.stringify({ type: "auth.result", id: frame.id, result: { system: "dashboard" } }));
                if (frame.type === "query.subscribe") {
                    publish = (value) => socket.send(JSON.stringify({ type: "query.result", id: frame.id, result: value, reason: "change" }));
                    publish({ tables: ["tasks"] });
                }
            });
        });
        const url = `http://127.0.0.1:${address.port}/dev/data/tables`;
        const init = { headers: { authorization: token ? `Bearer ${token}` : "", "x-gonvex-project-id": "one" } };
        const next = vi.fn();
        const fail = vi.fn();
        const stop = watchDashboard(url, init, next, fail);
        await vi.waitFor(() => expect(next).toHaveBeenCalledWith({ tables: ["tasks"] }));
        expect(frames.find((frame) => frame.type === "auth")?.token).toBe(token || undefined);
        stop();
        const second = vi.fn();
        watchDashboard(url, init, second, fail);
        expect(second).toHaveBeenCalledWith({ tables: ["tasks"] });
        expect(frames.filter((frame) => frame.type === "query.subscribe")).toHaveLength(1);
        publish({ tables: ["tasks", "users"] });
        await vi.waitFor(() => expect(second).toHaveBeenLastCalledWith({ tables: ["tasks", "users"] }));
        expect(fail).not.toHaveBeenCalled();
    });
    it("reports authentication rejection without falling back to HTTP", async () => {
        const server = new WebSocketServer({ port: 0, host: "127.0.0.1" });
        servers.push(server);
        await new Promise((resolve) => server.once("listening", resolve));
        const address = server.address();
        if (typeof address === "string")
            throw new Error("Expected TCP server");
        vi.stubGlobal("WebSocket", WebSocket);
        const fetch = vi.fn();
        vi.stubGlobal("fetch", fetch);
        server.on("connection", (socket) => {
            socket.send(JSON.stringify({ type: "session.ready" }));
            socket.on("message", (raw) => {
                const frame = JSON.parse(String(raw));
                if (frame.type === "auth")
                    socket.send(JSON.stringify({ type: "auth.error", id: frame.id, error: "revoked" }));
            });
        });
        const next = vi.fn();
        const fail = vi.fn();
        watchDashboard(`http://127.0.0.1:${address.port}/dev/projects`, { headers: { authorization: "Bearer revoked" } }, next, fail);
        await vi.waitFor(() => expect(fail).toHaveBeenCalled());
        await new Promise((resolve) => setTimeout(resolve, 550));
        expect(next).not.toHaveBeenCalled();
        expect(fetch).not.toHaveBeenCalled();
    });
});
