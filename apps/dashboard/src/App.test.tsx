import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { App, DatabaseHealthSection, LogDetailsSheet, RealtimeDashboard, SchedulesPage, dashboardEmailAllowed, dashboardMetricsWebSocketProtocols, dataEditorInputValue, dataRowIdentity, googleLoginEnabled, parseDataEditorValue, parseEmailAllowlist, runtimeLogSourceSummary, runtimeLogsForCopy } from "./App";

async function renderProjectApp() {
  const user = userEvent.setup();
  window.localStorage.setItem("gonvex-dashboard-session", JSON.stringify({ email: "gabriel@example.com", name: "Gabriel" }));
  render(<App />);
  await user.click(screen.getByRole("button", { name: /open project/i }));
  return user;
}

const trackedErrorProject = {
  id: "error-app",
  name: "Error App",
  environment: "local dev",
  runtimeUrl: "http://127.0.0.1:8080",
  database: "gonvex_error_app",
  storageBucket: "error-app-dev",
  status: "local",
  description: "Error tracking test project.",
  provisioned: false,
  runtimeCreated: true,
  databaseMode: "single",
  testTab: false,
  errorTrackingEnabled: true,
};

async function renderTrackedErrorProject(groupsResponse: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>) {
  vi.stubGlobal("WebSocket", undefined);
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    if (String(input).includes("/dev/projects") && init?.method === "POST") {
      return { ok: true, status: 200, statusText: "OK", json: async () => ({ project: trackedErrorProject, projectKey: "test-project-key" }) } as Response;
    }
    if (String(input).includes("/dev/errors/groups")) {
      const response = await groupsResponse(input, init);
      if (response.ok && !init?.method && response.json && !response.text) {
        const payload = await response.json();
        return { ...response, json: async () => ({ since: new URL(String(input)).searchParams.get("since"), ...payload }) } as Response;
      }
      return response;
    }
    return { ok: true, status: 200, statusText: "OK", json: async () => ({}) } as Response;
  }));
  const user = userEvent.setup();
  window.localStorage.setItem("gonvex-dashboard-session", JSON.stringify({ email: "gabriel@example.com", name: "Gabriel" }));
  render(<App />);
  await user.click(screen.getByRole("button", { name: /create project/i }));
  const createDialog = screen.getByRole("dialog", { name: /create project/i });
  await user.type(within(createDialog).getByLabelText(/name/i), "Error App");
  await user.click(within(createDialog).getByRole("button", { name: /create project/i }));
  await user.click(await screen.findByRole("button", { name: /^done$/i }));
  const projectTile = screen.getByText("Error App").closest(".project-tile");
  expect(projectTile).not.toBeNull();
  await user.click(within(projectTile as HTMLElement).getByRole("button", { name: /open project/i }));
  await user.click(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /^errors$/i }));
  return user;
}

describe("App", () => {
  beforeEach(() => {
    vi.unstubAllGlobals();
    window.localStorage.clear();
    window.history.replaceState(null, "", "/");
  });

  it("carries metrics WebSocket credentials outside the URL", () => {
    expect(dashboardMetricsWebSocketProtocols(" signed-token ")).toEqual([
      "gonvex-dashboard-auth.signed-token",
    ]);
    expect(dashboardMetricsWebSocketProtocols(" ")).toEqual([]);
  });

  it("preserves row value types while preparing dashboard edits", () => {
    expect(dataRowIdentity({ _id: "internal-7", id: "public-3" })).toBe("internal-7");
    expect(dataRowIdentity({ id: 42 })).toBe("42");
    expect(dataEditorInputValue({ enabled: true })).toBe('{"enabled":true}');
    expect(parseDataEditorValue("12.5", 2)).toBe(12.5);
    expect(parseDataEditorValue("false", true)).toBe(false);
    expect(parseDataEditorValue('{"enabled":false}', { enabled: true })).toEqual({ enabled: false });
    expect(parseDataEditorValue("0012", "2")).toBe("0012");
    expect(() => parseDataEditorValue("not-json", { enabled: true })).toThrow(/valid JSON/i);
  });

  it("groups tenant cron instances and makes their scope visible", async () => {
    class MetricsSocket {
      static current: MetricsSocket | null = null;
      private listeners = new Map<string, Array<(event: { data?: string }) => void>>();

      constructor() {
        MetricsSocket.current = this;
      }

      addEventListener(type: string, listener: (event: { data?: string }) => void) {
        this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
      }

      close() {}

      emit(type: string, event: { data?: string }) {
        for (const listener of this.listeners.get(type) ?? []) listener(event);
      }
    }

    vi.stubGlobal("WebSocket", MetricsSocket);
    render(<SchedulesPage project={{ ...trackedErrorProject, databaseMode: "single", provisioned: true, status: "local" }} />);

    act(() => MetricsSocket.current?.emit("message", {
      data: JSON.stringify({
        type: "metrics",
        metrics: {
          functions: {},
          cache: { hits: 0, misses: 0, bypasses: 0, requests: 0, hitRate: 0, series: [] },
          logs: [],
          scheduler: {
            running: 0,
            queued: 0,
            scheduled: 2,
            completed: 6,
            failed: 0,
            lagMs: 0,
            crons: [
              { name: "check overdue tool checkouts", project: "error-app", tenant: "tenant-a", function: "tools.checkOverdueItems", schedule: "0 8 * * *", nextRun: "2026-08-11T08:00:00Z", runs: 3, failures: 0 },
              { name: "check overdue tool checkouts", project: "error-app", tenant: "tenant-b", function: "tools.checkOverdueItems", schedule: "0 8 * * *", nextRun: "2026-08-11T08:00:00Z", runs: 3, failures: 0 },
            ],
            recent: [],
            series: [],
          },
        },
      }),
    }));

    const table = await screen.findByRole("table", { name: "Cron jobs" });
    expect(within(table).getByRole("columnheader", { name: "Scope" })).toBeInTheDocument();
    expect(within(table).getByText("2 tenants")).toBeInTheDocument();
    expect(within(table).getAllByText("check overdue tool checkouts")).toHaveLength(1);
    expect(within(table).getAllByText("3 runs")).toHaveLength(2);
  });

  it("shows database connection load and pool waits", () => {
    render(<DatabaseHealthSection database={{
      pools: 2,
      openConnections: 6,
      inUse: 2,
      idle: 4,
      maxOpenConnections: 0,
      waitCount: 3,
      waitDurationMs: 125,
      series: [{ time: "2026-07-16T19:09:28Z", openConnections: 6, inUse: 2, idle: 4, waitCount: 3, waitDurationMs: 125 }],
    }} />);

    const section = screen.getByRole("region", { name: /database load/i });
    expect(within(section).getByText("2", { selector: "strong" })).toBeInTheDocument();
    expect(within(section).getByText("2 in use")).toBeInTheDocument();
    expect(within(section).getByText("4 idle")).toBeInTheDocument();
    expect(within(section).getAllByText("3 waits")).toHaveLength(2);
    expect(within(section).getByText(/connections waited for a database slot/i)).toBeInTheDocument();
  });

  it("groups realtime connections by user and traces every destination", async () => {
    const user = userEvent.setup();
    const now = new Date().toISOString();
    render(<RealtimeDashboard
      project={{ ...trackedErrorProject, id: "whagons-5", name: "Whagons 5", databaseMode: "single", status: "local" }}
      reachable
      metrics={{
        functions: {},
        cache: { hits: 0, misses: 0, bypasses: 0, requests: 0, hitRate: 0, series: [] },
        logs: [{ time: now, project: "whagons-5", tenant: "tenant-a", userId: "user-a", userEmail: "ada@example.test", path: "tasks.update", kind: "mutation", outcome: "ok", durationMs: 18 }],
        websocket: {
          connections: 2,
          subscriptions: 4,
          users: 1,
          details: [
            { id: "conn-000001", project: "whagons-5", tenant: "tenant-a", userId: "user-a", userEmail: "ada@example.test", authenticated: true, connectedAt: now, lastActiveAt: now, lastActivity: "query.subscribe", lastPath: "bulk.tasksByWorkspace", browser: "Chrome 126", platform: "macOS", deviceType: "desktop", subscriptions: ["bulk.tasksByWorkspace", "tasks.count", "tasks.count"] },
            { id: "conn-000002", project: "whagons-5", tenant: "tenant-b", userId: "user-a", userEmail: "ada@example.test", authenticated: true, connectedAt: now, lastActiveAt: now, lastActivity: "mutation.call", lastPath: "tasks.update", browser: "Safari 18", platform: "iOS", deviceType: "mobile", subscriptions: ["notifications.list"] },
          ],
        },
      }}
    />);

    expect(screen.getByText("ada@example.test", { selector: ".realtime-user-name strong" })).toBeInTheDocument();
    expect(screen.getByText("2", { selector: ".realtime-user-count strong" })).toBeInTheDocument();
    expect(screen.getByLabelText("ada@example.test connects to tenant-a")).toBeInTheDocument();
    expect(screen.getByLabelText("ada@example.test connects to tenant-b")).toBeInTheDocument();
    const firstConnection = screen.getByText("conn-000001").closest(".realtime-connection");
    expect(firstConnection).not.toBeNull();
    const firstSubscriptions = firstConnection?.querySelector(".realtime-subscription-disclosure");
    expect(firstSubscriptions).not.toHaveAttribute("open");
    await user.click(within(firstConnection as HTMLElement).getByText("View paths"));
    expect(firstSubscriptions).toHaveAttribute("open");
    expect(within(firstConnection as HTMLElement).getByText("bulk.tasksByWorkspace")).toBeVisible();
    expect(within(firstConnection as HTMLElement).getAllByText("tasks.count")).toHaveLength(1);
    expect(within(firstConnection as HTMLElement).getByText("×2")).toBeVisible();
    expect(screen.getByRole("heading", { name: "Recent activity" })).toBeInTheDocument();

    await user.type(screen.getByRole("searchbox", { name: "Search connections" }), "Safari");
    expect(screen.queryByText("conn-000001")).not.toBeInTheDocument();
    expect(screen.getByText("conn-000002")).toBeInTheDocument();
  });

  it("keeps the project chooser vertically scrollable", () => {
    const css = readFileSync(resolve(process.cwd(), "src/App.css"), "utf8");
    const projectsShellRule = css.match(/\.projects-shell\s*\{([^}]*)\}/)?.[1] ?? "";

    expect(projectsShellRule).toMatch(/height:\s*100%/);
    expect(projectsShellRule).toMatch(/overflow-y:\s*auto/);
  });

  it("shows contextual runtime log details in an inspectable side sheet", async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();
    render(<LogDetailsSheet
      entry={{
        time: "2026-07-16T19:09:28.412Z",
        startedAt: "2026-07-16T19:09:27.987Z",
        completedAt: "2026-07-16T19:09:28.412Z",
        executionId: "exec-425",
        project: "whagons-production",
        tenant: "tenant-a",
        userId: "user-a",
        userEmail: "user@example.test",
        kind: "query",
        path: "bulk.tasksByWorkspace",
        outcome: "ok",
        durationMs: 425,
        request: { workspaceId: "workspace-a", accessToken: "[REDACTED]" },
        requestSizeBytes: 84,
        cache: "hit",
        source: "redis",
      }}
      onClose={onClose}
      onAction={vi.fn()}
    />);

    const sheet = screen.getByRole("dialog", { name: /bulk\.tasksbyworkspace execution/i });
    expect(within(sheet).getAllByText("exec-425")).toHaveLength(2);
    expect(within(sheet).getByText("tenant-a")).toBeInTheDocument();
    expect(within(sheet).getByText("425 ms")).toBeInTheDocument();
    expect(within(sheet).getByText("Redis")).toBeInTheDocument();
    expect(within(sheet).getByText("Redis hit; database skipped")).toBeInTheDocument();

    await user.click(within(sheet).getByRole("button", { name: /^request$/i }));
    expect(within(sheet).getByText(/workspace-a/)).toBeInTheDocument();
    expect(within(sheet).getByText(/\[REDACTED\]/)).toBeInTheDocument();

    await user.click(within(sheet).getByRole("button", { name: /close execution details/i }));
    expect(onClose).toHaveBeenCalledOnce();
  });

  it("distinguishes Redis hits from database cache misses", () => {
    expect(runtimeLogSourceSummary({
      time: "2026-07-16T19:09:28Z",
      path: "tasks.list",
      kind: "query",
      outcome: "ok",
      durationMs: 10,
      source: "redis",
      cache: "hit",
    })).toMatchObject({ label: "Redis", detail: "Redis hit; database skipped", key: "redis-hit" });

    expect(runtimeLogSourceSummary({
      time: "2026-07-16T19:09:28Z",
      path: "tasks.list",
      kind: "query",
      outcome: "ok",
      durationMs: 10,
      source: "database",
      cache: "miss",
    })).toMatchObject({ label: "Database", detail: "Redis miss; database executed", key: "database-miss" });

    expect(runtimeLogSourceSummary({
      time: "2026-07-16T19:09:30Z",
      path: "tasks.recentSync",
      kind: "sync",
      outcome: "ok",
      durationMs: 12,
      source: "websocket",
      reason: "snapshot",
    })).toMatchObject({ label: "WebSocket", detail: "Durable sync protocol", key: "websocket" });
  });

  it("shows the frontend responsible for a durable sync snapshot", () => {
    render(<LogDetailsSheet
      entry={{
        time: "2026-07-16T19:09:28Z",
        executionId: "sync-1",
        path: "tasks.recentSync",
        kind: "sync",
        outcome: "ok",
        durationMs: 12,
        source: "websocket",
        reason: "snapshot",
        connectionId: "conn-000042",
        browser: "Chrome 140",
        platform: "Linux",
        deviceType: "desktop",
        resultCount: 18,
      }}
      onClose={vi.fn()}
      onAction={vi.fn()}
    />);

    expect(screen.getByText("conn-000042")).toBeInTheDocument();
    expect(screen.getByText("Chrome 140 · Linux · desktop")).toBeInTheDocument();
    expect(screen.getByText("snapshot")).toBeInTheDocument();
    expect(screen.getByText("18")).toBeInTheDocument();
  });

  it("copies only explicitly selected runtime log rows", () => {
    const logs = [
      { time: "2026-07-16T19:09:28Z", executionId: "exec-a", path: "tasks.list", kind: "query", outcome: "ok", durationMs: 10 },
      { time: "2026-07-16T19:09:29Z", executionId: "exec-b", path: "tasks.update", kind: "mutation", outcome: "ok", durationMs: 20 },
      { time: "2026-07-16T19:09:30Z", executionId: "exec-c", path: "tasks.remove", kind: "mutation", outcome: "error", durationMs: 30 },
    ];

    expect(runtimeLogsForCopy(logs, new Set(["exec-b", "exec-c"])).map((entry) => entry.executionId)).toEqual(["exec-b", "exec-c"]);
    expect(runtimeLogsForCopy(logs, new Set())).toEqual(logs);
  });

  it("explains logs whose runtime did not capture an execution id", () => {
    render(<LogDetailsSheet
      entry={{ time: "2026-07-16T19:09:28Z", path: "tasks.list", kind: "query", outcome: "ok", durationMs: 10 }}
      onClose={vi.fn()}
      onAction={vi.fn()}
    />);

    expect(screen.getByText("Not captured by this runtime")).toBeInTheDocument();
    expect(screen.queryByText(/legacy log/i)).not.toBeInTheDocument();
  });

  it("signs in to the project list", async () => {
    const user = userEvent.setup();
    render(<App />);

    expect(screen.getByRole("heading", { name: /welcome back/i })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /sign in with google/i })).not.toBeInTheDocument();
    expect(window.location.pathname).toBe("/login");

    await user.type(screen.getByLabelText(/email/i), "gabriel@example.com");
    await user.click(screen.getByRole("button", { name: /^continue$/i }));

    expect(screen.getByRole("heading", { name: /choose a project/i })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/projects");
    expect(screen.getByRole("button", { name: /open project/i })).toBeInTheDocument();
  });

  it("redirects protected URLs to login", () => {
    window.history.replaceState(null, "", "/settings");
    render(<App />);

    expect(screen.getByRole("heading", { name: /welcome back/i })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/login");
  });

  it("stays on the project chooser after reload and runtime discovery", async () => {
    const fetchMock = vi.fn(async () => ({
      headers: new Headers({ "content-type": "application/json" }),
      json: async () => ({
        projects: [{
          id: "remote-app",
          name: "Remote App",
          environment: "production",
          runtimeUrl: "https://runtime.example.test",
          database: "gonvex_remote_app",
          storageBucket: "remote-app-production",
          status: "local",
          description: "Runtime project",
          provisioned: true,
          runtimeCreated: true,
          databaseMode: "single",
        }],
      }),
      ok: true,
      status: 200,
      statusText: "OK",
      url: "/dev/projects",
    } as Response));
    vi.stubGlobal("fetch", fetchMock);
    window.localStorage.setItem("gonvex-dashboard-session", JSON.stringify({ email: "gabriel@example.com", name: "Gabriel" }));
    window.localStorage.setItem("gonvex-dashboard-project", "previous-project");
    window.history.replaceState(null, "", "/projects");

    render(<App />);

    const projectsGrid = await screen.findByLabelText("Projects");
    expect(await within(projectsGrid).findByText("Remote App")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /choose a project/i })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/projects");
  });

  it("preserves a UUID project deep link while runtime projects load", async () => {
    const requestedProjectID = "3db6f499-421c-42a2-a43d-1daa44ecbc4d";
    const runtimeProjects = [
      {
        id: "skills",
        name: "Skills",
        environment: "production",
        runtimeUrl: "https://runtime.example.test",
        database: "gonvex_dev",
        storageBucket: "skills-production",
        status: "local",
        description: "Default runtime project",
        provisioned: true,
        runtimeCreated: true,
        databaseMode: "single",
      },
      {
        id: requestedProjectID,
        name: "Whagons Production",
        environment: "production",
        runtimeUrl: "https://runtime.example.test",
        database: "whagons_production",
        storageBucket: "whagons-production",
        status: "local",
        description: "Requested runtime project",
        provisioned: true,
        runtimeCreated: true,
        databaseMode: "multiTenant",
      },
    ];
    vi.stubGlobal("WebSocket", undefined);
    vi.stubGlobal("fetch", vi.fn(async () => ({
      headers: new Headers({ "content-type": "application/json" }),
      json: async () => ({ projects: runtimeProjects }),
      ok: true,
      status: 200,
      statusText: "OK",
      url: "/dev/projects",
    } as Response)));
    window.localStorage.setItem("gonvex-dashboard-session", JSON.stringify({ email: "gabriel@example.com", name: "Gabriel" }));
    window.history.replaceState(null, "", `/projects/${requestedProjectID}/overview`);

    render(<App />);

    expect(await screen.findByText("Whagons Production", { selector: ".project-card strong" })).toBeInTheDocument();
    expect(window.location.pathname).toBe(`/projects/${requestedProjectID}/overview`);
  });

  it("waits for a native Google session before discovering runtime projects", async () => {
    const project = {
      id: "google-owned-app",
      name: "Google-owned App",
      environment: "production",
      runtimeUrl: "https://runtime.example.test",
      database: "gonvex_google_owned_app",
      storageBucket: "google-owned-app-production",
      status: "local",
      description: "Runtime project",
      provisioned: true,
      runtimeCreated: true,
      databaseMode: "single",
    };
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/api/dashboard/session")) {
        return { ok: false, status: 401, statusText: "Unauthorized", json: async () => ({ error: "unauthorized" }) } as Response;
      }
      if (url.endsWith("/dev/auth/me")) {
        return {
          ok: true,
          status: 200,
          statusText: "OK",
          json: async () => ({ account: { email: "gabriel@example.com", name: "Gabriel", role: "admin" } }),
        } as Response;
      }
      if (url.endsWith("/dev/projects")) {
        const authorization = new Headers(init?.headers).get("authorization");
        if (!authorization) {
          return {
            headers: new Headers({ "content-type": "application/json" }),
            ok: false,
            status: 401,
            statusText: "Unauthorized",
            url,
            json: async () => ({ error: "unauthorized" }),
          } as Response;
        }
        return {
          headers: new Headers({ "content-type": "application/json" }),
          ok: true,
          status: 200,
          statusText: "OK",
          url,
          json: async () => ({ projects: [project] }),
        } as Response;
      }
      return { ok: true, status: 200, statusText: "OK", json: async () => ({}) } as Response;
    });
    vi.stubGlobal("fetch", fetchMock);
    window.history.replaceState(null, "", "/login");

    const fetchAccessToken = vi.fn(async () => "google-dashboard-access-token");
    const signIn = vi.fn(async () => undefined);
    const signOut = vi.fn(async () => undefined);
    const { rerender } = render(<App nativeAuth={{
      error: "",
      fetchAccessToken,
      isAuthenticated: false,
      isLoading: true,
      signIn,
      signOut,
      user: null,
    } as never} />);

    expect(fetchMock.mock.calls.some(([input]) => String(input).endsWith("/dev/projects"))).toBe(false);

    rerender(<App nativeAuth={{
      error: "",
      fetchAccessToken,
      isAuthenticated: true,
      isLoading: false,
      signIn,
      signOut,
      user: { email: "gabriel@example.com", name: "Gabriel", picture: "https://example.test/avatar.png" },
    } as never} />);

    expect(await screen.findByText("Google-owned App")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/projects");
  });

  it("does not discover projects with a stale cached Google token during native session restore", async () => {
    const project = {
      id: "restored-google-app",
      name: "Restored Google App",
      environment: "production",
      runtimeUrl: "https://runtime.example.test",
      database: "gonvex_restored_google_app",
      storageBucket: "restored-google-app-production",
      status: "local",
      description: "Runtime project",
      provisioned: true,
      runtimeCreated: true,
      databaseMode: "single",
    };
    let resolveAccessToken: ((token: string) => void) | undefined;
    const accessTokenPromise = new Promise<string>((resolve) => {
      resolveAccessToken = resolve;
    });
    const projectAuthorizations: Array<string | null> = [];
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/dev/auth/me")) {
        return {
          ok: true,
          status: 200,
          statusText: "OK",
          json: async () => ({ account: { email: "gabriel@example.com", name: "Gabriel", role: "admin" } }),
        } as Response;
      }
      if (url.endsWith("/dev/projects")) {
        const authorization = new Headers(init?.headers).get("authorization");
        projectAuthorizations.push(authorization);
        return {
          headers: new Headers({ "content-type": "application/json" }),
          ok: authorization === "Bearer fresh-google-token",
          status: authorization === "Bearer fresh-google-token" ? 200 : 401,
          statusText: authorization === "Bearer fresh-google-token" ? "OK" : "Unauthorized",
          url,
          json: async () => authorization === "Bearer fresh-google-token"
            ? { projects: [project] }
            : { error: "account sign-in or personal access token is required" },
        } as Response;
      }
      return { ok: true, status: 200, statusText: "OK", json: async () => ({}) } as Response;
    });
    vi.stubGlobal("fetch", fetchMock);
    window.localStorage.setItem("gonvex-dashboard-session", JSON.stringify({
      accessToken: "stale-google-token",
      email: "gabriel@example.com",
      name: "Gabriel",
      provider: "google",
      role: "admin",
    }));
    window.history.replaceState(null, "", "/projects");

    const fetchAccessToken = vi.fn(() => accessTokenPromise);
    const nativeAuth = {
      error: "",
      fetchAccessToken,
      isAuthenticated: true,
      isLoading: false,
      signIn: vi.fn(async () => undefined),
      signOut: vi.fn(async () => undefined),
      user: { email: "gabriel@example.com", name: "Gabriel", picture: "https://example.test/avatar.png" },
    } as never;

    render(<App nativeAuth={nativeAuth} />);

    await waitFor(() => expect(fetchAccessToken).toHaveBeenCalledOnce());
    expect(projectAuthorizations).toEqual([]);

    resolveAccessToken?.("fresh-google-token");

    expect(await screen.findByText("Restored Google App")).toBeInTheDocument();
    expect(projectAuthorizations).toEqual(["Bearer fresh-google-token"]);
    expect(screen.queryByText(/runtime projects unavailable/i)).not.toBeInTheDocument();
  });

  it("normalizes dashboard email allowlists", () => {
    expect(parseEmailAllowlist(" Gabriel@Example.com, gabriel@example.com ; admin@example.com\n")).toEqual([
      "gabriel@example.com",
      "admin@example.com",
    ]);
  });

  it("checks dashboard email allowlists case-insensitively", () => {
    const allowlist = parseEmailAllowlist("gabriel@example.com");

    expect(dashboardEmailAllowed(" Gabriel@Example.com ", allowlist, false)).toBe(true);
    expect(dashboardEmailAllowed("other@example.com", allowlist, false)).toBe(false);
    expect(dashboardEmailAllowed("other@example.com", [], true)).toBe(true);
    expect(dashboardEmailAllowed("other@example.com", [], false)).toBe(false);
  });

  it("keeps Google login disabled unless explicitly enabled", () => {
    expect(googleLoginEnabled(undefined, true)).toBe(false);
    expect(googleLoginEnabled("false", true)).toBe(false);
    expect(googleLoginEnabled("true", false)).toBe(false);
    expect(googleLoginEnabled("true", true)).toBe(true);
  });

  it("creates a runtime project card", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.fn().mockResolvedValue({
      json: async () => ({
        project: {
          id: "acme-app",
          name: "Acme App",
          environment: "local dev",
          database: "gonvex_acme_app",
          storageBucket: "acme-app-dev",
          status: "local",
          description: "Runtime-created project database.",
          provisioned: true,
          runtimeCreated: true,
          databaseMode: "single",
          testTab: false,
        },
      }),
      ok: true,
      statusText: "OK",
    });
    vi.stubGlobal("fetch", fetchMock);
    window.localStorage.setItem("gonvex-dashboard-session", JSON.stringify({ email: "gabriel@example.com", name: "Gabriel" }));
    render(<App />);

    await user.click(screen.getByRole("button", { name: /create project/i }));
    const dialog = screen.getByRole("dialog", { name: /create project/i });
    expect(within(dialog).getByText(/database structure/i)).toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: /single project database/i })).toBeInTheDocument();
    await user.type(within(dialog).getByLabelText(/name/i), "Acme App");
    await user.click(within(dialog).getByRole("button", { name: /create project/i }));

    expect(screen.getByText("Acme App")).toBeInTheDocument();
    expect(JSON.parse(window.localStorage.getItem("gonvex-dashboard-database-modes") ?? "{}")).toEqual({
      "acme-app": "single",
    });
    const createCall = fetchMock.mock.calls.find(([, init]) => init?.method === "POST");
    expect(JSON.parse(String(createCall?.[1]?.body ?? "{}"))).toMatchObject({ databaseMode: "single", testTab: false });
    expect(screen.queryByRole("button", { name: /^test$/i })).not.toBeInTheDocument();
    vi.unstubAllGlobals();
  });

  it("creates and revokes a runtime-wide admin API key from the account profile", async () => {
    const user = userEvent.setup();
    const existingToken = {
      id: "pat_existing",
      name: "Existing automation",
      prefix: "gvx_pat_existing",
      permissions: ["projects:*", "admin:projects"],
      createdAt: "2026-07-01T12:00:00Z",
    };
    const createdToken = {
      id: "pat_created",
      name: "Production provisioning",
      prefix: "gvx_pat_created",
      permissions: ["projects:*", "admin:projects"],
      createdAt: "2026-07-14T12:00:00Z",
    };
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/dev/auth/tokens") && init?.method === "POST") {
        return {
          ok: true,
          status: 201,
          statusText: "Created",
          json: async () => ({ token: createdToken, accessToken: "synthetic-admin-key-for-ui-test" }),
        } as Response;
      }
      if (url.endsWith("/dev/auth/tokens/pat_existing") && init?.method === "DELETE") {
        return { ok: true, status: 200, statusText: "OK", json: async () => ({ ok: true }) } as Response;
      }
      if (url.endsWith("/dev/auth/tokens")) {
        return { ok: true, status: 200, statusText: "OK", json: async () => ({ tokens: [existingToken] }) } as Response;
      }
      return { ok: true, status: 200, statusText: "OK", json: async () => ({}) } as Response;
    });
    vi.stubGlobal("fetch", fetchMock);
    window.localStorage.setItem("gonvex-dashboard-session", JSON.stringify({
      accessToken: "synthetic-dashboard-session",
      email: "admin@example.com",
      name: "Gabriel Admin",
      role: "admin",
    }));

    render(<App />);
    await user.click(screen.getByRole("button", { name: /open account profile/i }));

    const profile = screen.getByRole("dialog", { name: /gabriel admin/i });
    expect(within(profile).getByText("admin@example.com")).toBeInTheDocument();
    expect(within(profile).getByText("All projects")).toBeInTheDocument();
    expect(await within(profile).findByText("Existing automation")).toBeInTheDocument();

    await user.type(within(profile).getByLabelText(/key name/i), "Production provisioning");
    await user.click(within(profile).getByRole("button", { name: /create admin key/i }));

    expect(within(profile).getByLabelText(/new admin api key/i)).toHaveValue("synthetic-admin-key-for-ui-test");
    const createCall = fetchMock.mock.calls.find(([, init]) => init?.method === "POST");
    expect(JSON.parse(String(createCall?.[1]?.body))).toMatchObject({
      name: "Production provisioning",
      permissions: ["projects:*", "admin:projects"],
    });

    const existingRow = within(profile).getByText("Existing automation").closest(".account-token-row");
    expect(existingRow).not.toBeNull();
    await user.click(within(existingRow as HTMLElement).getByRole("button", { name: /revoke/i }));
    await waitFor(() => expect(existingRow).toHaveAttribute("data-status", "revoked"));
  });

  it("renders the dashboard overview", async () => {
    await renderProjectApp();

    expect(window.location.pathname).toBe("/projects/app/overview");
    expect(screen.getByRole("region", { name: /runtime summary/i })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /^functions$/i })).toBeInTheDocument();
  });

  it("hides the task-grid test surface for existing projects", async () => {
    await renderProjectApp();

    expect(within(screen.getByLabelText("Primary sections")).queryByRole("button", { name: /^test$/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: /livegrid test surface/i })).not.toBeInTheDocument();
  });

  it("shows error tracking navigation before the project reports its first error", async () => {
    await renderProjectApp();

    expect(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /^errors$/i })).toBeInTheDocument();
  });

  it("keeps an error tracking deep link available before registration", async () => {
    window.localStorage.setItem("gonvex-dashboard-session", JSON.stringify({ email: "gabriel@example.com", name: "Gabriel" }));
    window.history.replaceState(null, "", "/projects/app/errors");

    render(<App />);

    await waitFor(() => expect(window.location.pathname).toBe("/projects/app/errors"));
    expect(screen.getByRole("heading", { name: /errors affecting real users/i })).toBeInTheDocument();
  });

  it("uses a compact icon theme toggle and an uncluttered project switcher", async () => {
    const user = await renderProjectApp();

    const switcher = document.querySelector(".project-switcher");
    expect(switcher).not.toBeNull();
    expect(within(switcher as HTMLElement).queryByText(/^project$/i)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /all projects/i })).toBeInTheDocument();

    const button = screen.getByRole("button", { name: /switch to light mode/i });
    expect(button).toHaveTextContent("");
    expect(button.querySelectorAll("svg")).toHaveLength(2);
    await user.click(button);

    expect(screen.getByRole("button", { name: /switch to dark mode/i })).toBeInTheDocument();
  });

  it("switches pages from the sidebar", async () => {
    const user = await renderProjectApp();

    await user.click(screen.getByRole("button", { name: /data/i }));
    expect(screen.getByRole("heading", { name: /no tables/i })).toBeInTheDocument();
    expect(screen.getByLabelText("Tables", { selector: "aside" })).toBeInTheDocument();

    await user.click(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /files/i }));
    expect(screen.getByRole("heading", { name: /file storage/i })).toBeInTheDocument();
  });

  it("shows real file storage shell without fake rows", async () => {
    const user = await renderProjectApp();

    await user.click(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /files/i }));

    expect(screen.getByRole("table", { name: /stored files/i })).toBeInTheDocument();
    expect(screen.getByText(/no files yet/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /column settings/i })).not.toBeDisabled();
    expect(screen.queryByText(/kg27zrhezshg/i)).not.toBeInTheDocument();
  });

  it("defaults to recent events, follows export pages, and counts affected users", async () => {
    const requested: URL[] = [];
    await renderTrackedErrorProject(async (input) => {
      const url = new URL(String(input)); requested.push(url);
      const second = url.searchParams.has("cursor");
      return { ok: true, status: 200, json: async () => ({
        since: url.searchParams.get("since"), nextCursor: second ? "" : "first",
        groups: [{ fingerprint: second ? "second" : "first", title: second ? "Second failure" : "First failure", status: "unresolved", priority: "medium", count: second ? 3 : 2,
          firstSeen: "2026-09-08T10:00:00Z", lastSeen: "2026-09-08T10:00:00Z", tenants: { acme: 1 }, users: { sameUser: 1 }, devices: {}, releases: {}, latest: {} }],
      }) } as Response;
    });
    expect(await screen.findByText("Second failure")).toBeInTheDocument();
    expect(requested).toHaveLength(2);
    expect(requested[0].searchParams.get("export")).toBe("1");
    expect(Date.now()-Date.parse(requested[0].searchParams.get("since")!)).toBeLessThan(24*60*60*1000+10000);
    expect(requested[1].searchParams.get("since")).toBe(requested[0].searchParams.get("since"));
    const summary=screen.getByLabelText("Error impact summary");
    expect(within(summary).getByText("5")).toBeInTheDocument();
    expect(within(summary).getByText("Users hit").parentElement).toHaveTextContent("1");
  });

  it("expands a grouped error with tenant, release, and machine context", async () => {
    const user = await renderTrackedErrorProject(async () => ({
      ok: true,
      status: 200,
      statusText: "OK",
      json: async () => ({ groups: [{
          fingerprint: "group-1", title: "Checkout failed", culprit: "at submitOrder (src/checkout.ts:40:3)", status: "unresolved", priority: "high",
          count: 8, firstSeen: "2026-07-11T10:00:00Z", lastSeen: "2026-07-12T10:00:00Z", tenants: { acme: 5, beta: 3 }, users: { ada: 4 }, devices: { laptop: 5, phone: 3 }, releases: { "5.1.0": 8 }, environments: { production: 8 },
          latest: {
            timestamp: "2026-07-12T10:00:00Z",
            stack: "TypeError: Checkout failed\n at submitOrder (src/checkout.ts:40:3)",
            tenant: "acme",
            release: "5.1.0",
            environment: "production",
            url: "https://acme.example.com/checkout",
            userAgent: "Chrome",
            tags: { source: "runtime" },
            context: { executionId: "exec-123", durationMs: 42, request: { orderId: "order-7" } },
          },
        }] }),
    }) as Response);

    expect(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /^errors$/i })).toBeInTheDocument();

    expect(await screen.findByText("Checkout failed")).toBeInTheDocument();
    expect(screen.getByText("2", { selector: ".errors-stat-strip strong" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /checkout failed/i }));
    expect(screen.getByText(/latest exception/i)).toBeInTheDocument();
    expect(screen.getByText("5.1.0", { selector: ".error-detail-heading strong" })).toBeInTheDocument();
    expect(screen.getByText(/execution context/i)).toBeInTheDocument();
    expect(screen.getByText(/exec-123/i)).toBeInTheDocument();
    expect(screen.getByText(/order-7/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /copy agent brief/i })).toBeInTheDocument();
  });

  it("resolves only visible selected groups and retains failed selections for retry", async () => {
    const makeGroup = (fingerprint: string, title: string) => ({
      fingerprint, title, status: "unresolved", priority: "high", count: 2,
      firstSeen: "2026-09-01T10:00:00Z", lastSeen: "2026-09-02T10:00:00Z",
      tenants: {}, users: {}, devices: {}, releases: {}, latest: {},
    });
    let groups = [makeGroup("a", "Fixed icon"), makeGroup("b", "Fixed input"), makeGroup("c", "Active timeout")];
    const patches: string[] = [];
    let fail = true;
    const user = await renderTrackedErrorProject(async (input, init) => {
      const fingerprint = String(input).split("/").pop()!;
      if (init?.method === "PATCH") {
        expect(JSON.parse(String(init.body))).toEqual({ status: "resolved" });
        patches.push(fingerprint);
        if (fingerprint === "b" && fail) return { ok: false, status: 503, json: async () => ({ error: "Try again" }) } as Response;
        groups = groups.filter((group) => group.fingerprint !== fingerprint);
        return { ok: true, json: async () => ({}) } as Response;
      }
      return { ok: true, json: async () => ({ groups }) } as Response;
    });
    await screen.findByText("Fixed icon");
    await user.click(screen.getByRole("checkbox", { name: "Select Active timeout" }));
    await user.type(screen.getByRole("textbox", { name: "Search error groups" }), "Fixed");
    await user.click(screen.getByRole("checkbox", { name: "Select all visible unresolved groups" }));
    await user.click(screen.getByRole("button", { name: "Resolve selected (2)" }));
    expect(await screen.findByText(/1 group resolved.*1 could not be resolved/i)).toBeInTheDocument();
    expect(patches).toEqual(["a", "b"]);
    expect(screen.getByRole("checkbox", { name: "Select Fixed input" })).toBeChecked();
    fail = false;
    await user.click(screen.getByRole("button", { name: "Resolve selected (1)" }));
    expect(await screen.findByText(/1 group resolved\. History is preserved/i)).toBeInTheDocument();
    await user.clear(screen.getByRole("textbox", { name: "Search error groups" }));
    expect(await screen.findByText("Active timeout")).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Select Active timeout" })).not.toBeChecked();
    expect(patches).toEqual(["a", "b", "b"]);
  });

  it("filters error groups to the latest release", async () => {
    const requestedURLs: string[] = [];
    const user = await renderTrackedErrorProject(async (input) => {
      const url = String(input);
      requestedURLs.push(url);
      const selectedRelease = new URL(url).searchParams.get("release");
      return {
        ok: true,
        status: 200,
        statusText: "OK",
        json: async () => ({
          releases: ["5.2.0", "5.1.0"],
          groups: [{
            fingerprint: selectedRelease ? "latest-group" : "all-group",
            title: selectedRelease ? "Latest release failure" : "All release failure",
            culprit: "at submitOrder (src/checkout.ts:40:3)",
            status: "unresolved",
            priority: "high",
            count: 1,
            firstSeen: "2026-07-12T10:00:00Z",
            lastSeen: "2026-07-12T10:00:00Z",
            tenants: { acme: 1 },
            users: { ada: 1 },
            devices: { laptop: 1 },
            releases: { [selectedRelease || "5.1.0"]: 1 },
            environments: { production: 1 },
            latest: { timestamp: "2026-07-12T10:00:00Z", release: selectedRelease || "5.1.0" },
          }],
        }),
      } as Response;
    });

    expect(await screen.findByText("All release failure")).toBeInTheDocument();
    await user.click(screen.getByLabelText("Error release"));
    await user.click(await screen.findByRole("option", { name: /5\.2\.0.*latest/i }));

    expect(await screen.findByText("Latest release failure")).toBeInTheDocument();
    expect(requestedURLs.some((url) => new URL(url).searchParams.get("release") === "5.2.0")).toBe(true);
  });

  it("filters error groups by level and marks warnings", async () => {
    const requestedURLs: string[] = [];
    const user = await renderTrackedErrorProject(async (input) => {
      const url = String(input);
      requestedURLs.push(url);
      const selectedLevel = new URL(url).searchParams.get("level");
      return {
        ok: true,
        status: 200,
        statusText: "OK",
        json: async () => ({
          releases: ["5.2.0"],
          groups: [{
            fingerprint: selectedLevel === "warning" ? "warning-group" : "error-group",
            title: selectedLevel === "warning" ? "Sign-in rejected" : "Checkout failure",
            level: selectedLevel === "warning" ? "warning" : "error",
            culprit: "at submitOrder (src/checkout.ts:40:3)",
            status: "unresolved",
            priority: "high",
            count: 1,
            firstSeen: "2026-07-12T10:00:00Z",
            lastSeen: "2026-07-12T10:00:00Z",
            tenants: { acme: 1 },
            users: { ada: 1 },
            devices: { laptop: 1 },
            releases: { "5.2.0": 1 },
            environments: { production: 1 },
            latest: { timestamp: "2026-07-12T10:00:00Z", release: "5.2.0" },
          }],
        }),
      } as Response;
    });

    expect(await screen.findByText("Checkout failure")).toBeInTheDocument();
    // Unfiltered by default, so an older runtime that cannot filter still lists everything.
    expect(requestedURLs.every((url) => new URL(url).searchParams.get("level") === null)).toBe(true);

    await user.click(screen.getByLabelText("Error level"));
    await user.click(await screen.findByRole("option", { name: /warnings.*worth knowing/i }));

    expect(await screen.findByText("Sign-in rejected")).toBeInTheDocument();
    expect(requestedURLs.some((url) => new URL(url).searchParams.get("level") === "warning")).toBe(true);
    expect(screen.getByText("warning", { selector: ".error-level" })).toBeInTheDocument();
  });

  it("explains when the connected runtime predates error tracking", async () => {
    await renderTrackedErrorProject(async () => ({
      ok: false,
      status: 404,
      statusText: "Not Found",
      text: async () => "404 page not found\n",
    }) as Response);

    expect(await screen.findByRole("alert")).toHaveTextContent(/error tracking is unavailable at http:\/\/127\.0\.0\.1:8080/i);
    expect(screen.getByText("Unavailable", { selector: ".errors-live-indicator strong" })).toBeInTheDocument();
    expect(screen.queryByText(/unexpected non-whitespace character/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/no unresolved errors/i)).not.toBeInTheDocument();
  });

  it("shows settings sections", async () => {
    const user = await renderProjectApp();

    await user.click(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /settings/i }));
    expect(screen.getByRole("button", { name: /general/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /connection/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /environment variables/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /authentication/i })).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /connection/i }));
    expect(screen.getByText(/x-gonvex-project-id: app/i)).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /authentication/i }));
    expect(screen.getByText(/authentication providers configured/i)).toBeInTheDocument();
  });

  it("renames a project from general settings without changing its id", async () => {
    vi.stubGlobal("WebSocket", undefined);
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body ?? "{}")) as { name?: string };
      const project = {
        id: "rename-app",
        name: body.name ?? "Original Project",
        environment: "local dev",
        runtimeUrl: "http://runtime.test",
        database: "gonvex_rename_app",
        databaseMode: "single",
        storageBucket: "rename-app-dev",
        status: "local",
        description: "Rename test project.",
        provisioned: true,
        runtimeCreated: true,
      };
      return new Response(JSON.stringify({ project, projectKey: "rename-test-key" }), {
        headers: { "content-type": "application/json" },
        status: 200,
      });
    });
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    window.localStorage.setItem("gonvex-dashboard-session", JSON.stringify({ email: "gabriel@example.com", name: "Gabriel" }));
    render(<App />);

    await user.click(screen.getByRole("button", { name: /create project/i }));
    const createDialog = screen.getByRole("dialog", { name: /create project/i });
    await user.type(within(createDialog).getByLabelText(/name/i), "Original Project");
    await user.click(within(createDialog).getByRole("button", { name: /create project/i }));
    await user.click(await screen.findByRole("button", { name: /^done$/i }));
    const projectTile = screen.getByText("Original Project").closest(".project-tile");
    expect(projectTile).not.toBeNull();
    await user.click(within(projectTile as HTMLElement).getByRole("button", { name: /open project/i }));
    await user.click(within(screen.getByLabelText("Primary sections")).getByRole("button", { name: /settings/i }));

    const nameInput = screen.getByRole("textbox", { name: /project name/i });
    await user.clear(nameInput);
    await user.type(nameInput, "Renamed Project");
    await user.click(screen.getByRole("button", { name: /save name/i }));

    expect(await screen.findByText("Project name saved")).toBeInTheDocument();
    expect(screen.getByText("Renamed Project", { selector: ".project-card strong" })).toBeInTheDocument();
    const patchCall = fetchMock.mock.calls.find(([, init]) => init?.method === "PATCH");
    expect(patchCall).toBeDefined();
    expect(String(patchCall?.[0])).toContain("/dev/projects/rename-app");
    expect(JSON.parse(String(patchCall?.[1]?.body))).toEqual({ name: "Renamed Project" });
  });

  it("does not show schema before the project pushes tables", async () => {
    const user = await renderProjectApp();

    await user.click(screen.getByRole("button", { name: /data/i }));

    const tables = screen.getByLabelText("Tables");
    expect(within(tables).queryByRole("button", { name: /tasks/i })).not.toBeInTheDocument();
    expect(within(tables).queryByRole("button", { name: /files/i })).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /no tables/i })).toBeInTheDocument();
    expect(screen.getByText(/connect a project and push its schema/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/active database/i)).toHaveTextContent(/gonvex_dev/i);
    expect(screen.queryByText(/^landlord$/i)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /\+ tenant db/i })).not.toBeInTheDocument();
  });
});
