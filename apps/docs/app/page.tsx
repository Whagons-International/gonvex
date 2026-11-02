import Link from 'next/link';

const cards = [
  {
    href: '/docs/quickstart',
    title: 'Run it locally',
    body: 'Start services, runtime, dashboard, package watchers, and docs from the repo root.',
  },
  {
    href: '/docs/functions-and-bindings',
    title: 'Generated bindings',
    body: 'Register TypeScript Queries, Reducers, Actions, and Live Queries, then call them from React.',
  },
  {
    href: '/docs/live-queries',
    title: 'Realtime grid',
    body: 'Understand exact server result windows, persistent Local Replica state, and small result deltas.',
  },
];

export default function HomePage() {
  return (
    <main className="gonvex-home">
      <section className="gonvex-home__hero">
        <p className="gonvex-home__eyebrow">TypeScript modules on a Rust-ready Gonvex host</p>
        <h1>Gonvex Docs</h1>
        <p className="gonvex-home__lede">
          Gonvex is a SQLite-backed realtime runtime with TypeScript modules,
          generated bindings, WebSocket delivery, and a persistent Local Replica.
        </p>
        <div className="gonvex-home__actions">
          <Link href="/docs">Read the docs</Link>
          <Link href="/docs/quickstart">Quickstart</Link>
          <Link href="/docs/current-limits">Current limits</Link>
        </div>
      </section>

      <section className="gonvex-home__cards" aria-label="Documentation shortcuts">
        {cards.map((card) => (
          <Link href={card.href} key={card.href}>
            <span>{card.title}</span>
            <p>{card.body}</p>
          </Link>
        ))}
      </section>
    </main>
  );
}
