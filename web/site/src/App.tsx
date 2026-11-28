import type { ReactNode } from 'react'
import { ErasureArt, HealArt, LifecycleArt, NuggetsArt, PlacementArt, S3Art } from './Illustrations'

const REPO = 'https://github.com/404khai/lode'

type Vein = { id: string; used: number; state?: 'down' | 'repair' }

const zones: { name: string; veins: Vein[] }[] = [
  {
    name: 'zone-a',
    veins: [
      { id: 'vein-1', used: 0.62 },
      { id: 'vein-2', used: 0.48 },
      { id: 'vein-3', used: 0.71, state: 'down' },
    ],
  },
  {
    name: 'zone-b',
    veins: [
      { id: 'vein-4', used: 0.55, state: 'repair' },
      { id: 'vein-5', used: 0.66 },
      { id: 'vein-6', used: 0.39 },
    ],
  },
]

const events: { t: string; tag: string; tone: string; text: string }[] = [
  { t: '14:02', tag: 'canary', tone: 'red', text: 'vein-3 marked down (φ 9.4)' },
  { t: '14:02', tag: 'smelter', tone: 'gold', text: '1,284 repair jobs queued' },
  { t: '14:03', tag: 'smelter', tone: 'gold', text: 'rebuilding shards → vein-4' },
  { t: '14:07', tag: 'assay', tone: 'aqua', text: 'scrub clean · 0 mismatches' },
]

function Nav() {
  return (
    <header className="nav">
      <div className="nav-inner">
        <div className="nav-left">
          <a className="brand" href="/">
            <img src="/logo.webp" alt="" />
            lode
          </a>
          <nav className="nav-links">
            <a href="#product">Product</a>
            <a href="#foreman">Foreman</a>
            <a href={`${REPO}/tree/main/docs`}>Docs</a>
            <a href={`${REPO}/tree/main/docs/adr`}>ADRs</a>
          </nav>
        </div>
        <div className="nav-right">
          <a className="btn btn-ghost btn-sm" href={REPO}>
            GitHub
          </a>
          <a className="btn btn-primary btn-sm" href="#start">
            Get started
          </a>
        </div>
      </div>
    </header>
  )
}

function ClusterPanel() {
  return (
    <div className="panel">
      <div className="panel-bar">
        <div className="dots">
          <i />
          <i />
          <i />
        </div>
        <span>lantern · cluster/prod-mine</span>
        <span className="tag gold">repairing</span>
      </div>
      <div className="panel-body">
        <div className="panel-main">
          <div className="zones">
            {zones.map((z) => (
              <div className="zone" key={z.name}>
                <div className="zone-label">{z.name}</div>
                <div className="veins">
                  {z.veins.map((v) => (
                    <div className={`vein ${v.state ?? ''}`} key={v.id}>
                      <div className="vein-head">
                        {v.id}
                        <span className="dot" />
                      </div>
                      <div className="bar">
                        <span style={{ width: `${v.used * 100}%` }} />
                      </div>
                      <div className="vein-cap">{Math.round(v.used * 4)} / 4 TiB</div>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
        <div className="panel-side">
          <div className="zone-label">events</div>
          {events.map((e, i) => (
            <div className="event" key={i}>
              <time>{e.t}</time>
              <div>
                <span className={`tag ${e.tone}`}>{e.tag}</span> {e.text}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function Hero() {
  return (
    <section className="hero">
      <a className="pill" href="#foreman">
        <b>New</b> Foreman — your cluster as an MCP server
      </a>
      <h1>
        Object storage you can <em>dig into.</em>
      </h1>
      <p>
        Lode is a self-hostable, S3-compatible object store. Content-addressed, erasure-coded,
        self-healing — and every operation is open to agents.
      </p>
      <div className="hero-ctas">
        <a className="btn btn-primary" href="#start">
          Start mining
        </a>
        <a className="btn btn-ghost" href={REPO}>
          $ make up
        </a>
      </div>
      <div className="hero-note">Apache-2.0 · single Go binary per role · Postgres for metadata</div>
      <div className="hero-visual">
        <ClusterPanel />
      </div>
    </section>
  )
}

function Strip() {
  return (
    <div className="section strip">
      <span>Works with</span>
      <span>aws-cli</span>
      <span>boto3</span>
      <span>rclone</span>
      <span>s3fs</span>
      <span>aws-sdk-go</span>
      <span>Claude</span>
    </div>
  )
}

function Card({
  w,
  last,
  art,
  title,
  comp,
  children,
}: {
  w: 2 | 3 | 4
  last?: boolean
  art: ReactNode
  title: string
  comp: string
  children: ReactNode
}) {
  return (
    <article className={`card w${w}${last ? ' last' : ''}`}>
      <div className="card-art">{art}</div>
      <div>
        <h3>
          {title} <span className="comp">{comp}</span>
        </h3>
        <p>{children}</p>
      </div>
    </article>
  )
}

function Bento() {
  return (
    <section className="section" id="product">
      <div className="sec-head">
        <span className="eyebrow">The mine</span>
        <h2 className="h2">
          Every layer of the stack, <span className="dim">built to survive losing a few.</span>
        </h2>
      </div>
      <div className="bento">
        <Card w={4} art={<NuggetsArt />} title="Nuggets, not blobs" comp="nugget">
          FastCDC splits every upload into BLAKE3-addressed nuggets. Upload the same gigabyte twice and
          store it once.
        </Card>
        <Card w={2} last art={<S3Art />} title="Speaks S3" comp="adit">
          SigV4, multipart, versioning, ranges. Point any SDK at Adit and go.
        </Card>
        <Card w={2} art={<HealArt />} title="Heals itself" comp="smelter">
          Canary spots a failing vein; Smelter rebuilds its shards before anyone notices.
        </Card>
        <Card w={2} art={<ErasureArt />} title="Erasure-coded" comp="ec">
          Reed–Solomon per bucket. Degraded reads reconstruct on the fly.
        </Card>
        <Card w={2} last art={<PlacementArt />} title="Zone-aware placement" comp="survey">
          Rendezvous hashing spreads replicas across zones, hosts and disks.
        </Card>
        <Card w={3} art={<LifecycleArt />} title="Quotas & lifecycle" comp="strata">
          Expire old versions, tier hot replicas down to EC, and stop runaway buckets with
          QuotaExceeded.
        </Card>
        <Card w={3} last art={<AssayArt />} title="Never serves wrong bytes" comp="assay">
          Every read is re-hashed. Assay scrubs in the background and queues repairs on mismatch.
        </Card>
      </div>
    </section>
  )
}

function AssayArt() {
  const cells = Array.from({ length: 48 }, (_, i) => i)
  const bad = new Set([17, 33])
  return (
    <svg viewBox="0 0 300 200" fill="none" aria-hidden>
      {cells.map((i) => {
        const x = (i % 12) * 24 + 4
        const y = Math.floor(i / 12) * 24 + 20
        const scanned = i < 30
        return (
          <rect
            key={i}
            x={x}
            y={y}
            width={18}
            height={18}
            rx={3}
            className={bad.has(i) ? 'fill-bg ill-red' : scanned ? 'fill-aqua ill-aqua' : 'fill-surface ill-line'}
          />
        )
      })}
      <path d="M148 14 V120" className="ill-gold" strokeWidth={1.5} />
      <text x={154} y={12} fontSize={10} className="t-gold">scrub cursor</text>
      <text x={4} y={152} fontSize={11} className="t-muted">blake3(nugget) ≠ id</text>
      <text x={4} y={172} fontSize={11} className="t-red">2 mismatches → repair queued</text>
    </svg>
  )
}

function Foreman() {
  return (
    <section className="section agent" id="foreman">
      <div className="agent-copy">
        <span className="eyebrow">Foreman · MCP</span>
        <h2 className="h2">
          Your cluster, <span className="dim">fluent in agent.</span>
        </h2>
        <p>
          Everything an operator can do from the CLI, an agent can do over MCP. Read-only by default;
          write tools need <code>--allow-write</code> and are tagged destructive.
        </p>
        <div className="tools">
          {['cluster_status', 'locate_object', 'objects_at_risk', 'explain_repair', 'simulate_placement', 'bucket_usage'].map(
            (t) => (
              <code key={t}>{t}</code>
            ),
          )}
          {['drain_vein', 'set_quota', 'trigger_scrub'].map((t) => (
            <code key={t} className="w">
              {t}
            </code>
          ))}
        </div>
      </div>
      <div className="chat">
        <div className="msg-user">Which objects are at risk if vein-3 dies right now?</div>
        <div className="call">
          <div className="call-head">
            <span>objects_at_risk</span>
            <span>142 ms</span>
          </div>
          <pre>{`{ "vein": "vein-3",
  "at_risk": 14,
  "min_redundancy_left": 1 }`}</pre>
        </div>
        <div className="msg-agent">
          <strong>14 objects</strong> would drop to a single replica. Want me to drain vein-3 first? That needs
          write access.
        </div>
      </div>
    </section>
  )
}

function Stats() {
  return (
    <section className="section stats">
      <div className="stat">
        <b>
          2<span>/6</span>
        </b>
        <small>veins can fail on a 4+2 bucket</small>
      </div>
      <div className="stat">
        <b>
          ≤<span>1/N</span>
        </b>
        <small>data moved when a vein joins</small>
      </div>
      <div className="stat">
        <b>
          4<span> MiB</span>
        </b>
        <small>target nugget size</small>
      </div>
      <div className="stat">
        <b>
          0<span> lost</span>
        </b>
        <small>objects under make chaos</small>
      </div>
    </section>
  )
}

function Cta() {
  return (
    <section className="section cta" id="start">
      <h2 className="h2">
        Strike your first <em>vein</em> in five minutes.
      </h2>
      <div className="cta-cmd">
        <div className="cmd">
          <span>$</span> git clone {REPO.replace('https://', '')} &amp;&amp; make up
        </div>
        <small>Postgres · Adit · 6 veins in 2 zones · Foreman</small>
      </div>
    </section>
  )
}

function Footer() {
  return (
    <footer className="section footer">
      <span>© 2026 Lode. Dug by hand.</span>
      <nav>
        <a href={REPO}>GitHub</a>
        <a href={`${REPO}/tree/main/docs`}>Docs</a>
        <a href={`${REPO}/tree/main/docs/adr`}>ADRs</a>
        <a href={`${REPO}/blob/main/LICENSE`}>License</a>
      </nav>
    </footer>
  )
}

export default function App() {
  return (
    <>
      <Nav />
      <main className="frame">
        <Hero />
        <Strip />
        <Bento />
        <Foreman />
        <Stats />
        <Cta />
        <Footer />
      </main>
    </>
  )
}
