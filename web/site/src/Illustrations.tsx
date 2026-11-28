// Flat line illustrations for the bento grid. Colours come from CSS classes in index.css.

export function NuggetsArt() {
  const top = [
    { x: 150, w: 56, id: 'a3f9', dup: false },
    { x: 214, w: 44, id: '7c1e', dup: false },
    { x: 266, w: 70, id: 'e02b', dup: true },
    { x: 344, w: 50, id: '91dd', dup: false },
  ]
  const bottom = [
    { x: 150, w: 48, id: '4b08', dup: false },
    { x: 206, w: 70, id: 'e02b', dup: true },
    { x: 284, w: 56, id: 'c5a0', dup: false },
  ]
  const chip = (c: { x: number; w: number; id: string; dup: boolean }, y: number) => (
    <g key={`${c.id}-${c.x}-${y}`}>
      <rect
        x={c.x}
        y={y}
        width={c.w}
        height={36}
        rx={6}
        className={c.dup ? 'fill-gold ill-gold' : 'fill-aqua ill-aqua'}
      />
      <text x={c.x + 9} y={y + 22} fontSize={10} className={c.dup ? 't-gold' : 't-aqua'}>
        {c.id}…
      </text>
    </g>
  )
  return (
    <svg viewBox="0 0 510 200" fill="none" aria-hidden>
      <rect x={0} y={78} width={110} height={44} rx={8} className="fill-surface ill-line" />
      <text x={14} y={98} fontSize={11} className="t-muted">backup.tar</text>
      <text x={14} y={113} fontSize={10} className="t-faint">1 GiB</text>
      <path d="M110 100 H138" className="ill-faint" />
      <path d="M138 100 V58 H148 M138 100 V142 H148" className="ill-faint" />
      {top.map((c) => chip(c, 40))}
      {bottom.map((c) => chip(c, 124))}
      <path d="M301 76 V100 H241 V124" className="ill-gold flow" />
      <path d="M394 100 H420" className="ill-faint" />
      <rect x={420} y={80} width={82} height={40} rx={8} className="fill-bg ill-line" />
      <text x={430} y={97} fontSize={9} className="t-faint">on disk</text>
      <text x={430} y={112} fontSize={12} className="t-aqua">≈1.0 GiB</text>
      <text x={150} y={190} fontSize={10} className="t-faint">FastCDC · BLAKE3</text>
    </svg>
  )
}

export function S3Art() {
  const lines: { t: string; c: string }[] = [
    { t: '$ aws s3 cp ./ore s3://mine --recursive', c: 't-muted' },
    { t: 'upload: ore/vein-map.parquet', c: 't-faint' },
    { t: 'upload: ore/assay-2026.csv', c: 't-faint' },
    { t: '$ aws s3 ls s3://mine', c: 't-muted' },
    { t: '2026-11-28  vein-map.parquet', c: 't-aqua' },
    { t: '2026-11-28  assay-2026.csv', c: 't-aqua' },
  ]
  return (
    <svg viewBox="0 0 360 200" fill="none" aria-hidden>
      <rect x={0.5} y={0.5} width={359} height={199} rx={10} className="fill-bg ill-line" />
      <path d="M0 30 H360" className="ill-line" />
      {[18, 32, 46].map((cx) => (
        <circle key={cx} cx={cx} cy={15} r={4} className="fill-surface" />
      ))}
      {lines.map((l, i) => (
        <text key={i} x={16} y={56 + i * 22} fontSize={11} className={l.c}>
          {l.t}
        </text>
      ))}
      <rect x={16} y={176} width={7} height={13} className="t-gold" />
    </svg>
  )
}

export function HealArt() {
  return (
    <svg viewBox="0 0 300 200" fill="none" aria-hidden>
      <path d="M60 50 L150 150 L240 50 Z" className="ill-line" />
      <path d="M60 50 Q150 0 240 50" className="ill-aqua flow" strokeWidth={1.5} />
      <circle cx={60} cy={50} r={22} className="fill-aqua ill-aqua" />
      <text x={46} y={54} fontSize={11} className="t-aqua">v-1</text>
      <circle cx={240} cy={50} r={22} className="fill-aqua ill-aqua" />
      <text x={226} y={54} fontSize={11} className="t-aqua">v-4</text>
      <circle cx={150} cy={150} r={22} className="fill-bg ill-red" strokeDasharray="3 3" />
      <text x={136} y={154} fontSize={11} className="t-red">v-3</text>
      <path d="M140 140 L160 160 M160 140 L140 160" className="ill-red" opacity={0.4} />
      <rect x={186} y={160} width={108} height={24} rx={12} className="fill-gold" />
      <text x={198} y={176} fontSize={10} className="t-gold">smelter · 72%</text>
    </svg>
  )
}

export function ErasureArt() {
  const shards = ['d', 'd', 'x', 'd', 'p', 'x']
  return (
    <svg viewBox="0 0 300 200" fill="none" aria-hidden>
      {shards.map((s, i) => {
        const x = i * 50 + 2
        if (s === 'x')
          return (
            <g key={i}>
              <rect x={x} y={30} width={42} height={70} rx={6} className="fill-bg ill-red" strokeDasharray="3 3" />
              <path d={`M${x + 14} 58 l14 14 M${x + 28} 58 l-14 14`} className="ill-red" />
            </g>
          )
        return (
          <rect
            key={i}
            x={x}
            y={30}
            width={42}
            height={70}
            rx={6}
            className={s === 'p' ? 'fill-gold ill-gold' : 'fill-aqua ill-aqua'}
          />
        )
      })}
      <text x={2} y={124} fontSize={10} className="t-faint">4 data</text>
      <text x={244} y={124} fontSize={10} className="t-gold">+2 parity</text>
      <path d="M2 140 H294" className="ill-line" />
      <text x={2} y={178} fontSize={30} style={{ fill: "var(--text)" }}>
        0 B
      </text>
      <text x={72} y={176} fontSize={11} className="t-faint">lost with 2 veins down</text>
    </svg>
  )
}

export function PlacementArt() {
  // Rendezvous hashing: one nugget scored against every vein, top three win.
  const veins = [
    { id: 'vein-1', zone: 'a', score: 0.91, pick: true },
    { id: 'vein-2', zone: 'a', score: 0.34, pick: false },
    { id: 'vein-3', zone: 'a', score: 0.58, pick: false },
    { id: 'vein-4', zone: 'b', score: 0.77, pick: true },
    { id: 'vein-5', zone: 'b', score: 0.62, pick: true },
    { id: 'vein-6', zone: 'b', score: 0.12, pick: false },
  ]
  return (
    <svg viewBox="0 0 300 200" fill="none" aria-hidden>
      <rect x={0} y={84} width={64} height={32} rx={6} className="fill-gold ill-gold" />
      <text x={10} y={104} fontSize={10} className="t-gold">e02b…</text>
      {veins.map((v, i) => {
        const y = 14 + i * 30
        return (
          <g key={v.id}>
            <path d={`M64 100 C 90 100, 90 ${y + 8}, 110 ${y + 8}`} className={v.pick ? 'ill-aqua' : 'ill-line'} />
            <text x={116} y={y + 12} fontSize={10} className={v.pick ? 't-aqua' : 't-faint'}>
              {v.id}
            </text>
            <rect x={170} y={y + 3} width={110} height={8} rx={2} className="fill-surface" />
            <rect x={170} y={y + 3} width={110 * v.score} height={8} rx={2} className={v.pick ? 't-aqua' : 't-faint'} opacity={v.pick ? 1 : 0.4} />
            <text x={284} y={y + 11} fontSize={9} className="t-faint">{v.zone}</text>
          </g>
        )
      })}
    </svg>
  )
}

export function LifecycleArt() {
  const days = Array.from({ length: 12 }, (_, i) => i)
  return (
    <svg viewBox="0 0 300 200" fill="none" aria-hidden>
      <path d="M10 150 H290" className="ill-line" />
      {days.map((d) => (
        <path key={d} d={`M${10 + d * 25} 146 V154`} className="ill-line" />
      ))}
      <text x={10} y={174} fontSize={9} className="t-faint">day 0</text>
      <text x={248} y={174} fontSize={9} className="t-faint">day 90</text>
      <rect x={10} y={110} width={110} height={26} rx={5} className="fill-aqua ill-aqua" />
      <text x={20} y={127} fontSize={10} className="t-aqua">3× replicas</text>
      <path d="M120 123 H140" className="ill-faint flow" />
      <rect x={140} y={110} width={100} height={26} rx={5} className="fill-gold ill-gold" />
      <text x={150} y={127} fontSize={10} className="t-gold">EC 4+2</text>
      <path d="M240 123 H262" className="ill-faint flow" />
      <rect x={262} y={110} width={28} height={26} rx={5} className="fill-bg ill-faint" strokeDasharray="3 3" />
      <text x={10} y={40} fontSize={11} className="t-muted">quota  mine</text>
      <rect x={10} y={52} width={280} height={10} rx={3} className="fill-surface" />
      <rect x={10} y={52} width={210} height={10} rx={3} className="t-aqua" />
      <text x={10} y={84} fontSize={10} className="t-faint">742 GiB of 1 TiB</text>
      <text x={204} y={84} fontSize={10} className="t-gold">QuotaExceeded ›</text>
    </svg>
  )
}
