import * as d3 from "d3";
// Registers selection.transition(); Vite would otherwise tree-shake the
// side-effect import that patches it onto selections.
import "d3-transition";
import type { Snapshot, Vein } from "./types";
import { bytes } from "./format";

const W = 760;
const VEIN_W = 132;
const VEIN_H = 74;

/** Positions of each vein's card centre, used to draw repair flows. */
export const veinPos = new Map<string, { x: number; y: number }>();

let svg: d3.Selection<SVGSVGElement, unknown, HTMLElement, unknown>;
let flowLayer: d3.Selection<SVGGElement, unknown, HTMLElement, unknown>;

export function initTopology(): void {
  svg = d3.select<SVGSVGElement, unknown>("#topology").attr("viewBox", `0 0 ${W} 300`);
  svg.append("g").attr("class", "zones");
  flowLayer = svg.append("g").attr("class", "flows");
}

interface Host {
  name: string;
  veins: Vein[];
}

interface Zone {
  name: string;
  hosts: Host[];
}

function group(veins: Vein[]): Zone[] {
  const zones = d3.group(veins, (v) => v.zone || "default", (v) => v.host || v.id);
  return [...zones]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([name, hosts]) => ({
      name,
      hosts: [...hosts].sort(([a], [b]) => a.localeCompare(b)).map(([h, vs]) => ({ name: h, veins: vs })),
    }));
}

export function renderTopology(snap: Snapshot): void {
  const zones = group(snap.veins.filter((v) => v.state !== "removed"));
  const zoneW = Math.max(VEIN_W + 32, (W - 16 * (zones.length - 1)) / Math.max(zones.length, 1));
  let height = 0;
  veinPos.clear();
  const layout = zones.map((z, zi) => {
    const x = zi * (zoneW + 16);
    let y = 34;
    const hosts = z.hosts.map((h) => {
      const top = y;
      const cards = h.veins.map((v, vi) => {
        const cx = x + 16 + (vi % 2) * (VEIN_W + 8);
        const cy = top + 22 + Math.floor(vi / 2) * (VEIN_H + 8);
        veinPos.set(v.id, { x: cx + VEIN_W / 2, y: cy + VEIN_H / 2 });
        return { v, x: cx, y: cy };
      });
      y = top + 30 + Math.ceil(h.veins.length / 2) * (VEIN_H + 8);
      return { h, top, bottom: y - 4, cards };
    });
    height = Math.max(height, y + 8);
    return { z, x, hosts };
  });
  svg.attr("viewBox", `0 0 ${W} ${Math.max(height, 160)}`);

  const zoneSel = svg
    .select<SVGGElement>("g.zones")
    .selectAll<SVGGElement, (typeof layout)[number]>("g.zone")
    .data(layout, (d) => d.z.name)
    .join((enter) => {
      const g = enter.append("g").attr("class", "zone");
      g.append("rect").attr("class", "zone-box").attr("rx", 14);
      g.append("text").attr("class", "zone-label");
      return g;
    });
  zoneSel.select("rect.zone-box").attr("x", (d) => d.x).attr("y", 4).attr("width", zoneW).attr("height", height - 8);
  zoneSel.select("text.zone-label").attr("x", (d) => d.x + 14).attr("y", 24).text((d) => `zone ${d.z.name}`);

  const hostSel = zoneSel
    .selectAll<SVGGElement, (typeof layout)[number]["hosts"][number]>("g.host")
    .data((d) => d.hosts, (d) => d.h.name)
    .join((enter) => {
      const g = enter.append("g").attr("class", "host");
      g.append("text").attr("class", "host-label");
      return g;
    });
  hostSel.each(function (d) {
    const zx = d.cards[0]?.x ?? 0;
    d3.select(this).select("text.host-label").attr("x", zx).attr("y", d.top + 14).text(d.h.name);
  });

  const veinSel = hostSel
    .selectAll<SVGGElement, { v: Vein; x: number; y: number }>("g.vein")
    .data((d) => d.cards, (d) => d.v.id)
    .join((enter) => {
      const g = enter.append("g").attr("class", "vein");
      g.append("rect").attr("class", "vein-box").attr("rx", 10).attr("width", VEIN_W).attr("height", VEIN_H);
      g.append("circle").attr("class", "dot").attr("r", 5).attr("cx", 14).attr("cy", 17);
      g.append("text").attr("class", "vein-id").attr("x", 26).attr("y", 21);
      g.append("text").attr("class", "vein-state").attr("x", VEIN_W - 10).attr("y", 21).attr("text-anchor", "end");
      g.append("rect").attr("class", "cap-bg").attr("x", 10).attr("y", 34).attr("rx", 3).attr("height", 6).attr("width", VEIN_W - 20);
      g.append("rect").attr("class", "cap").attr("x", 10).attr("y", 34).attr("rx", 3).attr("height", 6);
      g.append("text").attr("class", "vein-meta").attr("x", 10).attr("y", 56);
      g.append("text").attr("class", "vein-warn").attr("x", 10).attr("y", 68);
      g.append("title");
      return g;
    });
  veinSel
    .attr("transform", (d) => `translate(${d.x},${d.y})`)
    .attr("data-state", (d) => d.v.state)
    .classed("flagged", (d) => d.v.io_errors > 0 || d.v.corrupt_nuggets > 0);
  veinSel.select("text.vein-id").text((d) => d.v.id);
  veinSel.select("text.vein-state").text((d) => d.v.state);
  veinSel
    .select("rect.cap")
    .transition()
    .duration(600)
    .attr("width", (d) => {
      // Lode's share of the disk, with a floor so tiny clusters still show a bar.
      const frac = d.v.capacity_bytes > 0 ? d.v.used_bytes / d.v.capacity_bytes : 0;
      return Math.max(frac * (VEIN_W - 20), d.v.used_bytes > 0 ? 3 : 0);
    });
  veinSel
    .select("text.vein-meta")
    .text((d) => `${bytes(d.v.used_bytes)} · ${d.v.holdings.replicas + d.v.holdings.shards} pieces`);
  veinSel.select("text.vein-warn").text((d) => {
    const w: string[] = [];
    if (d.v.io_errors) w.push(`${d.v.io_errors} I/O err`);
    if (d.v.corrupt_nuggets) w.push(`${d.v.corrupt_nuggets} corrupt`);
    if (d.v.heartbeat_age_seconds > 10) w.push(`silent ${Math.round(d.v.heartbeat_age_seconds)}s`);
    return w.join(" · ");
  });
  veinSel
    .select("title")
    .text((d) => `${d.v.id} (${d.v.addr})\n${d.v.holdings.replicas} replicas, ${d.v.holdings.shards} shards, ${bytes(d.v.holdings.bytes)} of objects\nheartbeat ${d.v.heartbeat_age_seconds.toFixed(1)}s ago`);
}

/** Animates one repair copy travelling from one vein to another. */
export function animateFlow(from: string, to: string): void {
  const a = veinPos.get(from);
  const b = veinPos.get(to);
  if (!a || !b || !flowLayer) return;
  const mx = (a.x + b.x) / 2;
  const my = Math.min(a.y, b.y) - 40 - Math.abs(a.x - b.x) * 0.15;
  const d = `M${a.x},${a.y} Q${mx},${my} ${b.x},${b.y}`;
  const path = flowLayer.append("path").attr("class", "flow").attr("d", d);
  const len = (path.node() as SVGPathElement).getTotalLength();
  path
    .attr("stroke-dasharray", `${len} ${len}`)
    .attr("stroke-dashoffset", len)
    .transition()
    .duration(900)
    .ease(d3.easeCubicOut)
    .attr("stroke-dashoffset", 0)
    .transition()
    .duration(1200)
    .style("opacity", 0)
    .remove();
  const dot = flowLayer.append("circle").attr("class", "flow-dot").attr("r", 4);
  dot
    .transition()
    .duration(900)
    .ease(d3.easeCubicOut)
    .attrTween("transform", () => (t) => {
      const p = (path.node() as SVGPathElement).getPointAtLength(t * len);
      return `translate(${p.x},${p.y})`;
    })
    .transition()
    .duration(300)
    .attr("r", 9)
    .style("opacity", 0)
    .remove();
}
