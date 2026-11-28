import * as d3 from "d3";
import type { Snapshot } from "./types";
import { el, num } from "./format";


export function renderKPIs(snap: Snapshot): void {
  const k = document.getElementById("kpis")!;
  k.replaceChildren();
  const items: [string, number, string][] = [
    ["under-replicated", snap.repairs.UnderReplicated, snap.repairs.UnderReplicated ? "warn" : "ok"],
    ["repairs queued", snap.repairs.Queued, snap.repairs.Queued ? "busy" : "ok"],
    ["repairs running", snap.repairs.Leased, snap.repairs.Leased ? "busy" : "ok"],
    ["lost", snap.repairs.Lost, snap.repairs.Lost ? "bad" : "ok"],
  ];
  for (const [label, value, tone] of items) {
    const box = el("div", `kpi ${tone}`);
    box.append(el("b", "", num(value)), el("span", "", label));
    k.append(box);
  }
}

export function renderTrend(snap: Snapshot): void {
  const history = (snap.trend ?? []).map((p) => ({ t: new Date(p.t), v: p.under_replicated }));
  history.push({ t: new Date(snap.time), v: snap.repairs.UnderReplicated });
  const svg = d3.select<SVGSVGElement, unknown>("#trend");
  const w = 340;
  const h = 70;
  svg.attr("viewBox", `0 0 ${w} ${h}`);
  const x = d3.scaleTime().domain(d3.extent(history, (d) => d.t) as [Date, Date]).range([4, w - 4]);
  const y = d3.scaleLinear().domain([0, Math.max(1, d3.max(history, (d) => d.v) ?? 1)]).nice().range([h - 14, 6]);
  const area = d3.area<{ t: Date; v: number }>().x((d) => x(d.t)).y0(h - 14).y1((d) => y(d.v)).curve(d3.curveMonotoneX);
  const line = d3.line<{ t: Date; v: number }>().x((d) => x(d.t)).y((d) => y(d.v)).curve(d3.curveMonotoneX);
  svg.selectAll("*").remove();
  svg.append("path").datum(history).attr("class", "trend-area").attr("d", area);
  svg.append("path").datum(history).attr("class", "trend-line").attr("d", line);
  const last = history[history.length - 1];
  svg.append("text").attr("class", "trend-label").attr("x", w - 4).attr("y", h - 2).attr("text-anchor", "end")
    .text(`now ${num(last.v)} · max ${num(d3.max(history, (d) => d.v) ?? 0)}`);
}

const COLS: [string, string][] = [["-1", "lost"], ["0", "0"], ["1", "1"], ["2", "2"], ["3", "3+"]];

export function renderHeatmap(snap: Snapshot): void {
  const root = document.getElementById("heatmap")!;
  root.replaceChildren();
  if (!snap.health.length) {
    root.append(el("p", "empty", "No objects stored yet."));
    return;
  }
  const max = d3.max(snap.health.flatMap((b) => Object.values(b.by_tolerance))) ?? 1;
  const table = el("table", "heat");
  const head = el("tr");
  head.append(el("th", "", "bucket"), el("th", "", "class"));
  for (const [, label] of COLS) head.append(el("th", "num", label));
  table.append(head);
  for (const b of snap.health) {
    const tr = el("tr");
    tr.append(el("td", "bucket", b.bucket), el("td", "class", b.class));
    for (const [k] of COLS) {
      const n = b.by_tolerance[k] ?? 0;
      const td = el("td", `cell t${k}`, n ? num(n) : "");
      td.style.setProperty("--a", n ? String(0.18 + 0.82 * Math.sqrt(n / max)) : "0");
      tr.append(td);
    }
    table.append(tr);
  }
  root.append(table);
}
