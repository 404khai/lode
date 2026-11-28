import "./styles.css";
import type { LodeEvent, Snapshot } from "./types";
import { animateFlow, initTopology, renderTopology } from "./topology";
import { renderHeatmap, renderKPIs, renderTrend } from "./redundancy";
import { addEvent } from "./timeline";
import { initObjectSearch, setSnapshot } from "./object";
import { initForeman } from "./foreman";
import { bytes, el, num } from "./format";

const STATUS_TEXT: Record<Snapshot["status"], string> = {
  green: "fully redundant",
  repairing: "repairing",
  degraded: "degraded",
  red: "data unreadable",
};

function renderHeader(s: Snapshot): void {
  const status = document.getElementById("status")!;
  status.setAttribute("data-status", s.status);
  status.querySelector("span")!.textContent = STATUS_TEXT[s.status];
  const t = document.getElementById("totals")!;
  const healthy = s.veins.filter((v) => v.state === "healthy").length;
  t.replaceChildren(
    ...[
      [`${healthy}/${s.veins.filter((v) => v.state !== "removed").length}`, "veins healthy"],
      [num(s.totals.objects), "objects"],
      [bytes(s.totals.logical_bytes), "stored"],
      [bytes(s.totals.unique_bytes), "unique"],
    ].map(([v, l]) => {
      const d = el("div");
      d.append(el("b", "", v), el("span", "", l));
      return d;
    }),
  );
}

/** Renders each panel independently so one failure cannot blank the rest. */
function safely(name: string, f: () => void): void {
  try {
    f();
  } catch (err) {
    console.error(name, err);
    const box = document.getElementById("render-error") ?? document.body.appendChild(el("pre", "render-error"));
    box.id = "render-error";
    box.textContent = `${name}: ${err instanceof Error ? err.stack ?? err.message : String(err)}`;
  }
}

function onSnapshot(s: Snapshot): void {
  setSnapshot(s);
  safely("header", () => renderHeader(s));
  safely("topology", () => renderTopology(s));
  safely("kpis", () => renderKPIs(s));
  safely("trend", () => renderTrend(s));
  safely("heatmap", () => renderHeatmap(s));
}

function onEvent(e: LodeEvent): void {
  addEvent(e);
  if (e.type === "repair.copy" && e.attrs) {
    animateFlow(String(e.attrs.from ?? ""), String(e.attrs.to ?? ""));
  }
}

function connect(): void {
  const src = new EventSource("api/events");
  src.addEventListener("snapshot", (m) => onSnapshot(JSON.parse((m as MessageEvent).data)));
  src.addEventListener("event", (m) => onEvent(JSON.parse((m as MessageEvent).data)));
  src.onerror = () => document.getElementById("status")!.setAttribute("data-status", "connecting");
}

initTopology();
initObjectSearch();
void initForeman();
connect();
