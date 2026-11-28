import type { LodeEvent } from "./types";
import { el } from "./format";

let count = 0;

/** Human description and tone for an event. */
function describe(e: LodeEvent): [string, string] {
  const a = e.attrs ?? {};
  const s = (k: string) => String(a[k] ?? "");
  const short = (k: string) => s(k).slice(0, 10);
  switch (e.type) {
    case "vein.state":
      return [`${s("vein")} ${s("from")} → ${s("to")}`, s("to") === "healthy" ? "ok" : s("to") === "down" ? "bad" : "warn"];
    case "vein.unreachable":
      return [`${s("vein")} unreachable`, "warn"];
    case "repair.queued":
      return [`${s("jobs")} repairs queued (${s("reason")}, ${s("vein")})`, "busy"];
    case "repair.copy":
      return [`copy ${short("nugget")} ${s("from") || "?"} → ${s("to")}`, "flow"];
    case "repair.done":
      return [`repaired ${short("nugget")} (${s("reason")})`, "ok"];
    case "nugget.lost":
      return [`nugget ${short("nugget")} lost`, "bad"];
    case "nugget.corrupt":
    case "assay.corrupt":
      return [`corrupt copy of ${short("nugget")} on ${s("vein") || "?"}`, "bad"];
    case "nugget.under_replicated":
      return [`degraded write ${short("nugget")}`, "warn"];
    case "gc.pass":
      return [`GC: ${s("nuggets_reclaimed")} reclaimed`, "muted"];
    case "lifecycle.pass":
      return [`lifecycle: ${s("expired")} expired, ${s("nuggets_transitioned")} transitioned`, "muted"];
    case "rebalance.pass":
      return [`rebalance moved ${s("moved")}`, "muted"];
    case "object.put":
      return [`put ${s("bucket")}/${s("key")}`, "muted"];
    case "object.deleted":
      return [`delete ${s("bucket")}/${s("key")}`, "muted"];
    default:
      return [e.type, "muted"];
  }
}

const NOISY = new Set(["object.put", "object.deleted", "upload.created", "upload.completed", "object.tagged"]);

export function addEvent(e: LodeEvent): void {
  const list = document.getElementById("timeline")!;
  if (NOISY.has(e.type) && list.childElementCount > 0 && list.firstElementChild?.getAttribute("data-type") === e.type) {
    // Collapse bursts of routine traffic into one line.
    const n = Number(list.firstElementChild.getAttribute("data-n") ?? "1") + 1;
    list.firstElementChild.setAttribute("data-n", String(n));
    list.firstElementChild.querySelector(".n")!.textContent = `×${n}`;
    return;
  }
  document.getElementById("timeline-empty")?.remove();
  const [text, tone] = describe(e);
  const li = el("li", tone);
  li.setAttribute("data-type", e.type);
  const time = new Date(e.time);
  li.append(el("time", "", time.toLocaleTimeString([], { hour12: false })), el("span", "c", e.component), el("span", "t", text), el("span", "n", ""));
  list.prepend(li);
  while (list.childElementCount > 300) list.lastElementChild?.remove();
  count++;
  document.getElementById("event-count")!.textContent = `${count} received`;
}
