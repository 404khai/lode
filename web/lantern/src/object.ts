import type { ObjectLocation, Snapshot } from "./types";
import { bytes, el } from "./format";

let latest: Snapshot | null = null;
export function setSnapshot(s: Snapshot): void {
  latest = s;
}

export function initObjectSearch(): void {
  const form = document.getElementById("object-form") as HTMLFormElement;
  const out = document.getElementById("object")!;
  form.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const fd = new FormData(form);
    const q = new URLSearchParams({ bucket: String(fd.get("bucket")), key: String(fd.get("key")) });
    out.replaceChildren(el("p", "empty", "Locating…"));
    const res = await fetch(`api/object?${q}`);
    if (!res.ok) {
      out.replaceChildren(el("p", "error", (await res.text()).trim()));
      return;
    }
    render(out, (await res.json()) as ObjectLocation);
  });
}

function render(out: HTMLElement, o: ObjectLocation): void {
  const veins = (latest?.veins ?? []).filter((v) => v.state !== "removed").map((v) => v.id);
  for (const v of o.veins) if (!veins.includes(v)) veins.push(v);
  const summary = el("p", "obj-summary");
  summary.innerHTML = "";
  summary.append(
    el("b", "", `${o.bucket}/${o.key}`),
    el("span", "", ` · ${bytes(o.size)} · ${o.nuggets.length} nuggets · version ${o.version_id} · `),
    el("span", o.tolerance > 0 ? "tol ok" : o.tolerance === 0 ? "tol warn" : "tol bad", `survives ${o.tolerance} more vein loss${o.tolerance === 1 ? "" : "es"}`),
  );
  const table = el("table", "placement");
  const head = el("tr");
  head.append(el("th", "", "nugget"), el("th", "", "layout"));
  for (const v of veins) head.append(el("th", "vcol", v));
  head.append(el("th", "num", "tol"));
  table.append(head);
  for (const n of o.nuggets) {
    const tr = el("tr");
    const id = el("td", "mono", n.nugget.slice(0, 12));
    id.title = `${n.nugget} (${bytes(n.length)})`;
    tr.append(id, el("td", "", n.layout));
    for (const v of veins) {
      const locs = n.locations.filter((l) => l.vein === v);
      const td = el("td", "slot");
      if (locs.length) {
        const l = locs[0];
        const chip = el("span", "chip", l.shard !== undefined ? `s${l.shard}` : "●");
        chip.setAttribute("data-state", l.state);
        chip.title = `${v}: ${l.shard !== undefined ? `shard ${l.shard}` : "replica"} (${l.state})`;
        td.append(chip);
      }
      tr.append(td);
    }
    tr.append(el("td", n.tolerance > 0 ? "num ok" : n.tolerance === 0 ? "num warn" : "num bad", String(n.tolerance)));
    table.append(tr);
  }
  out.replaceChildren(summary, table);
}
