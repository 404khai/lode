import { el } from "./format";

interface Tool {
  name: string;
  description: string;
  annotations?: { readOnlyHint?: boolean; destructiveHint?: boolean };
  inputSchema?: { properties?: Record<string, { type?: string; description?: string }>; required?: string[] };
}

const examples: Record<string, unknown> = {
  objects_at_risk: { vein: "vein3" },
  vein_detail: { vein: "vein1" },
  locate_object: { bucket: "photos", key: "cat.jpg" },
  bucket_usage: { bucket: "photos" },
  explain_repair: { id: 1 },
  simulate_placement: { bucket: "photos", sample: "hello" },
  drain_vein: { vein: "vein6", dry_run: true },
};

export async function initForeman(): Promise<void> {
  const select = document.getElementById("tool") as HTMLSelectElement;
  const args = document.getElementById("args") as HTMLTextAreaElement;
  const out = document.getElementById("foreman-out")!;
  const res = await fetch("api/foreman/tools");
  if (!res.ok) {
    out.replaceChildren(el("p", "error", "Foreman unavailable"));
    return;
  }
  const tools = (await res.json()) as Tool[];
  for (const t of tools) {
    const o = el("option", "", t.annotations?.destructiveHint ? `${t.name} ⚠` : t.name);
    o.value = t.name;
    o.title = t.description;
    select.append(o);
  }
  const fill = () => {
    const t = tools.find((x) => x.name === select.value);
    const props = Object.keys(t?.inputSchema?.properties ?? {});
    const ex = examples[select.value] ?? Object.fromEntries((t?.inputSchema?.required ?? []).map((k) => [k, ""]));
    args.value = props.length ? JSON.stringify(ex, null, 0) : "{}";
    out.replaceChildren(el("p", "hint", t?.description ?? ""));
  };
  select.addEventListener("change", fill);
  select.value = tools.some((t) => t.name === "cluster_status") ? "cluster_status" : tools[0]?.name;
  fill();
  (document.getElementById("foreman-form") as HTMLFormElement).addEventListener("submit", async (ev) => {
    ev.preventDefault();
    let parsed: unknown;
    try {
      parsed = JSON.parse(args.value || "{}");
    } catch {
      out.replaceChildren(el("p", "error", "Arguments must be JSON."));
      return;
    }
    out.replaceChildren(el("p", "empty", "Asking Foreman…"));
    const r = await fetch("api/foreman/call", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: select.value, arguments: parsed }),
    });
    if (!r.ok) {
      out.replaceChildren(el("p", "error", (await r.text()).trim()));
      return;
    }
    const body = (await r.json()) as { summary: string; is_error: boolean; result?: unknown };
    const details = el("details");
    details.append(el("summary", "", "structured result"), el("pre", "", JSON.stringify(body.result, null, 2)));
    out.replaceChildren(el("p", body.is_error ? "error" : "answer", body.summary), details);
  });
}
