export type VeinState = "healthy" | "suspect" | "down" | "draining" | "removed";

export interface Vein {
  id: string;
  state: VeinState;
  available: boolean;
  zone: string;
  host: string;
  addr: string;
  used_bytes: number;
  capacity_bytes: number;
  heartbeat_age_seconds: number;
  io_errors: number;
  corrupt_nuggets: number;
  holdings: { replicas: number; shards: number; bytes: number };
}

export interface Snapshot {
  time: string;
  veins: Vein[];
  totals: { buckets: number; objects: number; versions: number; logical_bytes: number; unique_nuggets: number; unique_bytes: number };
  repairs: { Queued: number; Leased: number; Lost: number; Runnable: number; UnderReplicated: number };
  trend: { t: string; under_replicated: number; open_repairs: number }[] | null;
  health: { bucket: string; class: string; by_tolerance: Record<string, number> }[];
  status: "green" | "repairing" | "degraded" | "red";
}

export interface LodeEvent {
  type: string;
  component: string;
  time: string;
  attrs?: Record<string, unknown>;
}

export interface ObjectLocation {
  bucket: string;
  key: string;
  version_id: string;
  size: number;
  tolerance: number;
  veins: string[];
  nuggets: {
    nugget: string;
    length: number;
    layout: string;
    tolerance: number;
    locations: { vein: string; state: string; shard?: number }[];
  }[];
}
