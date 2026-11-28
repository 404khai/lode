# Graft report: Lantern UI (Phase 8)

Agent: Claude (Opus 5.5), working in parallel with another agent building the
marketing homepage (`web/site`, graft `homepage`).
Graft: `lantern-ui`, reflink backend, created from `feat/lantern`, landed on
`feat/lantern` as `66c4b5d` via `graft_integrate`.
Graft version: 0.2.0 on macOS (APFS, clonefile).

## What I did with graft
1. `graft_init`, `graft_list`, then `graft_status homepage`. That told me exactly
   which paths the other agent owned (`web/site/**`), so I could place Lantern in
   `web/lantern/` with no overlap before writing a line.
2. `graft_new lantern-ui --from feat/lantern`, then wrote the UI (16 files) into
   the graft's working tree.
3. `graft_run` for every build (`npm install`, `npm run build`, i.e. `tsc` + Vite):
   confined to the graft, which is what I want for a toolchain that writes all
   over the place.
4. While the graft was live, the homepage landed on `main`. I rebased the
   *Root* branch (backend work) onto `main`; the graft stayed on its old base.
5. `graft_radar` (no overlaps), `graft_status`, `graft_integrate` dry run, then
   the real integrate with `update_checkout: true`.

## What I loved
- **Seeing the other agent's footprint before starting.** `graft_status homepage`
  listing `web/site/...` was the single most useful call: it turned "be careful
  not to collide" into a concrete boundary.
- **`graft_radar`** gives a cheap, explicit "you are safe to land" signal. An
  empty `overlaps` list before integrating is reassuring in a way that
  "git merge worked" is not.
- **Integrate's dry run is honest and specific.** It refused with
  `checkout_would_fall_behind` and told me the exact fix (`update_checkout: true`
  or land elsewhere with `onto`). Clear, actionable error codes throughout.
- **Structural merge across a moved base.** My graft was based on a commit that
  no longer existed on the branch after I rebased Root, and it still landed
  cleanly with one call. No manual rebase needed for additive work.
- **Reflink grafts are instant** and look like a normal checkout, so editors,
  `npm`, Vite and headless Chrome (pointed at the built `dist/`) all just worked.
- **`graft_run` structured output** (`exit_code`, `stdout`, `stderr`, `timed_out`)
  is easy for an agent to act on.

## Pain points
1. **The MCP server was not available to me.** The repo rule says "control graft
   via its MCP server", but `graft mcp` was not registered for this session, and
   MCP servers attach at session start. I wrote a 40-line stdio MCP client to
   call the tools. I've now added `.mcp.json` so future sessions get it natively.
2. **The MCP surface is smaller than the CLI.** No `rebase`, `claim`/`claims`,
   `materialize` or `queue` tools. I could not claim `web/lantern/**` for my
   graft, which is exactly what I wanted to do with another agent active.
3. **`graft_run` sandbox vs. package managers.** `npm install` failed with
   `EACCES` writing `~/.npm` (the error even suggests `sudo chown`, which is
   misleading because the directory is owned by the user and the sandbox is the
   cause). `dev_caches: true` did **not** cover npm's cache. The workaround was
   `npm install --cache ./.npm-cache` inside the graft.
4. **`command` must be an array** but the tool description doesn't make that
   obvious; my first call with a string failed with `invalid_arguments`.
5. **Paths contain spaces** (`~/Library/Application Support/graft/...`), and
   `graft_doctor` warns about this. It works if you quote everything, but it's a
   constant footgun in shell commands. A default `GRAFT_HOME` without spaces on
   macOS would remove a whole class of mistakes.
6. **Dry-run → real run needs re-specifying everything**, and the real integrate
   needed `confirm: true`, which the dry run didn't mention up front.

## Improvements I'd love
- Expose **`graft_claim` / `graft_claims`** over MCP, with path globs, so parallel
  agents can declare ownership (`web/lantern/**`) and get a refused write rather
  than a late conflict.
- Expose **`graft_rebase`** and **`graft_queue`** over MCP.
- Make `dev_caches` cover **npm, pnpm, bun and yarn** caches (and say which caches
  it covers in the tool description), or detect an `EACCES` under the user's home
  inside `graft_run` and hint at `dev_caches`.
- Accept a **string `command`** (run via `sh -c`) in `graft_run`, or show an
  example array in the schema description.
- A **`graft_integrate` dry-run result that returns the exact arguments** for the
  real call (including `confirm`), so an agent can replay it.
- A **"who else is here" summary** at `graft_new`: list live grafts and their
  changed paths automatically, since that's what every parallel agent should
  check first.
- `graft init` could offer to write `.mcp.json` / editor MCP config for the
  repository.
