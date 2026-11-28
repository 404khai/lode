# Graft report — lode homepage

Task: build the Lode marketing homepage in `web/site` (bun + Vite + React), Allo typeface,
aqua/gold on `#141414`, Supabase-style bento illustrations, Maxim-style framed hero.
One graft (`homepage`, from `main`), driven through the `graft` CLI.

## What I loved

- **It kept me off someone else's work.** Root was on `feat/lantern` with uncommitted Lantern
  changes from another session. `graft new homepage --from main` gave me a clean `main` tree in
  about a second (111 files reflinked), and I never touched those changes or had to stash
  anything.
- **`graft status` is a real changelist.** It listed exactly the files I'd added, which made
  writing the commit message and checking for stray build output easy.
- **`materialize --branch` fits the repo's PR flow.** I landed one commit on a branch in Root and
  had nothing to clean up afterwards.

## Pain points

- **No MCP server was wired into this session.** AGENTS.md says to drive graft via MCP, but
  `graft mcp` wasn't registered as a server for this project, so I fell back to the CLI. The
  CLI was fine; the mismatch with the written rule just cost time.
- **Untracked Root files don't follow you.** `web/logo.webp` exists only in Root's working tree,
  so it was missing from the graft and I had to copy it in by hand. `--clone-ignored` covers
  ignored paths, but not untracked ones.
- **The mount path is long and has a space in it** (`~/Library/Application Support/graft/...`),
  so every shell command needs careful quoting. `cd "$(graft path homepage)"` helps, but each
  tool call starts in a fresh shell.

## Improvements I'd like

- A `--clone-untracked <paths>` option (or have `--clone-ignored` accept untracked files) for
  assets that haven't been committed yet.
- A `graft mcp install` that registers the server for the current project with the agent
  client, so the AGENTS.md rule works out of the box.
- An optional short mount path without spaces (for example a `~/.graft/<name>` symlink).
