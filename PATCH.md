# PATCH — free slot on complete (not emergent)

Chris: slot free for mcp-background-job is **NOT** emergent — use **pattern-forge**.

## Problem

`JobManager.execute_command` counted only `RUNNING` jobs against
`max_concurrent_jobs`, but `cleanup_completed_jobs` never removed terminal
records from `_jobs` and never ran before the admit check. Under agent-fleet
floods, completed/failed/killed jobs left process wrappers (and eventually
stale state) while the periodic cleanup interval (300s) was never started.
Effective capacity collapsed; clients saw "Maximum concurrent jobs limit
reached" with no true RUNNING work.

## pattern-forge evidence

### `forge retrieve` (estate + package)

| Query | Top hits | Pattern taken |
|---|---|---|
| `SlotPool acquire release removeJob completed cleanup concurrent` | `python/robomp/src/slot_pool.py` (SlotPool), `corral/src/scheduledTasks.ts` (`removeJob` / `getActiveJobCount`) | **Release slot on terminal**; delete job record so active count drops |
| `cleanup_completed_jobs max_concurrent_jobs execute_command RUNNING` | `service.py` `execute_command` + `cleanup_completed_jobs` | Wire reclaim **before** the limit check |

### `forge race` (strategy ledger)

Strategies raced (first-valid-wins doctrine):

1. `slot_pool_release_on_terminal` — acquire/release; on COMPLETED/FAILED/KILLED release immediately
2. `cleanup_before_limit_check` — sync statuses + purge terminal, then recount before reject
3. `periodic_gc_loop` — server start spawns `MCP_BG_CLEANUP_INTERVAL` task

All three VALID; combined into the maximal patch below.

### `forge borrow` (prior art)

| Query | Top hits (score) | Signal |
|---|---|---|
| `background job pool free slot on complete concurrent limit` | Continuum (KV TTL), No Cords Attached (lock-free queues), DriftSched | TTL / release-on-complete for scarce slots |
| `asyncio job manager cleanup completed jobs max concurrent` | Capacity Planning… Uncertainty; Venn resource mgmt | Capacity must reclaim finished work |
| `MCP FastMCP stdio reconnect after error` | MCP Server Architecture Patterns (arxiv 2606.30317); Bridging Protocol and Production; AgentCheck / AgentTether | stdio MCP servers must survive tool floods; GC on the server side, not client reconnect alone |

GitHub code-search skipped (no `GITHUB_TOKEN`); exa skipped (`--skip exa`). Free sources (arXiv, HF Papers) OK; OpenAlex/S2 429 and DBLP bot-wall degraded as documented in pattern-forge honest limits.

## Changes

### `config.py`

- Default `max_concurrent_jobs`: **10 → 32**
- Default `cleanup_interval_seconds`: **300 → 60**
- New `job_retention_seconds` (default **0** = purge terminal on cleanup)
- Env: `MCP_BG_MAX_JOBS`, `MCP_BG_CLEANUP_INTERVAL`, `MCP_BG_JOB_RETENTION`, …

### `service.py`

- `execute_command`: **`await reclaim_slots(force_purge_terminal=True)`** before the max check
- `cleanup_completed_jobs`: actually **deletes** COMPLETED/FAILED/KILLED from `_jobs` (and process wrappers)
- `reclaim_slots`: sync statuses, optional timeout kill, then cleanup
- `run_cleanup_loop`: asyncio periodic GC

### `server.py`

- Start periodic cleanup on the running event loop (`ensure_cleanup_loop_async` / `_ensure_cleanup_loop`)
- Eager `get_job_manager()` at `main()` boot so `MCP_BG_*` applies immediately
- Cancel cleanup task on shutdown

### gatehouse `.dist` (ranch)

`barn/gatehouse/mcp_config.json.dist` background-job entry:

```json
"env": {
  "MCP_BG_MAX_JOBS": "32",
  "MCP_BG_CLEANUP_INTERVAL": "60"
}
```

(live `mcp_config.json` is gitignored — `.dist` is the template)

## Doctrine citation

pattern-forge: *many readers, one writer; the first valid answer wins; the slow
path stays hot because we write down who won.* Here the scarce writer-side
resource is the RUNNING slot; winners (terminal jobs) must leave the pool.
