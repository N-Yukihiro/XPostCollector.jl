# XPostCollector

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://N-Yukihiro.github.io/XPostCollector.jl/stable/)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://N-Yukihiro.github.io/XPostCollector.jl/dev/)
[![Build Status](https://github.com/N-Yukihiro/XPostCollector.jl/actions/workflows/CI.yml/badge.svg?branch=master)](https://github.com/N-Yukihiro/XPostCollector.jl/actions/workflows/CI.yml?query=branch%3Amaster)
[![Coverage](https://codecov.io/gh/N-Yukihiro/XPostCollector.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/N-Yukihiro/XPostCollector.jl)

XPostCollector.jl collects posts from the X API v2 search endpoints and filtered
stream endpoint. It stores raw JSONL safely, keeps state for resumable runs,
deduplicates collected posts with SQLite, and converts outputs to compact CSV,
wide CSV, and optionally Arrow files.

## Setup

Use Julia 1.12 or newer. In a development checkout, instantiate the project:

```julia
import Pkg
Pkg.instantiate()
```

Network collection requires an X API bearer token in the `BEARER_TOKEN`
environment variable:

```sh
export BEARER_TOKEN="..."
```

On PowerShell:

```powershell
$env:BEARER_TOKEN = "..."
```

## REST search collection

Use `SearchConfig` with `run_collector` for recent search or full-archive search.
The recent-search endpoint covers the last seven days.

```julia
using XPostCollector

cfg = SearchConfig(
    task_name = "recent_julia",
    keywords_or = ["Julia"],
    search_mode = :recent,
    extra_query_tail = "lang:en",
    target_posts = 500,
    out_dir = "data",
    log_dir = "logs",
)

run_collector(cfg)
convert_outputs(cfg)
convert_outputs_wide(cfg)
```

Set `start_time_jst` and `end_time_jst` when you need an explicit local time
window. With `search_mode = :auto`, XPostCollector chooses recent search when the
requested end time is within the recent-search window and full-archive search
when it is older. Full-archive search requires the corresponding X API access;
if that access is unavailable, use `search_mode = :recent` and keep the time
window within the last seven days.

## Long-running filtered stream

For unattended stream collection, run the collector under an external supervisor such
as `systemd`, `tmux`, or your job runner, and monitor the output directory size.

```julia
using XPostCollector

cfg = StreamConfig(
    task_name = "stream_weekly",
    keywords_or = ["Julia"],
    max_seconds = 0,
    max_posts = 0,
    reconnect = true,
    max_reconnects = 0,          # unlimited reconnect attempts
    rotate_jsonl_bytes = 1_000_000_000,
    state_flush_interval_seconds = 30.0,
)

run_stream_collector(cfg)
```

`run_stream_collector` manages filtered stream rules by default. Use
`list_stream_rules(cfg)` to inspect current rules and `ensure_stream_rule!(cfg)`
when you want to create or update the configured rule explicitly before opening
the stream.

Stream state is written to `task_name.stream.state.json`. Disconnect windows are
recorded in `task_name.stream.gaps.jsonl` so they can be reviewed or backfilled
later with REST search.

## Output conversion

Both REST and streaming collectors write JSONL first. Convert the collected JSONL
to analysis-friendly files with the same functions:

```julia
convert_outputs(cfg)       # compact CSV, and Arrow if enabled
convert_outputs_wide(cfg)  # CSV with joined includes such as users/media/places
```

For stream configs, conversion includes rotated JSONL files named
`task_name.rot-*.jsonl` as well as the active `task_name.jsonl` file.

Typical files are:

- `task_name.jsonl`: raw normalized JSONL records.
- `task_name.state.json`: REST collection and conversion state.
- `task_name.stream.state.json`: filtered stream state.
- `task_name.stream.gaps.jsonl`: recorded stream disconnect windows.
- `task_name.seen.sqlite`: SQLite database used for deduplication.
- `task_name.csv`: compact converted rows.
- `task_name.wide.csv`: wider converted rows with joined include data.
- `task_name.arrow` and `task_name.wide.arrow`: optional Arrow outputs.
- `task_name.rot-*.jsonl`: rotated stream JSONL files.

## Citing

See [`CITATION.bib`](CITATION.bib) for the relevant reference(s).
