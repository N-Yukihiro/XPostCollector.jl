# XPostCollector

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://N-Yukihiro.github.io/XPostCollector.jl/stable/)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://N-Yukihiro.github.io/XPostCollector.jl/dev/)
[![Build Status](https://github.com/N-Yukihiro/XPostCollector.jl/actions/workflows/CI.yml/badge.svg?branch=master)](https://github.com/N-Yukihiro/XPostCollector.jl/actions/workflows/CI.yml?query=branch%3Amaster)
[![Coverage](https://codecov.io/gh/N-Yukihiro/XPostCollector.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/N-Yukihiro/XPostCollector.jl)

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

Stream state is written to `task_name.stream.state.json`. Disconnect windows are
recorded in `task_name.stream.gaps.jsonl` so they can be reviewed or backfilled
later with REST search. Rotated stream JSONL files named `task_name.rot-*.jsonl`
are included by `convert_outputs(cfg)` and `convert_outputs_wide(cfg)`.

## Citing

See [`CITATION.bib`](CITATION.bib) for the relevant reference(s).
