# =========================================================
# JSONL書き込み
# =========================================================
write_jsonl(io::IO, kind::AbstractString, data) =
    (JSON3.write(io, (; kind = kind, data = data)); write(io, '\n'))

mutable struct StreamJsonlWriter
    io::IO
    opened_at::Float64
    rotation_count::Int
end

_stream_sink_io(io::IO) = io
_stream_sink_io(w::StreamJsonlWriter) = w.io

function _maybe_fsync(io::IO)
    try
        Base.Libc.fsync(fd(io))
    catch e
        @debug "fsync skipped" exception = e
    end
    return nothing
end

function _flush_stream_sink!(cfg::StreamConfig, sink)
    io = _stream_sink_io(sink)
    flush(io)
    cfg.durable_writes && _maybe_fsync(io)
    return nothing
end

function _rotated_jsonl_path(cfg::StreamConfig, rotation_count::Int)
    stamp = Dates.format(Dates.now(Dates.UTC), dateformat"yyyymmdd\THHMMSS")
    idx = rotation_count
    while true
        path = joinpath(cfg.out_dir, "$(cfg.task_name).rot-$stamp-$(lpad(string(idx), 6, '0')).jsonl")
        isfile(path) || return path
        idx += 1
    end
end

function _open_stream_jsonl_writer(cfg::StreamConfig)
    return StreamJsonlWriter(open(out_jsonl(cfg), "a"), time(), 0)
end

function _close_stream_jsonl_writer!(w::StreamJsonlWriter)
    close(w.io)
    return nothing
end

function _reopen_stream_jsonl_writer!(cfg::StreamConfig, sink::StreamJsonlWriter; open_file = open)
    sink.io = open_file(out_jsonl(cfg), "a")
    sink.opened_at = time()
    return nothing
end

function _rotate_stream_jsonl_if_needed!(
    cfg::StreamConfig,
    sink;
    move_file = mv,
    open_file = open,
)
    sink isa StreamJsonlWriter || return nothing
    size_limit = cfg.rotate_jsonl_bytes > 0
    age_limit = cfg.rotate_jsonl_seconds > 0
    (size_limit || age_limit) || return nothing

    path = out_jsonl(cfg)
    current_size = isfile(path) ? stat(path).size : 0
    should_rotate =
        (size_limit && current_size >= cfg.rotate_jsonl_bytes) ||
        (age_limit && (time() - sink.opened_at) >= cfg.rotate_jsonl_seconds)
    should_rotate || return nothing
    current_size <= 0 && return nothing

    _flush_stream_sink!(cfg, sink)
    close(sink.io)
    sink.rotation_count += 1
    rotated = _rotated_jsonl_path(cfg, sink.rotation_count)
    try
        move_file(path, rotated; force = false)
        try
            _reopen_stream_jsonl_writer!(cfg, sink; open_file = open_file)
        catch e
            try
                isfile(rotated) && !isfile(path) && move_file(rotated, path; force = false)
            catch restore_error
                @warn "Failed to restore active stream JSONL after reopen failure" exception =
                    restore_error
            end
            try
                _reopen_stream_jsonl_writer!(cfg, sink)
            catch reopen_error
                @warn "Failed to reopen active stream JSONL after rotation failure" exception =
                    reopen_error
            end
            rethrow(e)
        end
    catch e
        try
            _reopen_stream_jsonl_writer!(cfg, sink)
        catch reopen_error
            @warn "Failed to reopen active stream JSONL after rotation failure" exception =
                reopen_error
        end
        rethrow(e)
    end
    @info "Rotated stream JSONL" old = path new = rotated
    return rotated
end

function _write_stream_jsonl_entries!(cfg::StreamConfig, sink, entries)
    isempty(entries) && return nothing
    _rotate_stream_jsonl_if_needed!(cfg, sink)
    io = _stream_sink_io(sink)
    for (kind, data) in entries
        write_jsonl(io, kind, data)
    end
    _flush_stream_sink!(cfg, sink)
    return nothing
end

function stream_jsonl_paths(cfg::StreamConfig)
    isdir(cfg.out_dir) || return String[]
    rotated_prefix = "$(cfg.task_name).rot-"
    paths = String[]
    for name in readdir(cfg.out_dir)
        startswith(name, rotated_prefix) || continue
        endswith(name, ".jsonl") || continue
        path = joinpath(cfg.out_dir, name)
        isfile(path) && push!(paths, path)
    end
    sort!(paths)
    active = out_jsonl(cfg)
    isfile(active) && push!(paths, active)
    return paths
end
