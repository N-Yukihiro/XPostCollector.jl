struct StreamOutcome
    kind::Symbol
    stop_reason::String
    sleep_seconds::Float64
    activity::Bool
    status::Int
    error::String
    error_type::String
    error_detail::String
    rate_limit_limit::Int
    rate_limit_remaining::Int
    rate_limit_reset::Int
    retry_after::Int
end

Base.@kwdef mutable struct StreamGapSnapshot
    start_time_utc::Union{Nothing,String} = nothing
    reason::String = ""
    status::Int = 0
    error_type::String = ""
    error_detail::String = ""
    rate_limit_limit::Int = 0
    rate_limit_remaining::Int = 0
    rate_limit_reset::Int = 0
    retry_after::Int = 0
end

const STREAM_EMPTY_READ_SLEEP_SECONDS = 0.01

function StreamOutcome(;
    kind::Symbol,
    stop_reason::AbstractString = "",
    sleep_seconds::Real = 0,
    activity::Bool = false,
    status::Integer = 0,
    error::AbstractString = "",
    error_type::AbstractString = "",
    error_detail::AbstractString = "",
    rate_limit_limit::Integer = 0,
    rate_limit_remaining::Integer = 0,
    rate_limit_reset::Integer = 0,
    retry_after::Integer = 0,
)
    return StreamOutcome(
        kind,
        String(stop_reason),
        Float64(sleep_seconds),
        activity,
        Int(status),
        String(error),
        String(error_type),
        String(error_detail),
        Int(rate_limit_limit),
        Int(rate_limit_remaining),
        Int(rate_limit_reset),
        Int(retry_after),
    )
end

function _stream_line_payload(line::AbstractString)::Union{Nothing,String}
    s = strip(String(line))
    isempty(s) && return nothing
    if startswith(s, "data:")
        s = strip(s[6:end])
        isempty(s) && return nothing
    end
    s == "[DONE]" && return nothing
    return s
end

function _stream_event_meta(obj, received_at::AbstractString, page_new::Int)
    meta = Dict{String,Any}("received_at" => String(received_at), "result_count" => page_new)
    _json_haskey(obj, "matching_rules") &&
        (meta["matching_rules"] = _json_get(obj, "matching_rules", nothing))
    _json_haskey(obj, "errors") && (meta["errors"] = _json_get(obj, "errors", nothing))
    _json_haskey(obj, "meta") && (meta["stream_meta"] = _json_get(obj, "meta", nothing))
    return meta
end

function _stream_data_items(obj)
    data = _json_get(obj, "data", nothing)
    data === nothing && return Any[]
    if data isa AbstractVector
        return data
    end
    return Any[data]
end

function handle_stream_line!(
    cfg::StreamConfig,
    st::StreamState,
    sdb::SeenDB,
    sink,
    line::AbstractString,
)::Symbol
    payload = _stream_line_payload(line)
    if payload === nothing
        st.keepalive_count += 1
        st.last_heartbeat_at = stream_now_utc()
        return :keepalive
    end

    obj = try
        JSON3.read(payload)
    catch e
        @warn "Stream JSON parse error (skip)" exception = e line = payload
        return :invalid
    end

    new_tweets = Any[]
    new_ids = String[]
    if _json_haskey(obj, "data")
        seen_in_event = Set{String}()
        for tw in _stream_data_items(obj)
            tid = safe_str(_json_get(tw, "id", nothing); default = "")
            isempty(tid) && continue
            tid in seen_in_event && continue
            push!(seen_in_event, tid)
            if !seen_exists(sdb, tid)
                push!(new_tweets, tw)
                push!(new_ids, tid)
            end
        end
    end

    if !isempty(new_tweets)
        DBInterface.execute(sdb.db, "BEGIN;")
        try
            seen_at = utc_now_z(lag_seconds = 0)
            inserted = 0
            accepted_tweets = Any[]
            for (tw, tid) in zip(new_tweets, new_ids)
                if mark_seen!(sdb, tid, seen_at)
                    push!(accepted_tweets, tw)
                    inserted += 1
                end
            end

            page_new = length(accepted_tweets)
            if page_new == 0
                DBInterface.execute(sdb.db, "COMMIT;")
                return _json_haskey(obj, "errors") ? :errors : :ignored
            end

            entries = Vector{Tuple{String,Any}}()
            for tw in accepted_tweets
                push!(entries, ("tweet", tw))
            end

            include_count = 0
            if cfg.write_includes && _json_haskey(obj, "includes")
                inc = _json_get(obj, "includes", nothing)
                for (k, arr) in _json_pairs(inc)
                    kstr = String(k)
                    for item in _json_items(arr)
                        push!(entries, ("include:$kstr", item))
                        include_count += 1
                    end
                end
            end

            received_at = stream_now_utc()
            push!(
                entries,
                (
                    "page",
                    Dict(
                        "page" => st.total_tweets + page_new,
                        "endpoint" => "stream",
                        "meta" => _stream_event_meta(obj, received_at, page_new),
                    ),
                ),
            )

            _write_stream_jsonl_entries!(cfg, sink, entries)
            DBInterface.execute(sdb.db, "COMMIT;")
            st.total_tweets += page_new
            st.total_includes += include_count
            st.seen_count += inserted
            st.last_event_at = received_at
        catch e
            try
                DBInterface.execute(sdb.db, "ROLLBACK;")
            catch
            end
            rethrow(e)
        end
        return :tweet
    end

    return _json_haskey(obj, "errors") ? :errors : :ignored
end

function _stream_stop_reason(cfg::StreamConfig, st::StreamState, started_at::Float64)
    cfg.max_posts > 0 && st.total_tweets >= cfg.max_posts && return "max_posts"
    cfg.max_seconds > 0 && (time() - started_at) >= cfg.max_seconds && return "max_seconds"
    return nothing
end

stream_now_utc() = string(Dates.now(Dates.UTC))

function _refresh_stream_storage_stats!(cfg::StreamConfig, st::StreamState)
    st.db_path = out_db(cfg)
    st.db_size_bytes = isfile(st.db_path) ? stat(st.db_path).size : 0
    return st
end

function _persist_stream_state!(cfg::StreamConfig, st::StreamState)
    _refresh_stream_storage_stats!(cfg, st)
    st.timestamp = stream_now_utc()
    save_state(out_stream_state(cfg), st)
    return nothing
end

function _maybe_persist_stream_state!(
    cfg::StreamConfig,
    st::StreamState,
    last_flush_ref::Base.RefValue{Float64};
    force::Bool = false,
)
    now = time()
    due =
        cfg.state_flush_interval_seconds > 0 &&
        (last_flush_ref[] <= 0 || (now - last_flush_ref[]) >= cfg.state_flush_interval_seconds)
    if force || due
        _persist_stream_state!(cfg, st)
        last_flush_ref[] = now
    end
    return nothing
end

function _write_stream_gap!(
    cfg::StreamConfig,
    st::StreamState,
    start_time_utc::AbstractString,
    end_time_utc::AbstractString,
    reason::AbstractString,
)
    isempty(start_time_utc) && return nothing
    record = Dict{String,Any}(
        "task_name" => st.task_name,
        "query" => st.query,
        "rule_id" => st.rule_id,
        "rule_tag" => st.rule_tag,
        "start_time_utc" => String(start_time_utc),
        "end_time_utc" => String(end_time_utc),
        "reason" => String(reason),
        "recorded_at" => stream_now_utc(),
    )
    !isempty(st.last_error_type) && (record["error_type"] = st.last_error_type)
    !isempty(st.last_error_detail) && (record["error_detail"] = st.last_error_detail)
    st.last_status > 0 && (record["status"] = st.last_status)
    has_rate_limit_diagnostics =
        st.last_status == 429 ||
        st.last_rate_limit_limit > 0 ||
        st.last_rate_limit_reset > 0 ||
        st.last_retry_after > 0
    st.last_rate_limit_limit > 0 &&
        (record["rate_limit_limit"] = st.last_rate_limit_limit)
    has_rate_limit_diagnostics &&
        (record["rate_limit_remaining"] = st.last_rate_limit_remaining)
    st.last_rate_limit_reset > 0 &&
        (record["rate_limit_reset"] = st.last_rate_limit_reset)
    st.last_retry_after > 0 && (record["retry_after"] = st.last_retry_after)

    mkpath(cfg.out_dir)
    open(out_stream_gaps(cfg), "a") do io
        JSON3.write(io, record)
        write(io, '\n')
        flush(io)
        cfg.durable_writes && _maybe_fsync(io)
    end
    return nothing
end

function _write_stream_gap!(
    cfg::StreamConfig,
    st::StreamState,
    gap::StreamGapSnapshot,
    end_time_utc::AbstractString,
    reason::AbstractString = gap.reason,
)
    gap.start_time_utc === nothing && return nothing
    gap_reason = isempty(reason) ? "disconnected" : String(reason)
    record = Dict{String,Any}(
        "task_name" => st.task_name,
        "query" => st.query,
        "rule_id" => st.rule_id,
        "rule_tag" => st.rule_tag,
        "start_time_utc" => gap.start_time_utc::String,
        "end_time_utc" => String(end_time_utc),
        "reason" => gap_reason,
        "recorded_at" => stream_now_utc(),
    )
    !isempty(gap.error_type) && (record["error_type"] = gap.error_type)
    !isempty(gap.error_detail) && (record["error_detail"] = gap.error_detail)
    gap.status > 0 && (record["status"] = gap.status)
    has_rate_limit_diagnostics =
        gap.status == 429 ||
        gap.rate_limit_limit > 0 ||
        gap.rate_limit_reset > 0 ||
        gap.retry_after > 0
    gap.rate_limit_limit > 0 && (record["rate_limit_limit"] = gap.rate_limit_limit)
    has_rate_limit_diagnostics &&
        (record["rate_limit_remaining"] = gap.rate_limit_remaining)
    gap.rate_limit_reset > 0 && (record["rate_limit_reset"] = gap.rate_limit_reset)
    gap.retry_after > 0 && (record["retry_after"] = gap.retry_after)

    mkpath(cfg.out_dir)
    open(out_stream_gaps(cfg), "a") do io
        JSON3.write(io, record)
        write(io, '\n')
        flush(io)
        cfg.durable_writes && _maybe_fsync(io)
    end
    return nothing
end

function _stream_gap_snapshot(st::StreamState, start_time_utc, reason::AbstractString)
    return StreamGapSnapshot(
        start_time_utc = start_time_utc,
        reason = String(reason),
        status = st.last_status,
        error_type = st.last_error_type,
        error_detail = st.last_error_detail,
        rate_limit_limit = st.last_rate_limit_limit,
        rate_limit_remaining = st.last_rate_limit_remaining,
        rate_limit_reset = st.last_rate_limit_reset,
        retry_after = st.last_retry_after,
    )
end

function _begin_stream_gap!(
    st::StreamState,
    pending_gap::Base.RefValue{StreamGapSnapshot},
    reason::AbstractString,
)
    disconnected_at = stream_now_utc()
    st.last_disconnect_at = disconnected_at
    pending_gap[].start_time_utc === nothing &&
        (pending_gap[] = _stream_gap_snapshot(st, disconnected_at, reason))
    return disconnected_at
end

function _finish_stream_gap!(
    cfg::StreamConfig,
    st::StreamState,
    pending_gap::Base.RefValue{StreamGapSnapshot},
    end_time_utc::AbstractString,
    reason::AbstractString = pending_gap[].reason,
)
    pending_gap[].start_time_utc === nothing && return nothing
    gap_reason = isempty(reason) ? pending_gap[].reason : String(reason)
    gap_reason = isempty(gap_reason) ? "disconnected" : gap_reason
    _write_stream_gap!(cfg, st, pending_gap[], end_time_utc, gap_reason)
    pending_gap[] = StreamGapSnapshot()
    return nothing
end

function _stream_response_body(resp, http)
    try
        return _body_to_text(resp, read(http))
    catch
        return ""
    end
end

_stream_response_body(http) = try
    _bytes_to_safe_text(read(http))
catch
    ""
end

function _throw_stream_response_error(resp, url::AbstractString, body::AbstractString)
    body_text = _extract_x_api_error_detail(body)
    throw(XApiAccessError(resp.status, String(url), body_text))
end

function _response_header_int(resp, name::AbstractString)::Int
    try
        v = HTTP.header(resp, String(name))
        isempty(v) && return 0
        return parse(Int, v)
    catch
        return 0
    end
end

function _stream_error_metadata(body::AbstractString)
    txt = strip(String(body))
    isempty(txt) && return (type = "", title = "", detail = "")
    try
        obj = JSON3.read(txt)
        title = safe_str(_json_get(obj, "title", nothing); default = "")
        detail = safe_str(_json_get(obj, "detail", nothing); default = "")
        typ = safe_str(_json_get(obj, "type", nothing); default = "")
        if isempty(title) && isempty(detail) && isempty(typ) && _json_haskey(obj, "errors")
            for err in _json_items(_json_get(obj, "errors", nothing))
                title = safe_str(_json_get(err, "title", nothing); default = "")
                detail = safe_str(_json_get(err, "detail", nothing); default = "")
                typ = safe_str(_json_get(err, "type", nothing); default = "")
                break
            end
        end
        return (type = typ, title = title, detail = detail)
    catch
        return (type = "", title = "", detail = txt)
    end
end

function _stream_429_stop_reason(meta)::String
    text = _ascii_lowercase_text(join((meta.type, meta.title, meta.detail), " "))
    if occursin("usage-capped", text) ||
       occursin("usage capped", text) ||
       occursin("usage cap", text)
        return "usage_capped"
    end
    return "rate_limited"
end

function _stream_response_outcome(resp, url::AbstractString; body::AbstractString = "")
    resp.status == 200 &&
        return StreamOutcome(
            kind = :ok,
            stop_reason = "",
            sleep_seconds = 0,
            activity = false,
            status = 200,
            error = "",
        )
    meta = _stream_error_metadata(body)
    rate_limit_limit = _response_header_int(resp, "x-rate-limit-limit")
    rate_limit_remaining = _response_header_int(resp, "x-rate-limit-remaining")
    rate_limit_reset = _response_header_int(resp, "x-rate-limit-reset")
    retry_after = _response_header_int(resp, "retry-after")
    if resp.status == 429
        stop_reason = _stream_429_stop_reason(meta)
        return StreamOutcome(
            kind = :retryable,
            stop_reason = stop_reason,
            sleep_seconds = rate_limit_sleep_seconds(resp; min_backoff_seconds = 60),
            activity = false,
            status = resp.status,
            error = stop_reason == "usage_capped" ? "usage cap exceeded" : "rate limited",
            error_type = meta.type,
            error_detail = meta.detail,
            rate_limit_limit = rate_limit_limit,
            rate_limit_remaining = rate_limit_remaining,
            rate_limit_reset = rate_limit_reset,
            retry_after = retry_after,
        )
    elseif 500 <= resp.status < 600
        return StreamOutcome(
            kind = :retryable,
            stop_reason = "server_error",
            sleep_seconds = 0,
            activity = false,
            status = resp.status,
            error = "server error",
            error_type = meta.type,
            error_detail = meta.detail,
            rate_limit_limit = rate_limit_limit,
            rate_limit_remaining = rate_limit_remaining,
            rate_limit_reset = rate_limit_reset,
            retry_after = retry_after,
        )
    end
    _throw_stream_response_error(resp, url, body)
end

function _record_stream_outcome_diagnostics!(st::StreamState, outcome::StreamOutcome)
    outcome.status > 0 && (st.last_status = outcome.status)
    st.last_error = outcome.error
    st.last_error_type = outcome.error_type
    st.last_error_detail = outcome.error_detail
    st.last_rate_limit_limit = outcome.rate_limit_limit
    st.last_rate_limit_remaining = outcome.rate_limit_remaining
    st.last_rate_limit_reset = outcome.rate_limit_reset
    st.last_retry_after = outcome.retry_after
    return st
end

_stream_readtimeout_seconds(cfg::StreamConfig)::Int =
    cfg.http_readtimeout_seconds > 0 ? cfg.http_readtimeout_seconds :
    cfg.idle_timeout_seconds

function _record_stream_line!(
    cfg::StreamConfig,
    st::StreamState,
    sdb::SeenDB,
    sink,
    line::AbstractString,
    started_at::Float64,
    activity_ref::Base.RefValue{Bool},
    state_flush_ref::Base.RefValue{Float64} = Ref(time()),
)
    result = handle_stream_line!(cfg, st, sdb, sink, line)
    if result != :invalid
        activity_ref[] = true
        st.consecutive_failures = 0
        st.stop_reason = ""
    end
    force_state = result in (:tweet, :errors)
    _maybe_persist_stream_state!(cfg, st, state_flush_ref; force = force_state)

    stop_reason = _stream_stop_reason(cfg, st, started_at)
    if stop_reason !== nothing
        st.completed = true
        st.stop_reason = stop_reason
        _persist_stream_state!(cfg, st)
        state_flush_ref[] = time()
    end
    return (result = result, stop_reason = stop_reason)
end

function _process_stream_chunk!(
    cfg::StreamConfig,
    st::StreamState,
    sdb::SeenDB,
    sink,
    pending_line::IOBuffer,
    chunk,
    started_at::Float64,
    activity_ref::Base.RefValue{Bool},
    state_flush_ref::Base.RefValue{Float64} = Ref(time()),
)
    for b in chunk
        if b == UInt8('\n')
            line = String(take!(pending_line))
            res = _record_stream_line!(
                cfg,
                st,
                sdb,
                sink,
                line,
                started_at,
                activity_ref,
                state_flush_ref,
            )
            res.stop_reason !== nothing && return res
        else
            write(pending_line, b)
        end
    end
    return (result = :pending, stop_reason = nothing)
end

function _flush_pending_stream_line!(
    cfg::StreamConfig,
    st::StreamState,
    sdb::SeenDB,
    sink,
    pending_line::IOBuffer,
    started_at::Float64,
    activity_ref::Base.RefValue{Bool},
    state_flush_ref::Base.RefValue{Float64} = Ref(time()),
)
    position(pending_line) == 0 && return (result = :empty, stop_reason = nothing)
    line = String(take!(pending_line))
    return _record_stream_line!(
        cfg,
        st,
        sdb,
        sink,
        line,
        started_at,
        activity_ref,
        state_flush_ref,
    )
end

_stream_completed_outcome(stop_reason::AbstractString, activity_ref::Base.RefValue{Bool}) =
    StreamOutcome(
        kind = :completed,
        stop_reason = stop_reason,
        sleep_seconds = 0,
        activity = activity_ref[],
        status = 0,
        error = "",
    )

_stream_disconnected_outcome(activity_ref::Base.RefValue{Bool}) =
    StreamOutcome(
        kind = :disconnected,
        stop_reason = "disconnected",
        sleep_seconds = 0,
        activity = activity_ref[],
        status = 0,
        error = "",
    )

function _stream_body_reader(resp, http)
    encoding = _ascii_lowercase_text(_response_content_encoding(resp))
    return occursin("gzip", encoding) ? HTTP.GzipDecompressorStream(http) : http
end

function _read_stream_response_lines!(
    cfg::StreamConfig,
    st::StreamState,
    sdb::SeenDB,
    sink,
    resp,
    http,
    started_at::Float64,
    activity_ref::Base.RefValue{Bool},
    state_flush_ref::Base.RefValue{Float64} = Ref(time()),
    ;
    empty_read_sleep_seconds::Real = STREAM_EMPTY_READ_SLEEP_SECONDS,
    sleep_fn::Function = sleep,
)
    stream = _stream_body_reader(resp, http)
    try
        return _read_stream_lines!(
            cfg,
            st,
            sdb,
            sink,
            stream,
            started_at,
            activity_ref,
            state_flush_ref,
            empty_read_sleep_seconds = empty_read_sleep_seconds,
            sleep_fn = sleep_fn,
        )
    finally
        if stream !== http
            try
                close(stream)
            catch
            end
        end
    end
end

function _stream_reader_eof_or_unknown(stream)::Bool
    try
        return eof(stream)
    catch
        return true
    end
end

function _stream_empty_read_outcome!(
    cfg::StreamConfig,
    st::StreamState,
    started_at::Float64,
    activity_ref::Base.RefValue{Bool},
    state_flush_ref::Base.RefValue{Float64},
)
    stop_reason = _stream_stop_reason(cfg, st, started_at)
    if stop_reason !== nothing
        st.completed = true
        st.stop_reason = stop_reason
        _persist_stream_state!(cfg, st)
        return _stream_completed_outcome(stop_reason, activity_ref)
    end
    _maybe_persist_stream_state!(cfg, st, state_flush_ref)
    return nothing
end

function _flush_pending_stream_outcome!(
    cfg::StreamConfig,
    st::StreamState,
    sdb::SeenDB,
    sink,
    pending_line::IOBuffer,
    started_at::Float64,
    activity_ref::Base.RefValue{Bool},
    state_flush_ref::Base.RefValue{Float64} = Ref(time()),
)
    res = _flush_pending_stream_line!(
        cfg,
        st,
        sdb,
        sink,
        pending_line,
        started_at,
        activity_ref,
        state_flush_ref,
    )
    res.stop_reason !== nothing &&
        return _stream_completed_outcome(res.stop_reason, activity_ref)
    return nothing
end

function _read_stream_lines!(
    cfg::StreamConfig,
    st::StreamState,
    sdb::SeenDB,
    sink,
    http,
    started_at::Float64,
    activity_ref::Base.RefValue{Bool},
    state_flush_ref::Base.RefValue{Float64} = Ref(time()),
    ;
    empty_read_sleep_seconds::Real = STREAM_EMPTY_READ_SLEEP_SECONDS,
    sleep_fn::Function = sleep,
)
    pending_line = IOBuffer()
    try
        while true
            chunk = readavailable(http)
            if isempty(chunk)
                if !_stream_reader_eof_or_unknown(http)
                    outcome = _stream_empty_read_outcome!(
                        cfg,
                        st,
                        started_at,
                        activity_ref,
                        state_flush_ref,
                    )
                    outcome !== nothing && return outcome
                    sleep_fn(empty_read_sleep_seconds)
                    continue
                end
                outcome = _flush_pending_stream_outcome!(
                    cfg,
                    st,
                    sdb,
                    sink,
                    pending_line,
                    started_at,
                    activity_ref,
                    state_flush_ref,
                )
                outcome !== nothing && return outcome
                return _stream_disconnected_outcome(activity_ref)
            end
            res = _process_stream_chunk!(
                cfg,
                st,
                sdb,
                sink,
                pending_line,
                chunk,
                started_at,
                activity_ref,
                state_flush_ref,
            )
            if res.stop_reason !== nothing
                return _stream_completed_outcome(res.stop_reason, activity_ref)
            end
        end
    catch e
        outcome = _flush_pending_stream_outcome!(
            cfg,
            st,
            sdb,
            sink,
            pending_line,
            started_at,
            activity_ref,
            state_flush_ref,
        )
        outcome !== nothing && return outcome
        rethrow(e)
    end
end

function _stream_reconnect_decision(cfg::StreamConfig, reconnects::Int)
    if !cfg.reconnect
        return (should_reconnect = false, reconnects = reconnects, stop_reason = "")
    end
    next_reconnects = reconnects + 1
    if cfg.max_reconnects > 0 && next_reconnects > cfg.max_reconnects
        return (
            should_reconnect = false,
            reconnects = next_reconnects,
            stop_reason = "max_reconnects_exceeded",
        )
    end
    return (should_reconnect = true, reconnects = next_reconnects, stop_reason = "")
end

function _looks_like_stream_timeout(e)::Bool
    msg = _ascii_lowercase_text(_safe_showerror_text(e))
    return occursin("timeout", msg) || occursin("timed out", msg)
end
