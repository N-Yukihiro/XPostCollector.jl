# =========================================================
# State 型（StructTypes）
# =========================================================
mutable struct CollectorState
    timestamp::String
    task_name::String
    query::String
    start_time_utc::Union{Nothing,String}
    end_time_utc::Union{Nothing,String}
    next_token::Union{Nothing,String}
    page_count::Int
    total_tweets::Int
    total_includes::Int
    completed::Bool
    stop_reason::String
    converted_jsonl_offset::Int
    converted_at::String
end

CollectorState() =
    CollectorState("", "", "", nothing, nothing, nothing, 0, 0, 0, false, "", 0, "")
StructTypes.StructType(::Type{CollectorState}) = StructTypes.Mutable()

Base.@kwdef mutable struct StreamDiagnostics
    error::String = ""
    status::Int = 0
    error_type::String = ""
    error_detail::String = ""
    rate_limit_limit::Int = 0
    rate_limit_remaining::Int = 0
    rate_limit_reset::Int = 0
    retry_after::Int = 0
end
StructTypes.StructType(::Type{StreamDiagnostics}) = StructTypes.Mutable()

mutable struct StreamState
    timestamp::String
    task_name::String
    query::String
    rule_id::Union{Nothing,String}
    rule_tag::String
    connection_count::Int
    total_tweets::Int
    total_includes::Int
    keepalive_count::Int
    last_heartbeat_at::String
    last_event_at::String
    diagnostics::StreamDiagnostics
    consecutive_failures::Int
    last_connected_at::String
    last_disconnect_at::String
    seen_count::Int
    db_path::String
    db_size_bytes::Int
    completed::Bool
    stop_reason::String
end

StreamState() =
    StreamState(
        "",
        "",
        "",
        nothing,
        "",
        0,
        0,
        0,
        0,
        "",
        "",
        StreamDiagnostics(),
        0,
        "",
        "",
        0,
        "",
        0,
        false,
        "",
    )
StructTypes.StructType(::Type{StreamState}) = StructTypes.Mutable()

function atomic_write(path::AbstractString, bytes::Vector{UInt8})
    tmp = path * ".tmp"
    open(tmp, "w") do io
        write(io, bytes)
        flush(io)
    end
    mv(tmp, path; force = true)
end

function save_state(path::AbstractString, st::CollectorState)
    atomic_write(path, Vector{UInt8}(JSON3.write(st)))
end

function save_state(path::AbstractString, st::StreamState)
    atomic_write(path, Vector{UInt8}(JSON3.write(st)))
end

function load_state(path::AbstractString)::Union{Nothing,CollectorState}
    !isfile(path) && return nothing
    try
        st = CollectorState()
        JSON3.read!(read(path, String), st)
        return st
    catch e
        @warn "State file unreadable; start fresh" path = path exception = e
        return nothing
    end
end

function load_stream_state(path::AbstractString)::Union{Nothing,StreamState}
    !isfile(path) && return nothing
    try
        return _stream_state_from_json(JSON3.read(read(path, String)))
    catch e
        @warn "Stream state file unreadable; start fresh" path = path exception = e
        return nothing
    end
end

function _stream_state_from_json(obj)::StreamState
    st = StreamState()
    st.timestamp = safe_str(_json_get(obj, "timestamp", st.timestamp); default = st.timestamp)
    st.task_name = safe_str(_json_get(obj, "task_name", st.task_name); default = st.task_name)
    st.query = safe_str(_json_get(obj, "query", st.query); default = st.query)
    rule_id = _json_get(obj, "rule_id", st.rule_id)
    st.rule_id = isnull(rule_id) ? nothing : safe_str(rule_id; default = "")
    st.rule_tag = safe_str(_json_get(obj, "rule_tag", st.rule_tag); default = st.rule_tag)
    st.connection_count =
        safe_int(_json_get(obj, "connection_count", st.connection_count); default = st.connection_count)
    st.total_tweets =
        safe_int(_json_get(obj, "total_tweets", st.total_tweets); default = st.total_tweets)
    st.total_includes =
        safe_int(_json_get(obj, "total_includes", st.total_includes); default = st.total_includes)
    st.keepalive_count =
        safe_int(_json_get(obj, "keepalive_count", st.keepalive_count); default = st.keepalive_count)
    st.last_heartbeat_at = safe_str(
        _json_get(obj, "last_heartbeat_at", st.last_heartbeat_at);
        default = st.last_heartbeat_at,
    )
    st.last_event_at =
        safe_str(_json_get(obj, "last_event_at", st.last_event_at); default = st.last_event_at)
    st.consecutive_failures = safe_int(
        _json_get(obj, "consecutive_failures", st.consecutive_failures);
        default = st.consecutive_failures,
    )
    st.last_connected_at = safe_str(
        _json_get(obj, "last_connected_at", st.last_connected_at);
        default = st.last_connected_at,
    )
    st.last_disconnect_at = safe_str(
        _json_get(obj, "last_disconnect_at", st.last_disconnect_at);
        default = st.last_disconnect_at,
    )
    st.seen_count =
        safe_int(_json_get(obj, "seen_count", st.seen_count); default = st.seen_count)
    st.db_path = safe_str(_json_get(obj, "db_path", st.db_path); default = st.db_path)
    st.db_size_bytes =
        safe_int(_json_get(obj, "db_size_bytes", st.db_size_bytes); default = st.db_size_bytes)
    st.completed =
        safe_bool(_json_get(obj, "completed", st.completed); default = st.completed)
    st.stop_reason =
        safe_str(_json_get(obj, "stop_reason", st.stop_reason); default = st.stop_reason)
    _migrate_stream_state_diagnostics!(st, obj)
    return st
end

function _migrate_stream_state_diagnostics!(st::StreamState, obj)
    diag = _json_get(obj, "diagnostics", nothing)
    if diag !== nothing
        st.diagnostics.error =
            safe_str(_json_get(diag, "error", st.diagnostics.error); default = "")
        st.diagnostics.status =
            safe_int(_json_get(diag, "status", st.diagnostics.status); default = 0)
        st.diagnostics.error_type =
            safe_str(_json_get(diag, "error_type", st.diagnostics.error_type); default = "")
        st.diagnostics.error_detail =
            safe_str(_json_get(diag, "error_detail", st.diagnostics.error_detail); default = "")
        st.diagnostics.rate_limit_limit = safe_int(
            _json_get(diag, "rate_limit_limit", st.diagnostics.rate_limit_limit);
            default = 0,
        )
        st.diagnostics.rate_limit_remaining = safe_int(
            _json_get(
                diag,
                "rate_limit_remaining",
                st.diagnostics.rate_limit_remaining,
            );
            default = 0,
        )
        st.diagnostics.rate_limit_reset = safe_int(
            _json_get(diag, "rate_limit_reset", st.diagnostics.rate_limit_reset);
            default = 0,
        )
        st.diagnostics.retry_after =
            safe_int(_json_get(diag, "retry_after", st.diagnostics.retry_after); default = 0)
        return st
    end

    st.diagnostics.error =
        safe_str(_json_get(obj, "last_error", st.diagnostics.error); default = "")
    st.diagnostics.status =
        safe_int(_json_get(obj, "last_status", st.diagnostics.status); default = 0)
    st.diagnostics.error_type = safe_str(
        _json_get(obj, "last_error_type", st.diagnostics.error_type);
        default = "",
    )
    st.diagnostics.error_detail = safe_str(
        _json_get(obj, "last_error_detail", st.diagnostics.error_detail);
        default = "",
    )
    st.diagnostics.rate_limit_limit = safe_int(
        _json_get(obj, "last_rate_limit_limit", st.diagnostics.rate_limit_limit);
        default = 0,
    )
    st.diagnostics.rate_limit_remaining = safe_int(
        _json_get(
            obj,
            "last_rate_limit_remaining",
            st.diagnostics.rate_limit_remaining,
        );
        default = 0,
    )
    st.diagnostics.rate_limit_reset = safe_int(
        _json_get(obj, "last_rate_limit_reset", st.diagnostics.rate_limit_reset);
        default = 0,
    )
    st.diagnostics.retry_after =
        safe_int(_json_get(obj, "last_retry_after", st.diagnostics.retry_after); default = 0)
    return st
end
