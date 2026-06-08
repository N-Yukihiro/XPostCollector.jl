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
    converted_jsonl_offset::Int
    converted_at::String
end

CollectorState() =
    CollectorState("", "", "", nothing, nothing, nothing, 0, 0, 0, false, 0, "")
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
        txt = read(path, String)
        st = StreamState()
        JSON3.read!(txt, st)
        _migrate_stream_state_diagnostics!(st, JSON3.read(txt))
        return st
    catch e
        @warn "Stream state file unreadable; start fresh" path = path exception = e
        return nothing
    end
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
