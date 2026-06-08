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
    last_error::String
    last_status::Int
    last_error_type::String
    last_error_detail::String
    last_rate_limit_limit::Int
    last_rate_limit_remaining::Int
    last_rate_limit_reset::Int
    last_retry_after::Int
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
        "",
        0,
        "",
        "",
        0,
        0,
        0,
        0,
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
        st = StreamState()
        JSON3.read!(read(path, String), st)
        return st
    catch e
        @warn "Stream state file unreadable; start fresh" path = path exception = e
        return nothing
    end
end
