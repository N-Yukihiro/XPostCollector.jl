# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
mutable struct CollectLogger <: AbstractLogger
    messages::Vector{String}
end

Logging.min_enabled_level(::CollectLogger) = Logging.Debug
Logging.shouldlog(::CollectLogger, level, _module, group, id) = true
Logging.catch_exceptions(::CollectLogger) = false

function Logging.handle_message(
    logger::CollectLogger,
    level,
    message,
    _module,
    group,
    id,
    file,
    line;
    kwargs...,
)
    push!(logger.messages, string(message))
    return nothing
end

function with_temp_cfg(f::Function; task = "ut_task")
    mktempdir() do dir
        cfg = SearchConfig(
            task_name = task,
            keywords_or = ["foo", "bar baz"],
            out_dir = dir,
            log_dir = joinpath(dir, "logs"),
            db_path = joinpath(dir, "seen.sqlite"),
            emit_csv = true,
            emit_arrow = true,
            arrow_path = joinpath(dir, "$task.arrow"),
            arrow_append = true,
            convert_incremental = true,
            convert_batch_size = 2,
        )
        validate!(cfg)
        try
            f(cfg, dir)
        finally
            GC.gc()
            sleep(0.1)
        end
    end
end

function with_temp_stream_cfg(f::Function; task = "stream_ut")
    mktempdir() do dir
        cfg = StreamConfig(
            task_name = task,
            keywords_or = ["foo", "bar baz"],
            api_base_url = "http://127.0.0.1:8080/",
            out_dir = dir,
            log_dir = joinpath(dir, "logs"),
            db_path = joinpath(dir, "seen.sqlite"),
            emit_csv = true,
            emit_arrow = false,
            convert_incremental = true,
            convert_batch_size = 2,
            max_posts = 2,
            max_seconds = 10.0,
        )
        validate!(cfg)
        try
            f(cfg, dir)
        finally
            GC.gc()
            sleep(0.1)
        end
    end
end

function utc_dt_to_local_str(dt_utc::DateTime, tzname::String)
    zdt = ZonedDateTime(dt_utc, tz"UTC") |> x -> astimezone(x, TimeZone(tzname))
    return Dates.format(DateTime(zdt), DT_FMT_ISO)
end

function count_csv_rows(path::String)
    isfile(path) || return 0
    data = read(path)
    tbl = CSV.File(IOBuffer(data))
    return sum(1 for _ in tbl)
end

function count_arrow_rows(path::String)
    isfile(path) || return 0
    data = read(path)
    tbl = Arrow.Table(IOBuffer(data))
    try
        return Tables.rowcount(tbl)
    catch
        return sum(1 for _ in Tables.rows(tbl))
    end
end

function write_jsonl_lines(path::String, lines::Vector{String})
    open(path, "a") do io
        for ln in lines
            write(io, ln)
            write(io, '\n')
        end
    end
end

function gzip_bytes(s::AbstractString)
    return read(HTTP.GzipCompressorStream(IOBuffer(String(s))))
end

function jsonl_tweet_line(
    id::String;
    created_at = "2026-01-01T00:00:00Z",
    author_id = "10",
    lang = "en",
    possibly_sensitive = nothing,
    like = 1,
    rt = 2,
    reply = 3,
    qte = 4,
    bookmark = 5,
    impression = 6,
    text = "hi",
)
    tw = Dict{String,Any}(
        "id" => id,
        "created_at" => created_at,
        "author_id" => author_id,
        "lang" => lang,
        "text" => text,
        "public_metrics" => Dict(
            "like_count" => like,
            "retweet_count" => rt,
            "reply_count" => reply,
            "quote_count" => qte,
            "bookmark_count" => bookmark,
            "impression_count" => impression,
        ),
    )
    if possibly_sensitive !== nothing
        tw["possibly_sensitive"] = possibly_sensitive
    end
    obj = (; kind = "tweet", data = tw)
    return String(JSON3.write(obj))
end

function jsonl_include_line(kind_suffix = "users"; data = Dict("dummy" => "x"))
    obj = (; kind = "include:$kind_suffix", data = data)
    return String(JSON3.write(obj))
end

function jsonl_page_line(page::Int = 1; meta = Dict("result_count" => 0))
    obj = (; kind = "page", data = Dict("page" => page, "meta" => meta))
    return String(JSON3.write(obj))
end

function stream_event_json(
    id::String;
    text = "hello stream",
    author_id = "42",
    username = "alice",
)
    event = Dict{String,Any}(
        "data" => Dict(
            "id" => id,
            "text" => text,
            "created_at" => "2026-01-01T00:00:00Z",
            "author_id" => author_id,
            "public_metrics" => Dict(
                "like_count" => 1,
                "retweet_count" => 0,
                "reply_count" => 0,
                "quote_count" => 0,
            ),
        ),
        "includes" => Dict(
            "users" => [
                Dict(
                    "id" => author_id,
                    "username" => username,
                    "name" => "Alice",
                ),
            ],
        ),
        "matching_rules" => [Dict("id" => "r1", "tag" => "tag1")],
    )
    return String(JSON3.write(event))
end

fixture_path(name::AbstractString) = joinpath(dirname(@__DIR__), "fixtures", name)

function read_fixture(name::AbstractString)
    path = fixture_path(name)
    isfile(path) || error("Fixture not found: $path")
    return read(path, String)
end

function with_env_token(f::Function)
    Base.withenv("BEARER_TOKEN" => "dummy") do
        f()
    end
end

mutable struct FakeReadavailableStream
    chunks::Vector{Vector{UInt8}}
    calls::Int
end

FakeReadavailableStream(chunks::Vector{Vector{UInt8}}) =
    FakeReadavailableStream(chunks, 0)

function Base.readavailable(s::FakeReadavailableStream)
    s.calls += 1
    isempty(s.chunks) && return UInt8[]
    return popfirst!(s.chunks)
end

mutable struct FakeReadavailableEofStream
    chunks::Vector{Vector{UInt8}}
    calls::Int
end

FakeReadavailableEofStream(chunks::Vector{Vector{UInt8}}) =
    FakeReadavailableEofStream(chunks, 0)

function Base.readavailable(s::FakeReadavailableEofStream)
    s.calls += 1
    isempty(s.chunks) && return UInt8[]
    return popfirst!(s.chunks)
end

Base.eof(s::FakeReadavailableEofStream) = isempty(s.chunks)

struct FailingIO <: IO end
Base.write(::FailingIO, ::UInt8) = error("write failed")
Base.write(::FailingIO, ::StridedVector{UInt8}) = error("write failed")
Base.write(::FailingIO, ::AbstractVector{UInt8}) = error("write failed")
Base.write(::FailingIO, ::AbstractString) = error("write failed")
Base.write(::FailingIO, ::Char) = error("write failed")
Base.flush(::FailingIO) = nothing

struct BinaryShowError <: Exception end
Base.showerror(io::IO, ::BinaryShowError) = write(io, UInt8[0x8b, 0xff, 0x20])
