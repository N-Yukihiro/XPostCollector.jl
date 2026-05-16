using XPostCollector
using XPostCollector:
    quote_term,
    build_query,
    isnull,
    safe_int,
    safe_bool,
    local_str_to_utc_z,
    ensure_offset_boundary!,
    out_jsonl,
    out_state,
    out_csv,
    out_arrow,
    out_csv_wide,
    out_stream_state,
    out_stream_gaps,
    save_state,
    load_state,
    init_seen_db,
    mark_seen!,
    utc_now_dt,
    utc_now_z,
    resolve_time_window,
    dt_to_utc_z,
    parse_utc_z,
    tweet_to_row,
    parse_usage_summary,
    rate_limit_sleep_seconds,
    build_stream_params,
    handle_stream_line!,
    _process_stream_chunk!,
    _flush_pending_stream_line!,
    _read_stream_lines!,
    _stream_response_outcome,
    _stream_reconnect_decision,
    _open_stream_jsonl_writer,
    _close_stream_jsonl_writer!,
    _rotate_stream_jsonl_if_needed!,
    _write_stream_gap!,
    stream_jsonl_paths,
    DT_FMT_ISO,
    MAX_RESULTS_ALL,
    HTTP,
    JSON3
using Test
using Dates
using TimeZones
using CSV
using Arrow
using Tables
using Logging
using Random
using DBInterface

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
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

fixture_path(name::AbstractString) = joinpath(@__DIR__, "fixtures", name)

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

struct FailingIO <: IO end
Base.write(::FailingIO, ::UInt8) = error("write failed")
Base.write(::FailingIO, ::StridedVector{UInt8}) = error("write failed")
Base.write(::FailingIO, ::AbstractVector{UInt8}) = error("write failed")
Base.write(::FailingIO, ::AbstractString) = error("write failed")
Base.write(::FailingIO, ::Char) = error("write failed")
Base.flush(::FailingIO) = nothing

# ---------------------------------------------------------
# validate!
# ---------------------------------------------------------
@testset "validate! checks" begin
    cfg = SearchConfig(task_name = "", keywords_or = ["x"])
    @test_throws ErrorException validate!(cfg)

    cfg = SearchConfig(task_name = "t", keywords_or = String[])
    @test_throws ErrorException validate!(cfg)

    cfg = SearchConfig(task_name = "t", keywords_or = ["x"], window_hours = -1)
    @test_throws ErrorException validate!(cfg)

    cfg = SearchConfig(
        task_name = "t",
        keywords_or = ["x"],
        local_tz_name = "Invalid/Timezone",
    )
    @test_throws ArgumentError validate!(cfg)

    cfg = SearchConfig(task_name = "t", keywords_or = ["x"], search_mode = :bogus)
    @test_throws ErrorException validate!(cfg)

    cfg = SearchConfig(task_name = "t", keywords_or = ["x"], max_results = 999)
    validate!(cfg)
    @test cfg.max_results == MAX_RESULTS_ALL
end

@testset "usage config clamps days" begin
    cfg = SearchConfig(task_name = "t", keywords_or = ["x"], usage_days = 999)
    validate!(cfg)
    @test cfg.usage_days == 90
end

@testset "StreamConfig validation and params" begin
    cfg = StreamConfig(
        task_name = "stream",
        keywords_or = ["foo"],
        api_base_url = "http://127.0.0.1:8080/",
        rule_tag = "",
        max_posts = -1,
        max_seconds = -1,
        idle_timeout_seconds = 0,
        rotate_jsonl_bytes = -1,
        rotate_jsonl_seconds = -1,
        state_flush_interval_seconds = -1,
    )
    validate!(cfg)
    @test cfg.api_base_url == "http://127.0.0.1:8080"
    @test cfg.rule_tag == "stream"
    @test cfg.max_posts == 0
    @test cfg.max_seconds == 0.0
    @test cfg.idle_timeout_seconds == 1
    @test cfg.max_reconnects == 0
    @test cfg.rotate_jsonl_bytes == 0
    @test cfg.rotate_jsonl_seconds == 0.0
    @test cfg.state_flush_interval_seconds == 0.0

    params = build_stream_params(cfg)
    @test haskey(params, "tweet.fields")
    @test haskey(params, "expansions")
    @test occursin("created_at", params["tweet.fields"])
    @test endswith(out_stream_state(cfg), "stream.stream.state.json")

    scfg = SearchConfig(
        task_name = "search",
        keywords_or = ["x"],
        api_base_url = "http://127.0.0.1:8080/",
    )
    validate!(scfg)
    @test scfg.api_base_url == "http://127.0.0.1:8080"
end

# ---------------------------------------------------------
# Query / utils
# ---------------------------------------------------------
@testset "Query builder" begin
    @test quote_term("foo") == "foo"
    @test quote_term("bar baz") == "\"bar baz\""
    @test quote_term("\"already quoted\"") == "\"already quoted\""
    @test occursin("\\\"", quote_term("a \"b\" c"))

    cfg = SearchConfig(task_name = "t", keywords_or = ["foo", "bar baz"])
    q = build_query(cfg)
    @test occursin("(foo OR \"bar baz\")", q)
    @test occursin("-is:retweet", q)

    cfg.exclude_reposts = false
    q2 = build_query(cfg)
    @test !occursin("-is:retweet", q2)

    cfg.extra_query_tail = "lang:ja -is:reply"
    q3 = build_query(cfg)
    @test occursin("lang:ja", q3)
end

@testset "Null compatibility + safe_int / safe_bool" begin
    @test isnull(nothing) == true
    @test isnull(missing) == true
    if isdefined(JSON3, :null)
        @test isnull(JSON3.null) == true
    end

    @test safe_int(nothing) === missing
    @test safe_int(3) == 3
    @test safe_int(3.0) == 3
    @test safe_int(3.14) === missing
    @test safe_int("123") == 123
    @test safe_int("x") === missing

    @test safe_bool(nothing) === missing
    @test safe_bool(true) == true
    @test safe_bool(1) == true
    @test safe_bool(0) == false
    @test safe_bool("TRUE") == true
    @test safe_bool("no") == false
    @test safe_bool("maybe") === missing
end

@testset "TimeZones conversion local_str_to_utc_z" begin
    cfg = SearchConfig(task_name = "t", keywords_or = ["x"], local_tz_name = "Asia/Tokyo")
    utc_z = local_str_to_utc_z(cfg, "2026-01-01T00:00:00")
    @test utc_z == "2025-12-31T15:00:00Z"
end

# ---------------------------------------------------------
# ensure_offset_boundary!
# ---------------------------------------------------------
@testset "ensure_offset_boundary!" begin
    mktempdir() do dir
        p = joinpath(dir, "t.txt")
        open(p, "w") do io
            write(io, "aaa\nbbb\nccc\n")
        end

        open(p, "r") do io
            ensure_offset_boundary!(io, 2)
            @test readline(io) == "bbb"
        end

        open(p, "r") do io
            ensure_offset_boundary!(io, 4)
            @test readline(io) == "bbb"
        end
    end
end

# ---------------------------------------------------------
# convert_outputs
# ---------------------------------------------------------
@testset "convert_outputs incremental" begin
    with_temp_cfg(task = "conv") do cfg, dir
        jsonl = out_jsonl(cfg)
        state = out_state(cfg)
        csvp = out_csv(cfg)
        arrp = out_arrow(cfg)

        st = CollectorState()
        save_state(state, st)

        lines1 = [
            jsonl_tweet_line("1"),
            jsonl_include_line("users"),
            jsonl_page_line(1),
            jsonl_tweet_line("2"),
            jsonl_tweet_line("3"),
            jsonl_page_line(2),
        ]
        write_jsonl_lines(jsonl, lines1)

        convert_outputs(cfg)
        @test count_csv_rows(csvp) == 3
        @test count_arrow_rows(arrp) == 3

        st1 = load_state(state)
        @test st1 !== nothing
        @test st1.converted_jsonl_offset > 0

        lines2 = [
            jsonl_tweet_line("4"),
            jsonl_page_line(3),
            jsonl_tweet_line("5"),
            jsonl_page_line(4),
        ]
        write_jsonl_lines(jsonl, lines2)

        convert_outputs(cfg)
        @test count_csv_rows(csvp) == 5
        @test count_arrow_rows(arrp) == 5

        st2 = load_state(state)
        @test st2.converted_jsonl_offset > st1.converted_jsonl_offset
    end
end

@testset "convert_outputs resets offset if outputs missing" begin
    with_temp_cfg(task = "conv_reset") do cfg, dir
        with_logger(NullLogger()) do
            jsonl = out_jsonl(cfg)
            state = out_state(cfg)
            csvp = out_csv(cfg)
            arrp = out_arrow(cfg)

            write_jsonl_lines(
                jsonl,
                [jsonl_tweet_line("1"), jsonl_tweet_line("2"), jsonl_page_line(1)],
            )

            st = CollectorState()
            st.converted_jsonl_offset = 10
            save_state(state, st)

            @test !isfile(csvp)
            @test !isfile(arrp)

            convert_outputs(cfg)
            @test isfile(csvp)
            @test isfile(arrp)
            @test count_csv_rows(csvp) == 2
            @test count_arrow_rows(arrp) == 2
        end
    end
end

@testset "convert_outputs handles corrupt lines and non-tweet kinds" begin
    with_temp_cfg(task = "conv_robust") do cfg, dir
        with_logger(NullLogger()) do
            jsonl = out_jsonl(cfg)
            csvp = out_csv(cfg)

            lines = [
                jsonl_tweet_line("1"),
                "THIS IS NOT JSON",
                jsonl_include_line("users"),
                jsonl_page_line(1),
                jsonl_tweet_line("2"),
            ]
            write_jsonl_lines(jsonl, lines)

            convert_outputs(cfg)
            @test count_csv_rows(csvp) == 2
        end
    end
end

@testset "convert_outputs arrow overwrite mode" begin
    with_temp_cfg(task = "arrow_overwrite") do cfg, dir
        with_logger(NullLogger()) do
            jsonl = out_jsonl(cfg)
            arrp = out_arrow(cfg)

            write_jsonl_lines(
                jsonl,
                [jsonl_tweet_line("old1"), jsonl_tweet_line("old2"), jsonl_page_line(1)],
            )
            convert_outputs(cfg)
            @test count_arrow_rows(arrp) == 2

            write_jsonl_lines(jsonl, [jsonl_tweet_line("new1"), jsonl_page_line(2)])

            cfg.arrow_append = false
            convert_outputs(cfg)

            @test count_arrow_rows(arrp) == 1
        end
    end
end

# ---------------------------------------------------------
# SQLite dedup
# ---------------------------------------------------------
@testset "SeenDB dedup (unit)" begin
    with_temp_cfg(task = "dedup") do cfg, dir
        sdb = init_seen_db(cfg)
        try
            seen_at = utc_now_z(lag_seconds = 0)
            @test mark_seen!(sdb, "1", seen_at) == true
            @test mark_seen!(sdb, "1", seen_at) == false
            @test mark_seen!(sdb, "2", seen_at) == true
        finally
            try
                DBInterface.close!(sdb.db)
            catch
            end
        end
    end
end

# ---------------------------------------------------------
# State I/O
# ---------------------------------------------------------
@testset "State save/load (unit)" begin
    mktempdir() do dir
        p = joinpath(dir, "state.json")
        st = CollectorState()
        st.timestamp = "t"
        st.task_name = "task"
        st.query = "q"
        st.start_time_utc = "2026-01-01T00:00:00Z"
        st.end_time_utc = "2026-01-01T01:00:00Z"
        st.next_token = "tok"
        st.page_count = 7
        st.total_tweets = 123
        st.total_includes = 9
        st.completed = false
        st.converted_jsonl_offset = 456
        st.converted_at = "c"

        save_state(p, st)
        st2 = load_state(p)
        @test st2 !== nothing
        @test st2.task_name == "task"
        @test st2.page_count == 7
        @test st2.total_tweets == 123
        @test st2.converted_jsonl_offset == 456
        @test st2.start_time_utc == "2026-01-01T00:00:00Z"
    end
end

# ---------------------------------------------------------
# Rate limit helper
# ---------------------------------------------------------
@testset "rate_limit_sleep_seconds" begin
    resp1 = HTTP.Response(429, ["retry-after" => "10"])
    @test rate_limit_sleep_seconds(resp1; min_backoff_seconds = 15) >= 15

    future = Int(floor(time())) + 100
    resp2 = HTTP.Response(429, ["x-rate-limit-reset" => string(future)])
    @test rate_limit_sleep_seconds(resp2; min_backoff_seconds = 15) >= 15
end

# ---------------------------------------------------------
# Time window logic
#   resolve_time_window は endpoint デフォルトあり。ここでは明示。
#   now 依存の揺れを減らすため now を1回だけ取る。
# ---------------------------------------------------------
@testset "resolve_time_window uses explicit start/end (JST input)" begin
    cfg = SearchConfig(
        task_name = "t",
        keywords_or = ["x"],
        local_tz_name = "Asia/Tokyo",
        use_state_highwater_start = true,
        window_hours = 24,
        now_lag_seconds = 45,
    )

    now_dt = utc_now_dt()
    end_dt_utc = now_dt - Hour(1)
    start_dt_utc = end_dt_utc - Hour(1)

    cfg.start_time_jst = utc_dt_to_local_str(start_dt_utc, cfg.local_tz_name)
    cfg.end_time_jst = utc_dt_to_local_str(end_dt_utc, cfg.local_tz_name)

    tw = resolve_time_window(cfg, nothing, "(x)"; endpoint = :recent)

    @test tw.start_utc == dt_to_utc_z(start_dt_utc)
    @test tw.end_utc == dt_to_utc_z(end_dt_utc)
    @test parse_utc_z(tw.start_utc) < parse_utc_z(tw.end_utc)
end

@testset "explicit start wins over high-water" begin
    cfg = SearchConfig(
        task_name = "t",
        keywords_or = ["x"],
        local_tz_name = "Asia/Tokyo",
        use_state_highwater_start = true,
        window_hours = 24,
        now_lag_seconds = 45,
    )

    now_dt = utc_now_dt()
    end_dt_utc = now_dt - Hour(1)
    start_dt_utc = end_dt_utc - Hour(1)

    cfg.start_time_jst = utc_dt_to_local_str(start_dt_utc, cfg.local_tz_name)
    cfg.end_time_jst = utc_dt_to_local_str(end_dt_utc, cfg.local_tz_name)

    st = CollectorState()
    st.completed = true
    st.query = "(x)"
    st.end_time_utc = dt_to_utc_z(end_dt_utc)

    tw = resolve_time_window(cfg, st, "(x)"; endpoint = :recent)

    @test tw.start_utc == dt_to_utc_z(start_dt_utc)
    @test tw.end_utc == dt_to_utc_z(end_dt_utc)
end

@testset "high-water overlap applied" begin
    cfg = SearchConfig(
        task_name = "t",
        keywords_or = ["x"],
        local_tz_name = "Asia/Tokyo",
        use_state_highwater_start = true,
        window_hours = nothing,
        highwater_overlap_seconds = 30,
        now_lag_seconds = 45,
    )

    now_dt = utc_now_dt()
    end_dt_utc = now_dt - Hour(1)

    cfg.start_time_jst = ""
    cfg.end_time_jst = utc_dt_to_local_str(end_dt_utc, cfg.local_tz_name)

    st = CollectorState()
    st.completed = true
    st.query = "(x)"
    st.end_time_utc = dt_to_utc_z(end_dt_utc)

    tw = resolve_time_window(cfg, st, "(x)"; endpoint = :recent)
    @test tw.start_utc == dt_to_utc_z(end_dt_utc - Second(30))
    @test tw.end_utc == dt_to_utc_z(end_dt_utc)
end

@testset "resolve_time_window errors if end_time older than 7 days (recent)" begin
    cfg = SearchConfig(
        task_name = "t",
        keywords_or = ["x"],
        local_tz_name = "Asia/Tokyo",
        window_hours = nothing,
        use_state_highwater_start = false,
        now_lag_seconds = 45,
    )

    now_dt = utc_now_dt()
    too_old_end = now_dt - Day(10)
    cfg.end_time_jst = utc_dt_to_local_str(too_old_end, cfg.local_tz_name)

    @test_throws ErrorException resolve_time_window(cfg, nothing, "(x)"; endpoint = :recent)
end

@testset "resolve_time_window clips future end_time to now-lag" begin
    with_logger(NullLogger()) do
        cfg = SearchConfig(
            task_name = "t",
            keywords_or = ["x"],
            local_tz_name = "Asia/Tokyo",
            window_hours = nothing,
            use_state_highwater_start = false,
            now_lag_seconds = 45,
        )

        t0 = utc_now_dt()
        future_end = t0 + Minute(10)

        cfg.end_time_jst = utc_dt_to_local_str(future_end, cfg.local_tz_name)
        cfg.start_time_jst = ""

        tw = resolve_time_window(cfg, nothing, "(x)"; endpoint = :recent)
        end_dt = parse_utc_z(tw.end_utc)

        clipped_target = t0 - Second(cfg.now_lag_seconds)
        @test abs(end_dt - clipped_target) <= Second(10)
    end
end

@testset "resolve_time_window hard-clips explicit start older than 7 days (recent)" begin
    with_logger(NullLogger()) do
        cfg = SearchConfig(
            task_name = "t",
            keywords_or = ["x"],
            local_tz_name = "Asia/Tokyo",
            window_hours = nothing,
            use_state_highwater_start = false,
            now_lag_seconds = 45,
        )

        now_dt = utc_now_dt()
        too_old_start = now_dt - Day(10)
        end_dt = now_dt - Day(1)

        cfg.start_time_jst = utc_dt_to_local_str(too_old_start, cfg.local_tz_name)
        cfg.end_time_jst = utc_dt_to_local_str(end_dt, cfg.local_tz_name)

        tw = resolve_time_window(cfg, nothing, "(x)"; endpoint = :recent)
        start_dt = parse_utc_z(tw.start_utc)
        limit_start = utc_now_dt() - Day(7)

        @test start_dt >= limit_start - Second(10)
    end
end

@testset "resolve_time_window errors if start >= end" begin
    with_logger(NullLogger()) do
        cfg = SearchConfig(
            task_name = "t",
            keywords_or = ["x"],
            local_tz_name = "Asia/Tokyo",
            window_hours = nothing,
            use_state_highwater_start = false,
            now_lag_seconds = 0,
        )

        now_dt = utc_now_dt()
        end_dt_utc = now_dt - Hour(1)
        start_dt_utc = end_dt_utc + Minute(1)

        cfg.start_time_jst = utc_dt_to_local_str(start_dt_utc, cfg.local_tz_name)
        cfg.end_time_jst = utc_dt_to_local_str(end_dt_utc, cfg.local_tz_name)

        @test_throws ErrorException resolve_time_window(
            cfg,
            nothing,
            "(x)";
            endpoint = :recent,
        )
    end
end

@testset "resolve_time_window uses window_hours fallback" begin
    cfg = SearchConfig(
        task_name = "t",
        keywords_or = ["x"],
        local_tz_name = "Asia/Tokyo",
        window_hours = 2,
        use_state_highwater_start = false,
        now_lag_seconds = 0,
    )

    now_dt = utc_now_dt() - Second(2)
    cfg.end_time_jst = utc_dt_to_local_str(now_dt, cfg.local_tz_name)
    cfg.start_time_jst = ""

    tw = resolve_time_window(cfg, nothing, "(x)"; endpoint = :recent)

    start_dt = parse_utc_z(tw.start_utc)
    end_dt = parse_utc_z(tw.end_utc)

    @test abs(start_dt - (end_dt - Hour(2))) <= Second(10)
end

# ---------------------------------------------------------
# tweet_to_row robustness
# ---------------------------------------------------------
@testset "tweet_to_row handles missing public_metrics" begin
    tw = Dict{String,Any}(
        "id" => "1",
        "created_at" => "2026-01-01T00:00:00Z",
        "author_id" => "10",
        "lang" => "en",
        "text" => "hi",
    )
    row = tweet_to_row(tw)
    @test row.id == "1"
    @test row.like_count === missing
    @test row.retweet_count === missing
    @test row.reply_count === missing
    @test row.quote_count === missing
    @test row.bookmark_count === missing
    @test row.impression_count === missing
end

@testset "parse_usage_summary extracts project usage" begin
    raw = Dict(
        "data" =>
            Dict("project_cap" => 1000, "project_usage" => 250, "cap_reset_day" => 17),
    )
    s = parse_usage_summary(raw)
    @test s.project_cap == 1000
    @test s.project_usage == 250
    @test s.cap_reset_day == 17
    @test s.remaining == 750
end


@testset "tweet_to_row reads extended public_metrics" begin
    tw = Dict{String,Any}(
        "id" => "1",
        "created_at" => "2026-01-01T00:00:00Z",
        "author_id" => "10",
        "lang" => "en",
        "text" => "hi",
        "public_metrics" => Dict(
            "like_count" => 1,
            "retweet_count" => 2,
            "reply_count" => 3,
            "quote_count" => 4,
            "bookmark_count" => 5,
            "impression_count" => 6,
        ),
    )
    row = tweet_to_row(tw)
    @test row.bookmark_count == 5
    @test row.impression_count == 6
end

# ---------------------------------------------------------
# Fixtures (existing)
# ---------------------------------------------------------
@testset "convert_outputs converts fixture MIN (strict fields)" begin
    with_temp_cfg(task = "conv_fixture_min") do cfg, dir
        with_logger(NullLogger()) do
            jsonl = out_jsonl(cfg)
            csvp = out_csv(cfg)
            arrp = out_arrow(cfg)

            sample = read_fixture("x_recent_search_sample.min.json")
            resp = JSON3.read(sample)

            tw = resp["data"][1]
            inc_tw = resp["includes"]["tweets"][1]

            tweet_line = String(JSON3.write((; kind = "tweet", data = tw)))
            include_line = String(JSON3.write((; kind = "include:tweets", data = inc_tw)))
            page_line = String(
                JSON3.write((;
                    kind = "page",
                    data = Dict("page" => 1, "meta" => get(resp, "meta", Dict())),
                )),
            )

            write_jsonl_lines(jsonl, [tweet_line, include_line, page_line])

            convert_outputs(cfg)

            @test count_csv_rows(csvp) == 1
            @test count_arrow_rows(arrp) == 1

            csv_tbl = CSV.File(
                IOBuffer(read(csvp));
                types = Dict(
                    :id => String,
                    :author_id => String,
                    :created_at => String,
                    :lang => String,
                    :text => String,
                    :possibly_sensitive => Union{Missing,Bool},
                    :like_count => Union{Missing,Int},
                    :retweet_count => Union{Missing,Int},
                    :reply_count => Union{Missing,Int},
                    :quote_count => Union{Missing,Int},
                    :bookmark_count => Union{Missing,Int},
                    :impression_count => Union{Missing,Int},
                ),
            )
            row = first(csv_tbl)

            @test row.id == "1212092628029698048"
            @test row.created_at == "2019-12-31T19:26:16.000Z"
            @test row.author_id == "2244994945"
            @test row.lang == "en"
            @test row.possibly_sensitive == false
            @test row.like_count == 38
            @test row.retweet_count == 7
            @test row.reply_count == 3
            @test row.quote_count == 1
            @test row.bookmark_count === missing || row.bookmark_count isa Int
            @test row.impression_count === missing || row.impression_count isa Int
            @test occursin("best future version of our API", row.text)

            arrow_tbl = Arrow.Table(IOBuffer(read(arrp)))
            arow = first(Tables.rows(arrow_tbl))

            @test String(arow.id) == row.id
            @test String(arow.created_at) == row.created_at
            @test String(arow.author_id) == row.author_id
            @test String(arow.lang) == row.lang
            @test arow.possibly_sensitive == row.possibly_sensitive
            @test isequal(arow.like_count, row.like_count)
            @test isequal(arow.retweet_count, row.retweet_count)
            @test isequal(arow.reply_count, row.reply_count)
            @test isequal(arow.quote_count, row.quote_count)
            @test isequal(arow.bookmark_count, row.bookmark_count)
            @test isequal(arow.impression_count, row.impression_count)
            @test occursin("best future version of our API", String(arow.text))
        end
    end
end

@testset "convert_outputs converts fixture FULL (smoke + key fields)" begin
    with_temp_cfg(task = "conv_fixture_full") do cfg, dir
        with_logger(NullLogger()) do
            jsonl = out_jsonl(cfg)
            csvp = out_csv(cfg)
            arrp = out_arrow(cfg)

            sample = read_fixture("x_recent_search_sample.full.json")
            resp = JSON3.read(sample)

            tw = resp["data"][1]

            lines = String[]
            push!(lines, String(JSON3.write((; kind = "tweet", data = tw))))
            if haskey(resp, "includes") &&
               haskey(resp["includes"], "tweets") &&
               length(resp["includes"]["tweets"]) > 0
                inc_tw = resp["includes"]["tweets"][1]
                push!(
                    lines,
                    String(JSON3.write((; kind = "include:tweets", data = inc_tw))),
                )
            end
            push!(lines, jsonl_page_line(1))

            write_jsonl_lines(jsonl, lines)
            convert_outputs(cfg)

            @test count_csv_rows(csvp) == 1
            @test count_arrow_rows(arrp) == 1

            csv_tbl = CSV.File(
                IOBuffer(read(csvp));
                types = Dict(
                    :id => String,
                    :author_id => String,
                    :created_at => String,
                    :lang => String,
                    :text => String,
                ),
            )
            row = first(csv_tbl)

            @test !isempty(row.id)
            @test !isempty(row.author_id)
            @test !isempty(row.created_at)
            @test !isempty(row.text)
            @test !isempty(row.lang)
        end
    end
end

# ---------------------------------------------------------
# Wide conversion: page マーカーで flush + 必須カラム検証
# ---------------------------------------------------------
@testset "convert_outputs_wide flushes on page markers and keeps required fields" begin
    with_temp_cfg(task = "wide_page") do cfg, dir
        with_logger(NullLogger()) do
            cfg.emit_arrow = false

            jsonl = out_jsonl(cfg)
            csvw = out_csv_wide(cfg)

            t1 = Dict(
                "id" => "1",
                "author_id" => "10",
                "created_at" => "2026-01-01T00:00:00Z",
                "lang" => "en",
                "text" => "a",
            )
            u10 = Dict("id" => "10", "username" => "alice", "name" => "Alice")
            t2 = Dict(
                "id" => "2",
                "author_id" => "11",
                "created_at" => "2026-01-01T00:01:00Z",
                "lang" => "en",
                "text" => "b",
            )
            u11 = Dict("id" => "11", "username" => "bob", "name" => "Bob")

            lines = [
                String(JSON3.write((; kind = "tweet", data = t1))),
                String(JSON3.write((; kind = "include:users", data = u10))),
                jsonl_page_line(1),
                String(JSON3.write((; kind = "tweet", data = t2))),
                String(JSON3.write((; kind = "include:users", data = u11))),
                jsonl_page_line(2),
            ]
            write_jsonl_lines(jsonl, lines)

            convert_outputs_wide(cfg)

            @test isfile(csvw)
            @test count_csv_rows(csvw) == 2

            tbl = CSV.File(IOBuffer(read(csvw)))
            rows = collect(tbl)
            @test length(rows) == 2

            # 必須：id/author_id/created_at/text が欠損でない
            @test !ismissing(rows[1].id)
            @test !ismissing(rows[1].author_id)
            @test !ismissing(rows[1].created_at)
            @test !ismissing(rows[1].text)

            # includes があるケースなので username も埋まること
            @test rows[1].author_username == "alice"
            @test rows[2].author_username == "bob"
        end
    end
end

# ---------------------------------------------------------
# Filtered Stream
# ---------------------------------------------------------
@testset "ensure_stream_rule! creates, reuses, and protects tagged rules" begin
    with_temp_stream_cfg(task = "stream_rules") do cfg, dir
        with_env_token() do
            headers = ["Authorization" => "Bearer dummy"]
            headers_seen = Ref(false)
            bodies = Any[]
            fetch_stub =
                (method, url, headers, params; body = nothing, max_retries = 6, readtimeout = 30) -> begin
                    headers_seen[] = any(first(h) == "Authorization" for h in headers)
                    if method == "GET"
                        return Dict("data" => Any[], "meta" => Dict("result_count" => 0))
                    end
                    push!(bodies, body)
                    add = body["add"][1]
                    return Dict(
                        "data" => Any[
                            Dict(
                                "id" => "rule-created",
                                "value" => add["value"],
                                "tag" => add["tag"],
                            ),
                        ],
                    )
                end

            created = ensure_stream_rule!(cfg, headers, fetch_stub)
            @test headers_seen[] == true
            @test created.created == true
            @test created.id == "rule-created"
            @test length(bodies) == 1
            @test bodies[1]["add"][1]["tag"] == cfg.rule_tag

            rule_value = created.value
            reuse_stub =
                (method, url, headers, params; body = nothing, max_retries = 6, readtimeout = 30) -> begin
                    @test method == "GET"
                    return Dict(
                        "data" => Any[
                            Dict("id" => "rule-existing", "value" => rule_value, "tag" => cfg.rule_tag),
                        ],
                    )
                end

            reused = ensure_stream_rule!(cfg, headers, reuse_stub)
            @test reused.created == false
            @test reused.id == "rule-existing"

            conflict_stub =
                (method, url, headers, params; body = nothing, max_retries = 6, readtimeout = 30) -> begin
                    return Dict(
                        "data" => Any[
                            Dict("id" => "rule-conflict", "value" => "other", "tag" => cfg.rule_tag),
                        ],
                    )
                end

            cfg.replace_rule_by_tag = false
            @test_throws ErrorException ensure_stream_rule!(cfg, headers, conflict_stub)
        end
    end
end

@testset "ensure_stream_rule! replaces tagged rules when requested" begin
    with_temp_stream_cfg(task = "stream_replace") do cfg, dir
        with_env_token() do
            headers = ["Authorization" => "Bearer dummy"]
            methods = String[]
            bodies = Any[]
            fetch_stub =
                (method, url, headers, params; body = nothing, max_retries = 6, readtimeout = 30) -> begin
                    push!(methods, method)
                    push!(bodies, body)
                    if method == "GET"
                        return Dict(
                            "data" => Any[
                                Dict("id" => "old-rule", "value" => "other", "tag" => cfg.rule_tag),
                            ],
                        )
                    elseif haskey(body, "delete")
                        return Dict("meta" => Dict("summary" => Dict("deleted" => 1)))
                    end
                    add = body["add"][1]
                    return Dict(
                        "data" => Any[
                            Dict("id" => "new-rule", "value" => add["value"], "tag" => add["tag"]),
                        ],
                    )
                end

            cfg.replace_rule_by_tag = true
            result = ensure_stream_rule!(cfg, headers, fetch_stub)
            @test result.created == true
            @test result.id == "new-rule"
            @test methods == ["GET", "POST", "POST"]
            @test bodies[2]["delete"]["ids"] == ["old-rule"]
            @test haskey(bodies[3], "add")
        end
    end
end

@testset "ensure_stream_rule! rejects failed rules update responses" begin
    with_temp_stream_cfg(task = "stream_rule_failures") do cfg, dir
        with_env_token() do
            headers = ["Authorization" => "Bearer dummy"]

            function add_response_stub(add_response)
                return (
                    method,
                    url,
                    headers,
                    params;
                    body = nothing,
                    max_retries = 6,
                    readtimeout = 30,
                ) -> begin
                    method == "GET" &&
                        return Dict("data" => Any[], "meta" => Dict("result_count" => 0))
                    return add_response
                end
            end

            @test_throws ErrorException ensure_stream_rule!(
                cfg,
                headers,
                add_response_stub(Dict("errors" => Any[Dict("detail" => "bad rule")])),
            )

            @test_throws ErrorException ensure_stream_rule!(
                cfg,
                headers,
                add_response_stub(
                    Dict("meta" => Dict("summary" => Dict("invalid" => 1, "not_created" => 1))),
                ),
            )

            @test_throws ErrorException ensure_stream_rule!(
                cfg,
                headers,
                add_response_stub(
                    Dict(
                        "data" => Any[
                            Dict("id" => "other", "value" => "different", "tag" => cfg.rule_tag),
                        ],
                    ),
                ),
            )

            rule_value = build_query(cfg)
            @test_throws ErrorException ensure_stream_rule!(
                cfg,
                headers,
                add_response_stub(
                    Dict(
                        "data" => Any[
                            Dict("id" => "", "value" => rule_value, "tag" => cfg.rule_tag),
                        ],
                    ),
                ),
            )

            cfg.replace_rule_by_tag = true
            delete_failure_stub =
                (method, url, headers, params; body = nothing, max_retries = 6, readtimeout = 30) -> begin
                    if method == "GET"
                        return Dict(
                            "data" => Any[
                                Dict("id" => "old-rule", "value" => "other", "tag" => cfg.rule_tag),
                            ],
                        )
                    end
                    if haskey(body, "delete")
                        return Dict("meta" => Dict("summary" => Dict("not_deleted" => 1)))
                    end
                    return Dict("data" => Any[])
            end
            @test_throws ErrorException ensure_stream_rule!(cfg, headers, delete_failure_stub)

            methods = String[]
            missing_id_stub =
                (method, url, headers, params; body = nothing, max_retries = 6, readtimeout = 30) -> begin
                    push!(methods, method)
                    method == "GET" &&
                        return Dict(
                            "data" => Any[Dict("value" => "other", "tag" => cfg.rule_tag)],
                        )
                    error("delete should not be called when conflicting rule id is missing")
                end
            @test_throws ErrorException ensure_stream_rule!(cfg, headers, missing_id_stub)
            @test methods == ["GET"]
        end
    end
end

@testset "stream response and reconnect helpers classify outcomes" begin
    ok = _stream_response_outcome(HTTP.Response(200), "http://x.test")
    @test ok.kind == :ok

    limited =
        _stream_response_outcome(HTTP.Response(429, ["retry-after" => "1"]), "http://x.test")
    @test limited.kind == :retryable
    @test limited.stop_reason == "rate_limited"

    server = _stream_response_outcome(HTTP.Response(503), "http://x.test")
    @test server.kind == :retryable
    @test server.stop_reason == "server_error"

    @test_throws XPostCollector.XApiAccessError _stream_response_outcome(
        HTTP.Response(403),
        "http://x.test";
        body = "{\"detail\":\"denied\"}",
    )

    cfg = StreamConfig(task_name = "stream_retry", keywords_or = ["foo"], max_reconnects = 1)
    validate!(cfg)
    first = _stream_reconnect_decision(cfg, 0)
    @test first.should_reconnect == true
    @test first.reconnects == 1
    second = _stream_reconnect_decision(cfg, first.reconnects)
    @test second.should_reconnect == false
    @test second.stop_reason == "max_reconnects_exceeded"

    unlimited = StreamConfig(task_name = "stream_retry_unlimited", keywords_or = ["foo"])
    validate!(unlimited)
    @test unlimited.max_reconnects == 0
    u1 = _stream_reconnect_decision(unlimited, 10_000)
    @test u1.should_reconnect == true
    @test u1.reconnects == 10_001
    @test u1.stop_reason == ""

    cfg.reconnect = false
    disabled = _stream_reconnect_decision(cfg, 0)
    @test disabled.should_reconnect == false
    @test disabled.stop_reason == ""
end

@testset "handle_stream_line! writes reusable JSONL and deduplicates" begin
    with_temp_stream_cfg(task = "stream_lines") do cfg, dir
        with_logger(NullLogger()) do
            cfg.emit_arrow = false
            st = StreamState()
            st.task_name = cfg.task_name
            st.query = build_query(cfg)
            st.rule_tag = cfg.rule_tag

            sdb = init_seen_db(cfg)
            try
                open(out_jsonl(cfg), "w") do io
                    @test handle_stream_line!(cfg, st, sdb, io, "") == :keepalive
                    @test st.keepalive_count == 1

                    tweet = Dict(
                        "id" => "100",
                        "created_at" => "2026-01-01T00:00:00Z",
                        "author_id" => "10",
                        "lang" => "ja",
                        "text" => "hello stream",
                        "public_metrics" => Dict("like_count" => 1),
                    )
                    user = Dict("id" => "10", "username" => "alice", "name" => "Alice")
                    event = Dict(
                        "data" => tweet,
                        "includes" => Dict("users" => Any[user]),
                        "matching_rules" => Any[Dict("id" => "r1", "tag" => cfg.rule_tag)],
                    )
                    @test handle_stream_line!(cfg, st, sdb, io, String(JSON3.write(event))) == :tweet
                    @test handle_stream_line!(cfg, st, sdb, io, "data: " * String(JSON3.write(event))) == :ignored
                    @test handle_stream_line!(cfg, st, sdb, io, "not-json") == :invalid
                end
            finally
                DBInterface.close!(sdb.db)
            end

            lines = readlines(out_jsonl(cfg))
            @test count(ln -> occursin("\"kind\":\"tweet\"", ln), lines) == 1
            @test count(ln -> occursin("\"kind\":\"include:users\"", ln), lines) == 1
            @test count(ln -> occursin("\"endpoint\":\"stream\"", ln), lines) == 1

            convert_outputs(cfg)
            convert_outputs_wide(cfg)
            @test count_csv_rows(out_csv(cfg)) == 1
            rows = collect(CSV.File(out_csv_wide(cfg)))
            @test length(rows) == 1
            @test rows[1].author_username == "alice"
        end
    end
end

@testset "stream persistence is durable before seen DB and throttles keepalive state" begin
    with_temp_stream_cfg(task = "stream_persist_safety") do cfg, dir
        with_logger(NullLogger()) do
            st = StreamState()
            st.task_name = cfg.task_name
            st.query = build_query(cfg)
            st.rule_tag = cfg.rule_tag
            sdb = init_seen_db(cfg)
            try
                @test_throws ErrorException handle_stream_line!(
                    cfg,
                    st,
                    sdb,
                    FailingIO(),
                    stream_event_json("write-fail"),
                )
                @test mark_seen!(sdb, "write-fail", utc_now_z(lag_seconds = 0)) == true
            finally
                DBInterface.close!(sdb.db)
            end
        end
    end

    with_temp_stream_cfg(task = "stream_state_throttle") do cfg, dir
        with_logger(NullLogger()) do
            cfg.state_flush_interval_seconds = 3600.0
            st = StreamState()
            st.task_name = cfg.task_name
            st.query = build_query(cfg)
            st.rule_tag = cfg.rule_tag
            sdb = init_seen_db(cfg)
            try
                open(out_jsonl(cfg), "w") do io
                    pending = IOBuffer()
                    activity = Ref(false)
                    state_flush_ref = Ref(time())
                    res = _process_stream_chunk!(
                        cfg,
                        st,
                        sdb,
                        io,
                        pending,
                        collect(codeunits("\n")),
                        time(),
                        activity,
                        state_flush_ref,
                    )
                    @test res.result == :pending
                    @test st.keepalive_count == 1
                    @test st.last_heartbeat_at != ""
                    @test activity[] == true
                    @test !isfile(out_stream_state(cfg))
                end
            finally
                DBInterface.close!(sdb.db)
            end
        end
    end
end

@testset "stream JSONL rotation converts across rotated files" begin
    with_temp_stream_cfg(task = "stream_rotate") do cfg, dir
        with_logger(NullLogger()) do
            cfg.emit_arrow = false
            cfg.rotate_jsonl_bytes = 1
            cfg.max_posts = 0
            st = StreamState()
            st.task_name = cfg.task_name
            st.query = build_query(cfg)
            st.rule_tag = cfg.rule_tag
            sdb = init_seen_db(cfg)
            writer = _open_stream_jsonl_writer(cfg)
            try
                @test handle_stream_line!(cfg, st, sdb, writer, stream_event_json("rot-1")) == :tweet
                @test handle_stream_line!(cfg, st, sdb, writer, stream_event_json("rot-2")) == :tweet
            finally
                _close_stream_jsonl_writer!(writer)
                DBInterface.close!(sdb.db)
            end

            paths = stream_jsonl_paths(cfg)
            @test length(paths) == 2
            @test occursin(".rot-", basename(paths[1]))
            @test paths[end] == out_jsonl(cfg)

            convert_outputs(cfg)
            convert_outputs_wide(cfg)
            @test count_csv_rows(out_csv(cfg)) == 2
            @test count_csv_rows(out_csv_wide(cfg)) == 2
        end
    end
end

@testset "stream conversion retries active partial final lines" begin
    with_temp_stream_cfg(task = "stream_partial_convert") do cfg, dir
        with_logger(NullLogger()) do
            cfg.emit_arrow = false
            cfg.convert_incremental = true
            good = jsonl_tweet_line("partial-1")
            partial = jsonl_tweet_line("partial-2")
            prefix = first(partial, length(partial) - 5)
            suffix = last(partial, 5)

            open(out_jsonl(cfg), "w") do io
                write(io, good)
                write(io, '\n')
                write(io, prefix)
            end

            first_res = convert_outputs(cfg)
            @test count_csv_rows(out_csv(cfg)) == 1
            @test first_res.converted_offset == ncodeunits(good) + 1
            st = load_state(joinpath(dir, "$(cfg.task_name).state.json"))
            @test st.converted_jsonl_offset == ncodeunits(good) + 1

            open(out_jsonl(cfg), "a") do io
                write(io, suffix)
                write(io, '\n')
            end
            second_res = convert_outputs(cfg)
            @test second_res.converted_offset == stat(out_jsonl(cfg)).size
            @test count_csv_rows(out_csv(cfg)) == 2
        end
    end

    with_temp_stream_cfg(task = "stream_partial_wide") do cfg, dir
        with_logger(NullLogger()) do
            cfg.emit_arrow = false
            cfg.convert_incremental = true
            good_lines = [jsonl_tweet_line("wide-1"), jsonl_page_line(1)]
            good_bytes = sum(ncodeunits(ln) + 1 for ln in good_lines)
            partial = jsonl_tweet_line("wide-2")
            prefix = first(partial, length(partial) - 5)
            suffix = last(partial, 5)

            open(out_jsonl(cfg), "w") do io
                for ln in good_lines
                    write(io, ln)
                    write(io, '\n')
                end
                write(io, prefix)
            end

            first_res = convert_outputs_wide(cfg)
            @test count_csv_rows(out_csv_wide(cfg)) == 1
            @test first_res.converted_offset == good_bytes
            st = load_state(joinpath(dir, "$(cfg.task_name).wide.state.json"))
            @test st.converted_jsonl_offset == good_bytes

            open(out_jsonl(cfg), "a") do io
                write(io, suffix)
                write(io, '\n')
            end
            second_res = convert_outputs_wide(cfg)
            @test second_res.converted_offset == stat(out_jsonl(cfg)).size
            @test count_csv_rows(out_csv_wide(cfg)) == 2
        end
    end
end

@testset "stream conversion skips corrupt rotated lines" begin
    with_temp_stream_cfg(task = "stream_rotated_corrupt") do cfg, dir
        with_logger(NullLogger()) do
            cfg.emit_arrow = false
            rotated = joinpath(dir, "$(cfg.task_name).rot-20260101T000000-000001.jsonl")
            bad = jsonl_tweet_line("rot-bad")
            open(rotated, "w") do io
                write(io, jsonl_tweet_line("rot-good-1"))
                write(io, '\n')
                write(io, first(bad, length(bad) - 5))
            end
            open(out_jsonl(cfg), "w") do io
                write(io, jsonl_tweet_line("rot-good-2"))
                write(io, '\n')
            end

            res = convert_outputs(cfg)
            @test count_csv_rows(out_csv(cfg)) == 2
            @test res.converted_offset == stat(rotated).size + stat(out_jsonl(cfg)).size
        end
    end
end

@testset "stream rotation failure reopens active writer" begin
    with_temp_stream_cfg(task = "stream_rotate_recover") do cfg, dir
        with_logger(NullLogger()) do
            cfg.rotate_jsonl_bytes = 1
            writer = _open_stream_jsonl_writer(cfg)
            try
                write(writer.io, "seed\n")
                flush(writer.io)
                fail_move = (src, dst; force = false) -> error("mv failed")
                @test_throws ErrorException _rotate_stream_jsonl_if_needed!(
                    cfg,
                    writer;
                    move_file = fail_move,
                )
                @test isopen(writer.io)
                write(writer.io, "after\n")
                flush(writer.io)
            finally
                _close_stream_jsonl_writer!(writer)
            end
            @test occursin("seed\n", read(out_jsonl(cfg), String))
            @test occursin("after\n", read(out_jsonl(cfg), String))
        end
    end
end

@testset "stream gap records capture reconnect windows" begin
    with_temp_stream_cfg(task = "stream_gap") do cfg, dir
        st = StreamState()
        st.task_name = cfg.task_name
        st.query = build_query(cfg)
        st.rule_tag = cfg.rule_tag
        st.rule_id = "rule-1"
        _write_stream_gap!(
            cfg,
            st,
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:30Z",
            "disconnected",
        )
        lines = readlines(out_stream_gaps(cfg))
        @test length(lines) == 1
        obj = JSON3.read(lines[1])
        @test obj["task_name"] == cfg.task_name
        @test obj["query"] == st.query
        @test obj["start_time_utc"] == "2026-01-01T00:00:00Z"
        @test obj["end_time_utc"] == "2026-01-01T00:00:30Z"
        @test obj["reason"] == "disconnected"
    end
end

@testset "stream pending line flush writes final JSON event" begin
    with_temp_stream_cfg(task = "stream_pending") do cfg, dir
        with_logger(NullLogger()) do
            cfg.emit_arrow = false
            st = StreamState()
            st.task_name = cfg.task_name
            st.query = build_query(cfg)
            st.rule_tag = cfg.rule_tag

            sdb = init_seen_db(cfg)
            try
                open(out_jsonl(cfg), "w") do io
                    tweet1 = Dict(
                        "id" => "200",
                        "created_at" => "2026-01-01T00:00:00Z",
                        "author_id" => "20",
                        "lang" => "ja",
                        "text" => "pending one",
                    )
                    tweet2 = Dict(
                        "id" => "201",
                        "created_at" => "2026-01-01T00:00:01Z",
                        "author_id" => "21",
                        "lang" => "ja",
                        "text" => "pending two",
                    )
                    event = Dict("data" => Any[tweet1, tweet2])
                    pending = IOBuffer()
                    activity = Ref(false)

                    chunk = collect(codeunits(String(JSON3.write(event))))
                    res = _process_stream_chunk!(
                        cfg,
                        st,
                        sdb,
                        io,
                        pending,
                        chunk,
                        time(),
                        activity,
                    )
                    @test res.result == :pending
                    @test activity[] == false
                    @test count(ln -> occursin("\"kind\":\"tweet\"", ln), readlines(out_jsonl(cfg))) == 0

                    flushed =
                        _flush_pending_stream_line!(cfg, st, sdb, io, pending, time(), activity)
                    @test flushed.result == :tweet
                    @test activity[] == true
                    @test st.total_tweets == 2
                end
            finally
                DBInterface.close!(sdb.db)
            end

            lines = readlines(out_jsonl(cfg))
            @test count(ln -> occursin("\"kind\":\"tweet\"", ln), lines) == 2
            @test count(ln -> occursin("\"endpoint\":\"stream\"", ln), lines) == 1
        end
    end
end

@testset "stream read loop handles empty chunks without spinning" begin
    with_temp_stream_cfg(task = "stream_empty_chunk") do cfg, dir
        with_logger(NullLogger()) do
            st = StreamState()
            st.task_name = cfg.task_name
            st.query = build_query(cfg)
            st.rule_tag = cfg.rule_tag
            sdb = init_seen_db(cfg)
            try
                open(out_jsonl(cfg), "w") do io
                    stream = FakeReadavailableStream(Vector{UInt8}[UInt8[]])
                    activity = Ref(false)
                    outcome = _read_stream_lines!(cfg, st, sdb, io, stream, time(), activity)
                    @test outcome.kind == :disconnected
                    @test outcome.stop_reason == "disconnected"
                    @test outcome.activity == false
                    @test activity[] == false
                    @test stream.calls == 1
                end
            finally
                DBInterface.close!(sdb.db)
            end
            @test readlines(out_jsonl(cfg)) == String[]
        end
    end

    with_temp_stream_cfg(task = "stream_empty_after_pending") do cfg, dir
        with_logger(NullLogger()) do
            cfg.max_posts = 0
            st = StreamState()
            st.task_name = cfg.task_name
            st.query = build_query(cfg)
            st.rule_tag = cfg.rule_tag
            sdb = init_seen_db(cfg)
            try
                open(out_jsonl(cfg), "w") do io
                    tweet = Dict(
                        "id" => "300",
                        "created_at" => "2026-01-01T00:00:00Z",
                        "author_id" => "30",
                        "lang" => "ja",
                        "text" => "pending before empty",
                    )
                    event = String(JSON3.write(Dict("data" => tweet)))
                    stream = FakeReadavailableStream(
                        Vector{UInt8}[collect(codeunits(event)), UInt8[]],
                    )
                    activity = Ref(false)
                    outcome = _read_stream_lines!(cfg, st, sdb, io, stream, time(), activity)
                    @test outcome.kind == :disconnected
                    @test outcome.stop_reason == "disconnected"
                    @test outcome.activity == true
                    @test activity[] == true
                    @test st.total_tweets == 1
                    @test stream.calls == 2
                end
            finally
                DBInterface.close!(sdb.db)
            end
            lines = readlines(out_jsonl(cfg))
            @test count(ln -> occursin("\"kind\":\"tweet\"", ln), lines) == 1
            @test count(ln -> occursin("\"endpoint\":\"stream\"", ln), lines) == 1
        end
    end

    with_temp_stream_cfg(task = "stream_empty_after_max_posts") do cfg, dir
        with_logger(NullLogger()) do
            cfg.max_posts = 1
            st = StreamState()
            st.task_name = cfg.task_name
            st.query = build_query(cfg)
            st.rule_tag = cfg.rule_tag
            sdb = init_seen_db(cfg)
            try
                open(out_jsonl(cfg), "w") do io
                    tweet = Dict(
                        "id" => "301",
                        "created_at" => "2026-01-01T00:00:00Z",
                        "author_id" => "31",
                        "lang" => "ja",
                        "text" => "pending max posts",
                    )
                    event = String(JSON3.write(Dict("data" => tweet)))
                    stream = FakeReadavailableStream(
                        Vector{UInt8}[collect(codeunits(event)), UInt8[]],
                    )
                    activity = Ref(false)
                    outcome = _read_stream_lines!(cfg, st, sdb, io, stream, time(), activity)
                    @test outcome.kind == :completed
                    @test outcome.stop_reason == "max_posts"
                    @test outcome.activity == true
                    @test st.completed == true
                    @test st.stop_reason == "max_posts"
                end
            finally
                DBInterface.close!(sdb.db)
            end
        end
    end
end

# ---------------------------------------------------------
# run_collector のネットワーク無しテスト（search/all + target_posts）
# ---------------------------------------------------------
@testset "run_collector uses search/all and stops at target_posts (drain)" begin
    with_temp_cfg(task = "drain_all") do cfg, dir
        with_logger(NullLogger()) do
            with_env_token() do
                # auto で all に落ちるよう end を 9日前にする
                now_dt = utc_now_dt()
                end_dt_utc = now_dt - Day(9)
                start_dt_utc = end_dt_utc - Hour(1)

                cfg.local_tz_name = "Asia/Tokyo"
                cfg.search_mode = :auto
                cfg.start_time_jst = Dates.format(
                    DateTime(
                        astimezone(
                            ZonedDateTime(start_dt_utc, tz"UTC"),
                            TimeZone(cfg.local_tz_name),
                        ),
                    ),
                    DT_FMT_ISO,
                )
                cfg.end_time_jst = Dates.format(
                    DateTime(
                        astimezone(
                            ZonedDateTime(end_dt_utc, tz"UTC"),
                            TimeZone(cfg.local_tz_name),
                        ),
                    ),
                    DT_FMT_ISO,
                )

                cfg.target_posts = 3
                cfg.write_includes = false
                cfg.max_results = 10

                urls = String[]
                calls = Ref(0)

                fetch_stub =
                    (url, _headers, _params) -> begin
                        push!(urls, url)
                        calls[] += 1
                        if calls[] == 1
                            return Dict(
                                "data" => Any[
                                    Dict(
                                        "id" => "1",
                                        "text" => "a",
                                        "author_id" => "10",
                                        "created_at" => "2020-01-01T00:00:00Z",
                                        "lang" => "en",
                                    ),
                                    Dict(
                                        "id" => "2",
                                        "text" => "b",
                                        "author_id" => "10",
                                        "created_at" => "2020-01-01T00:00:01Z",
                                        "lang" => "en",
                                    ),
                                ],
                                "meta" =>
                                    Dict("next_token" => "N1", "result_count" => 2),
                            )
                        elseif calls[] == 2
                            return Dict(
                                "data" => Any[
                                    Dict(
                                        "id" => "3",
                                        "text" => "c",
                                        "author_id" => "10",
                                        "created_at" => "2020-01-01T00:00:02Z",
                                        "lang" => "en",
                                    ),
                                    Dict(
                                        "id" => "4",
                                        "text" => "d",
                                        "author_id" => "10",
                                        "created_at" => "2020-01-01T00:00:03Z",
                                        "lang" => "en",
                                    ),
                                ],
                                "meta" =>
                                    Dict("next_token" => "N2", "result_count" => 2),
                            )
                        end
                        return Dict("data" => Any[], "meta" => Dict("result_count" => 0))
                    end

                @eval XPostCollector begin
                    function fetch_with_retry(
                        url::AbstractString,
                        headers,
                        params::Dict{String,String};
                        max_retries::Int = 6,
                    )
                        return $fetch_stub(String(url), headers, params)
                    end
                end

                st = Base.invokelatest(run_collector, cfg)

                @test length(urls) == 2
                @test occursin("/2/tweets/search/all", urls[1])
                @test occursin("/2/tweets/search/all", urls[2])

                @test st.total_tweets >= 3

                # JSONL に page マーカーが入っている
                jlines = readlines(out_jsonl(cfg))
                @test any(occursin("\"kind\":\"page\"", ln) for ln in jlines)
            end
        end
    end
end
