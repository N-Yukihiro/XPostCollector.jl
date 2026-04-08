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
