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
