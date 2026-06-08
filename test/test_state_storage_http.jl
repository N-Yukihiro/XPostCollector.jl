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

@testset "StreamState diagnostics save/load and legacy migration" begin
    mktempdir() do dir
        p = joinpath(dir, "stream.state.json")
        st = StreamState()
        st.task_name = "stream"
        st.query = "(foo)"
        st.rule_tag = "tag"
        st.diagnostics.status = 429
        st.diagnostics.error = "rate limited"
        st.diagnostics.error_type = "https://api.x.com/2/problems/rate-limit-exceeded"
        st.diagnostics.error_detail = "Rate limit exceeded"
        st.diagnostics.rate_limit_limit = 50
        st.diagnostics.rate_limit_remaining = 0
        st.diagnostics.rate_limit_reset = 123
        st.diagnostics.retry_after = 5

        save_state(p, st)
        raw = JSON3.read(read(p, String))
        @test haskey(raw, "diagnostics")
        @test !haskey(raw, "last_status")

        loaded = load_stream_state(p)
        @test loaded !== nothing
        @test loaded.diagnostics.status == 429
        @test loaded.diagnostics.error == "rate limited"
        @test loaded.diagnostics.error_type == "https://api.x.com/2/problems/rate-limit-exceeded"
        @test loaded.diagnostics.rate_limit_limit == 50
        @test loaded.diagnostics.retry_after == 5

        legacy = Dict(
            "timestamp" => "t",
            "task_name" => "legacy",
            "query" => "(bar)",
            "rule_id" => nothing,
            "rule_tag" => "old",
            "connection_count" => 1,
            "total_tweets" => 2,
            "total_includes" => 3,
            "keepalive_count" => 4,
            "last_heartbeat_at" => "h",
            "last_event_at" => "e",
            "last_error" => "usage cap exceeded",
            "last_status" => 429,
            "last_error_type" => "https://api.x.com/2/problems/usage-capped",
            "last_error_detail" => "Usage cap exceeded",
            "last_rate_limit_limit" => 50,
            "last_rate_limit_remaining" => 0,
            "last_rate_limit_reset" => 456,
            "last_retry_after" => 7,
            "consecutive_failures" => 5,
            "last_connected_at" => "c",
            "last_disconnect_at" => "d",
            "seen_count" => 6,
            "db_path" => "db",
            "db_size_bytes" => 7,
            "completed" => false,
            "stop_reason" => "usage_capped",
        )
        legacy_path = joinpath(dir, "legacy.stream.state.json")
        write(legacy_path, String(JSON3.write(legacy)))
        migrated = load_stream_state(legacy_path)
        @test migrated !== nothing
        @test migrated.task_name == "legacy"
        @test migrated.diagnostics.status == 429
        @test migrated.diagnostics.error_type == "https://api.x.com/2/problems/usage-capped"
        @test migrated.diagnostics.rate_limit_reset == 456
        @test migrated.diagnostics.retry_after == 7
    end
end

# ---------------------------------------------------------
# Rate limit helper
# ---------------------------------------------------------
@testset "XApiClient builds bearer headers from explicit token and env" begin
    explicit = XApiClient(bearer_token = "explicit-token")
    explicit_headers = bearer_headers(
        explicit;
        user_agent = "ua",
        accept_encoding = "identity",
    )
    @test ("Authorization" => "Bearer explicit-token") in explicit_headers
    @test ("User-Agent" => "ua") in explicit_headers
    @test ("Accept-Encoding" => "identity") in explicit_headers

    Base.withenv("BEARER_TOKEN" => "env-token") do
        env_headers = bearer_headers(XApiClient(); user_agent = "env-ua")
        @test ("Authorization" => "Bearer env-token") in env_headers
        @test ("User-Agent" => "env-ua") in env_headers
    end
end

@testset "rate_limit_sleep_seconds" begin
    resp1 = HTTP.Response(429, ["retry-after" => "10"])
    @test rate_limit_sleep_seconds(resp1; min_backoff_seconds = 15) >= 15

    future = Int(floor(time())) + 100
    resp2 = HTTP.Response(429, ["x-rate-limit-reset" => string(future)])
    @test rate_limit_sleep_seconds(resp2; min_backoff_seconds = 15) >= 15
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

