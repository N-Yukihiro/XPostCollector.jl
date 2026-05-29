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

