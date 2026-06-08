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

                request_stub =
                    (
                        method,
                        url;
                        headers = nothing,
                        query = Dict{String,String}(),
                        body = UInt8[],
                        status_exception = true,
                        readtimeout = 30,
                    ) -> begin
                        @test method == "GET"
                        push!(urls, String(url))
                        calls[] += 1
                        if calls[] == 1
                            obj = Dict(
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
                            return HTTP.Response(200, String(JSON3.write(obj)))
                        elseif calls[] == 2
                            obj = Dict(
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
                            return HTTP.Response(200, String(JSON3.write(obj)))
                        end
                        obj = Dict("data" => Any[], "meta" => Dict("result_count" => 0))
                        return HTTP.Response(200, String(JSON3.write(obj)))
                    end

                client = XApiClient(
                    bearer_token = "dummy",
                    request_fn = request_stub,
                    sleep_fn = _ -> nothing,
                    rand_fn = () -> 0.0,
                )

                st = run_collector(cfg; client = client)

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
