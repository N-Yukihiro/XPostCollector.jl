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

@testset "stream rules 2xx responses and safe error bodies" begin
    with_temp_stream_cfg(task = "stream_rules_201") do cfg, dir
        with_env_token() do
            cfg.rule_tag = "stream-rules-201"
            rule_value = build_query(cfg)
            server = HTTP.serve!(listenany = true) do req
                if String(req.method) == "GET"
                    return HTTP.Response(
                        200,
                        ["Content-Type" => "application/json"],
                        String(JSON3.write(Dict("data" => Any[]))),
                    )
                end
                return HTTP.Response(
                    201,
                    ["Content-Type" => "application/json"],
                    String(
                        JSON3.write(
                            Dict(
                                "data" => Any[
                                    Dict(
                                        "id" => "created-201",
                                        "value" => rule_value,
                                        "tag" => cfg.rule_tag,
                                    ),
                                ],
                                "meta" => Dict(
                                    "summary" => Dict(
                                        "created" => 1,
                                        "not_created" => 0,
                                        "valid" => 1,
                                        "invalid" => 0,
                                    ),
                                ),
                            ),
                        ),
                    ),
                )
            end
            try
                cfg.api_base_url = "http://127.0.0.1:$(HTTP.port(server))"
                result = ensure_stream_rule!(
                    cfg,
                    ["Authorization" => "Bearer dummy"],
                    fetch_stream_json_with_retry,
                )
                @test result.created == true
                @test result.id == "created-201"
            finally
                close(server)
            end
        end
    end

    gzip_body = gzip_bytes("""{"title":"Payment Required","detail":"upgrade required"}""")
    gzip_resp = HTTP.Response(402, ["Content-Encoding" => "gzip"], gzip_body)
    gzip_text = _body_to_text(gzip_resp)
    @test occursin("Payment Required", gzip_text)
    @test occursin("upgrade required", gzip_text)
    gzip_err = try
        _stream_response_outcome(gzip_resp, "http://x.test"; body = gzip_text)
        nothing
    catch e
        e
    end
    @test gzip_err isa XPostCollector.XApiAccessError
    @test gzip_err.status == 402
    @test occursin("upgrade required", sprint(showerror, gzip_err))

    bad_resp = HTTP.Response(402, ["Content-Encoding" => "gzip"], UInt8[0x8b, 0xff, 0x00])
    bad_text = _body_to_text(bad_resp)
    @test isvalid(bad_text)
    @test occursin("non-UTF-8 response body", bad_text)
    bad_err = try
        _stream_response_outcome(bad_resp, "http://x.test"; body = bad_text)
        nothing
    catch e
        e
    end
    @test bad_err isa XPostCollector.XApiAccessError
    @test bad_err.status == 402

    @test _looks_like_stream_timeout(BinaryShowError()) == false
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

@testset "run_stream_collector retries 503 without response/outcome confusion" begin
    with_temp_stream_cfg(task = "stream_503_retry") do cfg, dir
        with_env_token() do
            with_logger(NullLogger()) do
                cfg.manage_rules = false
                cfg.max_posts = 1
                cfg.max_reconnects = 1
                attempts = Ref(0)
                server = HTTP.serve!(listenany = true) do req
                    if startswith(String(req.target), "/2/tweets/search/stream")
                        attempts[] += 1
                        if attempts[] == 1
                            return HTTP.Response(503)
                        end
                        return HTTP.Response(
                            200,
                            ["Content-Type" => "application/json"],
                            stream_event_json("after-503") * "\n",
                        )
                    end
                    return HTTP.Response(404)
                end
                try
                    cfg.api_base_url = "http://127.0.0.1:$(HTTP.port(server))"
                    st = run_stream_collector(
                        cfg;
                        sleep_fn = _ -> nothing,
                        rand_fn = () -> 0.0,
                    )
                    @test attempts[] == 2
                    @test st.total_tweets == 1
                    @test st.completed == true
                    @test st.stop_reason == "max_posts"
                    @test st.last_status == 200
                    gaps = readlines(out_stream_gaps(cfg))
                    @test length(gaps) == 1
                    @test JSON3.read(gaps[1])["reason"] == "server_error"
                finally
                    close(server)
                end
            end
        end
    end
end

@testset "run_stream_collector requests identity and handles gzip stream body" begin
    with_temp_stream_cfg(task = "stream_gzip_body") do cfg, dir
        with_env_token() do
            with_logger(NullLogger()) do
                cfg.manage_rules = false
                cfg.max_posts = 1
                seen_accept_encoding = Ref("")
                server = HTTP.serve!(listenany = true) do req
                    if startswith(String(req.target), "/2/tweets/search/stream")
                        seen_accept_encoding[] = HTTP.header(req, "Accept-Encoding")
                        return HTTP.Response(
                            200,
                            [
                                "Content-Type" => "application/json",
                                "Content-Encoding" => "gzip",
                            ],
                            gzip_bytes(stream_event_json("gzip-stream") * "\n"),
                        )
                    end
                    return HTTP.Response(404)
                end
                try
                    cfg.api_base_url = "http://127.0.0.1:$(HTTP.port(server))"
                    st = run_stream_collector(
                        cfg;
                        sleep_fn = _ -> nothing,
                        rand_fn = () -> 0.0,
                    )
                    @test seen_accept_encoding[] == "identity"
                    @test st.total_tweets == 1
                    @test st.completed == true
                    @test st.stop_reason == "max_posts"
                    lines = readlines(out_jsonl(cfg))
                    @test count(ln -> occursin("\"kind\":\"tweet\"", ln), lines) == 1
                    @test count(ln -> occursin("\"endpoint\":\"stream\"", ln), lines) == 1
                finally
                    close(server)
                end
            end
        end
    end
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

    with_temp_stream_cfg(task = "stream_empty_non_eof_after_pending") do cfg, dir
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
                        "id" => "302",
                        "created_at" => "2026-01-01T00:00:00Z",
                        "author_id" => "32",
                        "lang" => "ja",
                        "text" => "pending across empty non-eof",
                    )
                    event = String(JSON3.write(Dict("data" => tweet))) * "\n"
                    bytes = collect(codeunits(event))
                    split_at = max(1, length(bytes) ÷ 2)
                    stream = FakeReadavailableEofStream(
                        Vector{UInt8}[
                            bytes[1:split_at],
                            UInt8[],
                            UInt8[],
                            bytes[(split_at + 1):end],
                        ],
                    )
                    activity = Ref(false)
                    sleep_calls = Ref(0)
                    outcome = _read_stream_lines!(
                        cfg,
                        st,
                        sdb,
                        io,
                        stream,
                        time(),
                        activity;
                        sleep_fn = _ -> (sleep_calls[] += 1),
                    )
                    @test outcome.kind == :completed
                    @test outcome.stop_reason == "max_posts"
                    @test outcome.activity == true
                    @test activity[] == true
                    @test st.total_tweets == 1
                    @test stream.calls == 4
                    @test sleep_calls[] == 2
                end
            finally
                DBInterface.close!(sdb.db)
            end
            lines = readlines(out_jsonl(cfg))
            @test count(ln -> occursin("\"kind\":\"tweet\"", ln), lines) == 1
            @test count(ln -> occursin("\"endpoint\":\"stream\"", ln), lines) == 1
        end
    end

    with_temp_stream_cfg(task = "stream_empty_non_eof_max_seconds") do cfg, dir
        with_logger(NullLogger()) do
            cfg.max_seconds = 1
            st = StreamState()
            st.task_name = cfg.task_name
            st.query = build_query(cfg)
            st.rule_tag = cfg.rule_tag
            sdb = init_seen_db(cfg)
            try
                open(out_jsonl(cfg), "w") do io
                    stream = FakeEmptyNonEofStream()
                    activity = Ref(false)
                    sleep_calls = Ref(0)
                    outcome = _read_stream_lines!(
                        cfg,
                        st,
                        sdb,
                        io,
                        stream,
                        time() - 2,
                        activity;
                        sleep_fn = _ -> (sleep_calls[] += 1),
                    )
                    @test outcome.kind == :completed
                    @test outcome.stop_reason == "max_seconds"
                    @test outcome.activity == false
                    @test st.completed == true
                    @test st.stop_reason == "max_seconds"
                    @test stream.calls == 1
                    @test sleep_calls[] == 0
                end
            finally
                DBInterface.close!(sdb.db)
            end
            @test readlines(out_jsonl(cfg)) == String[]
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
