function run_stream_collector(cfg::StreamConfig)
    validate!(cfg)
    mkpath(cfg.out_dir)

    headers = bearer_headers(user_agent = "julia-x-collector/stream")
    rule = ensure_stream_rule!(cfg, headers)
    query = build_query(cfg)
    sdb = init_seen_db(cfg)

    st = load_stream_state(out_stream_state(cfg))
    if st === nothing || st.query != query || st.rule_tag != cfg.rule_tag
        st = StreamState()
        st.task_name = cfg.task_name
        st.query = query
        st.rule_tag = cfg.rule_tag
    end
    st.rule_id = rule.id
    st.completed = false
    st.stop_reason = ""
    st.last_error = ""
    st.last_status = 0
    st.consecutive_failures = 0
    st.seen_count = count_seen(sdb)
    _persist_stream_state!(cfg, st)
    state_flush_ref = Ref(time())

    params = build_stream_params(cfg)
    url = stream_url(cfg)
    started_at = time()
    reconnects = 0
    delay = cfg.reconnect_initial_delay_seconds
    pending_gap_start = Ref{Union{Nothing,String}}(nothing)
    pending_gap_reason = Ref("")
    writer = Ref{Union{Nothing,StreamJsonlWriter}}(nothing)

    try
        writer[] = _open_stream_jsonl_writer(cfg)
        while true
            stop_reason = _stream_stop_reason(cfg, st, started_at)
            if stop_reason !== nothing
                st.completed = true
                st.stop_reason = stop_reason
                _finish_stream_gap!(
                    cfg,
                    st,
                    pending_gap_start,
                    pending_gap_reason,
                    stream_now_utc(),
                    stop_reason,
                )
                _persist_stream_state!(cfg, st)
                state_flush_ref[] = time()
                break
            end

            st.connection_count += 1
            _persist_stream_state!(cfg, st)
            state_flush_ref[] = time()
            @info "Connect stream" connection = st.connection_count tweets =
                st.total_tweets

            activity_this_connection = Ref(false)
            try
                outcome = HTTP.open(
                    :GET,
                    url,
                    headers;
                    query = params,
                    status_exception = false,
                    readtimeout = cfg.idle_timeout_seconds,
                ) do http
                    resp = HTTP.startread(http)
                    if resp.status != 200
                        body =
                            resp.status == 429 || 500 <= resp.status < 600 ? "" :
                            _stream_response_body(resp, http)
                        status_outcome =
                            _stream_response_outcome(resp, url; body = body)
                        if status_outcome.stop_reason == "rate_limited"
                            @warn "Stream rate limited" sleep_seconds =
                                status_outcome.sleep_seconds
                        elseif status_outcome.stop_reason == "server_error"
                            @warn "Stream server error" status = resp.status
                        end
                        return status_outcome
                    end

                    connected_at = stream_now_utc()
                    st.last_connected_at = connected_at
                    st.last_status = 200
                    st.last_error = ""
                    _finish_stream_gap!(
                        cfg,
                        st,
                        pending_gap_start,
                        pending_gap_reason,
                        connected_at,
                    )
                    _maybe_persist_stream_state!(
                        cfg,
                        st,
                        state_flush_ref;
                        force = true,
                    )

                    return _read_stream_lines!(
                        cfg,
                        st,
                        sdb,
                        writer[]::StreamJsonlWriter,
                        http,
                        started_at,
                        activity_this_connection,
                        state_flush_ref,
                    )
                end

                if st.completed
                    break
                end

                if activity_this_connection[]
                    reconnects = 0
                    delay = cfg.reconnect_initial_delay_seconds
                    st.consecutive_failures = 0
                end

                if !isempty(outcome.stop_reason)
                    st.stop_reason = outcome.stop_reason
                    st.last_status = outcome.status
                    st.last_error = outcome.error
                    if outcome.kind in (:retryable, :disconnected)
                        _begin_stream_gap!(
                            st,
                            pending_gap_start,
                            pending_gap_reason,
                            outcome.stop_reason,
                        )
                    end
                    _persist_stream_state!(cfg, st)
                    state_flush_ref[] = time()
                end

                decision = _stream_reconnect_decision(cfg, reconnects)
                reconnects = decision.reconnects
                st.consecutive_failures = reconnects
                if !decision.should_reconnect
                    st.stop_reason =
                        isempty(decision.stop_reason) ? outcome.stop_reason :
                        decision.stop_reason
                    _finish_stream_gap!(
                        cfg,
                        st,
                        pending_gap_start,
                        pending_gap_reason,
                        stream_now_utc(),
                        st.stop_reason,
                    )
                    _persist_stream_state!(cfg, st)
                    state_flush_ref[] = time()
                    break
                end
                _persist_stream_state!(cfg, st)
                state_flush_ref[] = time()
                sleep_seconds =
                    outcome.sleep_seconds > 0 ? outcome.sleep_seconds :
                    delay + rand() * 0.5
                sleep(sleep_seconds)
                delay = min(delay * 1.8, cfg.reconnect_max_delay_seconds)

            catch e
                if e isa InterruptException
                    st.stop_reason = "interrupted"
                    _finish_stream_gap!(
                        cfg,
                        st,
                        pending_gap_start,
                        pending_gap_reason,
                        stream_now_utc(),
                        "interrupted",
                    )
                    _persist_stream_state!(cfg, st)
                    rethrow()
                end
                api_error = _unwrap_x_api_access_error(e)
                if api_error !== nothing
                    st.stop_reason = "api_error"
                    st.last_status = api_error.status
                    st.last_error = _safe_showerror_text(api_error)
                    _finish_stream_gap!(
                        cfg,
                        st,
                        pending_gap_start,
                        pending_gap_reason,
                        stream_now_utc(),
                        "api_error",
                    )
                    _persist_stream_state!(cfg, st)
                    throw(api_error)
                end

                stop_reason = _stream_stop_reason(cfg, st, started_at)
                if stop_reason !== nothing
                    st.completed = true
                    st.stop_reason = stop_reason
                    _finish_stream_gap!(
                        cfg,
                        st,
                        pending_gap_start,
                        pending_gap_reason,
                        stream_now_utc(),
                        stop_reason,
                    )
                    _persist_stream_state!(cfg, st)
                    state_flush_ref[] = time()
                    break
                end

                if activity_this_connection[]
                    reconnects = 0
                    delay = cfg.reconnect_initial_delay_seconds
                    st.consecutive_failures = 0
                end

                error_text = _safe_showerror_text(e)
                reason = _looks_like_stream_timeout(e) ? "idle_timeout" : "stream_error"
                st.stop_reason = reason
                st.last_status = 0
                st.last_error = error_text
                _begin_stream_gap!(
                    st,
                    pending_gap_start,
                    pending_gap_reason,
                    reason,
                )

                if _looks_like_stream_timeout(e) && !cfg.reconnect
                    st.completed = true
                    st.stop_reason = "idle_timeout"
                    _finish_stream_gap!(
                        cfg,
                        st,
                        pending_gap_start,
                        pending_gap_reason,
                        stream_now_utc(),
                        "idle_timeout",
                    )
                    _persist_stream_state!(cfg, st)
                    state_flush_ref[] = time()
                    break
                end

                @warn "Stream connection failed" exception = e reconnects = reconnects
                decision = _stream_reconnect_decision(cfg, reconnects)
                reconnects = decision.reconnects
                st.consecutive_failures = reconnects
                if !decision.should_reconnect
                    st.stop_reason =
                        isempty(decision.stop_reason) ? "stream_error" :
                        decision.stop_reason
                    _finish_stream_gap!(
                        cfg,
                        st,
                        pending_gap_start,
                        pending_gap_reason,
                        stream_now_utc(),
                        st.stop_reason,
                    )
                    _persist_stream_state!(cfg, st)
                    state_flush_ref[] = time()
                    rethrow()
                end
                _persist_stream_state!(cfg, st)
                state_flush_ref[] = time()
                sleep(delay + rand() * 0.5)
                delay = min(delay * 1.8, cfg.reconnect_max_delay_seconds)
            end
        end

        if !st.completed && isempty(st.stop_reason)
            st.stop_reason = "disconnected"
            _finish_stream_gap!(
                cfg,
                st,
                pending_gap_start,
                pending_gap_reason,
                stream_now_utc(),
                "disconnected",
            )
            _persist_stream_state!(cfg, st)
        end
        return st
    finally
        try
            writer[] !== nothing && _close_stream_jsonl_writer!(writer[]::StreamJsonlWriter)
        catch
        end
        try
            DBInterface.close!(sdb.db)
        catch
        end
    end
end
