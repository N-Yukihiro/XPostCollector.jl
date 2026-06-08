function _open_stream_connection!(
    cfg::StreamConfig,
    st::StreamState,
    sdb::SeenDB,
    writer::StreamJsonlWriter,
    url::AbstractString,
    headers,
    params::Dict{String,String},
    started_at::Float64,
    pending_gap::Base.RefValue{StreamGapSnapshot},
    state_flush_ref::Base.RefValue{Float64},
    activity_this_connection::Base.RefValue{Bool};
    open_http = HTTP.open,
    sleep_fn::Function = sleep,
)::StreamOutcome
    outcome_ref = Ref{Union{Nothing,StreamOutcome}}(nothing)
    open_http(
        :GET,
        url,
        headers;
        query = params,
        status_exception = false,
        readtimeout = _stream_readtimeout_seconds(cfg),
        decompress = false,
    ) do http
        resp = HTTP.startread(http)
        if resp.status != 200
            body = _stream_response_body(resp, http)
            status_outcome = _stream_response_outcome(resp, url; body = body)
            _record_stream_outcome_diagnostics!(st, status_outcome)
            if status_outcome.stop_reason in ("rate_limited", "usage_capped")
                @warn "Stream rate limited" reason = status_outcome.stop_reason sleep_seconds =
                    status_outcome.sleep_seconds retry_after = status_outcome.retry_after rate_limit_reset =
                    status_outcome.rate_limit_reset
            elseif status_outcome.stop_reason == "server_error"
                @warn "Stream server error" status = resp.status detail =
                    status_outcome.error_detail
            end
            outcome_ref[] = status_outcome
            return nothing
        end

        connected_at = stream_now_utc()
        _finish_stream_gap!(cfg, st, pending_gap, connected_at)
        st.last_connected_at = connected_at
        st.last_status = 200
        st.last_error = ""
        st.last_error_type = ""
        st.last_error_detail = ""
        _maybe_persist_stream_state!(
            cfg,
            st,
            state_flush_ref;
            force = true,
        )

        outcome_ref[] = _read_stream_response_lines!(
            cfg,
            st,
            sdb,
            writer,
            resp,
            http,
            started_at,
            activity_this_connection,
            state_flush_ref,
            sleep_fn = sleep_fn,
        )
        return nothing
    end

    outcome = outcome_ref[]
    outcome === nothing && error("Stream connection closed before an outcome was recorded")
    return outcome
end

function run_stream_collector(
    cfg::StreamConfig;
    open_http = HTTP.open,
    sleep_fn = sleep,
    rand_fn = rand,
)
    validate!(cfg)
    mkpath(cfg.out_dir)

    rule_headers = bearer_headers(user_agent = "julia-x-collector/stream")
    stream_headers = bearer_headers(
        user_agent = "julia-x-collector/stream",
        accept_encoding = "identity",
    )
    rule = ensure_stream_rule!(cfg, rule_headers)
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
    pending_gap = Ref(StreamGapSnapshot())
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
                    pending_gap,
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
                outcome = _open_stream_connection!(
                    cfg,
                    st,
                    sdb,
                    writer[]::StreamJsonlWriter,
                    url,
                    stream_headers,
                    params,
                    started_at,
                    pending_gap,
                    state_flush_ref,
                    activity_this_connection;
                    open_http = open_http,
                    sleep_fn = sleep_fn,
                )

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
                    _record_stream_outcome_diagnostics!(st, outcome)
                    if outcome.kind in (:retryable, :disconnected)
                        _begin_stream_gap!(
                            st,
                            pending_gap,
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
                        pending_gap,
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
                    delay + rand_fn() * 0.5
                sleep_fn(sleep_seconds)
                delay = min(delay * 1.8, cfg.reconnect_max_delay_seconds)

            catch e
                if e isa InterruptException
                    st.stop_reason = "interrupted"
                    _finish_stream_gap!(
                        cfg,
                        st,
                        pending_gap,
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
                    st.last_error_type = "api_error"
                    st.last_error_detail = api_error.body
                    _finish_stream_gap!(
                        cfg,
                        st,
                        pending_gap,
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
                        pending_gap,
                        stream_now_utc(),
                        stop_reason,
                    )
                    _persist_stream_state!(cfg, st)
                    state_flush_ref[] = time()
                    break
                end

                if activity_this_connection[] && !_looks_like_stream_timeout(e)
                    reconnects = 0
                    delay = cfg.reconnect_initial_delay_seconds
                    st.consecutive_failures = 0
                end

                error_text = _safe_showerror_text(e)
                reason = _looks_like_stream_timeout(e) ? "idle_timeout" : "stream_error"
                st.stop_reason = reason
                st.last_status = 0
                st.last_error = error_text
                st.last_error_type = reason
                st.last_error_detail = error_text
                st.last_rate_limit_limit = 0
                st.last_rate_limit_remaining = 0
                st.last_rate_limit_reset = 0
                st.last_retry_after = 0
                _begin_stream_gap!(
                    st,
                    pending_gap,
                    reason,
                )

                if _looks_like_stream_timeout(e) && !cfg.reconnect
                    st.completed = true
                    st.stop_reason = "idle_timeout"
                    _finish_stream_gap!(
                        cfg,
                        st,
                        pending_gap,
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
                        pending_gap,
                        stream_now_utc(),
                        st.stop_reason,
                    )
                    _persist_stream_state!(cfg, st)
                    state_flush_ref[] = time()
                    rethrow()
                end
                _persist_stream_state!(cfg, st)
                state_flush_ref[] = time()
                sleep_fn(delay + rand_fn() * 0.5)
                delay = min(delay * 1.8, cfg.reconnect_max_delay_seconds)
            end
        end

        if !st.completed && isempty(st.stop_reason)
            st.stop_reason = "disconnected"
            _finish_stream_gap!(
                cfg,
                st,
                pending_gap,
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
