# =========================================================
# 収集本体
# =========================================================
build_search_params(
    cfg::SearchConfig,
    final_query::String,
    start_utc,
    end_utc,
    endpoint::Symbol,
) = begin
    maxr =
        endpoint === :all ? clamp(cfg.max_results, 10, MAX_RESULTS_ALL) :
        clamp(cfg.max_results, 10, MAX_RESULTS_RECENT)
    params = Dict{String,String}(
        "query" => final_query,
        "max_results" => string(maxr),
        "end_time" => end_utc,
    )
    start_utc !== nothing && (params["start_time"] = start_utc)
    for (k, v) in API_FIELDS
        params[k] = v
    end
    return params
end

function run_collector(cfg::SearchConfig; client::XApiClient = XApiClient())
    validate!(cfg)
    mkpath(cfg.out_dir)

    headers = bearer_headers(client; user_agent = "julia-x-collector/repl")

    st = load_state(out_state(cfg))
    prev_conv_offset = st === nothing ? 0 : st.converted_jsonl_offset
    prev_conv_at = st === nothing ? "" : st.converted_at

    final_query = build_query(cfg)

    endpoint = choose_endpoint(cfg)
    url = endpoint_url(cfg, endpoint)

    tw = resolve_time_window(cfg, st, final_query; endpoint = endpoint)
    start_utc, end_utc = tw.start_utc, tw.end_utc

    resume_next = nothing
    if st !== nothing
        same_window =
            (st.query == final_query) &&
            (st.start_time_utc == start_utc) &&
            (st.end_time_utc == end_utc)

        if cfg.skip_if_completed_same_window &&
           same_window &&
           st.completed &&
           st.next_token === nothing
            @info "Already completed for same window; skip" task = cfg.task_name
            return st
        end

        if same_window && !st.completed && st.next_token !== nothing
            resume_next = st.next_token
        elseif same_window && !st.completed && st.next_token === nothing
            @warn "State incomplete but next_token missing; starting fresh for same window" task =
                cfg.task_name
            st = nothing
        elseif !same_window && !cfg.force_resume_on_mismatch
            if cfg.use_state_highwater_start && st.completed && st.query == final_query
                @info "New window; starting fresh (high-water enabled)" task = cfg.task_name
            else
                @warn "State mismatch; starting fresh (avoid mixing)" task = cfg.task_name
            end
            st = nothing
        end
    end

    params_base = build_search_params(cfg, final_query, start_utc, end_utc, endpoint)

    st2 = st === nothing ? CollectorState() : st
    if st === nothing
        st2.converted_jsonl_offset = prev_conv_offset
        st2.converted_at = prev_conv_at
    end
    st2.task_name = cfg.task_name
    st2.query = final_query
    st2.start_time_utc = start_utc
    st2.end_time_utc = end_utc
    st2.next_token = resume_next
    st2.completed = false

    sdb = init_seen_db(cfg)
    last_all_req_time = Ref{Float64}(0.0)
    stop_reason = :none

    function maybe_throttle_all()
        endpoint !== :all && return
        cfg.all_min_interval_seconds <= 0 && return
        t = time()
        last = last_all_req_time[]
        if last > 0
            dt = t - last
            if dt < cfg.all_min_interval_seconds
                sleep(cfg.all_min_interval_seconds - dt)
            end
        end
        last_all_req_time[] = time()
    end

    function persist_state!()
        st2.timestamp = string(Dates.now(Dates.UTC))
        save_state(out_state(cfg), st2)
        return nothing
    end

    try
        open(out_jsonl(cfg), "a") do io
            next_token = resume_next

            while true
                st2.page_count += 1
                params = copy(params_base)
                next_token !== nothing && (params["next_token"] = next_token)

                maybe_throttle_all()

                @info "Fetch page" endpoint = String(endpoint) page = st2.page_count tweets =
                    st2.total_tweets

                res = try
                    fetch_with_retry(client, url, headers, params)
                catch e
                    if e isa InvalidPaginationTokenError
                        @warn "Invalid pagination token; stopping gracefully" exception = e
                        stop_reason = :invalid_pagination_token
                        st2.next_token = nothing
                        st2.completed = true
                        persist_state!()
                        break
                    elseif e isa FullArchiveAccessError
                        msg = "Full-archive search requires Self-serve or Enterprise access. Set search_mode=:recent, narrow the time window to the last 7 days, or upgrade API access."
                        throw(
                            ErrorException(
                                msg * " Original error: " * sprint(showerror, e),
                            ),
                        )
                    elseif e isa XApiAccessError
                        throw(ErrorException(sprint(showerror, e)))
                    end
                    rethrow()
                end

                page_new = 0
                if _json_haskey(res, "data")
                    DBInterface.execute(sdb.db, "BEGIN;")
                    try
                        seen_at = utc_now_z(lag_seconds = 0)
                        for tw in _json_items(_json_get(res, "data", nothing))
                            tid = safe_str(_json_get(tw, "id", nothing); default = "")
                            isempty(tid) && continue
                            if mark_seen!(sdb, tid, seen_at)
                                write_jsonl(io, "tweet", tw)
                                st2.total_tweets += 1
                                page_new += 1
                            end
                        end
                        DBInterface.execute(sdb.db, "COMMIT;")
                    catch e
                        try
                            DBInterface.execute(sdb.db, "ROLLBACK;")
                        catch
                        end
                        rethrow(e)
                    end
                end

                if cfg.write_includes && page_new > 0 && _json_haskey(res, "includes")
                    inc = _json_get(res, "includes", nothing)
                    for (k, arr) in _json_pairs(inc)
                        kstr = String(k)
                        for obj in _json_items(arr)
                            write_jsonl(io, "include:$kstr", obj)
                            st2.total_includes += 1
                        end
                    end
                end

                meta = _json_get(res, "meta", nothing)
                write_jsonl(
                    io,
                    "page",
                    Dict(
                        "page" => st2.page_count,
                        "endpoint" => String(endpoint),
                        "meta" => (meta === nothing ? Dict{String,Any}() : meta),
                    ),
                )
                flush(io)

                next_raw = _json_get(meta, "next_token", nothing)
                next_token = isnull(next_raw) ? nothing : safe_str(next_raw; default = "")

                st2.next_token = next_token
                st2.completed = (next_token === nothing)
                persist_state!()

                @info "Page done" page = st2.page_count new_tweets = page_new total =
                    st2.total_tweets

                if cfg.usage_check_every_pages > 0
                    try
                        usage = maybe_check_usage(cfg, client, headers, st2)
                        if usage !== nothing &&
                           cfg.drain_to_cap &&
                           !ismissing(usage.remaining) &&
                           usage.remaining <= 0
                            stop_reason = :usage_cap_reached
                            @warn "Stopping because usage cap appears exhausted" project_usage =
                                usage.project_usage project_cap = usage.project_cap
                            st2.next_token = nothing
                            st2.completed = true
                            persist_state!()
                            break
                        end
                    catch e
                        @warn "Usage check failed; continuing without it" exception = e
                    end
                end

                if cfg.target_posts > 0 && st2.total_tweets >= cfg.target_posts
                    stop_reason = :target_posts
                    @info "Stop by target_posts (page boundary)" target_posts =
                        cfg.target_posts total = st2.total_tweets
                    break
                end

                st2.completed && break
            end
        end

        if stop_reason == :none
            @info "Collection complete" total_tweets = st2.total_tweets pages =
                st2.page_count
        end
        return st2
    finally
        try
            DBInterface.close!(sdb.db)
        catch
        end
    end
end
