# =========================================================
# TimeZones: ローカル時刻文字列 -> UTC Z
# =========================================================
dt_to_utc_z(dt::DateTime) = Dates.format(dt, DT_FMT_ISO) * "Z"
parse_utc_z(s::AbstractString) = DateTime(replace(String(s), "Z" => ""), DT_FMT_ISO)

function local_str_to_utc_z(cfg::SearchConfig, s::AbstractString)::Union{Nothing,String}
    t = strip(String(s))
    isempty(t) && return nothing
    tz_local = TimeZone(cfg.local_tz_name)
    dt_local = DateTime(t, DT_FMT_ISO)
    zdt_local = ZonedDateTime(dt_local, tz_local)
    zdt_utc = astimezone(zdt_local, TZ_UTC)
    dt_utc = DateTime(zdt_utc, TimeZones.UTC)
    return dt_to_utc_z(dt_utc)
end

utc_now_dt() = DateTime(now(TZ_UTC), TimeZones.UTC)

function utc_now_z(; lag_seconds::Int = 0)
    dt = utc_now_dt() - Second(lag_seconds)
    return dt_to_utc_z(dt)
end

# =========================================================
# クエリビルド（OR 検索 + repost除外）
# =========================================================
function quote_term(t::AbstractString)
    s = strip(String(t))
    isempty(s) && return ""
    if startswith(s, "\"") && endswith(s, "\"")
        return s
    end
    occursin(r"\s", s) ? "\"$(replace(s, "\""=>"\\\""))\"" : s
end

function _build_query(
    keywords_or::Vector{String},
    extra_query_tail::AbstractString,
    exclude_reposts::Bool,
)::String
    terms = filter(!isempty, quote_term.(keywords_or))
    isempty(terms) && error("no valid keyword")
    base = "(" * join(terms, " OR ") * ")"
    tail = strip(String(extra_query_tail))
    q = isempty(tail) ? base : "$base $tail"
    exclude_reposts ? "$q -is:retweet" : q
end

build_query(cfg::SearchConfig)::String =
    _build_query(cfg.keywords_or, cfg.extra_query_tail, cfg.exclude_reposts)
build_query(cfg::StreamConfig)::String =
    _build_query(cfg.keywords_or, cfg.extra_query_tail, cfg.exclude_reposts)

# =========================================================
# Endpoint selection
# =========================================================
function _candidate_end_dt(cfg::SearchConfig)
    now_dt = utc_now_dt()
    limit_end = now_dt - Second(cfg.now_lag_seconds)
    end_utc = local_str_to_utc_z(cfg, cfg.end_time_jst)
    end_dt = end_utc === nothing ? limit_end : parse_utc_z(end_utc)
    end_dt > limit_end && (end_dt = limit_end)
    return end_dt
end

function choose_endpoint(cfg::SearchConfig)::Symbol
    cfg.search_mode === :recent && return :recent
    cfg.search_mode === :all && return :all
    # :auto
    end_dt = _candidate_end_dt(cfg)
    now_dt = utc_now_dt()
    return (end_dt < (now_dt - MAX_LOOKBACK_RECENT)) ? :all : :recent
end

# =========================================================
# 時間窓決定（explicit / high-water / window）+ endpoint制約
# =========================================================
function resolve_time_window(
    cfg::SearchConfig,
    st::Union{Nothing,CollectorState},
    final_query::String;
    endpoint::Symbol = :recent,
)
    now_dt = utc_now_dt()
    limit_end = now_dt - Second(cfg.now_lag_seconds)

    end_utc = begin
        v = local_str_to_utc_z(cfg, cfg.end_time_jst)
        v === nothing ? dt_to_utc_z(limit_end) : v
    end
    end_dt = parse_utc_z(end_utc)

    if end_dt > limit_end
        @warn "end_time is in the future; clipped" old = end_dt new = limit_end
        end_dt = limit_end
        end_utc = dt_to_utc_z(end_dt)
    end

    # recent のみ 7日制約
    if endpoint === :recent
        limit_start = now_dt - MAX_LOOKBACK_RECENT
        if end_dt < limit_start
            error(
                "end_time is older than recent-search lookback (7 days). end=$end_utc limit_start=$(dt_to_utc_z(limit_start))",
            )
        end
    end

    start_utc = local_str_to_utc_z(cfg, cfg.start_time_jst)

    # high-water
    if start_utc === nothing && cfg.use_state_highwater_start && st !== nothing
        prev_end_utc = st.end_time_utc
        if st.completed && st.query == final_query && prev_end_utc !== nothing
            prev_end_dt = parse_utc_z(prev_end_utc)
            start_dt = prev_end_dt - Second(max(cfg.highwater_overlap_seconds, 0))
            start_dt < end_dt && (start_utc = dt_to_utc_z(start_dt))
        end
    end

    # window fallback
    window_hours = cfg.window_hours
    if start_utc === nothing && window_hours !== nothing
        start_utc = dt_to_utc_z(end_dt - Hour(window_hours))
    end

    # recent のみ start を 7日内にクリップ
    if start_utc !== nothing && endpoint === :recent
        limit_start = now_dt - MAX_LOOKBACK_RECENT
        start_dt = parse_utc_z(start_utc)
        if start_dt < limit_start
            @warn "start_time exceeds 7-day lookback; clipped" old = start_dt new =
                limit_start
            start_dt = limit_start
            start_utc = dt_to_utc_z(start_dt)
        end
        start_dt < end_dt || error("start_time >= end_time")
    elseif start_utc !== nothing
        # all の場合は未来/逆転のみ弾く
        start_dt = parse_utc_z(start_utc)
        start_dt < end_dt || error("start_time >= end_time")
    end

    return (start_utc = start_utc, end_utc = end_utc)
end
