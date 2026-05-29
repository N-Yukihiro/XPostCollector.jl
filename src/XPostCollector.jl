module XPostCollector

export SearchConfig,
    StreamConfig,
    CollectorState,
    StreamState,
    validate!,
    run_collector,
    run_stream_collector,
    list_stream_rules,
    ensure_stream_rule!,
    convert_outputs,
    convert_outputs_wide

using HTTP
using JSON3
using Dates
using Random
using TimeZones
using Logging

using SQLite
using DBInterface

using Arrow
using StructTypes
using CSV

# =========================================================
# API endpoints
# =========================================================
const API_BASE_URL_DEFAULT = "https://api.x.com"
const PATH_RECENT = "/2/tweets/search/recent"
const PATH_ALL = "/2/tweets/search/all"
const PATH_STREAM = "/2/tweets/search/stream"
const PATH_STREAM_RULES = "/2/tweets/search/stream/rules"
const PATH_USAGE = "/2/usage/tweets"

normalize_api_base_url(url::AbstractString) = rstrip(strip(String(url)), '/')
api_url(base_url::AbstractString, path::AbstractString) =
    normalize_api_base_url(base_url) * "/" * lstrip(String(path), '/')

const URL_RECENT = api_url(API_BASE_URL_DEFAULT, PATH_RECENT)
const URL_ALL = api_url(API_BASE_URL_DEFAULT, PATH_ALL)
const URL_STREAM = api_url(API_BASE_URL_DEFAULT, PATH_STREAM)
const URL_STREAM_RULES = api_url(API_BASE_URL_DEFAULT, PATH_STREAM_RULES)

# usage endpoint はプラン/仕様で変わり得るので「設定で差し替え前提」にする
const URL_USAGE_DEFAULT = api_url(API_BASE_URL_DEFAULT, PATH_USAGE)

endpoint_url(endpoint::Symbol) = endpoint === :all ? URL_ALL : URL_RECENT
endpoint_path(endpoint::Symbol) = endpoint === :all ? PATH_ALL : PATH_RECENT
endpoint_url(cfg, endpoint::Symbol) = api_url(cfg.api_base_url, endpoint_path(endpoint))
stream_url(cfg) = api_url(cfg.api_base_url, PATH_STREAM)
stream_rules_url(cfg) = api_url(cfg.api_base_url, PATH_STREAM_RULES)

# =========================================================
# Time / format
# =========================================================
const DT_FMT_ISO = dateformat"yyyy-mm-dd\THH:MM:SS"
const LOG_TS_FMT = dateformat"yyyy-mm-dd\THH:MM:SS"
const TZ_UTC = tz"UTC"
const MAX_LOOKBACK_RECENT = Day(7)

# =========================================================
# API Fields
# =========================================================
const API_FIELDS = Dict{String,String}(
    "tweet.fields" => join(
        [
            "id",
            "text",
            "created_at",
            "lang",
            "source",
            "possibly_sensitive",
            "author_id",
            "conversation_id",
            "in_reply_to_user_id",
            "referenced_tweets",
            "reply_settings",
            "attachments",
            "entities",
            "context_annotations",
            "withheld",
            "public_metrics",
            "geo",
            "edit_history_tweet_ids",
            "edit_controls",
        ],
        ",",
    ),
    "expansions" => join(
        [
            "author_id",
            "referenced_tweets.id",
            "referenced_tweets.id.author_id",
            "entities.mentions.username",
            "attachments.media_keys",
            "geo.place_id",
        ],
        ",",
    ),
    "user.fields" => join(
        [
            "id",
            "name",
            "username",
            "created_at",
            "description",
            "location",
            "verified",
            "protected",
            "url",
            "entities",
            "profile_image_url",
            "public_metrics",
        ],
        ",",
    ),
    "media.fields" => join(
        [
            "media_key",
            "type",
            "url",
            "preview_image_url",
            "alt_text",
            "width",
            "height",
            "public_metrics",
            "duration_ms",
            "variants",
        ],
        ",",
    ),
    "place.fields" => join(
        [
            "id",
            "full_name",
            "name",
            "country",
            "country_code",
            "place_type",
            "contained_within",
            "geo",
        ],
        ",",
    ),
)

# =========================================================
# Limits
# =========================================================
const MAX_RESULTS_RECENT = 100
# v2 full archive は 500 のことが多い（プランで差があるので定数化）
const MAX_RESULTS_ALL = 500

# =========================================================
# 設定（REPL微調整しやすいよう mutable）
# =========================================================
Base.@kwdef mutable struct SearchConfig
    task_name::String
    keywords_or::Vector{String}

    extra_query_tail::String = ""
    exclude_reposts::Bool = true

    start_time_jst::String = ""
    end_time_jst::String = ""
    local_tz_name::String = "Asia/Tokyo"

    # :recent / :all / :auto
    search_mode::Symbol = :auto

    # 高水位再開
    use_state_highwater_start::Bool = true
    highwater_overlap_seconds::Int = 30
    window_hours::Union{Nothing,Int} = 24
    now_lag_seconds::Int = 45

    # 収集停止条件（ページ境界で停止）
    target_posts::Int = 0  # 0 => 無制限

    # drain（capを使い切る等）は将来用。デフォルトはOFF。
    drain_to_cap::Bool = false
    usage_url::String = URL_USAGE_DEFAULT
    usage_check_every_pages::Int = 5  # 0 => 無効化（validate! で許容）
    usage_days::Int = 7
    usage_fields::String = "project_cap,project_usage,cap_reset_day"

    # リクエストパラメータ
    api_base_url::String = API_BASE_URL_DEFAULT
    max_results::Int = 100
    write_includes::Bool = true
    force_resume_on_mismatch::Bool = false
    skip_if_completed_same_window::Bool = true

    # full archive のクライアント側スロットル
    all_min_interval_seconds::Float64 = 1.0

    out_dir::String = "."
    log_dir::String = "logs"

    db_path::String = ""  # 空なら out_dir/task_name.seen.sqlite

    convert_incremental::Bool = true
    convert_batch_size::Int = 50_000

    emit_csv::Bool = true
    emit_arrow::Bool = false
    arrow_path::String = ""
    arrow_append::Bool = true
end

Base.@kwdef mutable struct StreamConfig
    task_name::String
    keywords_or::Vector{String}

    extra_query_tail::String = ""
    exclude_reposts::Bool = true

    api_base_url::String = API_BASE_URL_DEFAULT
    rule_tag::String = ""
    manage_rules::Bool = true
    replace_rule_by_tag::Bool = false

    max_posts::Int = 0
    max_seconds::Float64 = 0.0
    idle_timeout_seconds::Int = 30

    reconnect::Bool = true
    max_reconnects::Int = 0
    reconnect_initial_delay_seconds::Float64 = 5.0
    reconnect_max_delay_seconds::Float64 = 60.0

    write_includes::Bool = true
    durable_writes::Bool = false
    rotate_jsonl_bytes::Int = 0
    rotate_jsonl_seconds::Float64 = 0.0
    state_flush_interval_seconds::Float64 = 30.0

    out_dir::String = "."
    log_dir::String = "logs"
    db_path::String = ""

    convert_incremental::Bool = true
    convert_batch_size::Int = 50_000

    emit_csv::Bool = true
    emit_arrow::Bool = false
    arrow_path::String = ""
    arrow_append::Bool = true
end

out_jsonl(cfg::SearchConfig) = joinpath(cfg.out_dir, "$(cfg.task_name).jsonl")
out_state(cfg::SearchConfig) = joinpath(cfg.out_dir, "$(cfg.task_name).state.json")
out_csv(cfg::SearchConfig) = joinpath(cfg.out_dir, "$(cfg.task_name).csv")
out_arrow(cfg::SearchConfig) =
    isempty(cfg.arrow_path) ? joinpath(cfg.out_dir, "$(cfg.task_name).arrow") :
    cfg.arrow_path
out_db(cfg::SearchConfig) =
    isempty(cfg.db_path) ? joinpath(cfg.out_dir, "$(cfg.task_name).seen.sqlite") :
    cfg.db_path
out_log(cfg::SearchConfig) = joinpath(cfg.log_dir, "$(cfg.task_name).log")

out_jsonl(cfg::StreamConfig) = joinpath(cfg.out_dir, "$(cfg.task_name).jsonl")
out_stream_state(cfg::StreamConfig) = joinpath(cfg.out_dir, "$(cfg.task_name).stream.state.json")
out_stream_gaps(cfg::StreamConfig) = joinpath(cfg.out_dir, "$(cfg.task_name).stream.gaps.jsonl")
out_csv(cfg::StreamConfig) = joinpath(cfg.out_dir, "$(cfg.task_name).csv")
out_arrow(cfg::StreamConfig) =
    isempty(cfg.arrow_path) ? joinpath(cfg.out_dir, "$(cfg.task_name).arrow") :
    cfg.arrow_path
out_db(cfg::StreamConfig) =
    isempty(cfg.db_path) ? joinpath(cfg.out_dir, "$(cfg.task_name).seen.sqlite") :
    cfg.db_path
out_log(cfg::StreamConfig) = joinpath(cfg.log_dir, "$(cfg.task_name).log")

function as_search_config(cfg::StreamConfig)
    return SearchConfig(
        task_name = cfg.task_name,
        keywords_or = copy(cfg.keywords_or),
        extra_query_tail = cfg.extra_query_tail,
        exclude_reposts = cfg.exclude_reposts,
        api_base_url = cfg.api_base_url,
        out_dir = cfg.out_dir,
        log_dir = cfg.log_dir,
        db_path = cfg.db_path,
        convert_incremental = cfg.convert_incremental,
        convert_batch_size = cfg.convert_batch_size,
        emit_csv = cfg.emit_csv,
        emit_arrow = cfg.emit_arrow,
        arrow_path = cfg.arrow_path,
        arrow_append = cfg.arrow_append,
    )
end

function validate!(cfg::SearchConfig)
    isempty(cfg.task_name) && error("task_name is empty")
    isempty(cfg.keywords_or) && error("keywords_or is empty")

    cfg.search_mode in (:auto, :recent, :all) ||
        error("invalid search_mode: $(cfg.search_mode)")

    cfg.highwater_overlap_seconds = max(cfg.highwater_overlap_seconds, 0)
    cfg.now_lag_seconds = max(cfg.now_lag_seconds, 0)
    cfg.convert_batch_size = max(cfg.convert_batch_size, 1)
    cfg.target_posts = max(cfg.target_posts, 0)

    # ★ 変更: 0 を無効化として許容
    cfg.usage_check_every_pages = max(cfg.usage_check_every_pages, 0)
    cfg.usage_days = clamp(cfg.usage_days, 1, 90)

    if cfg.window_hours !== nothing && cfg.window_hours <= 0
        error("window_hours must be positive or nothing")
    end

    # max_results は validate では ALL 側の上限まで許容し、endpoint確定後に再クランプする
    cfg.max_results = clamp(cfg.max_results, 10, MAX_RESULTS_ALL)
    cfg.api_base_url = normalize_api_base_url(cfg.api_base_url)
    isempty(cfg.api_base_url) && error("api_base_url is empty")

    TimeZone(cfg.local_tz_name)
    return cfg
end

function validate!(cfg::StreamConfig)
    isempty(cfg.task_name) && error("task_name is empty")
    isempty(cfg.keywords_or) && error("keywords_or is empty")

    cfg.api_base_url = normalize_api_base_url(cfg.api_base_url)
    isempty(cfg.api_base_url) && error("api_base_url is empty")

    cfg.rule_tag = strip(cfg.rule_tag)
    isempty(cfg.rule_tag) && (cfg.rule_tag = cfg.task_name)

    cfg.max_posts = max(cfg.max_posts, 0)
    cfg.max_seconds = max(cfg.max_seconds, 0.0)
    cfg.idle_timeout_seconds = max(cfg.idle_timeout_seconds, 1)
    cfg.max_reconnects = max(cfg.max_reconnects, 0)
    cfg.reconnect_initial_delay_seconds =
        max(cfg.reconnect_initial_delay_seconds, 0.1)
    cfg.reconnect_max_delay_seconds =
        max(cfg.reconnect_max_delay_seconds, cfg.reconnect_initial_delay_seconds)
    cfg.rotate_jsonl_bytes = max(cfg.rotate_jsonl_bytes, 0)
    cfg.rotate_jsonl_seconds = max(cfg.rotate_jsonl_seconds, 0.0)
    cfg.state_flush_interval_seconds = max(cfg.state_flush_interval_seconds, 0.0)
    cfg.convert_batch_size = max(cfg.convert_batch_size, 1)

    return cfg
end

# =========================================================
# State 型（StructTypes）
# =========================================================
mutable struct CollectorState
    timestamp::String
    task_name::String
    query::String
    start_time_utc::Union{Nothing,String}
    end_time_utc::Union{Nothing,String}
    next_token::Union{Nothing,String}
    page_count::Int
    total_tweets::Int
    total_includes::Int
    completed::Bool
    converted_jsonl_offset::Int
    converted_at::String
end

CollectorState() =
    CollectorState("", "", "", nothing, nothing, nothing, 0, 0, 0, false, 0, "")
StructTypes.StructType(::Type{CollectorState}) = StructTypes.Mutable()

mutable struct StreamState
    timestamp::String
    task_name::String
    query::String
    rule_id::Union{Nothing,String}
    rule_tag::String
    connection_count::Int
    total_tweets::Int
    total_includes::Int
    keepalive_count::Int
    last_heartbeat_at::String
    last_event_at::String
    last_error::String
    last_status::Int
    consecutive_failures::Int
    last_connected_at::String
    last_disconnect_at::String
    seen_count::Int
    db_path::String
    db_size_bytes::Int
    completed::Bool
    stop_reason::String
end

StreamState() =
    StreamState("", "", "", nothing, "", 0, 0, 0, 0, "", "", "", 0, 0, "", "", 0, "", 0, false, "")
StructTypes.StructType(::Type{StreamState}) = StructTypes.Mutable()

# =========================================================
# Logging: Base Loggingだけで動く DualLogger
# =========================================================
const _LOG_IO = Ref{Union{Nothing,IO}}(nothing)

struct DualLogger <: AbstractLogger
    io_console::IO
    io_file::IO
    level::LogLevel
end

Logging.min_enabled_level(l::DualLogger) = l.level
Logging.catch_exceptions(::DualLogger) = true

function Logging.shouldlog(l::DualLogger, level, _module, group, id)
    return level >= l.level
end

function Logging.handle_message(
    l::DualLogger,
    level,
    message,
    _module,
    group,
    id,
    file,
    line;
    kwargs...,
)
    ts = Dates.format(Dates.now(Dates.UTC), LOG_TS_FMT) * "Z"
    lvl = string(level)
    msg = string(message)

    function write_one(io::IO)
        print(io, ts, " [", lvl, "] ", msg)
        if file !== nothing && line !== nothing
            print(io, " (", file, ":", line, ")")
        end
        for (k, v) in kwargs
            print(io, " ", k, "=", v)
        end
        println(io)
        flush(io)
    end

    write_one(l.io_console)
    write_one(l.io_file)
    return nothing
end

function setup_logging(cfg; level::LogLevel = Logging.Info)
    mkpath(cfg.log_dir)

    if _LOG_IO[] !== nothing
        try
            close(_LOG_IO[])
        catch
        end
        _LOG_IO[] = nothing
    end

    io = open(out_log(cfg), "a")
    _LOG_IO[] = io
    global_logger(DualLogger(stderr, io, level))
    return io
end

# =========================================================
# JSON3のNull表現（バージョン差吸収）
# =========================================================
const _JSON3_NULL_VALUE = begin
    @static if isdefined(JSON3, :null)
        JSON3.null
    else
        nothing
    end
end

const _JSON3_NULL_TYPE = begin
    @static if isdefined(JSON3, :Null)
        JSON3.Null
    elseif isdefined(JSON3, :NullType)
        JSON3.NullType
    else
        Nothing
    end
end

is_json3_null(x) =
    (_JSON3_NULL_VALUE !== nothing && x === _JSON3_NULL_VALUE) ||
    (_JSON3_NULL_TYPE !== Nothing && x isa _JSON3_NULL_TYPE)

isnull(x) = (x === nothing) || ismissing(x) || is_json3_null(x)

_json_haskey(::Any, ::AbstractString) = false
_json_haskey(obj::JSON3.Object, key::AbstractString) = haskey(obj, key)
_json_haskey(obj::AbstractDict, key::AbstractString) = haskey(obj, key)

_json_get(::Any, ::AbstractString, default = nothing) = default
_json_get(obj::JSON3.Object, key::AbstractString, default = nothing) = get(obj, key, default)
_json_get(obj::AbstractDict, key::AbstractString, default = nothing) = get(obj, key, default)

_json_items(::Any) = Any[]
_json_items(xs::AbstractVector) = xs

_json_pairs(::Any) = Pair{String,Any}[]
function _json_pairs(obj::JSON3.Object)
    out = Pair{String,Any}[]
    for (k, v) in obj
        push!(out, String(k) => v)
    end
    return out
end
function _json_pairs(obj::AbstractDict)
    out = Pair{String,Any}[]
    for (k, v) in obj
        push!(out, String(k) => v)
    end
    return out
end

# =========================================================
# 小物（型変換 / atomic）
# =========================================================
safe_str(x; default = "") =
    isnull(x) ? default : (x isa AbstractString ? String(x) : string(x))

function safe_int(x; default = missing)
    isnull(x) && return default
    x isa Int && return x
    x isa Integer && return Int(x)
    x isa AbstractString && return something(tryparse(Int, x), default)
    if x isa Real
        (isfinite(x) && x == floor(x)) ? Int(x) : default
    else
        default
    end
end

function safe_bool(x; default = missing)
    isnull(x) && return default
    x isa Bool && return x
    x isa Integer && return x != 0
    if x isa AbstractString
        s = lowercase(strip(x))
        s in ("true", "t", "1", "yes", "y") && return true
        s in ("false", "f", "0", "no", "n") && return false
    end
    return default
end

function atomic_write(path::AbstractString, bytes::Vector{UInt8})
    tmp = path * ".tmp"
    open(tmp, "w") do io
        write(io, bytes)
        flush(io)
    end
    mv(tmp, path; force = true)
end

function save_state(path::AbstractString, st::CollectorState)
    atomic_write(path, Vector{UInt8}(JSON3.write(st)))
end

function save_state(path::AbstractString, st::StreamState)
    atomic_write(path, Vector{UInt8}(JSON3.write(st)))
end

function load_state(path::AbstractString)::Union{Nothing,CollectorState}
    !isfile(path) && return nothing
    try
        st = CollectorState()
        JSON3.read!(read(path, String), st)
        return st
    catch e
        @warn "State file unreadable; start fresh" path = path exception = e
        return nothing
    end
end

function load_stream_state(path::AbstractString)::Union{Nothing,StreamState}
    !isfile(path) && return nothing
    try
        st = StreamState()
        JSON3.read!(read(path, String), st)
        return st
    catch e
        @warn "Stream state file unreadable; start fresh" path = path exception = e
        return nothing
    end
end

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

# =========================================================
# SQLite: seen_tweets（重複排除）
# =========================================================
struct SeenDB
    db::SQLite.DB
    stmt_insert_ret::Union{Nothing,SQLite.Stmt}
    stmt_insert::SQLite.Stmt
    stmt_changes::SQLite.Stmt
end

function init_seen_db(cfg)::SeenDB
    db = SQLite.DB(out_db(cfg))

    # ★ 追加: ロック競合で即死しないよう待つ
    try
        if isdefined(SQLite, :busy_timeout!)
            SQLite.busy_timeout!(db, 30_000) # 30秒
        else
            DBInterface.execute(db, "PRAGMA busy_timeout = 30000;")
        end
    catch e
        @warn "Failed to set SQLite busy_timeout (continuing)" exception = e
    end

    DBInterface.execute(
        db,
        """
        CREATE TABLE IF NOT EXISTS seen_tweets(
            id TEXT PRIMARY KEY,
            seen_at_utc TEXT NOT NULL
        );
        """,
    )

    stmt_insert_ret = nothing
    try
        stmt_insert_ret = SQLite.Stmt(
            db,
            """
            INSERT INTO seen_tweets(id, seen_at_utc)
            VALUES(?, ?)
            ON CONFLICT(id) DO NOTHING
            RETURNING id;
            """,
        )
    catch
        stmt_insert_ret = nothing
    end

    stmt_insert = SQLite.Stmt(
        db,
        """
        INSERT INTO seen_tweets(id, seen_at_utc)
        VALUES(?, ?)
        ON CONFLICT(id) DO NOTHING;
        """,
    )
    stmt_changes = SQLite.Stmt(db, "SELECT changes() AS c;")

    return SeenDB(db, stmt_insert_ret, stmt_insert, stmt_changes)
end

function _reset_stmt!(stmt)
    stmt === nothing && return
    if isdefined(SQLite, :reset!) && hasmethod(SQLite.reset!, Tuple{typeof(stmt)})
        try
            SQLite.reset!(stmt)
        catch
        end
    end
    return
end

function mark_seen!(sdb::SeenDB, id::String, seen_at_utc::String)::Bool
    if sdb.stmt_insert_ret !== nothing
        inserted = false
        q = DBInterface.execute(sdb.stmt_insert_ret, (id, seen_at_utc))
        for _ in q
            inserted = true
        end
        _reset_stmt!(sdb.stmt_insert_ret)
        return inserted
    end

    DBInterface.execute(sdb.stmt_insert, (id, seen_at_utc))
    _reset_stmt!(sdb.stmt_insert)

    if isdefined(SQLite, :changes)
        return SQLite.changes(sdb.db) == 1
    end

    c = 0
    q = DBInterface.execute(sdb.stmt_changes)
    for row in q
        c = hasproperty(row, :c) ? row.c : first(values(row))
    end
    _reset_stmt!(sdb.stmt_changes)
    return c == 1
end

function seen_exists(sdb::SeenDB, id::String)::Bool
    q = DBInterface.execute(
        sdb.db,
        "SELECT 1 AS seen FROM seen_tweets WHERE id = ? LIMIT 1;",
        (id,),
    )
    for _ in q
        return true
    end
    return false
end

function count_seen(sdb::SeenDB)::Int
    q = DBInterface.execute(sdb.db, "SELECT COUNT(*) AS c FROM seen_tweets;")
    for row in q
        return Int(hasproperty(row, :c) ? row.c : first(values(row)))
    end
    return 0
end

# =========================================================
# HTTP + Retry / API errors / usage
# =========================================================
function rate_limit_sleep_seconds(resp; min_backoff_seconds = 30)::Int
    retry_after = try
        v = HTTP.header(resp, "retry-after")
        isempty(v) ? 0 : parse(Int, v)
    catch
        0
    end
    reset_epoch = try
        v = HTTP.header(resp, "x-rate-limit-reset")
        isempty(v) ? 0 : parse(Int, v)
    catch
        0
    end
    now_epoch = Int(floor(time()))
    by_reset = reset_epoch > 0 ? max(reset_epoch - now_epoch + 1, 0) : 0
    return max(min_backoff_seconds, max(retry_after, by_reset))
end

struct InvalidPaginationTokenError <: Exception
    status::Int
    body::String
end
Base.showerror(io::IO, e::InvalidPaginationTokenError) =
    print(io, "Invalid pagination token (HTTP $(e.status)): $(e.body)")

struct XApiAccessError <: Exception
    status::Int
    url::String
    body::String
end
Base.showerror(io::IO, e::XApiAccessError) =
    print(io, "X API access error (HTTP $(e.status)) for $(e.url): $(e.body)")

struct FullArchiveAccessError <: Exception
    status::Int
    url::String
    body::String
end
Base.showerror(io::IO, e::FullArchiveAccessError) = print(
    io,
    "Full-archive search is not available for this app/account (HTTP $(e.status)) at $(e.url): $(e.body)",
)

function _looks_like_invalid_pagination_token(body::AbstractString)
    s = _ascii_lowercase_text(body)
    return (occursin("next_token", s) || occursin("pagination", s)) &&
           occursin("token", s) &&
           occursin("invalid", s)
end

function _ascii_lowercase_text(s::AbstractString)::String
    out = UInt8[]
    for b in codeunits(s)
        if 0x41 <= b <= 0x5a
            push!(out, b + 0x20)
        elseif b < 0x80
            push!(out, b)
        else
            push!(out, 0x20)
        end
    end
    return String(out)
end

function _bytes_to_safe_text(bytes::AbstractVector{UInt8}; fallback_label::AbstractString = "response body")::String
    raw = Vector{UInt8}(bytes)
    isempty(raw) && return ""
    isvalid(String, raw) && return String(copy(raw))
    n = min(length(raw), 48)
    hex_io = IOBuffer()
    for i = 1:n
        print(hex_io, string(raw[i], base = 16, pad = 2))
    end
    hex = String(take!(hex_io))
    suffix = length(raw) > n ? "..." : ""
    return "<non-UTF-8 $(fallback_label): $(length(raw)) bytes; hex=$(hex)$(suffix)>"
end

_bytes_to_safe_text(s::AbstractString; fallback_label::AbstractString = "response body") =
    _bytes_to_safe_text(Vector{UInt8}(codeunits(s)); fallback_label = fallback_label)

_bytes_to_safe_text(x; fallback_label::AbstractString = "response body") =
    _bytes_to_safe_text(string(x); fallback_label = fallback_label)

function _safe_showerror_text(e)::String
    try
        return _bytes_to_safe_text(codeunits(sprint(showerror, e)); fallback_label = "exception")
    catch err
        return "<error while rendering exception: $(typeof(err))>"
    end
end

function _response_body_bytes(resp)
    try
        body = resp.body
        body === HTTP.nobody && return UInt8[]
        body isa AbstractVector{UInt8} && return Vector{UInt8}(body)
        body isa AbstractString && return Vector{UInt8}(codeunits(body))
        return Vector{UInt8}(body)
    catch
        return UInt8[]
    end
end

function _response_content_encoding(resp)::String
    try
        return HTTP.header(resp, "content-encoding")
    catch
        return ""
    end
end

function _decode_response_body_bytes(resp, raw::Vector{UInt8})::Vector{UInt8}
    encoding = _ascii_lowercase_text(_response_content_encoding(resp))
    if occursin("gzip", encoding)
        try
            return HTTP.decode(HTTP.Response(resp.status, resp.headers, raw), "gzip")
        catch
            return raw
        end
    end
    return raw
end

function _body_to_text(resp, raw_body = nothing)
    raw =
        raw_body === nothing ? _response_body_bytes(resp) :
        (raw_body isa AbstractVector{UInt8} ? Vector{UInt8}(raw_body) :
         raw_body isa AbstractString ? Vector{UInt8}(codeunits(raw_body)) :
         Vector{UInt8}(raw_body))
    decoded = _decode_response_body_bytes(resp, raw)
    return _bytes_to_safe_text(decoded)
end

function _json_response_body(resp)
    txt = strip(_body_to_text(resp))
    isempty(txt) && return Dict{String,Any}()
    return JSON3.read(txt)
end

function _unwrap_x_api_access_error(e)
    current = e
    for _ = 1:8
        current isa XApiAccessError && return current
        hasproperty(current, :error) || return nothing
        current = getproperty(current, :error)
    end
    return nothing
end

function _extract_x_api_error_detail(body::AbstractString)::String
    txt = strip(_bytes_to_safe_text(codeunits(String(body))))
    isempty(txt) && return txt
    try
        obj = JSON3.read(txt)
        parts = String[]

        if _json_haskey(obj, "title")
            push!(parts, safe_str(_json_get(obj, "title", nothing); default = ""))
        end
        if _json_haskey(obj, "detail")
            push!(parts, safe_str(_json_get(obj, "detail", nothing); default = ""))
        end
        if _json_haskey(obj, "errors")
            for err in _json_items(_json_get(obj, "errors", nothing))
                ttl = safe_str(_json_get(err, "title", nothing); default = "")
                det = safe_str(_json_get(err, "detail", nothing); default = "")
                status = safe_str(_json_get(err, "status", nothing); default = "")
                piece = join(filter(!isempty, [ttl, det, status]), " / ")
                !isempty(piece) && push!(parts, piece)
            end
        end

        parts = unique(filter(!isempty, strip.(parts)))
        return isempty(parts) ? txt : join(parts, " | ")
    catch
        return txt
    end
end

function _looks_like_full_archive_access_error(
    status::Integer,
    url::AbstractString,
    body::AbstractString,
)
    status == 403 || return false
    occursin("/2/tweets/search/all", String(url)) || return false
    s = _ascii_lowercase_text(body)
    return occursin("full-archive", s) ||
           occursin("full archive", s) ||
           occursin("enterprise", s) ||
           occursin("self-serve", s) ||
           occursin("upgrade", s) ||
           occursin("access", s)
end

# ★ 重要：url を引数に取る（テストでスタブ差し替え可能にする）
function fetch_with_retry(
    url::AbstractString,
    headers,
    params::Dict{String,String};
    max_retries::Int = 6,
)
    wait_sec = 5.0
    for attempt = 1:max_retries
        resp = try
            HTTP.get(
                String(url);
                headers = headers,
                query = params,
                status_exception = false,
                readtimeout = 30,
            )
        catch e
            @warn "Network/IO error" attempt = attempt exception = e
            sleep(wait_sec + rand() * 0.5)
            wait_sec = min(wait_sec * 1.8, 60.0)
            continue
        end

        if resp.status == 200
            try
                return JSON3.read(String(resp.body))
            catch e
                @warn "JSON decode failed" attempt = attempt exception = e
                sleep(wait_sec + rand() * 0.5)
                wait_sec = min(wait_sec * 1.6, 60.0)
            end
        elseif resp.status == 429
            s = rate_limit_sleep_seconds(resp)
            @warn "Rate limited" sleep_seconds = s
            sleep(s)
        elseif 500 <= resp.status < 600
            @warn "Server error" status = resp.status attempt = attempt
            sleep(wait_sec + rand() * 0.5)
            wait_sec = min(wait_sec * 2.0, 60.0)
        else
            body = _extract_x_api_error_detail(_body_to_text(resp))
            if 400 <= resp.status < 500 && resp.status != 429
                if resp.status == 400 && _looks_like_invalid_pagination_token(body)
                    throw(InvalidPaginationTokenError(resp.status, body))
                elseif _looks_like_full_archive_access_error(resp.status, url, body)
                    throw(FullArchiveAccessError(resp.status, String(url), body))
                end
                throw(XApiAccessError(resp.status, String(url), body))
            end
            throw(XApiAccessError(resp.status, String(url), body))
        end
    end
    error("Max retries exceeded")
end

Base.@kwdef struct UsageSummary
    project_cap::Union{Missing,Int} = missing
    project_usage::Union{Missing,Int} = missing
    cap_reset_day::Union{Missing,Int} = missing
    remaining::Union{Missing,Int} = missing
end

function parse_usage_summary(obj)::UsageSummary
    data = _json_get(obj, "data", nothing)
    data === nothing && return UsageSummary()

    cap = safe_int(_json_get(data, "project_cap", nothing))
    usage = safe_int(_json_get(data, "project_usage", nothing))
    reset_day = safe_int(_json_get(data, "cap_reset_day", nothing))
    remaining = (ismissing(cap) || ismissing(usage)) ? missing : max(cap - usage, 0)

    return UsageSummary(
        project_cap = cap,
        project_usage = usage,
        cap_reset_day = reset_day,
        remaining = remaining,
    )
end

build_usage_params(cfg::SearchConfig) = begin
    params = Dict{String,String}("days" => string(cfg.usage_days))
    fields = strip(cfg.usage_fields)
    !isempty(fields) && (params["usage.fields"] = fields)
    params
end

function fetch_usage_with_retry(url::AbstractString, headers; max_retries::Int = 6)
    return fetch_usage_with_retry(
        url,
        headers,
        Dict{String,String}();
        max_retries = max_retries,
    )
end

function fetch_usage_with_retry(
    url::AbstractString,
    headers,
    params::Dict{String,String};
    max_retries::Int = 6,
)
    wait_sec = 5.0
    for attempt = 1:max_retries
        resp = try
            HTTP.get(
                String(url);
                headers = headers,
                query = params,
                status_exception = false,
                readtimeout = 30,
            )
        catch e
            @warn "Usage API IO error" attempt = attempt exception = e
            sleep(wait_sec + rand() * 0.5)
            wait_sec = min(wait_sec * 1.8, 60.0)
            continue
        end

        if resp.status == 200
            try
                return JSON3.read(String(resp.body))
            catch e
                @warn "Usage JSON decode failed" attempt = attempt exception = e
                sleep(wait_sec + rand() * 0.5)
                wait_sec = min(wait_sec * 1.6, 60.0)
            end
        elseif resp.status == 429
            s = rate_limit_sleep_seconds(resp)
            @warn "Usage rate limited" sleep_seconds = s
            sleep(s)
        elseif 500 <= resp.status < 600
            @warn "Usage server error" status = resp.status attempt = attempt
            sleep(wait_sec + rand() * 0.5)
            wait_sec = min(wait_sec * 2.0, 60.0)
        else
            body = _extract_x_api_error_detail(_body_to_text(resp))
            throw(XApiAccessError(resp.status, String(url), body))
        end
    end
    error("Usage max retries exceeded")
end

function maybe_check_usage(cfg::SearchConfig, headers, st::CollectorState)
    cfg.usage_check_every_pages <= 0 && return nothing
    st.page_count <= 0 && return nothing
    (st.page_count % cfg.usage_check_every_pages == 0) || return nothing

    raw = fetch_usage_with_retry(cfg.usage_url, headers, build_usage_params(cfg))
    summary = parse_usage_summary(raw)

    @info "Usage snapshot" project_usage = summary.project_usage project_cap =
        summary.project_cap remaining = summary.remaining cap_reset_day =
        summary.cap_reset_day
    return summary
end

# =========================================================
# JSONL書き込み
# =========================================================
write_jsonl(io::IO, kind::AbstractString, data) =
    (JSON3.write(io, (; kind = kind, data = data)); write(io, '\n'))

mutable struct StreamJsonlWriter
    io::IO
    opened_at::Float64
    rotation_count::Int
end

_stream_sink_io(io::IO) = io
_stream_sink_io(w::StreamJsonlWriter) = w.io

function _maybe_fsync(io::IO)
    try
        Base.Libc.fsync(fd(io))
    catch e
        @debug "fsync skipped" exception = e
    end
    return nothing
end

function _flush_stream_sink!(cfg::StreamConfig, sink)
    io = _stream_sink_io(sink)
    flush(io)
    cfg.durable_writes && _maybe_fsync(io)
    return nothing
end

function _rotated_jsonl_path(cfg::StreamConfig, rotation_count::Int)
    stamp = Dates.format(Dates.now(Dates.UTC), dateformat"yyyymmdd\THHMMSS")
    idx = rotation_count
    while true
        path = joinpath(cfg.out_dir, "$(cfg.task_name).rot-$stamp-$(lpad(string(idx), 6, '0')).jsonl")
        isfile(path) || return path
        idx += 1
    end
end

function _open_stream_jsonl_writer(cfg::StreamConfig)
    return StreamJsonlWriter(open(out_jsonl(cfg), "a"), time(), 0)
end

function _close_stream_jsonl_writer!(w::StreamJsonlWriter)
    close(w.io)
    return nothing
end

function _reopen_stream_jsonl_writer!(cfg::StreamConfig, sink::StreamJsonlWriter; open_file = open)
    sink.io = open_file(out_jsonl(cfg), "a")
    sink.opened_at = time()
    return nothing
end

function _rotate_stream_jsonl_if_needed!(
    cfg::StreamConfig,
    sink;
    move_file = mv,
    open_file = open,
)
    sink isa StreamJsonlWriter || return nothing
    size_limit = cfg.rotate_jsonl_bytes > 0
    age_limit = cfg.rotate_jsonl_seconds > 0
    (size_limit || age_limit) || return nothing

    path = out_jsonl(cfg)
    current_size = isfile(path) ? stat(path).size : 0
    should_rotate =
        (size_limit && current_size >= cfg.rotate_jsonl_bytes) ||
        (age_limit && (time() - sink.opened_at) >= cfg.rotate_jsonl_seconds)
    should_rotate || return nothing
    current_size <= 0 && return nothing

    _flush_stream_sink!(cfg, sink)
    close(sink.io)
    sink.rotation_count += 1
    rotated = _rotated_jsonl_path(cfg, sink.rotation_count)
    try
        move_file(path, rotated; force = false)
        try
            _reopen_stream_jsonl_writer!(cfg, sink; open_file = open_file)
        catch e
            try
                isfile(rotated) && !isfile(path) && move_file(rotated, path; force = false)
            catch restore_error
                @warn "Failed to restore active stream JSONL after reopen failure" exception =
                    restore_error
            end
            try
                _reopen_stream_jsonl_writer!(cfg, sink)
            catch reopen_error
                @warn "Failed to reopen active stream JSONL after rotation failure" exception =
                    reopen_error
            end
            rethrow(e)
        end
    catch e
        try
            _reopen_stream_jsonl_writer!(cfg, sink)
        catch reopen_error
            @warn "Failed to reopen active stream JSONL after rotation failure" exception =
                reopen_error
        end
        rethrow(e)
    end
    @info "Rotated stream JSONL" old = path new = rotated
    return rotated
end

function _write_stream_jsonl_entries!(cfg::StreamConfig, sink, entries)
    isempty(entries) && return nothing
    _rotate_stream_jsonl_if_needed!(cfg, sink)
    io = _stream_sink_io(sink)
    for (kind, data) in entries
        write_jsonl(io, kind, data)
    end
    _flush_stream_sink!(cfg, sink)
    return nothing
end

function stream_jsonl_paths(cfg::StreamConfig)
    isdir(cfg.out_dir) || return String[]
    rotated_prefix = "$(cfg.task_name).rot-"
    paths = String[]
    for name in readdir(cfg.out_dir)
        startswith(name, rotated_prefix) || continue
        endswith(name, ".jsonl") || continue
        path = joinpath(cfg.out_dir, name)
        isfile(path) && push!(paths, path)
    end
    sort!(paths)
    active = out_jsonl(cfg)
    isfile(active) && push!(paths, active)
    return paths
end

# =========================================================
# Filtered Stream
# =========================================================
function bearer_headers(; user_agent::AbstractString = "julia-x-collector/repl")
    token = get(ENV, "BEARER_TOKEN", "")
    isempty(token) && error("BEARER_TOKEN is missing (env/.env)")
    return ["Authorization" => "Bearer $token", "User-Agent" => String(user_agent)]
end

function fetch_stream_json_with_retry(
    method::AbstractString,
    url::AbstractString,
    headers,
    params::Dict{String,String} = Dict{String,String}();
    body = nothing,
    max_retries::Int = 6,
    readtimeout::Int = 30,
)
    wait_sec = 5.0
    request_headers = collect(headers)
    request_body = UInt8[]
    if body !== nothing
        push!(request_headers, "Content-Type" => "application/json")
        request_body = Vector{UInt8}(JSON3.write(body))
    end

    for attempt = 1:max_retries
        resp = try
            HTTP.request(
                String(method),
                String(url);
                headers = request_headers,
                query = params,
                body = request_body,
                status_exception = false,
                readtimeout = readtimeout,
            )
        catch e
            @warn "Stream rules API IO error" attempt = attempt exception = e
            sleep(wait_sec + rand() * 0.5)
            wait_sec = min(wait_sec * 1.8, 60.0)
            continue
        end

        if 200 <= resp.status < 300
            try
                return _json_response_body(resp)
            catch e
                @warn "Stream rules JSON decode failed" attempt = attempt exception = e
                sleep(wait_sec + rand() * 0.5)
                wait_sec = min(wait_sec * 1.6, 60.0)
            end
        elseif resp.status == 429
            s = rate_limit_sleep_seconds(resp)
            @warn "Stream rules rate limited" sleep_seconds = s
            sleep(s)
        elseif 500 <= resp.status < 600
            @warn "Stream rules server error" status = resp.status attempt = attempt
            sleep(wait_sec + rand() * 0.5)
            wait_sec = min(wait_sec * 2.0, 60.0)
        else
            body_text = _extract_x_api_error_detail(_body_to_text(resp))
            throw(XApiAccessError(resp.status, String(url), body_text))
        end
    end
    error("Stream rules max retries exceeded")
end

build_stream_params(cfg::StreamConfig) = begin
    params = Dict{String,String}()
    for (k, v) in API_FIELDS
        params[k] = v
    end
    return params
end

function list_stream_rules(cfg::StreamConfig)
    validate!(cfg)
    return list_stream_rules(cfg, bearer_headers(user_agent = "julia-x-collector/stream"))
end

function list_stream_rules(cfg::StreamConfig, headers)
    return list_stream_rules(cfg, headers, fetch_stream_json_with_retry)
end

function list_stream_rules(cfg::StreamConfig, headers, fetch_json::Function)
    return fetch_json(
        "GET",
        stream_rules_url(cfg),
        headers,
        Dict{String,String}(),
    )
end

function delete_stream_rules!(
    cfg::StreamConfig,
    headers,
    ids::Vector{String},
    fetch_json::Function = fetch_stream_json_with_retry,
)
    isempty(ids) && return nothing
    body = Dict("delete" => Dict("ids" => ids))
    return fetch_json(
        "POST",
        stream_rules_url(cfg),
        headers,
        Dict{String,String}(),
        body = body,
    )
end

function add_stream_rule!(
    cfg::StreamConfig,
    headers,
    value::AbstractString,
    tag::AbstractString,
    fetch_json::Function = fetch_stream_json_with_retry,
)
    body = Dict("add" => [Dict("value" => String(value), "tag" => String(tag))])
    return fetch_json(
        "POST",
        stream_rules_url(cfg),
        headers,
        Dict{String,String}(),
        body = body,
    )
end

function _rules_data(obj)
    data = _json_get(obj, "data", nothing)
    data === nothing && return Any[]
    return _json_items(data)
end

function _rule_namedtuple(rule)
    return (
        id = safe_str(_json_get(rule, "id", nothing); default = ""),
        value = safe_str(_json_get(rule, "value", nothing); default = ""),
        tag = safe_str(_json_get(rule, "tag", nothing); default = ""),
    )
end

function _stream_rules_summary(obj)
    meta = _json_get(obj, "meta", nothing)
    meta === nothing && return nothing
    return _json_get(meta, "summary", nothing)
end

function _stream_rules_summary_count(obj, key::AbstractString)::Int
    summary = _stream_rules_summary(obj)
    summary === nothing && return 0
    return safe_int(_json_get(summary, key, nothing); default = 0)
end

function _stream_rules_error_detail(obj)::String
    parts = String[]
    if _json_haskey(obj, "errors")
        errors = _json_get(obj, "errors", nothing)
        push!(parts, "errors=$(String(JSON3.write(errors)))")
    end
    summary = _stream_rules_summary(obj)
    if summary !== nothing
        push!(parts, "summary=$(String(JSON3.write(summary)))")
    end
    return isempty(parts) ? "unknown stream rules API failure" : join(parts, "; ")
end

function _assert_stream_rules_response_ok(obj; action::Symbol)
    if _json_haskey(obj, "errors")
        error("Stream rules API $(action) failed: $(_stream_rules_error_detail(obj))")
    end
    if action === :add
        invalid = _stream_rules_summary_count(obj, "invalid")
        not_created = _stream_rules_summary_count(obj, "not_created")
        if invalid > 0 || not_created > 0
            error("Stream rules API add failed: $(_stream_rules_error_detail(obj))")
        end
    elseif action === :delete
        not_deleted = _stream_rules_summary_count(obj, "not_deleted")
        if not_deleted > 0
            error("Stream rules API delete failed: $(_stream_rules_error_detail(obj))")
        end
    end
    return obj
end

function _find_stream_rule(obj, tag::AbstractString, value::AbstractString)
    for r in _rules_data(obj)
        rt = _rule_namedtuple(r)
        if rt.tag == tag && rt.value == value
            return rt
        end
    end
    return nothing
end

function _stream_rule_delete_ids_or_error(rules, tag::AbstractString)::Vector{String}
    ids = String[]
    missing_values = String[]
    for r in rules
        if isempty(r.id)
            push!(missing_values, r.value)
        else
            push!(ids, r.id)
        end
    end
    if !isempty(missing_values)
        detail = join(missing_values, "; ")
        error("Stream rule tag '$tag' cannot be replaced because a conflicting rule has no id: $detail")
    end
    isempty(ids) && error("Stream rule tag '$tag' cannot be replaced because no rule ids were returned")
    return ids
end

function ensure_stream_rule!(cfg::StreamConfig)
    validate!(cfg)
    headers = bearer_headers(user_agent = "julia-x-collector/stream")
    return ensure_stream_rule!(cfg, headers)
end

function ensure_stream_rule!(cfg::StreamConfig, headers)
    return ensure_stream_rule!(cfg, headers, fetch_stream_json_with_retry)
end

function ensure_stream_rule!(cfg::StreamConfig, headers, fetch_json::Function)
    validate!(cfg)
    value = build_query(cfg)
    tag = cfg.rule_tag

    if !cfg.manage_rules
        return (id = nothing, value = value, tag = tag, created = false, response = nothing)
    end

    rules = list_stream_rules(cfg, headers, fetch_json)
    same_tag = [_rule_namedtuple(r) for r in _rules_data(rules) if safe_str(_json_get(r, "tag", nothing); default = "") == tag]
    for r in same_tag
        if r.value == value
            rid = isempty(r.id) ? nothing : r.id
            return (id = rid, value = value, tag = tag, created = false, response = rules)
        end
    end

    if !isempty(same_tag)
        if !cfg.replace_rule_by_tag
            error("Stream rule tag '$tag' already exists with a different value")
        end
        delete_ids = _stream_rule_delete_ids_or_error(same_tag, tag)
        delete_res = delete_stream_rules!(
            cfg,
            headers,
            delete_ids,
            fetch_json,
        )
        _assert_stream_rules_response_ok(delete_res; action = :delete)
    end

    res = add_stream_rule!(cfg, headers, value, tag, fetch_json)
    _assert_stream_rules_response_ok(res; action = :add)
    created_rule = _find_stream_rule(res, tag, value)
    created_rule === nothing &&
        error("Stream rules API add succeeded but the expected rule was not returned")
    isempty(created_rule.id) &&
        error("Stream rules API add succeeded but the created rule id was not returned")
    rid = created_rule.id
    return (id = rid, value = value, tag = tag, created = true, response = res)
end

function _stream_line_payload(line::AbstractString)::Union{Nothing,String}
    s = strip(String(line))
    isempty(s) && return nothing
    if startswith(s, "data:")
        s = strip(s[6:end])
        isempty(s) && return nothing
    end
    s == "[DONE]" && return nothing
    return s
end

function _stream_event_meta(obj, received_at::AbstractString, page_new::Int)
    meta = Dict{String,Any}("received_at" => String(received_at), "result_count" => page_new)
    _json_haskey(obj, "matching_rules") &&
        (meta["matching_rules"] = _json_get(obj, "matching_rules", nothing))
    _json_haskey(obj, "errors") && (meta["errors"] = _json_get(obj, "errors", nothing))
    _json_haskey(obj, "meta") && (meta["stream_meta"] = _json_get(obj, "meta", nothing))
    return meta
end

function _stream_data_items(obj)
    data = _json_get(obj, "data", nothing)
    data === nothing && return Any[]
    if data isa AbstractVector
        return data
    end
    return Any[data]
end

function handle_stream_line!(
    cfg::StreamConfig,
    st::StreamState,
    sdb::SeenDB,
    sink,
    line::AbstractString,
)::Symbol
    payload = _stream_line_payload(line)
    if payload === nothing
        st.keepalive_count += 1
        st.last_heartbeat_at = stream_now_utc()
        return :keepalive
    end

    obj = try
        JSON3.read(payload)
    catch e
        @warn "Stream JSON parse error (skip)" exception = e line = payload
        return :invalid
    end

    new_tweets = Any[]
    new_ids = String[]
    if _json_haskey(obj, "data")
        seen_in_event = Set{String}()
        for tw in _stream_data_items(obj)
            tid = safe_str(_json_get(tw, "id", nothing); default = "")
            isempty(tid) && continue
            tid in seen_in_event && continue
            push!(seen_in_event, tid)
            if !seen_exists(sdb, tid)
                push!(new_tweets, tw)
                push!(new_ids, tid)
            end
        end
    end

    if !isempty(new_tweets)
        DBInterface.execute(sdb.db, "BEGIN;")
        try
            seen_at = utc_now_z(lag_seconds = 0)
            inserted = 0
            accepted_tweets = Any[]
            for (tw, tid) in zip(new_tweets, new_ids)
                if mark_seen!(sdb, tid, seen_at)
                    push!(accepted_tweets, tw)
                    inserted += 1
                end
            end

            page_new = length(accepted_tweets)
            if page_new == 0
                DBInterface.execute(sdb.db, "COMMIT;")
                return _json_haskey(obj, "errors") ? :errors : :ignored
            end

            entries = Vector{Tuple{String,Any}}()
            for tw in accepted_tweets
                push!(entries, ("tweet", tw))
            end

            include_count = 0
            if cfg.write_includes && _json_haskey(obj, "includes")
                inc = _json_get(obj, "includes", nothing)
                for (k, arr) in _json_pairs(inc)
                    kstr = String(k)
                    for item in _json_items(arr)
                        push!(entries, ("include:$kstr", item))
                        include_count += 1
                    end
                end
            end

            received_at = stream_now_utc()
            push!(
                entries,
                (
                    "page",
                    Dict(
                        "page" => st.total_tweets + page_new,
                        "endpoint" => "stream",
                        "meta" => _stream_event_meta(obj, received_at, page_new),
                    ),
                ),
            )

            _write_stream_jsonl_entries!(cfg, sink, entries)
            DBInterface.execute(sdb.db, "COMMIT;")
            st.total_tweets += page_new
            st.total_includes += include_count
            st.seen_count += inserted
            st.last_event_at = received_at
        catch e
            try
                DBInterface.execute(sdb.db, "ROLLBACK;")
            catch
            end
            rethrow(e)
        end
        return :tweet
    end

    return _json_haskey(obj, "errors") ? :errors : :ignored
end

function _stream_stop_reason(cfg::StreamConfig, st::StreamState, started_at::Float64)
    cfg.max_posts > 0 && st.total_tweets >= cfg.max_posts && return "max_posts"
    cfg.max_seconds > 0 && (time() - started_at) >= cfg.max_seconds && return "max_seconds"
    return nothing
end

stream_now_utc() = string(Dates.now(Dates.UTC))

function _refresh_stream_storage_stats!(cfg::StreamConfig, st::StreamState)
    st.db_path = out_db(cfg)
    st.db_size_bytes = isfile(st.db_path) ? stat(st.db_path).size : 0
    return st
end

function _persist_stream_state!(cfg::StreamConfig, st::StreamState)
    _refresh_stream_storage_stats!(cfg, st)
    st.timestamp = stream_now_utc()
    save_state(out_stream_state(cfg), st)
    return nothing
end

function _maybe_persist_stream_state!(
    cfg::StreamConfig,
    st::StreamState,
    last_flush_ref::Base.RefValue{Float64};
    force::Bool = false,
)
    now = time()
    due =
        cfg.state_flush_interval_seconds > 0 &&
        (last_flush_ref[] <= 0 || (now - last_flush_ref[]) >= cfg.state_flush_interval_seconds)
    if force || due
        _persist_stream_state!(cfg, st)
        last_flush_ref[] = now
    end
    return nothing
end

function _write_stream_gap!(
    cfg::StreamConfig,
    st::StreamState,
    start_time_utc::AbstractString,
    end_time_utc::AbstractString,
    reason::AbstractString,
)
    isempty(start_time_utc) && return nothing
    mkpath(cfg.out_dir)
    open(out_stream_gaps(cfg), "a") do io
        JSON3.write(
            io,
            Dict(
                "task_name" => st.task_name,
                "query" => st.query,
                "rule_id" => st.rule_id,
                "rule_tag" => st.rule_tag,
                "start_time_utc" => String(start_time_utc),
                "end_time_utc" => String(end_time_utc),
                "reason" => String(reason),
                "recorded_at" => stream_now_utc(),
            ),
        )
        write(io, '\n')
        flush(io)
        cfg.durable_writes && _maybe_fsync(io)
    end
    return nothing
end

function _begin_stream_gap!(
    st::StreamState,
    pending_gap_start::Base.RefValue{Union{Nothing,String}},
    pending_gap_reason::Base.RefValue{String},
    reason::AbstractString,
)
    disconnected_at = stream_now_utc()
    st.last_disconnect_at = disconnected_at
    pending_gap_start[] === nothing && (pending_gap_start[] = disconnected_at)
    isempty(pending_gap_reason[]) && (pending_gap_reason[] = String(reason))
    return disconnected_at
end

function _finish_stream_gap!(
    cfg::StreamConfig,
    st::StreamState,
    pending_gap_start::Base.RefValue{Union{Nothing,String}},
    pending_gap_reason::Base.RefValue{String},
    end_time_utc::AbstractString,
    reason::AbstractString = pending_gap_reason[],
)
    pending_gap_start[] === nothing && return nothing
    gap_reason = isempty(reason) ? "disconnected" : String(reason)
    _write_stream_gap!(cfg, st, pending_gap_start[]::String, end_time_utc, gap_reason)
    pending_gap_start[] = nothing
    pending_gap_reason[] = ""
    return nothing
end

function _stream_response_body(resp, http)
    try
        return _body_to_text(resp, read(http))
    catch
        return ""
    end
end

_stream_response_body(http) = try
    _bytes_to_safe_text(read(http))
catch
    ""
end

function _throw_stream_response_error(resp, url::AbstractString, body::AbstractString)
    body_text = _extract_x_api_error_detail(body)
    throw(XApiAccessError(resp.status, String(url), body_text))
end

function _stream_response_outcome(resp, url::AbstractString; body::AbstractString = "")
    resp.status == 200 &&
        return (
            kind = :ok,
            stop_reason = "",
            sleep_seconds = 0,
            activity = false,
            status = 200,
            error = "",
        )
    if resp.status == 429
        return (
            kind = :retryable,
            stop_reason = "rate_limited",
            sleep_seconds = rate_limit_sleep_seconds(resp),
            activity = false,
            status = resp.status,
            error = "rate limited",
        )
    elseif 500 <= resp.status < 600
        return (
            kind = :retryable,
            stop_reason = "server_error",
            sleep_seconds = 0,
            activity = false,
            status = resp.status,
            error = "server error",
        )
    end
    _throw_stream_response_error(resp, url, body)
end

function _record_stream_line!(
    cfg::StreamConfig,
    st::StreamState,
    sdb::SeenDB,
    sink,
    line::AbstractString,
    started_at::Float64,
    activity_ref::Base.RefValue{Bool},
    state_flush_ref::Base.RefValue{Float64} = Ref(time()),
)
    result = handle_stream_line!(cfg, st, sdb, sink, line)
    if result != :invalid
        activity_ref[] = true
        st.consecutive_failures = 0
        st.stop_reason = ""
    end
    force_state = result in (:tweet, :errors)
    _maybe_persist_stream_state!(cfg, st, state_flush_ref; force = force_state)

    stop_reason = _stream_stop_reason(cfg, st, started_at)
    if stop_reason !== nothing
        st.completed = true
        st.stop_reason = stop_reason
        _persist_stream_state!(cfg, st)
        state_flush_ref[] = time()
    end
    return (result = result, stop_reason = stop_reason)
end

function _process_stream_chunk!(
    cfg::StreamConfig,
    st::StreamState,
    sdb::SeenDB,
    sink,
    pending_line::IOBuffer,
    chunk,
    started_at::Float64,
    activity_ref::Base.RefValue{Bool},
    state_flush_ref::Base.RefValue{Float64} = Ref(time()),
)
    for b in chunk
        if b == UInt8('\n')
            line = String(take!(pending_line))
            res = _record_stream_line!(
                cfg,
                st,
                sdb,
                sink,
                line,
                started_at,
                activity_ref,
                state_flush_ref,
            )
            res.stop_reason !== nothing && return res
        else
            write(pending_line, b)
        end
    end
    return (result = :pending, stop_reason = nothing)
end

function _flush_pending_stream_line!(
    cfg::StreamConfig,
    st::StreamState,
    sdb::SeenDB,
    sink,
    pending_line::IOBuffer,
    started_at::Float64,
    activity_ref::Base.RefValue{Bool},
    state_flush_ref::Base.RefValue{Float64} = Ref(time()),
)
    position(pending_line) == 0 && return (result = :empty, stop_reason = nothing)
    line = String(take!(pending_line))
    return _record_stream_line!(
        cfg,
        st,
        sdb,
        sink,
        line,
        started_at,
        activity_ref,
        state_flush_ref,
    )
end

_stream_completed_outcome(stop_reason::AbstractString, activity_ref::Base.RefValue{Bool}) = (
    kind = :completed,
    stop_reason = String(stop_reason),
    sleep_seconds = 0,
    activity = activity_ref[],
    status = 0,
    error = "",
)

_stream_disconnected_outcome(activity_ref::Base.RefValue{Bool}) = (
    kind = :disconnected,
    stop_reason = "disconnected",
    sleep_seconds = 0,
    activity = activity_ref[],
    status = 0,
    error = "",
)

function _flush_pending_stream_outcome!(
    cfg::StreamConfig,
    st::StreamState,
    sdb::SeenDB,
    sink,
    pending_line::IOBuffer,
    started_at::Float64,
    activity_ref::Base.RefValue{Bool},
    state_flush_ref::Base.RefValue{Float64} = Ref(time()),
)
    res = _flush_pending_stream_line!(
        cfg,
        st,
        sdb,
        sink,
        pending_line,
        started_at,
        activity_ref,
        state_flush_ref,
    )
    res.stop_reason !== nothing &&
        return _stream_completed_outcome(res.stop_reason, activity_ref)
    return nothing
end

function _read_stream_lines!(
    cfg::StreamConfig,
    st::StreamState,
    sdb::SeenDB,
    sink,
    http,
    started_at::Float64,
    activity_ref::Base.RefValue{Bool},
    state_flush_ref::Base.RefValue{Float64} = Ref(time()),
)
    pending_line = IOBuffer()
    try
        while true
            chunk = readavailable(http)
            if isempty(chunk)
                outcome = _flush_pending_stream_outcome!(
                    cfg,
                    st,
                    sdb,
                    sink,
                    pending_line,
                    started_at,
                    activity_ref,
                    state_flush_ref,
                )
                outcome !== nothing && return outcome
                return _stream_disconnected_outcome(activity_ref)
            end
            res = _process_stream_chunk!(
                cfg,
                st,
                sdb,
                sink,
                pending_line,
                chunk,
                started_at,
                activity_ref,
                state_flush_ref,
            )
            if res.stop_reason !== nothing
                return _stream_completed_outcome(res.stop_reason, activity_ref)
            end
        end
    catch e
        outcome = _flush_pending_stream_outcome!(
            cfg,
            st,
            sdb,
            sink,
            pending_line,
            started_at,
            activity_ref,
            state_flush_ref,
        )
        outcome !== nothing && return outcome
        rethrow(e)
    end
end

function _stream_reconnect_decision(cfg::StreamConfig, reconnects::Int)
    if !cfg.reconnect
        return (should_reconnect = false, reconnects = reconnects, stop_reason = "")
    end
    next_reconnects = reconnects + 1
    if cfg.max_reconnects > 0 && next_reconnects > cfg.max_reconnects
        return (
            should_reconnect = false,
            reconnects = next_reconnects,
            stop_reason = "max_reconnects_exceeded",
        )
    end
    return (should_reconnect = true, reconnects = next_reconnects, stop_reason = "")
end

function _looks_like_stream_timeout(e)::Bool
    msg = _ascii_lowercase_text(_safe_showerror_text(e))
    return occursin("timeout", msg) || occursin("timed out", msg)
end

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

function run_collector(cfg::SearchConfig)
    validate!(cfg)
    mkpath(cfg.out_dir)

    token = get(ENV, "BEARER_TOKEN", "")
    isempty(token) && error("BEARER_TOKEN is missing (env/.env)")

    headers = ["Authorization" => "Bearer $token", "User-Agent" => "julia-x-collector/repl"]

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
                    fetch_with_retry(url, headers, params)
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
                        usage = maybe_check_usage(cfg, headers, st2)
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

# =========================================================
# 変換（CSV + Arrow）: 固定列の横長1表
# =========================================================
const MaybeString = Union{Missing,String}
const MaybeBool = Union{Missing,Bool}
const MaybeInt = Union{Missing,Int}

const TWEET_ROW_NAMES = (
    :tweet_id,
    :created_at,
    :author_id,
    :author_username,
    :author_name,
    :lang,
    :text,
    :possibly_sensitive,
    :reply_settings,
    :source,
    :conversation_id,
    :in_reply_to_user_id,
    :is_retweet,
    :is_quote,
    :is_reply,
    :referenced_tweet_types,
    :referenced_tweet_ids,
    :original_tweet_id,
    :original_author_id,
    :original_created_at,
    :original_lang,
    :original_text,
    :retweet_count,
    :reply_count,
    :like_count,
    :quote_count,
    :bookmark_count,
    :impression_count,
    :original_retweet_count,
    :original_reply_count,
    :original_like_count,
    :original_quote_count,
    :original_bookmark_count,
    :original_impression_count,
    :mention_usernames,
    :mention_ids,
    :mention_names,
    :hashtags,
    :urls,
    :expanded_urls,
    :unwound_urls,
    :context_domain_names,
    :context_entity_names,
    :media_keys,
    :media_types,
    :media_urls,
    :media_preview_image_urls,
    :media_widths,
    :media_heights,
    :media_duration_ms,
    :media_view_counts,
    :place_id,
    :place_full_name,
    :place_country_code,
    :author_created_at,
    :author_location,
    :author_verified,
    :author_protected,
    :author_followers_count,
    :author_following_count,
    :author_tweet_count,
    :author_listed_count,
    :author_like_count,
    :author_media_count,
    :edit_history_tweet_ids,
    :is_edit_eligible,
    :editable_until,
    :edits_remaining,
    :entities_json,
    :context_annotations_json,
    :referenced_tweets_json,
    :attachments_json,
    :public_metrics_json,
    :original_public_metrics_json,
    :author_user_json,
    :media_json,
    :place_json,
)

const TweetRow = NamedTuple{
    TWEET_ROW_NAMES,
    Tuple{
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeBool,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        Bool,
        Bool,
        Bool,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeInt,
        MaybeInt,
        MaybeInt,
        MaybeInt,
        MaybeInt,
        MaybeInt,
        MaybeInt,
        MaybeInt,
        MaybeInt,
        MaybeInt,
        MaybeInt,
        MaybeInt,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeBool,
        MaybeBool,
        MaybeInt,
        MaybeInt,
        MaybeInt,
        MaybeInt,
        MaybeInt,
        MaybeInt,
        MaybeString,
        MaybeBool,
        MaybeString,
        MaybeInt,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
        MaybeString,
    },
}

flat_maybe_str(x) = begin
    s = safe_str(x; default = "")
    isempty(s) ? missing : s
end

flat_join_str(xs::Vector{String}, sep::AbstractString) =
    isempty(xs) ? missing : join(xs, sep)

flat_json_str(x) = begin
    isnull(x) && return missing
    try
        String(JSON3.write(x))
    catch
        missing
    end
end

flat_getv(obj, key, default = nothing) = _json_get(obj, String(key), default)

function _metric(pm, key::AbstractString)
    pm === nothing && return missing
    return safe_int(flat_getv(pm, key, nothing))
end

function tweet_to_row(
    tdata;
    sep::AbstractString = ";",
    users::Dict{String,Any} = Dict{String,Any}(),
    media::Dict{String,Any} = Dict{String,Any}(),
    places::Dict{String,Any} = Dict{String,Any}(),
    included_tweets::Dict{String,Any} = Dict{String,Any}(),
)::TweetRow
    tweet_id = safe_str(flat_getv(tdata, "id", nothing); default = "")
    author_id = safe_str(flat_getv(tdata, "author_id", nothing); default = "")
    created_at = safe_str(flat_getv(tdata, "created_at", nothing); default = "")
    lang = safe_str(flat_getv(tdata, "lang", nothing); default = "")
    text = safe_str(flat_getv(tdata, "text", nothing); default = "")
    reply_settings = safe_str(flat_getv(tdata, "reply_settings", nothing); default = "")
    source = safe_str(flat_getv(tdata, "source", nothing); default = "")
    conversation_id = safe_str(flat_getv(tdata, "conversation_id", nothing); default = "")
    in_reply_to_user_id =
        safe_str(flat_getv(tdata, "in_reply_to_user_id", nothing); default = "")

    pm = flat_getv(tdata, "public_metrics", nothing)
    ents = flat_getv(tdata, "entities", nothing)
    rts = flat_getv(tdata, "referenced_tweets", nothing)
    attachments = flat_getv(tdata, "attachments", nothing)
    context_annotations = flat_getv(tdata, "context_annotations", nothing)
    edit_controls = flat_getv(tdata, "edit_controls", nothing)

    ref_types = String[]
    ref_ids = String[]
    original_tweet_id = ""
    if rts !== nothing
        for r in rts
            ty = safe_str(flat_getv(r, "type", nothing); default = "")
            rid = safe_str(flat_getv(r, "id", nothing); default = "")
            !isempty(ty) && push!(ref_types, ty)
            !isempty(rid) && push!(ref_ids, rid)
            if ty == "retweeted" && isempty(original_tweet_id)
                original_tweet_id = rid
            end
        end
    end
    is_retweet = any(==("retweeted"), ref_types)
    is_quote = any(==("quoted"), ref_types)
    is_reply = any(==("replied_to"), ref_types)

    original = isempty(original_tweet_id) ? nothing : get(included_tweets, original_tweet_id, nothing)
    original_pm = original === nothing ? nothing : flat_getv(original, "public_metrics", nothing)

    mention_usernames = String[]
    mention_ids = String[]
    mention_names = String[]
    hashtags = String[]
    urls = String[]
    expanded_urls = String[]
    unwound_urls = String[]

    if ents !== nothing
        mentions = flat_getv(ents, "mentions", nothing)
        if mentions !== nothing
            for m in mentions
                mid = safe_str(flat_getv(m, "id", nothing); default = "")
                username = safe_str(flat_getv(m, "username", nothing); default = "")
                !isempty(mid) && push!(mention_ids, mid)
                !isempty(username) && push!(mention_usernames, username)
                u = isempty(mid) ? nothing : get(users, mid, nothing)
                nm = u === nothing ? "" : safe_str(flat_getv(u, "name", nothing); default = "")
                !isempty(nm) && push!(mention_names, nm)
            end
        end

        hs = flat_getv(ents, "hashtags", nothing)
        if hs !== nothing
            for h in hs
                tag = safe_str(flat_getv(h, "tag", nothing); default = "")
                !isempty(tag) && push!(hashtags, tag)
            end
        end

        us = flat_getv(ents, "urls", nothing)
        if us !== nothing
            for u in us
                url = safe_str(flat_getv(u, "url", nothing); default = "")
                ex = safe_str(flat_getv(u, "expanded_url", nothing); default = "")
                uw = safe_str(flat_getv(u, "unwound_url", nothing); default = "")
                !isempty(url) && push!(urls, url)
                !isempty(ex) && push!(expanded_urls, ex)
                !isempty(uw) && push!(unwound_urls, uw)
            end
        end
    end

    context_domain_names = String[]
    context_entity_names = String[]
    if context_annotations !== nothing
        for ca in context_annotations
            domain = flat_getv(ca, "domain", nothing)
            entity = flat_getv(ca, "entity", nothing)
            dnm = domain === nothing ? "" : safe_str(flat_getv(domain, "name", nothing); default = "")
            enm = entity === nothing ? "" : safe_str(flat_getv(entity, "name", nothing); default = "")
            !isempty(dnm) && push!(context_domain_names, dnm)
            !isempty(enm) && push!(context_entity_names, enm)
        end
    end

    media_keys = String[]
    media_types = String[]
    media_urls = String[]
    media_preview_image_urls = String[]
    media_widths = String[]
    media_heights = String[]
    media_duration_ms = String[]
    media_view_counts = String[]
    media_objs = Any[]
    keys = attachments === nothing ? nothing : flat_getv(attachments, "media_keys", nothing)
    if keys !== nothing
        for k in keys
            mk = safe_str(k; default = "")
            isempty(mk) && continue
            push!(media_keys, mk)
            m = get(media, mk, nothing)
            m === nothing && continue
            push!(media_objs, m)
            typ = safe_str(flat_getv(m, "type", nothing); default = "")
            url = safe_str(flat_getv(m, "url", nothing); default = "")
            preview = safe_str(flat_getv(m, "preview_image_url", nothing); default = "")
            width = safe_str(flat_getv(m, "width", nothing); default = "")
            height = safe_str(flat_getv(m, "height", nothing); default = "")
            duration = safe_str(flat_getv(m, "duration_ms", nothing); default = "")
            mpm = flat_getv(m, "public_metrics", nothing)
            views = mpm === nothing ? "" : safe_str(flat_getv(mpm, "view_count", nothing); default = "")
            !isempty(typ) && push!(media_types, typ)
            !isempty(url) && push!(media_urls, url)
            !isempty(preview) && push!(media_preview_image_urls, preview)
            !isempty(width) && push!(media_widths, width)
            !isempty(height) && push!(media_heights, height)
            !isempty(duration) && push!(media_duration_ms, duration)
            !isempty(views) && push!(media_view_counts, views)
        end
    end

    geo = flat_getv(tdata, "geo", nothing)
    place_id = geo === nothing ? "" : safe_str(flat_getv(geo, "place_id", nothing); default = "")
    place = isempty(place_id) ? nothing : get(places, place_id, nothing)

    author = isempty(author_id) ? nothing : get(users, author_id, nothing)
    author_pm = author === nothing ? nothing : flat_getv(author, "public_metrics", nothing)

    edit_history = flat_getv(tdata, "edit_history_tweet_ids", nothing)

    return (
        tweet_id = flat_maybe_str(tweet_id),
        created_at = flat_maybe_str(created_at),
        author_id = flat_maybe_str(author_id),
        author_username = author === nothing ? missing :
                          flat_maybe_str(flat_getv(author, "username", nothing)),
        author_name = author === nothing ? missing :
                      flat_maybe_str(flat_getv(author, "name", nothing)),
        lang = flat_maybe_str(lang),
        text = flat_maybe_str(text),
        possibly_sensitive = safe_bool(flat_getv(tdata, "possibly_sensitive", nothing)),
        reply_settings = flat_maybe_str(reply_settings),
        source = flat_maybe_str(source),
        conversation_id = flat_maybe_str(conversation_id),
        in_reply_to_user_id = flat_maybe_str(in_reply_to_user_id),
        is_retweet = is_retweet,
        is_quote = is_quote,
        is_reply = is_reply,
        referenced_tweet_types = flat_join_str(ref_types, sep),
        referenced_tweet_ids = flat_join_str(ref_ids, sep),
        original_tweet_id = flat_maybe_str(original_tweet_id),
        original_author_id = original === nothing ? missing :
                             flat_maybe_str(flat_getv(original, "author_id", nothing)),
        original_created_at = original === nothing ? missing :
                              flat_maybe_str(flat_getv(original, "created_at", nothing)),
        original_lang = original === nothing ? missing :
                        flat_maybe_str(flat_getv(original, "lang", nothing)),
        original_text = original === nothing ? missing :
                        flat_maybe_str(flat_getv(original, "text", nothing)),
        retweet_count = _metric(pm, "retweet_count"),
        reply_count = _metric(pm, "reply_count"),
        like_count = _metric(pm, "like_count"),
        quote_count = _metric(pm, "quote_count"),
        bookmark_count = _metric(pm, "bookmark_count"),
        impression_count = _metric(pm, "impression_count"),
        original_retweet_count = _metric(original_pm, "retweet_count"),
        original_reply_count = _metric(original_pm, "reply_count"),
        original_like_count = _metric(original_pm, "like_count"),
        original_quote_count = _metric(original_pm, "quote_count"),
        original_bookmark_count = _metric(original_pm, "bookmark_count"),
        original_impression_count = _metric(original_pm, "impression_count"),
        mention_usernames = flat_join_str(mention_usernames, sep),
        mention_ids = flat_join_str(mention_ids, sep),
        mention_names = flat_join_str(mention_names, sep),
        hashtags = flat_join_str(hashtags, sep),
        urls = flat_join_str(urls, sep),
        expanded_urls = flat_join_str(expanded_urls, sep),
        unwound_urls = flat_join_str(unwound_urls, sep),
        context_domain_names = flat_join_str(context_domain_names, sep),
        context_entity_names = flat_join_str(context_entity_names, sep),
        media_keys = flat_join_str(media_keys, sep),
        media_types = flat_join_str(media_types, sep),
        media_urls = flat_join_str(media_urls, sep),
        media_preview_image_urls = flat_join_str(media_preview_image_urls, sep),
        media_widths = flat_join_str(media_widths, sep),
        media_heights = flat_join_str(media_heights, sep),
        media_duration_ms = flat_join_str(media_duration_ms, sep),
        media_view_counts = flat_join_str(media_view_counts, sep),
        place_id = flat_maybe_str(place_id),
        place_full_name = place === nothing ? missing :
                          flat_maybe_str(flat_getv(place, "full_name", nothing)),
        place_country_code = place === nothing ? missing :
                             flat_maybe_str(flat_getv(place, "country_code", nothing)),
        author_created_at = author === nothing ? missing :
                            flat_maybe_str(flat_getv(author, "created_at", nothing)),
        author_location = author === nothing ? missing :
                          flat_maybe_str(flat_getv(author, "location", nothing)),
        author_verified = author === nothing ? missing :
                          safe_bool(flat_getv(author, "verified", nothing)),
        author_protected = author === nothing ? missing :
                           safe_bool(flat_getv(author, "protected", nothing)),
        author_followers_count = _metric(author_pm, "followers_count"),
        author_following_count = _metric(author_pm, "following_count"),
        author_tweet_count = _metric(author_pm, "tweet_count"),
        author_listed_count = _metric(author_pm, "listed_count"),
        author_like_count = _metric(author_pm, "like_count"),
        author_media_count = _metric(author_pm, "media_count"),
        edit_history_tweet_ids =
            edit_history === nothing ? missing :
            flat_join_str([safe_str(x; default = "") for x in edit_history], sep),
        is_edit_eligible = edit_controls === nothing ? missing :
                           safe_bool(flat_getv(edit_controls, "is_edit_eligible", nothing)),
        editable_until = edit_controls === nothing ? missing :
                         flat_maybe_str(flat_getv(edit_controls, "editable_until", nothing)),
        edits_remaining = edit_controls === nothing ? missing :
                          safe_int(flat_getv(edit_controls, "edits_remaining", nothing)),
        entities_json = flat_json_str(ents),
        context_annotations_json = flat_json_str(context_annotations),
        referenced_tweets_json = flat_json_str(rts),
        attachments_json = flat_json_str(attachments),
        public_metrics_json = flat_json_str(pm),
        original_public_metrics_json = flat_json_str(original_pm),
        author_user_json = flat_json_str(author),
        media_json = isempty(media_objs) ? missing : flat_json_str(media_objs),
        place_json = flat_json_str(place),
    )
end

# ★ 変更: offset 境界合わせをより保守的に
function ensure_offset_boundary!(io::IO, offset::Int)
    if offset <= 0
        try
            seekstart(io)
        catch
        end
        return
    end

    # EOF 超えを避ける（IOStream想定）
    try
        offset = min(offset, filesize(io))
    catch
        # noop
    end

    if offset <= 0
        try
            seekstart(io)
        catch
        end
        return
    end

    try
        seek(io, offset)
        seek(io, offset - 1)
        prev = read(io, UInt8)
        seek(io, offset)
        if prev != UInt8('\n')
            try
                readline(io)  # 途中行を破棄して次の行頭へ
            catch
            end
        end
    catch
        # 落ちない優先
        try
            seek(io, offset)
        catch
        end
    end
    return
end

function _same_jsonl_path(a::AbstractString, b::AbstractString)::Bool
    try
        return abspath(a) == abspath(b)
    catch
        return String(a) == String(b)
    end
end

function _looks_like_json_unexpected_eof(e)::Bool
    msg = lowercase(sprint(showerror, e))
    return occursin("unexpectedeof", msg) ||
           occursin("unexpected eof", msg) ||
           occursin("unexpected end", msg) ||
           occursin("unexpected end-of-file", msg)
end

function _is_active_partial_jsonl_error(
    e,
    jsonl::AbstractString,
    active_path::Union{Nothing,String},
    line_end_offset::Int,
    file_end_offset::Int,
)::Bool
    active_path === nothing && return false
    _same_jsonl_path(jsonl, active_path) || return false
    line_end_offset == file_end_offset || return false
    return _looks_like_json_unexpected_eof(e)
end

# ★ 追加: Arrow append の “stream format only” を安全復旧
function _arrow_append_safe!(path::String, batch)
    try
        Arrow.append(path, batch)
        return
    catch e
        msg = sprint(showerror, e)
        if e isa ArgumentError && occursin("arrow stream format", msg)
            @warn "Arrow file is not stream format; rewriting to stream for append" path =
                path

            old_bytes = read(path)
            old_tbl = Arrow.Table(IOBuffer(old_bytes))

            tmp = path * ".tmp"
            Arrow.write(tmp, old_tbl; file = false)   # 既存を stream 化
            Arrow.append(tmp, batch)                # 追記
            mv(tmp, path; force = true)               # 置換
            return
        end
        rethrow()
    end
end

function _convert_flat_outputs_from_jsonl_paths(
    cfg::SearchConfig,
    jsonl_paths::Vector{String},
    state_path::AbstractString;
    csv_path::AbstractString,
    arrow_path::AbstractString,
    sep::AbstractString = ";",
    active_path::Union{Nothing,String} = nothing,
)
    validate!(cfg)
    mkpath(cfg.out_dir)

    isempty(jsonl_paths) && error("JSONL not found")

    st = load_state(state_path)
    st === nothing && (st = CollectorState())

    offset = cfg.convert_incremental ? st.converted_jsonl_offset : 0

    if offset > 0
        if cfg.emit_csv && !isfile(csv_path)
            @warn "CSV missing while incremental offset>0; resetting offset to 0"
            offset = 0
        end
        if cfg.emit_arrow && !isfile(arrow_path)
            @warn "Arrow missing while incremental offset>0; resetting offset to 0"
            offset = 0
        end
    end

    fsz = sum(stat(p).size for p in jsonl_paths)
    if offset > fsz
        @warn "Converted offset beyond JSONL size; resetting to 0" offset = offset size =
            fsz
        offset = 0
    end

    if offset == 0
        cfg.emit_csv && isfile(csv_path) && rm(csv_path; force = true)
        cfg.emit_arrow && isfile(arrow_path) && rm(arrow_path; force = true)
    end

    batch = TweetRow[]
    sizehint!(batch, cfg.convert_batch_size)
    new_offset = offset
    last_good_offset = offset
    wrote_arrow = false

    pending_tweets = Vector{Any}()
    users_cache = Dict{String,Any}()
    media_cache = Dict{String,Any}()
    places_cache = Dict{String,Any}()
    included_tweets_cache = Dict{String,Any}()

    # page マーカーが無い古いJSONL用の安全弁
    MAX_PENDING_WITHOUT_PAGE = 50_000

    function write_arrow_flat_batch!()
        if !wrote_arrow
            if cfg.arrow_append && isfile(arrow_path)
                _arrow_append_safe!(String(arrow_path), batch)
            else
                !cfg.arrow_append && isfile(arrow_path) && rm(arrow_path; force = true)
                Arrow.write(arrow_path, batch; file = false)
            end
        else
            _arrow_append_safe!(String(arrow_path), batch)
        end
        wrote_arrow = true
    end

    function write_nonempty_batch!()
        isempty(batch) && return
        cfg.emit_csv && CSV.write(csv_path, batch; append = isfile(csv_path))
        cfg.emit_arrow && write_arrow_flat_batch!()
        empty!(batch)
    end

    function write_batch_if_full!()
        length(batch) < cfg.convert_batch_size && return
        write_nonempty_batch!()
    end

    function flush_pending!()
        isempty(pending_tweets) && return
        for tdata in pending_tweets
            push!(
                batch,
                tweet_to_row(
                    tdata;
                    sep = sep,
                    users = users_cache,
                    media = media_cache,
                    places = places_cache,
                    included_tweets = included_tweets_cache,
                ),
            )
            write_batch_if_full!()
        end
        empty!(pending_tweets)
        empty!(users_cache)
        empty!(media_cache)
        empty!(places_cache)
        empty!(included_tweets_cache)
    end

    last_kind = :none

    function handle_flat_record!(obj)
        kind = safe_str(_json_get(obj, "kind", nothing); default = "")
        if kind == "tweet"
            if length(pending_tweets) >= MAX_PENDING_WITHOUT_PAGE
                flush_pending!()
            end
            if last_kind in (:include, :page) && !isempty(pending_tweets)
                flush_pending!()
            end
            data = _json_get(obj, "data", nothing)
            data === nothing || push!(pending_tweets, data)
            last_kind = :tweet
        elseif startswith(kind, "include:")
            suf = replace(kind, "include:" => "")
            d = _json_get(obj, "data", nothing)
            if suf == "users"
                uid = safe_str(flat_getv(d, "id", nothing); default = "")
                !isempty(uid) && (users_cache[uid] = d)
            elseif suf == "media"
                mk = safe_str(flat_getv(d, "media_key", nothing); default = "")
                !isempty(mk) && (media_cache[mk] = d)
            elseif suf == "places"
                pid = safe_str(flat_getv(d, "id", nothing); default = "")
                !isempty(pid) && (places_cache[pid] = d)
            elseif suf == "tweets"
                tid = safe_str(flat_getv(d, "id", nothing); default = "")
                !isempty(tid) && (included_tweets_cache[tid] = d)
            end
            last_kind = :include
        elseif kind == "page"
            flush_pending!()
            last_kind = :page
        end
    end

    file_base_offset = 0
    stop_at_partial = false

    for jsonl in jsonl_paths
        file_size = stat(jsonl).size
        if offset >= file_base_offset + file_size
            file_base_offset += file_size
            continue
        end
        file_offset = max(offset - file_base_offset, 0)
        open(jsonl, "r") do io
            ensure_offset_boundary!(io, file_offset)
            last_good_offset = max(last_good_offset, file_base_offset + position(io))
            new_offset = last_good_offset

            while !eof(io)
                line_start_offset = file_base_offset + position(io)
                line = readline(io)
                line_end_offset = file_base_offset + position(io)

                s = strip(line)
                if isempty(s)
                    last_good_offset = line_end_offset
                    new_offset = last_good_offset
                    continue
                end

                obj = try
                    JSON3.read(s)
                catch e
                    if _is_active_partial_jsonl_error(
                        e,
                        jsonl,
                        active_path,
                        line_end_offset,
                        file_base_offset + file_size,
                    )
                        @warn "JSONL partial final line; will retry on next conversion" path =
                            jsonl offset = line_start_offset
                        new_offset = last_good_offset
                        stop_at_partial = true
                        break
                    end
                    @warn "JSONL parse error (skip)" exception = e path = jsonl
                    last_good_offset = line_end_offset
                    new_offset = last_good_offset
                    continue
                end

                handle_flat_record!(obj)
                last_good_offset = line_end_offset
                new_offset = last_good_offset
            end
        end
        file_base_offset += file_size
        stop_at_partial && break
    end

    flush_pending!()

    write_nonempty_batch!()

    st.converted_jsonl_offset = new_offset
    st.converted_at = string(Dates.now(Dates.UTC))
    save_state(state_path, st)

    @info "Conversion done" csv = cfg.emit_csv arrow = cfg.emit_arrow new_offset =
        new_offset csv_path = csv_path arrow_path = arrow_path
    return (
        converted_offset = new_offset,
        converted_at = st.converted_at,
        csv_path = csv_path,
        arrow_path = arrow_path,
    )
end

function _convert_outputs_from_jsonl_paths(
    cfg::SearchConfig,
    jsonl_paths::Vector{String},
    state_path::AbstractString;
    active_path::Union{Nothing,String} = nothing,
)
    return _convert_flat_outputs_from_jsonl_paths(
        cfg,
        jsonl_paths,
        state_path;
        csv_path = out_csv(cfg),
        arrow_path = out_arrow(cfg),
        active_path = active_path,
    )
end

function convert_outputs(cfg::SearchConfig)
    jsonl = out_jsonl(cfg)
    isfile(jsonl) || error("JSONL not found: $jsonl")
    return _convert_outputs_from_jsonl_paths(cfg, [jsonl], out_state(cfg))
end

function convert_outputs(cfg::StreamConfig)
    validate!(cfg)
    paths = stream_jsonl_paths(cfg)
    isempty(paths) && error("JSONL not found: $(out_jsonl(cfg))")
    scfg = as_search_config(cfg)
    return _convert_outputs_from_jsonl_paths(
        scfg,
        paths,
        out_state(scfg);
        active_path = out_jsonl(cfg),
    )
end

# =========================================================
# Wide変換（includes: users/media/places/tweets の結合）
#   - ページマーカー "kind=page" で flush する（メモリ安定）
#   - page が無い古いJSONLでも安全弁でOOMを回避
# =========================================================
out_state_wide(cfg::SearchConfig) =
    joinpath(cfg.out_dir, "$(cfg.task_name).wide.state.json")
out_csv_wide(cfg::SearchConfig) = joinpath(cfg.out_dir, "$(cfg.task_name).wide.csv")
out_arrow_wide(cfg::SearchConfig) = joinpath(cfg.out_dir, "$(cfg.task_name).wide.arrow")
out_state_wide(cfg::StreamConfig) =
    joinpath(cfg.out_dir, "$(cfg.task_name).wide.state.json")
out_csv_wide(cfg::StreamConfig) = joinpath(cfg.out_dir, "$(cfg.task_name).wide.csv")
out_arrow_wide(cfg::StreamConfig) = joinpath(cfg.out_dir, "$(cfg.task_name).wide.arrow")

function _convert_outputs_wide_from_jsonl_paths(
    cfg::SearchConfig,
    jsonl_paths::Vector{String},
    state_path::AbstractString;
    sep::AbstractString = ";",
    active_path::Union{Nothing,String} = nothing,
)
    return _convert_flat_outputs_from_jsonl_paths(
        cfg,
        jsonl_paths,
        state_path;
        csv_path = out_csv_wide(cfg),
        arrow_path = out_arrow_wide(cfg),
        sep = sep,
        active_path = active_path,
    )
end

function convert_outputs_wide(cfg::SearchConfig; sep::AbstractString = ";")
    jsonl = out_jsonl(cfg)
    isfile(jsonl) || error("JSONL not found: $jsonl")
    return _convert_outputs_wide_from_jsonl_paths(
        cfg,
        [jsonl],
        out_state_wide(cfg);
        sep = sep,
    )
end

function convert_outputs_wide(cfg::StreamConfig; sep::AbstractString = ";")
    validate!(cfg)
    paths = stream_jsonl_paths(cfg)
    isempty(paths) && error("JSONL not found: $(out_jsonl(cfg))")
    scfg = as_search_config(cfg)
    return _convert_outputs_wide_from_jsonl_paths(
        scfg,
        paths,
        out_state_wide(scfg);
        sep = sep,
        active_path = out_jsonl(cfg),
    )
end

end # module
