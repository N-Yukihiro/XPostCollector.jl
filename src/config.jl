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
