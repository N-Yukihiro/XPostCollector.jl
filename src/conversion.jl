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
