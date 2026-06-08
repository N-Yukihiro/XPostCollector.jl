# =========================================================
# Filtered Stream
# =========================================================
function bearer_headers(;
    user_agent::AbstractString = "julia-x-collector/repl",
    accept_encoding::Union{Nothing,AbstractString} = nothing,
)
    token = get(ENV, "BEARER_TOKEN", "")
    isempty(token) && error("BEARER_TOKEN is missing (env/.env)")
    headers = ["Authorization" => "Bearer $token", "User-Agent" => String(user_agent)]
    accept_encoding !== nothing &&
        push!(headers, "Accept-Encoding" => String(accept_encoding))
    return headers
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
