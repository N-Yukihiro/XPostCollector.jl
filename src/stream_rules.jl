# =========================================================
# Filtered Stream
# =========================================================
function _stream_field_params(cfg::StreamConfig)
    if cfg.field_profile === :full
        params = copy(API_FIELDS)
    elseif cfg.field_profile === :lean
        params = copy(STREAM_LEAN_FIELDS)
    else
        params = copy(STREAM_MINIMAL_FIELDS)
    end

    if !cfg.write_includes
        for key in ("expansions", "user.fields", "media.fields", "place.fields", "poll.fields")
            pop!(params, key, nothing)
        end
    end
    return params
end

build_stream_params(cfg::StreamConfig) = begin
    params = Dict{String,String}()
    for (k, v) in _stream_field_params(cfg)
        params[k] = v
    end
    return params
end

function list_stream_connections(
    cfg::StreamConfig;
    client::XApiClient = XApiClient(),
    status::AbstractString = "active",
    endpoints::AbstractString = "filtered_stream",
    max_results::Integer = 10,
)
    validate!(cfg)
    headers = bearer_headers(client; user_agent = "julia-x-collector/stream")
    return list_stream_connections(
        cfg,
        client,
        headers;
        status = status,
        endpoints = endpoints,
        max_results = max_results,
    )
end

function list_stream_connections(
    cfg::StreamConfig,
    client::XApiClient,
    fetch_json::Function;
    status::AbstractString = "active",
    endpoints::AbstractString = "filtered_stream",
    max_results::Integer = 10,
)
    return list_stream_connections(
        cfg,
        bearer_headers(client; user_agent = "julia-x-collector/stream"),
        fetch_json;
        status = status,
        endpoints = endpoints,
        max_results = max_results,
    )
end

function list_stream_connections(
    cfg::StreamConfig,
    client::XApiClient,
    headers;
    status::AbstractString = "active",
    endpoints::AbstractString = "filtered_stream",
    max_results::Integer = 10,
)
    return list_stream_connections(
        cfg,
        headers,
        (method, url, headers, params; kwargs...) -> fetch_stream_json_with_retry(
            client,
            method,
            url,
            headers,
            params;
            kwargs...,
        );
        status = status,
        endpoints = endpoints,
        max_results = max_results,
    )
end

function list_stream_connections(
    cfg::StreamConfig,
    headers;
    status::AbstractString = "active",
    endpoints::AbstractString = "filtered_stream",
    max_results::Integer = 10,
)
    return list_stream_connections(
        cfg,
        XApiClient(),
        headers;
        status = status,
        endpoints = endpoints,
        max_results = max_results,
    )
end

function list_stream_connections(
    cfg::StreamConfig,
    headers,
    fetch_json::Function;
    status::AbstractString = "active",
    endpoints::AbstractString = "filtered_stream",
    max_results::Integer = 10,
)
    validate!(cfg)
    params = Dict{String,String}(
        "status" => String(status),
        "max_results" => string(clamp(Int(max_results), 1, 100)),
        "connection.fields" => "id,endpoint_name,connected_at,disconnected_at,disconnect_reason,client_ip",
    )
    endpoint_filter = strip(String(endpoints))
    !isempty(endpoint_filter) && (params["endpoints"] = endpoint_filter)
    return fetch_json("GET", connections_url(cfg), headers, params)
end

function list_stream_rules(cfg::StreamConfig; client::XApiClient = XApiClient())
    validate!(cfg)
    return list_stream_rules(
        cfg,
        client,
        bearer_headers(client; user_agent = "julia-x-collector/stream"),
    )
end

function list_stream_rules(cfg::StreamConfig, client::XApiClient, headers)
    return list_stream_rules(
        cfg,
        headers,
        (method, url, headers, params; kwargs...) -> fetch_stream_json_with_retry(
            client,
            method,
            url,
            headers,
            params;
            kwargs...,
        ),
    )
end

function list_stream_rules(cfg::StreamConfig, client::XApiClient, fetch_json::Function)
    return list_stream_rules(
        cfg,
        bearer_headers(client; user_agent = "julia-x-collector/stream"),
        fetch_json,
    )
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
    return ensure_stream_rule!(cfg, XApiClient())
end

function ensure_stream_rule!(cfg::StreamConfig, client::XApiClient)
    validate!(cfg)
    headers = bearer_headers(client; user_agent = "julia-x-collector/stream")
    return ensure_stream_rule!(cfg, client, headers)
end

function ensure_stream_rule!(cfg::StreamConfig, client::XApiClient, headers)
    return ensure_stream_rule!(
        cfg,
        headers,
        (method, url, headers, params; kwargs...) -> fetch_stream_json_with_retry(
            client,
            method,
            url,
            headers,
            params;
            kwargs...,
        ),
    )
end

function ensure_stream_rule!(cfg::StreamConfig, client::XApiClient, fetch_json::Function)
    return ensure_stream_rule!(
        cfg,
        bearer_headers(client; user_agent = "julia-x-collector/stream"),
        fetch_json,
    )
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
