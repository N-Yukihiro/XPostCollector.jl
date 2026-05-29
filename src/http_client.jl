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
