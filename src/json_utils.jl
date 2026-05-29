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
