# =========================================================
# API endpoints
# =========================================================
const API_BASE_URL_DEFAULT = "https://api.x.com"
const PATH_RECENT = "/2/tweets/search/recent"
const PATH_ALL = "/2/tweets/search/all"
const PATH_STREAM = "/2/tweets/search/stream"
const PATH_STREAM_RULES = "/2/tweets/search/stream/rules"
const PATH_USAGE = "/2/usage/tweets"
const PATH_CONNECTIONS = "/2/connections"

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
connections_url(cfg) = api_url(cfg.api_base_url, PATH_CONNECTIONS)

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

const STREAM_LEAN_FIELDS = Dict{String,String}(
    "tweet.fields" => join(
        [
            "id",
            "text",
            "created_at",
            "lang",
            "author_id",
            "public_metrics",
        ],
        ",",
    ),
    "expansions" => "author_id",
    "user.fields" => "id,name,username,verified,protected,public_metrics",
)

const STREAM_MINIMAL_FIELDS = Dict{String,String}(
    "tweet.fields" => "id,text,created_at,lang,author_id,public_metrics",
)

# =========================================================
# Limits
# =========================================================
const MAX_RESULTS_RECENT = 100
# v2 full archive は 500 のことが多い（プランで差があるので定数化）
const MAX_RESULTS_ALL = 500
