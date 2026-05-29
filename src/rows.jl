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
