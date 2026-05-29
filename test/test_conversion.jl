# ---------------------------------------------------------
# ensure_offset_boundary!
# ---------------------------------------------------------
@testset "ensure_offset_boundary!" begin
    mktempdir() do dir
        p = joinpath(dir, "t.txt")
        open(p, "w") do io
            write(io, "aaa\nbbb\nccc\n")
        end

        open(p, "r") do io
            ensure_offset_boundary!(io, 2)
            @test readline(io) == "bbb"
        end

        open(p, "r") do io
            ensure_offset_boundary!(io, 4)
            @test readline(io) == "bbb"
        end
    end
end

# ---------------------------------------------------------
# convert_outputs
# ---------------------------------------------------------
@testset "convert_outputs incremental" begin
    with_temp_cfg(task = "conv") do cfg, dir
        jsonl = out_jsonl(cfg)
        state = out_state(cfg)
        csvp = out_csv(cfg)
        arrp = out_arrow(cfg)

        st = CollectorState()
        save_state(state, st)

        lines1 = [
            jsonl_tweet_line("1"),
            jsonl_include_line("users"),
            jsonl_page_line(1),
            jsonl_tweet_line("2"),
            jsonl_tweet_line("3"),
            jsonl_page_line(2),
        ]
        write_jsonl_lines(jsonl, lines1)

        convert_outputs(cfg)
        @test count_csv_rows(csvp) == 3
        @test count_arrow_rows(arrp) == 3

        st1 = load_state(state)
        @test st1 !== nothing
        @test st1.converted_jsonl_offset > 0

        lines2 = [
            jsonl_tweet_line("4"),
            jsonl_page_line(3),
            jsonl_tweet_line("5"),
            jsonl_page_line(4),
        ]
        write_jsonl_lines(jsonl, lines2)

        convert_outputs(cfg)
        @test count_csv_rows(csvp) == 5
        @test count_arrow_rows(arrp) == 5

        st2 = load_state(state)
        @test st2.converted_jsonl_offset > st1.converted_jsonl_offset
    end
end

@testset "convert_outputs resets offset if outputs missing" begin
    with_temp_cfg(task = "conv_reset") do cfg, dir
        with_logger(NullLogger()) do
            jsonl = out_jsonl(cfg)
            state = out_state(cfg)
            csvp = out_csv(cfg)
            arrp = out_arrow(cfg)

            write_jsonl_lines(
                jsonl,
                [jsonl_tweet_line("1"), jsonl_tweet_line("2"), jsonl_page_line(1)],
            )

            st = CollectorState()
            st.converted_jsonl_offset = 10
            save_state(state, st)

            @test !isfile(csvp)
            @test !isfile(arrp)

            convert_outputs(cfg)
            @test isfile(csvp)
            @test isfile(arrp)
            @test count_csv_rows(csvp) == 2
            @test count_arrow_rows(arrp) == 2
        end
    end
end

@testset "convert_outputs handles corrupt lines and non-tweet kinds" begin
    with_temp_cfg(task = "conv_robust") do cfg, dir
        with_logger(NullLogger()) do
            jsonl = out_jsonl(cfg)
            csvp = out_csv(cfg)

            lines = [
                jsonl_tweet_line("1"),
                "THIS IS NOT JSON",
                jsonl_include_line("users"),
                jsonl_page_line(1),
                jsonl_tweet_line("2"),
            ]
            write_jsonl_lines(jsonl, lines)

            convert_outputs(cfg)
            @test count_csv_rows(csvp) == 2
        end
    end
end

@testset "convert_outputs propagates writer failures without parse skip" begin
    with_temp_cfg(task = "conv_writer_failure") do cfg, dir
        jsonl = out_jsonl(cfg)
        state = out_state(cfg)
        arrp = out_arrow(cfg)
        cfg.emit_csv = false
        cfg.emit_arrow = true
        cfg.convert_batch_size = 1

        write_jsonl_lines(jsonl, [jsonl_tweet_line("1"), jsonl_page_line(1)])
        mkpath(arrp)

        logger = CollectLogger(String[])
        with_logger(logger) do
            @test_throws Exception convert_outputs(cfg)
        end

        st = load_state(state)
        @test st === nothing || st.converted_jsonl_offset == 0
        @test !any(==("JSONL parse error (skip)"), logger.messages)
    end
end

@testset "convert_outputs arrow overwrite mode" begin
    with_temp_cfg(task = "arrow_overwrite") do cfg, dir
        with_logger(NullLogger()) do
            jsonl = out_jsonl(cfg)
            arrp = out_arrow(cfg)

            write_jsonl_lines(
                jsonl,
                [jsonl_tweet_line("old1"), jsonl_tweet_line("old2"), jsonl_page_line(1)],
            )
            convert_outputs(cfg)
            @test count_arrow_rows(arrp) == 2

            write_jsonl_lines(
                jsonl,
                [
                    jsonl_tweet_line("new1"),
                    jsonl_tweet_line("new2"),
                    jsonl_page_line(2),
                    jsonl_tweet_line("new3"),
                    jsonl_page_line(3),
                ],
            )

            cfg.arrow_append = false
            convert_outputs(cfg)

            @test count_arrow_rows(arrp) == 3
            rows = collect(Tables.rows(Arrow.Table(IOBuffer(read(arrp)))))
            @test [String(r.tweet_id) for r in rows] == ["new1", "new2", "new3"]
        end
    end
end


# ---------------------------------------------------------
# tweet_to_row robustness
# ---------------------------------------------------------
@testset "tweet_to_row handles missing public_metrics" begin
    tw = Dict{String,Any}(
        "id" => "1",
        "created_at" => "2026-01-01T00:00:00Z",
        "author_id" => "10",
        "lang" => "en",
        "text" => "hi",
    )
    row = tweet_to_row(tw)
    @test row.tweet_id == "1"
    @test row.like_count === missing
    @test row.retweet_count === missing
    @test row.reply_count === missing
    @test row.quote_count === missing
    @test row.bookmark_count === missing
    @test row.impression_count === missing
    @test row.original_retweet_count === missing
end


@testset "tweet_to_row reads extended public_metrics" begin
    tw = Dict{String,Any}(
        "id" => "1",
        "created_at" => "2026-01-01T00:00:00Z",
        "author_id" => "10",
        "lang" => "en",
        "text" => "hi",
        "public_metrics" => Dict(
            "like_count" => 1,
            "retweet_count" => 2,
            "reply_count" => 3,
            "quote_count" => 4,
            "bookmark_count" => 5,
            "impression_count" => 6,
        ),
    )
    row = tweet_to_row(tw)
    @test row.bookmark_count == 5
    @test row.impression_count == 6
end

# ---------------------------------------------------------
# Fixtures (existing)
# ---------------------------------------------------------
@testset "convert_outputs converts fixture MIN (strict fields)" begin
    with_temp_cfg(task = "conv_fixture_min") do cfg, dir
        with_logger(NullLogger()) do
            jsonl = out_jsonl(cfg)
            csvp = out_csv(cfg)
            arrp = out_arrow(cfg)

            sample = read_fixture("x_recent_search_sample.min.json")
            resp = JSON3.read(sample)

            tw = resp["data"][1]
            inc_tw = resp["includes"]["tweets"][1]

            tweet_line = String(JSON3.write((; kind = "tweet", data = tw)))
            include_line = String(JSON3.write((; kind = "include:tweets", data = inc_tw)))
            page_line = String(
                JSON3.write((;
                    kind = "page",
                    data = Dict("page" => 1, "meta" => get(resp, "meta", Dict())),
                )),
            )

            write_jsonl_lines(jsonl, [tweet_line, include_line, page_line])

            convert_outputs(cfg)

            @test count_csv_rows(csvp) == 1
            @test count_arrow_rows(arrp) == 1

            csv_tbl = CSV.File(
                IOBuffer(read(csvp));
                types = Dict(
                    :tweet_id => String,
                    :author_id => String,
                    :created_at => String,
                    :lang => String,
                    :text => String,
                    :possibly_sensitive => Union{Missing,Bool},
                    :like_count => Union{Missing,Int},
                    :retweet_count => Union{Missing,Int},
                    :reply_count => Union{Missing,Int},
                    :quote_count => Union{Missing,Int},
                    :bookmark_count => Union{Missing,Int},
                    :impression_count => Union{Missing,Int},
                ),
            )
            row = first(csv_tbl)

            @test row.tweet_id == "1212092628029698048"
            @test row.created_at == "2019-12-31T19:26:16.000Z"
            @test row.author_id == "2244994945"
            @test row.lang == "en"
            @test row.possibly_sensitive == false
            @test row.like_count == 38
            @test row.retweet_count == 7
            @test row.reply_count == 3
            @test row.quote_count == 1
            @test row.bookmark_count === missing || row.bookmark_count isa Int
            @test row.impression_count === missing || row.impression_count isa Int
            @test occursin("best future version of our API", row.text)

            arrow_tbl = Arrow.Table(IOBuffer(read(arrp)))
            arow = first(Tables.rows(arrow_tbl))

            @test String(arow.tweet_id) == row.tweet_id
            @test String(arow.created_at) == row.created_at
            @test String(arow.author_id) == row.author_id
            @test String(arow.lang) == row.lang
            @test arow.possibly_sensitive == row.possibly_sensitive
            @test isequal(arow.like_count, row.like_count)
            @test isequal(arow.retweet_count, row.retweet_count)
            @test isequal(arow.reply_count, row.reply_count)
            @test isequal(arow.quote_count, row.quote_count)
            @test isequal(arow.bookmark_count, row.bookmark_count)
            @test isequal(arow.impression_count, row.impression_count)
            @test occursin("best future version of our API", String(arow.text))
        end
    end
end

@testset "convert_outputs converts fixture FULL (smoke + key fields)" begin
    with_temp_cfg(task = "conv_fixture_full") do cfg, dir
        with_logger(NullLogger()) do
            jsonl = out_jsonl(cfg)
            csvp = out_csv(cfg)
            arrp = out_arrow(cfg)

            sample = read_fixture("x_recent_search_sample.full.json")
            resp = JSON3.read(sample)

            tw = resp["data"][1]

            lines = String[]
            push!(lines, String(JSON3.write((; kind = "tweet", data = tw))))
            if haskey(resp, "includes") &&
               haskey(resp["includes"], "tweets") &&
               length(resp["includes"]["tweets"]) > 0
                inc_tw = resp["includes"]["tweets"][1]
                push!(
                    lines,
                    String(JSON3.write((; kind = "include:tweets", data = inc_tw))),
                )
            end
            push!(lines, jsonl_page_line(1))

            write_jsonl_lines(jsonl, lines)
            convert_outputs(cfg)

            @test count_csv_rows(csvp) == 1
            @test count_arrow_rows(arrp) == 1

            csv_tbl = CSV.File(
                IOBuffer(read(csvp));
                types = Dict(
                    :tweet_id => String,
                    :author_id => String,
                    :created_at => String,
                    :lang => String,
                    :text => String,
                ),
            )
            row = first(csv_tbl)

            @test !isempty(row.tweet_id)
            @test !isempty(row.author_id)
            @test !isempty(row.created_at)
            @test !isempty(row.text)
            @test !isempty(row.lang)
        end
    end
end

# ---------------------------------------------------------
# Wide conversion: page マーカーで flush + 必須カラム検証
# ---------------------------------------------------------
@testset "convert_outputs_wide flushes on page markers and keeps required fields" begin
    with_temp_cfg(task = "wide_page") do cfg, dir
        with_logger(NullLogger()) do
            cfg.emit_arrow = false

            jsonl = out_jsonl(cfg)
            csvw = out_csv_wide(cfg)

            t1 = Dict(
                "id" => "1",
                "author_id" => "10",
                "created_at" => "2026-01-01T00:00:00Z",
                "lang" => "en",
                "text" => "a",
            )
            u10 = Dict("id" => "10", "username" => "alice", "name" => "Alice")
            t2 = Dict(
                "id" => "2",
                "author_id" => "11",
                "created_at" => "2026-01-01T00:01:00Z",
                "lang" => "en",
                "text" => "b",
            )
            u11 = Dict("id" => "11", "username" => "bob", "name" => "Bob")

            lines = [
                String(JSON3.write((; kind = "tweet", data = t1))),
                String(JSON3.write((; kind = "include:users", data = u10))),
                jsonl_page_line(1),
                String(JSON3.write((; kind = "tweet", data = t2))),
                String(JSON3.write((; kind = "include:users", data = u11))),
                jsonl_page_line(2),
            ]
            write_jsonl_lines(jsonl, lines)

            convert_outputs_wide(cfg)

            @test isfile(csvw)
            @test count_csv_rows(csvw) == 2

            tbl = CSV.File(IOBuffer(read(csvw)))
            rows = collect(tbl)
            @test length(rows) == 2

            # 必須：tweet_id/author_id/created_at/text が欠損でない
            @test !ismissing(rows[1].tweet_id)
            @test !ismissing(rows[1].author_id)
            @test !ismissing(rows[1].created_at)
            @test !ismissing(rows[1].text)

            # includes があるケースなので username も埋まること
            @test rows[1].author_username == "alice"
            @test rows[2].author_username == "bob"
        end
    end
end

@testset "convert_outputs uses fixed flat schema and separates original metrics" begin
    with_temp_cfg(task = "flat_retweet_metrics") do cfg, dir
        with_logger(NullLogger()) do
            jsonl = out_jsonl(cfg)
            csvp = out_csv(cfg)
            arrp = out_arrow(cfg)

            metrics(rt, reply, like, qte, bookmark, impression) = Dict(
                "retweet_count" => rt,
                "reply_count" => reply,
                "like_count" => like,
                "quote_count" => qte,
                "bookmark_count" => bookmark,
                "impression_count" => impression,
            )

            retweet = Dict(
                "id" => "rt-1",
                "author_id" => "10",
                "created_at" => "2026-01-01T00:00:00Z",
                "lang" => "en",
                "text" => "RT @orig: original text",
                "public_metrics" => metrics(1, 2, 3, 4, 5, 6),
                "referenced_tweets" => Any[Dict("type" => "retweeted", "id" => "orig-1")],
            )
            quoted = Dict(
                "id" => "quote-1",
                "author_id" => "11",
                "created_at" => "2026-01-01T00:01:00Z",
                "lang" => "en",
                "text" => "quote text",
                "public_metrics" => metrics(7, 8, 9, 10, 11, 12),
                "referenced_tweets" => Any[Dict("type" => "quoted", "id" => "quoted-src")],
            )
            plain = Dict(
                "id" => "plain-1",
                "author_id" => "12",
                "created_at" => "2026-01-01T00:02:00Z",
                "lang" => "en",
                "text" => "plain text",
                "public_metrics" => metrics(13, 14, 15, 16, 17, 18),
            )
            original = Dict(
                "id" => "orig-1",
                "author_id" => "99",
                "created_at" => "2025-12-31T23:59:00Z",
                "lang" => "en",
                "text" => "original text",
                "public_metrics" => metrics(100, 200, 300, 400, 500, 600),
            )
            quoted_src = Dict(
                "id" => "quoted-src",
                "author_id" => "98",
                "created_at" => "2025-12-31T23:58:00Z",
                "lang" => "en",
                "text" => "quoted source",
                "public_metrics" => metrics(1000, 2000, 3000, 4000, 5000, 6000),
            )
            users = [
                Dict("id" => "10", "username" => "alice", "name" => "Alice"),
                Dict("id" => "11", "username" => "bob", "name" => "Bob"),
                Dict("id" => "12", "username" => "carol", "name" => "Carol"),
            ]

            lines = String[
                String(JSON3.write((; kind = "tweet", data = retweet))),
                String(JSON3.write((; kind = "tweet", data = quoted))),
                String(JSON3.write((; kind = "tweet", data = plain))),
                String(JSON3.write((; kind = "include:tweets", data = original))),
                String(JSON3.write((; kind = "include:tweets", data = quoted_src))),
            ]
            append!(lines, [String(JSON3.write((; kind = "include:users", data = u))) for u in users])
            push!(lines, jsonl_page_line(1))
            write_jsonl_lines(jsonl, lines)

            convert_outputs(cfg)

            csv_tbl = CSV.File(
                IOBuffer(read(csvp));
                types = Dict(
                    :tweet_id => String,
                    :author_id => String,
                    :author_username => String,
                    :referenced_tweet_ids => String,
                    :original_tweet_id => String,
                    :original_author_id => String,
                    :original_text => String,
                ),
            )
            arrow_tbl = Arrow.Table(IOBuffer(read(arrp)))
            @test collect(Tables.columnnames(csv_tbl)) == collect(TWEET_ROW_NAMES)
            @test collect(Tables.columnnames(arrow_tbl)) == collect(TWEET_ROW_NAMES)

            rows = collect(csv_tbl)
            @test length(rows) == 3
            @test rows[1].tweet_id == "rt-1"
            @test rows[1].author_username == "alice"
            @test rows[1].is_retweet == true
            @test rows[1].is_quote == false
            @test rows[1].retweet_count == 1
            @test rows[1].like_count == 3
            @test rows[1].original_tweet_id == "orig-1"
            @test rows[1].original_author_id == "99"
            @test rows[1].original_text == "original text"
            @test rows[1].original_retweet_count == 100
            @test rows[1].original_like_count == 300

            @test rows[2].is_retweet == false
            @test rows[2].is_quote == true
            @test rows[2].referenced_tweet_ids == "quoted-src"
            @test ismissing(rows[2].original_tweet_id)
            @test ismissing(rows[2].original_retweet_count)

            @test rows[3].is_retweet == false
            @test ismissing(rows[3].original_tweet_id)
            @test ismissing(rows[3].original_retweet_count)

            arows = collect(Tables.rows(arrow_tbl))
            @test String(arows[1].tweet_id) == "rt-1"
            @test arows[1].retweet_count == rows[1].retweet_count
            @test arows[1].original_retweet_count == rows[1].original_retweet_count
            @test ismissing(arows[2].original_retweet_count)
        end
    end
end
