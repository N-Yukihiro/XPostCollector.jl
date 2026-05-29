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
