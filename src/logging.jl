# =========================================================
# Logging: Base Loggingだけで動く DualLogger
# =========================================================
const _LOG_IO = Ref{Union{Nothing,IO}}(nothing)

struct DualLogger <: AbstractLogger
    io_console::IO
    io_file::IO
    level::LogLevel
end

Logging.min_enabled_level(l::DualLogger) = l.level
Logging.catch_exceptions(::DualLogger) = true

function Logging.shouldlog(l::DualLogger, level, _module, group, id)
    return level >= l.level
end

function Logging.handle_message(
    l::DualLogger,
    level,
    message,
    _module,
    group,
    id,
    file,
    line;
    kwargs...,
)
    ts = Dates.format(Dates.now(Dates.UTC), LOG_TS_FMT) * "Z"
    lvl = string(level)
    msg = string(message)

    function write_one(io::IO)
        print(io, ts, " [", lvl, "] ", msg)
        if file !== nothing && line !== nothing
            print(io, " (", file, ":", line, ")")
        end
        for (k, v) in kwargs
            print(io, " ", k, "=", v)
        end
        println(io)
        flush(io)
    end

    write_one(l.io_console)
    write_one(l.io_file)
    return nothing
end

function setup_logging(cfg; level::LogLevel = Logging.Info)
    mkpath(cfg.log_dir)

    if _LOG_IO[] !== nothing
        try
            close(_LOG_IO[])
        catch
        end
        _LOG_IO[] = nothing
    end

    io = open(out_log(cfg), "a")
    _LOG_IO[] = io
    global_logger(DualLogger(stderr, io, level))
    return io
end
