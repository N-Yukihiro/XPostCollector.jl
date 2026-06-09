using XPostCollector
using Documenter

DocMeta.setdocmeta!(XPostCollector, :DocTestSetup, :(using XPostCollector); recursive=true)

function default_branch_name()
    env_branch = String(strip(get(ENV, "GITHUB_DEFAULT_BRANCH", "")))
    !isempty(env_branch) && return env_branch

    ref = try
        String(strip(read(`git symbolic-ref --short refs/remotes/origin/HEAD`, String)))
    catch
        ""
    end
    prefix = "origin/"
    branch = startswith(ref, prefix) ? String(ref[(lastindex(prefix)+1):end]) : ref
    !isempty(branch) && return branch

    return try
        String(strip(read(`git branch --show-current`, String)))
    catch
        ""
    end
end

const DEFAULT_BRANCH = default_branch_name()
isempty(DEFAULT_BRANCH) &&
    error("Unable to determine the default branch; set GITHUB_DEFAULT_BRANCH")

makedocs(;
    modules=[XPostCollector],
    authors="Nakajima, Yukihiro <yukihiro@sfc.keio.ac.jp>",
    sitename="XPostCollector.jl",
    format=Documenter.HTML(;
        canonical="https://N-Yukihiro.github.io/XPostCollector.jl",
        edit_link=DEFAULT_BRANCH,
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/N-Yukihiro/XPostCollector.jl",
    devbranch=DEFAULT_BRANCH,
)
