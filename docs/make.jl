using XPostCollector
using Documenter

DocMeta.setdocmeta!(XPostCollector, :DocTestSetup, :(using XPostCollector); recursive=true)

makedocs(;
    modules=[XPostCollector],
    authors="Nakajima, Yukihiro <yukihiro@sfc.keio.ac.jp>",
    sitename="XPostCollector.jl",
    format=Documenter.HTML(;
        canonical="https://N-Yukihiro.github.io/XPostCollector.jl",
        edit_link="master",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/N-Yukihiro/XPostCollector.jl",
    devbranch="master",
)
