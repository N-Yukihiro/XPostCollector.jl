@testset "Aqua.jl" begin
    Aqua.test_all(XPostCollector)
end

@testset "JET.jl" begin
    JET.test_package(XPostCollector; target_modules = (XPostCollector,), toplevel_logger = nothing)
end
