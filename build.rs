fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure().compile_protos(
        &[
            "proto/tsz.proto",
            "proto/tsql.proto",
            "proto/config.proto",
            "proto/collection.proto",
            "proto/query.proto",
        ],
        &["proto"],
    )?;
    Ok(())
}
