use crate::tsdb2::{
    config_service_server::ConfigServiceServer, tsz_collection_server::TszCollectionServer,
};
use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tonic::transport::Server;

mod bucketer;
mod clock;
mod config;
mod counter;
mod distribution;
mod event_metric;
mod exporter;
mod f64;
mod fields;
mod server;

pub mod tsz {
    tonic::include_proto!("tsz");
}

pub mod tsql {
    tonic::include_proto!("tsql");
}

pub mod tsdb2 {
    tonic::include_proto!("tsdb2");
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The local address the server will listen on, e.g. `[::1]:8080`.
    #[arg(long)]
    local_address: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config_service_impl = Arc::new(config::ConfigServiceImpl::default());
    let config_service = config::ConfigService::new(config_service_impl.clone());
    let time_series_service = server::TimeSeriesService::new(config_service_impl);

    let builder = Server::builder()
        .add_service(ConfigServiceServer::new(config_service))
        .add_service(TszCollectionServer::new(time_series_service));

    println!("listening on {}", args.local_address);
    builder.serve(args.local_address.parse()?).await?;

    Ok(())
}
