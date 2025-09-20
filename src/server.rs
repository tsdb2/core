use crate::config::ConfigServiceImpl;
use crate::proto;
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct TimeSeriesService {
    config_service_impl: Arc<ConfigServiceImpl>,
}

impl TimeSeriesService {
    pub fn new(config_service_impl: Arc<ConfigServiceImpl>) -> Self {
        Self {
            config_service_impl,
        }
    }
}

#[tonic::async_trait]
impl proto::tsdb2::tsz_collection_server::TszCollection for TimeSeriesService {
    async fn define_metrics(
        &self,
        _request: Request<proto::tsz::DefineMetricsRequest>,
    ) -> Result<Response<proto::tsz::DefineMetricsResponse>, Status> {
        todo!()
    }

    async fn write_entity(
        &self,
        _request: Request<proto::tsdb2::WriteEntityRequest>,
    ) -> Result<Response<proto::tsdb2::WriteEntityResponse>, Status> {
        todo!()
    }

    async fn read_schedules(
        &self,
        _request: Request<proto::tsdb2::ReadSchedulesRequest>,
    ) -> Result<Response<proto::tsdb2::ReadSchedulesResponse>, Status> {
        todo!()
    }

    async fn write_target(
        &self,
        _request: Request<proto::tsdb2::WriteTargetRequest>,
    ) -> Result<Response<proto::tsdb2::WriteTargetResponse>, Status> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO
}
