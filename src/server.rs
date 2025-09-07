use crate::config::ConfigServiceImpl;
use crate::tsdb2;
use crate::tsz;
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
#[tonic::async_trait]
impl tsdb2::tsz_collection_server::TszCollection for TimeSeriesService {
    async fn define_metrics(
        &self,
        _request: Request<tsz::DefineMetricsRequest>,
    ) -> Result<Response<tsz::DefineMetricsResponse>, Status> {
        todo!()
    }

    async fn write_entity(
        &self,
        _request: Request<tsdb2::WriteEntityRequest>,
    ) -> Result<Response<tsdb2::WriteEntityResponse>, Status> {
        todo!()
    }

    async fn read_schedules(
        &self,
        _request: Request<tsdb2::ReadSchedulesRequest>,
    ) -> Result<Response<tsdb2::ReadSchedulesResponse>, Status> {
        todo!()
    }

    async fn write_target(
        &self,
        _request: Request<tsdb2::WriteTargetRequest>,
    ) -> Result<Response<tsdb2::WriteTargetResponse>, Status> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO
}
