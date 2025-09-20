use crate::proto;
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
pub struct ConfigServiceImpl {
    // TODO
}

#[derive(Debug)]
pub struct ConfigService {
    config_service_impl: Arc<ConfigServiceImpl>,
}

impl ConfigService {
    pub fn new(config_service_impl: Arc<ConfigServiceImpl>) -> Self {
        Self {
            config_service_impl,
        }
    }
}

#[tonic::async_trait]
impl proto::tsdb2::config_service_server::ConfigService for ConfigService {
    async fn define_metrics(
        &self,
        _request: Request<proto::tsz::DefineMetricsRequest>,
    ) -> Result<Response<proto::tsz::DefineMetricsResponse>, Status> {
        todo!()
    }

    async fn force_define_metrics(
        &self,
        _request: Request<proto::tsdb2::ForceDefineMetricsRequest>,
    ) -> Result<Response<proto::tsdb2::ForceDefineMetricsResponse>, Status> {
        todo!()
    }

    async fn get_module(
        &self,
        _request: Request<proto::tsdb2::GetModuleRequest>,
    ) -> Result<Response<proto::tsdb2::GetModuleResponse>, Status> {
        todo!()
    }

    async fn set_module(
        &self,
        _request: Request<proto::tsdb2::SetModuleRequest>,
    ) -> Result<Response<proto::tsdb2::SetModuleResponse>, Status> {
        todo!()
    }

    async fn delete_module(
        &self,
        _request: Request<proto::tsdb2::DeleteModuleRequest>,
    ) -> Result<Response<proto::tsdb2::DeleteModuleResponse>, Status> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO
}
