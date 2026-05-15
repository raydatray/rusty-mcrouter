use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("config must define exactly one of `route` or `routes`; both were provided")]
    BothRouteAndRoutes,

    #[error("config must define exactly one of `route` or `routes`; neither was provided")]
    MissingRoute,
}
