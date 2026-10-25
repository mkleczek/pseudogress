//! The admin HTTP API, for scripts that drive a test: metrics, notifications,
//! terminations, the open databases and the classes' settings.
//!
//! - `GET /metrics`
//! - `GET /databases/{name}`
//! - `POST /notify` with `{"channel", "payload"}` and a target
//! - `POST /terminate` with a target
//! - `GET` and `PUT /classes/{name}/settings`
//!
//! A target is `{"database": name}` or `{"class": name}`, optionally with a
//! `"fraction"` of the class's open databases.

use std::sync::Arc;

use tokio::net::TcpListener;

use crate::{class::Classes, databases::Databases};

#[cfg(test)]
mod tests;

pub async fn serve(
    listener: TcpListener,
    classes: Classes,
    databases: Arc<Databases>,
) -> std::io::Result<()> {
    let _ = (listener, classes, databases);
    todo!()
}
