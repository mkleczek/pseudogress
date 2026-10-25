//! Record mode: backends that forward to each class's upstream PostgreSQL and
//! record what it answers.

use std::time::Duration;

use bytes::BytesMut;
use pgwire::messages::PgWireFrontendMessage;

use crate::{
    config::Config,
    session::{Backend, Connector},
};

#[cfg(test)]
mod tests;

pub struct Record {}

impl Record {
    /// Every class needs an upstream.
    pub fn new(config: &Config) -> Result<Self, String> {
        let _ = config;
        todo!()
    }

    /// Writes every class's recording. Called once, at shutdown.
    pub fn write(&self) -> Result<(), String> {
        todo!()
    }
}

impl Connector for Record {
    type Backend = Recorder;

    async fn connect(&self, class: usize, database: &str, user: &str) -> Result<Recorder, String> {
        let _ = (class, database, user);
        todo!()
    }
}

pub struct Recorder {}

impl Backend for Recorder {
    fn parameter_statuses(&self) -> &[(String, String)] {
        todo!()
    }

    async fn batch(&mut self, batch: &[PgWireFrontendMessage], out: &mut BytesMut) -> Duration {
        let _ = (batch, out);
        todo!()
    }

    fn cancelled(&mut self, out: &mut BytesMut) {
        let _ = out;
        todo!()
    }
}
