//! Replay mode: backends that answer from the classes' recordings.

use std::time::Duration;

use bytes::BytesMut;
use pgwire::messages::PgWireFrontendMessage;

use crate::{
    class::Classes,
    config::Config,
    session::{Backend, Connector},
};

#[cfg(test)]
mod tests;

pub struct Replay {}

impl Replay {
    /// Reads and indexes every class's recording.
    pub fn load(config: &Config, classes: Classes) -> Result<Self, String> {
        let _ = (config, classes);
        todo!()
    }
}

impl Connector for Replay {
    type Backend = Replayer;

    async fn connect(&self, class: usize, database: &str, user: &str) -> Result<Replayer, String> {
        let _ = (class, database, user);
        todo!()
    }
}

pub struct Replayer {}

impl Backend for Replayer {
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
