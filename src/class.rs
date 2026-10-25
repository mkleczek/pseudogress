//! A class at run time: what its connections read, and its metrics.

use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::{config, metrics::ClassMetrics};

pub struct Class {
    pub name: String,
    pub users: Vec<String>,
    /// Without it, passwords are derived.
    pub password: Option<String>,
    /// Connections load it once per batch; the admin API replaces it.
    pub settings: ArcSwap<config::Settings>,
    pub metrics: ClassMetrics,
}

/// Every class, in configuration order. A database's class is an index into
/// it.
pub type Classes = Arc<[Class]>;

impl Class {
    pub fn new(config: &config::Class) -> Self {
        Class {
            name: config.name.clone(),
            users: config.users.clone(),
            password: config.password.clone(),
            settings: ArcSwap::from_pointee(config.settings.clone()),
            metrics: ClassMetrics::default(),
        }
    }
}
