//! A class at run time: what its connections read, and its metrics.

use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::{config, metrics::ClassMetrics, registry::Registry};

pub struct Class {
    pub name: String,
    pub users: Vec<String>,
    /// Without it, passwords are derived.
    pub password: Option<String>,
    /// Connections load it once per batch; the admin API replaces it.
    pub settings: ArcSwap<config::Settings>,
    pub metrics: ClassMetrics,
}

/// Every class: the configuration's in its order, then the ones added. A
/// database's class is an index into it.
pub type Classes = Arc<Registry<Class>>;

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
