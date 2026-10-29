//! A class at run time: what its connections read, and its metrics.

use std::sync::Arc;

use arc_swap::ArcSwap;
use serde::Serialize;

use crate::{config, metrics::ClassMetrics, registry::Registry};

pub struct Class {
    pub name: String,
    /// The pattern of the databases it is for.
    pub databases: String,
    pub source: Source,
    pub users: Vec<String>,
    /// Without it, passwords are derived.
    pub password: Option<String>,
    /// Connections load it once per batch; the admin API replaces it.
    pub settings: ArcSwap<config::Settings>,
    pub metrics: ClassMetrics,
}

/// Where a class came from.
#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    Config,
    /// Loaded on demand from a class service.
    Service,
}

/// Every class: the configuration's in its order, then the ones added. A
/// database's class is an index into it.
pub type Classes = Arc<Registry<Class>>;

impl Class {
    /// The classes of `configs`, in their order, with room for `room` more.
    pub fn registry(configs: &[config::Class], room: usize) -> Classes {
        let classes = Registry::with_capacity(configs.len() + room);
        for config in configs {
            classes
                .push(Class::new(config))
                .expect("there is room for the given classes");
        }
        Arc::new(classes)
    }

    /// A class of the configuration.
    pub fn new(config: &config::Class) -> Self {
        Class {
            name: config.name.clone(),
            databases: config.databases.clone(),
            source: Source::Config,
            users: config.users.clone(),
            password: config.password.clone(),
            settings: ArcSwap::from_pointee(config.settings.clone()),
            metrics: ClassMetrics::default(),
        }
    }

    /// A class that a class service returned.
    pub fn from_service(config: &config::Class) -> Self {
        Class {
            source: Source::Service,
            ..Class::new(config)
        }
    }
}
