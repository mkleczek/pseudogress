//! The command line: the mode and the configuration, and the flags that
//! make classes come from a class service or from the recordings.

use std::path::PathBuf;

use crate::service::Template;

#[allow(dead_code, reason = "nothing loads classes on demand yet")]
pub const USAGE: &str = concat!(
    "usage: pseudogress replay CONFIG [--recordings DIR]\n",
    "       pseudogress record CONFIG [--class-url URL --recordings DIR [--max-classes N]]"
);

/// How many classes a class service may add, unless `--max-classes` says.
#[allow(dead_code, reason = "nothing loads classes on demand yet")]
pub const DEFAULT_MAX_CLASSES: usize = 256;

#[allow(dead_code, reason = "nothing loads classes on demand yet")]
#[derive(Debug)]
pub struct Options {
    pub config: PathBuf,
    pub mode: Mode,
}

/// What the flags that a mode takes say about it, and nothing else: a flag
/// that doesn't go with the mode is a usage error.
#[allow(dead_code, reason = "nothing loads classes on demand yet")]
#[derive(Debug)]
pub enum Mode {
    /// With `--recordings DIR`, it also serves the classes of
    /// `DIR/classes.toml`, which record mode wrote.
    Replay { recordings: Option<PathBuf> },
    /// With `--class-url`, it fetches the classes of databases that no class
    /// of the configuration matches.
    Record { service: Option<Service> },
}

/// A class service, and where its classes are recorded.
#[allow(dead_code, reason = "nothing loads classes on demand yet")]
#[derive(Debug)]
pub struct Service {
    pub url: Template,
    /// Where the classes of the service are recorded, and `classes.toml` goes.
    pub recordings: PathBuf,
    /// How many classes the service may add.
    pub max_classes: usize,
}

/// Parses the arguments of the process, the program's name first. The error is
/// what to print: the reason, if there is one, and the usage.
#[allow(dead_code, reason = "nothing loads classes on demand yet")]
pub fn parse(args: &[String]) -> Result<Options, String> {
    let _ = args;
    todo!()
}
