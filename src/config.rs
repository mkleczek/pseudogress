//! The configuration file: where to listen, and the classes of virtual
//! databases.

use std::{
    collections::HashSet,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::recording;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Addresses that accept PostgreSQL connections.
    pub listen: Vec<SocketAddr>,
    /// Address of the admin HTTP API.
    pub admin: Option<SocketAddr>,
    /// Secret that derived passwords are made from.
    #[serde(default)]
    pub secret: String,
    /// Without it, SSLRequest is refused.
    pub tls: Option<Tls>,
    /// Matched in order against the database a client connects to.
    #[serde(rename = "class")]
    pub classes: Vec<Class>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    /// PEM file with the certificate chain.
    pub certificate: PathBuf,
    /// PEM file with the private key.
    pub key: PathBuf,
}

/// A class of virtual databases: a `[[class]]` table of the configuration, or
/// the answer of a class service, which leaves out `recording`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[cfg_attr(test, schemars(title = "Pseudogress class"))]
#[serde(deny_unknown_fields)]
pub struct Class {
    /// Identifies the class. A class from a class service is recorded in
    /// `<name>.jsonl`, which restricts its name.
    #[cfg_attr(
        test,
        schemars(
            regex(pattern = crate::service::NAME_PATTERN),
            length(max = crate::service::MAX_NAME_LENGTH)
        )
    )]
    pub name: String,
    /// Pattern of database names, where `*` matches any characters. A class
    /// service's must match the database that was asked about.
    pub databases: String,
    /// Users that may connect.
    pub users: Vec<String>,
    /// Password of every user. Without it, passwords are derived from the
    /// secret, the database and the user.
    pub password: Option<String>,
    /// Read by replay mode, written by record mode. The configuration must
    /// have it; a class service must not, as Pseudogress chooses it.
    #[serde(default)]
    #[cfg_attr(test, schemars(skip))]
    pub recording: PathBuf,
    /// The real PostgreSQL that record mode forwards to. A class service must
    /// have it, as its classes are for record mode.
    #[cfg_attr(test, schemars(with = "Upstream"))]
    pub upstream: Option<Upstream>,
    /// Statements whose answer doesn't depend on their parameters, by the
    /// start of their text: answered by their text alone, and recorded once.
    #[serde(default)]
    pub ignore_parameters: Vec<String>,
    /// Connections per database; more get FATAL 53300. Without it, there is no
    /// limit.
    pub max_connections: Option<u32>,
    /// Replaces a column of recorded rows with generated JSON.
    #[serde(rename = "override")]
    pub column_override: Option<ColumnOverride>,
    #[serde(default)]
    pub settings: Settings,
}

impl Class {
    /// Checks what both the configuration and a class service's answers must
    /// satisfy: valid settings and no empty prefix, which would match every
    /// statement.
    pub fn validate(&self) -> Result<(), String> {
        self.settings.validate()?;
        if self.ignore_parameters.iter().any(String::is_empty) {
            return Err("ignore_parameters: a prefix must not be empty".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    pub host: String,
    pub port: u16,
    /// The user that Pseudogress connects as.
    pub user: String,
    /// Needed when the upstream asks for a password.
    pub password: Option<String>,
    /// The database to connect to, where `{database}` is replaced by the
    /// virtual database's name.
    pub database: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ColumnOverride {
    /// Name of the column.
    pub column: String,
    /// Size of the generated JSON.
    pub bytes: usize,
}

/// Latency and faults, which the admin API can change while connections run.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields, default)]
pub struct Settings {
    /// Network round-trip time, added to every batch and every round trip of
    /// connection setup. A duration in humantime form, such as `500us` or
    /// `2s`.
    #[serde(with = "humantime_serde")]
    #[cfg_attr(test, schemars(with = "String"))]
    pub round_trip: Duration,
    /// Factor applied to recorded execution times.
    pub execution_scale: f64,
    /// A time drawn for every statement, added to its recorded one.
    pub execution: Option<Execution>,
    /// Time added to every statement for each MiB of its answer. A duration
    /// in humantime form, such as `500us` or `2s`.
    #[serde(with = "humantime_serde")]
    #[cfg_attr(test, schemars(with = "String"))]
    pub execution_per_mib: Duration,
    /// What goes wrong, and how often.
    pub faults: Faults,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            round_trip: Duration::ZERO,
            execution_scale: 1.0,
            execution: None,
            execution_per_mib: Duration::ZERO,
            faults: Faults::default(),
        }
    }
}

impl Settings {
    /// Checks what the types can't: the scale is not negative and the
    /// probabilities are between 0 and 1; the execution times of a statement
    /// are log-normal, so the median is above 0 and the 99th percentile is not
    /// below it.
    pub fn validate(&self) -> Result<(), String> {
        let scale = self.execution_scale;
        if !scale.is_finite() || scale < 0.0 {
            return Err(format!(
                "execution_scale: {scale} must be finite and not below 0"
            ));
        }
        let faults = &self.faults;
        for (name, probability) in [
            ("refuse", faults.refuse),
            ("hang", faults.hang),
            ("reject_password", faults.reject_password),
            ("reset", faults.reset),
            ("stall", faults.stall),
        ] {
            if !(0.0..=1.0).contains(&probability) {
                return Err(format!(
                    "faults: {name} ({probability}) must be between 0 and 1"
                ));
            }
        }
        match self.execution {
            Some(Execution { median, .. }) if median.is_zero() => {
                Err("execution: median must be above 0".into())
            }
            Some(Execution { median, p99 }) if p99 < median => Err(format!(
                "execution: p99 ({p99:?}) must not be below the median ({median:?})"
            )),
            _ => Ok(()),
        }
    }
}

/// The times of statements: log-normal, with a given median and 99th
/// percentile.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Execution {
    /// Half of the times are below it. A duration in humantime form, such as
    /// `500us` or `2s`.
    #[serde(with = "humantime_serde")]
    #[cfg_attr(test, schemars(with = "String"))]
    pub median: Duration,
    /// 99 of 100 times are below it, so it is not below the median. A
    /// duration in humantime form, such as `500us` or `2s`.
    #[serde(with = "humantime_serde")]
    #[cfg_attr(test, schemars(with = "String"))]
    pub p99: Duration,
}

/// Faults, each with its probability, between 0 and 1.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields, default)]
pub struct Faults {
    /// An accepted connection is closed at once.
    pub refuse: f64,
    /// An accepted connection never gets an answer.
    pub hang: f64,
    /// A correct password gets FATAL 28P01.
    pub reject_password: f64,
    /// A batch ends its connection with a reset.
    pub reset: f64,
    /// A batch's responses are held back for `stall_for`.
    pub stall: f64,
    /// How long a stalled batch's responses are held back. A duration in
    /// humantime form, such as `500us` or `2s`.
    #[serde(with = "humantime_serde")]
    #[cfg_attr(test, schemars(with = "String"))]
    pub stall_for: Duration,
    /// A batch whose execution time exceeds it fails with 57014. A duration
    /// in humantime form, such as `500us` or `2s`; without it, no batch fails
    /// for its time.
    #[serde(with = "humantime_serde")]
    #[cfg_attr(test, schemars(with = "Option<String>"))]
    pub statement_timeout: Option<Duration>,
}

/// The file of the classes that record mode took from a class service, in the
/// directory of the recordings: `[[class]]` tables like the configuration's,
/// with their recordings relative to the directory.
pub const CLASSES_FILE: &str = "classes.toml";

/// What `classes.toml` is made of.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ClassesFile {
    #[serde(default, rename = "class")]
    classes: Vec<Class>,
}

/// Appends the classes of `dir/classes.toml` to the configuration's, for
/// replay mode, with the recordings resolved against `dir`. The classes get
/// the checks of the configuration's: a name that an earlier class has is
/// an error. The file must exist.
pub fn load_classes(config: &mut Config, dir: &Path) -> Result<(), String> {
    let path = dir.join(CLASSES_FILE);
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut file: ClassesFile =
        toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut names = config
        .classes
        .iter()
        .map(|class| class.name.as_str())
        .collect();
    check(&file.classes, &mut names, &path)?;
    for class in &mut file.classes {
        class.recording = dir.join(&class.recording);
    }
    config.classes.append(&mut file.classes);
    Ok(())
}

/// Writes `dir/classes.toml`, for replay mode: the classes sorted by name,
/// with their recordings relative to `dir`. The file is written aside and
/// renamed, as the recordings are. Without classes it is an empty, valid file.
pub fn write_classes<'a>(
    dir: &Path,
    classes: impl IntoIterator<Item = &'a Class>,
) -> Result<(), String> {
    let mut classes: Vec<Class> = classes.into_iter().cloned().collect();
    classes.sort_by(|a, b| a.name.cmp(&b.name));
    for class in &mut classes {
        if let Ok(relative) = class.recording.strip_prefix(dir) {
            class.recording = relative.to_owned();
        }
    }
    let path = dir.join(CLASSES_FILE);
    let text = toml::to_string(&ClassesFile { classes })
        .map_err(|e| format!("{}: {e}", path.display()))?;
    let lines: Vec<String> = text.lines().map(str::to_owned).collect();
    let partial = recording::write_partial(&path, &lines)?;
    recording::rename(&partial, &path)
}

pub fn load(path: &Path) -> Result<Config, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let config: Config = toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    check(&config.classes, &mut HashSet::new(), path)?;
    Ok(config)
}

/// Checks the classes that `path` has, in order, after the classes whose
/// names are `names`, to which theirs are added.
fn check<'a>(
    classes: &'a [Class],
    names: &mut HashSet<&'a str>,
    path: &Path,
) -> Result<(), String> {
    for class in classes {
        let context = |e| format!("{}: class {}: {e}", path.display(), class.name);
        if !names.insert(&class.name) {
            return Err(context("name: an earlier class has it".into()));
        }
        // The classes of a class service get theirs from Pseudogress.
        if class.recording.as_os_str().is_empty() {
            return Err(context("recording: required".into()));
        }
        class.validate().map_err(context)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_readme_example() {
        let readme = include_str!("../README.md");
        let example = readme
            .split("```toml\n")
            .nth(1)
            .and_then(|rest| rest.split("```").next())
            .expect("the README has a TOML example");
        let config: Config = toml::from_str(example).unwrap();
        let class = &config.classes[0];
        assert_eq!(class.databases, "small-*");
        assert_eq!(class.settings.round_trip, Duration::from_micros(500));
        assert_eq!(class.settings.faults.stall_for, Duration::from_secs(2));
        assert_eq!(class.settings.faults.statement_timeout, None);
        assert_eq!(class.settings.execution, None);
        assert_eq!(class.settings.execution_per_mib, Duration::ZERO);
        assert!(class.ignore_parameters.is_empty());
    }

    #[test]
    fn settings_without_a_latency_model_are_the_defaults() {
        let settings: Settings = toml::from_str("").unwrap();
        assert_eq!(settings, Settings::default());
        assert_eq!(settings.execution, None);
        assert_eq!(settings.execution_per_mib, Duration::ZERO);
        assert_eq!(settings.validate(), Ok(()));
    }

    #[test]
    fn reads_a_latency_model() {
        let settings: Settings = toml::from_str(
            "execution = { median = \"1ms\", p99 = \"20ms\" }\n\
             execution_per_mib = \"250us\"",
        )
        .unwrap();
        assert_eq!(
            settings.execution,
            Some(Execution {
                median: Duration::from_millis(1),
                p99: Duration::from_millis(20),
            })
        );
        assert_eq!(settings.execution_per_mib, Duration::from_micros(250));
        assert_eq!(settings.validate(), Ok(()));
    }

    #[test]
    fn a_latency_model_needs_its_median_and_its_p99() {
        for model in ["{ median = \"1ms\" }", "{ p99 = \"1ms\" }"] {
            let text = format!("execution = {model}");
            assert!(toml::from_str::<Settings>(&text).is_err(), "{text}");
        }
    }

    #[test]
    fn validates_the_latency_model() {
        let model = |median: u64, p99: u64| Settings {
            execution: Some(Execution {
                median: Duration::from_millis(median),
                p99: Duration::from_millis(p99),
            }),
            ..Settings::default()
        };
        assert_eq!(model(1, 20).validate(), Ok(()));
        assert_eq!(model(5, 5).validate(), Ok(()));
        let error = model(0, 20).validate().unwrap_err();
        assert!(error.contains("median must be above 0"), "{error}");
        let error = model(0, 0).validate().unwrap_err();
        assert!(error.contains("median must be above 0"), "{error}");
        let error = model(20, 1).validate().unwrap_err();
        assert!(
            error.contains("p99 (1ms) must not be below the median (20ms)"),
            "{error}"
        );
    }

    #[test]
    fn loading_refuses_an_invalid_latency_model_naming_the_class() {
        let dir = std::env::temp_dir().join(format!("pseudogress-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pseudogress.toml");
        let text = |model: &str| {
            format!(
                "listen = []\n[[class]]\nname = \"slow\"\ndatabases = \"*\"\nusers = []\n\
                 recording = \"slow.jsonl\"\n[class.settings]\nexecution = {model}\n"
            )
        };
        std::fs::write(&path, text("{ median = \"1ms\", p99 = \"20ms\" }")).unwrap();
        assert!(load(&path).is_ok());
        std::fs::write(&path, text("{ median = \"20ms\", p99 = \"1ms\" }")).unwrap();
        let error = load(&path).unwrap_err();
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(error.contains("class slow"), "{error}");
        assert!(error.contains("p99"), "{error}");
    }

    #[test]
    fn validates_the_execution_scale() {
        let with = |scale: f64| Settings {
            execution_scale: scale,
            ..Settings::default()
        };
        for scale in [0.0, 0.5, 1.0, 25.0] {
            assert_eq!(with(scale).validate(), Ok(()), "{scale}");
        }
        for scale in [-1.0, -0.001, f64::NEG_INFINITY, f64::INFINITY, f64::NAN] {
            let error = with(scale).validate().unwrap_err();
            assert!(error.contains("execution_scale"), "{scale}: {error}");
        }
    }

    #[test]
    fn validates_the_probabilities_of_faults() {
        let with = |name: &str, p: f64| {
            let mut faults = Faults::default();
            *match name {
                "refuse" => &mut faults.refuse,
                "hang" => &mut faults.hang,
                "reject_password" => &mut faults.reject_password,
                "reset" => &mut faults.reset,
                "stall" => &mut faults.stall,
                _ => panic!("{name}"),
            } = p;
            Settings {
                faults,
                ..Settings::default()
            }
        };
        for name in ["refuse", "hang", "reject_password", "reset", "stall"] {
            for p in [0.0, 0.001, 1.0] {
                assert_eq!(with(name, p).validate(), Ok(()), "{name} {p}");
            }
            for p in [-0.1, 1.1, f64::NEG_INFINITY, f64::INFINITY, f64::NAN] {
                let error = with(name, p).validate().unwrap_err();
                assert!(error.contains("faults"), "{name} {p}: {error}");
                assert!(error.contains(name), "{name} {p}: {error}");
            }
        }
    }

    #[test]
    fn settings_read_from_a_file_are_validated() {
        let settings: Settings =
            toml::from_str("execution_scale = -1.0\n[faults]\nreset = 2.0").unwrap();
        assert!(settings.validate().is_err());
    }

    #[test]
    fn loading_refuses_an_invalid_scale_or_probability_naming_the_class() {
        let dir = std::env::temp_dir().join(format!("pseudogress-scale-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pseudogress.toml");
        let text = |settings: &str| {
            format!(
                "listen = []\n[[class]]\nname = \"flaky\"\ndatabases = \"*\"\nusers = []\n\
                 recording = \"flaky.jsonl\"\n[class.settings]\n{settings}\n"
            )
        };
        std::fs::write(
            &path,
            text("execution_scale = 0.5\nfaults = { reset = 0.5 }"),
        )
        .unwrap();
        assert!(load(&path).is_ok());
        let refused: Vec<_> = [
            ("execution_scale = -1.0", "execution_scale"),
            ("faults = { reset = 2.0 }", "reset"),
        ]
        .into_iter()
        .map(|(settings, field)| {
            std::fs::write(&path, text(settings)).unwrap();
            (field, load(&path))
        })
        .collect();
        std::fs::remove_dir_all(&dir).unwrap();
        for (field, result) in refused {
            let error = result.unwrap_err();
            assert!(error.contains("class flaky"), "{error}");
            assert!(error.contains(field), "{error}");
        }
    }

    #[test]
    fn loading_refuses_two_classes_with_the_same_name() {
        let dir = std::env::temp_dir().join(format!("pseudogress-names-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pseudogress.toml");
        let text = |second: &str| {
            format!(
                "listen = []\n\
                 [[class]]\nname = \"small\"\ndatabases = \"small-*\"\nusers = []\n\
                 recording = \"small.jsonl\"\n\
                 [[class]]\nname = \"{second}\"\ndatabases = \"large-*\"\nusers = []\n\
                 recording = \"large.jsonl\"\n"
            )
        };
        std::fs::write(&path, text("large")).unwrap();
        assert!(load(&path).is_ok());
        std::fs::write(&path, text("small")).unwrap();
        let error = load(&path).unwrap_err();
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(error.contains("class small"), "{error}");
        assert!(error.contains("name"), "{error}");
    }

    #[test]
    fn statements_ignore_no_parameters_by_default() {
        let class: Class =
            toml::from_str("name = \"c\"\ndatabases = \"*\"\nusers = []\nrecording = \"c.jsonl\"")
                .unwrap();
        assert!(class.ignore_parameters.is_empty());
    }

    #[test]
    fn reads_the_prefixes_of_statements_that_ignore_parameters() {
        let class: Class = toml::from_str(
            "name = \"c\"\ndatabases = \"*\"\nusers = []\nrecording = \"c.jsonl\"\n\
             ignore_parameters = [\"select set_config(\", \"select pg_sleep(\"]",
        )
        .unwrap();
        assert_eq!(
            class.ignore_parameters,
            ["select set_config(", "select pg_sleep("]
        );
    }

    #[test]
    fn loading_refuses_an_empty_prefix_naming_the_class() {
        let dir = std::env::temp_dir().join(format!("pseudogress-prefix-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pseudogress.toml");
        let text = |prefixes: &str| {
            format!(
                "listen = []\n[[class]]\nname = \"tenants\"\ndatabases = \"*\"\nusers = []\n\
                 recording = \"tenants.jsonl\"\nignore_parameters = {prefixes}\n"
            )
        };
        std::fs::write(&path, text("[\"select set_config(\"]")).unwrap();
        assert!(load(&path).is_ok());
        std::fs::write(&path, text("[\"select set_config(\", \"\"]")).unwrap();
        let error = load(&path).unwrap_err();
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(error.contains("class tenants"), "{error}");
        assert!(error.contains("ignore_parameters"), "{error}");
    }

    #[test]
    fn loading_refuses_a_class_without_a_recording_naming_the_class() {
        let dir =
            std::env::temp_dir().join(format!("pseudogress-recording-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pseudogress.toml");
        let text = |recording: &str| {
            format!(
                "listen = []\n[[class]]\nname = \"small\"\ndatabases = \"*\"\nusers = []\n{recording}"
            )
        };
        std::fs::write(&path, text("recording = \"small.jsonl\"\n")).unwrap();
        assert!(load(&path).is_ok());
        std::fs::write(&path, text("")).unwrap();
        let error = load(&path).unwrap_err();
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(error.contains("class small"), "{error}");
        assert!(error.contains("recording"), "{error}");
    }

    #[test]
    fn a_class_has_no_recording_until_it_is_given_one() {
        let class: Class = toml::from_str("name = \"c\"\ndatabases = \"*\"\nusers = []").unwrap();
        assert_eq!(class.recording, PathBuf::new());
    }

    /// What the file of the service's classes is made of.
    #[derive(Debug, PartialEq, Deserialize, Serialize)]
    struct Classes {
        #[serde(rename = "class")]
        classes: Vec<Class>,
    }

    #[test]
    fn classes_written_as_toml_read_back_equal() {
        let minimal = Class {
            name: "minimal".into(),
            databases: "minimal-*".into(),
            users: vec![],
            password: None,
            recording: "minimal.jsonl".into(),
            upstream: None,
            ignore_parameters: vec![],
            max_connections: None,
            column_override: None,
            settings: Settings::default(),
        };
        let full = Class {
            name: "full".into(),
            databases: "full-*".into(),
            users: vec!["authenticator".into(), "anon".into()],
            password: Some("secret".into()),
            recording: "full.jsonl".into(),
            upstream: Some(Upstream {
                host: "db.internal".into(),
                port: 6432,
                user: "postgres".into(),
                password: Some("upstream secret".into()),
                database: "{database}".into(),
            }),
            ignore_parameters: vec!["select set_config(".into()],
            max_connections: Some(60),
            column_override: Some(ColumnOverride {
                column: "payload".into(),
                bytes: 4096,
            }),
            settings: Settings {
                round_trip: Duration::from_micros(500),
                execution_scale: 0.5,
                execution: Some(Execution {
                    median: Duration::from_millis(1),
                    p99: Duration::from_millis(20),
                }),
                execution_per_mib: Duration::from_micros(250),
                faults: Faults {
                    refuse: 0.01,
                    hang: 0.02,
                    reject_password: 0.03,
                    reset: 0.04,
                    stall: 0.05,
                    stall_for: Duration::from_secs(2),
                    statement_timeout: Some(Duration::from_secs(30)),
                },
            },
        };
        let classes = Classes {
            classes: vec![minimal, full],
        };
        let text = toml::to_string(&classes).unwrap();
        assert_eq!(text.matches("[[class]]").count(), 2, "{text}");
        assert!(text.contains("recording = \"full.jsonl\""), "{text}");
        assert_eq!(toml::from_str::<Classes>(&text).unwrap(), classes, "{text}");
        let empty = Classes { classes: vec![] };
        let text = toml::to_string(&empty).unwrap();
        assert_eq!(toml::from_str::<Classes>(&text).unwrap(), empty, "{text}");
    }

    #[test]
    fn refuses_unknown_settings() {
        let error = toml::from_str::<Settings>("round_trp = \"1ms\"").unwrap_err();
        assert!(error.to_string().contains("round_trp"), "{error}");
    }

    // The file of the classes of a class service.

    /// A directory for one test: tests run in parallel, so they can't share.
    fn temp_dir(test: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("pseudogress-classes-{}-{test}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn configuration(classes: &str) -> Config {
        toml::from_str(&format!("listen = []\n{classes}")).unwrap()
    }

    const APP: &str = "[[class]]\nname = \"app\"\ndatabases = \"app-*\"\nusers = []\n\
                       recording = \"app.jsonl\"\n";

    /// A class of a class service, recorded in `recording`.
    fn service_class(name: &str, recording: &Path) -> Class {
        Class {
            name: name.into(),
            databases: format!("{name}-*"),
            users: vec!["client".into()],
            password: Some("secret".into()),
            recording: recording.into(),
            upstream: Some(Upstream {
                host: "127.0.0.1".into(),
                port: 5432,
                user: "postgres".into(),
                password: None,
                database: "{database}".into(),
            }),
            ignore_parameters: vec![],
            max_connections: None,
            column_override: None,
            settings: Settings::default(),
        }
    }

    fn classes_file(dir: &Path) -> PathBuf {
        dir.join(CLASSES_FILE)
    }

    #[test]
    fn loading_classes_appends_them_after_the_configurations_with_recordings_against_the_directory()
    {
        let dir = temp_dir("append");
        std::fs::write(
            classes_file(&dir),
            "[[class]]\nname = \"b\"\ndatabases = \"b-*\"\nusers = [\"client\"]\n\
             recording = \"b.jsonl\"\n\
             [[class]]\nname = \"a\"\ndatabases = \"a-*\"\nusers = []\n\
             recording = \"/elsewhere/a.jsonl\"\n",
        )
        .unwrap();
        let mut config = configuration(APP);

        load_classes(&mut config, &dir).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();

        let loaded: Vec<_> = config
            .classes
            .iter()
            .map(|class| (class.name.as_str(), class.recording.clone()))
            .collect();
        assert_eq!(
            loaded,
            [
                ("app", PathBuf::from("app.jsonl")),
                ("b", dir.join("b.jsonl")),
                ("a", PathBuf::from("/elsewhere/a.jsonl")),
            ]
        );
    }

    #[test]
    fn a_classes_file_without_classes_adds_none() {
        let dir = temp_dir("none");
        let mut config = configuration(APP);
        for text in ["", "class = []\n"] {
            std::fs::write(classes_file(&dir), text).unwrap();
            load_classes(&mut config, &dir).unwrap();
        }
        std::fs::remove_dir_all(&dir).unwrap();
        assert_eq!(config.classes.len(), 1);
    }

    #[test]
    fn loading_classes_needs_the_file() {
        let dir = temp_dir("missing");
        let mut config = configuration(APP);
        let error = load_classes(&mut config, &dir).unwrap_err();
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(error.contains("classes.toml"), "{error}");
        assert_eq!(config.classes.len(), 1);
    }

    #[test]
    fn loading_classes_refuses_what_the_configuration_refuses_naming_the_file_and_the_class() {
        let dir = temp_dir("refused");
        let tenants = |fields: &str| {
            format!("[[class]]\nname = \"tenants\"\ndatabases = \"t-*\"\nusers = []\n{fields}")
        };
        let recorded = tenants("recording = \"t.jsonl\"\n");
        for (configuration_text, file, reasons) in [
            // The name of a class of the configuration.
            (
                recorded.clone(),
                recorded.clone(),
                vec!["class tenants", "name"],
            ),
            // The name of an earlier class of the file.
            (
                APP.to_owned(),
                recorded.clone() + &recorded,
                vec!["class tenants", "name"],
            ),
            (
                APP.to_owned(),
                tenants(""),
                vec!["class tenants", "recording"],
            ),
            (
                APP.to_owned(),
                tenants("recording = \"t.jsonl\"\n[class.settings]\nexecution_scale = -1.0\n"),
                vec!["class tenants", "execution_scale"],
            ),
            (
                APP.to_owned(),
                tenants("recording = \"t.jsonl\"\nignore_parameters = [\"\"]\n"),
                vec!["class tenants", "ignore_parameters"],
            ),
            (
                APP.to_owned(),
                tenants("recording = \"t.jsonl\"\nfrobnicate = 1\n"),
                vec!["frobnicate"],
            ),
            (APP.to_owned(), "listen = []\n".to_owned(), vec!["listen"]),
            (APP.to_owned(), "[[class\n".to_owned(), vec![]),
        ] {
            std::fs::write(classes_file(&dir), &file).unwrap();
            let mut config = configuration(&configuration_text);
            let error = load_classes(&mut config, &dir).unwrap_err();
            assert!(error.contains("classes.toml"), "{file}: {error}");
            for reason in reasons {
                assert!(error.contains(reason), "{file}: {error}");
            }
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// What `classes.toml` is made of, as the tests read it.
    #[derive(Deserialize)]
    struct File {
        #[serde(rename = "class")]
        classes: Vec<Class>,
    }

    fn read_file(dir: &Path) -> File {
        let text = std::fs::read_to_string(classes_file(dir)).unwrap();
        toml::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}"))
    }

    #[test]
    fn writing_classes_sorts_them_by_name_and_makes_the_recordings_relative() {
        let dir = temp_dir("write");
        let classes = [
            service_class("zeta", &dir.join("zeta.jsonl")),
            service_class("alpha", &dir.join("alpha.jsonl")),
            service_class("Mid-1", &dir.join("Mid-1.jsonl")),
        ];

        write_classes(&dir, &classes).unwrap();

        let file = read_file(&dir);
        let written: Vec<_> = file
            .classes
            .iter()
            .map(|class| (class.name.as_str(), class.recording.to_str().unwrap()))
            .collect();
        assert_eq!(
            written,
            [
                ("Mid-1", "Mid-1.jsonl"),
                ("alpha", "alpha.jsonl"),
                ("zeta", "zeta.jsonl")
            ]
        );
        // The rest of a class is as it was.
        for class in &file.classes {
            assert_eq!(class, &service_class(&class.name, &class.recording));
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn written_classes_are_loaded_with_their_recordings_in_the_directory() {
        let dir = temp_dir("roundtrip");
        let classes = [
            service_class("b", &dir.join("b.jsonl")),
            service_class("a", &dir.join("a.jsonl")),
        ];
        write_classes(&dir, &classes).unwrap();
        let mut config = configuration(APP);

        load_classes(&mut config, &dir).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();

        assert_eq!(config.classes.len(), 3);
        assert_eq!(config.classes[1], classes[1]);
        assert_eq!(config.classes[2], classes[0]);
    }

    #[test]
    fn writing_no_classes_makes_an_empty_valid_file() {
        let dir = temp_dir("empty");
        write_classes(&dir, &[]).unwrap();
        let mut config = configuration(APP);

        load_classes(&mut config, &dir).unwrap();
        let written = read_file(&dir);
        std::fs::remove_dir_all(&dir).unwrap();

        assert!(written.classes.is_empty());
        assert_eq!(config.classes.len(), 1);
    }

    #[test]
    fn writing_classes_replaces_the_file_and_leaves_nothing_beside_it() {
        let dir = temp_dir("replace");
        write_classes(&dir, &[service_class("old", &dir.join("old.jsonl"))]).unwrap();
        write_classes(&dir, &[service_class("new", &dir.join("new.jsonl"))]).unwrap();

        let file = read_file(&dir);
        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        std::fs::remove_dir_all(&dir).unwrap();

        assert_eq!(file.classes.len(), 1);
        assert_eq!(file.classes[0].name, "new");
        assert_eq!(names, ["classes.toml"]);
    }

    #[test]
    fn writing_classes_to_a_directory_that_is_not_one_is_an_error() {
        let dir = temp_dir("notadir");
        let file = dir.join("file");
        std::fs::write(&file, "").unwrap();
        let error = write_classes(&file, &[]).unwrap_err();
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(error.contains("file"), "{error}");
    }
}
