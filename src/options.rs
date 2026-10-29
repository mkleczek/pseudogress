//! The command line: the mode and the configuration, and the flags that
//! make classes come from a class service or from the recordings.

use std::path::PathBuf;

use crate::service::Template;

pub const USAGE: &str = concat!(
    "usage: pseudogress replay CONFIG [--recordings DIR]\n",
    "       pseudogress record CONFIG [--class-url URL --recordings DIR [--max-classes N]]"
);

/// How many classes a class service may add, unless `--max-classes` says.
pub const DEFAULT_MAX_CLASSES: usize = 256;

#[derive(Debug)]
pub struct Options {
    pub config: PathBuf,
    pub mode: Mode,
}

/// A mode, with what its flags say. A flag that doesn't go with the mode is a
/// usage error, so a mode has only its own.
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
pub fn parse(args: &[String]) -> Result<Options, String> {
    let [_, mode, config, flags @ ..] = args else {
        return Err(USAGE.to_owned());
    };
    if !matches!(mode.as_str(), "replay" | "record") {
        return Err(USAGE.to_owned());
    }
    let (mut class_url, mut recordings, mut max_classes) = (None, None, None);
    let mut flags = flags.iter();
    while let Some(flag) = flags.next() {
        if !flag.starts_with('-') {
            return Err(USAGE.to_owned());
        }
        let slot = match flag.as_str() {
            "--class-url" => &mut class_url,
            "--recordings" => &mut recordings,
            "--max-classes" => &mut max_classes,
            _ => return Err(usage_error(format!("unknown option {flag}"))),
        };
        let value = flags
            .next()
            .ok_or_else(|| usage_error(format!("{flag} needs a value")))?;
        if slot.replace(value).is_some() {
            return Err(usage_error(format!("{flag} is given twice")));
        }
    }
    let mode = if mode == "replay" {
        for (flag, given) in [
            ("--class-url", class_url.is_some()),
            ("--max-classes", max_classes.is_some()),
        ] {
            if given {
                return Err(usage_error(format!("{flag} is for record mode")));
            }
        }
        Mode::Replay {
            recordings: recordings.map(PathBuf::from),
        }
    } else {
        let service = match (class_url, recordings) {
            (Some(url), Some(recordings)) => {
                let url = Template::parse(url)
                    .map_err(|reason| usage_error(format!("--class-url: {reason}")))?;
                let max_classes = match max_classes {
                    None => DEFAULT_MAX_CLASSES,
                    Some(n) => n.parse().map_err(|_| {
                        usage_error(format!("--max-classes: {n:?} is not a number"))
                    })?,
                };
                // With none, every lookup would fail.
                if max_classes == 0 {
                    return Err(usage_error("--max-classes must be at least 1"));
                }
                Some(Service {
                    url,
                    recordings: recordings.into(),
                    max_classes,
                })
            }
            (Some(_), None) => return Err(usage_error("--class-url needs --recordings")),
            (None, Some(_)) => return Err(usage_error("--recordings needs --class-url")),
            (None, None) if max_classes.is_some() => {
                return Err(usage_error("--max-classes needs --class-url"));
            }
            (None, None) => None,
        };
        Mode::Record { service }
    };
    Ok(Options {
        config: config.into(),
        mode,
    })
}

fn usage_error(reason: impl std::fmt::Display) -> String {
    format!("pseudogress: {reason}\n{USAGE}")
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "http://classes.internal:8080/classes/{database}?user={user}";

    fn args(args: &[&str]) -> Vec<String> {
        let program = "pseudogress";
        std::iter::once(program)
            .chain(args.iter().copied())
            .map(str::to_owned)
            .collect()
    }

    fn parsed(arguments: &[&str]) -> Options {
        parse(&args(arguments)).unwrap()
    }

    fn refused(arguments: &[&str]) -> String {
        parse(&args(arguments)).unwrap_err()
    }

    /// The service of record mode with the flags `--class-url` and
    /// `--recordings`.
    fn service(arguments: &[&str]) -> Service {
        match parsed(arguments).mode {
            Mode::Record {
                service: Some(service),
            } => service,
            mode => panic!("{mode:?}"),
        }
    }

    #[test]
    fn the_modes_without_flags_take_a_configuration_as_they_always_did() {
        let options = parsed(&["replay", "pseudogress.toml"]);
        assert_eq!(options.config, PathBuf::from("pseudogress.toml"));
        assert!(matches!(options.mode, Mode::Replay { recordings: None }));

        let options = parsed(&["record", "other.toml"]);
        assert_eq!(options.config, PathBuf::from("other.toml"));
        assert!(matches!(options.mode, Mode::Record { service: None }));
    }

    #[test]
    fn replay_takes_the_directory_of_recordings() {
        let options = parsed(&["replay", "c.toml", "--recordings", "recordings/dir"]);
        let Mode::Replay {
            recordings: Some(recordings),
        } = options.mode
        else {
            panic!("{options:?}");
        };
        assert_eq!(recordings, PathBuf::from("recordings/dir"));
    }

    #[test]
    fn record_takes_a_class_service_and_the_directory_of_recordings() {
        let service = service(&[
            "record",
            "c.toml",
            "--class-url",
            URL,
            "--recordings",
            "recordings",
        ]);
        assert_eq!(service.recordings, PathBuf::from("recordings"));
        assert_eq!(service.max_classes, 256);
        assert_eq!(service.max_classes, DEFAULT_MAX_CLASSES);
        // The URL is the parsed template: its values are encoded.
        let url = format!("{:?}", service.url.fill("a b", "u/1"));
        assert!(url.contains("classes.internal"), "{url}");
        assert!(url.contains("8080"), "{url}");
        assert!(url.contains("/classes/a%20b?user=u%2F1"), "{url}");
    }

    #[test]
    fn record_takes_the_most_classes_that_the_service_may_add() {
        let service = service(&[
            "record",
            "c.toml",
            "--class-url",
            URL,
            "--recordings",
            "r",
            "--max-classes",
            "12",
        ]);
        assert_eq!(service.max_classes, 12);
    }

    #[test]
    fn the_flags_come_in_any_order() {
        let service = service(&[
            "record",
            "c.toml",
            "--max-classes",
            "3",
            "--recordings",
            "r",
            "--class-url",
            URL,
        ]);
        assert_eq!(service.max_classes, 3);
        assert_eq!(service.recordings, PathBuf::from("r"));
    }

    #[test]
    fn arguments_that_are_not_a_mode_and_a_configuration_get_the_usage_only() {
        for arguments in [
            vec![],
            vec!["replay"],
            vec!["record"],
            vec!["frobnicate", "c.toml"],
            vec!["Replay", "c.toml"],
            vec!["replay", "c.toml", "extra"],
            vec!["record", "c.toml", "extra"],
        ] {
            assert_eq!(refused(&arguments), USAGE, "{arguments:?}");
        }
    }

    /// An error that says what is wrong, and then the usage.
    fn assert_refused(arguments: &[&str], reason: &str) {
        let error = refused(arguments);
        assert!(error.contains(reason), "{arguments:?}: {error}");
        assert!(error.ends_with(USAGE), "{arguments:?}: {error}");
    }

    #[test]
    fn an_unknown_option_is_refused() {
        assert_refused(
            &["replay", "c.toml", "--verbose"],
            "unknown option --verbose",
        );
        assert_refused(&["record", "c.toml", "-r", "dir"], "unknown option -r");
        // The value goes with its flag: it is not taken for a configuration.
        assert_refused(
            &["replay", "c.toml", "--recordings=dir"],
            "--recordings=dir",
        );
    }

    #[test]
    fn an_option_needs_its_value() {
        assert_refused(
            &["replay", "c.toml", "--recordings"],
            "--recordings needs a value",
        );
        assert_refused(
            &["record", "c.toml", "--recordings", "d", "--class-url"],
            "--class-url needs a value",
        );
        assert_refused(
            &["record", "c.toml", "--max-classes"],
            "--max-classes needs a value",
        );
    }

    #[test]
    fn an_option_is_given_once() {
        assert_refused(
            &["replay", "c.toml", "--recordings", "a", "--recordings", "b"],
            "--recordings is given twice",
        );
        assert_refused(
            &[
                "record",
                "c.toml",
                "--class-url",
                URL,
                "--recordings",
                "r",
                "--class-url",
                URL,
            ],
            "--class-url is given twice",
        );
        assert_refused(
            &[
                "record",
                "c.toml",
                "--class-url",
                URL,
                "--recordings",
                "r",
                "--max-classes",
                "1",
                "--max-classes",
                "2",
            ],
            "--max-classes is given twice",
        );
    }

    #[test]
    fn the_class_url_and_the_most_classes_are_for_record_mode_only() {
        assert_refused(
            &["replay", "c.toml", "--class-url", URL],
            "--class-url is for record mode",
        );
        assert_refused(
            &[
                "replay",
                "c.toml",
                "--recordings",
                "r",
                "--max-classes",
                "3",
            ],
            "--max-classes is for record mode",
        );
    }

    #[test]
    fn the_class_url_needs_the_directory_of_recordings() {
        assert_refused(
            &["record", "c.toml", "--class-url", URL],
            "--class-url needs --recordings",
        );
    }

    #[test]
    fn in_record_mode_the_directory_of_recordings_needs_the_class_url() {
        assert_refused(
            &["record", "c.toml", "--recordings", "r"],
            "--recordings needs --class-url",
        );
    }

    #[test]
    fn the_most_classes_need_the_class_url() {
        assert_refused(
            &["record", "c.toml", "--max-classes", "3"],
            "--max-classes needs --class-url",
        );
    }

    #[test]
    fn the_most_classes_is_a_number() {
        for value in ["many", "-1", "", "1.5", "0x10", "99999999999999999999999"] {
            let error = refused(&[
                "record",
                "c.toml",
                "--class-url",
                URL,
                "--recordings",
                "r",
                "--max-classes",
                value,
            ]);
            assert!(error.contains("--max-classes"), "{value:?}: {error}");
            assert!(error.contains(&format!("{value:?}")), "{value:?}: {error}");
            assert!(error.ends_with(USAGE), "{value:?}: {error}");
        }
    }

    #[test]
    fn the_most_classes_is_at_least_1_as_none_would_fail_every_lookup() {
        let error = refused(&[
            "record",
            "c.toml",
            "--class-url",
            URL,
            "--recordings",
            "r",
            "--max-classes",
            "0",
        ]);
        assert!(
            error.contains("--max-classes must be at least 1"),
            "{error}"
        );
        assert!(error.ends_with(USAGE), "{error}");
    }

    #[test]
    fn the_class_url_is_checked_when_the_command_line_is() {
        for (url, reason) in [
            ("https://classes.internal/{database}", "http://"),
            ("classes.internal/{database}", "http://"),
            ("http://classes.internal/classes", "{database}"),
            ("http://classes.internal/{databse}", "placeholder"),
            ("http://:80/{database}", "host"),
            ("http://classes.internal:0/{database}", "port"),
            ("", "http://"),
        ] {
            let error = refused(&["record", "c.toml", "--class-url", url, "--recordings", "r"]);
            assert!(error.contains("--class-url"), "{url:?}: {error}");
            assert!(error.contains(reason), "{url:?}: {error}");
            assert!(error.ends_with(USAGE), "{url:?}: {error}");
        }
    }
}
