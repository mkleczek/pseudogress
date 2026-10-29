//! The recording of a class, in two files: record mode writes them and replay
//! mode reads them. Protocol messages are kept as the bytes PostgreSQL sent,
//! so replay never encodes a value.
//!
//! The recording file has one JSON object per line, one per entry, sorted and
//! without the entry's times. The order in which concurrent connections got to
//! ask and the times change on every run, so recording again would rewrite the
//! whole file: this way it changes only the lines whose statement or answer
//! changed, and the file can be kept in version control and reviewed as a
//! diff.
//!
//! Values of 64 KiB and more are zlib-compressed (`zlib:` and the base64 of
//! the compressed bytes), so that a recording of large answers fits in version
//! control: GitHub refuses files over 100 MB.
//!
//! The times are in the times file next to it (`small.times.jsonl` for
//! `small.jsonl`), one line per entry that has any, in the order of the
//! recording: `{"entry":"<hash>","exec_us":[...]}`, or `start_us` for a
//! Startup. The hash is the first 16 hex digits of the SHA-256 of the entry's
//! line in the recording, so that times of another recording are refused
//! rather than replayed against the wrong entries.

use std::{
    collections::HashMap,
    fs,
    io::{BufRead, BufReader, BufWriter, Write},
    mem,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Entry {
    /// What a new connection got.
    Startup {
        /// ParameterStatus messages, as name and value.
        parameters: Vec<(String, String)>,
        /// Times from the startup message to ReadyForQuery, in microseconds.
        /// Kept in the times file.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        start_us: Vec<u32>,
    },
    /// An Execute of a bound statement.
    Execute {
        sql: String,
        param_formats: Vec<i16>,
        #[serde(with = "base64_params")]
        params: Vec<Option<Bytes>>,
        result_formats: Vec<i16>,
        /// The answer to Describe of the portal: RowDescription or NoData.
        #[serde(with = "base64_bytes")]
        description: Bytes,
        /// The answer to Execute: DataRows, then CommandComplete,
        /// EmptyQueryResponse or ErrorResponse.
        #[serde(with = "base64_bytes")]
        rows: Bytes,
        /// Execution times seen, in microseconds. Kept in the times file.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        exec_us: Vec<u32>,
    },
    /// A Describe of a statement.
    Describe {
        sql: String,
        param_types: Vec<u32>,
        /// ParameterDescription, then RowDescription or NoData.
        #[serde(with = "base64_bytes")]
        response: Bytes,
    },
    /// A simple query.
    Query {
        sql: String,
        /// Every response before ReadyForQuery.
        #[serde(with = "base64_bytes")]
        response: Bytes,
        /// Execution times seen, in microseconds. Kept in the times file.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        exec_us: Vec<u32>,
    },
}

impl Entry {
    /// The times of an entry that has any, with the name of their field. They
    /// are not serialized: the times file keeps them.
    fn times(&mut self) -> Option<(&'static str, &mut Vec<u32>)> {
        match self {
            Entry::Startup { start_us, .. } => Some(("start_us", start_us)),
            Entry::Execute { exec_us, .. } | Entry::Query { exec_us, .. } => {
                Some(("exec_us", exec_us))
            }
            Entry::Describe { .. } => None,
        }
    }
}

/// A line of the times file.
#[derive(Deserialize, Serialize)]
struct Times {
    /// The hash of the entry's line in the recording.
    entry: String,
    /// The times, by the name of their field.
    #[serde(flatten)]
    times: HashMap<String, Vec<u32>>,
}

fn times_path(path: &Path) -> PathBuf {
    path.with_extension("times.jsonl")
}

/// What the times file calls a line of the recording.
fn hash(line: &str) -> String {
    Sha256::digest(line)
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The lines of a file that aren't empty, by their numbers.
fn lines(path: &Path) -> Result<Vec<(usize, String)>, String> {
    let error = |e: std::io::Error| format!("{}: {e}", path.display());
    let file = fs::File::open(path).map_err(error)?;
    let mut lines = Vec::new();
    for (number, line) in BufReader::new(file).lines().enumerate() {
        let line = line.map_err(error)?;
        if !line.is_empty() {
            lines.push((number + 1, line));
        }
    }
    Ok(lines)
}

pub fn read(path: &Path) -> Result<Vec<Entry>, String> {
    let recording = lines(path)?;
    let times_path = times_path(path);
    let mut times = HashMap::new();
    for (number, line) in lines(&times_path)? {
        let Times { entry, times: kept } = serde_json::from_str(&line)
            .map_err(|e| format!("{}:{number}: {e}", times_path.display()))?;
        times.insert(entry, kept);
    }
    let mut entries = Vec::new();
    for (number, line) in recording {
        let mut entry: Entry =
            serde_json::from_str(&line).map_err(|e| format!("{}:{number}: {e}", path.display()))?;
        if let Some((field, kept)) = entry.times() {
            *kept = times
                .get(&hash(&line))
                .and_then(|times| times.get(field))
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "{}: no times for {}:{number}: the times file doesn't match the recording",
                        times_path.display(),
                        path.display()
                    )
                })?;
        }
        entries.push(entry);
    }
    Ok(entries)
}

/// Replaces both files, so that readers never see a partial recording. Both
/// are written and synced aside before either is renamed, so that a failed
/// write (a full disk) leaves the previous pair. The recording goes last: it
/// is the one that says what the times are of.
pub fn write(path: &Path, entries: &[Entry]) -> Result<(), String> {
    let error = |e: serde_json::Error| format!("{}: {e}", path.display());
    let mut lines = Vec::new();
    for entry in entries {
        let mut entry = entry.clone();
        let times = entry
            .times()
            .map(|(field, times)| (field, mem::take(times)));
        lines.push((serde_json::to_string(&entry).map_err(error)?, times));
    }
    lines.sort();
    let mut recording = Vec::new();
    let mut times_lines = Vec::new();
    for (line, times) in lines {
        if let Some((field, times)) = times {
            times_lines.push(
                serde_json::to_string(&Times {
                    entry: hash(&line),
                    times: HashMap::from([(field.to_owned(), times)]),
                })
                .map_err(error)?,
            );
        }
        recording.push(line);
    }
    let times_path = times_path(path);
    let partial_times = write_partial(&times_path, &times_lines)?;
    let partial_recording = write_partial(path, &recording)?;
    rename(&partial_times, &times_path)?;
    rename(&partial_recording, path)
}

/// Writes lines next to `path`, where they wait to replace it.
pub(crate) fn write_partial(path: &Path, lines: &[String]) -> Result<PathBuf, String> {
    let error = |e: std::io::Error| format!("{}: {e}", path.display());
    let partial = path.with_extension("partial");
    let mut out = BufWriter::new(fs::File::create(&partial).map_err(error)?);
    for line in lines {
        out.write_all(line.as_bytes()).map_err(error)?;
        out.write_all(b"\n").map_err(error)?;
    }
    out.into_inner()
        .map_err(|e| error(e.into_error()))?
        .sync_all()
        .map_err(error)?;
    Ok(partial)
}

pub(crate) fn rename(partial: &Path, path: &Path) -> Result<(), String> {
    fs::rename(partial, path).map_err(|e| format!("{}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A directory for one test: tests run in parallel, so they can't share.
    fn temp_dir(test: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pseudogress-recording-{}-{test}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// An entry of every kind, in no particular order. `us` offsets the times.
    fn entries(us: u32) -> Vec<Entry> {
        let execute = |param: u8, exec_us| Entry::Execute {
            sql: "select $1".into(),
            param_formats: vec![1],
            params: vec![Some(Bytes::from(vec![0, 0, 0, param]))],
            result_formats: vec![1],
            description: Bytes::from_static(b"n\0\0\0\x04"),
            rows: Bytes::from_static(b"C\0\0\0\x0dSELECT 1\0"),
            exec_us,
        };
        vec![
            Entry::Startup {
                parameters: vec![("server_version".into(), "18.0".into())],
                start_us: vec![us, us + 1],
            },
            execute(2, vec![us + 2]),
            Entry::Describe {
                sql: "select $1".into(),
                param_types: vec![23],
                response: Bytes::from_static(b"t\0\0\0\x06\0\0n\0\0\0\x04"),
            },
            Entry::Query {
                sql: "select 1".into(),
                response: Bytes::from_static(b"C\0\0\0\x0dSELECT 1\0"),
                exec_us: vec![us + 3, us + 4],
            },
            execute(1, vec![us + 5]),
        ]
    }

    fn contents(path: &Path) -> (Vec<u8>, Vec<u8>) {
        (
            fs::read(path).unwrap(),
            fs::read(path.with_extension("times.jsonl")).unwrap(),
        )
    }

    #[test]
    fn writes_what_it_reads() {
        // In the order of the recording, which is sorted.
        let entries = vec![
            Entry::Execute {
                sql: "select $1".into(),
                param_formats: vec![1],
                params: vec![Some(Bytes::from_static(&[0, 0, 0, 1])), None],
                result_formats: vec![1],
                description: Bytes::from_static(b"n\0\0\0\x04"),
                rows: Bytes::from_static(b"C\0\0\0\x0dSELECT 1\0"),
                exec_us: vec![12, 15],
            },
            Entry::Startup {
                parameters: vec![("server_version".into(), "18.0".into())],
                start_us: vec![900],
            },
        ];
        let dir = temp_dir("round-trip");
        let path = dir.join("recording.jsonl");
        write(&path, &entries).unwrap();
        let read_back = read(&path);
        fs::remove_dir_all(&dir).unwrap();
        assert_eq!(read_back.unwrap(), entries);
    }

    #[test]
    fn the_times_file_has_a_line_per_entry_with_times() {
        let dir = temp_dir("format");
        let path = dir.join("recording.jsonl");
        let mut entries = entries(100);
        entries.retain(|entry| !matches!(entry, Entry::Execute { .. }));
        write(&path, &entries).unwrap();
        let (recording, times) = contents(&path);
        // The hashes are `printf %s <line> | shasum -a 256 | cut -c1-16`. The
        // Describe has no times.
        assert_eq!(
            String::from_utf8(recording).unwrap(),
            r#"{"kind":"describe","sql":"select $1","param_types":[23],"response":"dAAAAAYAAG4AAAAE"}
{"kind":"query","sql":"select 1","response":"QwAAAA1TRUxFQ1QgMQA="}
{"kind":"startup","parameters":[["server_version","18.0"]]}
"#
        );
        assert_eq!(
            String::from_utf8(times).unwrap(),
            r#"{"entry":"76d776f9b12cece4","exec_us":[103,104]}
{"entry":"9b39bcaf941a775e","start_us":[100,101]}
"#
        );
        // Lines that no entry uses are ignored.
        let mut times = fs::OpenOptions::new()
            .append(true)
            .open(path.with_extension("times.jsonl"))
            .unwrap();
        writeln!(times, r#"{{"entry":"0000000000000000","exec_us":[1]}}"#).unwrap();
        let read_back = read(&path);
        fs::remove_dir_all(&dir).unwrap();
        assert_eq!(read_back.unwrap().len(), 3);
    }

    #[test]
    fn the_order_of_the_entries_does_not_change_the_files() {
        let dir = temp_dir("order");
        let (forward, backward) = (dir.join("forward.jsonl"), dir.join("backward.jsonl"));
        let mut entries = entries(100);
        write(&forward, &entries).unwrap();
        entries.reverse();
        write(&backward, &entries).unwrap();
        let (forward, backward) = (contents(&forward), contents(&backward));
        let read_back = read(&dir.join("backward.jsonl"));
        fs::remove_dir_all(&dir).unwrap();
        assert_eq!(forward, backward);
        // Whatever the order, the lines are sorted.
        let recording = String::from_utf8(forward.0).unwrap();
        let mut sorted: Vec<_> = recording.lines().collect();
        sorted.sort();
        assert_eq!(recording.lines().collect::<Vec<_>>(), sorted);
        assert_eq!(read_back.unwrap().len(), 5);
    }

    #[test]
    fn other_times_leave_the_recording_as_it_was() {
        let dir = temp_dir("times");
        let path = dir.join("recording.jsonl");
        write(&path, &entries(100)).unwrap();
        let (recording, times) = contents(&path);
        write(&path, &entries(200)).unwrap();
        let (again, other_times) = contents(&path);
        fs::remove_dir_all(&dir).unwrap();
        assert_eq!(recording, again);
        assert_ne!(times, other_times);
    }

    #[test]
    fn times_of_another_recording_are_refused() {
        let dir = temp_dir("stale");
        let path = dir.join("recording.jsonl");
        write(&path, &entries(100)).unwrap();
        let times = fs::read(path.with_extension("times.jsonl")).unwrap();
        let mut changed = entries(100);
        let Entry::Execute { rows, .. } = &mut changed[1] else {
            panic!("{:?}", changed[1]);
        };
        *rows = Bytes::from_static(b"C\0\0\0\x0dSELECT 2\0");
        write(&path, &changed).unwrap();
        fs::write(path.with_extension("times.jsonl"), times).unwrap();
        let error = read(&path).unwrap_err();
        fs::remove_dir_all(&dir).unwrap();
        assert!(error.contains("recording.times.jsonl"), "{error}");
        // The changed entry sorts after the other Execute.
        assert!(error.contains("recording.jsonl:3"), "{error}");
        assert!(error.contains("doesn't match"), "{error}");
    }

    /// Writes a recording, then fails to write another over it, with a
    /// directory where a partial file would be created.
    fn assert_a_failed_write_keeps_the_previous_files(test: &str, blocked: &str) {
        let dir = temp_dir(test);
        let path = dir.join("recording.jsonl");
        let files = || {
            (
                fs::read_to_string(&path).unwrap(),
                fs::read_to_string(times_path(&path)).unwrap(),
            )
        };
        write(&path, &entries(100)).unwrap();
        let before = files();
        fs::create_dir(path.with_extension(blocked)).unwrap();
        let other = vec![Entry::Query {
            sql: "select 2".into(),
            response: Bytes::new(),
            exec_us: vec![1],
        }];
        let failed = write(&path, &other);
        let after = files();
        let read_back = read(&path);
        fs::remove_dir_all(&dir).unwrap();
        assert!(failed.is_err());
        assert_eq!(before, after);
        assert_eq!(read_back.unwrap().len(), 5);
    }

    #[test]
    fn a_recording_that_fails_to_write_leaves_the_previous_files() {
        assert_a_failed_write_keeps_the_previous_files("failed-recording", "partial");
    }

    #[test]
    fn times_that_fail_to_write_leave_the_previous_files() {
        assert_a_failed_write_keeps_the_previous_files("failed-times", "times.partial");
    }

    #[test]
    fn large_values_are_compressed() {
        let dir = temp_dir("compressed");
        let path = dir.join("recording.jsonl");
        let large = Bytes::from(vec![b'x'; 1 << 20]);
        let entries = vec![Entry::Execute {
            sql: "select $1, $2, $3".into(),
            param_formats: vec![1, 1, 1],
            params: vec![
                Some(large.clone()),
                None,
                Some(Bytes::from_static(&[0, 0, 0, 1])),
            ],
            result_formats: vec![1],
            description: Bytes::from_static(b"n\0\0\0\x04"),
            rows: large,
            exec_us: vec![12],
        }];
        write(&path, &entries).unwrap();
        let recording = contents(&path).0;
        let read_back = read(&path);
        // The same bytes are the same line every time.
        write(&path, &entries).unwrap();
        let again = contents(&path).0;
        fs::remove_dir_all(&dir).unwrap();
        assert_eq!(read_back.unwrap(), entries);
        assert_eq!(recording, again);
        let recording = String::from_utf8(recording).unwrap();
        assert!(recording.len() < 16 * 1024, "{}", recording.len());
        assert_eq!(recording.matches(r#""zlib:"#).count(), 2);
        // Small values stay as they were.
        assert!(recording.contains(r#",null,"AAAAAQ=="]"#), "{recording}");
    }
}

mod base64_bytes {
    use std::io::{Read, Write};

    use base64::{Engine, engine::general_purpose::STANDARD};
    use bytes::Bytes;
    use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    /// The size from which values are compressed.
    const COMPRESSED_FROM: usize = 64 * 1024;
    /// What starts a compressed value. `:` is not in the base64 alphabet.
    const PREFIX: &str = "zlib:";

    pub fn encode(bytes: &[u8]) -> String {
        if bytes.len() < COMPRESSED_FROM {
            return STANDARD.encode(bytes);
        }
        // A fixed level, so that the same bytes are the same text every time.
        let mut zlib = ZlibEncoder::new(Vec::new(), Compression::new(6));
        zlib.write_all(bytes).expect("writing to memory");
        let compressed = zlib.finish().expect("writing to memory");
        format!("{PREFIX}{}", STANDARD.encode(compressed))
    }

    pub fn decode(text: &str) -> Result<Bytes, String> {
        let Some(compressed) = text.strip_prefix(PREFIX) else {
            return STANDARD
                .decode(text)
                .map(Bytes::from)
                .map_err(|e| e.to_string());
        };
        let compressed = STANDARD.decode(compressed).map_err(|e| e.to_string())?;
        let mut bytes = Vec::new();
        ZlibDecoder::new(&compressed[..])
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        Ok(bytes.into())
    }

    pub fn serialize<S: Serializer>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Bytes, D::Error> {
        decode(<&str>::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

mod base64_params {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    use super::base64_bytes::{decode, encode};

    pub fn serialize<S: Serializer>(
        params: &[Option<Bytes>],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(params.iter().map(|p| p.as_ref().map(|p| encode(p))))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<Option<Bytes>>, D::Error> {
        Vec::<Option<String>>::deserialize(deserializer)?
            .into_iter()
            .map(|p| p.map(|p| decode(&p)).transpose().map_err(D::Error::custom))
            .collect()
    }
}
