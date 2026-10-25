use std::{sync::Arc, thread};

use super::*;

fn class(name: &str, databases: &str, max_connections: Option<u32>) -> config::Class {
    let max_connections = max_connections
        .map(|n| format!("max_connections = {n}"))
        .unwrap_or_default();
    toml::from_str(&format!(
        "name = {name:?}\ndatabases = {databases:?}\nusers = [\"u\"]\n\
         recording = \"r.jsonl\"\n{max_connections}"
    ))
    .unwrap()
}

/// Classes as (name, database pattern, max connections).
fn registry(classes: &[(&str, &str, Option<u32>)]) -> Databases {
    let classes: Vec<_> = classes
        .iter()
        .map(|&(name, databases, max)| class(name, databases, max))
        .collect();
    Databases::new(&classes)
}

/// Every database name matches, without a limit.
fn any() -> Databases {
    registry(&[("all", "*", None)])
}

fn connect(databases: &Databases, name: &str) -> Connection {
    databases.connect(name).unwrap()
}

fn connects(databases: &Databases, name: &str) -> bool {
    databases.connect(name).is_ok()
}

/// Events are queued by the time `notify` and `terminate` return, so an event
/// that is not ready now does not exist.
async fn ready_event(connection: &mut Connection) -> Option<Event> {
    tokio::select! {
        biased;
        event = connection.event() => Some(event),
        () = std::future::ready(()) => None,
    }
}

fn notification(channel: &str, payload: &str) -> Event {
    Event::Notification {
        channel: channel.into(),
        payload: payload.into(),
    }
}

fn database(name: &str) -> Target {
    Target::Database {
        database: name.into(),
    }
}

fn class_target(name: &str, fraction: f64) -> Target {
    Target::Class {
        class: name.into(),
        fraction,
    }
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn a_prefix_pattern_matches_names_that_start_with_it() {
    let databases = registry(&[("small", "small-*", None)]);
    for name in ["small-", "small-1", "small-1-x"] {
        assert!(connects(&databases, name), "{name}");
    }
    for name in ["", "small", "smal-1", "big-small-1", "Small-1"] {
        assert!(!connects(&databases, name), "{name}");
    }
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn a_suffix_pattern_matches_names_that_end_with_it() {
    let databases = registry(&[("primary", "*-primary", None)]);
    for name in ["-primary", "a-primary", "a-b-primary"] {
        assert!(connects(&databases, name), "{name}");
    }
    for name in ["", "primary", "a-primary-x", "a-primar"] {
        assert!(!connects(&databases, name), "{name}");
    }
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn a_pattern_with_a_star_inside_matches_both_ends() {
    let databases = registry(&[("ab", "a*b", None)]);
    for name in ["ab", "a-b", "aXXb", "abb", "aab"] {
        assert!(connects(&databases, name), "{name}");
    }
    for name in ["", "a", "b", "ba", "a-b-", "-a-b"] {
        assert!(!connects(&databases, name), "{name}");
    }
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn a_pattern_with_several_stars_finds_its_parts_in_order() {
    let databases = registry(&[("abc", "a*b*c", None)]);
    for name in ["abc", "a-b-c", "abbc", "aabcc"] {
        assert!(connects(&databases, name), "{name}");
    }
    for name in ["ac", "acb", "a-c-b", "bac"] {
        assert!(!connects(&databases, name), "{name}");
    }
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn the_parts_of_a_pattern_do_not_overlap() {
    let databases = registry(&[("ends", "ab*ba", None), ("middle", "*aa*aa*", None)]);
    assert!(!connects(&databases, "aba"));
    assert!(connects(&databases, "abba"));
    assert!(!connects(&databases, "aaa"));
    assert!(connects(&databases, "aaaa"));
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn a_pattern_without_a_star_matches_exactly() {
    let databases = registry(&[("exact", "a.b", None)]);
    assert!(connects(&databases, "a.b"));
    for name in ["", "a", "axb", "a.b1", "xa.b"] {
        assert!(!connects(&databases, name), "{name}");
    }
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn a_lone_star_matches_every_name() {
    let databases = any();
    for name in ["", "a", "a*b", "any name at all"] {
        assert!(connects(&databases, name), "{name:?}");
    }
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn the_first_class_that_matches_wins() {
    let databases = registry(&[("first", "a*", None), ("second", "*", None)]);
    assert_eq!(connect(&databases, "abc").class(), 0);
    assert_eq!(connect(&databases, "b").class(), 1);

    let databases = registry(&[("catch-all", "*", None), ("a", "a*", None)]);
    assert_eq!(connect(&databases, "abc").class(), 0);
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn a_name_that_matches_no_class_does_not_exist() {
    let databases = registry(&[("small", "small-*", None)]);
    assert_eq!(
        databases.connect("big-1").err(),
        Some(ConnectError::NoSuchDatabase)
    );
    assert_eq!(databases.info("big-1"), None);
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn max_connections_refuses_the_extra_connection_until_one_drops() {
    let databases = registry(&[("small", "small-*", Some(2))]);
    let first = connect(&databases, "small-1");
    let _second = connect(&databases, "small-1");
    assert_eq!(
        databases.connect("small-1").err(),
        Some(ConnectError::TooManyConnections)
    );
    assert_eq!(databases.info("small-1").unwrap().connections, 2);

    drop(first);
    let _third = connect(&databases, "small-1");
    assert_eq!(databases.info("small-1").unwrap().connections, 2);
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn max_connections_counts_each_database_on_its_own() {
    let databases = registry(&[("small", "small-*", Some(1))]);
    let _one = connect(&databases, "small-1");
    assert!(connects(&databases, "small-2"));
    assert_eq!(
        databases.connect("small-1").err(),
        Some(ConnectError::TooManyConnections)
    );
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn a_class_without_max_connections_has_no_limit() {
    let databases = any();
    let connections: Vec<_> = (0..500).map(|_| connect(&databases, "db")).collect();
    assert_eq!(databases.info("db").unwrap().connections, connections.len());
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn a_refused_connection_leaves_no_database_behind() {
    let databases = registry(&[("none", "*", Some(0))]);
    assert_eq!(
        databases.connect("db").err(),
        Some(ConnectError::TooManyConnections)
    );
    assert_eq!(databases.info("db"), None);
}

#[tokio::test]
#[ignore = "xfail: databases is not implemented yet"]
async fn a_notification_reaches_only_the_listeners_of_its_channel_and_database() {
    let databases = any();
    let mut listening = connect(&databases, "a");
    let mut other_channel = connect(&databases, "a");
    let mut silent = connect(&databases, "a");
    let mut other_database = connect(&databases, "b");
    listening.listen("ch");
    other_channel.listen("other");
    other_database.listen("ch");

    assert_eq!(databases.notify(&database("a"), "ch", "hello"), 1);

    assert_eq!(
        ready_event(&mut listening).await,
        Some(notification("ch", "hello"))
    );
    assert_eq!(ready_event(&mut other_channel).await, None);
    assert_eq!(ready_event(&mut silent).await, None);
    assert_eq!(ready_event(&mut other_database).await, None);
}

#[tokio::test]
#[ignore = "xfail: databases is not implemented yet"]
async fn listening_twice_notifies_once() {
    let databases = any();
    let mut connection = connect(&databases, "a");
    connection.listen("ch");
    connection.listen("ch");

    assert_eq!(databases.notify(&database("a"), "ch", "x"), 1);

    assert_eq!(
        ready_event(&mut connection).await,
        Some(notification("ch", "x"))
    );
    assert_eq!(ready_event(&mut connection).await, None);
    assert_eq!(databases.info("a").unwrap().listening["ch"], 1);
}

#[tokio::test]
#[ignore = "xfail: databases is not implemented yet"]
async fn unlisten_stops_one_channel_and_star_stops_all() {
    let databases = any();
    let mut connection = connect(&databases, "a");
    for channel in ["one", "two", "three"] {
        connection.listen(channel);
    }

    connection.unlisten("one");
    assert_eq!(databases.notify(&database("a"), "one", "x"), 0);
    assert_eq!(databases.notify(&database("a"), "two", "x"), 1);
    assert_eq!(
        ready_event(&mut connection).await,
        Some(notification("two", "x"))
    );

    connection.unlisten("*");
    for channel in ["one", "two", "three"] {
        assert_eq!(databases.notify(&database("a"), channel, "x"), 0);
    }
    assert_eq!(ready_event(&mut connection).await, None);
    assert!(databases.info("a").unwrap().listening.is_empty());
}

#[tokio::test]
#[ignore = "xfail: databases is not implemented yet"]
async fn events_arrive_in_the_order_they_were_sent() {
    let databases = any();
    let mut connection = connect(&databases, "a");
    connection.listen("ch");

    databases.notify(&database("a"), "ch", "1");
    databases.terminate(&database("a"));
    databases.notify(&database("a"), "ch", "2");

    assert_eq!(connection.event().await, notification("ch", "1"));
    assert_eq!(connection.event().await, Event::Terminate);
    assert_eq!(connection.event().await, notification("ch", "2"));
}

#[tokio::test]
#[ignore = "xfail: databases is not implemented yet"]
async fn event_waits_for_an_event_that_a_task_sends_later() {
    let databases = Arc::new(any());
    let mut connection = connect(&databases, "a");
    connection.listen("ch");
    let waiting = tokio::spawn(async move { connection.event().await });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());

    databases.notify(&database("a"), "ch", "late");

    assert_eq!(waiting.await.unwrap(), notification("ch", "late"));
}

#[tokio::test]
#[ignore = "xfail: databases is not implemented yet"]
async fn a_class_target_with_fraction_one_reaches_every_database_of_the_class() {
    let databases = registry(&[("small", "small-*", None), ("big", "big-*", None)]);
    let mut connections = [
        connect(&databases, "small-1"),
        connect(&databases, "small-1"),
        connect(&databases, "small-2"),
        connect(&databases, "big-1"),
    ];
    for connection in &connections {
        connection.listen("ch");
    }

    assert_eq!(databases.notify(&class_target("small", 1.0), "ch", "x"), 3);

    let [one, two, three, big] = &mut connections;
    for small in [one, two, three] {
        assert_eq!(ready_event(small).await, Some(notification("ch", "x")));
    }
    assert_eq!(ready_event(big).await, None);
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn a_class_target_without_a_fraction_reaches_every_database() {
    let target: Target = serde_json::from_str(r#"{"class": "small"}"#).unwrap();
    let databases = registry(&[("small", "small-*", None)]);
    let _connections: Vec<_> = (0..20)
        .map(|i| connect(&databases, &format!("small-{i}")))
        .collect();

    assert_eq!(databases.terminate(&target), 20);
}

#[tokio::test]
#[ignore = "xfail: databases is not implemented yet"]
async fn a_class_target_with_fraction_zero_reaches_nothing() {
    let databases = any();
    let mut connection = connect(&databases, "a");
    connection.listen("ch");

    assert_eq!(databases.notify(&class_target("all", 0.0), "ch", "x"), 0);
    assert_eq!(databases.terminate(&class_target("all", 0.0)), 0);

    assert_eq!(ready_event(&mut connection).await, None);
}

#[tokio::test]
#[ignore = "xfail: databases is not implemented yet"]
async fn a_class_target_picks_each_database_at_random_as_a_whole() {
    let databases = registry(&[("small", "small-*", None)]);
    let mut pairs: Vec<_> = (0..1000)
        .map(|i| {
            let name = format!("small-{i}");
            (connect(&databases, &name), connect(&databases, &name))
        })
        .collect();

    let reached = databases.terminate(&class_target("small", 0.5));

    let mut picked = 0;
    for (first, second) in &mut pairs {
        let (first, second) = (
            ready_event(first).await.is_some(),
            ready_event(second).await.is_some(),
        );
        assert_eq!(first, second, "a database is picked as a whole");
        picked += usize::from(first);
    }
    assert_eq!(reached, 2 * picked);
    assert!((350..=650).contains(&picked), "{picked} of 1000 databases");
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn unknown_databases_and_classes_are_reached_by_nobody() {
    let databases = registry(&[("small", "small-*", None)]);
    let _open = connect(&databases, "small-1");

    for target in [
        database("small-2"),
        database("big-1"),
        class_target("big", 1.0),
    ] {
        assert_eq!(databases.notify(&target, "ch", "x"), 0);
        assert_eq!(databases.terminate(&target), 0);
    }
}

#[tokio::test]
#[ignore = "xfail: databases is not implemented yet"]
async fn terminate_reaches_every_connection_of_the_database() {
    let databases = any();
    let mut listening = connect(&databases, "a");
    let mut silent = connect(&databases, "a");
    let mut other_database = connect(&databases, "b");
    listening.listen("ch");

    assert_eq!(databases.terminate(&database("a")), 2);

    assert_eq!(ready_event(&mut listening).await, Some(Event::Terminate));
    assert_eq!(ready_event(&mut silent).await, Some(Event::Terminate));
    assert_eq!(ready_event(&mut other_database).await, None);
}

#[tokio::test]
#[ignore = "xfail: databases is not implemented yet"]
async fn terminate_reaches_every_connection_of_the_class() {
    let databases = registry(&[("small", "small-*", None), ("big", "big-*", None)]);
    let mut small = [
        connect(&databases, "small-1"),
        connect(&databases, "small-2"),
    ];
    let mut big = connect(&databases, "big-1");

    assert_eq!(databases.terminate(&class_target("small", 1.0)), 2);

    for connection in &mut small {
        assert_eq!(ready_event(connection).await, Some(Event::Terminate));
    }
    assert_eq!(ready_event(&mut big).await, None);
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn info_counts_connections_and_listeners_until_the_last_one_drops() {
    let databases = registry(&[("small", "small-*", None)]);
    assert_eq!(databases.info("small-1"), None);

    let first = connect(&databases, "small-1");
    let second = connect(&databases, "small-1");
    let third = connect(&databases, "small-1");
    first.listen("one");
    first.listen("two");
    second.listen("one");

    assert_eq!(
        databases.info("small-1"),
        Some(DatabaseInfo {
            class: "small".into(),
            connections: 3,
            listening: BTreeMap::from([("one".into(), 2), ("two".into(), 1)]),
        })
    );

    drop(first);
    let info = databases.info("small-1").unwrap();
    assert_eq!(info.connections, 2);
    assert_eq!(info.listening, BTreeMap::from([("one".into(), 1)]));

    drop(second);
    drop(third);
    assert_eq!(databases.info("small-1"), None);
}

#[tokio::test]
#[ignore = "xfail: databases is not implemented yet"]
async fn a_database_without_connections_is_closed_and_reopens_without_state() {
    let databases = any();
    let connection = connect(&databases, "a");
    connection.listen("ch");
    drop(connection);

    assert_eq!(databases.notify(&database("a"), "ch", "x"), 0);
    assert_eq!(databases.terminate(&class_target("all", 1.0)), 0);

    let mut reopened = connect(&databases, "a");
    assert_eq!(databases.notify(&database("a"), "ch", "x"), 0);
    assert_eq!(ready_event(&mut reopened).await, None);
    assert!(databases.info("a").unwrap().listening.is_empty());
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn concurrent_connects_and_drops_leave_no_state() {
    const NAMES: usize = 500;
    let databases = any();
    thread::scope(|scope| {
        // An admin that walks the databases while connections come and go.
        scope.spawn(|| {
            for _ in 0..2_000 {
                databases.notify(&class_target("all", 1.0), "ch", "x");
                databases.info("db-1");
            }
        });
        for worker in 0..8 {
            let databases = &databases;
            scope.spawn(move || {
                for i in 0..5_000 {
                    let name = format!("db-{}", (worker * 31 + i) % NAMES);
                    let first = connect(databases, &name);
                    first.listen("ch");
                    let second = connect(databases, &name);
                    drop(first);
                    drop(second);
                }
            });
        }
    });

    for i in 0..NAMES {
        assert_eq!(databases.info(&format!("db-{i}")), None);
    }
    assert_eq!(databases.terminate(&class_target("all", 1.0)), 0);
}

#[test]
#[ignore = "xfail: databases is not implemented yet"]
fn concurrent_connects_never_exceed_max_connections() {
    let databases = registry(&[("limited", "*", Some(4))]);
    thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                for i in 0..5_000 {
                    let name = format!("db-{}", i % 3);
                    if let Ok(connection) = databases.connect(&name) {
                        assert!(databases.info(&name).unwrap().connections <= 4);
                        drop(connection);
                    }
                }
            });
        }
    });
    for i in 0..3 {
        assert_eq!(databases.info(&format!("db-{i}")), None);
    }
}
