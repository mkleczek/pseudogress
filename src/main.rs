//! pseudogress simulates many virtual PostgreSQL databases on one server, so
//! that a client can be load-tested without a database's limits and costs.
//! Record mode forwards to a real PostgreSQL and records what it answers;
//! replay mode answers from the recordings.

mod admin;
mod auth;
mod class;
mod config;
mod databases;
mod metrics;
mod record;
mod recording;
mod replay;
mod session;
mod timer;
mod tls;
mod wire;

use std::{path::Path, process::ExitCode, sync::Arc};

use tokio::net::TcpListener;

use crate::{
    auth::Verifiers,
    class::{Class, Classes},
    config::Config,
    databases::Databases,
    record::Record,
    replay::Replay,
    session::Shared,
    tls::Tls,
};

const USAGE: &str = "usage: pseudogress replay|record CONFIG";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let [_, mode, path] = args.as_slice() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let result = config::load(Path::new(path)).and_then(|config| {
        // Every connection is a file descriptor.
        let _ = rlimit::increase_nofile_limit(u64::MAX);
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?
            .block_on(run(mode, config))
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("pseudogress: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(mode: &str, config: Config) -> Result<(), String> {
    let classes: Classes = config.classes.iter().map(Class::new).collect();
    let databases = Arc::new(Databases::new(&config.classes));
    if let Some(address) = config.admin {
        let listener = TcpListener::bind(address)
            .await
            .map_err(|e| format!("admin API on {address}: {e}"))?;
        announce("admin API", &listener);
        let (classes, databases) = (classes.clone(), databases.clone());
        tokio::spawn(async move {
            if let Err(error) = admin::serve(listener, classes, databases).await {
                eprintln!("pseudogress: admin API: {error}");
            }
        });
    }
    let tls = config.tls.as_ref().map(Tls::load).transpose()?;
    match mode {
        "replay" => {
            let replay = Replay::load(&config, classes.clone())?;
            let shared = share(&config, classes, databases, tls, replay);
            session::serve(listen(&config)?, shared)
                .await
                .map_err(|e| e.to_string())
        }
        "record" => {
            let shared = share(&config, classes, databases, tls, Record::new(&config)?);
            let listeners = listen(&config)?;
            tokio::select! {
                served = session::serve(listeners, shared.clone()) => {
                    served.map_err(|e| e.to_string())?;
                }
                () = shutdown() => {}
            }
            shared.connector.write()
        }
        _ => Err(USAGE.to_owned()),
    }
}

/// Binds the addresses for PostgreSQL connections, once the mode is ready to
/// serve them.
fn listen(config: &Config) -> Result<Vec<TcpListener>, String> {
    config
        .listen
        .iter()
        .map(|&address| {
            let listener = session::listen(address).map_err(|e| format!("{address}: {e}"))?;
            announce("PostgreSQL", &listener);
            Ok(listener)
        })
        .collect()
}

/// Port 0 binds a free port; this line says which, so that whoever started
/// pseudogress can connect.
fn announce(what: &str, listener: &TcpListener) {
    if let Ok(address) = listener.local_addr() {
        eprintln!("pseudogress: {what} on {address}");
    }
}

fn share<C>(
    config: &Config,
    classes: Classes,
    databases: Arc<Databases>,
    tls: Option<Tls>,
    connector: C,
) -> Arc<Shared<C>> {
    Arc::new(Shared {
        classes,
        databases,
        verifiers: Verifiers::new(),
        secret: config.secret.clone(),
        tls,
        connector,
    })
}

/// Completes on SIGINT or SIGTERM.
async fn shutdown() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate()).expect("signal handlers can be installed");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}
