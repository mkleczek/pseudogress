//! pseudogress simulates many virtual PostgreSQL databases on one server, so
//! that a client can be load-tested without a database's limits and costs.
//! Record mode forwards to a real PostgreSQL and records what it answers;
//! replay mode answers from the recordings.

mod admin;
mod auth;
mod class;
mod config;
mod databases;
mod loader;
mod metrics;
mod options;
mod record;
mod recording;
mod registry;
mod replay;
mod service;
mod session;
mod timer;
mod tls;
mod wire;

use std::{process::ExitCode, sync::Arc};

use tokio::net::TcpListener;

use crate::{
    auth::Verifiers,
    class::{Class, Classes},
    config::Config,
    databases::Databases,
    loader::Loader,
    options::Mode,
    record::Record,
    replay::Replay,
    session::Shared,
    tls::Tls,
};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let options = match options::parse(&args) {
        Ok(options) => options,
        Err(usage) => {
            eprintln!("{usage}");
            return ExitCode::from(2);
        }
    };
    let result = config::load(&options.config).and_then(|config| {
        // Every connection is a file descriptor.
        let _ = rlimit::increase_nofile_limit(u64::MAX);
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?
            .block_on(run(options.mode, config))
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("pseudogress: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(mode: Mode, mut config: Config) -> Result<(), String> {
    // Replay knows its classes at startup; the class service of record mode
    // adds to the classes it starts with.
    let room = match &mode {
        Mode::Record {
            service: Some(service),
        } => service.max_classes,
        _ => 0,
    };
    if let Mode::Replay {
        recordings: Some(recordings),
    } = &mode
    {
        config::load_classes(&mut config, recordings)?;
    }
    let classes = Class::registry(&config.classes, room);
    let databases = Arc::new(Databases::with_room(&config.classes, room));
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
        Mode::Replay { .. } => {
            let replay = Replay::load(&config, classes.clone())?;
            let shared = share(&config, classes, databases, tls, replay, None);
            session::serve(listen(&config)?, shared)
                .await
                .map_err(|e| e.to_string())
        }
        Mode::Record { service } => {
            let loader = service.map(Loader::new).transpose()?;
            let record = Record::with_room(&config, room)?;
            let shared = share(&config, classes, databases, tls, record, loader);
            let listeners = listen(&config)?;
            tokio::select! {
                served = session::serve(listeners, shared.clone()) => {
                    served.map_err(|e| e.to_string())?;
                }
                () = shutdown() => {}
            }
            shared.connector.write()?;
            shared.loader.as_ref().map_or(Ok(()), Loader::write)
        }
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
    loader: Option<Loader>,
) -> Arc<Shared<C>> {
    Arc::new(Shared {
        classes,
        databases,
        verifiers: Verifiers::new(),
        secret: config.secret.clone(),
        tls,
        connector,
        loader,
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
