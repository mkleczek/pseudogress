//! A connection's life after setup: batches of messages, the answers the
//! backend gives, and when, and whether, they are sent.

use std::{
    io,
    pin::pin,
    sync::atomic::Ordering::Relaxed,
    time::{Duration, Instant},
};

use bytes::BytesMut;
use pgwire::{
    error::PgWireResult,
    messages::{PgWireFrontendMessage, response::NotificationResponse},
};
use tokio::sync::watch;

use super::{
    Backend,
    cancel::Registration,
    chance, listen,
    peer::{BUFFER, Peer, Stream},
};
use crate::{
    class::Class,
    config::Settings,
    databases::{Connection, Event},
    timer,
    wire::put,
};

/// Buffers above this size are given back after a batch: connections are
/// mostly idle, and thousands of them cannot all keep the memory of one big
/// answer.
const LARGE: usize = 8 * BUFFER;

/// When a batch's answer is due, and what faults it meets.
struct Plan {
    /// When the execution ends, which is when the next batch can begin.
    done: Instant,
    send_at: Instant,
    reset: bool,
    stalled: bool,
    timed_out: bool,
}

/// Simulated time: the batch crosses half a round trip to the server, waits
/// for the batches before it to finish, executes, and the answer crosses the
/// other half. Batches that were sent together pay the network once, but
/// their executions add up, as on a real server.
fn plan(settings: &Settings, arrival: Instant, previous_done: Instant, exec: Duration) -> Plan {
    let half = settings.round_trip / 2;
    let faults = &settings.faults;
    let begin = (arrival + half).max(previous_done);
    let (exec, timed_out) = match faults.statement_timeout {
        Some(limit) if exec > limit => (limit, true),
        _ => (exec, false),
    };
    let done = begin + exec;
    let stalled = chance(faults.stall);
    let stall = if stalled {
        faults.stall_for
    } else {
        Duration::ZERO
    };
    Plan {
        done,
        send_at: done + half + stall,
        reset: chance(faults.reset),
        stalled,
        timed_out,
    }
}

enum Collected {
    Batch,
    Incomplete,
    Terminate,
}

/// Moves the messages of the input into `batch`, up to the end of a batch: a
/// Sync, a Flush or a simple Query.
fn collect<S: Stream>(
    peer: &mut Peer<S>,
    batch: &mut Vec<PgWireFrontendMessage>,
    connection: &Connection,
) -> PgWireResult<Collected> {
    loop {
        let Some(message) = peer.decode()? else {
            return Ok(Collected::Incomplete);
        };
        let ends = match &message {
            PgWireFrontendMessage::Terminate(_) => return Ok(Collected::Terminate),
            PgWireFrontendMessage::Query(query) => {
                listen::apply(&query.query, connection);
                true
            }
            PgWireFrontendMessage::Sync(_) | PgWireFrontendMessage::Flush(_) => true,
            _ => false,
        };
        batch.push(message);
        if ends {
            return Ok(Collected::Batch);
        }
    }
}

/// Executes and simple queries: what a PostgreSQL server counts as statements.
fn statements(batch: &[PgWireFrontendMessage]) -> u64 {
    let statements = batch.iter().filter(|message| {
        matches!(
            message,
            PgWireFrontendMessage::Execute(_) | PgWireFrontendMessage::Query(_)
        )
    });
    statements.count() as u64
}

enum Wake {
    Due,
    Cancelled,
    Terminated,
}

/// Waits until the answer is due. A cancel replaces the answer in `out` with
/// the backend's. Notifications are held back in `late`: PostgreSQL only
/// sends them when it is idle.
async fn wait<B: Backend>(
    send_at: Instant,
    signal: &mut watch::Receiver<()>,
    connection: &mut Connection,
    backend: &mut B,
    out: &mut BytesMut,
    late: &mut BytesMut,
) -> Wake {
    let mut due = pin!(timer::sleep_until(send_at));
    loop {
        tokio::select! {
            () = &mut due => return Wake::Due,
            Ok(()) = signal.changed() => {
                out.clear();
                backend.cancelled(out);
                return Wake::Cancelled;
            }
            event = connection.event() => match event {
                Event::Notification { channel, payload } => {
                    put(late, &NotificationResponse::new(0, channel, payload));
                }
                Event::Terminate => return Wake::Terminated,
            }
        }
    }
}

async fn terminate<S: Stream>(peer: &mut Peer<S>, out: &mut BytesMut) -> io::Result<()> {
    out.clear();
    let message = "terminating connection due to administrator command";
    peer.fatal(out, "57P01", message).await
}

fn trim(buffer: &mut BytesMut) {
    if buffer.capacity() > LARGE && buffer.is_empty() {
        *buffer = BytesMut::with_capacity(BUFFER);
    }
}

/// Serves the connection's batches until it ends. `out` is reused for every
/// answer.
pub(super) async fn run<S: Stream, B: Backend>(
    mut peer: Peer<S>,
    mut out: BytesMut,
    mut connection: Connection,
    mut cancel: Registration,
    mut backend: B,
    class: &Class,
) -> io::Result<()> {
    let metrics = &class.metrics;
    let mut batch = Vec::new();
    let mut late = BytesMut::new();
    // When the bytes of the batch arrived, which is not when it is looked at
    // if it came together with the batch before.
    let mut arrival = Instant::now();
    let mut previous_done = arrival;
    loop {
        match collect(&mut peer, &mut batch, &connection) {
            Err(error) => return peer.fatal(&mut out, "08P01", &error.to_string()).await,
            Ok(Collected::Terminate) => return Ok(()),
            Ok(Collected::Incomplete) => {
                tokio::select! {
                    read = peer.fill() => {
                        metrics.bytes_in.fetch_add(read? as u64, Relaxed);
                        arrival = Instant::now();
                    }
                    event = connection.event() => match event {
                        Event::Notification { channel, payload } => {
                            put(&mut out, &NotificationResponse::new(0, channel, payload));
                            metrics.bytes_out.fetch_add(out.len() as u64, Relaxed);
                            peer.send(&mut out).await?;
                        }
                        Event::Terminate => return terminate(&mut peer, &mut out).await,
                    }
                }
                continue;
            }
            Ok(Collected::Batch) => {}
        }

        // Only a cancel that arrives from now on is for this batch.
        cancel.signal.mark_unchanged();
        let started = Instant::now();
        let exec = backend.batch(&batch, &mut out).await;
        let finished = Instant::now();
        // Without the client's and the simulated delays.
        metrics.processing.record(finished - started);
        metrics.statements.fetch_add(statements(&batch), Relaxed);
        batch.clear();

        let plan = plan(&class.settings.load(), arrival, previous_done, exec);
        if plan.reset {
            metrics.faults.fetch_add(1, Relaxed);
            peer.reset_on_close();
            return Ok(());
        }
        let injected = u64::from(plan.stalled) + u64::from(plan.timed_out);
        if injected > 0 {
            metrics.faults.fetch_add(injected, Relaxed);
        }
        previous_done = plan.done;
        if plan.timed_out {
            out.clear();
            backend.cancelled(&mut out);
        }

        let mut cancelled = false;
        if plan.send_at > finished {
            metrics.in_flight.fetch_add(1, Relaxed);
            let woken = wait(
                plan.send_at,
                &mut cancel.signal,
                &mut connection,
                &mut backend,
                &mut out,
                &mut late,
            )
            .await;
            metrics.in_flight.fetch_sub(1, Relaxed);
            match woken {
                Wake::Due => {}
                // The execution stops, and the next batch need not wait for it.
                Wake::Cancelled => {
                    previous_done = previous_done.min(Instant::now());
                    cancelled = true;
                }
                Wake::Terminated => return terminate(&mut peer, &mut out).await,
            }
        }

        // How far past its simulated time the answer goes out, also when it
        // did not wait. A cancelled answer is not due.
        if !cancelled {
            let lateness = Instant::now().saturating_duration_since(plan.send_at);
            metrics.lateness.record(lateness);
        }

        metrics
            .bytes_out
            .fetch_add((out.len() + late.len()) as u64, Relaxed);
        peer.send(&mut out).await?;
        if !late.is_empty() {
            peer.send(&mut late).await?;
        }
        if backend.broken() {
            return Ok(());
        }
        trim(&mut out);
        trim(&mut peer.input);
    }
}
