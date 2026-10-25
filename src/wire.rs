//! Appending backend messages to an output buffer.

use bytes::BytesMut;
use pgwire::messages::{
    Message,
    response::{ErrorResponse, ReadyForQuery, TransactionStatus},
};

pub fn put(out: &mut BytesMut, message: &impl Message) {
    message
        .encode(out)
        .expect("backend messages fit their size limits");
}

/// `severity` is `ERROR` or `FATAL`.
pub fn put_error(out: &mut BytesMut, severity: &str, code: &str, message: &str) {
    put(
        out,
        &ErrorResponse::new(vec![
            (b'S', severity.to_owned()),
            (b'V', severity.to_owned()),
            (b'C', code.to_owned()),
            (b'M', message.to_owned()),
        ]),
    );
}

pub fn put_ready(out: &mut BytesMut, status: TransactionStatus) {
    put(out, &ReadyForQuery::new(status));
}
