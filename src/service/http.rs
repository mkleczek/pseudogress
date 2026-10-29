//! A minimal HTTP/1.1 client: one `GET` per connection.

use std::time::Duration;

use super::Url;

/// Requests `url` with `Connection: close`. The result is the body of a 200,
/// `None` for a 404, and the reason for any other answer or failure, within
/// `timeout` as a whole.
#[allow(dead_code, reason = "nothing looks classes up yet")]
pub(super) async fn get(url: &Url, timeout: Duration) -> Result<Option<Vec<u8>>, String> {
    let _ = (url, timeout);
    todo!()
}
