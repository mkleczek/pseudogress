//! The column override: replaces one column of recorded rows with generated
//! JSON, so that a test can vary the size of responses without recording a
//! fixture per size.

use bytes::Bytes;

use super::frames::frames;
use crate::config::ColumnOverride;

const MALFORMED_DESCRIPTION: &str = "malformed RowDescription";
const MALFORMED_ROW: &str = "malformed DataRow";

/// `rows` with the column's value replaced in every DataRow, or `None` when
/// `description` has no such column. The value's encoding follows the column's
/// type and format, so that a client decodes it as it would a recorded one.
pub fn apply(
    over: &ColumnOverride,
    description: &[u8],
    result_formats: &[i16],
    rows: &[u8],
) -> Result<Option<Bytes>, String> {
    let Some((index, type_id)) = find(&over.column, description)? else {
        return Ok(None);
    };
    let format = match result_formats {
        [] => 0,
        [all] => *all,
        formats => formats.get(index).copied().unwrap_or(0),
    };
    let json = json(over.bytes);
    let value = match (format, type_id) {
        (0, _) | (1, 25 | 1043 | 114 | 705) => json,
        // The version of jsonb's binary format
        (1, 3802) => [&[1][..], &json].concat(),
        _ => {
            return Err(format!(
                "column {:?} has binary type {type_id}, which cannot hold JSON",
                over.column
            ));
        }
    };
    let mut out = Vec::with_capacity(rows.len());
    for frame in frames(rows) {
        let (kind, body) = frame?;
        let start = out.len();
        out.extend_from_slice(&[kind, 0, 0, 0, 0]);
        if kind == b'D' {
            replace(body, index, &value, &mut out)?;
        } else {
            out.extend_from_slice(body);
        }
        let length = (out.len() - start - 1) as i32;
        out[start + 1..start + 5].copy_from_slice(&length.to_be_bytes());
    }
    Ok(Some(out.into()))
}

/// `[{"x":"aaa…"}]` of `bytes` bytes, or the shortest of this shape.
fn json(bytes: usize) -> Vec<u8> {
    let mut json = br#"[{"x":""#.to_vec();
    json.resize(bytes.saturating_sub(3).max(json.len()), b'a');
    json.extend_from_slice(br#""}]"#);
    json
}

/// The position and type of the named column in a RowDescription.
fn find(column: &str, description: &[u8]) -> Result<Option<(usize, u32)>, &'static str> {
    let Some((b'T', body)) = frames(description).next().transpose()? else {
        return Ok(None);
    };
    let (count, mut rest) = body.split_first_chunk::<2>().ok_or(MALFORMED_DESCRIPTION)?;
    for index in 0..u16::from_be_bytes(*count) as usize {
        let name_end = rest
            .iter()
            .position(|&b| b == 0)
            .ok_or(MALFORMED_DESCRIPTION)?;
        let (name, tail) = (&rest[..name_end], &rest[name_end + 1..]);
        // table, column, type, size, modifier and format
        let (fixed, tail) = tail
            .split_first_chunk::<18>()
            .ok_or(MALFORMED_DESCRIPTION)?;
        if name == column.as_bytes() {
            let type_id = u32::from_be_bytes([fixed[6], fixed[7], fixed[8], fixed[9]]);
            return Ok(Some((index, type_id)));
        }
        rest = tail;
    }
    Ok(None)
}

/// Appends the DataRow body `row` to `out`, with column `index` set to `value`.
fn replace(row: &[u8], index: usize, value: &[u8], out: &mut Vec<u8>) -> Result<(), &'static str> {
    let (count, mut rest) = row.split_first_chunk::<2>().ok_or(MALFORMED_ROW)?;
    let count = u16::from_be_bytes(*count) as usize;
    if index >= count {
        return Err(MALFORMED_ROW);
    }
    out.extend_from_slice(&row[..2]);
    for column in 0..count {
        let (length, tail) = rest.split_first_chunk::<4>().ok_or(MALFORMED_ROW)?;
        // A NULL has length -1 and no bytes.
        let size = i32::from_be_bytes(*length).max(0) as usize;
        let (data, tail) = tail.split_at_checked(size).ok_or(MALFORMED_ROW)?;
        if column == index {
            out.extend_from_slice(&(value.len() as i32).to_be_bytes());
            out.extend_from_slice(value);
        } else {
            out.extend_from_slice(length);
            out.extend_from_slice(data);
        }
        rest = tail;
    }
    Ok(())
}
