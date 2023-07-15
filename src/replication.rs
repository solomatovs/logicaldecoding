use core::pin::Pin;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::{
    task::Poll,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use bytes::{BufMut, BytesMut};
use futures::{
    future::{self},
    ready, Sink, StreamExt,
};
use futures::future::err;
use prost::Message;
use tokio::sync::{broadcast, oneshot};
use tokio_postgres::{Client, CopyBothDuplex, Error, NoTls, SimpleQueryMessage, SimpleQueryRow};
use tracing::{debug, trace, Instrument};

use decoderbufs::{Op, RowMessage};

use crate::replication::LSNError::ParseError;

pub mod decoderbufs {
    include!(concat!(env!("OUT_DIR"), "/decoderbufs.rs"));
}

static MICROSECONDS_FROM_UNIX_EPOCH_TO_2000: u128 = 946_684_800_000_000;

#[repr(u8)]
pub enum XLogDataByteId {
    XLogDataByteID = b'w',
    PrimaryKeepaliveMessageByteID = b'k',
    StandbyStatusUpdateByteID = b'r',
}

impl fmt::Display for XLogDataByteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::XLogDataByteID => "XLogDataByteID",
                Self::PrimaryKeepaliveMessageByteID => "PrimaryKeepaliveMessageByteID",
                Self::StandbyStatusUpdateByteID => "StandbyStatusUpdateByteID",
            }
        )
    }
}

#[derive(Debug)]
pub struct LogicalReplicationModeOptions {
    plugin: String,
}

#[repr(u32)]
pub enum ReplicationMode {
    LogicalReplication(LogicalReplicationModeOptions) = 0,
    PhysicalReplication = 1,
}

impl fmt::Display for ReplicationMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::LogicalReplication(plugin) => format!(r#"LOGICAL "{:?}""#, plugin),
                Self::PhysicalReplication => "PHYSICAL".into(),
            }
        )
    }
}

// LSN is a PostgreSQL Log Sequence Number. See https://www.postgresql.org/docs/current/datatype-pg-lsn.html.
pub struct LSN(u64);

#[derive(Debug)]
pub enum LSNError<'a> {
    ParseError(&'a String),
}

macro_rules! scan {
    ( $string:expr, $sep:expr, $( $x:ty ),+ ) => {{
        let mut iter = $string.split($sep);
        ($(iter.next().and_then(|word| word.parse::<$x>().ok()),)*)
    }}
}

impl LSN {
    pub fn parse_lsn(text_lsn: &String) -> Result<LSN, LSNError> {
        let (left, right) = scan!(text_lsn, "/", u64, u64);

        let left = match left {
            None => return Err(ParseError(text_lsn)),
            Some(left) => left,
        };

        let right = match right {
            None => return Err(ParseError(text_lsn)),
            Some(right) => right,
        };

        let lsn: u64 = (left << 32) + right;
        let lsn = LSN(lsn);
        Ok(lsn)
    }

    pub fn decode_text(text_lsn: &String) -> Option<LSN> {
        let lsn = Self::parse_lsn(text_lsn);
        match lsn {
            Ok(lsn) => Some(lsn),
            Err(_) => None,
        }
    }

    pub fn decode_bytes(text_lsn: Vec<u8>) -> Option<LSN> {
        todo!()
    }
}

// String formats the LSN value into the XXX/XXX format which is the text format used by PostgreSQL.
impl Debug for LSN {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let left = self.0 >> 32;
        let right = self.0 as u32;
        let text = format!("{}/{}", left, right);
        return f.write_str(&text);
    }
}

pub enum PostgresStreamingError {
    TokioPostgres(tokio_postgres::Error),
    QueryWrongResult(String, Vec<SimpleQueryMessage>),
    CreateReplicationResultParsingError(String),
}

impl From<tokio_postgres::Error> for PostgresStreamingError {
    fn from(value: Error) -> Self {
        Self::TokioPostgres(value)
    }
}

impl fmt::Debug for PostgresStreamingError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            PostgresStreamingError::TokioPostgres(err) => write!(fmt, "{:?}", err)?,
            PostgresStreamingError::QueryWrongResult(query, res) => {
                writeln!(fmt, "query: {:#?}", query)?;

                for q in res {
                    match q {
                        SimpleQueryMessage::Row(row) => {
                            if row.is_empty() {
                                writeln!(fmt, "query result is empty")?;
                            } else {
                                for r in row.columns() {
                                    let name = r.name();
                                    match row.get(name) {
                                        Some(val) => writeln!(fmt, "{}: {}", name, val)?,
                                        None => writeln!(fmt, "{}", name)?,
                                    };
                                }
                            }
                        }
                        SimpleQueryMessage::CommandComplete(num) => {
                            writeln!(fmt, "num: {}", num)?;
                        }
                        _ => (),
                    }
                }
            },
            PostgresStreamingError::CreateReplicationResultParsingError(error) => {
                writeln!(fmt, "{}", error)?;
            }
        };

        Ok(())
    }
}

pub struct CreateReplicationSlotOptions {
    temporary: bool,
    snapshot_action: String,
    mode: ReplicationMode,
}

// CreateReplicationSlotResult is the parsed results the CREATE_REPLICATION_SLOT command.
pub struct CreateReplicationSlotResult {
    slot_name: String,
    consistent_point: String,
    snapshot_name: Option<String>,
    output_plugin: Option<String>,
}

impl CreateReplicationSlotResult {
    pub fn parse_query_row(out: SimpleQueryRow) -> Result<Self, PostgresStreamingError> {
        Ok(Self {
            slot_name: match out.get("slot_name") {
                Some(out) => out.to_owned(),
                _ => return Err(PostgresStreamingError::CreateReplicationResultParsingError("slot_name not found in result from postgres".into())),
            },
            consistent_point: match out.get("consistent_point") {
                Some(out) => out.to_owned(),
                _ => return Err(PostgresStreamingError::CreateReplicationResultParsingError("consistent_point not found in result from postgres".into())),
            },
            snapshot_name: out.get("snapshot_name").and_then(|row| Some(row.to_owned())),
            output_plugin: out.get("output_plugin").and_then(|row| Some(row.to_owned())),
        })
    }
}

// create_replication_slot creates a logical replication slot.
pub async fn create_replication_slot(
    client: &Client,
    slot_name: &String,
    options: CreateReplicationSlotOptions
) -> Result<CreateReplicationSlotResult, PostgresStreamingError> {
    let temp = if options.temporary {"TEMPORARY"} else {""};

    let query = format!(
        r#"CREATE_REPLICATION_SLOT {} {} {} {}"#,
        slot_name, temp, options.mode.to_string(), options.snapshot_action
    );

    let mut res = client.simple_query(&query).await?.into_iter();
    let row = loop {
        match res.next() {
            Some(SimpleQueryMessage::Row(msg)) => break msg,
            Some(SimpleQueryMessage::CommandComplete(num)) => continue,
            _ => return Err(PostgresStreamingError::CreateReplicationResultParsingError("received message from postgres is not a row".into())),
        }
    };

    Ok(CreateReplicationSlotResult::parse_query_row(row)?)
}


#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Transaction {
    pub xid: u32,
    pub commit_time: u64,
    pub events: Vec<RowMessage>,
}

async fn keep_alive_response(
    duplex_stream: &mut Pin<Box<CopyBothDuplex<Bytes>>>,
) -> Result<(), PostgresStreamingError> {
    //unimplemented!();
    // not sure if sending the client system's "time since 2000-01-01" is actually necessary, but lets do as postgres asks just in case
    const SECONDS_FROM_UNIX_EPOCH_TO_2000: u128 = 946684800;
    let time_since_2000: u64 = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
        - (SECONDS_FROM_UNIX_EPOCH_TO_2000 * 1000 * 1000))
        .try_into()
        .unwrap();

    // see here for format details: https://www.postgresql.org/docs/10/protocol-replication.html
    let mut data_to_send: Vec<u8> = vec![];
    // Byte1('r'); Identifies the message as a receiver status update.
    data_to_send.extend_from_slice(&[114]); // "r" in ascii
                                            // The location of the last WAL byte + 1 received and written to disk in the standby.
    data_to_send.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    // The location of the last WAL byte + 1 flushed to disk in the standby.
    data_to_send.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    // The location of the last WAL byte + 1 applied in the standby.
    data_to_send.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    // The client's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
    //0, 0, 0, 0, 0, 0, 0, 0,
    data_to_send.extend_from_slice(&time_since_2000.to_be_bytes());
    // Byte1; If 1, the client requests the server to reply to this message immediately. This can be used to ping the server, to test if the connection is still healthy.
    data_to_send.extend_from_slice(&[1]);

    let buf = Bytes::from(data_to_send);

    debug!("trying to send keep-alive response...");
    trace!("{:#02x}", buf);

    future::poll_fn(|cx| {
        for step_number in 1..5 {
            trace!("keep-alive step:{}", step_number);

            let step = match step_number {
                1 => ready!(duplex_stream.as_mut().poll_ready(cx)),
                2 => duplex_stream.as_mut().start_send(buf.clone()),
                3 => ready!(duplex_stream.as_mut().poll_flush(cx)),
                _ => break,
            };

            if let Err(err) = step {
                return Poll::Ready(Err(err));
            }
        }

        return Poll::Ready(Ok(()));
    })
    .await?;

    trace!("keep-alive sent");

    Ok(())
}
// connect to the database
/**
 * There appear to be three ways to use replication slots:
 * 1) "CREATE_REPLICATION_SLOT" followed by "pg_logical_slot_get_changes()".
 * 2) "CREATE_REPLICATION_SLOT" followed by "START_REPLICATION" (with stream listener).
 * 3) Connecting to postgres pod through shell, then running "pg_recvlogical".
 * In this function, we use approach 2.
 */
pub async fn start_streaming_changes(db_config: String) -> Result<(), PostgresStreamingError> {
    // connect to the database
    let (client, connection) = tokio_postgres::connect(&db_config, NoTls).await?;

    // the connection object performs the actual communication with the database, so spawn it off to run on its own
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let slot_name = "slot_".to_owned()
        + &SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();

    let options = CreateReplicationSlotOptions {
        temporary: true,
        mode: ReplicationMode::PhysicalReplication,
        snapshot_action: "".into(),
    };

    let repl_res = create_replication_slot(
        &client,
        &slot_name,
        options,
    ).await?;

    let query = format!(r#"START_REPLICATION SLOT {} LOGICAL {}"#, slot_name, repl_res.consistent_point);
    //let query = format!("START_REPLICATION SLOT {} PHYSICAL {}", slot_name, lsn);
    let mut duplex_stream = Box::pin(client.copy_both_simple::<bytes::Bytes>(&query).await?);

    while let Some(event) = duplex_stream.as_mut().next().await {
        let event = event?;

        // see here for list of message-types: https://www.postgresql.org/docs/10/protocol-replication.html
        // type: XLogData (WAL data, ie. change of data in db)
        trace!("event: {:#02x}", event);

        match event[0] {
            b'k' => {
                let last_byte = event.last().unwrap();
                let timeout_imminent = last_byte == &1;
                debug!("keep-alive timeout: {}", timeout_imminent);
                trace!("receive message:{:#02x}", event);

                // if timeout = true, then send keep-alive message to postgres server
                if timeout_imminent {
                    keep_alive_response(&mut duplex_stream).await?;
                }
            }
            b'w' => {
                let deb = String::from_utf8_lossy(&event[25..]);
                println!("{:#?}", deb);
            }
            x => {
                println!("event type {:#02x} not implemented", x);
            }
        }
    }

    Ok(())
}

pub async fn clickhouse_worker(mut rx: broadcast::Receiver<Transaction>) {
    loop {
        match rx.recv().await {
            Ok(t) => println!("{:?}", t),
            Err(_) => break,
        }
    }
}
