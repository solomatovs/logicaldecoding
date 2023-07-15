pub mod decoderbufs {
    include!(concat!(env!("OUT_DIR"), "/decoderbufs.rs"));
}
use bytes::{BufMut, BytesMut};
use decoderbufs::{Op, RowMessage};
use futures::{
    future::{self},
    ready, Sink, StreamExt,
};

use prost::Message;
use std::{
    task::Poll,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{broadcast, oneshot};
use tokio_postgres::{Error, NoTls, SimpleQueryMessage, CopyBothDuplex};
use tracing::{debug, error, event, Instrument, trace};

static MICROSECONDS_FROM_UNIX_EPOCH_TO_2000: u128 = 946_684_800_000_000;

use core::pin::Pin;
use std::{env};
use std::fmt;
use std::fmt::{Debug, Formatter};
use bytes::Bytes;
use sqlx::Postgres;
use sqlx::types::JsonValue;
use tokio_postgres::{Client, SimpleQueryRow};

// LSN is a PostgreSQL Log Sequence Number. See https://www.postgresql.org/docs/current/datatype-pg-lsn.html.
pub struct LSN(u32);

#[derive(Debug)]
pub struct LSNError {

}

macro_rules! scan {
    ( $string:expr, $sep:expr, $( $x:ty ),+ ) => {{
        let mut iter = $string.split($sep);
        ($(iter.next().and_then(|word| word.parse::<$x>().ok()),)*)
    }}
}

impl LSN {
    pub fn parse_lsn(text_lsn: &String) -> Result<LSN, LSNError> {
        let (left, right) = scan!(text_lsn, "/", String, String);
        if let None = left {
            return
        }

    }
    pub fn decode_text(text_lsn: String) -> Result<LSN, LSNError> {
        todo!()
    }
    pub fn decode_bytes(text_lsn: Vec<u8>) -> Result<LSN, LSNError> {
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


#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Transaction {
    pub xid: u32,
    pub commit_time: u64,
    pub events: Vec<RowMessage>,
}

/// starts streaming changes
pub async fn postgres_worker(
    database: impl Into<String> + std::fmt::Display,
    replica_rx: oneshot::Sender<()>,
    transaction_tx: broadcast::Sender<Transaction>,
) -> Result<(), tokio_postgres::Error> {
    let db_config = format!(
        "user=postgres password=password host=localhost port=5432 dbname={} replication=database",
        database
    );
    println!("CONNECT");

    // connect to the database
    let (client, connection) = tokio_postgres::connect(&db_config, NoTls).await.unwrap();

    // the connection object performs the actual communication with the database, so spawn it off to run on its own
    tokio::spawn(async move { connection.await });

    //let slot_name = "slot";
    let slot_name = "slot_".to_owned()
        + &SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();
    let slot_query = format!(
        "CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL \"decoderbufs\"",
        slot_name
    );

    let lsn = client
        .simple_query(&slot_query)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>()
        .first()
        .unwrap()
        .get("consistent_point")
        .unwrap()
        .to_owned();

    let query = format!("START_REPLICATION SLOT {} LOGICAL {}", slot_name, lsn);
    let duplex_stream = client
        .copy_both_simple::<bytes::Bytes>(&query)
        .await
        .unwrap();
    let mut duplex_stream_pin = Box::pin(duplex_stream);

    // see here for format details: https://www.postgresql.org/docs/current/protocol-replication.html
    let mut keepalive = BytesMut::with_capacity(34);
    keepalive.put_u8(b'r');
    // the last 8 bytes of these are overwritten with a timestamp to meet the protocol spec
    keepalive.put_bytes(0, 32);
    keepalive.put_u8(1);

    // set the timestamp of the keepalive message
    keepalive[26..34].swap_with_slice(
        &mut ((SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
            - MICROSECONDS_FROM_UNIX_EPOCH_TO_2000) as u64)
            .to_be_bytes(),
    );

    // send the keepalive to ensure connection is functioning
    future::poll_fn(|cx| {
        ready!(duplex_stream_pin.as_mut().poll_ready(cx)).unwrap();
        duplex_stream_pin
            .as_mut()
            .start_send(keepalive.clone().into())
            .unwrap();
        ready!(duplex_stream_pin.as_mut().poll_flush(cx)).unwrap();
        Poll::Ready(())
    })
    .await;

    // notify ready
    replica_rx.send(()).unwrap();

    let mut transaction = None;
    loop {
        match duplex_stream_pin.as_mut().next().await {
            None => break,
            Some(Err(_)) => continue,
            // type: XLogData (WAL data, ie. change of data in db)
            Some(Ok(event)) if event[0] == b'w' => {
                let row_message = RowMessage::decode(&event[25..]).unwrap();
                debug!("Got XLogData/data-change event: {:?}", row_message);

                match row_message.op {
                    Some(op) if op == Op::Begin as i32 => {
                        transaction = Some(Transaction {
                            xid: row_message.transaction_id(),
                            commit_time: row_message.commit_time(),
                            events: vec![],
                        })
                    }
                    Some(op) if op == Op::Commit as i32 => {
                        debug!("{:?}", &transaction.as_ref().unwrap());
                        let transaction = transaction.take().unwrap();
                        transaction_tx.send(transaction).unwrap();
                    }
                    Some(_) => {
                        transaction.as_mut().unwrap().events.push(row_message);
                    }
                    None => unimplemented!(),
                }
            }
            // type: keepalive message
            Some(Ok(event)) if event[0] == b'k' => {
                let last_byte = event.last().unwrap();
                let timeout_imminent = last_byte == &1;
                trace!(
                    "Got keepalive message:{:x?} @timeoutImminent:{}",
                    event,
                    timeout_imminent
                );
                if timeout_imminent {
                    keepalive[26..34].swap_with_slice(
                        &mut ((SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_micros()
                            - MICROSECONDS_FROM_UNIX_EPOCH_TO_2000)
                            as u64)
                            .to_be_bytes(),
                    );

                    trace!(
                        "Trying to send response to keepalive message/warning!:{:x?}",
                        keepalive
                    );

                    future::poll_fn(|cx| {
                        ready!(duplex_stream_pin.as_mut().poll_ready(cx)).unwrap();
                        duplex_stream_pin
                            .as_mut()
                            .start_send(keepalive.clone().into())
                            .unwrap();
                        ready!(duplex_stream_pin.as_mut().poll_flush(cx)).unwrap();
                        Poll::Ready(())
                    })
                    .await;

                    trace!(
                        "Sent response to keepalive message/warning!:{:x?}",
                        keepalive
                    );
                }
            }
            _ => (),
        }
    }

    Ok(())
}

pub enum PostgresStreamingError {
    TokioPostgres(tokio_postgres::Error),
    QueryWrongResult(String, Vec<SimpleQueryMessage>),
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
                        },
                        SimpleQueryMessage::CommandComplete(num) => {
                            writeln!(fmt, "num: {}", num)?;
                        },
                        _ => (),
                    }
                }
            },
        };

        Ok(())
    }
}


async fn keep_alive_response(duplex_stream: &mut Pin<Box<CopyBothDuplex<Bytes>>>) -> Result<(), PostgresStreamingError> {
    //unimplemented!();
    // not sure if sending the client system's "time since 2000-01-01" is actually necessary, but lets do as postgres asks just in case
    const SECONDS_FROM_UNIX_EPOCH_TO_2000: u128 = 946684800;
    let time_since_2000: u64 = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() - (SECONDS_FROM_UNIX_EPOCH_TO_2000 * 1000 * 1000)).try_into().unwrap();

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
    }).await?;

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

    let slot_name = "slot_".to_owned() + &SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis().to_string();
    let query = format!(r#"CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL "{}""#, slot_name, "wal2json");
    //let query = format!("CREATE_REPLICATION_SLOT {} TEMPORARY PHYSICAL", slot_name);
    let res = client.simple_query(&query).await?;
    let lsn = match res.first() {
        Some(SimpleQueryMessage::Row(msg)) => msg,
        _ => return Err(PostgresStreamingError::QueryWrongResult(query, res)),
    };

    for c in lsn.columns() {
        println!("name: {}", c.name());
    }

    let lsn = match lsn.get("consistent_point") {
        Some(lsn) => lsn.to_owned(),
        _ => return Err(PostgresStreamingError::QueryWrongResult(query, res)),
    };

    let query = format!(r#"START_REPLICATION SLOT {} LOGICAL {}"#, slot_name, lsn);
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
            },
            b'w' => {
                let deb = String::from_utf8_lossy(&event[25..]);
                println!("{:#?}", deb);
            },
            x => {
                println!("event type {:#02x} not implemented", x);
            },
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