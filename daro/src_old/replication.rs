use core::pin::Pin;
use std::fmt;
use bytes::Bytes;

use std::fmt::{Debug, Display, Formatter};
use std::{
    task::Poll,
    time::{SystemTime, UNIX_EPOCH},
};
use alloc::vec::IntoIter;
use std::convert::Into;

use futures::{
    future::{self},
    ready, Sink, StreamExt,
};
use prost::alloc;
use tokio::pin;
use tokio_postgres::{Client, Error, NoTls, SimpleQueryMessage, SimpleQueryRow, CopyBothDuplex};
use tokio_postgres::replication::LogicalReplicationStream;
use postgres_protocol::message::backend::ReplicationMessage;

use tracing::{debug, trace};

use crate::replication::LSNError::ParseError;



unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    ::core::slice::from_raw_parts(
        (p as *const T) as *const u8,
        ::core::mem::size_of::<T>(),
    )
}

unsafe fn u8_slice_as_any<T>(p: &[u8]) -> &T {
    let len = p.len();
    let size = ::core::mem::size_of::<T>();

    &*(p.as_ptr() as *const T)
}

macro_rules! print_offsets {
    ( $root:ty ; $($member:ident),* ) => {
        let x: $root = unsafe { std::mem::zeroed() };
        let pstart: *const u8 = (&x) as *const _ as *const u8;
        $(
        let offset:usize = {
            let pmember: *const u8 = (&x.$member) as *const _ as *const u8;
            (pmember as usize) - (pstart as usize)
        };
        println!("{:4}  {}", offset, stringify!($member));
        )*
    }
}

// fn main(){
//     print_offsets!(IdentifyControllerResponse; vid, cntrltype, nvmsr, sqes, sgls, subnqn, ioccsz, psd, vs);
// }
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


// LSN is a PostgreSQL Log Sequence Number. See https://www.postgresql.org/docs/current/datatype-pg-lsn.html.
#[repr(C)]
pub struct LSN(u64);

#[derive(Debug)]
pub enum LSNError {
    ParseError(String),
}

impl Display for LSNError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", match self {
            ParseError(err) => err,
        })
    }
}

macro_rules! scan {
    ( $string:expr, $sep:expr, $( $x:ty ),+ ) => {{
        let mut iter = $string.split($sep);
        ($(iter.next().and_then(|word| word.parse::<$x>().ok()),)*)
    }}
}

impl LSN {
    pub fn to_postgres_string(&self) -> String {
        let left = self.0 >> 32;
        let right = self.0 as u64;
        format!("{:01X}/{:01X}", left, right)
    }

    pub fn parse_lsn(text_lsn: String) -> Result<LSN, LSNError> {
        let mut parts = text_lsn.split('/');

        let left = match parts.next() {
            None => return Err(ParseError(text_lsn)),
            Some(val) => match u64::from_str_radix(val, 16) {
                Ok(val) => val,
                Err(err) => return Err(ParseError(err.to_string())),
            },
        };

        let right = match parts.next() {
            None => return Err(ParseError(text_lsn)),
            Some(val) => match u64::from_str_radix(val, 16) {
                Ok(val) => val,
                Err(err) => return Err(ParseError(err.to_string())),
            },
        };

        let lsn: u64 = (left << 32) + right;
        let lsn = LSN(lsn);
        Ok(lsn)
    }
}

// String formats the LSN value into the XXX/XXX format which is the text format used by PostgreSQL.
impl Debug for LSN {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        return f.write_str(&self.to_postgres_string());
    }
}
impl Display for LSN {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        return f.write_str(&self.to_postgres_string());
    }
}

pub trait LogicalReplicationModeOptions {
    fn plugin_name(&self) -> String;
    fn plugin_args(&self) -> String;
}

// #[derive(Debug)]
// pub struct Wal2JsonReplicationModeOptions {
//     include_xids: Option<bool>, //add xid to each changeset. Default is false.
//     include_timestamp: Option<bool>,//add timestamp to each changeset. Default is false.
//     include_schemas: Option<bool>, //add schema to each change. Default is true.
//     include_types: Option<bool>,//add type to each change. Default is true.
//     include_typmod: Option<bool>,//add modifier to types that have it (eg. varchar(20) instead of varchar). Default is true.
//     include_type_oids: Option<bool>,//add type oids. Default is false.
//     include_domain_data_type: Option<bool>,//replace domain name with the underlying data type. Default is false.
//     include_column_positions: Option<bool>,//add column position (pg_attribute.attnum). Default is false.
//     include_origin: Option<bool>,//add origin of a piece of data. Default is false.
//     include_not_null: Option<bool>,//add not null information as columnoptionals. Default is false.
//     include_default: Option<bool>,//add default expression. Default is false.
//     include_pk: Option<bool>,//add primary key information as pk. Column name and data type is included. Default is false.
//     numeric_data_types_as_string: Option<bool>,//use string for numeric data types. JSON specification does not recognize Infinity and NaN as valid numeric values. There might be potential interoperability problems for double precision numbers. Default is false.
//     pretty_print: Option<bool>,//add spaces and indentation to JSON structures. Default is false.
//     write_in_chunks: Option<bool>,//write after every change instead of every changeset. Only used when format-version is 1. Default is false.
//     include_lsn: Option<bool>,//add nextlsn to each changeset. Default is false.
//     include_transaction: Option<bool>,//emit records denoting the start and end of each transaction. Default is true.
//     filter_origins: Option<Vec<String>>,//exclude changes from the specified origins. Default is empty which means that no origin will be filtered. It is a comma separated value.
//     filter_tables: Option<Vec<String>>,//exclude rows from the specified tables. Default is empty which means that no table will be filtered. It is a comma separated value. The tables should be schema-qualified. *.foo means table foo in all schemas and bar.* means all tables in schema bar. Special characters (space, single quote, comma, period, asterisk) must be escaped with backslash. Schema and table are case-sensitive. Table "public"."Foo bar" should be specified as public.Foo\ bar.
//     add_tables: Option<Vec<String>>,//include only rows from the specified tables. Default is all tables from all schemas. It has the same rules from filter-tables.
//     filter_msg_prefixes: Option<Vec<String>>,//exclude messages if prefix is in the list. Default is empty which means that no message will be filtered. It is a comma separated value.
//     add_msg_prefixes: Option<Vec<String>>,//include only messages if prefix is in the list. Default is all prefixes. It is a comma separated value. wal2json applies filter-msg-prefixes before this parameter.
//     format_version: Option<u32>,//defines which format to use. Default is 1.
//     actions: Option<String>,//define which operations will be sent. Default is all actions (insert, update, delete, and truncate). However, if you are using format-version 1, truncate is not enabled (backward compatibility).
// }

// impl Wal2JsonReplicationModeOptions {
//     pub fn default() -> Self {
//         Self {
//             include_xids: None, //add xid to each changeset. Default is false.
//             include_timestamp: None,//add timestamp to each changeset. Default is false.
//             include_schemas: None, //add schema to each change. Default is true.
//             include_types: None,//add type to each change. Default is true.
//             include_typmod: None,//add modifier to types that have it (eg. varchar(20) instead of varchar). Default is true.
//             include_type_oids: None,//add type oids. Default is false.
//             include_domain_data_type: None,//replace domain name with the underlying data type. Default is false.
//             include_column_positions: None,//add column position (pg_attribute.attnum). Default is false.
//             include_origin: None,//add origin of a piece of data. Default is false.
//             include_not_null: None,//add not null information as columnoptionals. Default is false.
//             include_default: None,//add default expression. Default is false.
//             include_pk: None,//add primary key information as pk. Column name and data type is included. Default is false.
//             numeric_data_types_as_string: None,//use string for numeric data types. JSON specification does not recognize Infinity and NaN as valid numeric values. There might be potential interoperability problems for double precision numbers. Default is false.
//             pretty_print: None,//add spaces and indentation to JSON structures. Default is false.
//             write_in_chunks: None,//write after every change instead of every changeset. Only used when format-version is 1. Default is false.
//             include_lsn: None,//add nextlsn to each changeset. Default is false.
//             include_transaction: None,//emit records denoting the start and end of each transaction. Default is true.
//             filter_origins: None,//exclude changes from the specified origins. Default is empty which means that no origin will be filtered. It is a comma separated value.
//             filter_tables: None,//exclude rows from the specified tables. Default is empty which means that no table will be filtered. It is a comma separated value. The tables should be schema-qualified. *.foo means table foo in all schemas and bar.* means all tables in schema bar. Special characters (space, single quote, comma, period, asterisk) must be escaped with backslash. Schema and table are case-sensitive. Table "public"."Foo bar" should be specified as public.Foo\ bar.
//             add_tables: None,//include only rows from the specified tables. Default is all tables from all schemas. It has the same rules from filter-tables.
//             filter_msg_prefixes: None,//exclude messages if prefix is in the list. Default is empty which means that no message will be filtered. It is a comma separated value.
//             add_msg_prefixes: None,//include only messages if prefix is in the list. Default is all prefixes. It is a comma separated value. wal2json applies filter-msg-prefixes before this parameter.
//             format_version: None,//defines which format to use. Default is 1.
//             actions: None,//define which operations will be sent. Default is all actions (insert, update, delete, and truncate). However, if you are using format-version 1, truncate is not enabled (backward compatibility).
//         }
//     }
// }

#[derive(Debug)]
pub struct CustomReplicationModeOptions {
    plugin_name: String,
    plugin_args: Option<String>,
}
impl CustomReplicationModeOptions {
    fn new(plugin_name: String, plugin_args: Option<String>) -> Self {
        Self {
            plugin_name,
            plugin_args
        }
    }
}
impl LogicalReplicationModeOptions for CustomReplicationModeOptions {
    fn plugin_name(&self) -> String {
        self.plugin_name.clone()
    }
    fn plugin_args(&self) -> String {
        if let Some(var) = &self.plugin_args {
            format!(" ({})", var)
        } else {
            "".into()
        }
    }
}


#[repr(u32)]
#[derive(Debug)]
pub enum ReplicationMode<T>
{
    LogicalReplication(T) = 0,
    PhysicalReplication = 1,
}

impl<T> ReplicationMode<T>
    where T : LogicalReplicationModeOptions
{
    pub fn to_create_replication_slot_part(&self) -> String
    {
        format!("{}", match self {
                Self::LogicalReplication(plugin) => format!(r#"LOGICAL "{}""#, plugin.plugin_name()),
                Self::PhysicalReplication => "PHYSICAL".into(),
            }
        )
    }
    pub fn to_start_replication_part(&self) -> String
    {
        format!("{}", match self {
                Self::LogicalReplication(_) => "LOGICAL",
                Self::PhysicalReplication => "PHYSICAL".into(),
            }
        )
    }
    pub fn to_plugin_options(&self) -> String
    {
        format!("{}", match self {
                ReplicationMode::LogicalReplication(op) => op.plugin_args(),
                ReplicationMode::PhysicalReplication => "".into(),
            }
        )
    }
}

impl<T> fmt::Display for ReplicationMode<T>
    where T : LogicalReplicationModeOptions
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_create_replication_slot_part())
    }
}

#[derive(Debug)]
pub struct CreateReplicationSlotOptions<T>
    where T : LogicalReplicationModeOptions
{
    slot_name: String,
    temporary: bool,
    snapshot_action: String,
    mode: ReplicationMode<T>,
}

impl<T> CreateReplicationSlotOptions<T>
    where T : LogicalReplicationModeOptions
{
    pub fn build_query(&self) -> String {
        let temp = if self.temporary {"TEMPORARY"} else {""};

        format!(
            r#"CREATE_REPLICATION_SLOT {} {} {} {}"#,
            self.slot_name, temp, self.mode.to_create_replication_slot_part(), self.snapshot_action
        )
    }
}

// CreateReplicationSlotResult is the parsed results the CREATE_REPLICATION_SLOT command.
#[derive(Debug)]
pub struct CreateReplicationSlotResult {
    slot_name: String,
    consistent_point: LSN,
    snapshot_name: Option<String>,
    output_plugin: Option<String>,
}

fn simple_query_message_to_simple_query_row(res: &mut IntoIter<SimpleQueryMessage>) -> Result<SimpleQueryRow, PostgresStreamingError> {
     let out = loop {
        match res.next() {
            Some(SimpleQueryMessage::Row(msg)) => break msg,
            Some(SimpleQueryMessage::CommandComplete(num)) => continue,
            _ => return Err(PostgresStreamingError::CreateReplicationResultParsingError("received message from postgres is not a row".into())),
        }
    };
    Ok(out)
}

impl CreateReplicationSlotResult {
    pub fn parse_query_row(res: &mut IntoIter<SimpleQueryMessage>) -> Result<Self, PostgresStreamingError> {
        let row = simple_query_message_to_simple_query_row(res)?;

        Ok(Self {
            slot_name: match row.get("slot_name") {
                Some(m) => m.to_owned(),
                _ => return Err(PostgresStreamingError::CreateReplicationResultParsingError("slot_name not found in result from postgres".into())),
            },
            consistent_point: match row.get("consistent_point") {
                Some(m) => LSN::parse_lsn(m.to_owned())?,
                _ => return Err(PostgresStreamingError::CreateReplicationResultParsingError("consistent_point not found in result from postgres".into())),
            },
            snapshot_name: row.get("snapshot_name").and_then(|m| Some(m.to_owned())),
            output_plugin: row.get("output_plugin").and_then(|m| Some(m.to_owned())),
        })
    }
}

#[derive(Debug)]
pub struct StartReplicationOptions<T>
    where T : LogicalReplicationModeOptions
{
    slot_name: String,
    mode: ReplicationMode<T>,
    start_lsn: LSN,
}

impl<T> StartReplicationOptions<T>
    where T : LogicalReplicationModeOptions
{
    pub fn build_query(&self) -> String {
        format!(
            r#"START_REPLICATION SLOT {} {} {}{}"#,
            self.slot_name, self.mode.to_start_replication_part(), self.start_lsn, self.mode.to_plugin_options()
        )
    }
}

#[derive(Debug)]
pub struct StartReplicationResult {

}

impl fmt::Display for StartReplicationResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", "")
    }
}

impl StartReplicationResult {
    pub fn parse_query_row(res: &mut alloc::vec::IntoIter<SimpleQueryMessage>) -> Result<Self, PostgresStreamingError> {
        let row = simple_query_message_to_simple_query_row(res)?;

        for r in row.columns() {
            let name = r.name();
            let val = match row.get(name) {
                Some(r) => r,
                None => "null",
            };

            println!("{}: {}", name, val);
        }

        Ok(Self {

        })
    }
}


pub enum PostgresStreamingError {
    TokioPostgres(tokio_postgres::Error),
    QueryWrongResult(String, Vec<SimpleQueryMessage>),
    CreateReplicationResultParsingError(String),
    LSNError(LSNError),
}

impl From<tokio_postgres::Error> for PostgresStreamingError {
    fn from(value: Error) -> Self {
        Self::TokioPostgres(value)
    }
}

impl From<LSNError> for PostgresStreamingError {
    fn from(value: LSNError) -> Self {
        Self::LSNError(value)
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
            },
            PostgresStreamingError::LSNError(error) => {
                writeln!(fmt, "{}", error)?;
            }
        };

        Ok(())
    }
}


#[repr(C, packed)]
pub struct PrimaryKeepaliveMessage {
    wal_pos: LSN,
    server_time: u64,
    reply_requested: bool,
}

impl PrimaryKeepaliveMessage {
    pub fn parse(buf: &Bytes) -> &Self {
        let res = unsafe {
            u8_slice_as_any::<Self>(buf)
        };

        res
    }
}

#[repr(C, packed)]
pub struct XLogData {
    wal_start: LSN,
    server_wal_end: LSN,
    server_time: u64
}

impl XLogData {
    pub fn parse(buf: &Bytes) -> &Self{
        let res = unsafe {
            u8_slice_as_any::<Self>(buf)
        };

        res
    }
}

// create_replication_slot creates a logical replication slot.
pub async fn create_replication_slot<T>(
    client: &Client,
    options: &CreateReplicationSlotOptions<T>
) -> Result<CreateReplicationSlotResult, PostgresStreamingError>
    where T : LogicalReplicationModeOptions
{
    let query = options.build_query();
    let mut res = client.simple_query(&query).await?.into_iter();

    CreateReplicationSlotResult::parse_query_row(&mut res)
}

// StartReplication begins the replication process by executing the START_REPLICATION command.
pub async fn start_replication<T>(
    client: &Client,
    options: &StartReplicationOptions<T>
) -> Result<CopyBothDuplex<Bytes>, PostgresStreamingError>
    where T : LogicalReplicationModeOptions
{
    let query = options.build_query();
    let duplex_stream = client.copy_both_simple::<bytes::Bytes>(&query).await?;
    // let duplex_stream = Box::pin(duplex_stream);

    Ok(duplex_stream)
}


async fn keep_alive_response(duplex_stream: &mut Pin<Box<CopyBothDuplex<Bytes>>>) -> Result<(), PostgresStreamingError> {
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

    let plugin = CustomReplicationModeOptions::new(
        "wal2json".into(),
        Some(r#""format-version" '2', "actions" 'insert,update,delete,truncate'"#.into()),
    );

    // let plugin = CustomReplicationModeOptions::new(
    //     "decoderbufs".into(),
    //     None,
    // );

    let options = CreateReplicationSlotOptions {
        slot_name,
        temporary: true,
        mode: ReplicationMode::LogicalReplication(plugin),
        snapshot_action: "".into(),
    };

    let create_slot_res = create_replication_slot(&client, &options).await?;

    let options = StartReplicationOptions {
        slot_name: create_slot_res.slot_name,
        mode: options.mode,
        start_lsn: create_slot_res.consistent_point,
    };
    let query = options.build_query();
    let stream = client.copy_both_simple::<bytes::Bytes>(&query).await?;

    // let stream = start_replication(&client, &options).await?;
    // let mut stream = replication::ReplicationStream::new(stream);
    let mut stream = LogicalReplicationStream::new(stream);
    pin!(stream);

    while let Some(event) = stream.next().await {
    // while let Some(event) = duplex_stream.as_mut().next().await {
        let event = event?;

        match event {
            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
              println!("{:?}", keepalive);
            },
            ReplicationMessage::XLogData(data) => {
              println!("{:?}", data);
            },
            _ => {}
        }
    }

    Ok(())
}


