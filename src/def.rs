extern crate std;
extern crate num;
extern crate uuid;
extern crate mio;
extern crate eventual;

use std::time::Duration;
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::thread::{Builder,sleep};
use std::net::{Ipv4Addr,Ipv6Addr,SocketAddr,IpAddr};
use self::uuid::Uuid;
use std::borrow::Cow;
use std::ops::Deref;
use std::error::Error;
use self::eventual::{Future,Async, Timer};

pub type CowStr = Cow<'static, str>;


#[derive(Clone, Copy,PartialEq)]
pub enum OpcodeRequest {
    OpcodeStartup = 0x01,
    OpcodeOptions = 0x05,
    OpcodeQuery = 0x07,
    OpcodePrepare = 0x09,
    OpcodeExecute = 0x0A,
    OpcodeRegister = 0x0B,
    OpcodeBatch = 0x0D,
    OpcodeAuthResponse = 0x0F,
}

#[derive(Debug,PartialEq)]
pub enum OpcodeResponse {
    OpcodeError = 0x00,
    OpcodeReady = 0x02,
    OpcodeAuthenticate = 0x03,
    OpcodeSupported = 0x06,
    OpcodeResult = 0x08,
    OpcodeEvent = 0x0C,
    OpcodeAuthChallenge = 0x0E,
    OpcodeAuthSuccess = 0x10,

    OpcodeUnknown
}

#[derive(Debug)]
pub struct CqlFrameHeader {
    pub version: u8,
    pub flags: u8,
    pub stream: i16,
    pub opcode: u8,
}

enum_from_primitive! {
#[derive(Debug, PartialEq)]
pub enum KindResult {
    KindVoid = 0x0001,
    KindRows = 0x0002,
    KindSetKeyspace = 0x0003,
    KindPrepared = 0x0004,
    KindSchemaChange = 0x0005
}
}

pub fn opcode_response(val: u8) -> OpcodeResponse {
    match val {
        0x00 => OpcodeResponse::OpcodeError,
        0x02 => OpcodeResponse::OpcodeReady,
        0x03 => OpcodeResponse::OpcodeAuthenticate,
        0x06 => OpcodeResponse::OpcodeSupported,
        0x08 => OpcodeResponse::OpcodeResult,
        0x0C => OpcodeResponse::OpcodeEvent,
        0x0E => OpcodeResponse::OpcodeAuthChallenge,
        0x10 => OpcodeResponse::OpcodeAuthSuccess,

        _ => OpcodeResponse::OpcodeUnknown
    }
}

// Stream id is -1 for Event messages
impl OpcodeResponse {
    pub fn is_event_code(&self) -> bool{
        match *self{
            OpcodeResponse::OpcodeEvent => true,
            _ => false,
        }
    }
}

#[derive(Clone, Copy)]
pub enum Consistency {
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    Unknown,
}

#[derive(Clone, Copy)]
pub enum BatchType {
    Logged = 0x00,
    Unlogged = 0x01,
    Counter = 0x02
}

#[derive(Debug,PartialEq)]
pub enum CqlValueType {
    ColumnCustom = 0x0000,
    ColumnASCII = 0x0001,
    ColumnBigInt = 0x0002,
    ColumnBlob = 0x0003,
    ColumnBoolean = 0x0004,
    ColumnCounter = 0x0005,
    ColumnDecimal = 0x0006,
    ColumnDouble = 0x0007,
    ColumnFloat = 0x0008,
    ColumnInt = 0x0009,
    ColumnText = 0x000A,
    ColumnTimestamp = 0x000B,
    ColumnUuid = 0x000C,
    ColumnVarChar = 0x000D,
    ColumnVarint = 0x000E,
    ColumnTimeUuid = 0x000F,
    ColumnInet = 0x0010,
    ColumnList = 0x0020,
    ColumnMap = 0x0021,
    ColumnSet = 0x0022,
    ColumnUnknown,
}

pub fn cql_column_type(val: u16) -> CqlValueType {
    match val {
        0x0000 => CqlValueType::ColumnCustom,
        0x0001 => CqlValueType::ColumnASCII,
        0x0002 => CqlValueType::ColumnBigInt,
        0x0003 => CqlValueType::ColumnBlob,
        0x0004 => CqlValueType::ColumnBoolean,
        0x0005 => CqlValueType::ColumnCounter,
        0x0006 => CqlValueType::ColumnDecimal,
        0x0007 => CqlValueType::ColumnDouble,
        0x0008 => CqlValueType::ColumnFloat,
        0x0009 => CqlValueType::ColumnInt,
        0x000A => CqlValueType::ColumnText,
        0x000B => CqlValueType::ColumnTimestamp,
        0x000C => CqlValueType::ColumnUuid,
        0x000D => CqlValueType::ColumnVarChar,
        0x000E => CqlValueType::ColumnVarint,
        0x000F => CqlValueType::ColumnTimeUuid,
        0x0010 => CqlValueType::ColumnInet,
        0x0020 => CqlValueType::ColumnList,
        0x0021 => CqlValueType::ColumnMap,
        0x0022 => CqlValueType::ColumnSet,
        _ => CqlValueType::ColumnUnknown
    }
}

pub const TOPOLOGY_CHANGE:  &'static str = "TOPOLOGY_CHANGE";
pub const STATUS_CHANGE:  &'static str = "STATUS_CHANGE";
pub const SCHEMA_CHANGE:  &'static str = "SCHEMA_CHANGE";

pub const TOPOLOGY_CHANGE_NEW_NODE:  &'static str = "NEW_NODE";
pub const TOPOLOGY_CHANGE_REMOVED_NODE:  &'static str = "REMOVED_NODE";
pub const TOPOLOGY_CHANGE_MOVED_NODE:  &'static str = "MOVED_NODE";

pub const STATUS_CHANGE_UP:  &'static str = "UP";
pub const STATUS_CHANGE_DOWN:  &'static str = "DOWN";

pub const SCHEMA_CHANGE_TYPE_CREATED:  &'static str = "CREATED";
pub const SCHEMA_CHANGE_TYPE_UPDATED:  &'static str = "UPDATED";
pub const SCHEMA_CHANGE_TYPE_DROPPED:  &'static str = "DROPPED";

pub const SCHEMA_CHANGE_TARGET_KEYSPACE:  &'static str = "KEYSPACE";
pub const SCHEMA_CHANGE_TARGET_TABLE:  &'static str = "TABLE";
pub const SCHEMA_CHANGE_TARGET_TYPE:  &'static str = "TYPE";

#[derive(Debug)]
pub enum CqlEventType {
    TopologyChange,
    StatusChange,
    SchemaChange,
    UnknowEventType
}


impl CqlEventType{
    pub fn from_str(str: &str) -> CqlEventType {
        match str {
            TOPOLOGY_CHANGE  => CqlEventType::TopologyChange,
            STATUS_CHANGE    => CqlEventType::StatusChange,
            SCHEMA_CHANGE    => CqlEventType::SchemaChange,
            _ => CqlEventType::UnknowEventType
        }
    }

    pub fn get_str(&self) -> CowStr {
        match *self {
            CqlEventType::TopologyChange   => Cow::Borrowed(TOPOLOGY_CHANGE),
            CqlEventType::StatusChange     => Cow::Borrowed(STATUS_CHANGE),
            CqlEventType::SchemaChange     => Cow::Borrowed(SCHEMA_CHANGE),
            _ => Cow::Borrowed("UNKNOWN_EVENT")
        }
    }
}

#[derive(Debug,PartialEq)]
pub enum CqlEvent {
    TopologyChange(TopologyChangeType,SocketAddr),
    StatusChange(StatusChangeType,SocketAddr),
    SchemaChange(SchemaChangeType,SchemaChangeOptions),
    UnknownEvent
}

#[derive(Debug,PartialEq)]
pub enum TopologyChangeType{
    NewNode,
    RemovedNode,
    MovedNode,
    UnknownChange
}

impl TopologyChangeType{
    pub fn from_str(string: &str) -> TopologyChangeType {
        match string {
            TOPOLOGY_CHANGE_NEW_NODE         => TopologyChangeType::NewNode,
            TOPOLOGY_CHANGE_REMOVED_NODE     => TopologyChangeType::RemovedNode,
            TOPOLOGY_CHANGE_MOVED_NODE       => TopologyChangeType::MovedNode,
            _ => TopologyChangeType::UnknownChange
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum StatusChangeType{
    Up,
    Down,
    UnknownStatus
}

impl StatusChangeType{
    pub fn from_str(string: &str) -> StatusChangeType {
        match string {
            STATUS_CHANGE_UP      => StatusChangeType::Up,
            STATUS_CHANGE_DOWN     => StatusChangeType::Down,
            _ => StatusChangeType::UnknownStatus
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum SchemaChangeType{
    Created,
    Updated,
    Dropped,
    UnknownSchema
}

impl SchemaChangeType{
    pub fn from_str(string: &str) -> SchemaChangeType {
        match string {
            SCHEMA_CHANGE_TYPE_CREATED     => SchemaChangeType::Created,
            SCHEMA_CHANGE_TYPE_UPDATED     => SchemaChangeType::Updated,
            SCHEMA_CHANGE_TYPE_DROPPED     => SchemaChangeType::Dropped,
            _ => SchemaChangeType::UnknownSchema
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum SchemaChangeOptions{
    Keyspace(CowStr),
    Table(CowStr,CowStr),
    Type(CowStr,CowStr)
}

#[derive(Debug)]
pub enum RCErrorType {
    ReadError,
    WriteError,
    SerializeError,
    ConnectionError,
    NoDataError,
    GenericError,
    IOError,
    EventLoopError,
    ClusterError
}

#[derive(Debug)]
pub struct RCError {
    pub kind: RCErrorType,
    pub desc: CowStr,
}


impl RCError {
    pub fn new<S: Into<CowStr>>(msg: S, kind: RCErrorType) -> RCError {
        RCError {
            kind: kind,
            desc: msg.into()
        }
    }

    pub fn description(&self) -> &str {
        return self.desc.deref();
    }

}


impl std::error::Error for RCError {
    fn description(&self) -> &str {
        return self.desc.deref();
    }

    fn cause(&self) -> Option<&std::error::Error> {
        return None;
    }
}

impl std::fmt::Display for RCError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "Error: {}", self.desc)
    }
}

pub type RCResult<T> = Result<T, RCError>;


#[derive(Debug, Clone)]
pub struct CqlStringMap {
    pub pairs: Vec<CqlPair>,
}

#[derive(Debug, Clone)]
pub struct CqlPair {
    pub key: &'static str,
    pub value: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub enum CqlBytesSize {
   Cqli32,
   Cqli16,
   Cqli8 
}

pub struct CqlTableDesc {
    pub keyspace: String,
    pub tablename: String
}

#[derive(Debug, PartialEq)]
pub struct CqlColMetadata {
    pub keyspace: CowStr,
    pub table: CowStr,
    pub col_name: CowStr,
    pub custom_type: Option<CowStr>,
    pub col_type: CqlValueType,
    pub col_type_aux1: CqlValueType,
    pub col_type_aux2: CqlValueType
}

#[derive(Debug, PartialEq)]
pub struct CqlMetadata {
    pub flags: u32,
    pub column_count: u32,
    pub keyspace: CowStr,
    pub table: CowStr,
    pub row_metadata: Vec<CqlColMetadata>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pair<T, V> {
    pub key: T,
    pub value: V
}

pub type CQLList = Vec<CqlValue>;

pub type CQLMap = Vec<Pair<CqlValue, CqlValue>>;

pub type CQLSet = Vec<CqlValue>;

#[derive(Debug, Clone, PartialEq)]
pub enum CqlValue {
    CqlASCII(Option<CowStr>),
    CqlBigInt(Option<i64>),
    CqlBlob(Option<Vec<u8>>),
    CqlBoolean(Option<bool>),
    CqlCounter(Option<i64>),
    CqlDecimal(Option<num::BigInt>),
    CqlDouble(Option<f64>),
    CqlFloat(Option<f32>),
    CqlInet(Option<IpAddr>),
    CqlInt(Option<i32>),
    CqlList(Option<CQLList>),
    CqlMap(Option<CQLMap>),
    CqlSet(Option<CQLSet>),
    CqlText(Option<CowStr>),
    CqlTimestamp(Option<u64>),
    CqlUuid(Option<Uuid>),
    CqlTimeUuid(Option<Uuid>),
    CqlVarchar(Option<CowStr>),
    CqlVarint(Option<num::BigInt>),
    CqlUnknown,
}

#[derive(Debug,Clone,PartialEq)]
pub struct CqlRow {
    pub cols: Vec<CqlValue>,
}

#[derive(Debug,PartialEq)]
pub struct CqlRows {
    pub metadata: CqlMetadata,
    pub rows: Vec<CqlRow>,
}

pub struct CqlRequest {
    pub version: u8,
    pub flags: u8,
    pub stream: i16,
    pub opcode: OpcodeRequest,
    pub body: CqlRequestBody,
}

impl CqlRequest{
    pub fn set_stream(&mut self,stream: i16) -> RCResult<()>{
        if self.version==1 || self.version==2 {
            if stream > 128{
               return Err(RCError::new("Stream id can't be more than 128 for v1 and v2", RCErrorType::EventLoopError))
            }
        }
        else if self.version == 3{
            if stream > 32768{
               return Err(RCError::new("Stream id can't be more than 128 for v1 and v2", RCErrorType::EventLoopError))
            }
        }
        self.stream=stream;
        Ok(())
    }
}

pub enum CqlRequestBody {
    RequestStartup(CqlStringMap),
    RequestCred(Vec<CowStr>),
    RequestQuery(String, Consistency, u8),
    RequestPrepare(String),
    RequestExec(Vec<u8>, Vec<CqlValue>, Consistency, u8),
    RequestBatch(Vec<Query>, BatchType, Consistency, u8),
    RequestOptions,
    RequestAuthResponse(Vec<u8>),
    RequestRegister(Vec<CqlValue>)
}

#[derive(Debug, PartialEq)]
pub struct CqlResponse {
    pub version: u8,
    pub flags: u8,
    pub stream: i16,
    pub opcode: OpcodeResponse,
    pub body: CqlResponseBody,
}

impl CqlResponse{
    pub fn is_event(&self)-> bool{
        self.opcode.is_event_code() && self.stream==-1
    }
}


#[derive(Debug,PartialEq)]
pub enum CqlResponseBody {
    ResponseError(u32, CowStr),
    ResponseReady,
    ResponseAuthenticate(CowStr),
    ResponseAuthChallenge(Vec<u8>),
    ResponseAuthSuccess(Vec<u8>),
    ResponseEvent(CqlEvent),

    ResultVoid,
    ResultRows(CqlRows),
    ResultKeyspace(CowStr),
    ResultPrepared(CqlPreparedStat),
    ResultSchemaChange(CowStr, CowStr, CowStr),
    ResultUnknown,

    ResponseEmpty,
}

#[derive(Debug,PartialEq)]
pub struct CqlPreparedStat {
    pub id: Vec<u8>,
    pub meta: CqlMetadata,
    pub meta_result: Option<CqlMetadata>
}


pub enum Query {
    QueryStr(CowStr),
    QueryPrepared(Vec<u8>, Vec<CqlValue>),
    QueryBatch(Vec<Query>)
}



pub type CassFuture = Future<RCResult<CqlResponse>,()>;


pub static CQL_VERSION_STRINGS:  [&'static str; 3] = ["3.0.0", "3.0.0", "3.0.0"];
pub static CQL_MAX_SUPPORTED_VERSION: u8 = 0x03;
pub static CQL_DEFAULT_PORT: u16 = 9042;

pub const CQL_MAX_STREAM_ID_V1_V2 : i16 = 128;

pub const CQL_MAX_STREAM_ID_V3 : i16 = 32768;

pub fn to_hex_string(bytes: &Vec<u8>) -> String {
  let strs: Vec<String> = bytes.iter()
                               .map(|b| format!("{:02X}", b))
                               .collect();
  strs.connect(" ")
}

pub fn max_stream_id(stream_id: i16,version: u8) -> bool{
    (stream_id >= CQL_MAX_STREAM_ID_V1_V2 && (version == 1 || version == 2))
      || (stream_id== CQL_MAX_STREAM_ID_V3 && version == 3)
}

pub fn set_interval<F>(delay: Duration,f: F) -> std::sync::mpsc::Sender<()>
    where F: Fn(), F: Send + 'static + Sync{

    let (tx, rx) = channel::<(())>();
    thread::Builder::new().name("tick".to_string()).spawn(move || {
        while !rx.try_recv().is_ok() {
            sleep(delay);
            println!("Hello there! From tick :)");
            f();  //Do stuff here
        }
    }).unwrap();
    tx
}
