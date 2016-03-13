extern crate std;
extern crate num;
extern crate uuid;
extern crate eventual;

use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use self::uuid::Uuid;
use std::borrow::Cow;
use std::ops::Deref;
use std::error::Error;
use self::eventual::Future;

pub type CowStr = Cow<'static, str>;


#[derive(Clone, Copy)]
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

#[derive(Debug)]
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
    pub fn isEventCode(&self) -> bool{
        match self{
            &OpcodeResponse::OpcodeEvent => true,
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

#[derive(Debug)]
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

#[derive(Debug)]
pub enum CqlEventType {
    EventTopologyChange,
    EventStatusChange,
    EventSchemaChange,
    EventUnknow
}

impl CqlEventType{
    pub fn get_str(&self) -> CowStr {
        match *self {
            CqlEventType::EventTopologyChange => Cow::Borrowed("TOPOLOGY_CHANGE"),
            CqlEventType::EventStatusChange => Cow::Borrowed("STATUS_CHANGE"),
            CqlEventType::EventSchemaChange => Cow::Borrowed("SCHEMA_CHANGE"),
            _ => Cow::Borrowed("UNKNOWN_EVENT")
        }
    }
}

#[derive(Debug)]
pub enum RCErrorType {
    ReadError,
    WriteError,
    SerializeError,
    ConnectionError,
    NoDataError,
    GenericError,
    IOError
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
pub enum IpAddress {
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr)
}

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
   Cqli16 
}

pub struct CqlTableDesc {
    pub keyspace: String,
    pub tablename: String
}

#[derive(Debug)]
pub struct CqlColMetadata {
    pub keyspace: CowStr,
    pub table: CowStr,
    pub col_name: CowStr,
    pub col_type: CqlValueType,
    pub col_type_aux1: CqlValueType,
    pub col_type_aux2: CqlValueType
}

#[derive(Debug)]
pub struct CqlMetadata {
    pub flags: u32,
    pub column_count: u32,
    pub keyspace: CowStr,
    pub table: CowStr,
    pub row_metadata: Vec<CqlColMetadata>,
}

#[derive(Debug, Clone)]
pub struct Pair<T, V> {
    pub key: T,
    pub value: V
}

pub type CQLList = Vec<CqlValue>;
pub type CQLMap = Vec<Pair<CqlValue, CqlValue>>;
pub type CQLSet = Vec<CqlValue>;

#[derive(Debug, Clone)]
pub enum CqlValue {
    CqlASCII(Option<CowStr>),
    CqlBigInt(Option<i64>),
    CqlBlob(Option<Vec<u8>>),
    CqlBoolean(Option<bool>),
    CqlCounter(Option<i64>),
    CqlDecimal(Option<num::BigInt>),
    CqlDouble(Option<f64>),
    CqlFloat(Option<f32>),
    CqlInet(Option<IpAddress>),
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

#[derive(Debug)]
pub struct CqlRow {
    pub cols: Vec<CqlValue>,
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct CqlResponse {
    pub version: u8,
    pub flags: u8,
    pub stream: i16,
    pub opcode: OpcodeResponse,
    pub body: CqlResponseBody,
}



#[derive(Debug)]
pub enum CqlResponseBody {
    ResponseError(u32, CowStr),
    ResponseReady,
    ResponseAuthenticate(CowStr),
    ResponseAuthChallenge(Vec<u8>),
    ResponseAuthSuccess(Vec<u8>),
    ResponseEvent(CQLList),

    ResultVoid,
    ResultRows(CqlRows),
    ResultKeyspace(CowStr),
    ResultPrepared(CqlPreparedStat),
    ResultSchemaChange(CowStr, CowStr, CowStr),
    ResultUnknown,

    ResponseEmpty,
}

#[derive(Debug)]
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
pub static CQL_MAX_SUPPORTED_VERSION:u8 = 0x03;