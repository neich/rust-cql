extern crate std;
extern crate num;
extern crate uuid;

use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use self::uuid::Uuid;
use std::borrow::Cow;
use std::ops::Deref;
use std::error::Error;

pub type CowStr = Cow<'static, str>;


#[derive(Clone, Copy)]
pub enum OpcodeRequest {
    //requests
    OpcodeStartup = 0x01,
    OpcodeCredentials = 0x04,
    OpcodeOptions = 0x05,
    OpcodeQuery = 0x07,
    OpcodePrepare = 0x09,
    OpcodeExecute = 0x0A,
    OpcodeRegister = 0x0B,
    OpcodeBatch = 0x0D
}

#[derive(Debug)]
pub enum OpcodeResponse {
    //responces
    OpcodeError = 0x00,
    OpcodeReady = 0x02,
    OpcodeAuthenticate = 0x03,
    OpcodeSupported = 0x06,
    OpcodeResult = 0x08,
    OpcodeEvent = 0x0C,

    OpcodeUnknown
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
        /* requests
        0x01 => OpcodeStartup,
        0x04 => OpcodeCredentials,
        0x05 => OpcodeOptions,
        0x07 => OpcodeQuery,
        0x09 => OpcodePrepare,
        0x0B => OpcodeRegister,
        */

        0x00 => OpcodeResponse::OpcodeError,
        0x02 => OpcodeResponse::OpcodeReady,
        0x03 => OpcodeResponse::OpcodeAuthenticate,
        0x06 => OpcodeResponse::OpcodeSupported,
        0x08 => OpcodeResponse::OpcodeResult,
        // 0x0A => OpcodeExecute,
        0x0C => OpcodeResponse::OpcodeEvent,

        _ => OpcodeResponse::OpcodeUnknown
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

/*
impl<'a> std::convert::From<&'a std::io::Error> for RCError {
    fn from(err: &'a std::io::Error) -> RCError {
        RCError::new(Cow::Borrowed(err.description()), RCErrorType::IOError)
    }
}
*/

pub type RCResult<T> = Result<T, RCError>;

#[derive(Debug)]
pub enum IpAddr {
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr)
}

pub struct CqlStringMap {
    pub pairs: Vec<CqlPair>,
}

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

#[derive(Debug)]
pub struct Pair<T, V> {
    pub key: T,
    pub value: V
}

pub type CQLList = Vec<CqlValue>;
pub type CQLMap = Vec<Pair<CqlValue, CqlValue>>;
pub type CQLSet = Vec<CqlValue>;

#[derive(Debug)]
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

#[derive(Debug)]
pub struct CqlRow {
    pub cols: Vec<CqlValue>,
}

/*
impl CqlRow {
    fn get_column(&self, col_name: &str) -> Option<CqlValue> {
        let mut i = 0;
        let len = self.metadata.row_metadata.len();
        while i < len {
            if self.metadata.row_metadata.get(i).col_name.as_slice() == col_name {
                return Some(*self.cols.get(i));
            }
            i += 1;
        }
        None
    }
}
*/

#[derive(Debug)]
pub struct CqlRows {
    pub metadata: Box<CqlMetadata>,
    pub rows: Vec<CqlRow>,
}

pub struct CqlRequest<'a> {
    pub version: u8,
    pub flags: u8,
    pub stream: i8,
    pub opcode: OpcodeRequest,
    pub body: CqlRequestBody<'a>,
}

pub enum CqlRequestBody<'a> {
    RequestStartup(CqlStringMap),
    RequestCred(&'a Vec<CowStr>),
    RequestQuery(&'a str, Consistency, u8),
    RequestPrepare(&'a str),
    RequestExec(CowStr, &'a [CqlValue], Consistency, u8),
    RequestBatch(Vec<Query>, BatchType, Consistency, u8),
    RequestOptions,
}

#[derive(Debug)]
pub struct CqlResponse {
    pub version: u8,
    pub flags: u8,
    pub stream: i8,
    pub opcode: OpcodeResponse,
    pub body: CqlResponseBody,
}

#[derive(Debug)]
pub enum CqlResponseBody {
    ResponseError(u32, CowStr),
    ResponseReady,
    ResponseAuth(CowStr),

    ResultVoid,
    ResultRows(Box<CqlRows>),
    ResultKeyspace(CowStr),
    ResultPrepared(Box<CqlPreparedStat>),
    ResultSchemaChange(CowStr, CowStr, CowStr),
    ResultUnknown,

    ResponseEmpty,
}

#[derive(Debug)]
pub struct CqlPreparedStat {
    pub id: Vec<u8>,
    pub meta: Box<CqlMetadata>,
    pub meta_result: Option<Box<CqlMetadata>>
}


pub enum Query {
    QueryStr(CowStr),
    QueryPrepared(CowStr, Vec<CqlValue>),
    QueryBatch(Vec<Query>)
}




