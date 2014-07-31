extern crate std;
extern crate num;
extern crate uuid;

use std::str::SendStr;
use std::io::net::ip::IpAddr;
use self::uuid::Uuid;

pub enum OpcodeRequest {
    //requests
    OpcodeStartup = 0x01,
    OpcodeCredentials = 0x04,
    OpcodeOptions = 0x05,
    OpcodeQuery = 0x07,
    OpcodePrepare = 0x09,
    OpcodeExecute = 0x0A,
    OpcodeRegister = 0x0B,
}

#[deriving(Show)]
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

#[deriving(Show, FromPrimitive)]
pub enum KindResult {
    KindVoid = 0x0001,
    KindRows = 0x0002,
    KindSetKeyspace = 0x0003,
    KindPrepared = 0x0004,
    KindSchemaChange = 0x0005
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

        0x00 => OpcodeError,
        0x02 => OpcodeReady,
        0x03 => OpcodeAuthenticate,
        0x06 => OpcodeSupported,
        0x08 => OpcodeResult,
        // 0x0A => OpcodeExecute,
        0x0C => OpcodeEvent,

        _ => OpcodeUnknown
    }
}

pub mod Consistency {
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
}

#[deriving(Show)]
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
        0x0000 => ColumnCustom,
        0x0001 => ColumnASCII,
        0x0002 => ColumnBigInt,
        0x0003 => ColumnBlob,
        0x0004 => ColumnBoolean,
        0x0005 => ColumnCounter,
        0x0006 => ColumnDecimal,
        0x0007 => ColumnDouble,
        0x0008 => ColumnFloat,
        0x0009 => ColumnInt,
        0x000A => ColumnText,
        0x000B => ColumnTimestamp,
        0x000C => ColumnUuid,
        0x000D => ColumnVarChar,
        0x000E => ColumnVarint,
        0x000F => ColumnTimeUuid,
        0x0010 => ColumnInet,
        0x0020 => ColumnList,
        0x0021 => ColumnMap,
        0x0022 => ColumnSet,
        _ => ColumnUnknown
    }
}


#[deriving(Show)]
pub enum RCErrorType {
    ReadError,
    WriteError,
    SerializeError,
    ConnectionError,
    NoDataError,
    GenericError
}

#[deriving(Show)]
pub struct RCError {
    pub kind: RCErrorType,
    pub desc: SendStr,
}

impl RCError {
    pub fn new<T: IntoMaybeOwned<'static>>(msg: T, kind: RCErrorType) -> RCError {
        RCError {
            kind: kind,
            desc: msg.into_maybe_owned()
        }
    }
}

pub type RCResult<T> = Result<T, RCError>;

pub struct CqlStringMap {
    pub pairs: Vec<CqlPair>,
}

pub struct CqlPair {
    pub key: &'static str,
    pub value: &'static str,
}

pub enum CqlBytesSize {
   Cqli32,
   Cqli16 
}



#[deriving(Show)]
pub struct CqlColMetadata {
    pub keyspace: SendStr,
    pub table: SendStr,
    pub col_name: SendStr,
    pub col_type: CqlValueType,
    pub col_type_aux1: CqlValueType,
    pub col_type_aux2: CqlValueType
}

#[deriving(Show)]
pub struct CqlMetadata {
    pub flags: u32,
    pub column_count: u32,
    pub keyspace: SendStr,
    pub table: SendStr,
    pub row_metadata: Vec<CqlColMetadata>,
}

#[deriving(Show)]
pub struct Pair<T, V> {
    pub key: T,
    pub value: V
}

pub type CQLList = Vec<CqlValue>;
pub type CQLMap = Vec<Pair<CqlValue, CqlValue>>;
pub type CQLSet = Vec<CqlValue>;

#[deriving(Show)]
pub enum CqlValue {
    CqlASCII(Option<SendStr>),
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
    CqlText(Option<SendStr>),
    CqlTimestamp(Option<u64>),
    CqlUuid(Option<Uuid>),
    CqlTimeUuid(Option<Uuid>),
    CqlVarchar(Option<SendStr>),
    CqlVarint(Option<num::BigInt>),
    CqlUnknown,
}

#[deriving(Show)]
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

#[deriving(Show)]
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
    RequestCred(&'a Vec<SendStr>),
    RequestQuery(&'a str, Consistency::Consistency, u8),
    RequestPrepare(&'a str),
    RequestExec(&'a CqlPreparedStat, &'a [CqlValue], Consistency::Consistency, u8),
    RequestOptions,
}

#[deriving(Show)]
pub struct CqlResponse {
    pub version: u8,
    pub flags: u8,
    pub stream: i8,
    pub opcode: OpcodeResponse,
    pub body: CqlResponseBody,
}

#[deriving(Show)]
pub enum CqlResponseBody {
    ResponseError(u32, SendStr),
    ResponseReady,
    ResponseAuth(SendStr),

    ResultVoid,
    ResultRows(Box<CqlRows>),
    ResultKeyspace(SendStr),
    ResultPrepared(Box<CqlPreparedStat>),
    ResultSchemaChange(SendStr, SendStr, SendStr),
    ResultUnknown,

    ResponseEmpty,
}

#[deriving(Show)]
pub struct CqlPreparedStat {
    pub id: Vec<u8>,
    pub meta: Box<CqlMetadata>,
    pub meta_result: Option<Box<CqlMetadata>>
}




