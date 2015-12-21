#![crate_name = "cql"]
#![crate_type = "rlib"]
#![crate_type = "dylib"]

#[macro_use] extern crate enum_primitive as ep;

pub use client::connect;
pub use client::CassFuture;
pub use def::Consistency;
pub use def::BatchType;
pub use def::CqlValue;
pub use def::CqlValue::CqlFloat;
pub use def::CqlValue::CqlVarchar;
pub use def::CQLList;
pub use def::CQLMap;
pub use def::CQLSet;
pub use def::Query::QueryStr;
pub use def::Query::QueryPrepared;
pub use def::OpcodeResponse;
pub use def::CqlResponseBody;
pub use def::RCResult;
pub use def::RCError;

#[macro_export]
macro_rules! try_bo(
    ($call: expr, $msg: expr) => {
        match $call {
            Ok(val) => val,
            Err(self::byteorder::Error::UnexpectedEOF) => return Err($crate::def::RCError::new(format!("{} -> {}", $msg, "Unexpected EOF"), $crate::def::RCErrorType::IOError)),
            Err(self::byteorder::Error::Io(ref err)) => {
            	use std::error::Error;
            	return Err($crate::def::RCError::new(format!("{} -> {}", $msg, err.description()), $crate::def::RCErrorType::IOError))
            }
        };
    }
);

#[macro_export]
macro_rules! try_io(
    ($call: expr, $msg: expr) => {
        match $call {
            Ok(val) => val,
            Err(ref err) => {
            	use std::error::Error;
            	return Err(RCError::new(format!("{} -> {}", $msg, err.description()), RCErrorType::IOError))
            }
        };
    }
);


#[macro_export]
macro_rules! try_rc(
    ($call: expr, $msg: expr) => {
        match $call {
            Ok(val) => val,
            Err(ref err) => return Err($crate::def::RCError::new(format!("{} -> {}", $msg, err.description()), $crate::def::RCErrorType::IOError))
        };
    }
);

macro_rules! try_rc_length(
    ($call: expr, $msg: expr) => {
        match $call {
            Ok(-1) => return Ok(None),
            Ok(val) => val,
            Err(ref err) => return Err($crate::def::RCError::new(format!("{} -> {}", $msg, err.description()), $crate::def::RCErrorType::IOError))
        };
    }
);

macro_rules! try_rc_noption(
    ($call: expr, $msg: expr) => {
        match $call {
            Ok(option) => match option {
                None => return Err($crate::def::RCError::new(format!("{} -> {}", $msg, "No data found (length == -1)"), $crate::def::RCErrorType::IOError)),
                Some(val) => val
            },
            Err(ref err) => return Err($crate::def::RCError::new(format!("{} -> {}", $msg, err.description()), $crate::def::RCErrorType::IOError))
        };
    }
);


macro_rules! CowStr_tuple_void(
    () => {
        (Cow::Borrowed(""), Cow::Borrowed(""))
    }
);

mod def;
mod reader;
mod serialize;
pub mod client;
