#![crate_id = "cql#0.1"]
#![comment = "A Rust CQl binary protocol implementation"]
#![license = "MIT/ASL2"]
#![crate_type = "rlib"]
#![crate_type = "dylib"]

#![feature(macro_rules)]
#![feature(globs)]
#![feature(phase)]
// #[phase(plugin, link)] extern crate log;

pub use client::connect;
pub use def::Consistency;
pub use def::CqlValue;
pub use def::CqlFloat;
pub use def::CqlVarchar;
pub use def::CQLList;
pub use def::CQLMap;
pub use def::CQLSet;

#[macro_export]
macro_rules! read_and_check_io_error(
    ($reader: expr, $method: ident, $msg: expr) => {
        match $reader.$method() {
            Ok(val) => val,
            Err(e) => return Err(RCError::new(format!("{} -> {}", $msg, e.desc), ReadError))
        }
    };
    ($reader: expr, $method: ident, $arg: expr, $msg: expr) => {
        match $reader.$method($arg) {
            Ok(val) => val,
            Err(e) => return Err(RCError::new(format!("{} -> {}", $msg, e.desc), ReadError))
        }
    };
    ($reader: ident, $method: ident, $arg1: expr, $arg2: expr, $msg: expr) => {
        match $reader.$method($arg1, $arg2) {
            Ok(val) => val,
            Err(e) => return Err(RCError::new(format!("{} -> {}", $msg, e.desc), ReadError))
        }
    }
)



macro_rules! write_and_check_io_error(
    ($writer: ident, $method: ident, $arg: expr, $msg: expr) => {
        match $writer.$method($arg) {
            Err(e) => return Err(RCError { kind: WriteError, desc: format!("{} -> {}", $msg, e.desc).into_maybe_owned()}),
            _ => ()
        }
    }
)


macro_rules! read_and_check_io_option(
    ($method: ident, $val_type: ident, $msg: expr) => {
        match self.$method($val_type) {
            Ok(val) => Some(val),
            Err(_) => None
        }
    };
    ($method: ident, $val_type: ident, $arg: expr, $msg: expr) => {
        match self.$method($val_type, $arg) {
            Ok(val) => Some(val),
            Err(_) => None
        }
    }
)

macro_rules! serialize_and_check_error(
    ($writer: expr, $obj: ident, $version: expr, $msg: expr) => {
        match $obj.serialize($writer, $version) {
            Ok(_) => (),
            Err(e) => return Err(RCError { kind: SerializeError, desc: format!("{} -> {}", $msg, e.desc).into_maybe_owned()})
        }
    }
)


macro_rules! sendstr_tuple_void(
    () => {
        ("".into_maybe_owned(), "".into_maybe_owned())
    }
)

mod def;
mod reader;
mod serialize;
pub mod client;
