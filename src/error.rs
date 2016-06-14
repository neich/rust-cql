
extern crate std;

use std::net::{Ipv4Addr,Ipv6Addr,SocketAddr,IpAddr};
use uuid::Uuid;
use std::borrow::Cow;
use std::ops::Deref;
use std::error::Error;
use def::CowStr;

#[derive(Debug,Clone)]
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

#[derive(Debug,Clone)]
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