
extern crate std;
extern crate core;
extern crate num;
extern crate uuid;

use std::str::SendStr;

use super::def::*;
use super::serialize::*;
use super::reader::*;
use std::collections::TreeMap;

pub static CQL_VERSION_STRINGS:  [&'static str, .. 3] = ["3.0.0", "3.0.0", "3.0.0"];
pub static CQL_MAX_SUPPORTED_VERSION:u8 = 0x03;

macro_rules! serialize_and_check_io_error(
    ($writer: expr, $obj: ident, $version: expr, $msg: expr) => {
        match $obj.serialize($writer, $version) {
            Err(e) => return Err(RCError { kind: SerializeError, desc: format!("{} -> {}", $msg, e.desc).into_maybe_owned()}),
            _ => ()
        }
    }
)

type PrepsStore = TreeMap<String, Box<CqlPreparedStat>>;

pub struct CqlClient {
    socket: std::io::net::tcp::TcpStream,
    pub version: u8,
    prepared: PrepsStore
}


impl CqlClient {

    fn new(socket: std::io::net::tcp::TcpStream, version: u8) -> CqlClient {
        CqlClient {socket: socket, version: version, prepared: TreeMap::new()}
    }

    fn build_auth<'a>(&self, creds: &'a Vec<SendStr>, stream: i8) -> CqlRequest<'a> {
        return CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: stream,
            opcode: OpcodeOptions,
            body: RequestCred(creds),
        };
    }

    fn build_options(&self) -> CqlRequest {
        return CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeOptions,
            body: RequestOptions,
        };
    }

    pub fn exec_query(&mut self, query_str: &str, con: Consistency::Consistency) -> RCResult<CqlResponse> {
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeQuery,
            body: RequestQuery(query_str, con, 0)};

        let ref mut socket = self.socket;
        serialize_and_check_io_error!(socket, q, self.version, "Error serializing query");
        let res = read_and_check_io_error!(socket, read_cql_response, self.version, "Error reading query");
        Ok(res)
    }

    pub fn exec_prepared(&mut self, ps_id: &str, params: &[CqlValue], con: Consistency::Consistency) -> RCResult<CqlResponse> {
        let preps = match self.prepared.find(&String::from_str(ps_id)) {
            Some(ps) => &**ps,
            None => return Err(RCError::new(format!("Unknown prepared statement <{}>", ps_id), GenericError))
        };

        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeExecute,
            body: RequestExec(preps, params, con, 0x01),
        };

        serialize_and_check_io_error!(&mut self.socket, q, self.version, "Error serializing prepared statement execution");

        /* Code to debug prepared statements. Write to file the serialization of the request

        let path = Path::new("prepared_data.bin");
        let display = path.display();
        let mut file = match std::io::File::create(&path) {
            Err(why) => fail!("couldn't create {}: {}", display, why.desc),
            Ok(file) => file,
        };

        serialize_and_check_io_error!(&mut file, q, self.version, "Error serializing query");
        
        */

        Ok(read_and_check_io_error!(self.socket, read_cql_response, self.version, "Error reading prepared statement execution result"))
    }

    pub fn prepared_statement(&mut self, query_str: &str, query_id: &str) -> RCResult<()> {
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodePrepare,
            body: RequestPrepare(query_str),
        };


        serialize_and_check_io_error!(&mut self.socket, q, self.version, "Error serializing prepared statement");

        let res = read_and_check_io_error!(&mut self.socket, read_cql_response, self.version, "Error reading query");
        match res.body {
            ResultPrepared(preps) => {
                self.prepared.insert(String::from_str(query_id), preps);
                Ok(())
            },
            _ => Err(RCError::new("Response does not contain prepared statement", ReadError))
        }
    }
}


fn send_startup(socket: &mut std::io::TcpStream, version: u8, creds: Option<&Vec<SendStr>>) -> RCResult<()> {
    let body = CqlStringMap {
        pairs:vec![CqlPair{key: "CQL_VERSION", value: CQL_VERSION_STRINGS[(version-1) as uint]}],
    };
    let msg_startup = CqlRequest {
        version: version,
        flags: 0x00,
        stream: 0x01,
        opcode: OpcodeStartup,
        body: RequestStartup(body),
    };
    serialize_and_check_error!(socket, msg_startup, version, "Error serializing startup message");

    let response = read_and_check_io_error!(socket, read_cql_response, version, "Error reding response");
    match response.body {
        ResponseReady =>  Ok(()),
        ResponseAuth(_) => {
            match creds {
                Some(cred) => {
                    let msg_auth = CqlRequest {
                        version: version,
                        flags: 0x00,
                        stream: 0x01,
                        opcode: OpcodeOptions,
                        body: RequestCred(cred),
                    };
                    serialize_and_check_error!(socket, msg_auth, version, "Error serializing request (auth)");
                    
                    let response = read_and_check_io_error!(socket, read_cql_response, version, "Error reding authenticaton response");
                    match response.body {
                        ResponseReady => Ok(()),
                        ResponseError(_, ref msg) => Err(RCError::new(format!("Error in authentication: {}", msg), ReadError)),
                        _ => Err(RCError::new("Server returned unknown message", ReadError))
                    }
                },
                None => Err(RCError::new("Credential should be provided for authentication", ReadError))
            }
        },
        ResponseError(_, ref msg) => Err(RCError::new(format!("Error connecting: {}", msg), ReadError)),
        _ => Err(RCError::new("Wrong response to startup", ReadError))
    }
}

pub fn connect(ip: &'static str, port: u16, creds:Option<&Vec<SendStr>>) -> RCResult<CqlClient> {

    let mut version = CQL_MAX_SUPPORTED_VERSION;

    while version >= 0x01 {
        let res = std::io::TcpStream::connect(ip.as_slice(), port);
        if res.is_err() {
            return Err(RCError::new(format!("Failed to connect to server at {}:{}", ip.as_slice(), port), ConnectionError));
        }
        
        let mut socket = res.unwrap();

        match send_startup(& mut socket, version, creds) {
            Ok(_) => return Ok(CqlClient::new(socket, version)),
            Err(e) => println!("Error connecting with protocol version v{}: {}", version, e.desc)
        }
        version -= 1;
    }
    Err(RCError::new("Unable to find suitable protocol version (v1, v2, v3)", ReadError))
}
