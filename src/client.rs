extern crate std;
extern crate num;
extern crate uuid;

use std::collections::BTreeMap;
use std::borrow::Cow;
use std::path::Path;
use std::error::Error;

use super::def::*;
use super::def::OpcodeRequest::*;
use super::def::CqlRequestBody::*;
use super::def::RCErrorType::*;
use super::def::CqlResponseBody::*;
use super::serialize::CqlSerializable;
use super::reader::*;

pub static CQL_VERSION_STRINGS:  [&'static str; 3] = ["3.0.0", "3.0.0", "3.0.0"];
pub static CQL_MAX_SUPPORTED_VERSION:u8 = 0x03;

type PrepsStore = BTreeMap<String, Box<CqlPreparedStat>>;

pub struct Client {
    socket: std::net::TcpStream,
    pub version: u8,
    prepared: PrepsStore
}

impl Client {

    fn new(socket: std::net::TcpStream, version: u8) -> Client {
        Client {socket: socket, version: version, prepared: BTreeMap::new()}
    }

    fn build_auth<'a>(&self, creds: &'a Vec<CowStr>, stream: i8) -> CqlRequest<'a> {
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

    pub fn get_prepared_statement(&mut self, ps_id: &str) -> RCResult<&CqlPreparedStat> {
        match self.prepared.get(ps_id) {
            Some(ps) => Ok(&**ps),
            None => return Err(RCError::new(format!("Unknown prepared statement <{}>", ps_id), GenericError))
        }
    }

    pub fn exec_query(&mut self, query_str: &str, con: Consistency) -> RCResult<CqlResponse> {
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeQuery,
            body: RequestQuery(query_str, con, 0)};

        let mut socket = try_io!(self.socket.try_clone(), "Cannot clone tcp handle");
        try_rc!(q.serialize(&mut socket, self.version), "Error serializing query");
        Ok(try_rc!(socket.read_cql_response(self.version), "Error reading query"))
    }

    pub fn exec_prepared(&mut self, preps: &Vec<u8>, params: &[CqlValue], con: Consistency) -> RCResult<CqlResponse> {

        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeExecute,
            body: RequestExec(preps.clone(), params, con, 0x01),
        };

        let mut socket = try_io!(self.socket.try_clone(), "Cannot clone tcp handle");
        try_rc!(q.serialize(&mut socket, self.version), "Error serializing prepared statement execution");

        // Code to debug prepared statements. Write to file the serialization of the request
        let path = Path::new("prepared_data.bin");
        let display = path.display();
        let mut file = match std::fs::File::create(&path) {
            Err(err) => panic!("couldn't create {}: {}", display, err.description()),
            Ok(file) => file,
        };
        try_rc!(q.serialize(&mut file, self.version), "Error serializing query to file");

        Ok(try_rc!(socket.read_cql_response(self.version), "Error reading prepared statement execution result"))
    }

    pub fn exec_batch(&mut self, q_type: BatchType, q_vec: Vec<Query>, con: Consistency) -> RCResult<CqlResponse> {
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeBatch,
            body: RequestBatch(q_vec, q_type, con, 0)};

        /* Code to debug batch statements. Write to file the serialization of the request

        let path = Path::new("batch_data.bin");
        let display = path.display();
        let mut file = match std::old_io::File::create(&path) {
            Err(why) => panic!("couldn't create {}: {}", display, why.desc),
            Ok(file) => file,
        };

        serialize_and_check_io_error!(serialize_with_client, &mut file, q, self, "Error serializing to file");
        */

        let mut socket = try_io!(self.socket.try_clone(), "Cannot clone tcp handle");
        try_rc!(q.serialize(&mut socket, self.version), "Error serializing BATCH request");
        let res = try_rc!(socket.read_cql_response(self.version), "Error reading query");
        Ok(res)
    }


    pub fn prepared_statement(&mut self, query_str: &str) -> RCResult<CqlPreparedStat> {
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodePrepare,
            body: RequestPrepare(query_str),
        };

        let mut socket = try_io!(self.socket.try_clone(), "Cannot clone tcp handle");
        try_rc!(q.serialize(&mut socket, self.version), "Error serializing prepared statement");

        let res = try_rc!(socket.read_cql_response(self.version), "Error reading query");
        match res.body {
            ResultPrepared(preps) => {
                Ok(preps)
            },
            _ => Err(RCError::new("Response does not contain prepared statement", ReadError))
        }
    }
}


fn send_startup(socket: &mut std::net::TcpStream, version: u8, creds: Option<&Vec<CowStr>>) -> RCResult<()> {
    let body = CqlStringMap {
        pairs:vec![CqlPair{key: "CQL_VERSION", value: CQL_VERSION_STRINGS[(version-1) as usize]}],
    };
    let msg_startup = CqlRequest {
        version: version,
        flags: 0x00,
        stream: 0x01,
        opcode: OpcodeStartup,
        body: RequestStartup(body),
    };

    try_rc!(msg_startup.serialize(socket, version), "Error serializing startup message");

    let response = try_rc!(socket.read_cql_response(version), "Error reding response");
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
                    try_rc!(msg_auth.serialize(socket, version), "Error serializing request (auth)");
                    
                    let response = try_rc!(socket.read_cql_response(version), "Error reding authenticaton response");
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

pub fn connect(ip: &'static str, port: u16, creds:Option<&Vec<CowStr>>) -> RCResult<Client> {

    let mut version = CQL_MAX_SUPPORTED_VERSION;

    while version >= 0x01 {
        let res = std::net::TcpStream::connect((ip, port));
        if res.is_err() {
            return Err(RCError::new(format!("Failed to connect to server at {}:{}", ip, port), ConnectionError));
        }
        
        let mut socket = res.unwrap();

        match send_startup(& mut socket, version, creds) {
            Ok(_) => return Ok(Client::new(socket, version)),
            Err(e) => println!("Error connecting with protocol version v{}: {}", version, e.desc)
        }
        version -= 1;
    }
    Err(RCError::new("Unable to find suitable protocol version (v1, v2, v3)", ReadError))
}
