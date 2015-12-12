extern crate std;
extern crate num;
extern crate uuid;
extern crate eventual;


use std::collections::BTreeMap;
use std::borrow::Cow;
use std::error::Error;
use std::io::BufWriter;

use self::eventual::Future;
use std::net::TcpStream;

use super::def::*;
use super::def::OpcodeRequest::*;
use super::def::CqlRequestBody::*;
use super::def::RCErrorType::*;
use super::def::CqlResponseBody::*;
use super::serialize::CqlSerializable;

use super::reader::*;


pub static CQL_VERSION_STRINGS:  [&'static str; 3] = ["3.0.0", "3.0.0", "3.0.0"];
pub static CQL_MAX_SUPPORTED_VERSION:u8 = 0x03;

pub type PrepsStore = BTreeMap<String, Box<CqlPreparedStat>>;




pub struct Client {
    socket: TcpStream,
    pub version: u8,
    prepared: PrepsStore
}

impl Client {

    fn new(socket: TcpStream, version: u8) -> Client {
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
            body: RequestQuery(String::from(query_str), con, 0)};

        let mut socket = try_io!(self.socket.try_clone(), "Cannot clone tcp handle");
        try_rc!(q.serialize_with_client(&mut socket, self), "Error serializing query");
        Ok(try_rc!(socket.read_cql_response(self.version), "Error reading query"))
    }

    pub fn exec_prepared(&mut self, ps_id: &str, params: &Vec<CqlValue>, con: Consistency) -> RCResult<CqlResponse> {
        let mut p = Vec::new();
        p.clone_from(params);
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeExecute,
            body: RequestExec(String::from(ps_id), p, con, 0x01),
        };

        let mut socket = try_io!(self.socket.try_clone(), "Cannot clone tcp handle");
        try_rc!(q.serialize_with_client(&mut socket, self), "Error serializing prepared statement execution");

        /* Code to debug prepared statements. Write to file the serialization of the request

        let path = Path::new("prepared_data.bin");
        let display = path.display();
        let mut file = match std::old_io::File::create(&path) {
            Err(why) => fail!("couldn't create {}: {}", display, why.desc),
            Ok(file) => file,
        };

        serialize_and_check_io_error!(&mut file, q, self.version, "Error serializing query");
        
        */

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
        try_rc!(q.serialize_with_client(&mut socket, self), "Error serializing BATCH request");
        let res = try_rc!(socket.read_cql_response(self.version), "Error reading query");
        Ok(res)
    }


    pub fn prepared_statement(&mut self, query_str: &str, query_id: &str) -> RCResult<()> {
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodePrepare,
            body: RequestPrepare(query_str),
        };

        let mut socket = try_io!(self.socket.try_clone(), "Cannot clone tcp handle");
        try_rc!(q.serialize_with_client(&mut socket, self), "Error serializing prepared statement");

        let res = try_rc!(socket.read_cql_response(self.version), "Error reading query");
        match res.body {
            ResultPrepared(preps) => {
                self.prepared.insert(query_id.to_string(), preps);
                Ok(())
            },
            _ => Err(RCError::new("Response does not contain prepared statement", ReadError))
        }
    }


    pub fn async_exec_query(&mut self, query_str: &str, con: Consistency) -> Future<RCResult<CqlResponse>, ()> {
        match self.socket.try_clone() {
            Ok(mut socket) => {
                let q = CqlRequest {
                    version: self.version,
                    flags: 0x00,
                    stream: 0x01,
                    opcode: OpcodeQuery,
                    body: RequestQuery(String::from(query_str), con, 0)};

                eventual::Future::spawn(move || {
                    println!("Serializing query ...");
                    match q.serialize(&mut socket, q.version) {
                        Ok(_) => { println!("... Ok serializen query");},
                        Err(err) => println!("Error: {:?}", err.description())
                    }
                    println!("Reading response ...");
                    Ok(try_rc!(socket.read_cql_response(q.version), "Error reading query"))
                })
            },
            Err(ref err) => eventual::Future::of(Err(RCError::new(format!("{} -> {}", "Cannot clone socket", err.description()), RCErrorType::IOError)))
        }
    }

    pub fn async_exec_prepared(&mut self, ps_id: &str, params: &Vec<CqlValue>, con: Consistency) -> Future<RCResult<CqlResponse>, ()> {
        match self.socket.try_clone() {
            Ok(mut socket) => {
                let mut p = Vec::new();
                p.clone_from(params);

                let mut client =  from(&socket,self.version,&self.prepared);

                let q = CqlRequest {
                    version: self.version,
                    flags: 0x00,
                    stream: 0x01,
                    opcode: OpcodeExecute,
                    body: RequestExec(String::from(ps_id), p, con, 0x01)};

                Future::spawn(move || {
                    println!("Serializing prepared query ...");
                    match q.serialize_with_client(&mut socket,&mut client) {
                        Ok(_) => { println!("... Ok serialized prepared query");},
                        Err(err) => println!("Error: {:?}", err.description())
                    }
                    println!("Reading response from prepared ...");
                    Ok(try_rc!(socket.read_cql_response(q.version), "Error reading prepared query"))
                })
            },
            Err(ref err) => Future::of(Err(RCError::new(format!("{} -> {}", "Cannot clone socket", err.description()), RCErrorType::IOError)))
        }
     
    }

    pub fn async_exec_batch(&mut self, q_type: BatchType, q_vec: Vec<Query>, con: Consistency) -> Future<RCResult<CqlResponse>, ()>{
       

        // Code to debug batch statements. Write to file the serialization of the request
        //let path = Path::new("batch_data.bin");
        //let display = path.display();
        //let mut file = match std::old_io::File::create(&path) {
        //    Err(why) => panic!("couldn't create {}: {}", display, why.desc),
        //    Ok(file) => file,
        //};
        //serialize_and_check_io_error!(serialize_with_client, &mut file, q, self, "Error serializing to file");
        
        
         match self.socket.try_clone() {
            Ok(mut socket) => {
                //let mut p = Vec::new();
                //p.clone_from(params);
                let mut client =  from(&socket,self.version,&self.prepared);
                 let q = CqlRequest {
                    version: self.version,
                    flags: 0x00,
                    stream: 0x01,
                    opcode: OpcodeBatch,
                    body: RequestBatch(q_vec, q_type, con, 0)};

                eventual::Future::spawn(move || {
                    println!("Serializing batch query ...");
                    try_rc!(q.serialize_with_client(&mut socket, &mut client), "Error serializing BATCH request");
                    println!("Reading response from prepared ...");
                    Ok(try_rc!(socket.read_cql_response(q.version), "Error reading prepared query"))
                })
            },
            Err(ref err) => eventual::Future::of(Err(RCError::new(format!("{} -> {}", "Cannot clone socket", err.description()), RCErrorType::IOError)))
        }
    }
}

fn send_startup(socket: &mut TcpStream, version: u8, creds: Option<&Vec<CowStr>>) -> RCResult<()> {
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
    println!("At [method] Client::send_startup");

    try_rc!(msg_startup.serialize(socket, version), "Error serializing startup message");

    let response = try_rc!(socket.read_cql_response(version), "Error reding response");
     println!("End read_exact");
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


//Socket should be checked first
pub fn from(socket: &std::net::TcpStream, version: u8,prepared: & PrepsStore) -> Client {
    Client {socket: socket.try_clone().unwrap(), version: version, prepared: (*prepared).clone()}
}

pub fn connect(ip: &'static str, port: u16, creds:Option<&Vec<CowStr>>) -> RCResult<Client> {

    let mut version = CQL_MAX_SUPPORTED_VERSION;
    println!("At [method] Client::connect");
    while version >= 0x01 {
        let res = TcpStream::connect((ip, port));
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
