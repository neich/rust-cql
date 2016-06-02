extern crate mio;
extern crate bytes;
extern crate eventual;

use self::eventual::{Future, Async, Complete};
use self::mio::{EventLoop,Sender};
use std::net::SocketAddr;
use std::error::Error;
use std::thread;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::{Instant,Duration};

use def::*;
use def::OpcodeRequest::*;
use def::CqlRequestBody::*;
use def::RCErrorType::*;
use def::CqlResponseBody::*;
use def::CqlValue::*;
use connection::CqlMsg;
use connection_pool::ConnectionPool;

pub struct Node {
    channel_cpool: Sender<CqlMsg>, 
    pub version: u8,
    address: SocketAddr
}

impl Node{
    
    pub fn new(address: SocketAddr,channel_cpool: Sender<CqlMsg>) -> Node {

        Node{
            channel_cpool: channel_cpool,
            version: CQL_MAX_SUPPORTED_VERSION,
            address: address
        }
    }
    
    pub fn set_channel_cpool(&mut self,channel_cpool: Sender<CqlMsg>){
        /*
        match self.channel_pool {
            Some(c) =>  panic!("Channel pool can only be set once"),
            None    =>  self.channel_pool = Some(channel_pool),
        }
        */
        self.channel_cpool = channel_cpool;
    }

    /*
    pub fn get_channel_pool(&self)-> Rc<ChannelPool>{
        match self.channel_pool.upgrade() {
            Some(ch) => ch ,
            None     => panic!("Channel pool is None"),
        }
    }
    */
    //pub fn start(&mut self){
    //    self.run_event_loop();
    //}
    pub fn get_latency(&self) -> Duration{
        let start = Instant::now();
        self.exec_dummy_query().await();
        Instant::now() - start 
    }

    pub fn exec_query(& self, query_str: &str, con: Consistency) -> CassFuture {
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeQuery,
            body: RequestQuery(String::from(query_str), con, 0)};
        self.send_message(q)
    }
    
    pub fn get_peers(&self) -> CassFuture{
        //let query = "SELECT peer,data_center,host_id,rack,rpc_address,schema_version 
        //             FROM system.peers;";
        let query = "SELECT peer,data_center,host_id,rack,rpc_address
                     FROM system.peers;";
        self.exec_query(query,Consistency::One)
    }

    pub fn exec_prepared(&self, preps: &Vec<u8>, params: &Vec<CqlValue>, con: Consistency) -> CassFuture{
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeExecute,
            body: RequestExec(preps.clone(), params.clone(), con, 0x01),
        };
        self.send_message(q)
    }
    
    pub fn exec_batch(&self, q_type: BatchType, q_vec: Vec<Query>, con: Consistency) -> CassFuture {
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeBatch,
            body: RequestBatch(q_vec, q_type, con, 0)
        };

        /* Code to debug batch statements. Write to file the serialization of the request

        let path = Path::new("batch_data.bin");
        let display = path.display();
        let mut file = match std::old_io::File::create(&path) {
            Err(why) => panic!("couldn't create {}: {}", display, why.desc),
            Ok(file) => file,
        };

        serialize_and_check_io_error!(serialize_with_Node, &mut file, q, self, "Error serializing to file");
        */
        self.send_message(q)
    }


    pub fn prepared_statement(&self, query_str: &str) -> RCResult<CqlPreparedStat> {
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodePrepare,
            body: RequestPrepare(query_str.to_string()),
        };

        let future = self.send_message(q);
        let mut cql_response = future.await()
                                     .ok().expect("Couldn't recieve future")
                                     .ok().expect("Couldn't get CQL response");
        match cql_response.body {
            ResultPrepared(preps) => {
                Ok(preps)
            },
            _ => Err(RCError::new("Response does not contain prepared statement", ReadError))
        }
    }

    fn send_message(&self,request: CqlRequest) -> CassFuture{
        let (tx, future) = Future::<RCResult<CqlResponse>, ()>::pair();
        self.channel_cpool.send(CqlMsg::Request{
                                request: request,
                                tx: tx,
                                address: self.address});
        future
    }

    pub fn send_register(&self,params: Vec<CqlValue>) -> CassFuture{
        println!("Node::send_register");
                let params = vec![ CqlVarchar( Some(CqlEventType::StatusChange.get_str())),
                        CqlVarchar( Some(CqlEventType::TopologyChange.get_str() )),
                        CqlVarchar( Some(CqlEventType::TopologyChange.get_str() ))
                ];
        let msg_register = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,  
            opcode: OpcodeRegister,
            body: RequestRegister(params)
        };
        self.send_message(msg_register)
    }

    pub fn connect(&self) -> CassFuture{
        let (tx, future) = Future::<RCResult<CqlResponse>, ()>::pair();
        let body = CqlStringMap {
            pairs:vec![CqlPair{key: "CQL_VERSION", value: CQL_VERSION_STRINGS[(self.version-1) as usize]}],
        };
        let msg_startup = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeStartup,
            body: RequestStartup(body),
        };
        self.channel_cpool.send(CqlMsg::Connect{
                                request: msg_startup,
                                tx: tx,
                                address: self.address});
        future
    }

    pub fn get_sock_addr(&self) -> SocketAddr{
        self.address
    }

    fn exec_dummy_query(& self) -> CassFuture {
        let query_str = "SELECT now() FROM system.local;";
        let con = Consistency::One;
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeQuery,
            body: RequestQuery(String::from(query_str), con, 0)};
        self.send_message(q)
    }
}




