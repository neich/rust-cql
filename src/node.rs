extern crate mio;
extern crate bytes;
extern crate eventual;

use self::eventual::{Future, Async, Complete};
use self::mio::{EventLoop,Sender};
use std::net::SocketAddr;
use std::error::Error;
use std::thread;
use std::sync::mpsc::channel;
use std::rc::{Rc,Weak};

use def::*;
use def::OpcodeRequest::*;
use def::CqlRequestBody::*;
use def::RCErrorType::*;
use def::CqlResponseBody::*;
use def::CqlValue::*;
use connection::CqlMsg;
use connection_pool::ConnectionPool;

pub struct Node {
    channel_pool: Weak<ChannelPool>, //Set of channels
    pub version: u8,
    address: SocketAddr
}

impl Node{
    
    pub fn new(address: SocketAddr) -> Node {
        Node{
            channel_pool: Rc::downgrade(&Rc::new(ChannelPool::new())),
            version: CQL_MAX_SUPPORTED_VERSION,
            address: address
        }
    }
    
    pub fn set_channel_pool(&mut self,channel_pool: Weak<ChannelPool>){
        /*
        match self.channel_pool {
            Some(c) =>  panic!("Channel pool can only be set once"),
            None    =>  self.channel_pool = Some(channel_pool),
        }
        */
        self.channel_pool = channel_pool;
    }

    pub fn get_channel_pool(&self)-> Rc<ChannelPool>{
        match self.channel_pool.upgrade() {
            Some(ch) => ch ,
            None     => panic!("Channel pool is None"),
        }
    }
    //pub fn start(&mut self){
    //    self.run_event_loop();
    //}
    pub fn get_latency(&self) ->usize{
        1
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

    pub fn exec_prepared(&mut self, preps: &Vec<u8>, params: &Vec<CqlValue>, con: Consistency) -> CassFuture{
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeExecute,
            body: RequestExec(preps.clone(), params.clone(), con, 0x01),
        };
        self.send_message(q)
    }
    
    pub fn exec_batch(&mut self, q_type: BatchType, q_vec: Vec<Query>, con: Consistency) -> CassFuture {
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

        serialize_and_check_io_error!(serialize_with_Node, &mut file, q, self, "Error serializing to file");
        */
        self.send_message(q)
    }


    pub fn prepared_statement(&mut self, query_str: &str) -> RCResult<CqlPreparedStat> {
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
        match self.get_channel_pool().find_available_channel(){
            Ok(channel) => {
                channel.send(CqlMsg::Request{
                                request: request,
                                tx: tx,
                                address: self.address});
            },
            Err(e) => {
                tx.complete(Err(RCError::new("Sending error", IOError)));
            },
        }
        future
    }

    pub fn send_register(&self,params: Vec<CqlValue>) -> CassFuture{
        println!("Node::send_register");
                let params = vec![ CqlVarchar( Some(CqlEventType::EventStatusChange.get_str())),
                        CqlVarchar( Some(CqlEventType::EventTopologyChange.get_str() )),
                        CqlVarchar( Some(CqlEventType::EventTopologyChange.get_str() ))
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
        match self.get_channel_pool().find_available_channel(){
            Ok(channel) => {
                channel.send(CqlMsg::Connect{
                                request: msg_startup,
                                tx: tx,
                                address: self.address});
            },
            Err(e) => {
                tx.complete(Err(RCError::new("Sending error", IOError)));
            },
        }
        future
    }
}
   

// The idea is to have a set of event loop channels to send 
// the CqlRequests. This will be changed when we decide how 
// to manage our event loops (connections) but for now it is
// only use one event loop and so one channel
pub struct ChannelPool {
    channels: Vec<Sender<CqlMsg>>
}

impl ChannelPool {
    pub fn new() -> ChannelPool {
        ChannelPool {
            channels: Vec::new()
        }
    }
    pub fn add_channel(&mut self, channel: Sender<CqlMsg>){
        self.channels.push(channel);
    }
    // For now only use one channel (and one event loop)
    fn find_available_channel(&self) -> Result<&Sender<CqlMsg>,&'static str>{
        if self.channels.len() > 0 {
            return Ok(self.channels.last().unwrap());
        }
        Err("There is no channel created")
    }
}




