extern crate mio;
extern crate bytes;
extern crate eventual;

use self::eventual::{Future, Async, Complete};
use self::mio::{Token, EventLoop, Sender, TryRead, TryWrite, EventSet};
use self::mio::tcp::TcpStream;
use self::mio::util::Slab;
use self::bytes::{ByteBuf, MutByteBuf};
use std::{mem, str};
use std::net::{SocketAddr,IpAddr,Ipv4Addr};
use std::error::Error;
use connection_pool::ConnectionPool;
use std::collections::{VecDeque,BTreeMap};

use def::*;
use def::OpcodeRequest::*;
use def::CqlRequestBody::*;
use def::RCErrorType::*;
use def::CqlResponseBody::*;
use serialize::CqlSerializable;
use reader::*;


pub struct Connection {
    // The connection's TCP socket 
    socket: TcpStream,
    // The token used to register this connection with the EventLoop
    token: mio::Token,
    // The response from reading a socket
    response: CassResponse,
    // Pending messages to be send (CQL requests)
    pendings_send: VecDeque<CqlMsg>,
    // Pending messages to be complete (CQL requests)
    pendings_complete: BTreeMap<i16,CqlMsg>,
    // CQL version v1, v2 or v3
    version: u8,
    // Channel to EventHandler
    event_handler: Sender<CqlEvent>,
    // Stream id  of the next CQL Request to send
    stream_id: i16
}


impl Connection {

    pub fn new(socket:TcpStream,version: u8,event_handler: Sender<CqlEvent>) -> Connection{
        let max_request = 
            match version{
                1 | 2 => 128,
                3 => 32768,
                _ => -1
            };

        Connection {
            socket: socket,
            token: Token(1),
            response: CassResponse::new(),
            pendings_send: VecDeque::new(),
            pendings_complete: BTreeMap::new(),
            version: version,
            event_handler: event_handler,
            stream_id: 0
        }
    }

    pub fn reset_response(&mut self){
        self.response = CassResponse::new();
    }

    pub fn set_token(&mut self, token: Token){
        self.token = token;
    }

    pub fn insert_request(&mut self,msg: CqlMsg) -> RCResult<()>{
        let mut cql_msg = msg;
        self.stream_id = try_unwrap!(self.next_stream_id());
        println!("Stream id provided = {:?} for Token = {:?}",self.stream_id,self.token);
        try_unwrap!(cql_msg.set_stream(self.stream_id));
                
        self.pendings_send.push_front(cql_msg);
        Ok(())
    }

    pub fn are_pendings_send(&self) -> bool{
        !self.pendings_send.is_empty()
    }

    pub fn are_pendings_complete(&self) -> bool{
        !self.pendings_complete.is_empty()
    }

    fn total_requests(&self) -> usize{
        self.pendings_complete.len()+self.pendings_send.len() 
    }

    fn max_requests_reached(&self) -> bool{
        max_stream_id(self.stream_id,self.version) && (self.total_requests() as i16) >= self.stream_id
    }

    fn next_stream_id(&self) -> RCResult<i16>{
        let mut stream_id = 1;
        if self.are_pendings_send() || self.are_pendings_complete(){
            if !max_stream_id(self.stream_id,self.version) {
                stream_id = self.stream_id+1;
            }
            else{      
                if !self.max_requests_reached()
                {
                    // Look up for the lowest stream_id that is not in pendings_send nor in pendings_complete 
                    // (yes, it's a bit slow)
                    let mut lowest = CQL_MAX_STREAM_ID_V3;
                    let mut previous_id = -99999;

                    for pendings in self.pendings_send.iter(){
                        let next_id = pendings.get_stream().unwrap();
                        if  next_id - previous_id > 1{
                            lowest = next_id;
                        }
                        previous_id = next_id;
                    }

                    for pendings in self.pendings_send.iter(){
                        let next_id = pendings.get_stream().unwrap();
                        if  previous_id - next_id > 1{
                            lowest = next_id;
                        }
                        previous_id = next_id;
                    }
                    stream_id = lowest;
                }
                else{
                    // Max requests reached
                    return Err(RCError::new(format!("Maximum request reached for current CQL v{:?}",self.version)
                            , RCErrorType::EventLoopError));
                }
            }
        }
        Ok(stream_id)
    }

    fn decrease_stream(&mut self,stream: i16){
        if stream == self.stream_id{
            self.stream_id=self.stream_id-1;
        }
    }

    pub fn read(&mut self, event_loop: &mut EventLoop<ConnectionPool>) {
        let mut buf = ByteBuf::mut_with_capacity(2048);

        match self.socket.try_read_buf(&mut buf) {
            Ok(Some(0)) => {
                println!("read 0 bytes");
            }
            Ok(Some(n)) => {
                self.response.mut_read_buf().extend_from_slice(&buf.bytes());
                println!("read {} bytes", n);
                //println!("Read: {:?}",buf.bytes());
                self.read(event_loop);  //Recursion here, care

            }
            Ok(None) => {
                println!("Reading buf = None");
                if !self.are_pendings_send(){
                    self.reregister(event_loop,EventSet::readable());
                }
                else{
                    self.reregister(event_loop,EventSet::writable());
                }
            }
            Err(e) => {
                panic!("got an error trying to read; err={:?}", e);
            }
        }
    }

    pub fn write(&mut self, event_loop: &mut EventLoop<ConnectionPool>) {
        let mut buf = ByteBuf::mut_with_capacity(2048);
        println!("self.pendings.len = {:?}",self.pendings_send.len());
        match self.pendings_send
                  .pop_back()
                  .unwrap()
             {
             CqlMsg::Request{request,tx,address} => {
                println!("Sending a request.");
                request.serialize(&mut buf,self.version);
                self.pendings_complete.insert(request.stream,CqlMsg::Request{request:request,tx:tx,address:address});
             },
             CqlMsg::Connect{request,tx,address} =>{
                println!("Sending a connect request.");
                request.serialize(&mut buf,self.version);
                self.pendings_complete.insert(request.stream,CqlMsg::Connect{request:request,tx:tx,address:address});
             },
             CqlMsg::Shutdown => {
                panic!("Shutdown messages shouldn't be at pendings");
             },
        }
        match self.socket.try_write_buf(&mut buf.flip()) 
            {
            Ok(Some(n)) => {
                println!("Written {} bytes",n);
                self.reregister(event_loop,EventSet::readable());
                if !self.are_pendings_send(){
                    self.reregister(event_loop,EventSet::readable());
                }
                else{
                    self.reregister(event_loop,EventSet::writable());
                }
            }
            Ok(None) => {
                // The socket wasn't actually ready, re-register the socket
                // with the event loop
                self.reregister(event_loop,EventSet::writable());
            }
            Err(e) => {
                panic!("got an error trying to read; err={:?}", e);
            }
        }

        println!("Ended write"); 
    }

    pub fn reregister(&self, event_loop: &mut EventLoop<ConnectionPool>,events : EventSet) {
        // Maps the current client state to the mio `EventSet` that will provide us
        // with the notifications that we want. When we are currently reading from
        // the client, we want `readable` socket notifications. When we are writing
        // to the client, we want `writable` notifications.
        //println!("Connection::reregister for: {:?}",events);
        //println!("Registering socket ip: {:?} ",self.socket.peer_addr().ok().expect("Couldn't unwrap ip").ip());
        event_loop.reregister(&self.socket, self.token, events,  mio::PollOpt::oneshot())
                  .ok().expect("Couldn't reregister connection");
    }
    
    pub fn register(&self, event_loop: &mut EventLoop<ConnectionPool>,events : EventSet) {

        println!("Connection::register");
        //println!("Registering socket ip: {:?} ",self.socket.peer_addr().ok().expect("Couldn't unwrap ip").ip());
        event_loop.register(&self.socket, 
                            self.token, 
                            events,  
                            mio::PollOpt::edge() | mio::PollOpt::oneshot()).unwrap();
    }

    pub fn queue_message(&mut self,event_loop: &mut EventLoop<ConnectionPool>,request: CqlMsg){
        self.pendings_send.push_back(request);    //Inserted in the last position to give it more priority
        //self.reregister(event_loop,EventSet::writable());
    }


    fn approve_authenticator(&self, authenticator: &CowStr) -> bool {
        authenticator == "org.apache.cassandra.auth.PasswordAuthenticator"
    }

    ///
    /// Makes an authentication response token that is compatible with PasswordAuthenticator.
    ///
    fn make_token(&self, creds: &Vec<CowStr>) -> Vec<u8> {
        let mut token : Vec<u8> = Vec::new();
        for cred in creds {
            token.push(0);
            token.extend(cred.as_bytes());
        }
        return token;
    }

    pub fn continue_startup_request(&mut self,response: CqlResponse ,event_loop: &mut EventLoop<ConnectionPool>) -> RCResult<()> {
        
        match response.body {
            ResponseReady =>  Ok(()),
            /*
            ResponseAuthenticate(authenticator) => {
                if self.approve_authenticator(&authenticator) {
                    match creds {
                        Some(ref cred) => {
                            if self.version >= 2 {
                                let msg_auth = CqlRequest {
                                    version: self.version,
                                    flags: 0x00,
                                    stream: 0x01,
                                    opcode: OpcodeAuthResponse,
                                    body: RequestAuthResponse(self.make_token(cred)),
                                };
                                self.queue_message(event_loop,msg_auth);

                            } else {
                                Err(RCError::new("Authentication is not supported for v1 protocol", ReadError)) 
                            }
                        },
                        None => Err(RCError::new("Credential should be provided for authentication", ReadError))
                    }
                } else {
                    Err(RCError::new(format!("Unexpected authenticator: {}", authenticator), ReadError))
                }

            },
            */
            ResponseAuthSuccess(_) => Ok(()),
            // ResponseError(_, ref msg) => Err(RCError::new(format!("Error in authentication: {}", msg), ReadError)),
            ResponseError(_, ref msg) => Err(RCError::new(format!("Error connecting: {}", msg), ReadError)),
            _ => Err(RCError::new("Wrong response to startup", ReadError))
        }
    }

    pub fn send_startup(&mut self, creds: Option<Vec<CowStr>>,event_loop: &mut EventLoop<ConnectionPool>) -> RCResult<()>{
        println!("Connection::send_startup");
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
        let (tx, future) = Future::<RCResult<CqlResponse>, ()>::pair();

        let msg_connect = CqlMsg::Connect{
            request: msg_startup,
            tx: tx,
            // This macro can return an error
            address: try_unwrap!(self.socket.peer_addr())
        };
        let mut future = self.queue_message(event_loop,msg_connect);
        Ok(())
    }

    pub fn read_cql_response(&self) -> (RCResult<CqlResponse>,bool){
        self.response.read_cql_response(self.version)
    }

    pub fn handle_response(&mut self,response: RCResult<CqlResponse>, event_loop: &mut EventLoop<ConnectionPool>, is_event : bool ){
        if is_event {
            println!("It seems we've got an event!");
            //Do event stuff
            match response {
                Ok(event) => {
                    match event.body {
                        ResponseEvent(cql_event) =>{
                            self.event_handler.send(cql_event);
                        },
                        _ =>{
                            println!("Oops! The event wasn't an event at all..");
                        }
                    }
                },
                RCError => (),
            }
        }
        else{
            let cql_response = response.unwrap();
            let stream = cql_response.stream;
            // Completes the future with a CqlResponse
            // which is a RCResult<CqlResponse>
            // so we can handle errors properly
            if self.are_pendings_complete(){
                match self.pendings_complete
                          .remove(&stream)
                          .unwrap() 
                {
                    CqlMsg::Request{request,tx,address} => {
                        tx.complete(Ok(cql_response));
                        self.decrease_stream(stream);
                        self.reset_response();
                    },
                    CqlMsg::Connect{request,tx,address} => {
                        //let result = self.continue_startup_request(response.clone().unwrap(),event_loop);
                        tx.complete(Ok(cql_response));
                        self.decrease_stream(stream);
                        self.reset_response();
                    },
                    CqlMsg::Shutdown => {
                        panic!("Shutdown messages shouldn't be at pendings");
                    },
                }
            }
        }
    }
}

pub fn connect(address: SocketAddr, creds:Option<Vec<CowStr>>,event_loop: &mut EventLoop<ConnectionPool>,event_handler: Sender<CqlEvent>) -> RCResult<Connection> {

    let mut version = CQL_MAX_SUPPORTED_VERSION;
    println!("Connection::connect");

    let res = TcpStream::connect(&address);
    if res.is_err() {
        return Err(RCError::new(format!("Failed to connect to server at {}", address), ConnectionError));
    }
    let mut socket = res.ok().expect("Failed to unwrap the socket");
    let mut conn = Connection::new(socket,version,event_handler);
    // Once a connection is created we have to register it,
    // later on we can 'reregister' if necessary
    conn.register(event_loop,EventSet::writable());
    let result = conn.send_startup(creds.clone(),event_loop);
    match result{
        Ok(_) => Ok(conn),
        Err(err) => Err(err)
    }
    
}

struct CassResponse {
    data : Vec<u8>
}

impl CassResponse {

    fn new() -> CassResponse {
        CassResponse {
            data: Vec::new()
        }
    }

    
    fn read_buf(&self) -> &Vec<u8> {
        &self.data
    }

    fn mut_read_buf(&mut self) -> &mut Vec<u8>{
        &mut self.data
    }

    pub fn read_cql_response(&self,version: u8) -> (RCResult<CqlResponse>,bool){
        println!("Connection::CassResponse::read_cql_response");
        //println!("CqlResponse := {:?}",self.read_buf());
        println!("Length slice vec: {:?}",self.read_buf().len());
        //let mut response : ByteBuf = ByteBuf::from_slice(self.read_buf().as_slice());
        //println!("Capacity: {:?}",response.capacity());
        let rc_result = self.read_buf().as_slice().read_cql_response(version);
        let cql_response =  match rc_result {
            Ok(val) => val,
            Err(ref err) => {
                use std::error::Error;
                println!("We've got an error reading response: {:?}",err);
                return (Err(RCError::new(format!("{} -> {}", "", err.description()), RCErrorType::IOError)),false)
            }
        };
        println!("CqlResponse := {:?}",cql_response);
        let is_event = cql_response.is_event();
        (Ok(cql_response),is_event)
    }

}

pub enum CqlMsg{
    Request{
        request: CqlRequest,
        tx: Complete<RCResult<CqlResponse>,()>,
        address: SocketAddr
    },
    Connect{
        request: CqlRequest,
        tx: Complete<RCResult<CqlResponse>,()>,
        address: SocketAddr
    },
    Shutdown
}

impl CqlMsg{
    pub fn get_ip(&self) -> IpAddr
    {
        match self{
            &CqlMsg::Request{ref request,ref tx,ref address} => {
                address.ip().clone()
            }
            &CqlMsg::Connect{ref request,ref tx,ref address} => {
                address.ip().clone()
            }
            _ =>{
                panic!("Invalid type for get_ip");
            }
        }
    }

    //Consume 'self' to complete the future,
    //self won't be usable after this
    pub fn complete(self,result: RCResult<CqlResponse>) 
    {
        match self {
            CqlMsg::Request{request,tx,address} => {
               tx.complete(result);
            }
            CqlMsg::Connect{request,tx,address} => {
               tx.complete(result);
            }
            _ =>{
                panic!("Invalid type for complete");
            }
        }
    }

    pub fn get_stream(&self,) -> RCResult<i16>
    {
        match *self{
            CqlMsg::Request{ref request,ref tx,ref address} => {
                Ok(request.stream)
            },
            CqlMsg::Connect{ref request,ref tx,ref address} =>{
                Ok(request.stream)
            },
            _ =>{
                panic!("Invalid type for get_stream");
            }
        }
    }

    pub fn set_stream(&mut self,stream: i16) -> RCResult<()>
    {
        match *self{
            CqlMsg::Request{ref mut request,ref tx,ref address} => {
                request.set_stream(stream);
            },
            CqlMsg::Connect{ref mut request,ref tx,ref address} =>{
                request.set_stream(stream);
            },
            _ =>{
                panic!("Invalid type for set_stream");
            }
        }
        Ok(())
    }
}