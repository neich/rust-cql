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
    pendings: Vec<CqlMsg>,
    // CQL version v1, v2 or v3
    version: u8,
    //Temporal bool
    pub register: bool 
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
}

impl Connection {

    pub fn new(socket:TcpStream,version: u8)-> Connection{
        Connection {
            socket: socket,
            token: Token(1),
            response: CassResponse::new(),
            pendings: Vec::new(),
            version: version,
            register: false
        }
    }

    pub fn reset_response(&mut self){
        self.response = CassResponse::new();
    }

    pub fn set_token(&mut self, token: Token){
        self.token = token;
    }

    pub fn insert_request(&mut self,msg: CqlMsg){
        self.pendings.insert(0,msg);
    }

    pub fn are_pendings(&self) -> bool{
        self.pendings.len() > 0
    }

    pub fn read(&mut self, event_loop: &mut EventLoop<ConnectionPool>) {
        match self.socket.try_read_buf(self.response.mut_read_buf()) {
            Ok(Some(0)) => {
                println!("read 0 bytes");
            }
            Ok(Some(n)) => {
                println!("read {} bytes", n);
                // self.read(event_loop);  // Recursion here, care
                if self.pendings.len() == 1{
                    self.reregister(event_loop,EventSet::readable());
                }
                else{
                    self.reregister(event_loop,EventSet::writable());
                }
            }
            Ok(None) => {
                println!("Reading buf = None");
                self.reregister(event_loop,EventSet::readable());
            }
            Err(e) => {
                panic!("got an error trying to read; err={:?}", e);
            }
        }
    }

    pub fn write(&mut self, event_loop: &mut EventLoop<ConnectionPool>) {
        let mut buf = ByteBuf::mut_with_capacity(2048);
        println!("self.pendings.len = {:?}",self.pendings.len());
        match self.pendings
                  .pop()
                  .unwrap()
             {
             CqlMsg::Request{request,tx,address} => {
                request.serialize(&mut buf,self.version);
                self.pendings.push(CqlMsg::Request{request:request,tx:tx,address:address});
             },
             CqlMsg::Connect{request,tx,address} =>{
                request.serialize(&mut buf,self.version);
                self.pendings.push(CqlMsg::Connect{request:request,tx:tx,address:address});
                //Last??
             },
             CqlMsg::Shutdown => {
                panic!("Shutdown messages shouldn't be at pendings");
             },
        }
        //println!("Serialized: {:?}",buf);
        match self.socket.try_write_buf(&mut buf.flip()) 
            {
            Ok(Some(n)) => {
                println!("Written {} bytes",n);

                // Re-register the socket with the event loop.
                self.reregister(event_loop,EventSet::readable());

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
        println!("Connection::reregister for: {:?}",events);
        println!("Registering socket ip: {:?} ",self.socket.peer_addr().ok().expect("Couldn't unwrap ip").ip());
        event_loop.reregister(&self.socket, self.token, events,  mio::PollOpt::oneshot())
                  .ok().expect("Couldn't reregister connection");
        println!("Line 472");
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
        self.pendings.push(request);    //Inserted in the last position to give it more priority
        self.reregister(event_loop,EventSet::writable());
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

    pub fn send_startup(&mut self, creds: Option<Vec<CowStr>>,event_loop: &mut EventLoop<ConnectionPool>){
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
            address: self.socket.peer_addr().ok().expect("Couldn't unwrap SocketAddr")
        };
        let mut future = self.queue_message(event_loop,msg_connect);
    }

    pub fn read_cql_response(&self) -> (RCResult<CqlResponse>,bool){
        self.response.read_cql_response(self.version)
    }

    pub fn handle_response(&mut self,response: RCResult<CqlResponse>, event_loop: &mut EventLoop<ConnectionPool>, is_event : bool ){
        if is_event {
            println!("It seems we've got an event!")
            //Do event stuff
        }
        else{
            // Completes the future with a CqlResponse
            // which is a RCResult<CqlResponse>
            // so we can handle errors properly
            if self.are_pendings(){
                match self.pendings
                          .pop()
                          .take()
                          .unwrap() 
                {
                    CqlMsg::Request{request,tx,address} => {
                        tx.complete(response);
                        self.reset_response();
                    },
                    CqlMsg::Connect{request,tx,address} => {
                        //let result = self.continue_startup_request(response.clone().unwrap(),event_loop);
                        tx.complete(response);
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

pub fn connect(address: SocketAddr, creds:Option<Vec<CowStr>>,event_loop: &mut EventLoop<ConnectionPool>) -> RCResult<Connection> {

    let mut version = CQL_MAX_SUPPORTED_VERSION;
    println!("Connection::connect");

    let res = TcpStream::connect(&address);
    if res.is_err() {
        return Err(RCError::new(format!("Failed to connect to server at {}", address), ConnectionError));
    }
    let mut socket = res.ok().expect("Failed to unwrap the socket");
    let mut conn = Connection::new(socket,version);
    conn.register = true;   //test
    // Once a connection is created we have to register it
    // then we can later 'reregister' if necessary
    conn.register(event_loop,EventSet::writable());
    conn.send_startup(creds.clone(),event_loop);
    Ok(conn)
}

struct CassResponse {
    data : MutByteBuf
}

impl CassResponse {

    fn new() -> CassResponse {
        CassResponse {
            data: ByteBuf::mut_with_capacity(2048)
        }
    }


    fn read_buf(&self) -> &MutByteBuf {
        &self.data
    }

    fn mut_read_buf(&mut self) -> &mut MutByteBuf{
        &mut self.data
    }

    fn unwrap_read_buf(self) -> MutByteBuf {
        self.data
    }

    pub fn read_cql_response(&self,version: u8) -> (RCResult<CqlResponse>,bool){
        println!("Connection::CassResponse::read_cql_response");
        let bytes_buf = self.read_buf().bytes();   
        println!("CqlResponseBytes := {:?}",to_hex_string(&bytes_buf.to_vec()));
        // By now, just copy it to avoid borrowing troubles
        let mut response : ByteBuf = ByteBuf::from_slice(bytes_buf);
        let rc_result = response.read_cql_response(version);
        let cql_response =  match rc_result {
            Ok(val) => val,
            Err(ref err) => {
                use std::error::Error;
                println!("We've got an error there: {:?}",err);
                return (Err(RCError::new(format!("{} -> {}", "", err.description()), RCErrorType::IOError)),false)
            }
        };
        println!("CqlResponse := {:?}",cql_response);
        let is_event = cql_response.is_event();
        (Ok(cql_response),is_event)
    }

}