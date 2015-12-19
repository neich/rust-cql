extern crate mio;
extern crate bytes;
extern crate eventual;

use self::eventual::{Future, Async, Complete};
use self::mio::{Token, EventLoop, Sender, TryRead, TryWrite};
use self::mio::tcp::TcpStream;
use self::mio::util::Slab;
use self::bytes::{ByteBuf, MutByteBuf};
use std::{mem, str};
use std::io::Cursor;
use std::net::SocketAddr;
use std::collections::BTreeMap;
use std::borrow::Cow;
use std::error::Error;
use std::thread;
use std::sync::mpsc::channel;

use super::def::*;
use super::def::OpcodeRequest::*;
use super::def::CqlRequestBody::*;
use super::def::RCErrorType::*;
use super::def::CqlResponseBody::*;
use super::serialize::CqlSerializable;
use super::reader::*;


pub static CQL_VERSION_STRINGS:  [&'static str; 3] = ["3.0.0", "3.0.0", "3.0.0"];
pub static CQL_MAX_SUPPORTED_VERSION:u8 = 0x03;

pub static TOKEN_1 : Token = Token(1);

pub struct Client {
    pool: Pool, //Conjunt de channels
    pub version: u8
}

impl Client{
    
    fn new(version:u8) -> Client {
        Client{
            pool: Pool::new(),
            version: version
        }
    }
        
    pub fn async_exec_query(&mut self, query_str: &str, con: Consistency) -> Future<RCResult<CqlResponse>,()> {
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeQuery,
            body: RequestQuery(String::from(query_str), con, 0)};
        self.send_message(q,TOKEN_1)
    }
    
    pub fn async_exec_prepared(&mut self, preps: &Vec<u8>, params: &Vec<CqlValue>, con: Consistency) -> Future<RCResult<CqlResponse>,()>{
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeExecute,
            body: RequestExec(preps.clone(), params.clone(), con, 0x01),
        };
        self.send_message(q,TOKEN_1)
    }
    
    pub fn async_exec_batch(&mut self, q_type: BatchType, q_vec: Vec<Query>, con: Consistency) -> Future<RCResult<CqlResponse>,()> {
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
        self.send_message(q,TOKEN_1)
    }


    pub fn prepared_statement(&mut self, query_str: &str) -> RCResult<CqlPreparedStat> {
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodePrepare,
            body: RequestPrepare(query_str.to_string()),
        };

        let future = self.send_message(q,TOKEN_1);
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

    fn send_startup(&mut self, creds: Option<Vec<CowStr>>,token: Token) -> RCResult<()> {
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

        let mut future = self.send_message(msg_startup, token);
        let mut cql_response = future.await()
                                     .ok().expect("Couldn't recieve future")
                                     .ok().expect("Couldn't get CQL response");
        
        match cql_response.body {
            ResponseReady =>  Ok(()),
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
                                let response = self.send_message(msg_auth, token).await().ok().expect("Couldn't recieve future").ok().expect("Couldn't get CQL response");
                                match response.body {
                                    ResponseAuthSuccess(_) => Ok(()),
                                    ResponseError(_, ref msg) => Err(RCError::new(format!("Error in authentication: {}", msg), ReadError)),
                                    _ => Err(RCError::new("Server returned unknown message", ReadError))
                                }
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
            ResponseError(_, ref msg) => Err(RCError::new(format!("Error connecting: {}", msg), ReadError)),
            _ => Err(RCError::new("Wrong response to startup", ReadError))
        }
    }

    fn send_message(&mut self,request: CqlRequest,token:Token) -> Future<RCResult<CqlResponse>, ()>{
        let (tx, future) = Future::<RCResult<CqlResponse>, ()>::pair();
        self.pool.find_channel_by_token(token)
                 .send(CqlMsg { 
                        request: request, 
                        tx: tx});
        future
    }

    fn run_event_loop_with_connection(&mut self ,socket: TcpStream,token:Token){
        let mut event_loop : EventLoop<Connection> = mio::EventLoop::new().ok().expect("Couldn't create event loop");
        println!("Adding connection!!");
        self.pool.add_channel_with_token(event_loop.channel(),token);
        println!("It's seems we could add a connection ");
        event_loop.register(
                &socket,
                token,
                mio::EventSet::writable(),
                mio::PollOpt::edge() | mio::PollOpt::oneshot()).unwrap();
        //We will need the event loop to register a new socket
        //But on creating the thread we borrow the even_loop
        let mut connection =  Connection {
                socket: socket,
                token: token,
                state: State::Waiting,
                pendings: Vec::new(),
                version: self.version
            };

        println!("Even loop starting...");
        //We only keep event loop channel
        thread::spawn(move||{
                event_loop.run(&mut connection).ok().expect("Failed to start event loop");
            });
    }
}


pub fn connect(address: SocketAddr, creds:Option<Vec<CowStr>>) -> RCResult<Client> {

        let mut version = CQL_MAX_SUPPORTED_VERSION;
        println!("At [method] Client::connect");

        while version >= 0x01 {
            let res = TcpStream::connect(&address);
            if res.is_err() {
                return Err(RCError::new(format!("Failed to connect to server at {}", address), ConnectionError));
            }
            let token = Token(1);
            let mut socket = res.unwrap();
            let mut client = Client::new(version);
            //There is no shutdown yet
            client.run_event_loop_with_connection(socket,token);

            match client.send_startup(creds.clone(),token) {
                Ok(_) => return Ok(client),
                Err(e) => println!("Error connecting with protocol version v{}: {}", version, e.desc)
            }
            version -= 1;
        }
        Err(RCError::new("Unable to find suitable protocol version (v1, v2, v3)", ReadError))
    }
    

pub struct CqlMsg {
    request: CqlRequest,
    tx: Complete<RCResult<CqlResponse>,()>
}

// The idea is to have a set of event loop channels
// to send the CqlRequests. 
pub struct Pool {
    channels: Slab<Sender<CqlMsg>>
}

impl Pool {
    fn new() -> Pool {
        Pool {
            // Allocate a slab that is able to hold exactly the right number of
            // connections.
            channels: Slab::new_starting_at(Token(1),128)
        }
    }
    // Find a channel in the slab using the given token.
    fn find_channel_by_token<'a>(&'a mut self, token: Token) -> &'a mut Sender<CqlMsg> {
        &mut self.channels[token]
    }
    fn add_channel_with_token(& mut self,channel: Sender<CqlMsg>,token: Token){
        self.channels.insert_with(|token| {channel});
    }
}

impl mio::Handler for Connection {
    type Timeout = ();
    type Message = CqlMsg;

    // Push pending messages to be send
    // across the event loop. Only change state in case it is waiting
    fn notify(&mut self, event_loop: &mut EventLoop<Connection>, msg: CqlMsg) {
        println!("[Connection::notify]");
        match self.state {
            State::Waiting(..) => {
                self.set_state(event_loop,State::Writing);
                // Ineficient, consider using a LinkedList
                self.pendings.insert(0,msg); 
            }
            _ => {
                self.pendings.insert(0,msg);
            }
        }
    }

    
    fn ready(&mut self, event_loop: &mut mio::EventLoop<Connection>, token: mio::Token, events: mio::EventSet) {
        println!("[Connection::ready]");      
        println!("Assigned token is: {:?}",self.token);

        match self.state {
            State::Reading(..) => {
                println!("    connection-state=Reading");
                assert!(events.is_readable(), "unexpected events; events={:?}", events);
                self.read(event_loop);

                let mut response = self.state.read_cql_response(self.version);
                // Completes the future with a CqlResponse
                // CqlMsg contains a RCResult<CqlResponse>
                // so we can handle errors
                self.pendings.pop()
                             .take()
                             .unwrap()
                             .tx.complete(response); 
                
            }
            State::Writing(..) => {
                println!("    connection-state=Writing");
                assert!(events.is_writable(), "unexpected events; events={:?}", events);
                self.write(event_loop,token)
            }
            State::Closed(..) => {
                println!("    connection-state=Closed");
                event_loop.shutdown();
            }
            _ => (),
        }

        if self.pendings.len() == 0 {
            self.state.transition_to_waiting();
        }
        println!("[Connection::Ended ready]");
    }
}


struct Connection {
    // The connection's TCP socket 
    socket: TcpStream,
    // The token used to register this connection with the EventLoop
    token: mio::Token,
    // The current state of the connection (reading, writing or waiting)
    state: State,
    // Pending messages to be send (CQL requests)
    pendings: Vec<CqlMsg>,
    // CQL version v1, v2 or v3
    version: u8
}

impl Connection {

    fn read(&mut self, event_loop: &mut mio::EventLoop<Connection>) {
        match self.socket.try_read_buf(self.state.mut_read_buf()) {
            Ok(Some(0)) => {
                 println!("    connection-state=Closed");
                self.state = State::Closed;
            }
            Ok(Some(n)) => {
                println!("read {} bytes", n);

                //self.state.try_transition_to_writing(&mut self.remaining);

                // Re-register the socket with the event loop. The current
                // state is used to determine whether we are currently reading
                // or writing.
                self.reregister(event_loop);
            }
            Ok(None) => {
                println!("Reading buf = None");
                self.reregister(event_loop);
            }
            Err(e) => {
                panic!("got an error trying to read; err={:?}", e);
            }
        }
    }

    fn write(&mut self, event_loop: &mut mio::EventLoop<Connection>,token: Token) {
        let mut buf = ByteBuf::mut_with_capacity(2048);
        
        self.pendings.last_mut()
                     .unwrap()
                     .request.serialize(&mut buf,self.version);
        match self.socket.try_write_buf(&mut buf.flip()) 
            {
            Ok(Some(n)) => {
                println!("Written {} bytes",n);
                // If the entire buffer has been written, transition to the
                // reading state.
                self.state.try_transition_to_reading();

                // Re-register the socket with the event loop.
                self.reregister(event_loop);

            }
            Ok(None) => {
                // The socket wasn't actually ready, re-register the socket
                // with the event loop
                self.reregister(event_loop);
            }
            Err(e) => {
                panic!("got an error trying to read; err={:?}", e);
            }
        }
        println!("Ended write"); 
    }

    fn reregister(&self, event_loop: &mut mio::EventLoop<Connection>) {
        // Maps the current client state to the mio `EventSet` that will provide us
        // with the notifications that we want. When we are currently reading from
        // the client, we want `readable` socket notifications. When we are writing
        // to the client, we want `writable` notifications.
        let event_set = match self.state {
            State::Reading(..) => mio::EventSet::readable(),
            State::Writing(..) => mio::EventSet::writable(),
            _ => return,
        };
        event_loop.reregister(&self.socket, self.token, event_set, mio::PollOpt::oneshot())
            .unwrap();
    }

    #[inline]
    fn set_state(&mut self,event_loop: &mut mio::EventLoop<Connection>,state: State){
        self.state = state;
        self.reregister(event_loop);
    }
    
    #[inline]
    fn is_closed(&self) -> bool {
        match self.state {
            State::Closed => true,
            _ => false,
        }
    }
}

enum State {
    Reading(MutByteBuf),
    Writing,
    Waiting,
    Closed
}

impl State {
    
    fn try_transition_to_reading(&mut self) {
        //if !self.write_buf().has_remaining() {
        self.transition_to_reading();
        //}
    }
    
    //
    fn transition_to_reading(&mut self) {
        //println!("[State::transition_to_reading");
        //let mut buf = mem::replace(self, State::Closed)
        //    .unwrap_write_buf();

        //let mut mut_buf = buf.flip();
        //mut_buf.clear();
        //println!("[State::transition_to_reading] Ending..");
        *self = State::Reading(ByteBuf::mut_with_capacity(2048));
    }

    fn transition_to_waiting(&mut self){
        *self = State::Waiting;
    }

    fn read_buf(&self) -> &MutByteBuf {
        match *self {
            State::Reading(ref buf) => buf,
            _ => panic!("connection not in reading state"),
        }
    }

    fn mut_read_buf(&mut self) -> &mut MutByteBuf{
        match *self {
            State::Reading(ref mut buf) => buf,
            _ => panic!("connection not in reading state"),
        }
    }

    fn unwrap_read_buf(self) -> MutByteBuf {
        match self {
            State::Reading(buf) => buf,
            _ => panic!("connection not in reading state"),
        }
    }

    fn read_cql_response(&self,version: u8) -> RCResult<CqlResponse>{
        match *self {
            State::Reading(ref buf) => {
                let bytes_buf = self.read_buf().bytes();   
                //By now, just copy it to avoid borrowing troubles
                let mut response : ByteBuf = ByteBuf::from_slice(bytes_buf);
                response.read_cql_response(version)
            },
            _ => panic!("connection not in reading state"),
        }
    }
    /*
    fn write_buf(&self) -> &ByteBuf {
        match *self {
            State::Writing(ref buf) => buf,
            _ => panic!("connection not in writing state"),
        }
    }

    fn mut_write_buf(&mut self) -> &mut ByteBuf{
        match *self {
            State::Writing(ref mut buf) => buf,
            _ => panic!("connection not in writing state"),
        }
    }

    fn unwrap_write_buf(self) -> ByteBuf {
        match self {
            State::Writing(buf) => buf,
            _ => panic!("connection not in writing state"),
        }
    }
    */
}