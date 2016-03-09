extern crate mio;
extern crate bytes;
extern crate eventual;

use self::eventual::{Future, Async, Complete};
use self::mio::{Token, EventLoop, Sender, TryRead, TryWrite, EventSet};
use self::mio::tcp::TcpStream;
use self::mio::util::Slab;
use self::bytes::{ByteBuf, MutByteBuf};
use std::{mem, str};
use std::io::Cursor;
use std::net::{SocketAddr,IpAddr,Ipv4Addr};
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
pub static CQL_MAX_SUPPORTED_VERSION:u8 = 0x02;

pub type CassFuture = Future<RCResult<CqlResponse>,()>;

pub struct Client {
    channel_pool: ChannelPool, //Set of channels
    pub version: u8,
    address: SocketAddr
}

impl Client{
    
    fn new(address: SocketAddr,version:u8) -> Client {
        Client{
            channel_pool: ChannelPool::new(),
            version: version,
            address: address
        }
    }
    
    pub fn exec_query(&mut self, query_str: &str, con: Consistency) -> CassFuture {
        let q = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,
            opcode: OpcodeQuery,
            body: RequestQuery(String::from(query_str), con, 0)};
        self.send_message(q)
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

        serialize_and_check_io_error!(serialize_with_client, &mut file, q, self, "Error serializing to file");
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

    fn send_startup(&mut self, creds: Option<Vec<CowStr>>) -> RCResult<()> {
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

        let mut future = self.send_message(msg_startup);
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
                                let response = self.send_message(msg_auth).await().ok().expect("Couldn't recieve future").ok().expect("Couldn't get CQL response");
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

    fn send_message(&mut self,request: CqlRequest) -> CassFuture{
        let (tx, future) = Future::<RCResult<CqlResponse>, ()>::pair();
        match self.channel_pool.find_available_channel(){
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

    pub fn send_register(&mut self,params: Vec<CqlValue>) -> CassFuture{
        let msg_register = CqlRequest {
            version: self.version,
            flags: 0x00,
            stream: 0x01,  
            opcode: OpcodeRegister,
            body: RequestRegister(params)
        };
        self.send_message(msg_register)
    }

    fn run_event_loop_with_connection(&mut self ,socket: TcpStream){
        let mut event_loop : EventLoop<ConnectionPool> = 
                mio::EventLoop::new().ok().expect("Couldn't create event loop");
        // It will be changed depending how it is decided to handle multiple connections and event loops
        let token = Token(1);
        let socket2 = socket.try_clone().unwrap();
        let ip1 = socket.peer_addr().unwrap().ip();
        let ip2 = socket2.peer_addr().unwrap().ip();
        println!("Adding connection");
        self.channel_pool.add_channel(event_loop.channel());
        println!("It seems we could add a connection ");
        event_loop.register(
                &socket,
                token,
                mio::EventSet::writable(),
                mio::PollOpt::edge() | mio::PollOpt::oneshot()).unwrap();
        // We will need the event loop to register a new socket
        // but on creating the thread we borrow the even_loop.
        // So we 'give away' the connection pool and keep the channel.
        let mut connection_pool = ConnectionPool::new();
        let mut conn =  Connection {
                socket: socket,
                token:Token(1),
                response: CassResponse::new(),
                pendings: Vec::new(),
                version: self.version
            };  

        // Connection only for recieve event messages
        let mut conn2 =  Connection {
                socket: socket2,
                token:Token(2),
                response: CassResponse::new(),
                pendings: Vec::new(),
                version: self.version
            };  

        connection_pool.add_connection(ip1,conn);
        connection_pool.add_connection(ip2,conn2);
        println!("Even loop starting...");
        // Only keep event loop channel
        thread::spawn(move||{
                event_loop.run(&mut connection_pool).ok().expect("Failed to start event loop");
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
            let mut socket = res.unwrap();
            let mut client = Client::new(address,version);
            // Shutdown is not needed here because
            // a client is created each loop
            client.run_event_loop_with_connection(socket);
            match client.send_startup(creds.clone()) {
                Ok(_) => return Ok(client),
                Err(e) => println!("Error connecting with protocol version v{}: {}", version, e.desc)
            }

            version -= 1;
        }
        Err(RCError::new("Unable to find suitable protocol version (v1, v2, v3)", ReadError))
    }
    
pub enum CqlMsg{
    Request{
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
            &CqlMsg::Shutdown =>{
                panic!("Shutdown doesn't have IP");
            }
        }
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
    fn new() -> ChannelPool {
        ChannelPool {
            channels: Vec::new()
        }
    }
    fn add_channel(&mut self, channel: Sender<CqlMsg>){
        self.channels.push(channel);
    }
    // For now only use one channel (and one event loop)
    fn find_available_channel(&mut self) -> Result<&Sender<CqlMsg>,&'static str>{
        if self.channels.len() > 0 {
            return Ok(self.channels.last().unwrap());
        }
        Err("There is no channel created")
    }
}


struct Connection {
    // The connection's TCP socket 
    socket: TcpStream,
    // The token used to register this connection with the EventLoop
    token: mio::Token,
    // The response from reading a socket
    response: CassResponse,
    // Pending messages to be send (CQL requests)
    pendings: Vec<CqlMsg>,
    // CQL version v1, v2 or v3
    version: u8
}


pub struct ConnectionPool {
    token_by_ip: BTreeMap<IpAddr,Token>,
    connections: Slab<Connection>
}

impl ConnectionPool {
    fn new() -> ConnectionPool {
        ConnectionPool {
            token_by_ip: BTreeMap::new(),
            connections: Slab::new_starting_at(Token(1), 128)
        }
    }
    /*
    fn get_connection_with_ip(&mut self,address:&IpAddr) -> Result<&mut Connection,&'static str>{
        if !self.exists_connection_by_ip(address){
            let conn = Connection::new(3);
            self.add_connection(address,conn);
            // Here is where it should be the connect and send_startup
            return Ok(&conn)
        }
        find_connection_by_ip(address)
    }
    */

    fn add_connection(&mut self, address:IpAddr,connection: Connection){
        let result = self.connections.insert(connection);
        match result{
            Ok(r) => {
               self.token_by_ip.insert(address,r);
            },
            Err(err) => {
                println!("Couldn't insert a new connection")
            }
        }
    }
    

    fn exists_connection_by_ip(&mut self,address:&IpAddr) -> bool{
        self.token_by_ip.contains_key(address)
    }

    fn exists_connection_by_token(&mut self,token: Token) -> bool{
        self.connections.contains(token)
    }

    fn find_connection_by_ip(&mut self,address:&IpAddr) -> Result<&mut Connection,&'static str>{
        if !self.connections.is_empty() {
            // Needs to be improved
            return Ok(self.connections.get_mut(self.token_by_ip
                                                   .get(address).unwrap().clone()
                                              ).unwrap());
        }
        Err("There is no connection found")
    }

    fn find_connection_by_token(&mut self,token: Token) -> Result<&mut Connection,&'static str>{
        if !self.connections.is_empty() {
            return Ok(self.connections.get_mut(token).unwrap());
        }
        Err("There is no connection found")
    }
}


impl mio::Handler for ConnectionPool {
    type Timeout = ();
    // Left one is the internal Handler message type and
    // right one is our defined type
    type Message = CqlMsg; 

    // Push pending messages to be send across the event 
    // loop. Only change state in case it is waiting
    fn notify(&mut self, event_loop: &mut EventLoop<ConnectionPool>, msg: CqlMsg) {
        println!("[ConnectionPool::notify]");
        match msg {
            CqlMsg::Request{..} => {
                let mut result = self.find_connection_by_ip(&msg.get_ip());  
                
                match result {
                    Ok(conn) =>{
                        // Ineficient, consider using a LinkedList
                        conn.pendings.insert(0,msg);
                        conn.reregister(event_loop,EventSet::writable());
                    },
                    Err(err) =>{

                    }
                }
            },
            CqlMsg::Shutdown => {
                event_loop.shutdown();
            },
        }
    }

    
    fn ready(&mut self, event_loop: &mut EventLoop<ConnectionPool>, token: Token, events: EventSet) {
        println!("[Connection::ready]");      
        println!("Assigned token is: {:?}",token);
        println!("Events: {:?}",events);
        let mut connection = self.find_connection_by_token(token).unwrap();      //This is just so it compiles for now          
        if events.is_readable() {
            println!("    connection-EventSet::Readable");
            connection.read(event_loop);

            let pair = connection.response.read_cql_response(connection.version);
            let response = pair.0;
            let is_event = pair.1;
            println!("Response from event_loop: {:?}",response);
            if is_event {
                println!("It seems we've got an Event message!");
                // Do proper stuff with the Event message here
            }
            else {
                // Completes the future with a CqlResponse
                // which is a RCResult<CqlResponse>
                // so we can handle errors properly
                match connection.pendings
                          .pop()
                          .take()
                          .unwrap() 
                {
                    CqlMsg::Request{request,tx,address} => {
                       tx.complete(response);
                    },
                    CqlMsg::Shutdown => {
                        panic!("Shutdown messages shouldn't be at pendings");
                    },
                }
            }
            
        }

        if events.is_writable() && connection.pendings.len() > 0{
            println!("    connection-EventSet::Writable");
            connection.write(event_loop)
        }

        if connection.pendings.len() == 0 {
            // Maybe do something here
        }
        println!("[Connection::Ended ready]");
    }
}

impl Connection {

        
    fn new(socket:TcpStream,token: Token,version: u8)-> Connection{
        Connection {
            socket: socket,
            token: token,
            response: CassResponse::new(),
            pendings: Vec::new(),
            version: version
        }
    }

    fn read(&mut self, event_loop: &mut EventLoop<ConnectionPool>) {
        match self.socket.try_read_buf(self.response.mut_read_buf()) {
            Ok(Some(0)) => {
                println!("read 0 bytes");
            }
            Ok(Some(n)) => {
                println!("read {} bytes", n);
                // self.read(event_loop);  // Recursion here, care
                // Re-register the socket with the event loop. The current
                // state is used to determine whether we are currently reading
                // or writing.
                self.reregister(event_loop,EventSet::writable());
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

    fn write(&mut self, event_loop: &mut EventLoop<ConnectionPool>) {
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
             CqlMsg::Shutdown => {
                panic!("Shutdown messages shouldn't be at pendings");
             },
        }
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

    fn reregister(&self, event_loop: &mut EventLoop<ConnectionPool>,events : EventSet) {
        // Maps the current client state to the mio `EventSet` that will provide us
        // with the notifications that we want. When we are currently reading from
        // the client, we want `readable` socket notifications. When we are writing
        // to the client, we want `writable` notifications.
        event_loop.reregister(&self.socket, self.token, events, mio::PollOpt::oneshot())
            .unwrap();
    }
    
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

    fn read_cql_response(&self,version: u8) -> (RCResult<CqlResponse>,bool){
            let bytes_buf = self.read_buf().bytes();   
            // By now, just copy it to avoid borrowing troubles
            let mut response : ByteBuf = ByteBuf::from_slice(bytes_buf);
            (response.read_cql_response(version),response.cql_response_is_event(version).unwrap())
    }
}