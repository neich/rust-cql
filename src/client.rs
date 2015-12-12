extern crate mio;
extern crate bytes;
extern crate eventual;

use self::eventual::{Future,Async};
use self::mio::*;
use self::mio::tcp::TcpStream;
use self::mio::util::Slab;
use self::mio::buf::ByteBuf;
use self::mio::buf::MutByteBuf;
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

pub type PrepsStore = BTreeMap<String, Box<CqlPreparedStat>>;


pub struct Client {
    pool: Pool, //Conjunt de channels
    pub version: u8,
    prepared: PrepsStore
}

impl Client{
    
    fn new(version:u8) -> Client {
        Client{
            pool: Pool::new(),
            version: version,
            prepared: BTreeMap::new()
        }
    }
    
    pub fn get_prepared_statement(&mut self, ps_id: &str) -> RCResult<&CqlPreparedStat> {
        match self.prepared.get(ps_id) {
            Some(ps) => Ok(&**ps),
            None => return Err(RCError::new(format!("Unknown prepared statement <{}>", ps_id), GenericError))
        }
    }

    fn send_startup(&mut self, creds: Option<&Vec<CowStr>>,token: Token) -> RCResult<()> {
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
        println!("At [method] Client::send_startup");

        let mut future = self.send_message(msg_startup,token);
        let mut buf = future.await().ok().expect("Couldn't recieve future");
        
        let response = try_rc!(buf.read_cql_response(self.version), "Error reading response");
        println!("Matching response...");
        
        match response.body {
            ResponseReady =>  Ok(()),
            ResponseAuth(_) => {
                match creds {
                    Some(cred) => {
                        let msg_auth = CqlRequest {
                            version: self.version,
                            flags: 0x00,
                            stream: 0x01,
                            opcode: OpcodeOptions,
                            body: RequestCred(cred),
                        };
                        future = self.send_message(msg_auth,token);
                        buf = future.await().ok().expect("Couldn't recieve future");
                        let response = try_rc!(buf.read_cql_response(self.version), "Error reading authenticaton response");
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

    fn send_message(&mut self,cql_msg: CqlRequest,token:Token) -> Future<ByteBuf, ()>{
        let mut buf = ByteBuf::mut_with_capacity(2048);
        cql_msg.serialize(&mut buf, self.version);  //Faltaria comprovar si hi ha error al serialitzar
        let (tx, future) = Future::<ByteBuf, ()>::pair();
        self.pool.find_channel_by_token(token)
                 .send(MyMsg { 
                        buf: buf.flip(), 
                        tx: tx});
        future
    }

    fn add_connection(&mut self ,socket: TcpStream,token:Token){
        let mut event_loop : EventLoop<Connection> = mio::EventLoop::new().ok().expect("Couldn't create event loop");
        println!("Adding connection!!");
        self.pool.add_channel_with_token(event_loop.channel(),token);
        println!("It's seems we could add a connection ");
        event_loop.register_opt(
                &socket,
                token,
                mio::EventSet::writable(),
                mio::PollOpt::edge() | mio::PollOpt::oneshot()).unwrap();
        //We'll need the event loop to register a new socket
        //But on creating the thread we borrow the even_loop
        let mut connection =  Connection {
                socket: socket,
                token: token,
                state: State::Waiting};

        println!("Even loop starting...");
        //Li dono la conexio i no la guardo perque si no es queixa del borrow
        //Nomes ens guardem el channel
        thread::spawn(move||{
            event_loop.run(&mut connection).ok().expect("Failed to start event loop");
            });
    }
}


pub fn connect(address: SocketAddr, creds:Option<&Vec<CowStr>>) -> RCResult<Client> {

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
            client.add_connection(socket,token);
            //S'hauria de fer un shutdown en cas que falli la connexio
            match client.send_startup(creds,token) {
                Ok(_) => return Ok(client),
                Err(e) => println!("Error connecting with protocol version v{}: {}", version, e.desc)
            }
            version -= 1;
        }
        Err(RCError::new("Unable to find suitable protocol version (v1, v2, v3)", ReadError))

    }
    

pub struct MyMsg {
    buf: ByteBuf,
    tx: eventual::Complete<ByteBuf,()>
}


pub struct Pool {
    connections: Slab<Sender<MyMsg>>
}

impl Pool {
    fn new() -> Pool {
        Pool {
            // Allocate a slab that is able to hold exactly the right number of
            // connections.
            connections: Slab::new_starting_at(Token(1),128)
        }
    }
    /// Find a connection in the slab using the given token.
    fn find_channel_by_token<'a>(&'a mut self, token: Token) -> &'a mut Sender<MyMsg> {
        &mut self.connections[token]
    }
    fn add_channel_with_token(& mut self,channel: Sender<MyMsg>,token: Token){
        self.connections.insert_with(|token| {channel});
    }
}

impl mio::Handler for Connection {
    type Timeout = ();
    type Message = MyMsg;

    fn notify(&mut self, event_loop: &mut EventLoop<Connection>, msg: MyMsg) {
        println!("[Connection::notify]");
        self.set_state(event_loop,State::Writing(msg.buf));
        self.write(event_loop);
        //Transition to reading is called after write
        //Now we can read from the socket
        //self.set_state(event_loop,State::Reading(ByteBuf::mut_with_capacity(2048)));  
        //thread::sleep_ms(1);  
        self.read(event_loop);  //Response should be at self.state = Reading(Response)

        let bytes_buf = self.state.read_buf().bytes();   
        //De moment fem la copia perque no deixa obtenir el read buffer directament

        let response : ByteBuf = ByteBuf::from_slice(bytes_buf);
        //let asdf = buf.flip();

        msg.tx.complete(response);
        println!("[Connection::notify] ended");
        //??
        //Does it work?!¿?
        //event_loop.shutdown();
    }

    //There's no shutdown by now
    fn ready(&mut self, event_loop: &mut mio::EventLoop<Connection>, token: mio::Token, events: mio::EventSet) {
        println!("[Connection::ready]");
        /*
        println!("Assigned token is: {:?}",self.token);

        // Check the current state of the connection and handle the event
        // appropriately.
        match self.state {
            State::Reading(..) => {
                println!("    connection-state=Reading");
                assert!(events.is_readable(), "unexpected events; events={:?}", events);
                self.read(event_loop)
            }
            State::Writing(..) => {
                println!("    connection-state=Writing");
                assert!(events.is_writable(), "unexpected events; events={:?}", events);
                self.write(event_loop)
            }
            State::Closed(..) => {
                println!("    connection-state=Closed");
                event_loop.shutdown();
            }
            _ => (),
        }
        */
        println!("[Connection::Ended ready]");
    }
}

struct Connection {
    // The connection's TCP socket 
    socket: TcpStream,
    // The token used to register this connection with the EventLoop
    token: mio::Token,
    // The current state of the connection (reading or writing)
    state: State,
    // Response from the server
    //response: MutByteBuf,
}

impl Connection {


    fn read(&mut self, event_loop: &mut mio::EventLoop<Connection>) {
        match self.socket.try_read_buf(self.state.mut_read_buf()) {
            Ok(Some(0)) => {
                 println!("    connection-state=Closed");
                // The socket (or at least the read half) is closed. There is
                // nothing more that can be done, so just close the socket.
                self.state = State::Closed;
            }
            Ok(Some(n)) => {
                println!("read {} bytes", n);

                // Check for a newline, if there is a newline, then print the
                // received data. Otherwise, stay in the reading state.
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

    fn write(&mut self, event_loop: &mut mio::EventLoop<Connection>) {
        match self.socket.try_write_buf(self.state.mut_write_buf()) {
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
        //println!("[Connection::reregister]");
        // Maps the current client state to the mio `EventSet` that will provide us
        // with the notifications that we want. When we are currently reading from
        // the client, we want `readable` socket notifications. When we are writing
        // to the client, we want `writable` notifications.
        let event_set = match self.state {
            State::Reading(..) => mio::EventSet::readable(),
            State::Writing(..) => mio::EventSet::writable(),
            _ => return,
        };
        //println!("Trying to Reregister");  
        event_loop.reregister(&self.socket, self.token, event_set, mio::PollOpt::oneshot())
            .unwrap();
        //println!("Reregistered!"); 
    }

    fn set_state(&mut self,event_loop: &mut mio::EventLoop<Connection>,state: State){
        self.state = state;
        let event_set = match self.state {
            State::Reading(..) => mio::EventSet::readable(),
            State::Writing(..) => mio::EventSet::writable(),
            _ => return,
        };
        event_loop.reregister(&self.socket, self.token, event_set, mio::PollOpt::oneshot())
            .unwrap();
    }

    fn is_closed(&self) -> bool {
        match self.state {
            State::Closed => true,
            _ => false,
        }
    }
}

//#[derive(Debug)]
enum State {
    Reading(MutByteBuf),
    Writing(ByteBuf),
    Waiting,
    Closed
}

impl State {
    
    fn try_transition_to_reading(&mut self) {
        if !self.write_buf().has_remaining() {
            self.transition_to_reading();
        }
    }
    
    //Dangerous function
    fn transition_to_reading(&mut self) {
        println!("[State::transition_to_reading");
        let mut buf = mem::replace(self, State::Closed)
            .unwrap_write_buf();

        let mut mut_buf = buf.flip();
        mut_buf.clear();
        println!("[State::transition_to_reading] Ending..");
        *self = State::Reading(mut_buf);
    }

    /*
    fn try_transition_to_writing(&mut self, remaining: &mut Vec<Vec<u8>>) {
        match self.read_buf().last() {
            Some(&c) if c == b'\n' => {
                // Wrap in a scope to work around borrow checker
                {
                    // Get a string back
                    let s = str::from_utf8(self.read_buf()).unwrap();
                    println!("Got from server: {}", s);
                }

                //self.transition_to_writing(remaining);
            }
            _ => {}
        }
    }
    */
    /*
    fn transition_to_writing(&mut self, remaining: &mut Vec<Vec<u8>>) {
        if remaining.is_empty() {
            *self = State::Closed;
            return;
        }

        let line = remaining.remove(0);
        *self = State::Writing(Cursor::new(line));
    }
    */
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
}