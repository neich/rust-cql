extern crate mio;
extern crate bytes;
extern crate eventual;

use self::eventual::{Future, Async, Complete};
use self::mio::{Token, EventLoop, Sender, TryRead, TryWrite, EventSet};
use self::mio::tcp::TcpStream;
use self::mio::util::Slab;

use std::net::{SocketAddr,IpAddr,Ipv4Addr};
use std::collections::BTreeMap;
use std::borrow::Cow;
use std::error::Error;

use connection::{Connection,CqlMsg,connect};




pub struct ConnectionPool {
    token_by_ip: BTreeMap<IpAddr,Token>,
    connections: Slab<Connection>,
    pending_startup: Vec<Token>
}

impl ConnectionPool {
    pub fn new() -> ConnectionPool {
        ConnectionPool {
            token_by_ip: BTreeMap::new(),
            connections: Slab::new_starting_at(Token(1), 128),
            pending_startup: Vec::new()
        }
    }
    
    fn get_connection_with_ip(&mut self,event_loop: &mut EventLoop<ConnectionPool>,address:&IpAddr) -> Result<&mut Connection,&'static str>{
        println!("[ConnectionPool::get_connection_with_ip]");
        if !self.exists_connection_by_ip(address){
            println!("Connection doesn't exist, let's created!");
            let mut conn = connect(SocketAddr::new(address.clone(),9042),None,event_loop).ok().expect("Couldn't unwrap the connection");

            let token = self.add_connection(address.clone(),conn).ok().expect("Couldn't unwrap the token");
            self.pending_startup.push(token);
            return self.find_connection_by_token(token)
        }
        else {
            self.find_connection_by_ip(address)
        }
    }
    

    fn add_connection(&mut self, address:IpAddr,connection: Connection)-> Result<Token,&'static str>{
        println!("[ConnectionPool::add_connection]");
        let result = self.connections.insert(connection);

        match result{
            Ok(token) => {
                {
                let conn = self.find_connection_by_token(token).ok().expect("Couldn't unwrap the connection");
                conn.set_token(token);
                }
                self.token_by_ip.insert(address,token);
                Ok(token)
            },
            Err(err) => {
                println!("Couldn't insert a new connection");
                Err("Couldn't insert a new connection")
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
        println!("[ConnectionPool::find_connection_by_ip]");
        if !self.connections.is_empty() {
            // Needs to be improved
            return Ok(self.connections.get_mut(self.token_by_ip
                                                   .get(address).unwrap().clone()
                                              ).unwrap());
        }
        Err("There is no connection found")
    }

    fn find_connection_by_token(&mut self,token: Token) -> Result<&mut Connection,&'static str>{
        println!("[ConnectionPool::find_connection_by_token]");
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
                let mut result = self.get_connection_with_ip(event_loop,&msg.get_ip());  
                // Here is where we should do create a new connection if it doesn't exists.
                // We do the connect, and then we can do the send_startup with the queue_message
                // and wait for the response
                println!("Line 319");
                match result {
                    Ok(conn) =>{
                        // Ineficient, consider using a LinkedList
                        conn.insert_request(msg);
                        conn.reregister(event_loop,EventSet::writable());
                    },
                    Err(err) =>{

                    }
                }
            },
            CqlMsg::Shutdown => {
                event_loop.shutdown();
            },
            _ => {
                unimplemented!();
            },
        }
    }

    
    fn ready(&mut self, event_loop: &mut EventLoop<ConnectionPool>, token: Token, events: EventSet) {
        println!("[Connection::ready]");      
        println!("Assigned token is: {:?}",token);
        println!("Events: {:?}",events);
        let mut connection = self.find_connection_by_token(token).ok().expect("Couldn't get connection");           
        if events.is_readable() {
            println!("    connection-EventSet::Readable");
            connection.read(event_loop);

            let pair = connection.read_cql_response();
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
                connection.handle_response(response,event_loop);
            }
        }

        if events.is_writable() && connection.are_pendings(){
            println!("    connection-EventSet::Writable");
            connection.write(event_loop);
        }

        println!("[Connection::Ended ready]");
    }
}