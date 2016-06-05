use eventual::{Future, Async, Complete};
use mio::{Token, EventLoop, Sender, TryRead, TryWrite, EventSet};
use mio::tcp::TcpStream;
use mio::util::Slab;
use mio;

use std::net::{SocketAddr,IpAddr,Ipv4Addr};
use std::collections::BTreeMap;
use std::borrow::Cow;
use std::error::Error;
use def::CqlEvent;
use error::{RCResult,RCError,RCErrorType};
use error::RCErrorType::*;
use connection::{Connection,CqlMsg,connect};




pub struct ConnectionPool {
    token_by_ip: BTreeMap<IpAddr,Token>,
    connections: Slab<Connection>,
    event_handler: Sender<CqlEvent>
}

impl ConnectionPool {
    pub fn new(event_handler: Sender<CqlEvent>) -> ConnectionPool {
        ConnectionPool {
            token_by_ip: BTreeMap::new(),
            connections: Slab::new_starting_at(Token(1), 128),
            event_handler: event_handler
        }
    }
    
    fn get_connection_with_ip(&mut self,event_loop: &mut EventLoop<ConnectionPool>,address:&IpAddr) -> Result<&mut Connection,&'static str>{
        println!("[ConnectionPool::get_connection_with_ip]");
        if !self.exists_connection_by_ip(address){
            //let token = self.create_connection(event_loop,address).unwrap();
            //return self.find_connection_by_token(token)
            return Err("Connection doesn't exist")
        }
        else {
            self.find_connection_by_ip(address)
        }
    }
    
    fn create_connection(&mut self,event_loop: &mut EventLoop<ConnectionPool>,address:&IpAddr) -> RCResult<Token>{
        println!("[ConnectionPool::create_connection]");
        let mut conn = try_rc!(connect(SocketAddr::new(address.clone(),9042),
                                None,
                                event_loop,
                                self.event_handler.clone()),"Failed connecting");
        let token = try_rc!(self.add_connection(address.clone(),conn),"Failed adding a new connection");
        Ok(token)
    }

    fn add_connection(&mut self, address:IpAddr,connection: Connection)-> RCResult<Token>{
        println!("[ConnectionPool::add_connection]");
        let result = self.connections.insert(connection);

        match result{
            Ok(token) => {
                {
                let conn = self.find_connection_by_token(token).ok().expect("Couldn't unwrap the connection");
                println!("Setting token {:?}",token);
                conn.set_token(token);
                }
                self.token_by_ip.insert(address,token);
                Ok(token)
            },
            Err(err) => {
                Err(RCError::new("Credential should be provided for authentication", ReadError))
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
        let ip = &msg.get_ip();
        //let complete = msg.get_complete();
        match msg {
            CqlMsg::Request{..} => {
                let mut result = self.get_connection_with_ip(event_loop,ip);  
                // Here is where we should do create a new connection if it doesn't exist.
                // Connect, then send_startup with the queue_message
                match result {
                    Ok(conn) =>{
                        conn.insert_request(msg);
                        conn.reregister(event_loop,EventSet::writable());
                    },
                    Err(err) =>{
                        //TO-DO 
                        //Complete all requests with connection error
                    }
                }
            },
            CqlMsg::Connect{..} => {
                let mut result = self.create_connection(event_loop,ip);
                match result {
                    Ok(token) =>{
                        let conn = self.find_connection_by_ip(ip).unwrap();
                        conn.insert_request(msg);
                        conn.reregister(event_loop,EventSet::writable());
                    },
                    Err(ref err) =>{
                        msg.complete((Err(RCError::new("Couldn't connect to host", RCErrorType::ConnectionError))));
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
        let mut connection = self.find_connection_by_token(token).ok().expect("Couldn't get connection");           
        if events.is_readable() {
            println!("    connection-EventSet::Readable");
            connection.read(event_loop);

            let pair = connection.read_cql_response();
            let response = pair.0;
            let is_event = pair.1;
            //println!("Response from event_loop: {:?}",response);
            println!("Handling response..");
            connection.handle_response(response,event_loop,is_event);
        }

        if events.is_writable() && connection.are_pendings_send(){
            println!("    connection-EventSet::Writable");
            connection.write(event_loop);
        }

        println!("[Connection::Ended ready]");
    }
}