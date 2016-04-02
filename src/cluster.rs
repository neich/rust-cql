
use std::net::SocketAddr;
use std::error::Error;
use std::thread;
use def::mio::{EventLoop,Sender};
use def::{RCResult,CqlResponse,CassFuture};
use def::eventual::Async;
use node::{Node,ChannelPool};
use connection_pool::ConnectionPool;
use std::convert::AsRef;
use std::rc::Rc;
use std::boxed::Box;
use std::cell::RefCell;
use load_balancing::*;
use def::Consistency;

type RcRefCellBox<T> = Rc<RefCell<Box<T>>>;

pub struct Cluster{
	// Index of the current_node we are using
	current_node: usize,	
	available_nodes: Vec<Node>,
	channel_pool: Rc<ChannelPool>,
	// https://doc.rust-lang.org/error-index.html#E0038
	balancer:  Rc<LoadBalancing>	
}

impl Cluster {

	pub fn new() -> Cluster{
		Cluster{
			available_nodes: Vec::new(),
			channel_pool: Rc::new(ChannelPool::new()),
			current_node: 0,
			balancer: Rc::new(RoundRobin{index:0})
		}
	}

	pub fn set_load_balancing(&mut self,balancer: Rc<LoadBalancing>){
		self.balancer = balancer;
	}

	pub fn connect_cluster(&mut self,address: SocketAddr) -> RCResult<CqlResponse>{
		let mut node = Node::new(address);
		node.set_channel_pool(Rc::downgrade(&self.channel_pool));
		self.available_nodes.push(node);
		self.get_current_node().connect().await().unwrap()
	}

	fn get_current_node(&mut self) -> &Node{
		self.current_node = Rc::get_mut(&mut self.balancer).unwrap().select_node(&self.available_nodes);
		&self.available_nodes[self.current_node]
	}

	pub fn start_cluster(&mut self){
		self.run_event_loop();
	}

	pub fn get_peers(&mut self) -> CassFuture{
		self.get_current_node().get_peers()
	}


	pub fn exec_query(&mut self, query_str: &str, con: Consistency) -> CassFuture {
		self.get_current_node().exec_query(query_str,con)
	}

	pub fn register(&mut self) -> CassFuture{

		self.get_current_node().send_register(Vec::new())
	}
	fn run_event_loop(&mut self){

        let mut event_loop : EventLoop<ConnectionPool> = 
                EventLoop::new().ok().expect("Couldn't create event loop");
        
        Rc::get_mut(&mut self.channel_pool).unwrap().add_channel(event_loop.channel());
        // We will need the event loop to register a new socket
        // but on creating the thread we borrow the even_loop.
        // So we 'give away' the connection pool and keep the channel.
        let mut connection_pool = ConnectionPool::new();

        println!("Starting event loop...");
        // Only keep the event loop channel
        thread::spawn(move||{
                event_loop.run(&mut connection_pool).ok().expect("Failed to start event loop");
            });
    }
}

