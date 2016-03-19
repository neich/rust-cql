
use std::net::SocketAddr;
use std::error::Error;
use std::thread;
use def::mio::{EventLoop,Sender};
use def::{RCResult,CqlResponse};
use def::eventual::Async;
use node::{Node,ChannelPool};
use connection_pool::ConnectionPool;


pub struct Cluster<'a>{
	current_node: Node<'a>,
	available_nodes: Vec<Node<'a>>,
	channel_pool: ChannelPool
}

impl<'b,'a:'b> Cluster <'a>{

	pub fn new() -> Cluster<'a>{
		Cluster{
			available_nodes: Vec::new(),
			channel_pool: ChannelPool::new(),
			current_node: Node::new("0.0.0.0:9042".parse().unwrap(),&channel_pool)
		}
	}

	pub fn connect_cluster(&mut self,address: SocketAddr) -> RCResult<CqlResponse>{
		let mut current_node = Node::new(address,&self.channel_pool);
		current_node.connect().await().unwrap()
	}

	pub fn start_cluster(&mut self){
		self.run_event_loop();
	}

	fn run_event_loop(&mut self){

        let mut event_loop : EventLoop<ConnectionPool> = 
                EventLoop::new().ok().expect("Couldn't create event loop");
        
        self.channel_pool.add_channel(event_loop.channel());
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

