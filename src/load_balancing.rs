use node::Node;

// Methods cannot use 'Self'; &self or &mut self is OK
// https://doc.rust-lang.org/error-index.html#E0038
pub trait LoadBalancing {
	//fn new() -> Self ;
	fn select_node(&mut self,&Vec<Node>) -> usize;
}

#[derive(Clone)]
pub struct RoundRobin {
	pub index: usize
}

impl LoadBalancing for RoundRobin {
	// fn new() -> RoundRobin{
	// 	RoundRobin{
	// 		index: 0
	// 	}
	// }
    fn select_node(&mut self,v: &Vec<Node>) -> usize{
    	self.index = self.index + 1;
    	if self.index==v.len(){
    		self.index = 0;
    	}
    	self.index
    }
}

#[derive(Clone)]
pub struct LatencyAware;


impl LoadBalancing for LatencyAware {
	// fn new() -> LatencyAware{
	// 	LatencyAware{
	// 		index: 0
	// 	}
	// }

    fn select_node(&mut self,v: &Vec<Node>) -> usize{
    	let mut latency = 1;
    	let mut index = 0;
    	for node in v{
    		if node.get_latency() < latency{
    			latency = node.get_latency();
    		}
    		index = index +1;
    	}
    	index
    }
}