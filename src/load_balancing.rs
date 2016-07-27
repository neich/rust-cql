use node::Node;
use std::collections::BTreeMap;
use std::net::{IpAddr,Ipv4Addr};
use std::time::Duration;
use std::u64;
use std::u32;

// Trait methods cannot use 'Self'; &self or &mut self is OK
// https://doc.rust-lang.org/error-index.html#E0038
pub trait LoadBalancing {
    //fn new() -> Self ;
    fn select_node(&mut self,&BTreeMap<IpAddr,Node>) -> IpAddr;
}

#[derive(Clone)]
pub struct RoundRobin {
    pub index: usize
}

impl LoadBalancing for RoundRobin {
    fn select_node(&mut self,map: &BTreeMap<IpAddr,Node>) -> IpAddr{
        self.index = self.index + 1;
        if self.index >= map.len(){
            self.index = 0;
        }
        map.iter().nth(self.index).unwrap().0.clone()
    }
}

#[derive(Clone)]
pub struct LatencyAware;


impl LoadBalancing for LatencyAware {
    fn select_node(&mut self,map: &BTreeMap<IpAddr,Node>) -> IpAddr{
        let mut latency = Duration::new(u64::max_value()/2, u32::max_value()/2);
        let mut node_ip = IpAddr::V4(Ipv4Addr::new(0,0,0,0));
        for (ip,node) in map{
            if node.get_latency() < latency{
                latency = node.get_latency();
                node_ip = ip.clone();
            }
        }
        node_ip
    }
}

pub enum BalancerType{
    RoundRobin,
    LatencyAware
}