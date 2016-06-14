extern crate std;
extern crate eventual;
extern crate cql;

use std::borrow::Cow;
use std::io::Write;
use cql::*;
use self::eventual::{Future,Async};
use std::thread;
use std::time::Duration;
use std::io::{self, Read};


#[macro_use]
macro_rules! assert_response(
    ($resp:expr) => (
        if match $resp.opcode { cql::OpcodeResponse::OpcodeError => true, _ => false } {
            panic!("Test failed at assertion: {}",
                match $resp.body { cql::CqlResponseBody::ResponseError(_, message) => message, _ => Cow::Borrowed("Ooops!")});
        }
    );
);

macro_rules! try_test(
    ($call: expr, $msg: expr) => {
        match $call {
            Ok(val) => val,
            Err(ref err) => panic!("Test failed at library call: {}", err.description())
        };
    }
);

pub fn to_hex_string(bytes: &Vec<u8>) -> String {
  let strs: Vec<String> = bytes.iter()
                               .map(|b| format!("{:02X}", b))
                               .collect();
  strs.connect(" ")
}

fn show_options(){
    println!("");
    println!("");
    println!("Enter any option");
    println!("Options: ");
    println!("");

    print!("\tshow info");
    println!("\tShow information about the cluster");

    print!("\tlatency aware");
    println!("\tApply load balancing 'latency aware' policy");

    print!("\tround robin");
    println!("\tApply load balancing 'round robin ' policy");

    print!("\tend");
    println!("\t\tEnd the test");
    println!("");
    println!("");
}
#[test]
fn test_events() {
    
    let ip = "172.17.0.2";
    let port = "9042";
    let ip_port = ip.to_string()+":"+port;
    println!("Connecting to {:?}",ip_port);
    let mut cluster = Cluster::new();
    let mut response = try_test!(cluster.connect_cluster(ip_port.parse().ok().expect("Couldn't parse address")),
                                "Error connecting to cluster");
    //println!("Result: {:?}", response);
    //assert_response!(response);

    show_options();
    let mut input = read_string().unwrap();
    let mut end = false;
    while !end {
        match input.as_str() {
           "show info\n" => cluster.show_cluster_information(),
           "end\n" => {
                println!("Ending program..");
                end = true;
            },
            "latency aware\n" => {
                cluster.set_load_balancing(BalancerType::LatencyAware,Duration::from_secs(1));
                println!("Latency aware policy applied.");
            },
            "round robin\n" => {
                cluster.set_load_balancing(BalancerType::RoundRobin,Duration::from_secs(1));
                println!("Round robin policy appield.");
            },
           _ => println!("Unknown option: {:?}",input),
        }
        if(!end){
            show_options();
            input = read_string().unwrap();
        }
    }
}

fn read_string() -> io::Result<String> {
    let mut buffer = String::new();
    try!(io::stdin().read_line(&mut buffer));
    Ok(buffer)
}
