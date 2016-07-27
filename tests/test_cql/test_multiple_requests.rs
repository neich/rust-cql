extern crate cql;
extern crate eventual;
extern crate mio;
extern crate uuid;

use std::borrow::Cow;
use self::uuid::Uuid;
use cql::*;
use self::eventual::{Future,Async};
use std::collections::VecDeque;
use std::thread;

pub fn to_hex_string(bytes: &Vec<u8>) -> String {
  let strs: Vec<String> = bytes.iter()
                               .map(|b| format!("{:02X}", b))
                               .collect();
  strs.connect(" ")
}

fn test_multiple_requests(n: u16,version: u8){

   println!("Connecting ...!");
    let ip = "172.17.0.2";
    let port = "9042";
    let ip_port = ip.to_string()+":"+port;
    
    let mut cluster = Cluster::new();
    
    let mut response = try_test!(cluster.connect_cluster(ip_port.parse().ok().expect("Couldn't parse address")),
                                "Error connecting to cluster");

    let q = "select now() from system.local";
    let q2 = "select * from system.local";
    println!("cql::Query: {}", q);
    let mut future_vec: Vec<cql::CassFuture> = Vec::new();

    for _ in 0..n {
        //println!("Sending..");
        let response = cluster.exec_query(q, cql::Consistency::One);
        future_vec.push(response);
    }

    println!("{:?} messages to send.",future_vec.len());

    for future in future_vec{
        future.receive(move |data| {
            let ver = version.clone();
            let result = match data {
                Ok(res) => res,
                Err(err) => {
                    panic!("Error: {:?}",err);
                    return;
                },
            };
            let response = try_test!(result, "Error selecting table test");
            assert_response!(response);
            if version == 3{
                if response.stream%5000==0 || response.stream > 32765{
                    println!("Received result with stream id {:?} \n", response.stream);
                }
            }
            else{
                if response.stream%30==0 || response.stream > 125 {
                    println!("Received result with stream id {:?} \n", response.stream);
                }
            }
        });
    }
}



// This is test is NOT multithreaded because the event loop 
// is single threaded. Stream id is diferent for every consecutive
// cql request.

//#[test]
fn test_max_requests_v3(){
    let n  = 32768;
    let version = 3;
    test_multiple_requests(n,version);
    thread::sleep_ms(5000);
}

#[test]
fn test_max_requests_v2(){
    let n  = 128;
    let version = 2;
    test_multiple_requests(n,version);
    thread::sleep_ms(1000);
}


//#[test]
//#[should_panic]
fn test_more_requests_v3(){
    let n  = 33000;
    let version = 3;
    test_multiple_requests(n,version);
    thread::sleep_ms(40000);
}


