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


fn test_requests_async(n: u16){

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
    let mut future_vec2: Vec<cql::CassFuture> = Vec::new();

    for _ in 0..n/2 {
        //println!("Sending..");
        let response = cluster.exec_query(q, cql::Consistency::One);
        future_vec2.push(response);
    }

    for _ in 0..n/2 {
        //println!("Sending..");
        let response = cluster.exec_query(q, cql::Consistency::One);
        future_vec.push(response);
    }

    println!("{:?} messages to send.",future_vec.len());



    for future in future_vec{
        future.receive(|data| {
            let result = match data {
                Ok(res) => res,
                Err(err) => {
                    panic!("Error: {:?}",err);
                    return;
                },
            };
            let response = try_test!(result, "Error selecting table test");
            assert_response!(response);
            println!("Received result with stream id {:?} \n", response.stream);
            //println!("Reponse: {:?}",response);
        });
    }
    thread::sleep_ms(100);
    for future in future_vec2{
        let result = future.await().unwrap().unwrap();
        assert_response!(result);
        println!("Result with stream id {:?} \n", result.stream);
        //println!("Reponse: {:?}",result);
    }
    // println!("Wait for first");
    // thread::sleep_ms(100);
    // 
    // //assert_response!(result);
    // println!("First request was id {:?} \n", result.stream);
}


#[test]
fn test_n_requests_async(){
    let n  = 10;
    test_requests_async(n);
    thread::sleep_ms(5000);
}

//#[test]
//#[should_panic]
fn test_more_requests(){
    test_multiple_requests(10000);
}


