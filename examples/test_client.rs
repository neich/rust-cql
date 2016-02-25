extern crate cql;
extern crate eventual;
extern crate mio;

use cql::*;
use std::borrow::Cow;
use std::io::Write;
use std::thread;
use eventual::*;

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

fn main() {
    test_client();
}

fn test_client() {
    println!("Connecting ...!");
    let ip = "172.17.0.2";
    let port = "9042";
    let ip_port = ip.to_string()+":"+port;
    let mut client = try_test!(cql::connect(ip_port.parse().ok().expect("Couldn't parse address"),None), "Error connecting to server at "+ip_port);
    println!("Connected with CQL binary version v{}", client.version);

    // let params = vec![cql::CqlVarchar(Some((Cow::Borrowed("TOPOLOGY_CHANGE")))), 
    //                                        cql::CqlVarchar(Some((Cow::Borrowed("STATUS_CHANGE")))) ];
    let params = vec![ CqlVarchar(Some(CqlEventType::EventStatusChange.get_str()))];

    let future = client.send_register(params);
    let response = try_test!(future.await().unwrap(),"Error sending register to events");
    //assert_response!(response);
    println!("Result: {:?} \n", response);

    // A long sleep because I'm trying to see if Cassandra sends 
    // any event message after a node change his status to up.
    thread::sleep_ms(600000);

}
