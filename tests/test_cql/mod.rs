extern crate std;
extern crate eventual;
extern crate cql;

use std::borrow::Cow;
use std::io::Write;
use cql::*;
use self::eventual::{Future,Async};

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

#[test]
fn test() {
    println!("Connecting ...!");
    let ip = "172.17.0.2";
    let port = "9042";
    let ip_port = ip.to_string()+":"+port;
    
    let mut cluster = Cluster::new();
    
    let mut response = try_test!(cluster.connect_cluster(ip_port.parse().ok().expect("Couldn't parse address")),
                                "Error connecting to cluster");
    println!("Result: {:?} \n", response);
    assert_response!(response);

    let mut q = "create keyspace if not exists rust with replication = {'class': 'SimpleStrategy', 'replication_factor':1}";
    println!("cql::Query: {}", q);
    let mut response = try_test!(cluster.exec_query(q, cql::Consistency::One).await().unwrap(), "Error creating keyspace");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "create table if not exists rust.test_types (
                ascii_ ascii,
                bigint_ bigint,
                blob_ blob,
                boolean_ boolean,
                date_ date,
                decimal_ decimal,
                double_ double,
                float_ float,
                inet_ inet,
                int_ int primary KEY,
                smallint_ smallint,
                text_ text,
                time_ time,
                timestamp_ timestamp,
                timeuuid_ timeuuid,
                tinyint_ tinyint,
                uuid_ uuid,
                varchar_ varchar,
                varint_ varint
                );";

    println!("cql::Query: {}", q);
    response = try_test!(cluster.exec_query(q, cql::Consistency::One).await().unwrap(), "Error creating table rust.test_types");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "INSERT INTO rust.test_types
        (int_, ascii_, bigint_, blob_, boolean_, date_, decimal_, double_, float_, inet_, smallint_, text_, time_, timestamp_, timeuuid_, tinyint_, uuid_, varchar_, varint_)
        VALUES(0, 'asdf', 0, 0x00A1, false, toDate(now()), 1.0, 1.2345, 1.234567890123, '127.0.0.1', 13, 'asdf', toTime(now()), toTimestamp(now()), now(), 12, now(), 'qwerty', 27);";
    println!("cql::Query: {}", q);
    response = try_test!(cluster.exec_query(q, cql::Consistency::One).await().unwrap(), "Error inserting into table test");
    assert_response!(response);
    println!("Result: {:?} \n", response);


}
