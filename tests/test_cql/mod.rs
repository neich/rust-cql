extern crate std;
extern crate eventual;
extern crate cql;

use std::borrow::Cow;
use std::io::Write;
use cql::*;
use self::eventual::{Future,Async};
use std::thread;
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
fn test_queries() {
    
    let ip = "172.17.0.2";
    let port = "9042";
    let ip_port = ip.to_string()+":"+port;
    println!("Connecting to {:?}",ip_port);
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
                decimal_ decimal,
                double_ double,
                float_ float,
                inet_ inet,
                int_ int primary KEY,
                text_ text,
                time_ time,
                timestamp_ timestamp,
                timeuuid_ timeuuid,
                uuid_ uuid,
                varchar_ varchar,
                varint_ varint
                );";

    println!("cql::Query: {}", q);
    response = try_test!(cluster.exec_query(q, cql::Consistency::One).await().unwrap(), "Error creating table rust.test_types");
    assert_response!(response);
    println!("Result: {:?} \n", response);
    
    q = "INSERT INTO rust.test_types
        (int_, ascii_, bigint_, blob_, boolean_, decimal_, double_, float_, inet_, text_, time_, timestamp_, timeuuid_, uuid_, varchar_, varint_)
        VALUES(0, 'asdf', 156234, 0x00A1, false, 1.6, 1.2345, 1.234567890123, '127.0.0.1', 'asdf', '13:30:54.234', toTimestamp(now()), now(), now(), 'qwerty', 27);";
    println!("cql::Query: {}", q);
    response = try_test!(cluster.exec_query(q, cql::Consistency::One).await().unwrap(), "Error inserting into table test");
    assert_response!(response);
    println!("Result: {:?} \n", response);
    
    q = "select * from rust.test_types";
    println!("cql::Query: {}", q);
    response = try_test!(cluster.exec_query(q, cql::Consistency::One).await().unwrap(), "Error selecting from table test");
    println!("Result: {:?} \n", response);
    assert_response!(response);

    q = "insert into rust.test_types (int_, ascii_,bigint_) values (?, ?,?)";
    println!("Create prepared: {}", q);
    let preps = try_test!(cluster.prepared_statement(q), "Error creating prepared statement");
    println!("Created prepared with id = {}", to_hex_string(&preps.id));

    println!("Execute prepared");
    let params = &vec![cql::CqlInt(Some(7)),CqlVarchar(Some(Cow::Borrowed(""))), cql::CqlBigInt(Some(1234567890))];
    response = try_test!(cluster.exec_prepared(&preps.id, params, cql::Consistency::One).await().unwrap(), "Error executing prepared statement");
    assert_response!(response);
    println!("Result: {:?} \n", response);
    
    q = "select * from rust.test_types";
    println!("cql::Query: {}", q);
    response = try_test!(cluster.exec_query(q, cql::Consistency::One).await().unwrap(), "Error selecting from table test");
    assert_response!(response);
    println!("Result: {:?} \n", response);
    
    println!("Execute batch");
    let params2 = vec![cql::CqlInt(Some(9)),CqlVarchar(Some(Cow::Borrowed("Arya Stark"))), cql::CqlBigInt(Some(987654321))];
    let q_vec = vec![cql::QueryStr(Cow::Borrowed("insert into rust.test_types (int_, float_) values (1, 34.56)")),
                     cql::QueryPrepared(preps.id, params2)];
    response = try_test!(cluster.exec_batch(cql::BatchType::Logged, q_vec, cql::Consistency::One).await().unwrap(), "Error executing batch cql::Query");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "select * from rust.test_types";
    println!("cql::Query: {}", q);
    response = try_test!(cluster.exec_query(q, cql::Consistency::One).await().unwrap(), "Error selecting from table test");
    assert_response!(response);
    println!("Result: {:?} \n", response);
}

mod test_reader;
mod test_multiple_requests;
mod test_events;
mod test_async;