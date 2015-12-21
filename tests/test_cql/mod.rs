extern crate std;
extern crate eventual;

use std::borrow::Cow;
use std::io::Write;
use cql;
use self::eventual::{Future,Async};

pub fn to_hex_string(bytes: &Vec<u8>) -> String {
  let strs: Vec<String> = bytes.iter()
                               .map(|b| format!("{:02X}", b))
                               .collect();
  strs.connect(" ")
}

#[test]
fn test() {
    println!("Connecting ...!");
    let mut client = try_test!(cql::connect("127.0.0.1:9042".parse().ok().expect("Couldn't parse address"),None), "Error connecting to server at 127.0.0.1:9042");
    println!("Connected with CQL binary version v{}", client.version);

    let mut q = "create keyspace if not exists rust with replication = {'class': 'SimpleStrategy', 'replication_factor':1}";
    println!("cql::Query: {}", q);
    let mut response = try_test!(client.exec_query(q, cql::Consistency::One).await().unwrap(), "Error creating keyspace");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "create table if not exists rust.test (id text primary key, f32 float, f64 double, i32 int, i64 bigint, b boolean, ip inet)";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One).await().unwrap(), "Error creating table rust.test");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "insert into rust.test (id, f32, f64, i32, i64, b, ip) values ('asdf', 1.2345, 3.14159, 47, 59, true, '127.0.0.1')";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One).await().unwrap(), "Error inserting into table test");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "select * from rust.test";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One).await().unwrap(), "Error selecting from table test");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "insert into rust.test (id, f32) values (?, ?)";
    println!("Create prepared: {}", q);
    let preps = try_test!(client.prepared_statement(q), "Error creating prepared statement");
    println!("Created prepared with id = {}", to_hex_string(&preps.id));

    println!("Execute prepared");
    let params: &Vec<cql::CqlValue> = &vec![cql::CqlVarchar(Some(Cow::Borrowed("ttrwe"))), cql::CqlFloat(Some(15.1617))];
    response = try_test!(client.exec_prepared(&preps.id, params, cql::Consistency::One).await().unwrap(), "Error executing prepared statement");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "select * from rust.test";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One).await().unwrap(), "Error selecting from table test");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    println!("Execute batch");
    let params2: Vec<cql::CqlValue> = vec![cql::CqlVarchar(Some(Cow::Borrowed("batch2"))), cql::CqlFloat(Some(666.65))];
    let q_vec = vec![cql::QueryStr(Cow::Borrowed("insert into rust.test (id, f32) values ('batch1', 34.56)")),
                     cql::QueryPrepared(preps.id, params2)];
    response = try_test!(client.exec_batch(cql::BatchType::Logged, q_vec, cql::Consistency::One).await().unwrap(), "Error executing batch cql::Query");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "select * from rust.test";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One).await().unwrap(), "Error selecting from table test");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "create table if not exists rust.test2 (id text primary key, l list<int>, m map<int, text>, s set<float>)";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One).await().unwrap(), "Error creating table test2");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "insert into rust.test2 (id, l, m, s) values ('asdf', [1,2,3,4,5], {0: 'aa', 1: 'bbb', 3: 'cccc'}, {1.234, 2.345, 3.456, 4.567})";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One).await().unwrap(), "Error inserting into table test2");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "select * from rust.test2";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One).await().unwrap(), "Error selecting from table test2");
    assert_response!(response);
    println!("Result: {:?} \n", response);
}


fn test_multiple_requests(n:u16){

    println!("Connecting ...!");
    let mut client = try_test!(cql::connect("127.0.0.1:9042".parse().ok().expect("Couldn't parse address"),None), "Error connecting to server at 127.0.0.1:9042");
    println!("Connected with CQL binary version v{}", client.version);

    let mut q = "create table if not exists rust.test (id text primary key, f32 float, f64 double, i32 int, i64 bigint, b boolean, ip inet)";
    println!("cql::Query: {}", q);
    let mut response = try_test!(client.exec_query(q, cql::Consistency::One).await().unwrap(), "Error creating table rust.test");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "insert into rust.test (id, f32, f64, i32, i64, b, ip) values ('asdf', 1.2345, 3.14159, 47, 59, true, '127.0.0.1')";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One).await().unwrap(), "Error inserting into table test");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "select * from rust.test";
    println!("cql::Query: {}", q);
    let mut future_vec: Vec<cql::CassFuture> = Vec::new();
    
    for _ in 1..n{
        let response = client.exec_query(q, cql::Consistency::One);
        future_vec.push(response);
    }
    
    for future in future_vec{
        future.receive(|data| {
            let response = try_test!(data.unwrap(), "Error selecting table test");
            assert_response!(response);
            println!("Result: {:?} \n", response);
        });
    }
}

// Default notify capacity (nc) is 4096 (n). If n > nc
// then another thread should panic. Test will pass
// anyway since main thread doesn't panic. We should
// catch thread panic which is a nightly feature (1.7).

// This is test is NOT multithreaded because the event loop 
// only uses one thread. Stream = 1 in every cql request.

#[test]
fn test_max_requests(){
    test_multiple_requests(4096);
}

#[test]
#[should_panic]
fn test_more_requests(){
    test_multiple_requests(10000);
}
