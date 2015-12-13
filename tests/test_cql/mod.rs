extern crate std;

use std::borrow::Cow;
use std::io::Write;
use cql;

pub fn to_hex_string(bytes: &Vec<u8>) -> String {
  let strs: Vec<String> = bytes.iter()
                               .map(|b| format!("{:02X}", b))
                               .collect();
  strs.connect(" ")
}

#[test]
fn test() {
    println!("Connecting ...!");
    let mut client = try_test!(cql::connect("127.0.0.1", 9042, None), "Error connecting to server at 127.0.0.1:9042");
    println!("Connected with CQL binary version v{}", client.version);

    let mut q = "create keyspace rust with replication = {'class': 'SimpleStrategy', 'replication_factor':1}";
    println!("cql::Query: {}", q);
    let mut response = try_test!(client.exec_query(q, cql::Consistency::One), "Error creating keyspace");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "create table rust.test (id text primary key, f32 float, f64 double, i32 int, i64 bigint, b boolean, ip inet)";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One), "Error creating table rust.test");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "insert into rust.test (id, f32, f64, i32, i64, b, ip) values ('asdf', 1.2345, 3.14159, 47, 59, true, '127.0.0.1')";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One), "Error inserting into table test");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "select * from rust.test";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One), "Error selecting from table test");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "insert into rust.test (id, f32) values (?, ?)";
    println!("Create prepared: {}", q);
    let preps = try_test!(client.prepared_statement(q), "Error creating prepared statement");
    println!("Created prepared with id = {}", to_hex_string(&preps.id));

    println!("Execute prepared");
    let params: &[cql::CqlValue] = &[cql::CqlVarchar(Some(Cow::Borrowed("ttrwe"))), cql::CqlFloat(Some(15.1617))];
    response = try_test!(client.exec_prepared(&preps.id, params, cql::Consistency::One), "Error executing prepared statement");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "select * from rust.test";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One), "Error selecting from table test");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    println!("Execute batch");
    let params2: Vec<cql::CqlValue> = vec![cql::CqlVarchar(Some(Cow::Borrowed("batch2"))), cql::CqlFloat(Some(666.65))];
    let q_vec = vec![cql::QueryStr(Cow::Borrowed("insert into rust.test (id, f32) values ('batch1', 34.56)")),
                     cql::QueryPrepared(preps.id, params2)];
    response = try_test!(client.exec_batch(cql::BatchType::Logged, q_vec, cql::Consistency::One), "Error executing batch cql::Query");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "select * from rust.test";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One), "Error selecting from table test");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "create table rust.test2 (id text primary key, l list<int>, m map<int, text>, s set<float>)";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One), "Error creating table test2");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "insert into rust.test2 (id, l, m, s) values ('asdf', [1,2,3,4,5], {0: 'aa', 1: 'bbb', 3: 'cccc'}, {1.234, 2.345, 3.456, 4.567})";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One), "Error inserting into table test2");
    assert_response!(response);
    println!("Result: {:?} \n", response);

    q = "select * from rust.test2";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One), "Error selecting from table test2");
    assert_response!(response);
    println!("Result: {:?} \n", response);
}
