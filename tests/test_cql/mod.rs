
extern crate "rust-cql" as cql;

#[test]
fn test() {
    println!("Connecting ...!");
    let c = cql::connect("127.0.0.1", 9042, None);
    assert!(c.is_ok(), "Error connecting to server at 127.0.0.1:9042");
    let mut client = c.unwrap();
    println!("Connected with CQL binary version v{}", client.version);

    let mut q = "create keyspace rust with replication = {'class': 'SimpleStrategy', 'replication_factor':1}";
    println!("Query: {}", q);
    let mut res = client.exec_query(q, cql::Consistency::One);
    assert!(res.is_ok(), "Error creating keyspace");
    let mut response = res.unwrap();
    assert_response!(response);
    println!("Result: {} \n", response);

    q = "create table rust.test (id text primary key, f32 float, f64 double, i32 int, i64 bigint, b boolean, ip inet)";
    println!("Query: {}", q);
    res = client.exec_query(q, cql::Consistency::One);
    assert!(res.is_ok(), "Error creating table rust.test");
    response = res.unwrap();
    assert_response!(response);
    println!("Result: {} \n", response);

    q = "insert into rust.test (id, f32, f64, i32, i64, b, ip) values ('asdf', 1.2345, 3.14159, 47, 59, true, '127.0.0.1')";
    println!("Query: {}", q);
    res = client.exec_query(q, cql::Consistency::One);
    assert!(res.is_ok(), "Error inserting into table test");
    response = res.unwrap();
    assert_response!(response);
    println!("Result: {} \n", response);

    q = "select * from rust.test";
    println!("Query: {}", q);
    res = client.exec_query(q, cql::Consistency::One);
    assert!(res.is_ok(), "Error selecting from table test");
    response = res.unwrap();
    assert_response!(response);
    println!("Result: {} \n", response);

    q = "insert into rust.test (id, f32) values (?, ?)";
    println!("Create prepared: {}", q);
    let res_id = client.prepared_statement(q, "test");
    assert!(res_id.is_ok(), "Error creating prepared statement");

    println!("Execute prepared");
    let params: &[cql::CqlValue] = &[cql::CqlVarchar(Some("ttrwe".into_cow())), cql::CqlFloat(Some(15.1617))];
    res = client.exec_prepared("test".into_cow(), params, cql::Consistency::One);
    assert!(res.is_ok(), "Error executing prepared statement");
    response = res.unwrap();
    assert_response!(response);
    println!("Result: {} \n", response);

    q = "select * from rust.test";
    println!("Query: {}", q);
    res = client.exec_query(q, cql::Consistency::One);
    assert!(res.is_ok(), "Error selecting from table test");
    response = res.unwrap();
    assert_response!(response);
    println!("Result: {} \n", response);

    println!("Execute batch");
    let params2: Vec<cql::CqlValue> = vec![cql::CqlVarchar(Some("batch2".into_cow())), cql::CqlFloat(Some(666.65))];
    let q_vec = vec![cql::QueryStr("insert into rust.test (id, f32) values ('batch1', 34.56)".into_cow()),
                     cql::QueryPrepared("test".into_cow(), params2)];
    res = client.exec_batch(cql::BatchType::Logged, q_vec, cql::Consistency::One);
    assert!(res.is_ok(), "Error executing batch query");
    response = res.unwrap();
    assert_response!(response);
    println!("Result: {} \n", response);

    q = "select * from rust.test";
    println!("Query: {}", q);
    res = client.exec_query(q, cql::Consistency::One);
    assert!(res.is_ok(), "Error selecting from table test");
    response = res.unwrap();
    assert_response!(response);
    println!("Result: {} \n", response);

    q = "create table rust.test2 (id text primary key, l list<int>, m map<int, text>, s set<float>)";
    println!("Query: {}", q);
    res = client.exec_query(q, cql::Consistency::One);
    assert!(res.is_ok(), "Error creating table test2");
    response = res.unwrap();
    assert_response!(response);
    println!("Result: {} \n", response);

    q = "insert into rust.test2 (id, l, m, s) values ('asdf', [1,2,3,4,5], {0: 'aa', 1: 'bbb', 3: 'cccc'}, {1.234, 2.345, 3.456, 4.567})";
    println!("Query: {}", q);
    res = client.exec_query(q, cql::Consistency::One);
    assert!(res.is_ok(), "Error inserting into table test2");
    response = res.unwrap();
    assert_response!(response);
    println!("Result: {} \n", response);

    q = "select * from rust.test2";
    println!("Query: {}", q);
    res = client.exec_query(q, cql::Consistency::One);
    assert!(res.is_ok(), "Error selecting from table test2");
    response = res.unwrap();
    assert_response!(response);
    println!("Result: {} \n", response);
}
