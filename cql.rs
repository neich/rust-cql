extern crate cql;

use std::str::Slice;

fn main() {
    println!("Connecting ...!");
    let mut client = match cql::connect("127.0.0.1", 9042, None) {
        Ok(c) => c,
        Err(e) => fail!(format!("Failed to connect: {}", e.desc))
    };
    println!("Connected with CQL binary version v{}", client.version);

    let mut q = "create keyspace rust with replication = {'class': 'SimpleStrategy', 'replication_factor':1}";
    println!("Query: {}", q);
    let mut res = client.exec_query(q, cql::Consistency::One);
    println!("Result: {} \n", res);

    q = "create table rust.test (id text primary key, f32 float, f64 double, i32 int, i64 bigint, b boolean, ip inet)";
    println!("Query: {}", q);
    res = client.exec_query(q, cql::Consistency::One);
    println!("Result: {} \n", res);

    q = "insert into rust.test (id, f32, f64, i32, i64, b, ip) values ('asdf', 1.2345, 3.14159, 47, 59, true, '127.0.0.1')";
    println!("Query: {}", q);
    res = client.exec_query(q, cql::Consistency::One);
    println!("Result: {} \n", res);

    q = "select * from rust.test";
    println!("Query: {}", q);
    res = client.exec_query(q, cql::Consistency::One);
    println!("Result: {} \n", res);

    q = "insert into rust.test (id, f32) values (?, ?)";
    println!("Create prepared: {}", q);
    let res_id = client.prepared_statement(q, "test");
    println!("Result prepared: {} \n", res);

    if res_id.is_err() {
        fail!("Error in creating prepared statement")
    }

    println!("Execute prepared");
    let params: &[cql::CqlValue] = &[cql::CqlVarchar(Some(Slice("ttrwe"))), cql::CqlFloat(Some(15.1617))];
    res = client.exec_prepared("test", params, cql::Consistency::One);
    println!("Result: {} \n", res);

    q = "select * from rust.test";
    println!("Query: {}", q);
    res = client.exec_query(q, cql::Consistency::One);
    println!("Result: {} \n", res);

    q = "create table rust.test2 (id text primary key, l list<int>, m map<int, text>, s set<float>)";
    println!("Query: {}", q);
    res = client.exec_query(q, cql::Consistency::One);
    println!("Result: {} \n", res);

    q = "insert into rust.test2 (id, l, m, s) values ('asdf', [1,2,3,4,5], {0: 'aa', 1: 'bbb', 3: 'cccc'}, {1.234, 2.345, 3.456, 4.567})";
    println!("Query: {}", q);
    res = client.exec_query(q, cql::Consistency::One);
    println!("Result: {} \n", res);

    q = "select * from rust.test2";
    println!("Query: {}", q);
    res = client.exec_query(q, cql::Consistency::One);
    println!("Result: {} \n", res);
}
