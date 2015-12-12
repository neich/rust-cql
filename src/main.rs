extern crate cql;
extern crate eventual;
extern crate mio;


use std::borrow::Cow;
use std::io::Write;
use eventual::*;
use std::thread;
use mio::{EventLoop, Handler};

struct MyHandler;

struct MyMsg {
    data: u32,
    tx: eventual::Complete<u32, &'static str>
}

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

impl Handler for MyHandler {
    type Timeout = ();
    type Message = MyMsg;

    fn notify(&mut self, event_loop: &mut EventLoop<MyHandler>, msg: MyMsg) {
        println!("data in notify = {}", msg.data);
        thread::sleep_ms(4000);
        msg.tx.complete(msg.data + 1);
        //event_loop.shutdown();
    }
}

fn test_eventloop() {
    let mut event_loop = EventLoop::new().unwrap();
    let sender1 = event_loop.channel();
    let sender2 = sender1.clone();

    let (tx1, future1) = Future::<u32, &'static str>::pair();
    future1.receive(|data| {
        println!("data in future = {}", data.unwrap());
    });
    let (tx2, future2) = Future::<u32, &'static str>::pair();
    future2.receive(|data| {
        println!("data in future = {}", data.unwrap());
    });

    // Send the notification from another thread
    thread::spawn(move || { sender1.send(MyMsg { data: 11, tx: tx1}); });
    println!("Sending second send ...");
    thread::spawn(move || { sender2.send(MyMsg { data: 564, tx: tx2}); });
    println!("Hello there from a thread!");
    thread::spawn(move || {event_loop.run(&mut MyHandler)});
    println!("Hello there!");
}

fn main() {
    test_async_client();
}

fn test_async_client() {

    println!("Connecting ...!");
    //let mut client = try_test!(cql::connect("127.0.0.1",9042, None), "Error connecting to server at 127.0.0.1:9042");
    let mut client = try_test!(cql::connect("127.0.0.1:9042".parse().ok().expect("Couldn't parse address"),None), "Error connecting to server at 127.0.0.1:9042");
    println!("Connected with CQL binary version v{}", client.version);
    /*
    let mut q = "select * from rust.test";
    println!("cql::Query: {}", q);
    let mut future_response = client.async_exec_query(q, cql::Consistency::One);
    let mut response = try_test!(future_response.await().unwrap(), "Error selecting from table test");
    //assert_response!(response);
    println!("Result: {:?} \n", response);
    
    q = "insert into rust.test (id, f32) values (?, ?)";
    println!("Create prepared: {}", q);
    let res_id = try_test!(client.prepared_statement(q, "test"), "Error creating prepared statement");
    println!("Execute prepared");


    let params: &Vec<cql::CqlValue> = &vec![cql::CqlVarchar(Some(Cow::Borrowed("ttrwe"))), cql::CqlFloat(Some(15.1617))];
    future_response = client.async_exec_prepared("test", params, cql::Consistency::One);
    
    response = try_test!(future_response.await().unwrap(),"Error executing prepared statement");
    //assert_response!(response);
    println!("Result: {:?} \n", response);
    
    println!("Execute batch");
    let params2: Vec<cql::CqlValue> = vec![cql::CqlVarchar(Some(Cow::Borrowed("batch2"))), cql::CqlFloat(Some(666.65))];
    let q_vec = vec![cql::QueryStr(Cow::Borrowed("insert into rust.test (id, f32) values ('batch1', 34.56)")),
                     cql::QueryPrepared(Cow::Borrowed("test"), params2)];
    future_response = client.async_exec_batch(cql::BatchType::Logged, q_vec, cql::Consistency::One);
    response = try_test!(future_response.await().unwrap(), "Error executing batch cql::Query");
    //assert_response!(response);
    println!("Result: {:?} \n", response);
    */


    /*
    q = "insert into rust.test (id, f32) values (?, ?)";
    println!("Create prepared: {}", q);
    let res_id = try_test!(client.prepared_statement(q, "test"), "Error creating prepared statement");
    println!("Execute prepared");


    let params: &Vec<cql::CqlValue> = &vec![cql::CqlVarchar(Some(Cow::Borrowed("ttrwe"))), cql::CqlFloat(Some(15.1617))];
    future_response = client.exec_prepared("test", params, cql::Consistency::One);
    
    response = try_test!(future_response.await().unwrap(),"Error executing prepared statement");
    //assert_response!(response);
    println!("Result: {:?} \n", response);
    */
    //q = "select * from rust.test";
    //println!("cql::Query: {}", q);
    //response = try_test!(client.exec_query(q, cql::Consistency::One), "Error selecting from table test");
    //assert_response!(response);
    //println!("Result: {:?} \n", response);

}

fn test_sync_client() {
    println!("Connecting ...!");
    //let mut client = try_test!(cql::connect("127.0.0.1:9042".parse().ok().expect("Couldn't parse address"), None), "Error connecting to server at 127.0.0.1:9042");
    //println!("Connected with CQL binary version v{}", client.version);

    /*
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
    */
    /*
    let mut q = "select * from rust.test";
    println!("cql::Query: {}", q);
    let mut response = try_test!(client.exec_query(q, cql::Consistency::One), "Error selecting from table test");
    assert_response!(response);
    println!("Result: {:?} \n", response);
    
    q = "insert into rust.test (id, f32) values (?, ?)";
    println!("Create prepared: {}", q);
    let res_id = try_test!(client.prepared_statement(q, "test"), "Error creating prepared statement");

    println!("Execute prepared");
    let params: &Vec<cql::CqlValue> = &vec![cql::CqlVarchar(Some(Cow::Borrowed("ttrwe"))), cql::CqlFloat(Some(15.1617))];
    response = try_test!(client.exec_prepared("test", params, cql::Consistency::One), "Error executing prepared statement");
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
                     cql::QueryPrepared(Cow::Borrowed("test"), params2)];
    response = try_test!(client.exec_batch(cql::BatchType::Logged, q_vec, cql::Consistency::One), "Error executing batch cql::Query");
    assert_response!(response);
    println!("Result: {:?} \n", response);
    */
/*
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
    */
}