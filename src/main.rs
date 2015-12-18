extern crate cql;
extern crate eventual;
extern crate mio;


use std::borrow::Cow;
use std::io::Write;
use self::eventual::{Future,Async};
use self::mio::*;
use self::mio::tcp::TcpStream;
use self::mio::util::Slab;
use self::mio::buf::{ByteBuf,MutByteBuf};
use std::{mem, str};
use std::io::Cursor;
use std::net::SocketAddr;
use std::collections::BTreeMap;
use std::error::Error;
use std::thread;
use std::sync::mpsc::channel;

struct MyHandler{   //MyMsg/CqlMsg
    val1: u32,
    val2: char,
    val3: bool,
    socket: TcpStream,
    token: mio::Token,
    pending: Vec<u32>,
    pending_complete: Vec<eventual::Complete<u32, ()>>
}

struct MyMsg {
    data: u32,
    tx: eventual::Complete<u32, ()>,
    token: mio::Token
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
        //self.val1 = msg.data;
        self.pending.push(msg.data);
        self.pending_complete.push(msg.tx);
        event_loop.reregister(&self.socket,
            self.token,
            mio::EventSet::writable(),
            mio::PollOpt::oneshot());
        //self.ready(event_loop,msg.token,mio::EventSet::readable());
        //println!("[notify] data in self(MyHandle) val1 = {}, val2 = {}, val3 = {} ", self.val1,self.val2,self.val3);
        //thread::sleep_ms(2000);
        //msg.tx.complete(msg.data + 1);
        //event_loop.shutdown();
    }

    fn ready(&mut self, event_loop: &mut mio::EventLoop<MyHandler>, token: mio::Token, events: mio::EventSet) {
        println!("[ready] data in self(MyHandle) val1 = {}, val2 = {}, val3 = {} {:?}", self.val1,self.val2,self.val3,token);
        while self.pending.len()>0 {
            //println!("{:?}",);
            self.pending_complete.pop().unwrap().complete(self.pending.pop().unwrap()+1); //It should be the first one
            event_loop.reregister(&self.socket, token, mio::EventSet::readable(),mio::PollOpt::edge() | mio::PollOpt::oneshot())
            .unwrap();
        }
    }
}

fn test_eventloop() {
    let mut event_loop = EventLoop::new().unwrap();
    let address = "127.0.0.1:9042".parse().ok().expect("Couldn't parse address");
    let socket = TcpStream::connect(&address).unwrap();
    let token = Token(1);
    let sender1 = event_loop.channel();
    let sender2 = sender1.clone();
    event_loop.register_opt(
        &socket,
        token,
        mio::EventSet::writable(),
        mio::PollOpt::edge() | mio::PollOpt::oneshot()).unwrap();
    let (tx1, future1) = Future::<u32, ()>::pair();
    future1.receive(|data| {
        println!("data in future = {}", data.unwrap());
    });
    let (tx2, future2) = Future::<u32, ()>::pair();

    future2.receive(|data| {
        println!("data in future = {}", data.unwrap());
    });
    println!("Sending first data ...");
    // Send the notification from another thread
    sender1.send(MyMsg { data: 11, tx: tx1,token:Token(1)});
    thread::spawn(move || {event_loop.run(&mut MyHandler{val1:1,val2:'a',val3:false,socket:socket,token:token,pending: Vec::new(),pending_complete: Vec::new()})});
    //thread::sleep_ms(2000);

    println!("Sending second data ...");
    sender2.send(MyMsg { data: 564, tx: tx2,token:Token(2)});
    println!("Hello there from a thread!");
    
    //thread::spawn(move || {event_loop.run(&mut MyHandler{val1:1,val2:'a',val3:false,socket:socket,token:Token(2)})});
    thread::sleep_ms(20000);
    println!("Hello there!");
}

fn main() {
    //test_eventloop();
    test_async_client();
}

fn test_paralelism(){
    let mut client = try_test!(cql::connect("127.0.0.1:9042".parse().ok().expect("Couldn't parse address"),None), "Error connecting to server at 127.0.0.1:9042");
    println!("Connected with CQL binary version v{}", client.version);
}

fn test_async_client() {

    println!("Connecting ...!");
    //let mut client = try_test!(cql::connect("127.0.0.1",9042, None), "Error connecting to server at 127.0.0.1:9042");
    let mut client = try_test!(cql::connect("127.0.0.1:9042".parse().ok().expect("Couldn't parse address"),None), "Error connecting to server at 127.0.0.1:9042");
    println!("Connected with CQL binary version v{}", client.version);
    
    let mut q = "select * from rust.test";
    println!("cql::Query: {}", q);
    let mut future_response = client.async_exec_query(q, cql::Consistency::One);
    let mut response = try_test!(future_response.await().unwrap(), "Error selecting from table test");
    assert_response!(response);
    println!("Result: {:?} \n", response);
    
    q = "insert into rust.test (id, f32) values (?, ?)";
    println!("Create prepared: {}", q);
    let preps = try_test!(client.prepared_statement(q), "Error creating prepared statement");
    println!("Created prepared with id = {}", to_hex_string(&preps.id));


    let params: &Vec<cql::CqlValue> = &vec![cql::CqlVarchar(Some(Cow::Borrowed("ttrwe"))), cql::CqlFloat(Some(15.1617))];
    future_response = client.async_exec_prepared(&preps.id, params, cql::Consistency::One);
    

    response = try_test!(future_response.await().unwrap(),"Error executing prepared statement");
    assert_response!(response);
    println!("Result: {:?} \n", response);
    
    println!("Execute batch");
    let params2: Vec<cql::CqlValue> = vec![cql::CqlVarchar(Some(Cow::Borrowed("batch2"))), cql::CqlFloat(Some(666.65))];
    let q_vec = vec![cql::QueryStr(Cow::Borrowed("insert into rust.test (id, f32) values ('batch1', 34.56)")),
                     cql::QueryPrepared(preps.id, params2)];
    future_response = client.async_exec_batch(cql::BatchType::Logged, q_vec, cql::Consistency::One);
    response = try_test!(future_response.await().unwrap(), "Error executing batch cql::Query");
    assert_response!(response);
    println!("Result: {:?} \n", response);
    


    //q = "select * from rust.test";
    //println!("cql::Query: {}", q);
    //response = try_test!(client.exec_query(q, cql::Consistency::One), "Error selecting from table test");
    //assert_response!(response);
    //println!("Result: {:?} \n", response);

}
pub fn to_hex_string(bytes: &Vec<u8>) -> String {
  let strs: Vec<String> = bytes.iter()
                               .map(|b| format!("{:02X}", b))
                               .collect();
  strs.connect(" ")
}

fn test_sync_client() {
    println!("Connecting ...!");
    let mut client = try_test!(cql::connect("127.0.0.1:9042".parse().ok().expect("Couldn't parse address"), None), "Error connecting to server at 127.0.0.1:9042");
    println!("Connected with CQL binary version v{}", client.version);

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
    */
    /*
    let mut q = "insert into rust.test (id, f32, f64, i32, i64, b, ip) values ('asdf', 1.2345, 3.14159, 47, 59, true, '127.0.0.1')";
    println!("cql::Query: {}", q);
    let mut response = try_test!(client.exec_query(q, cql::Consistency::One), "Error inserting into table test");
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
    let params: &Vec<cql::CqlValue> = &vec![cql::CqlVarchar(Some(Cow::Borrowed("ttrwe"))), cql::CqlFloat(Some(15.1617))];
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
    */
    /*
    q = "create table rust.test2 (id text primary key, l list<int>, m map<int, text>, s set<float>)";
    println!("cql::Query: {}", q);
    response = try_test!(client.exec_query(q, cql::Consistency::One), "Error creating table test2");
    assert_response!(response);
    println!("Result: {:?} \n", response);
    */
    /*
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