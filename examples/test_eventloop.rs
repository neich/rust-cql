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

pub fn to_hex_string(bytes: &Vec<u8>) -> String {
  let strs: Vec<String> = bytes.iter()
                               .map(|b| format!("{:02X}", b))
                               .collect();
  strs.connect(" ")
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
    test_eventloop();
}
