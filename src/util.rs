

use std::time::Duration;
use std::sync::mpsc::{Receiver,Sender, channel};
use std::thread;
use std::thread::{Builder,sleep};
use def::*;




//---------------------------------------------------------


//Auxiliar functions
pub fn to_hex_string(bytes: &Vec<u8>) -> String {
  let strs: Vec<String> = bytes.iter()
                               .map(|b| format!("{:02X}", b))
                               .collect();
  strs.connect(" ")
}

pub fn max_stream_id(stream_id: i16,version: u8) -> bool{
    (stream_id as i32 >= CQL_MAX_STREAM_ID_V1_V2 as i32 && (version == 1 || version == 2))
      || (stream_id as i32 == CQL_MAX_STREAM_ID_V3 as i32 && version == 3)
}

pub fn set_interval<F>(delay: Duration,f: F) -> Sender<()>
    where F: Fn(), F: Send + 'static + Sync{

    let (tx, rx) = channel::<(())>();
    thread::Builder::new().name("tick".to_string()).spawn(move || {
        while !rx.try_recv().is_ok() {
            sleep(delay);
            f();  //Do stuff here
        }
    }).unwrap();
    tx
}