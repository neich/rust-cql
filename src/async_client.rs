extern crate std;
extern crate num;
extern crate uuid;
extern crate eventual;

use std::collections::BTreeMap;
use std::borrow::Cow;

use super::def::*;
use super::def::OpcodeRequest::*;
use super::def::CqlRequestBody::*;
use super::def::RCErrorType::*;
use super::def::CqlResponseBody::*;
use super::serialize::CqlSerializable;
use super::reader::*;
use super::client::*;


pub struct Async_client {
    client: Client
}

impl Async_client{
	/*
	pub fn connect(&mut self,ip: &'static str, port: u16, creds:Option<&Vec<CowStr>>) -> eventual::Future<RCResult<Client>,()>{
		return eventual::Future::spawn(|| {connect(ip, port, creds)});
	}
	*/	
	fn new(client:Client) -> Async_client{
		Async_client{client: client}
	}

	pub fn connect(&mut self,ip: &'static str, port: u16, creds:Option<&Vec<CowStr>>) -> RCResult<Async_client>{
		let client = connect(ip, port, creds);

		match client{
			Ok(unwrapped_client) => Ok(Async_client::new(unwrapped_client)),
			Err(e) => Err(RCError::new("Unable to connect with Async_client", ReadError))
		}
	}

	/*
	pub fn exec_query(& mut self,query_str: & str, con: Consistency) ->  eventual::Future<RCResult<CqlResponse>,()>{
		eventual::Future::spawn(||{
			self.client.exec_query(query_str,con)
		});
	}
	*/
	
	/* Same as above function with lifetimes (it doesn't compile though)
	pub fn exec_query<'a>(&'a mut self,query_str: &'a str, con: Consistency) ->  eventual::Future<RCResult<CqlResponse<'a>>,()>{
		eventual::Future::spawn(||{
			self.client.exec_query(query_str,con)
		});
	}
	*/
	
	/*
	pub fn exec_prepared<'a>(&'a mut self, ps_id: CowStr, params: &'a [CqlValue], con: Consistency)  -> eventual::Future<RCResult<CqlResponse>,()> {
		eventual::Future::spawn(move ||{
			self.client.exec_prepared(ps_id,params,con)
		});
	}
*/
}