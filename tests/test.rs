
#![feature(macro_rules)]

extern crate "rust-cql" as cql;

macro_rules! assert_response(
    ($resp:expr) => (
        if match $resp.opcode { cql::OpcodeResponse::OpcodeError => true, _ => false } {
            panic!("Assertion failed: {}",
                match $resp.body { cql::CqlResponseBody::ResponseError(_, message) => message, _ => "Ooops!".into_cow()});
        }
    );
)

mod test_cql;