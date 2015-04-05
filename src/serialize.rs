extern crate std;
extern crate byteorder;

use std::iter::AdditiveIterator;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::borrow::IntoCow;
use self::byteorder::{ReadBytesExt, WriteBytesExt, BigEndian, LittleEndian, Error};
use std::raw::Slice;
use std::mem;

use super::def::*;
use super::def::CqlBytesSize::*;
use super::def::CqlRequestBody::*;
use super::def::RCErrorType::*;
use super::def::Query::*;
use super::def::CqlValue::*;
use super::client::Client;

// From
// http://stackoverflow.com/questions/26714984/rust-how-to-borrow-an-immutable-view-slice-to-a-vector-as-octets
fn view_as_bytes<'a, T: Copy>(items: &'a [T]) -> &'a [u8] {  // '
    let raw_items: Slice<T> = unsafe { mem::transmute(items) };
    let raw_bytes = Slice {
        data: raw_items.data as *const u8,
        len: raw_items.len * mem::size_of::<T>()
    };
    let bytes: &[u8] = unsafe { mem::transmute(raw_bytes) };
    bytes
}


pub trait CqlSerializable<'a> {
    fn len(&'a self, version: u8) -> usize;
    fn serialize_size<T: std::io::Write>(&'a self, buf: &mut T, bytes_size: CqlBytesSize, version: u8) -> RCResult<()>;
    fn serialize<T: std::io::Write>(&'a self, buf: &mut T, version: u8) -> RCResult<()>;
    fn serialize_with_client<T: std::io::Write>(&'a self, buf: &mut T, cl: &mut Client) -> RCResult<()> {
        self.serialize(buf, cl.version)
    }
    fn len_with_client(&'a self, cl: &mut Client) -> usize { 0 }
}

macro_rules! write_size(
    ($buf: ident, $size: expr, $bytes_size: ident) => {
        match $bytes_size {
            Cqli16 => try_bo!($buf.write_i16::<BigEndian>($size as i16), "Error serializing CqlValue (length of [short bytes])"),
            Cqli32 => try_bo!($buf.write_i32::<BigEndian>($size as i32), "Error serializing CqlValue (length of [bytes])"),
        }
    }
);

impl<'a> CqlSerializable<'a> for CqlPair {
    fn serialize_size<T: std::io::Write>(&'a self, buf: &mut T, bytes_size: CqlBytesSize, version: u8) -> RCResult<()> {
        try_bo!(buf.write_u16::<BigEndian>(self.key.len() as u16), "Error serializing CqlPair (key length)");
        try_io!(buf.write(self.key.as_bytes()), "Error serializing CqlPair (key)");
        try_bo!(buf.write_u16::<BigEndian>(self.value.len() as u16), "Error serializing CqlPair (value length)");
        try_io!(buf.write(self.value.as_bytes()), "Error serializing CqlPair (value)");
        Ok(())
    }
    fn serialize<T: std::io::Write>(&'a self, buf: &mut T, version: u8) -> RCResult<()> {
        self.serialize_size(buf, Cqli32, version)
    }


    fn len(&'a self, version: u8) -> usize {
        return 4 + self.key.len() + self.value.len();
    }
}


impl<'a> CqlSerializable<'a> for CqlStringMap {
    fn serialize_size<T: std::io::Write>(&'a self, buf: &mut T, bytes_size: CqlBytesSize, version: u8) -> RCResult<()> {
        try_bo!(buf.write_u16::<BigEndian>(self.pairs.len() as u16), "Error serializing CqlStringMap (length)");
        for pair in self.pairs.iter() {
            pair.serialize_size(buf, Cqli16, version);
        }
        Ok(())
    }
    fn serialize<T: std::io::Write>(&'a self, buf: &mut T, version: u8) -> RCResult<()> {
        self.serialize_size(buf, Cqli16, version)
    }


    fn len(&'a self, version: u8) -> usize {
        let mut len = 2usize;
        for pair in self.pairs.iter() {
            len += pair.len(version);
        }
        len
    }
}

fn serialize_header<T: std::io::Write>(buf: &mut T, version: &u8, flags: &u8, stream: &i8, opcode: &u8, len: &u32) -> RCResult<()> {
    try_bo!(buf.write_u8(*version), "Error serializing CqlRequest (version)");
    try_bo!(buf.write_u8(*flags), "Error serializing CqlRequest (flags)");
    try_bo!(buf.write_i8(*stream), "Error serializing CqlRequest (stream)");
    try_bo!(buf.write_u8(*opcode), "Error serializing CqlRequest (opcode)");
    try_bo!(buf.write_u32::<BigEndian>(*len), "Error serializing CqlRequest (length)");
    Ok(())
}

impl<'a> CqlSerializable<'a> for CqlRequest<'a> {
    fn serialize_with_client<T: std::io::Write>(&'a self, buf: &mut T, cl: &mut Client) -> RCResult<()> {
        match self.body {
            RequestExec(ref ps_id, ref params, ref cons, flags) => {
                let len = (self.len_with_client(cl)-8) as u32;
                let ocode = self.opcode as u8;
                serialize_header(buf, &cl.version, &self.flags, &self.stream, &ocode, &len);
                let version = self.version;
                let preps = match cl.get_prepared_statement(ps_id.as_slice()) {
                    Ok(ps) => ps,
                    Err(_) => return Err(RCError::new(format!("Unknown prepared statement <{}>", ps_id), GenericError))
                };

                try_bo!(buf.write_i16::<BigEndian>(preps.id.len() as i16), "Error serializing EXEC request (id length)");
                try_io!(buf.write(preps.id.as_slice()), "Error serializing EXEC request (id)");
                if version >= 2 {
                    try_bo!(buf.write_u16::<BigEndian>(*cons as u16), "Error serializing CqlRequest (query consistency)");
                    try_bo!(buf.write_u8(flags), "Error serializing CqlRequest (query flags)");

                    try_bo!(buf.write_i16::<BigEndian>(params.len() as i16), "Error serializing EXEC request (params length)");                
                    for v in params.iter() {
                        v.serialize_size(buf, Cqli32, version);
                    }
                } else {
                    try_bo!(buf.write_i16::<BigEndian>(params.len() as i16), "Error serializing EXEC request (params length)");                
                    for v in params.iter() {
                        v.serialize_size(buf, Cqli32, version);
                    }
                    try_bo!(buf.write_u16::<BigEndian>(*cons as u16), "Error serializing CqlRequest (query consistency)");
                }
                Ok(())
            },
            RequestBatch(ref q_vec, ref r_type, ref con, flags) => {
                let len = (self.len_with_client(cl)-8) as u32;
                let ocode = self.opcode as u8;
                serialize_header(buf, &cl.version, &self.flags, &self.stream, &ocode, &len);
                let version = self.version;

                try_bo!(buf.write_u8(*r_type as u8), "Error serializing BATCH request (request type)");
                try_bo!(buf.write_u16::<BigEndian>(q_vec.len() as u16), "Error serializing BATCH request (number of requests)");
                q_vec.iter().all(|r| { r.serialize_with_client(buf, cl); true });
                try_bo!(buf.write_u16::<BigEndian>(*con as u16), "Error serializing BATCH request (consistency)");
                Ok(())
            }
            _ => self.serialize(buf, cl.version)
        }
    }

    fn serialize_size<T: std::io::Write>(&'a self, buf: &mut T, bytes_size: CqlBytesSize, version: u8) -> RCResult<()> {
        Err(RCError::new("Cannot serialize REquest without Client context", WriteError))
    }

    fn serialize<T: std::io::Write>(&'a self, buf: &mut T, version: u8) -> RCResult<()> {
        match self.body {
            RequestExec(ref ps_id, ref params, ref cons, flags) => {
                Err(RCError::new("Cannot serialize a EXECUTE request without a Client object", WriteError))
            },
            _ => {             
                let len = (self.len(version)-8) as u32;
                let ocode = self.opcode as u8;
                serialize_header(buf, &version, &self.flags, &self.stream, &ocode, &len);

                match self.body {
                    RequestStartup(ref map) => {
                        map.serialize(buf, version)
                    },
                    RequestQuery(ref query_str, ref consistency, flags) => {
                        let len_str = query_str.len() as u32;
                        try_bo!(buf.write_u32::<BigEndian>(len_str), "Error serializing CqlRequest (query length)");
                        try_io!(buf.write(query_str.as_bytes()), "Error serializing CqlRequest (query)");
                        try_bo!(buf.write_u16::<BigEndian>(*consistency as u16), "Error serializing CqlRequest (query consistency)");
                        if version >= 2 {
                            try_bo!(buf.write_u8(flags), "Error serializing CqlRequest (query flags)");
                        }
                        Ok(())
                    },
                    RequestPrepare(ref query_str) => {
                        let len_str = query_str.len() as u32;
                        try_bo!(buf.write_u32::<BigEndian>(len_str), "Error serializing CqlRequest (query length)");
                        try_io!(buf.write(query_str.as_bytes()), "Error serializing CqlRequest (query)");
                        Ok(())               
                    },
                    _ => Ok(())
                }
            }
        }
    }

    fn len_with_client(&'a self, cl: &mut Client) -> usize {
        match self.body {
            RequestExec(ref ps_id, ref values, _, _) => {
                let version = self.version;
                let preps = match cl.get_prepared_statement(ps_id.as_slice()) {
                    Ok(ps) => ps,
                    Err(_) => return 0
                };

                let final_bytes = if version >= 2 { 3 } else { 2 };
                8 + 2 + preps.id.as_slice().len() + 2 + values.iter().map(|e| 4 + e.len(version)).sum() + final_bytes
            },
            RequestBatch(ref q_vec, ref r_type, ref con, flags) => {
                8 + 3 + q_vec.iter().map(|q| q.len_with_client(cl)).sum() + 2
            }
            _ => self.len(cl.version)
        }
    }

    fn len(&'a self, version: u8) -> usize {
        8 + match self.body {
            RequestStartup(ref map) => map.len(version),
            RequestQuery(ref query_str, _, _) => {
                let final_bytes = if version >= 2 { 3 } else { 2 };
                4 + query_str.len() + final_bytes
            },
            RequestPrepare(ref query_str) => 4 + query_str.len(),
            _ => 0
        }
    }
}

impl<'a> CqlSerializable<'a> for Query {
    fn serialize_size<T: std::io::Write>(&'a self, buf: &mut T, bytes_size: CqlBytesSize, version: u8) -> RCResult<()> {
        self.serialize(buf, version)
    }

    fn serialize<T: std::io::Write>(&'a self, buf: &mut T, version: u8) -> RCResult<()> {
        match *self {
            QueryStr(ref q_str) => {
                try_bo!(buf.write_u8(0u8), "Error serializing BATCH query (type)");
                write_size!(buf, q_str.len(), Cqli32);
                try_io!(buf.write(q_str.as_bytes()), "Error serializing BATCH query (query string)");
                try_bo!(buf.write_u16::<BigEndian>(0u16), "Error serializing BATCH query (values length)");
                Ok(())
            },
            _ => Err(RCError::new(" ad serialize query in BATH request", WriteError))
        }
    }

    fn serialize_with_client<T: std::io::Write>(&'a self, buf: &mut T, cl: &mut Client) -> RCResult<()> {
        match *self {
            QueryPrepared(ref p_name, ref values) => {
                let version = cl.version;
                let preps = match cl.get_prepared_statement(p_name.as_slice()) {
                    Ok(ps) => ps,
                    Err(_) => return Err(RCError::new(format!("Unknown prepared statement <{}>", p_name), GenericError))
                };

                try_bo!(buf.write_u8(1u8), "Error serializing BATCH prepared query (type)");
                write_size!(buf, preps.id.len(), Cqli16);
                try_io!(buf.write(preps.id.as_slice()), "Error serializing BATCH prepared query (id)");
                try_bo!(buf.write_u16::<BigEndian>(values.len() as u16), "Error serializing BATCH prepared query (values length)");
                values.iter().all(|v| { v.serialize(buf, version); true});

                Ok(())
            },
            _ => self.serialize(buf, cl.version)

        }
    }

    fn len_with_client(&'a self, cl: &mut Client) -> usize {
        match *self {
            QueryPrepared(ref p_name, ref values) => {
                let version = cl.version;
                let preps = match cl.get_prepared_statement(p_name.as_slice()) {
                    Ok(ps) => ps,
                    Err(_) => return 0
                };

                5 + preps.id.len() + values.iter().map(|e| 4 + e.len(version)).sum()
        },
        _ => self.len(cl.version)
        }
    }

    fn len(&'a self, version: u8) -> usize {
        match *self {
            QueryStr(ref q_str) => {
                7 + q_str.len()
            },
            _ => 0
        }
    }
}

impl<'a> CqlSerializable<'a> for CqlValue {
    fn serialize_size<T: std::io::Write>(&'a self, buf: &mut T, bytes_size: CqlBytesSize, version: u8) -> RCResult<()> {
        match *self {
            CqlASCII(ref o) => match *o {
                Some(ref s) => {
                    write_size!(buf, s.len(), bytes_size);
                    try_io!(buf.write(s.as_bytes()), "Error serializing CqlValue (ascci)");
                    Ok(())
                }
                None => Ok(())
            },
            CqlBigInt(ref o) => match *o {
                Some(ref i) => {
                    write_size!(buf, 8, bytes_size);
                    try_bo!(buf.write_i64::<BigEndian>(*i), "Error serializing CqlValue (Bigint)");
                    Ok(())
                }
                None => Ok(())                
            },
            CqlBlob(ref o) => match *o {
                Some(ref b) => {
                    write_size!(buf, b.len(), bytes_size);
                    try_io!(buf.write(b.as_slice()), "Error serializing CqlValue (Blob)");
                    Ok(())
                }
                None => Ok(())                
            },
            CqlBoolean(ref o) => match *o {
                Some(ref b) => {
                    write_size!(buf, 1, bytes_size);
                    try_bo!(buf.write_u8(*b as u8), "Error serializing CqlValue (Boolean)");
                    Ok(())
                }
                None => Ok(())                
            },
            CqlCounter(ref o) => match *o {
                Some(ref c) => {
                    write_size!(buf, 8, bytes_size);
                    try_bo!(buf.write_i64::<BigEndian>(*c), "Error serializing CqlValue (Counter)");
                    Ok(())
                }
                None => Ok(())                
            },
            CqlDecimal(_) => Err(RCError::new("Decimal seralization not implemented", SerializeError)),
            CqlDouble(ref o) => match *o {
                Some(ref d) => {
                    write_size!(buf, 8, bytes_size);
                    try_bo!(buf.write_f64::<BigEndian>(*d), "Error serializing CqlValue (Double)");
                    Ok(())
                }
                None => Ok(())                
            },
            CqlFloat(ref o) => match *o {
                Some(ref f) => {
                    write_size!(buf, 4, bytes_size);
                    try_bo!(buf.write_f32::<BigEndian>(*f), "Error serializing CqlValue (Float)");
                    Ok(())
                }
                None => Ok(())                
            },
            CqlInet(ref o) => match *o {
                Some(ref ip) => match *ip {
                    IpAddr::Ipv4(ref ipv4) => {
                        write_size!(buf, 5, bytes_size);
                        try_bo!(buf.write_u8(4), "Error serializing CqlValue (Ipv4Addr size)");
                        try_io!(buf.write(ipv4.octets().as_slice()), "Error serializing CqlValue (Ipv4Addr)");
                        Ok(())
                    },
                    IpAddr::Ipv6(ref ipv6) => {
                        write_size!(buf, 17, bytes_size);
                        try_bo!(buf.write_u8(16u8), "Error serializing CqlValue (Ipv4Addr size)");
                        try_io!(buf.write(view_as_bytes(ipv6.segments().as_slice())), "Error serializing CqlValue (Ipv4Addr)");
                        Ok(())
                    },
                },
                None => Ok(())
            },
            CqlInt(ref o) => match *o {
                Some(ref i) => {
                    write_size!(buf, std::mem::size_of::<i32>(), bytes_size);
                    try_bo!(buf.write_i32::<BigEndian>(*i), "Error serializing CqlValue (Int)");
                    Ok(())
                }
                None => Ok(())                
            },
            CqlList(ref o) => match *o {
                Some(ref v) => {
                    let len = v.len();
                    try_bo!(buf.write_i32::<BigEndian>(len as i32), "Error serializing CqlValue (List length)");
                    v.iter().map(|e| e.serialize_size(buf, Cqli16, version));
                    Ok(())
                },
                None => Ok(())
            },
            CqlMap(ref o) => match *o {
                Some(ref v) => {
                    let len = v.len();
                    try_bo!(buf.write_i32::<BigEndian>(len as i32), "Error serializing CqlValue (Map length)");
                    v.iter().map(|e| e.serialize_size(buf, Cqli16, version));
                    Ok(())
                },
                None => Ok(())
            },
            CqlSet(ref o) => match *o {
                Some(ref v) => {
                    let len = v.len();
                    try_bo!(buf.write_i32::<BigEndian>(len as i32), "Error serializing CqlValue (Set length)");
                    v.iter().map(|e| e.serialize_size(buf, Cqli16, version));
                    Ok(())
                },
                None => Ok(())
            },
            CqlText(ref o) => match *o {
                Some(ref s) => {
                    write_size!(buf, s.len(), bytes_size);
                    try_io!(buf.write(s.as_bytes()), "Error serializing CqlValue (Text)");
                    Ok(())
                }
                None => Ok(())
            },
            CqlTimestamp(ref o) => match *o {
                Some(ref i) => {
                    write_size!(buf, 8, bytes_size);
                    try_bo!(buf.write_u64::<BigEndian>(*i), "Error serializing CqlValue (Counter)");
                    Ok(())
                }
                None => Ok(())                
            },
            CqlUuid(ref o) => match *o {
                Some(ref u) => {
                    write_size!(buf, u.as_bytes().len(), bytes_size);
                    try_io!(buf.write(u.as_bytes()), "Error serializing CqlValue (Uuid)");
                    Ok(())
                }
                None => Ok(())                
            },
            CqlTimeUuid(ref o) => match *o {
                Some(ref u) => {
                    write_size!(buf, u.as_bytes().len(), bytes_size);
                    try_io!(buf.write(u.as_bytes()), "Error serializing CqlValue (TimeUuid)");
                    Ok(())
                }
                None => Ok(())                
            },
            CqlVarchar(ref o) => match *o {
                Some(ref s) => {
                    write_size!(buf, s.len(), bytes_size);
                    try_io!(buf.write(s.as_bytes()), "Error serializing CqlValue (Varchar)");
                    Ok(())
                }
                None => Ok(())
            },
            CqlVarint(_) => Err(RCError::new("Varint seralization not implemented", SerializeError)),
            _ => Err(RCError::new("Error serializing CqlValue (no", SerializeError))
        }

    }

    fn serialize<T: std::io::Write>(&'a self, buf: &mut T, version: u8) -> RCResult<()> {
        self.serialize_size(buf, Cqli32, version)
    }

    fn len(&'a self, version: u8) -> usize {
        match self {
            &CqlASCII(ref o) => match *o {
                Some(ref s) => s.len() as usize,
                None => 0
            },
            &CqlBigInt(ref o) => match *o {
                Some(_) => std::mem::size_of::<i64>(),
                None => 0     
            },
            &CqlBlob(ref o) => match *o {
                Some(ref b) => b.len() as usize,
                None => 0               
            },
            &CqlBoolean(ref o) => match *o {
                Some(_) => std::mem::size_of::<u8>(),
                None => 0                
            },
            &CqlCounter(ref o) => match *o {
                Some(_) => std::mem::size_of::<i64>(),
                None => 0     
            },
            &CqlDecimal(_) => 0,
            &CqlDouble(ref o) => match *o {
                Some(_) => std::mem::size_of::<f64>(),
                None => 0     
            },
            &CqlFloat(ref o) => match *o {
                Some(_) => std::mem::size_of::<f32>(),
                None => 0     
            },
            &CqlInet(ref o) => match *o {
                Some(ref ip) => match *ip {
                    IpAddr::Ipv4(_) => 5,
                    IpAddr::Ipv6(_) => 17
                },
                None => 0
            },
            &CqlInt(ref o) => match *o {
                Some(_) => std::mem::size_of::<i32>(),
                None => 0     
            },
            &CqlList(ref o) => match *o {
                Some(ref v) => {
                    if v.len() == 0 { 0 }
                    else {
                        // Lists contain [short bytes] elements, hence the 2
                        v.len() * (2 + v[0].len(version))
                    }                   
                },
                None => 0
            },
            &CqlMap(ref o) => match *o {
                Some(ref v) => {
                    if v.len() == 0 { 0 }
                    else {
                        // Maps contain [short bytes] elements, hence the 2
                        v.len() * (2 + v[0].len(version))
                    }                   
                },
                None => 0
            },
            &CqlSet(ref o) => match *o {
                Some(ref v) => {
                    if v.len() == 0 { 0 }
                    else {
                        // Sets contain [short bytes] elements, hence the 2
                        v.len() * (2 + v[0].len(version))
                    }                   
                },
                None => 0
            },
            &CqlTimestamp(ref o) => match *o {
                Some(_) => std::mem::size_of::<u64>(),
                None => 0     
            },
            &CqlUuid(ref o) => match *o {
                Some(ref u) => u.as_bytes().len(),
                None => 0     
            },
            &CqlTimeUuid(ref o) => match *o {
                Some(ref u) => u.as_bytes().len(),
                None => 0     
            },
            &CqlVarchar(ref o) => match *o {
                Some(ref s) => s.as_slice().len() as usize,
                None => 0
            },
            &CqlVarint(_) => 0,
            _ => 0
        }
    }

}


impl<'a, T:CqlSerializable<'a>, V:CqlSerializable<'a>> CqlSerializable<'a> for Pair<T, V> {
    fn serialize_size<S: std::io::Write>(&'a self, buf: &mut S, bytes_size: CqlBytesSize, version: u8) -> RCResult<()> {
        self.key.serialize_size(buf, bytes_size, version);
        self.value.serialize_size(buf, bytes_size, version);
        Ok(())
    }

    fn serialize<S: std::io::Write>(&'a self, buf: &mut S, version: u8) -> RCResult<()> {
        self.serialize_size(buf, Cqli32, version)
    }

    fn len(&'a self, version: u8) -> usize {
        0
    }
}

