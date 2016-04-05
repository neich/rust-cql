extern crate uuid;
extern crate std;
extern crate byteorder;
extern crate num;

use def::*;
use def::CqlValue::*;
use def::CqlValueType::*;
use def::CqlResponseBody::*;
use def::CqlEvent::*;
use def::RCErrorType::*;
use def::CqlBytesSize::*;
use def::KindResult::*;
use def::OpcodeResponse::*;

use self::uuid::Uuid;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::borrow::{Cow, ToOwned};
use std::io::{Read, Write, Cursor};
use self::byteorder::{ReadBytesExt, BigEndian, LittleEndian};
use std::mem::size_of;
use std::path::Path;
use std::error::Error;
use ep::FromPrimitive;

pub trait CqlReader {
    fn read_cql_bytes(&mut self, val_type: CqlBytesSize) -> RCResult<Vec<u8>>;
    fn read_cql_bytes_length(&mut self, val_type: CqlBytesSize) -> RCResult<i32>;
    fn read_cql_bytes_length_fixed(&mut self, val_type: CqlBytesSize, length: i32) -> RCResult<i32>;

    fn read_cql_str(&mut self, val_type: CqlBytesSize) -> RCResult<Option<CowStr>>;
    fn read_cql_f32(&mut self, val_type: CqlBytesSize) -> RCResult<Option<f32>>;
    fn read_cql_f64(&mut self, val_type: CqlBytesSize) -> RCResult<Option<f64>>;
    fn read_cql_i32(&mut self, val_type: CqlBytesSize) -> RCResult<Option<i32>>;
    fn read_cql_i64(&mut self, val_type: CqlBytesSize) -> RCResult<Option<i64>>;
    fn read_cql_u64(&mut self, val_type: CqlBytesSize) -> RCResult<Option<u64>>;
    fn read_cql_blob(&mut self, val_type: CqlBytesSize) -> RCResult<Option<Vec<u8>>>;
    fn read_cql_boolean(&mut self, val_type: CqlBytesSize) -> RCResult<Option<bool>>;
    fn read_cql_uuid(&mut self, val_type: CqlBytesSize) -> RCResult<Option<Uuid>>;
    fn read_cql_inet(&mut self, val_type: CqlBytesSize) -> RCResult<Option<IpAddress>>;
    fn read_cql_event(&mut self, val_type: CqlBytesSize) -> RCResult<CqlEvent>;

    fn read_cql_list(&mut self, col_meta: &CqlColMetadata, value_size: CqlBytesSize) -> RCResult<Option<CQLList>>;
    fn read_cql_set(&mut self, col_meta: &CqlColMetadata, value_size: CqlBytesSize) -> RCResult<Option<CQLSet>>;
    fn read_cql_map(&mut self, col_meta: &CqlColMetadata, value_size: CqlBytesSize) -> RCResult<Option<CQLMap>>;

    fn read_cql_metadata(&mut self) -> RCResult<CqlMetadata>;
    fn read_cql_frame_header(&mut self, version: u8) -> RCResult<CqlFrameHeader>;
    fn read_cql_response(&mut self, version: u8) -> RCResult<CqlResponse>;
    fn read_cql_rows(&mut self, collection_size: CqlBytesSize) -> RCResult<CqlRows>;

    fn read_cql_skip(&mut self, val_type: CqlBytesSize) -> RCResult<()>;

    fn read_cql_value(&mut self, col_meta: &CqlColMetadata, collection_size: CqlBytesSize) -> RCResult<CqlValue>;
    fn read_cql_value_single(&mut self, col_type: &CqlValueType, value_size: CqlBytesSize) -> RCResult<CqlValue>;


}


impl<T: Read> CqlReader for T {
    fn read_cql_bytes(&mut self, val_type: CqlBytesSize) -> RCResult<Vec<u8>> {
        let len:i32 = match val_type {
            CqlBytesSize::Cqli32 => try_bo!(self.read_i32::<BigEndian>(), "Error reading bytes length"),
            CqlBytesSize::Cqli16 => try_bo!(self.read_i16::<BigEndian>(), "Error reading collection bytes length") as i32,
        }; 

        if len < 0 {
            Ok(vec![])
        } else {
            let mut buf = Vec::with_capacity(len as usize);
            try_io!(std::io::copy(&mut self.take(len as u64), &mut buf), "Error at read_exact");
            Ok(buf)
        }
    }

    fn read_cql_bytes_length(&mut self, val_type: CqlBytesSize) -> RCResult<i32> {
        match val_type {
            CqlBytesSize::Cqli32 => Ok(try_bo!(self.read_i32::<BigEndian>(), "Error reading bytes length")),
            CqlBytesSize::Cqli16 => Ok(try_bo!(self.read_i16::<BigEndian>(), "Error reading collection bytes length") as i32)
        }       
    }

    fn read_cql_bytes_length_fixed(&mut self, val_type: CqlBytesSize, length: i32) -> RCResult<i32> {
        let len = match val_type {
            CqlBytesSize::Cqli32 => try_bo!(self.read_i32::<BigEndian>(), "Error reading bytes length") as i32,
            CqlBytesSize::Cqli16 => try_bo!(self.read_i16::<BigEndian>(), "Error reading collection bytes length") as i32
        };
        if len != -1 && len != length {
            Err(RCError::new(format!("Error reading bytes, length ({}) different than expected ({})", len, length), RCErrorType::ReadError))
        }  else {
            Ok(len)
        }    
    }

    fn read_cql_str(&mut self, val_type: CqlBytesSize) -> RCResult<Option<CowStr>> {
        let vec_u8 = try_rc!(self.read_cql_bytes(val_type), "Error reading string data");
        match std::str::from_utf8(&vec_u8) {
            Ok(s) => Ok(Some(Cow::Owned(s.to_owned()))),
            Err(_) => Err(RCError::new("Error reading string, invalid utf8 sequence", RCErrorType::ReadError))
        }     
    }

    fn read_cql_f32(&mut self, val_type: CqlBytesSize) -> RCResult<Option<f32>> {
        try_rc_length!(self.read_cql_bytes_length_fixed(val_type, size_of::<f32>() as i32), "Error reading bytes (float) length");
        Ok(Some(try_bo!(self.read_f32::<BigEndian>(), "Error reading float (float)")))  
    }

    fn read_cql_f64(&mut self, val_type: CqlBytesSize) -> RCResult<Option<f64>> {
        try_rc_length!(self.read_cql_bytes_length_fixed(val_type, size_of::<f64>() as i32), "Error reading bytes (double) length");
        Ok(Some(try_bo!(self.read_f64::<BigEndian>(), "Error reading double (double)")))    
    }

    fn read_cql_i32(&mut self, val_type: CqlBytesSize) -> RCResult<Option<i32>> {
        try_rc_length!(self.read_cql_bytes_length(val_type), "Error reading bytes (int) length");
        Ok(Some(try_bo!(self.read_i32::<BigEndian>(), "Error reading int (i32)")))
    }

    fn read_cql_i64(&mut self, val_type: CqlBytesSize) -> RCResult<Option<i64>> {
        try_rc_length!(self.read_cql_bytes_length(val_type), "Error reading bytes (long) length");
        Ok(Some(try_bo!(self.read_i64::<BigEndian>(), "Error reading long (i64)")))
    }

    fn read_cql_u64(&mut self, val_type: CqlBytesSize) -> RCResult<Option<u64>> {
        try_rc!(self.read_cql_bytes_length(val_type), "Error reading bytes (long) length");
        Ok(Some(try_bo!(self.read_u64::<BigEndian>(), "Error reading long (u64)")))
    }
    
    fn read_cql_blob(&mut self, val_type: CqlBytesSize) -> RCResult<Option<Vec<u8>>> {
        let len = try_rc_length!(self.read_cql_bytes_length(val_type), "Error reading blob length");
        Ok(Some(try_rc!(self.read_cql_bytes(val_type), "Error reading string data")))
    }

    fn read_cql_boolean(&mut self, val_type: CqlBytesSize) -> RCResult<Option<bool>> {
        try_rc_length!(self.read_cql_bytes_length(val_type), "Error reading boolean length");
        match try_bo!(self.read_u8(), "Error reading boolean data") {
            0 => Ok(Some(false)),
            _ => Ok(Some(true))
        }
    }

    fn read_cql_uuid(&mut self, val_type: CqlBytesSize) -> RCResult<Option<Uuid>> {
        let len = try_rc_length!(self.read_cql_bytes_length(val_type), "Error reading uuid length");
        if len != 16 {
            return Err(RCError::new("Invalid uuid length", RCErrorType::ReadError))  
        }
        let vec_u8 = try_rc!(self.read_cql_bytes(val_type), "Error reading uuid data");
        match Uuid::from_bytes(&vec_u8) {
            Some(u) => Ok(Some(u)),
            None => Err(RCError::new("Invalid uuid", RCErrorType::ReadError))
        }      
    }

    fn read_cql_inet(&mut self, val_type: CqlBytesSize) -> RCResult<Option<IpAddress>> {
        let vec = try_rc!(self.read_cql_bytes(val_type), "Error reading value data");
        if vec.len() == 0 {
            Ok(None)
        } else if vec.len() == 4 {
            Ok(Some(IpAddress::Ipv4(Ipv4Addr::new(vec[0], vec[1], vec[2], vec[3]))))
        } else {
            Ok(Some(IpAddress::Ipv6(Ipv6Addr::new(vec[1] as u16 + ((vec[0] as u16) << 8),
              vec[3] as u16 + ((vec[2] as u16) << 8),
              vec[5] as u16 + ((vec[4] as u16) << 8),
              vec[7] as u16 + ((vec[6] as u16) << 8),
              vec[9] as u16 + ((vec[8] as u16) << 8),
              vec[11] as u16 + ((vec[10] as u16) << 8),
              vec[13] as u16 + ((vec[12] as u16) << 8),
              vec[15] as u16 + ((vec[14] as u16) << 8)))))     
        }
    }

    fn read_cql_list(&mut self, col_meta: &CqlColMetadata, value_size: CqlBytesSize) -> RCResult<Option<CQLList>> {
        try_bo!(self.read_i32::<BigEndian>(), "Error reading list size");
        let len = match value_size {
            CqlBytesSize::Cqli32 => try_bo!(self.read_i32::<BigEndian>(), "Error reading list length"),
            CqlBytesSize::Cqli16 => try_bo!(self.read_i16::<BigEndian>(), "Error reading list length") as i32
        };

        let mut list: CQLList = vec![];
        for _ in 0 .. len {
            let col = try_rc!(self.read_cql_value_single(&col_meta.col_type_aux1, value_size), "Error reading list value");
            list.push(col);
        }
        Ok(Some(list))
    }

    fn read_cql_event(&mut self, val_type: CqlBytesSize) -> RCResult<CqlEvent> {

        let event_type = try_rc!(self.read_cql_str(val_type), "Error reading event type (str)").unwrap();
        
        let event_type = CqlEventType::from_str(&event_type.to_string());
        println!("{:?}", event_type);
        let error_msg = "Error reading ";
        match event_type {
            CqlEventType::TopologyChange =>{
                let msg = error_msg.to_string() + "CqlEventType::TopologyChange : "; 
                let change_type = try_rc!(self.read_cql_str(val_type), msg+" change_type (str)")
                                  .unwrap();
                let address = try_rc!(self.read_cql_inet(val_type), msg+" inet (address)");
                
                Ok(CqlEvent::TopologyChange(TopologyChangeType::from_str(
                                            &change_type.to_string()),
                                            address.unwrap()))
            },
            CqlEventType::StatusChange =>{
                println!("Line 206");
                let msg = error_msg.to_string() + "CqlEventType::StatusChange : "; 
                let change_type = try_rc!(self.read_cql_str(val_type), msg+" change_type (str)")
                                  .unwrap();
                                  println!("Line 210");
                let address = try_rc!(self.read_cql_inet(val_type), msg+" inet (address)");
                println!("Line 212");
                Ok(CqlEvent::StatusChange(  StatusChangeType::from_str(
                                            &change_type.to_string()),
                                            address.unwrap()))
            },
            CqlEventType::SchemaChange =>{
                let msg = error_msg.to_string() +"CqlEventType::SchemaChange : "; 
                let change_type = try_rc!(self.read_cql_str(val_type), msg+"change_type (str)");
                let target = try_rc!(self.read_cql_str(val_type), msg+" target (str)").unwrap();

                let options =
                    match target{
                        Cow::Borrowed(SCHEMA_CHANGE_TARGET_KEYSPACE) => {
                            let option = try_rc!(self.read_cql_str(val_type), msg+" options (str)").unwrap();
                            Ok(SchemaChangeOptions::Keyspace(option))
                        },
                        Cow::Borrowed(SCHEMA_CHANGE_TARGET_TABLE) => {
                            let option1 = try_rc!(self.read_cql_str(val_type), msg+" option1 (str)").unwrap();
                            let option2 = try_rc!(self.read_cql_str(val_type), msg+" option2 (str)").unwrap();
                            Ok(SchemaChangeOptions::Table(option1,option2))
                        },
                        Cow::Borrowed(SCHEMA_CHANGE_TARGET_TYPE) => {
                            let option1 = try_rc!(self.read_cql_str(val_type), msg+" option1 (str)").unwrap();
                            let option2 = try_rc!(self.read_cql_str(val_type), msg+" option2 (str)").unwrap();
                            Ok(SchemaChangeOptions::Type(option1,option2))
                        },
                        _ => Err(RCError::new("Unknown schema change type: ", ReadError))
                    };

                Ok(CqlEvent::SchemaChange(  SchemaChangeType::from_str(
                                            &change_type.unwrap().to_string()),
                                            options.unwrap()))
            },
            _=> Err(RCError::new("Unknown EventType", ReadError))
        }
    }



    fn read_cql_set(&mut self, col_meta: &CqlColMetadata, value_size: CqlBytesSize) -> RCResult<Option<CQLSet>> {
        try_bo!(self.read_i32::<BigEndian>(), "Error reading set size");
        let len = match value_size {
            CqlBytesSize::Cqli32 => try_bo!(self.read_i32::<BigEndian>(), "Error reading list length"),
            CqlBytesSize::Cqli16 => try_bo!(self.read_i16::<BigEndian>(), "Error reading list length") as i32
        };

        let mut set: CQLSet = vec![];
        for _ in 0 .. len {
            let col = try_rc!(self.read_cql_value_single(&col_meta.col_type_aux1, value_size), "Error reading set value");
            set.push(col);
        }
        Ok(Some(set))
    }

    fn read_cql_map(&mut self, col_meta: &CqlColMetadata, value_size: CqlBytesSize) -> RCResult<Option<CQLMap>> {
        try_bo!(self.read_i32::<BigEndian>(), "Error reading map size");
        let len = match value_size {
            CqlBytesSize::Cqli32 => try_bo!(self.read_i32::<BigEndian>(), "Error reading list length"),
            CqlBytesSize::Cqli16 => try_bo!(self.read_i16::<BigEndian>(), "Error reading list length") as i32
        };

        let mut map: CQLMap = vec![];
        for _ in 0 .. len {
            let key = try_rc!(self.read_cql_value_single(&col_meta.col_type_aux1, value_size), "Error reading map key");
            let value = try_rc!(self.read_cql_value_single(&col_meta.col_type_aux2, value_size), "Error reading map value");
            map.push(Pair { key: key, value: value});
        }
        Ok(Some(map))
    }

    fn read_cql_skip(&mut self, val_type: CqlBytesSize) -> RCResult<()> {
        let len = try_rc!(self.read_cql_bytes_length(val_type), "Error reading value length");
        try_rc!(self.read_cql_bytes(val_type), "Error reading value data");
        Ok(())     
    }

    fn read_cql_metadata(&mut self) -> RCResult<CqlMetadata> {
        let flags = try_bo!(self.read_u32::<BigEndian>(), "Error reading flags");
        let column_count = try_bo!(self.read_u32::<BigEndian>(), "Error reading column count");

        let (ks, tb) =
        if flags & 0x0001 != 0 {
            let keyspace_str = try_rc_noption!(self.read_cql_str(CqlBytesSize::Cqli16), "Error reading keyspace name");
            let table_str = try_rc_noption!(self.read_cql_str(CqlBytesSize::Cqli16), "Error reading table name");
            (keyspace_str, table_str)
        } else {
            CowStr_tuple_void!()
        };

        let mut row_metadata:Vec<CqlColMetadata> = vec![];
        for _ in 0u32 .. column_count {
            let (keyspace, table) =
            if flags & 0x0001 != 0 {
                (ks.clone(), tb.clone())
            } else {
                let keyspace_str = try_rc_noption!(self.read_cql_str(CqlBytesSize::Cqli16), "Error reading keyspace name");
                let table_str = try_rc_noption!(self.read_cql_str(CqlBytesSize::Cqli16), "Error reading table name");
                (keyspace_str, table_str)
            };
            let col_name = try_rc_noption!(self.read_cql_str(CqlBytesSize::Cqli16), "Error reading column name");
            let type_key = try_bo!(self.read_u16::<BigEndian>(), "Error reading type key");
            let type_aux1 =
            if type_key >= 0x20 {
                let ctype = try_bo!(self.read_u16::<BigEndian>(), "Error reading list/set/map type");
                cql_column_type(ctype)
            } else {
                CqlValueType::ColumnUnknown
            };
            let type_aux2 =
            if type_key == 0x21 {
                let ctype = try_bo!(self.read_u16::<BigEndian>(), "Error reading map type value");
                cql_column_type(ctype)
            } else {
                CqlValueType::ColumnUnknown
            };

            row_metadata.push(CqlColMetadata {
                keyspace: keyspace,
                table: table,
                col_name: col_name,
                col_type: cql_column_type(type_key),
                col_type_aux1: type_aux1,
                col_type_aux2: type_aux2
            });
        }

        Ok(CqlMetadata {
            flags: flags,
            column_count: column_count,
            keyspace: ks,
            table: tb,
            row_metadata: row_metadata,
        })
    }

    fn read_cql_value(&mut self, col_meta: &CqlColMetadata, collection_size: CqlBytesSize) -> RCResult<CqlValue> {
        match col_meta.col_type {
            ColumnList => { Ok(CqlList(try_rc!(self.read_cql_list(col_meta, collection_size), "Error reading column value (list)"))) },
            ColumnMap => { Ok(CqlMap(try_rc!(self.read_cql_map(col_meta, collection_size), "Error reading column value (map)"))) },
            ColumnSet => { Ok(CqlSet(try_rc!(self.read_cql_set(col_meta, collection_size), "Error reading column value (set)"))) },
            _ => self.read_cql_value_single(&col_meta.col_type, CqlBytesSize::Cqli32)
        }
    }

    fn read_cql_value_single(&mut self, col_type: &CqlValueType, val_type: CqlBytesSize) -> RCResult<CqlValue> {
        match *col_type {
            ColumnASCII => Ok(CqlASCII(try_rc!(self.read_cql_str(val_type), "Error reading column value (ASCII)"))),
            ColumnVarChar => Ok(CqlVarchar(try_rc!(self.read_cql_str(val_type), "Error reading column value (VarChar)"))),
            ColumnText => Ok(CqlText(try_rc!(self.read_cql_str(val_type), "Error reading column value (Text)"))),

            ColumnInt => Ok(CqlInt(try_rc!(self.read_cql_i32(val_type), "Error reading column value (Int)"))),
            ColumnBigInt => Ok(CqlBigInt(try_rc!(self.read_cql_i64(val_type), "Error reading column value (BigInt)"))),
            ColumnFloat => Ok(CqlFloat(try_rc!(self.read_cql_f32(val_type), "Error reading column value (Float)"))),
            ColumnDouble => Ok(CqlDouble(try_rc!(self.read_cql_f64(val_type), "Error reading column value (Double)"))),

            ColumnCustom => {
                try_rc!(self.read_cql_skip(val_type), "Error reading column value (custom)");
                println!("Custom parse not implemented");
                Ok(CqlUnknown)
            },
            ColumnBlob => Ok(CqlBlob(try_rc!(self.read_cql_blob(val_type), "Error reading column value (blob)"))),
            ColumnBoolean => Ok(CqlBoolean(try_rc!(self.read_cql_boolean(val_type), "Error reading column vaue (boolean)"))),
            ColumnCounter => Ok(CqlCounter(try_rc!(self.read_cql_i64(val_type), "Error reading column value (counter"))),
            ColumnDecimal => {
                try_rc!(self.read_cql_skip(val_type), "Error reading column value (decimal)");
                println!("Decimal parse not implemented");
                Ok(CqlUnknown)
            },
            ColumnTimestamp => Ok(CqlTimestamp(try_rc!(self.read_cql_u64(val_type), "Error reading column value (timestamp)"))),
            ColumnUuid => Ok(CqlUuid(try_rc!(self.read_cql_uuid(val_type), "Error reading column value (uuid)"))),
            ColumnVarint => {
                try_rc!(self.read_cql_skip(val_type), "Error reading column value (varint)");
                println!("Varint parse not implemented");
                Ok(CqlUnknown)
            },
            ColumnTimeUuid => Ok(CqlTimeUuid(try_rc!(self.read_cql_uuid(val_type), "Error reading column value (timeuuid)"))),
            ColumnInet => Ok(CqlInet(try_rc!(self.read_cql_inet(val_type), "Error reading column value (inet)"))),
            CqlValueType::ColumnUnknown => panic!("Unknown column type !"),
            _ => Err(RCError::new("Trying to read a non-single value type", ReadError))
        }
    }

    // fn read_cql_collection_value(&mut self, col_type: &CqlValueType) -> RCResult<CqlValue> {
    //     match *col_type {
    //         ColumnASCII => Ok(CqlASCII(try_rc!(self.read_cql_str(CqlBytesSize::Cqli16), "Error reading collection value (ASCII)"))),
    //         ColumnVarChar => Ok(CqlVarchar(try_rc!(self.read_cql_str(CqlBytesSize::Cqli16), "Error reading collection value (VarChar)"))),
    //         ColumnText => Ok(CqlText(try_rc!(self.read_cql_str(CqlBytesSize::Cqli16), "Error reading collection value (Text)"))),
    //         ColumnInt => Ok(CqlInt(try_rc!(self.read_cql_i32(CqlBytesSize::Cqli16), "Error reading collection value (Int)"))),
    //         ColumnBigInt => Ok(CqlBigInt(try_rc!(self.read_cql_i64(CqlBytesSize::Cqli16), "Error reading collection value (BigInt)"))),
    //         ColumnFloat => Ok(CqlFloat(try_rc!(self.read_cql_f32(CqlBytesSize::Cqli16), "Error reading collection value (Float)"))),
    //         ColumnDouble => Ok(CqlDouble(try_rc!(self.read_cql_f64(CqlBytesSize::Cqli16), "Error reading collection value (Double)"))),
    //         ColumnCustom => {
    //             try_rc!(self.read_cql_skip(CqlBytesSize::Cqli16), "Error reading collection value (custom)");
    //             println!("Custom parse not implemented");
    //             Ok(CqlUnknown)
    //         },
    //         ColumnBlob => Ok(CqlBlob(try_rc!(self.read_cql_blob(CqlBytesSize::Cqli16), "Error reading collection value (blob)"))),
    //         ColumnBoolean => Ok(CqlBoolean(try_rc!(self.read_cql_boolean(CqlBytesSize::Cqli16), "Error reading collection vaue (boolean)"))),
    //         ColumnCounter => Ok(CqlCounter(try_rc!(self.read_cql_i64(CqlBytesSize::Cqli16), "Error reading collection value (counter"))),
    //         ColumnDecimal => {
    //             try_rc!(self.read_cql_skip(CqlBytesSize::Cqli16), "Error reading collection value (decimal)");
    //             println!("Decimal parse not implemented");
    //             Ok(CqlUnknown)
    //         },
    //         ColumnTimestamp => Ok(CqlTimestamp(try_rc!(self.read_cql_u64(CqlBytesSize::Cqli16), "Error reading collection value (timestamp)"))),
    //         ColumnUuid => Ok(CqlUuid(try_rc!(self.read_cql_uuid(CqlBytesSize::Cqli16), "Error reading collection value (uuid)"))),
    //         ColumnVarint => {
    //             try_rc!(self.read_cql_skip(CqlBytesSize::Cqli16), "Error reading collection value (varint)");
    //             println!("Varint parse not implemented");
    //             Ok(CqlUnknown)
    //         },
    //         ColumnTimeUuid => Ok(CqlTimeUuid(try_rc!(self.read_cql_uuid(CqlBytesSize::Cqli16), "Error reading collection value (timeuuid)"))),
    //         ColumnInet => Ok(CqlInet(try_rc!(self.read_cql_inet(CqlBytesSize::Cqli16), "Error reading collection value (inet)"))),
    //         _ => panic!("Unknown column type !")
    //     }
    // }


    fn read_cql_rows(&mut self, collection_size: CqlBytesSize) -> RCResult<CqlRows> {
        let metadata = try_rc!(self.read_cql_metadata(), "Error reading metadata");
        let rows_count = try_bo!(self.read_u32::<BigEndian>(), "Error reading metadata");

        let mut rows:Vec<CqlRow> = vec![];
        for _ in 0u32..rows_count {
            let mut row = CqlRow{ cols: vec![] };
            for meta in metadata.row_metadata.iter() {
                let col = try_rc!(self.read_cql_value(meta, collection_size), "Error reading column value");
                row.cols.push(col);
            }
            rows.push(row);
        }

        Ok(CqlRows {
            metadata: metadata,
            rows: rows,
        })
    }

    fn read_cql_frame_header(&mut self, version: u8) -> RCResult<CqlFrameHeader> {
        if version >= 3 {
            let mut header_data = [0; 5];
            try_rc!(self.take(5).read(&mut header_data), "Error reading response header");
           
            let version_header = header_data[0];
            let flags = header_data[1];
            let stream = (header_data[3] as u16 + ((header_data[2] as u16) << 8)) as i16;
            let opcode = header_data[4];
            Ok(CqlFrameHeader{
                version: version_header,
                flags: flags,
                stream: stream,
                opcode: opcode,
            })
        } else {
            let mut header_data = [0; 4];
            try_rc!(self.take(4).read(&mut header_data), "Error reading response header");
           
            let version_header = header_data[0];
            let flags = header_data[1];
            let stream = header_data[2] as i16;
            let opcode = header_data[3];
            Ok(CqlFrameHeader{
                version: version_header,
                flags: flags,
                stream: stream,
                opcode: opcode,
            })
        }
    }

    fn read_cql_response(&mut self, version: u8) -> RCResult<CqlResponse> {
        let header = try_rc!(self.read_cql_frame_header(version), "Error reading CQL frame header");

        let body_data = try_rc!(self.read_cql_bytes(CqlBytesSize::Cqli32), "Error reading body response");
        // Code to debug response result. It writes the response's body to a file for inspecting.
        let path = Path::new("body_data.bin");
        let display = path.display();

        // Open a file in write-only mode, returns `IoResult<File>`
        let mut file = match std::fs::File::create(&path) {
            Err(why) => panic!("couldn't create {}: {}", display, why.description()),
            Ok(file) => file,
        };

        match file.write(&body_data) {
            Err(why) => {
                panic!("couldn't write to {}: {}", display, why.description())
            },
            Ok(_) => println!("successfully wrote to {}", display),
        }

        //

        let mut reader = std::io::BufReader::new(Cursor::new(body_data));

        let opcode = opcode_response(header.opcode);

        let body = match opcode {
            OpcodeReady => ResponseReady,
            OpcodeAuthenticate => {
                ResponseAuthenticate(try_rc_noption!(reader.read_cql_str(CqlBytesSize::Cqli16), "Error reading ResponseAuthenticate"))
            }
            OpcodeError => {
                let code = try_bo!(reader.read_u32::<BigEndian>(), "Error reading error code");
                let msg = try_rc_noption!(reader.read_cql_str(CqlBytesSize::Cqli16), "Error reading error message");
                ResponseError(code, msg)
            },
            OpcodeResult => {
                let kind = KindResult::from_u32(try_bo!(reader.read_u32::<BigEndian>(), "Error reading result kind"));
                match kind {
                    Some(KindVoid) => {
                        ResultVoid
                    },
                    Some(KindRows) => {
                        let collection_size = if version >= 3 { CqlBytesSize::Cqli32 } else { CqlBytesSize::Cqli16 };
                        ResultRows(try_rc!(reader.read_cql_rows(collection_size), "Error reading result Rows"))
                    },
                    Some(KindSetKeyspace) => {
                        let msg = try_rc_noption!(reader.read_cql_str(CqlBytesSize::Cqli16), "Error reading result Keyspace");
                        ResultKeyspace(msg)
                    },
                    Some(KindSchemaChange) => {
                        let change  = try_rc_noption!(reader.read_cql_str(CqlBytesSize::Cqli16), "Error reading result SchemaChange (change)");
                        let keyspace = try_rc_noption!(reader.read_cql_str(CqlBytesSize::Cqli16), "Error reading result SchemaChange (keyspace)");
                        let table = try_rc_noption!(reader.read_cql_str(CqlBytesSize::Cqli16), "Error reading result SchemaChange (table)");
                        ResultSchemaChange(change, keyspace, table)
                    },
                    Some(KindPrepared) => {
                        let id = try_rc!(reader.read_cql_bytes(CqlBytesSize::Cqli16), "Error reading result Prepared (id)");
                        let metadata = try_rc!(reader.read_cql_metadata(), "Error reading result Prepared (metadata)");
                        let meta_result = if version >= 0x02 { 
                            Some(try_rc!(reader.read_cql_metadata(), "Error reading result Prepared (metadata result)"))
                        } else {
                            None
                        };
                        ResultPrepared(CqlPreparedStat { id: id, meta: metadata, meta_result: meta_result})
                    }
                    None => return Err(RCError::new("Error reading response body (unknow result kind)", ReadError))
                }
            }
            OpcodeAuthChallenge => {
                ResponseAuthChallenge(try_rc!(reader.read_cql_bytes(CqlBytesSize::Cqli16), "Error reading ResponseAuthChallenge"))
            }
            OpcodeAuthSuccess => {
                ResponseAuthSuccess(try_rc!(reader.read_cql_bytes(CqlBytesSize::Cqli16), "Error reading ResponseAuthSuccess"))
            }
            OpcodeEvent => {
                println!("Reader.read_cql_response -> OpcodeEvent");
                ResponseEvent(try_rc!(reader.read_cql_event(CqlBytesSize::Cqli16), "Error reading ResponseEvent"))
            }
            _ => {
                ResultUnknown
            },
        };

        Ok(CqlResponse {
            version: header.version,
            flags: header.flags,
            stream: header.stream,
            opcode: opcode,
            body: body,
        })
    }

}






