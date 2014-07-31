extern crate uuid;
extern crate std;

use super::def::*;
use std::str::SendStr;
use self::uuid::Uuid;
use std::io::net::ip::IpAddr;
use std::str::Owned;

macro_rules! read_and_check_io_length(
    ($reader: expr, $method: ident, $arg: expr, $msg: expr) => {
        {
            let len = match $reader.$method($arg) {
                Ok(val) => val,
                Err(e) => return Err(RCError::new(format!("{} -> {}", $msg, e.desc), ReadError))
            };
            if len < 0 { 
                return Ok(None);
            } else {
                len
            }
        }
    };
    ($reader: expr, $method: ident, $msg: expr) => {
        {
            let len = match $reader.$method() {
                Ok(val) => val,
                Err(e) => return Err(RCError::new(format!("{} -> {}", $msg, e.desc), ReadError))
            };
            if len < 0 { 
                return Ok(None);
            } else {
                len
            }
        }
    }
)

pub trait CqlReader {
    fn read_cql_bytes_length(&mut self, val_type: CqlBytesSize) -> RCResult<i32>;
    fn read_cql_bytes(&mut self, val_type: CqlBytesSize) -> RCResult<Vec<u8>>;

    fn read_cql_str(&mut self, val_type: CqlBytesSize) -> RCResult<Option<SendStr>>;
    fn read_cql_f32(&mut self, val_type: CqlBytesSize) -> RCResult<Option<f32>>;
    fn read_cql_f64(&mut self, val_type: CqlBytesSize) -> RCResult<Option<f64>>;
    fn read_cql_i32(&mut self, val_type: CqlBytesSize) -> RCResult<Option<i32>>;
    fn read_cql_i64(&mut self, val_type: CqlBytesSize) -> RCResult<Option<i64>>;
    fn read_cql_u64(&mut self, val_type: CqlBytesSize) -> RCResult<Option<u64>>;
    fn read_cql_blob(&mut self, val_type: CqlBytesSize) -> RCResult<Option<Vec<u8>>>;
    fn read_cql_boolean(&mut self, val_type: CqlBytesSize) -> RCResult<Option<bool>>;
    fn read_cql_uuid(&mut self, val_type: CqlBytesSize) -> RCResult<Option<Uuid>>;
    fn read_cql_inet(&mut self, val_type: CqlBytesSize) -> RCResult<Option<IpAddr>>;

    fn read_cql_list(&mut self, col_meta: &CqlColMetadata) -> RCResult<Option<CQLList>>;
    fn read_cql_set(&mut self, col_meta: &CqlColMetadata) -> RCResult<Option<CQLSet>>;
    fn read_cql_map(&mut self, col_meta: &CqlColMetadata) -> RCResult<Option<CQLMap>>;

    fn read_cql_metadata(&mut self) -> RCResult<Box<CqlMetadata>>;
    fn read_cql_response(&mut self, version: u8) -> RCResult<CqlResponse>;
    fn read_cql_rows(&mut self) -> RCResult<Box<CqlRows>>;

    fn read_cql_skip(&mut self, val_type: CqlBytesSize) -> RCResult<()>;

    fn read_cql_column_value(&mut self, col_meta: &CqlColMetadata) -> RCResult<CqlValue>;
    fn read_cql_collection_value(&mut self, col_type: &CqlValueType) -> RCResult<CqlValue>;
}

impl<T: std::io::Reader> CqlReader for T {
    fn read_cql_bytes_length(&mut self, val_type: CqlBytesSize) -> RCResult<i32> {
        match val_type {
            Cqli32 => Ok(read_and_check_io_error!(self, read_be_i32, "Error reading bytes length")),
            Cqli16 => Ok(read_and_check_io_error!(self, read_be_i16, "Error reading collection bytes length") as i32)
        }       
    }

    fn read_cql_bytes(&mut self, val_type: CqlBytesSize) -> RCResult<Vec<u8>> {
        let len = read_and_check_io_error!(self, read_cql_bytes_length, val_type, "Error reading bytes length");
        let vec_u8 = read_and_check_io_error!(self, read_exact, len as uint, "Error reading bytes data");
        Ok(vec_u8)    
    }

    fn read_cql_str(&mut self, val_type: CqlBytesSize) -> RCResult<Option<SendStr>> {
        let len = read_and_check_io_length!(self, read_cql_bytes_length, val_type, "Error reading string length");
        let vec_u8 = read_and_check_io_error!(self, read_exact, len as uint, "Error reading string data");
        match String::from_utf8(vec_u8) {
            Ok(s) => Ok(Some(Owned(s))),
            Err(_) => Err(RCError::new("Error reading string, invalid utf8 sequence", ReadError))
        }     
    }

    fn read_cql_f32(&mut self, val_type: CqlBytesSize) -> RCResult<Option<f32>> {
        read_and_check_io_length!(self, read_cql_bytes_length, val_type, "Error reading string length");
        let val = read_and_check_io_error!(self, read_be_f32, "Error reading float (f32)");
        Ok(Some(val))    
    }

    fn read_cql_f64(&mut self, val_type: CqlBytesSize) -> RCResult<Option<f64>> {
        read_and_check_io_length!(self, read_cql_bytes_length, val_type, "Error reading bytes (double) length");
        let val = read_and_check_io_error!(self, read_be_f64, "Error reading double (f64)");
        Ok(Some(val))    
    }

    fn read_cql_i32(&mut self, val_type: CqlBytesSize) -> RCResult<Option<i32>> {
        read_and_check_io_length!(self, read_cql_bytes_length, val_type, "Error reading bytes (int) length");
        let val = read_and_check_io_error!(self, read_be_i32, "Error reading int (i32)");
        Ok(Some(val))
    }

    fn read_cql_i64(&mut self, val_type: CqlBytesSize) -> RCResult<Option<i64>> {
        read_and_check_io_length!(self, read_cql_bytes_length, val_type, "Error reading bytes (long) length");
        let val = read_and_check_io_error!(self, read_be_i64, "Error reading long (i64)");
        Ok(Some(val))
    }

    fn read_cql_u64(&mut self, val_type: CqlBytesSize) -> RCResult<Option<u64>> {
        read_and_check_io_length!(self, read_cql_bytes_length, val_type, "Error reading bytes (long) length");
        let val = read_and_check_io_error!(self, read_be_u64, "Error reading long (u64)");
        Ok(Some(val))
    }
    
    fn read_cql_blob(&mut self, val_type: CqlBytesSize) -> RCResult<Option<Vec<u8>>> {
        let len = read_and_check_io_length!(self, read_cql_bytes_length, val_type, "Error reading blob length");
        let vec_u8 = read_and_check_io_error!(self, read_exact, len as uint, "Error reading string data");
        Ok(Some(vec_u8))
    }

    fn read_cql_boolean(&mut self, val_type: CqlBytesSize) -> RCResult<Option<bool>> {
        read_and_check_io_length!(self, read_cql_bytes_length, val_type, "Error reading boolean length");
        let b = match read_and_check_io_error!(self, read_u8, "Error reading boolean data") {
            0 => Some(false),
            _ => Some(true)
        };
        Ok(b)     
    }

    fn read_cql_uuid(&mut self, val_type: CqlBytesSize) -> RCResult<Option<Uuid>> {
        let len = read_and_check_io_length!(self, read_cql_bytes_length, val_type, "Error reading uuid length");
        let vec_u8 = read_and_check_io_error!(self, read_exact, len as uint, "Error reading uuid data");
        match Uuid::from_bytes(vec_u8.as_slice()) {
            Some(u) => Ok(Some(u)),
            None => Err(RCError::new("Invalid uuid", ReadError))
        }      
    }


    fn read_cql_inet(&mut self, val_type: CqlBytesSize) -> RCResult<Option<IpAddr>> {
        let len = read_and_check_io_length!(self, read_cql_bytes_length, val_type, "Error reading value length");
        let vec = read_and_check_io_error!(self, read_exact, len as uint, "Error reading value data");
        if len == 4 {
            Ok(Some(std::io::net::ip::Ipv4Addr(*vec.get(0), *vec.get(1), *vec.get(2), *vec.get(3))))
        } else {
            Ok(Some(std::io::net::ip::Ipv6Addr(*vec.get(1) as u16 + ((*vec.get(0) as u16) << 8),
                                          *vec.get(3) as u16 + ((*vec.get(2) as u16) << 8),
                                          *vec.get(5) as u16 + ((*vec.get(4) as u16) << 8),
                                          *vec.get(7) as u16 + ((*vec.get(6) as u16) << 8),
                                          *vec.get(9) as u16 + ((*vec.get(8) as u16) << 8),
                                          *vec.get(11) as u16 + ((*vec.get(10) as u16) << 8),
                                          *vec.get(13) as u16 + ((*vec.get(12) as u16) << 8),
                                          *vec.get(15) as u16 + ((*vec.get(14) as u16) << 8))))           
        }
    }

    fn read_cql_list(&mut self, col_meta: &CqlColMetadata) -> RCResult<Option<CQLList>> {
        read_and_check_io_length!(self, read_be_i32, "Error reading list size");
        let len = read_and_check_io_error!(self, read_be_i16, "Error reading list length");
        let mut list: CQLList = vec![];
        for _ in range(0, len) {
            let col = read_and_check_io_error!(self, read_cql_collection_value, &col_meta.col_type_aux1, "Error reading list value");
            list.push(col);
        }
        Ok(Some(list))
    }

    fn read_cql_set(&mut self, col_meta: &CqlColMetadata) -> RCResult<Option<CQLSet>> {
        read_and_check_io_length!(self, read_be_i32, "Error reading set size");
        let len = read_and_check_io_error!(self, read_be_i16, "Error reading set length");
        let mut set: CQLSet = vec![];
        for _ in range(0, len) {
            let col = read_and_check_io_error!(self, read_cql_collection_value, &col_meta.col_type_aux1, "Error reading set value");
            set.push(col);
        }
        Ok(Some(set))
    }

    fn read_cql_map(&mut self, col_meta: &CqlColMetadata) -> RCResult<Option<CQLMap>> {
        read_and_check_io_length!(self, read_be_i32, "Error reading map size");
        let len = read_and_check_io_error!(self, read_be_i16, "Error reading map length");
        let mut map: CQLMap = vec![];
        for _ in range(0, len) {
            let key = read_and_check_io_error!(self, read_cql_collection_value, &col_meta.col_type_aux1, "Error reading map key");
            let value = read_and_check_io_error!(self, read_cql_collection_value, &col_meta.col_type_aux2, "Error reading map value");
            map.push(Pair { key: key, value: value});
        }
        Ok(Some(map))
    }

    fn read_cql_skip(&mut self, val_type: CqlBytesSize) -> RCResult<()> {
        let len = read_and_check_io_error!(self, read_cql_bytes_length, val_type, "Error reading value length");
        read_and_check_io_error!(self, read_exact, len as uint, "Error reading value data");
        Ok(())     
    }

    fn read_cql_metadata(&mut self) -> RCResult<Box<CqlMetadata>> {
        let flags = read_and_check_io_error!(self, read_be_u32, "Error reading flags");
        let column_count = read_and_check_io_error!(self, read_be_u32, "Error reading column count");

        let (ks, tb) =
            if flags & 0x0001 != 0 {
                let keyspace_str = read_and_check_io_error!(self, read_cql_str, Cqli16, "Error reading keyspace name").expect("Unknown error");
                let table_str = read_and_check_io_error!(self, read_cql_str, Cqli16, "Error reading table name").expect("Unknown error");
                (keyspace_str, table_str)
            } else {
                sendstr_tuple_void!()
            };

        let mut row_metadata:Vec<CqlColMetadata> = vec![];
        for _ in range(0u32, column_count) {
            let (keyspace, table) =
                if flags & 0x0001 != 0 {
                    (ks.clone(), tb.clone())
                } else {
                    let keyspace_str = read_and_check_io_error!(self, read_cql_str, Cqli16, "Error reading keyspace name").expect("Unknown error");
                    let table_str = read_and_check_io_error!(self, read_cql_str, Cqli16, "Error reading table name").expect("Unknown error");
                    (keyspace_str, table_str)
                };
            let col_name = read_and_check_io_error!(self, read_cql_str, Cqli16, "Error reading column name").expect("Unknown error");
            let type_key = read_and_check_io_error!(self, read_be_u16, "Error reading type key");
            let type_aux1 =
                if type_key >= 0x20 {
                    let ctype = read_and_check_io_error!(self, read_be_u16, "Error reading list/set/map type");
                    cql_column_type(ctype)
                } else {
                    ColumnUnknown
                };
            let type_aux2 =
                if type_key == 0x21 {
                    let ctype = read_and_check_io_error!(self, read_be_u16, "Error reading map type value");
                    cql_column_type(ctype)
                } else {
                    ColumnUnknown
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

        Ok(box CqlMetadata {
            flags: flags,
            column_count: column_count,
            keyspace: ks,
            table: tb,
            row_metadata: row_metadata,
        })
    }

    fn read_cql_column_value(&mut self, col_meta: &CqlColMetadata) -> RCResult<CqlValue> {
        match col_meta.col_type {
            ColumnASCII => Ok(CqlASCII(read_and_check_io_error!(self, read_cql_str, Cqli32, "Error reading column value (ASCII)"))),
            ColumnVarChar => Ok(CqlVarchar(read_and_check_io_error!(self, read_cql_str, Cqli32, "Error reading column value (VarChar)"))),
            ColumnText => Ok(CqlText(read_and_check_io_error!(self, read_cql_str, Cqli32, "Error reading column value (Text)"))),

            ColumnInt => Ok(CqlInt(read_and_check_io_error!(self, read_cql_i32, Cqli32, "Error reading column value (Int)"))),
            ColumnBigInt => Ok(CqlBigInt(read_and_check_io_error!(self, read_cql_i64, Cqli32, "Error reading column value (BigInt)"))),
            ColumnFloat => Ok(CqlFloat(read_and_check_io_error!(self, read_cql_f32, Cqli32, "Error reading column value (Float)"))),
            ColumnDouble => Ok(CqlDouble(read_and_check_io_error!(self, read_cql_f64, Cqli32, "Error reading column value (Double)"))),

            ColumnList => { Ok(CqlList(read_and_check_io_error!(self, read_cql_list, col_meta, "Error reading column value (list)"))) },
            ColumnCustom => {
                read_and_check_io_error!(self, read_cql_skip, Cqli32, "Error reading column value (custom)");
                println!("Custom parse not implemented");
                Ok(CqlUnknown)
            },
            ColumnBlob => Ok(CqlBlob(read_and_check_io_error!(self, read_cql_blob, Cqli32, "Error reading column value (blob)"))),
            ColumnBoolean => Ok(CqlBoolean(read_and_check_io_error!(self, read_cql_boolean, Cqli32, "Error reading column vaue (boolean)"))),
            ColumnCounter => Ok(CqlCounter(read_and_check_io_error!(self, read_cql_i64, Cqli32, "Error reading column value (counter"))),
            ColumnDecimal => {
                read_and_check_io_error!(self, read_cql_skip, Cqli32, "Error reading column value (decimal)");
                println!("Decimal parse not implemented");
                Ok(CqlUnknown)
            },
            ColumnTimestamp => Ok(CqlTimestamp(read_and_check_io_error!(self, read_cql_u64, Cqli32, "Error reading column value (timestamp)"))),
            ColumnUuid => Ok(CqlUuid(read_and_check_io_error!(self, read_cql_uuid, Cqli32, "Error reading column value (uuid)"))),
            ColumnVarint => {
                read_and_check_io_error!(self, read_cql_skip, Cqli32, "Error reading column value (varint)");
                println!("Varint parse not implemented");
                Ok(CqlUnknown)
            },
            ColumnTimeUuid => Ok(CqlTimeUuid(read_and_check_io_error!(self, read_cql_uuid, Cqli32, "Error reading column value (timeuuid)"))),
            ColumnInet => Ok(CqlInet(read_and_check_io_error!(self, read_cql_inet, Cqli32, "Error reading column value (inet)"))),
            ColumnMap => { Ok(CqlMap(read_and_check_io_error!(self, read_cql_map, col_meta, "Error reading column value (map)"))) },
            ColumnSet => { Ok(CqlSet(read_and_check_io_error!(self, read_cql_set, col_meta, "Error reading column value (set)"))) },
            ColumnUnknown => fail!("Unknown column type !")
        }
    }

    fn read_cql_collection_value(&mut self, col_type: &CqlValueType) -> RCResult<CqlValue> {
        match *col_type {
            ColumnASCII => Ok(CqlASCII(read_and_check_io_error!(self, read_cql_str, Cqli16, "Error reading collection value (ASCII)"))),
            ColumnVarChar => Ok(CqlVarchar(read_and_check_io_error!(self, read_cql_str, Cqli16, "Error reading collection value (VarChar)"))),
            ColumnText => Ok(CqlText(read_and_check_io_error!(self, read_cql_str, Cqli16, "Error reading collection value (Text)"))),
            ColumnInt => Ok(CqlInt(read_and_check_io_error!(self, read_cql_i32, Cqli16, "Error reading collection value (Int)"))),
            ColumnBigInt => Ok(CqlBigInt(read_and_check_io_error!(self, read_cql_i64, Cqli16, "Error reading collection value (BigInt)"))),
            ColumnFloat => Ok(CqlFloat(read_and_check_io_error!(self, read_cql_f32, Cqli16, "Error reading collection value (Float)"))),
            ColumnDouble => Ok(CqlDouble(read_and_check_io_error!(self, read_cql_f64, Cqli16, "Error reading collection value (Double)"))),
            ColumnCustom => {
                read_and_check_io_error!(self, read_cql_skip, Cqli16, "Error reading collection value (custom)");
                println!("Custom parse not implemented");
                Ok(CqlUnknown)
            },
            ColumnBlob => Ok(CqlBlob(read_and_check_io_error!(self, read_cql_blob, Cqli16, "Error reading collection value (blob)"))),
            ColumnBoolean => Ok(CqlBoolean(read_and_check_io_error!(self, read_cql_boolean, Cqli16, "Error reading collection vaue (boolean)"))),
            ColumnCounter => Ok(CqlCounter(read_and_check_io_error!(self, read_cql_i64, Cqli16, "Error reading collection value (counter"))),
            ColumnDecimal => {
                read_and_check_io_error!(self, read_cql_skip, Cqli16, "Error reading collection value (decimal)");
                println!("Decimal parse not implemented");
                Ok(CqlUnknown)
            },
            ColumnTimestamp => Ok(CqlTimestamp(read_and_check_io_error!(self, read_cql_u64, Cqli16, "Error reading collection value (timestamp)"))),
            ColumnUuid => Ok(CqlUuid(read_and_check_io_error!(self, read_cql_uuid, Cqli16, "Error reading collection value (uuid)"))),
            ColumnVarint => {
                read_and_check_io_error!(self, read_cql_skip, Cqli16, "Error reading collection value (varint)");
                println!("Varint parse not implemented");
                Ok(CqlUnknown)
            },
            ColumnTimeUuid => Ok(CqlTimeUuid(read_and_check_io_error!(self, read_cql_uuid, Cqli16, "Error reading collection value (timeuuid)"))),
            ColumnInet => Ok(CqlInet(read_and_check_io_error!(self, read_cql_inet, Cqli16, "Error reading collection value (inet)"))),
            _ => fail!("Unknown column type !")
        }
    }


    fn read_cql_rows(&mut self) -> RCResult<Box<CqlRows>> {
        let metadata = read_and_check_io_error!(self, read_cql_metadata, "Error reading metadata");
        let rows_count = read_and_check_io_error!(self, read_be_u32, "Error reading metadata");

        let mut rows:Vec<CqlRow> = vec![];
        for _ in std::iter::range(0u32, rows_count) {
            let mut row = CqlRow{ cols: vec![] };
            for meta in metadata.row_metadata.iter() {
                let col = read_and_check_io_error!(self, read_cql_column_value, meta, "Error reading column value");
                row.cols.push(col);
            }
            rows.push(row);
        }

        Ok(box CqlRows {
            metadata: metadata,
            rows: rows,
        })
    }

    fn read_cql_response(&mut self, version: u8) -> RCResult<CqlResponse> {
        let header_data = read_and_check_io_error!(self, read_exact, 4, "Error reading response header");

        let version_header = *header_data.get(0);
        let flags = *header_data.get(1);
        let stream = *header_data.get(2) as i8;
        let opcode = opcode_response(*header_data.get(3));
        
        let length = read_and_check_io_error!(self, read_be_i32, "Error reading length response");

        let body_data = read_and_check_io_error!(self, read_exact, length as uint, "Error reading body response");

        /* Code to debug response result. It writes teh response's body to a file for inspecting.

        let path = Path::new("body_data.bin");
        let display = path.display();

        // Open a file in write-only mode, returns `IoResult<File>`
        let mut file = match std::io::File::create(&path) {
            Err(why) => fail!("couldn't create {}: {}", display, why.desc),
            Ok(file) => file,
        };

        match file.write(body_data.as_slice()) {
            Err(why) => {
                fail!("couldn't write to {}: {}", display, why.desc)
            },
            Ok(_) => println!("successfully wrote to {}", display),
        }

        */

        let mut reader = std::io::BufReader::new(body_data.as_slice());

        let body = match opcode {
            OpcodeReady => ResponseReady,
            OpcodeAuthenticate => {
                ResponseAuth(read_and_check_io_error!(reader, read_cql_str, Cqli16, "Error reading ResponseAuth").unwrap())
            }
            OpcodeError => {
                let code = read_and_check_io_error!(reader, read_be_u32, "Error reading error code");
                let msg = read_and_check_io_error!(reader, read_cql_str, Cqli16, "Error reading error message").unwrap();
                ResponseError(code, msg)
            },
            OpcodeResult => {
                let kind: Option<KindResult> = std::num::FromPrimitive::from_u32(read_and_check_io_error!(reader, read_be_u32, "Error reading result kind"));
                match kind {
                    Some(KindVoid) => {
                        ResultVoid
                    },
                    Some(KindRows) => {
                        ResultRows(read_and_check_io_error!(reader, read_cql_rows, "Error reading result Rows"))
                    },
                    Some(KindSetKeyspace) => {
                        let msg = read_and_check_io_error!(reader, read_cql_str, Cqli16, "Error reading result Keyspace").unwrap();
                        ResultKeyspace(msg)
                    },
                    Some(KindSchemaChange) => {
                        let change  = read_and_check_io_error!(reader, read_cql_str, Cqli16, "Error reading result SchemaChange (change)").unwrap();
                        let keyspace = read_and_check_io_error!(reader, read_cql_str, Cqli16, "Error reading result SchemaChange (keyspace)").unwrap();
                        let table = read_and_check_io_error!(reader, read_cql_str, Cqli16, "Error reading result SchemaChange (table)").unwrap();
                        ResultSchemaChange(change, keyspace, table)
                    },
                    Some(KindPrepared) => {
                        let id = read_and_check_io_error!(reader, read_cql_bytes, Cqli16, "Error reading result Prepared (id)");
                        let metadata = read_and_check_io_error!(reader, read_cql_metadata, "Error reading result Prepared (metadata)");
                        let meta_result = if version >= 0x02 { 
                            Some(read_and_check_io_error!(reader, read_cql_metadata, "Error reading result Prepared (metadata result)"))
                        } else {
                            None
                        };
                        let preps = box CqlPreparedStat { id: id, meta: metadata, meta_result: meta_result};
                        ResultPrepared(preps)
                    }
                    None => return Err(RCError::new("Error reading response body (unknow result kind)", ReadError))
                }
            }
            _ => {
                ResultUnknown
            },//ResponseEmpty,
        };

        Ok(CqlResponse {
            version: version_header,
            flags: flags,
            stream: stream,
            opcode: opcode,
            body: body,
        })
    }
}






