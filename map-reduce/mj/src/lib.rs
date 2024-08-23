use log4rs;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::TcpStream;

// <----------------- COMMON UTILITIES ----------------- >
// Addresses of all servers
pub const VM_LIST: [&str; 10] = [
    "fa23-cs425-5701.cs.illinois.edu",
    "fa23-cs425-5702.cs.illinois.edu",
    "fa23-cs425-5703.cs.illinois.edu",
    "fa23-cs425-5704.cs.illinois.edu",
    "fa23-cs425-5705.cs.illinois.edu",
    "fa23-cs425-5706.cs.illinois.edu",
    "fa23-cs425-5707.cs.illinois.edu",
    "fa23-cs425-5708.cs.illinois.edu",
    "fa23-cs425-5709.cs.illinois.edu",
    "fa23-cs425-5710.cs.illinois.edu",
];

pub const DELIMITER_1: &str = "&&&&&";
pub const DELIMITER_2: &str = "*****";

pub const MJ_FILES: &str = "mj/files/";
pub const WORKER_TEMP_INPUT: &str = "mj/files/input";
pub const WORKER_TEMP_OUTPUT: &str = "mj/files/output";
pub const WORKER_TEMP_OUTPUT_SORTED: &str = "mj/files/sorted-out";

// Hard coding VM1 to always be leader
pub const LEADER_VM_ID: &str = VM_LIST[0];
pub const LEADER_VM_IDX: u8 = 0;

pub const SUCCESS_STATUS_CODE: u8 = 0;
pub const ERROR_STATUS_CODE: u8 = 1;

pub fn write_i32_to_stream(num: i32, stream: &mut TcpStream) -> Result<usize, std::io::Error> {
    let bytes = num.to_le_bytes();
    stream.write(&bytes)
}

pub fn read_i32_from_stream(stream: &mut TcpStream) -> Result<i32, std::io::Error> {
    let mut buf: [u8; 4] = [0; 4];
    stream.read_exact(&mut buf)?;
    let num: i32 = i32::from_le_bytes(buf);
    Ok(num)
}

pub fn write_u8_to_stream(num: u8, stream: &mut TcpStream) -> Result<usize, std::io::Error> {
    let buf: [u8; 1] = [num];
    stream.write(&buf)
}

pub fn read_u8_from_stream(stream: &mut TcpStream) -> Result<u8, std::io::Error> {
    let mut num: [u8; 1] = [0];
    if let Err(err) = stream.read_exact(&mut num) {
        return Err(err);
    }
    return Ok(num[0]);
}

pub fn write_str_to_stream(string: &str, stream: &mut TcpStream) -> Result<usize, std::io::Error> {
    let mut string_bytes = String::from(string).into_bytes();
    string_bytes.insert(0, string_bytes.len() as u8);
    return stream.write(&string_bytes);
}

pub fn read_str_from_stream(stream: &mut TcpStream) -> Result<String, std::io::Error> {
    let mut string_size: [u8; 1] = [0];
    stream.read_exact(&mut string_size)?;
    let mut string_bytes: Vec<u8> = vec![0; string_size[0] as usize];
    stream.read_exact(&mut string_bytes)?;
    let string: String = String::from_utf8(string_bytes).unwrap();
    Ok(string)
}

pub fn write_custom_params(custom_params: Vec<&str>, stream: &mut TcpStream) -> Result<(), String> {
    // Write number of custom params
    write_u8_to_stream(custom_params.len() as u8, stream).map_err(|e| e.to_string())?;

    for param in custom_params {
        write_str_to_stream(param, stream)
            .map_err(|e| format!("Failed to send custom params: {}", e))?;
    }

    Ok(())
}

pub fn read_custom_params(stream: &mut TcpStream) -> Result<Vec<String>, std::io::Error> {
    let arg_count = read_u8_from_stream(stream)?;

    let mut args: Vec<String> = Vec::new();
    for _ in 0..arg_count {
        let arg = read_str_from_stream(stream)?;
        args.push(arg);
    }

    Ok(args)
}

pub fn write_key_filename_tuple(stream: &mut TcpStream, vec_keys: &Vec<(String, String)>) {
    write_u8_to_stream(vec_keys.len() as u8, stream).unwrap();
    for (key, filename) in vec_keys {
        write_str_to_stream(key, stream).unwrap();
        write_str_to_stream(filename, stream).unwrap();
    }
}

pub fn read_key_filename_tuple(stream: &mut TcpStream) -> Result<Vec<(String, String)>, String> {
    let size = read_u8_from_stream(stream).unwrap();

    let mut output_vec: Vec<(String, String)> = Vec::new();
    for _ in 0..size {
        let key = read_str_from_stream(stream).unwrap();
        let filename = read_str_from_stream(stream).unwrap();
        output_vec.push((key, filename));
    }

    Ok(output_vec)
}

// <----------------- COMMUNICATIONS ----------------- >
// Well-known ports
// W - Worker; C - Client; L-Leader;
// E.g. WC is Worker -> Client
pub const WL_PORT: &str = "26776";
pub const CL_PORT: &str = "32338";
pub const LW_PORT: &str = "38336";

pub const VM_ID_PATH: &str = "client_id.txt";

// Open file to read client ID
pub fn get_vm_id() -> u8 {
    let mut id_buf = String::new();
    let mut id_file = File::open(VM_ID_PATH).unwrap();
    let _ = id_file.read_to_string(&mut id_buf);
    let client_id: u8 = id_buf.trim().parse().unwrap();
    return client_id;
}

pub fn setup_logger() {
    log4rs::init_file("logger/log4rs.yml", Default::default()).unwrap();
}

/* Message Interfaces:

Client->Leader Command Requests:
- Type
- Num_tasks
- Delete input (ignored for M)
- Exe
- Sdfs_intermediate_filename_prefix (inter_file_prefix)
- sdfs_src_filename/sdfs_dest_filename (depending on type)

Leader->Worker Maple Task:
- Type
- Input start line
- Input end line (inclusive)
- Exe
- Input file name
- Output file name (inter_file_prefix)

Leader->Worker Reduce Task:
- Type
- Key filenames, comma separated
- Exe
- Output file name (sdfs_dest_filename)
*/

// <----------------- MAPLE ----------------- >
pub const MAPLE_FUNC_INPUT_SIZE: u8 = 100;
pub const MAPLE_TYPE_ID: u8 = 0;
pub const WL_MAPLE_DONE: u8 = 1;
pub const WL_JUICE_DONE: u8 = 2;
pub const SQL_FILE_EXE: &str = "sql_filter";

// <----------------- JUICE ----------------- >
pub const JUICE_TYPE_ID: u8 = 1;

// <----------------- MEMBERSHIP - USING SDFS MEMBERSHIP ----------------- >
// Membership list related
pub const MEMBERSHIP_PATH: &str = "mj/membership";

pub fn get_membership() -> Vec<u8> {
    if !fs::metadata(MEMBERSHIP_PATH).is_ok() {
        // If the file doesn't exist, create an empty file
        let _ = fs::write(MEMBERSHIP_PATH, "");
    }
    let data = fs::read(MEMBERSHIP_PATH).unwrap();

    data
}
