use log4rs;
use std::fs::File;
use std::{io::Read, net::TcpStream};

// <----------------- COMMON ----------------- >
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
    log4rs::init_file("src/logging/log4rs.yml", Default::default()).unwrap();
}

// <----------------- LOGGER----------------- >
pub const PORT: &str = "55555";

pub const HEARTBEAT_PORT: &str = "55557";

pub const HEARTBEAT_FILE: &str = "heartbeat.now";

pub const HOSTS: [&str; 10] = [
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

pub const LOOPBACK: [&str; 1] = ["0.0.0.0"];

pub const DELIM: &str = "💖";

// Reads 4 bytes off the stream and return the i32 formed
// Note: Consumes bytes in stream!!
pub fn payload_size(stream: &mut TcpStream) -> i32 {
    let mut size_bytes: [u8; 4] = [0; 4];
    stream.read_exact(&mut size_bytes).unwrap();
    i32::from_le_bytes(size_bytes)
}

// Trim /r/n (Windows CRLF) or /n (Unix LF)
pub fn trim_newline(string: &mut String) {
    if string.ends_with('\n') {
        string.pop();
    }
    if string.ends_with('\r') {
        string.pop();
    }
}
